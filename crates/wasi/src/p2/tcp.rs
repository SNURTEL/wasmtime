use crate::MAX_READ_SIZE_ALLOC;
use crate::p2::bindings::sockets::network::ErrorCode;
use crate::p2::{
    DynInputStream, DynOutputStream, InputStream, OutputStream, Pollable, SocketResult, StreamError,
};
use crate::sockets::{
    MaybeReady, SocketBoundaryObserver, TcpListenStream, TcpReceiveStream, TcpSendStream,
    TcpSocket as P3Socket, noop_cx,
};
use std::future::poll_fn;
use std::mem;
use std::net::Shutdown;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Poll, ready};
use wasmtime::Result;
use wasmtime_wasi_io::streams::StreamResult;

/// A TCP socket + associated p2 bookkeeping.
pub struct TcpSocket {
    pub(crate) inner: P3Socket,
    pub(crate) in_progress_operation: Option<AsyncOperation>,
    pub(crate) listener: Option<TcpListenStream>,
    /// Host tracing connection id assigned at start-connect (egress) or accept.
    pub(crate) trace_conn_id: Option<u64>,
    reader: Option<TcpReader>,
    writer: Option<TcpWriter>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AsyncOperation {
    Bind,
    Connect,
    Listen,
}

impl TcpSocket {
    pub(crate) fn new(inner: P3Socket) -> Self {
        Self {
            inner,
            in_progress_operation: None,
            listener: None,
            trace_conn_id: None,
            reader: None,
            writer: None,
        }
    }
    pub(crate) fn take_streams(
        &mut self,
        observer: Option<Arc<dyn SocketBoundaryObserver>>,
        conn_id: u64,
    ) -> SocketResult<(DynInputStream, DynOutputStream)> {
        let reader = TcpReader::new(self.inner.take_receive_stream()?, observer.clone(), conn_id);
        let writer = TcpWriter::new(self.inner.take_send_stream()?, observer, conn_id);
        self.reader = Some(reader.clone());
        self.writer = Some(writer.clone());
        let input: DynInputStream = Box::new(reader);
        let output: DynOutputStream = Box::new(writer);
        Ok((input, output))
    }
    pub(crate) fn shutdown(&mut self, how: Shutdown) -> SocketResult<()> {
        let reader = self.reader.as_mut().ok_or(ErrorCode::InvalidState)?;
        let writer = self.writer.as_mut().ok_or(ErrorCode::InvalidState)?;

        if let Shutdown::Both | Shutdown::Read = how {
            reader.state.lock().unwrap().shutdown();
        }

        if let Shutdown::Both | Shutdown::Write = how {
            writer.state.lock().unwrap().shutdown();
        }

        Ok(())
    }
}

enum ReadState {
    Open(TcpReceiveStream),
    Closed,
}
impl ReadState {
    fn read(&mut self, size: usize) -> StreamResult<bytes::Bytes> {
        let Self::Open(stream) = self else {
            return Err(StreamError::Closed);
        };
        if size == 0 {
            return Ok(bytes::Bytes::new());
        }
        let mut buf = bytes::BytesMut::zeroed(size.min(crate::MAX_READ_SIZE_ALLOC));
        let n = match stream.poll_read(&mut noop_cx(), &mut buf) {
            Poll::Pending => 0,
            Poll::Ready(Ok(0)) => {
                *self = ReadState::Closed;
                return Err(StreamError::Closed);
            }
            Poll::Ready(Ok(n)) => n,
            Poll::Ready(Err(e)) => {
                *self = ReadState::Closed;
                return Err(StreamError::LastOperationFailed(e.into()));
            }
        };

        buf.truncate(n);
        Ok(buf.freeze())
    }

    fn shutdown(&mut self) {
        *self = ReadState::Closed;
    }

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<()> {
        match self {
            Self::Open(stream) => stream.poll_ready(cx),
            Self::Closed => Poll::Ready(()),
        }
    }
}

#[derive(Clone)]
struct TcpReader {
    state: Arc<Mutex<ReadState>>,
    observer: Option<Arc<dyn SocketBoundaryObserver>>,
    conn_id: u64,
}
impl TcpReader {
    fn new(
        stream: TcpReceiveStream,
        observer: Option<Arc<dyn SocketBoundaryObserver>>,
        conn_id: u64,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ReadState::Open(stream))),
            observer,
            conn_id,
        }
    }
}

#[async_trait::async_trait]
impl InputStream for TcpReader {
    fn read(&mut self, size: usize) -> StreamResult<bytes::Bytes> {
        let bytes = self.state.lock().unwrap().read(size)?;
        if !bytes.is_empty() {
            if let Some(obs) = &self.observer {
                obs.on_tcp_read(self.conn_id, &bytes);
            }
        }
        Ok(bytes)
    }
}

#[async_trait::async_trait]
impl Pollable for TcpReader {
    async fn ready(&mut self) {
        std::future::poll_fn(|cx| self.state.lock().unwrap().poll_ready(cx)).await
    }
}

/// A cloneable subset of StreamError
#[derive(Debug, Clone)]
enum WriteError {
    Closed,
    LastOperationFailed(ErrorCode),
}
impl From<WriteError> for StreamError {
    fn from(err: WriteError) -> Self {
        match err {
            WriteError::Closed => StreamError::Closed,
            WriteError::LastOperationFailed(e) => StreamError::LastOperationFailed(e.into()),
        }
    }
}

enum WriteState {
    Ready(TcpSendStream, usize),
    Writing(MaybeReady<Result<TcpSendStream, WriteError>>),
    Closing(MaybeReady<Result<(), WriteError>>),
    Closed(WriteError),
}

impl WriteState {
    fn take(&mut self) -> WriteState {
        mem::replace(self, WriteState::Closed(WriteError::Closed))
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        match self.poll_ready(&mut noop_cx()) {
            Poll::Pending => Ok(0),
            Poll::Ready(Ok((_, permit))) => {
                *permit = MAX_READ_SIZE_ALLOC;
                Ok(*permit)
            }
            Poll::Ready(Err(e)) => Err(e),
        }
    }

    fn write(&mut self, mut bytes: bytes::Bytes) -> StreamResult<()> {
        let mut stream = match self {
            WriteState::Ready(_, permit) if bytes.len() <= *permit => {
                if bytes.is_empty() {
                    return Ok(());
                }

                let WriteState::Ready(stream, _) = self.take() else {
                    unreachable!()
                };
                stream
            }
            WriteState::Closed(e) => {
                return Err(e.clone().into());
            }
            _ => {
                return Err(StreamError::Trap(wasmtime::format_err!(
                    "not permitted to write {} bytes",
                    bytes.len()
                )));
            }
        };

        *self = WriteState::Writing(MaybeReady::poll_or_spawn(async move {
            while !bytes.is_empty() {
                match stream.write(&bytes).await {
                    Ok(n) => {
                        let _ = bytes.split_to(n);
                    }
                    Err(crate::sockets::ErrorCode::ConnectionBroken) => {
                        return Err(WriteError::Closed);
                    }
                    Err(e) => {
                        return Err(WriteError::LastOperationFailed(e.into()));
                    }
                }
            }

            Ok(stream)
        }));

        // Attempt to finish the write, surfacing potential errors immediately:
        match self.poll_ready(&mut noop_cx()) {
            Poll::Pending | Poll::Ready(Ok(_)) => Ok(()),
            Poll::Ready(Err(e)) => Err(e),
        }
    }

    fn flush(&mut self) -> StreamResult<()> {
        // `flush` is a no-op here. Writes happen on background tasks and will
        // always be delivered to the OS as soon as possible. There's nothing
        // for `flush` to do here that will speed up that process.
        match self {
            WriteState::Ready(..) | WriteState::Writing(_) | WriteState::Closing(_) => Ok(()),
            WriteState::Closed(e) => Err(e.clone().into()),
        }
    }

    pub(crate) fn shutdown(&mut self) {
        *self = match self.take() {
            // No write in progress, immediately drop the inner stream:
            WriteState::Ready(..) => WriteState::Closed(WriteError::Closed),

            // Schedule the shutdown after the current write has finished:
            WriteState::Writing(write) => {
                WriteState::Closing(MaybeReady::poll_or_spawn(async move {
                    _ = write.into_future().await?;
                    Ok(())
                }))
            }

            s => s,
        };
    }

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<StreamResult<(&mut TcpSendStream, &mut usize)>> {
        match self {
            WriteState::Writing(write) => {
                ready!(write.poll_ready(cx));
                let WriteState::Writing(write) = self.take() else {
                    unreachable!()
                };
                *self = match write.unwrap_ready() {
                    Ok(stream) => WriteState::Ready(stream, 0),
                    Err(err) => WriteState::Closed(err),
                };
            }
            WriteState::Closing(close) => {
                ready!(close.poll_ready(cx));
                let WriteState::Closing(close) = self.take() else {
                    unreachable!()
                };
                *self = match close.unwrap_ready() {
                    Ok(()) => WriteState::Closed(WriteError::Closed),
                    Err(err) => WriteState::Closed(err),
                };
            }
            _ => {}
        }

        match self {
            WriteState::Ready(stream, permit) => match stream.poll_ready(cx) {
                Poll::Ready(()) => Poll::Ready(Ok((stream, permit))),
                Poll::Pending => Poll::Pending,
            },
            WriteState::Writing(..) | WriteState::Closing(..) => Poll::Pending,
            WriteState::Closed(e) => Poll::Ready(Err(e.clone().into())),
        }
    }
}

#[derive(Clone)]
struct TcpWriter {
    state: Arc<Mutex<WriteState>>,
    observer: Option<Arc<dyn SocketBoundaryObserver>>,
    conn_id: u64,
}
impl TcpWriter {
    fn new(
        stream: TcpSendStream,
        observer: Option<Arc<dyn SocketBoundaryObserver>>,
        conn_id: u64,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(WriteState::Ready(stream, 0))),
            observer,
            conn_id,
        }
    }
}

#[async_trait::async_trait]
impl OutputStream for TcpWriter {
    fn write(&mut self, bytes: bytes::Bytes) -> StreamResult<()> {
        let bytes = if let Some(obs) = &self.observer {
            match obs.accept_tcp_write(self.conn_id, bytes) {
                Ok(b) => b,
                Err(e) => {
                    return Err(StreamError::LastOperationFailed(wasmtime::Error::msg(
                        e.message(),
                    )));
                }
            }
        } else {
            bytes
        };
        if bytes.is_empty() {
            return Ok(());
        }
        self.state.lock().unwrap().write(bytes)
    }

    fn flush(&mut self) -> StreamResult<()> {
        if let Some(obs) = &self.observer {
            if let Err(e) = obs.flush_tcp_write(self.conn_id) {
                return Err(StreamError::LastOperationFailed(wasmtime::Error::msg(
                    e.message(),
                )));
            }
        }
        self.state.lock().unwrap().flush()
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        let native = self.state.lock().unwrap().check_write()?;
        if let Some(obs) = &self.observer {
            match obs.check_tcp_write(self.conn_id, native) {
                Ok(n) => Ok(n),
                Err(e) => Err(StreamError::LastOperationFailed(wasmtime::Error::msg(
                    e.message(),
                ))),
            }
        } else {
            Ok(native)
        }
    }

    fn write_zeroes(&mut self, nelem: usize) -> StreamResult<()> {
        // Always route through `write` so an installed boundary observer sees bytes.
        let n = self.check_write()?;
        if nelem > n {
            return Err(StreamError::trap(
                "cannot write more zeroes than `check_write` allows",
            ));
        }
        let bs = bytes::Bytes::from_iter(core::iter::repeat(0).take(nelem));
        self.write(bs)
    }

    async fn cancel(&mut self) {
        // Wait for background writes to finish in order to prevent silently
        // dropping data that (from the guest's perspective) was already written.
        self.ready().await
    }
}

#[async_trait::async_trait]
impl Pollable for TcpWriter {
    async fn ready(&mut self) {
        poll_fn(|cx| self.state.lock().unwrap().poll_ready(cx).map(|_| ())).await;
    }
}
