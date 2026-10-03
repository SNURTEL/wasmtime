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
    pub(crate) trace_lease: Option<Arc<TraceLease>>,
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
            trace_lease: None,
            reader: None,
            writer: None,
        }
    }
    pub(crate) fn take_streams(
        &mut self,
        observer: Option<Arc<dyn SocketBoundaryObserver>>,
        conn_id: u64,
    ) -> SocketResult<(DynInputStream, DynOutputStream)> {
        let lease = self
            .trace_lease
            .get_or_insert_with(|| {
                Arc::new(TraceLease {
                    observer: observer.clone(),
                    conn_id,
                })
            })
            .clone();
        let reader = TcpReader::new(
            self.inner.take_receive_stream()?,
            observer.clone(),
            conn_id,
            lease.clone(),
        );
        let writer = TcpWriter::new(self.inner.take_send_stream()?, observer, conn_id, lease);
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

/// Last-resource notification, retained by pending native writes as well as all socket resources.
pub(crate) struct TraceLease {
    pub(crate) observer: Option<Arc<dyn SocketBoundaryObserver>>,
    pub(crate) conn_id: u64,
}
impl Drop for TraceLease {
    fn drop(&mut self) {
        if let Some(obs) = &self.observer {
            obs.on_closed(self.conn_id);
        }
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
    _lease: Arc<TraceLease>,
}
impl TcpReader {
    fn new(
        stream: TcpReceiveStream,
        observer: Option<Arc<dyn SocketBoundaryObserver>>,
        conn_id: u64,
        lease: Arc<TraceLease>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ReadState::Open(stream))),
            observer,
            conn_id,
            _lease: lease,
        }
    }
}

#[async_trait::async_trait]
impl InputStream for TcpReader {
    fn read(&mut self, size: usize) -> StreamResult<bytes::Bytes> {
        if let Some(obs) = &self.observer {
            obs.check_tcp_state(self.conn_id)
                .map_err(|e| StreamError::LastOperationFailed(wasmtime::Error::msg(e.message())))?;
        }
        let (result, was_open) = {
            let mut state = self.state.lock().unwrap();
            let open = matches!(*state, ReadState::Open(_));
            (state.read(size), open)
        };
        let bytes = match result {
            Ok(bytes) => bytes,
            Err(e) => {
                if was_open {
                    if let Some(obs) = &self.observer {
                        match &e {
                            StreamError::Closed => obs.on_read_eof(self.conn_id),
                            _ => obs.on_error(self.conn_id, "read-error"),
                        }
                    }
                }
                return Err(e);
            }
        };
        if !bytes.is_empty() {
            if let Some(obs) = &self.observer {
                obs.validate_tcp_read(self.conn_id, &bytes).map_err(|e| {
                    StreamError::LastOperationFailed(wasmtime::Error::msg(e.message()))
                })?;
            }
        }
        Ok(bytes)
    }
}

#[async_trait::async_trait]
impl Pollable for TcpReader {
    async fn ready(&mut self) {
        if let Some(obs) = &self.observer {
            loop {
                if obs.check_tcp_state(self.conn_id).is_err() {
                    return;
                }
                if tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    std::future::poll_fn(|cx| self.state.lock().unwrap().poll_ready(cx)),
                )
                .await
                .is_ok()
                {
                    return;
                }
            }
        } else {
            std::future::poll_fn(|cx| self.state.lock().unwrap().poll_ready(cx)).await
        }
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

    fn write(&mut self, bytes: bytes::Bytes, lease: Arc<TraceLease>) -> StreamResult<()> {
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
            if let Err(error) = crate::sockets::drain_tcp_write(
                &mut stream,
                bytes,
                lease.observer.as_deref(),
                lease.conn_id,
            )
            .await
            {
                if let Some(obs) = &lease.observer {
                    obs.on_error(lease.conn_id, "native-write-error");
                }
                return Err(
                    if matches!(error, crate::sockets::ErrorCode::ConnectionBroken) {
                        WriteError::Closed
                    } else {
                        WriteError::LastOperationFailed(error.into())
                    },
                );
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

    fn abort(&mut self) {
        // Dropping MaybeReady aborts its owned background task and releases its lease.
        *self = WriteState::Closed(WriteError::Closed);
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
    guest_permit: Arc<Mutex<usize>>,
    state: Arc<Mutex<WriteState>>,
    observer: Option<Arc<dyn SocketBoundaryObserver>>,
    conn_id: u64,
    lease: Arc<TraceLease>,
}
impl TcpWriter {
    fn new(
        stream: TcpSendStream,
        observer: Option<Arc<dyn SocketBoundaryObserver>>,
        conn_id: u64,
        lease: Arc<TraceLease>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(WriteState::Ready(stream, 0))),
            guest_permit: Arc::new(Mutex::new(0)),
            observer,
            conn_id,
            lease,
        }
    }
}

#[async_trait::async_trait]
impl OutputStream for TcpWriter {
    fn write(&mut self, bytes: bytes::Bytes) -> StreamResult<()> {
        // Check the original guest write, before a transformer can buffer it or change its size.
        {
            let state = self.state.lock().unwrap();
            let mut permit = self.guest_permit.lock().unwrap();
            match &*state {
                WriteState::Closed(e) => return Err(e.clone().into()),
                WriteState::Ready(_, _) if bytes.len() <= *permit => {
                    if !bytes.is_empty() {
                        *permit = 0;
                    }
                }
                _ => {
                    return Err(StreamError::trap(
                        "guest write exceeds the last check-write permit",
                    ));
                }
            }
        }
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
        self.state.lock().unwrap().write(bytes, self.lease.clone())
    }

    fn flush(&mut self) -> StreamResult<()> {
        if let Some(obs) = &self.observer {
            let pending = obs
                .take_flush_tcp_write(self.conn_id)
                .map_err(|e| StreamError::LastOperationFailed(wasmtime::Error::msg(e.message())))?;
            if !pending.is_empty() {
                self.state
                    .lock()
                    .unwrap()
                    .write(pending, self.lease.clone())?;
            }
        }
        self.state.lock().unwrap().flush()
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        let native = self.state.lock().unwrap().check_write()?;
        let permit = if let Some(obs) = &self.observer {
            obs.check_tcp_write(self.conn_id, native)
                .map_err(|e| StreamError::LastOperationFailed(wasmtime::Error::msg(e.message())))?
                .min(native)
        } else {
            native
        };
        *self.guest_permit.lock().unwrap() = permit;
        Ok(permit)
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
        if self.observer.is_some() {
            if tokio::time::timeout(std::time::Duration::from_secs(2), self.ready())
                .await
                .is_err()
            {
                self.state.lock().unwrap().abort();
                if let Some(obs) = &self.observer {
                    obs.on_error(self.conn_id, "cancel-timeout");
                }
            }
            if let Some(obs) = &self.observer {
                obs.on_write_closed(self.conn_id);
            }
        } else {
            self.ready().await
        }
    }
}

#[async_trait::async_trait]
impl Pollable for TcpWriter {
    async fn ready(&mut self) {
        if let Some(observer) = &self.observer {
            loop {
                if observer.check_tcp_state(self.conn_id).is_err() {
                    self.state.lock().unwrap().abort();
                    return;
                }
                if tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    poll_fn(|cx| self.state.lock().unwrap().poll_ready(cx).map(|_| ())),
                )
                .await
                .is_ok()
                {
                    return;
                }
            }
        } else {
            poll_fn(|cx| self.state.lock().unwrap().poll_ready(cx).map(|_| ())).await;
        }
    }
}
