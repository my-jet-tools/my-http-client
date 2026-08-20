use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use rust_extensions::TaskCompletion;

use tokio::{
    io::{AsyncWriteExt, WriteHalf},
    sync::Mutex,
};

use crate::{MyHttpClientDisconnect, MyHttpClientError};

use super::{
    write_loop::WriteLoopEvent, HttpAwaiterTask, HttpAwaitingTask, MyHttpClientConnectionContext,
    MyHttpRequest, QueueOfRequests, WebSocketContextModel,
};

/// Terminating chunk of a chunked body: zero size, no trailers
const LAST_CHUNK: &[u8] = b"0\r\n\r\n";

/// The socket is written under the state lock, so every write is bounded by the
/// send-to-socket timeout - a stuck peer must not pin the connection forever
async fn write_to_socket<TStream: tokio::io::AsyncWrite + Send + Sync + 'static>(
    write_stream: &mut WriteHalf<TStream>,
    payload: &[u8],
    send_to_socket_timeout: Duration,
) -> Result<(), MyHttpClientError> {
    for chunk in payload.chunks(1024 * 1024) {
        let result = tokio::time::timeout(send_to_socket_timeout, write_stream.write_all(chunk))
            .await
            .map_err(|_| {
                MyHttpClientError::CanNotExecuteRequest(format!(
                    "Timeout {:?} writing the payload to the socket",
                    send_to_socket_timeout
                ))
            })?;

        result.map_err(|err| {
            MyHttpClientError::CanNotExecuteRequest(format!(
                "Can not write the payload to the socket: {}",
                err
            ))
        })?;
    }

    Ok(())
}

/// `<size in hex>\r\n<data>\r\n` - written as one chained buffer, so the payload itself
/// is not copied
async fn write_chunk_to_socket<TStream: tokio::io::AsyncWrite + Send + Sync + 'static>(
    write_stream: &mut WriteHalf<TStream>,
    chunk: &[u8],
    send_to_socket_timeout: Duration,
) -> Result<(), MyHttpClientError> {
    let chunk_size = format!("{:x}\r\n", chunk.len());

    let mut payload = bytes::Buf::chain(
        bytes::Buf::chain(chunk_size.as_bytes(), chunk),
        crate::CL_CR,
    );

    let result = tokio::time::timeout(
        send_to_socket_timeout,
        write_stream.write_all_buf(&mut payload),
    )
    .await
    .map_err(|_| {
        MyHttpClientError::CanNotExecuteRequest(format!(
            "Timeout {:?} writing the chunk to the socket",
            send_to_socket_timeout
        ))
    })?;

    result.map_err(|err| {
        MyHttpClientError::CanNotExecuteRequest(format!(
            "Can not write the chunk to the socket: {}",
            err
        ))
    })
}

pub enum WritePartState<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
> {
    Connected(MyHttpClientConnectionContext<TStream>),
    UpgradedToWebSocket(WebSocketContextModel),
    Disconnected,
    Disposed,
}

impl<TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static>
    WritePartState<TStream>
{
    pub fn get_payload_to_send(&mut self) -> Option<(&mut WriteHalf<TStream>, Vec<u8>, Duration)> {
        match self {
            WritePartState::Connected(inner) => {
                // The wire belongs to the streamed body until its terminating chunk;
                // whatever is queued goes out right after it
                if inner.streaming_request {
                    return None;
                }
                let payload = inner.queue_to_deliver.take()?;
                let write_stream = inner.write_stream.as_mut().unwrap();
                Some((write_stream, payload, inner.send_to_socket_timeout))
            }
            WritePartState::UpgradedToWebSocket(_) => None,
            WritePartState::Disconnected => None,
            WritePartState::Disposed => None,
        }
    }
    pub fn is_disposed(&self) -> bool {
        matches!(self, WritePartState::Disposed)
    }
}

impl<TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static>
    WritePartState<TStream>
{
    pub fn unwrap_as_connected_mut(
        &mut self,
    ) -> Result<&mut MyHttpClientConnectionContext<TStream>, MyHttpClientError> {
        match self {
            WritePartState::Connected(inner) => Ok(inner),
            WritePartState::UpgradedToWebSocket(_) => Err(MyHttpClientError::UpgradedToWebSocket),

            WritePartState::Disconnected => Err(MyHttpClientError::Disconnected),
            WritePartState::Disposed => Err(MyHttpClientError::Disposed),
        }
    }
}

pub struct MyHttpClientInner<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
> {
    pub state: Mutex<(
        WritePartState<TStream>,
        Option<tokio::sync::mpsc::Sender<WriteLoopEvent>>,
    )>,

    connection_id: AtomicU64,
    waiting_ws_upgrade: AtomicBool,
    pub queue_of_requests: QueueOfRequests<TStream>,

    pub metrics: Option<Arc<dyn super::MyHttpClientMetrics + Send + Sync + 'static>>,
    pub name: Arc<String>,
}

impl<TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static>
    MyHttpClientInner<TStream>
{
    pub fn new(
        name: String,
        metrics: Option<Arc<dyn super::MyHttpClientMetrics + Send + Sync + 'static>>,
    ) -> Self {
        let result = Self {
            state: Mutex::new((WritePartState::Disconnected, None)),
            connection_id: AtomicU64::new(0),
            waiting_ws_upgrade: AtomicBool::new(false),
            queue_of_requests: QueueOfRequests::new(),

            metrics,
            name: Arc::new(name),
        };

        if let Some(metrics) = &result.metrics {
            metrics.instance_created(&result.name);
        }
        result
    }

    pub async fn set_sender(&self, sender: tokio::sync::mpsc::Sender<WriteLoopEvent>) {
        let mut state = self.state.lock().await;
        state.1 = Some(sender);
    }

    pub async fn new_connection(
        &self,
        connection_id: u64,
        write_stream: WriteHalf<TStream>,
        send_to_socket_timeout: std::time::Duration,
    ) {
        let mut state = self.state.lock().await;

        if state.0.is_disposed() {
            panic!("Disposed");
        }

        self.process_disconnect(&mut state.0, WritePartState::Disconnected)
            .await;

        state.0 = WritePartState::Connected(MyHttpClientConnectionContext {
            write_stream: Some(write_stream),
            queue_to_deliver: None,
            send_to_socket_timeout,
            streaming_request: false,
        });

        self.waiting_ws_upgrade.store(false, Ordering::Relaxed);
        self.connection_id.store(connection_id, Ordering::Release);

        if let Some(metrics) = self.metrics.as_ref() {
            metrics.tcp_connect(&self.name);
        }
    }

    pub fn is_my_connection_id(&self, connection_id: u64) -> bool {
        self.connection_id.load(Ordering::Acquire) == connection_id
    }

    pub async fn send(
        &self,
        req: &MyHttpRequest,
    ) -> Result<(HttpAwaiterTask<TStream>, u64), MyHttpClientError> {
        let mut writer = self.state.lock().await;

        let (awaiter, connection_id) = {
            let connection_context = writer.0.unwrap_as_connected_mut()?;
            let mut task = TaskCompletion::new();
            let awaiter = task.get_awaiter();

            self.queue_of_requests.push(req.get_method(), task);

            match connection_context.queue_to_deliver.as_mut() {
                Some(vec) => {
                    req.write_to(vec);
                }
                None => {
                    let mut vec = Vec::new();
                    req.write_to(&mut vec);
                    connection_context.queue_to_deliver = Some(vec);
                }
            }

            (awaiter, self.connection_id.load(Ordering::Relaxed))
        };

        let _ = writer
            .1
            .as_ref()
            .unwrap()
            .send(WriteLoopEvent::Flush(connection_id))
            .await;

        Ok((awaiter, connection_id))
    }

    /// Writes the head of a streamed request and keeps the wire for its body. Everything
    /// which is buffered belongs to the requests queued before this one, so it goes to
    /// the socket first - the wire order stays the same as the order of the queue.
    ///
    /// From here until [`Self::finish_streamed_request`] the connection writes nothing
    /// but the chunks of this body
    ///
    /// `content_size` picks the framing of the payload: `Some` writes a content-length
    /// and the chunks go to the socket as they are, `None` writes
    /// `transfer-encoding: chunked` and every chunk is framed
    pub async fn start_streamed_request(
        &self,
        req: &MyHttpRequest,
        content_size: Option<usize>,
    ) -> Result<(HttpAwaiterTask<TStream>, u64), MyHttpClientError> {
        let mut state = self.state.lock().await;

        let connection_id = self.connection_id.load(Ordering::Relaxed);

        let (awaiter, write_result) = {
            let context = state.0.unwrap_as_connected_mut()?;

            if context.streaming_request {
                return Err(MyHttpClientError::CanNotExecuteRequest(
                    "Another streamed request is in progress on this connection".to_string(),
                ));
            }

            let mut task = TaskCompletion::new();
            let awaiter = task.get_awaiter();

            self.queue_of_requests.push(req.get_method(), task);

            let mut payload = context.queue_to_deliver.take().unwrap_or_default();
            req.write_streamed_head_to(&mut payload, content_size);

            let send_to_socket_timeout = context.send_to_socket_timeout;
            let write_stream = context.write_stream.as_mut().unwrap();

            let write_result =
                write_to_socket(write_stream, &payload, send_to_socket_timeout).await;

            if write_result.is_ok() {
                context.streaming_request = true;
            }

            (awaiter, write_result)
        };

        if let Err(err) = write_result {
            // The queued tasks - this one included - are failed by process_disconnect
            self.connection_id.store(0, Ordering::Release);
            self.process_disconnect(&mut state.0, WritePartState::Disconnected)
                .await;
            return Err(err);
        }

        Ok((awaiter, connection_id))
    }

    /// Writes the next piece of a streamed body: framed as a chunk when the size of the
    /// payload was not known upfront, as it is when the request carries a content-length.
    ///
    /// An empty piece is skipped: a zero sized chunk is what terminates a chunked body
    pub async fn publish_chunk(
        &self,
        connection_id: u64,
        chunk: &[u8],
        content_size: Option<usize>,
    ) -> Result<(), MyHttpClientError> {
        if chunk.is_empty() {
            return Ok(());
        }

        let mut state = self.state.lock().await;

        if self.connection_id.load(Ordering::Relaxed) != connection_id {
            return Err(MyHttpClientError::Disconnected);
        }

        let write_result = {
            let context = state.0.unwrap_as_connected_mut()?;

            if !context.streaming_request {
                return Err(MyHttpClientError::Disconnected);
            }

            let send_to_socket_timeout = context.send_to_socket_timeout;
            let write_stream = context.write_stream.as_mut().unwrap();

            match content_size {
                Some(_) => write_to_socket(write_stream, chunk, send_to_socket_timeout).await,
                None => write_chunk_to_socket(write_stream, chunk, send_to_socket_timeout).await,
            }
        };

        if let Err(err) = write_result {
            self.connection_id.store(0, Ordering::Release);
            self.process_disconnect(&mut state.0, WritePartState::Disconnected)
                .await;
            return Err(err);
        }

        Ok(())
    }

    /// Closes a streamed body and releases the wire. A chunked body needs its terminating
    /// chunk; a body with a content-length is over as soon as the announced amount of
    /// bytes has been written
    pub async fn finish_streamed_request(
        &self,
        connection_id: u64,
        content_size: Option<usize>,
    ) -> Result<(), MyHttpClientError> {
        let sender = {
            let mut state = self.state.lock().await;

            if self.connection_id.load(Ordering::Relaxed) != connection_id {
                return Err(MyHttpClientError::Disconnected);
            }

            let write_result = {
                let context = state.0.unwrap_as_connected_mut()?;

                if !context.streaming_request {
                    return Err(MyHttpClientError::Disconnected);
                }

                let send_to_socket_timeout = context.send_to_socket_timeout;
                let write_stream = context.write_stream.as_mut().unwrap();

                let result = match content_size {
                    Some(_) => Ok(()),
                    None => write_to_socket(write_stream, LAST_CHUNK, send_to_socket_timeout).await,
                };

                context.streaming_request = false;

                result
            };

            if let Err(err) = write_result {
                self.connection_id.store(0, Ordering::Release);
                self.process_disconnect(&mut state.0, WritePartState::Disconnected)
                    .await;
                return Err(err);
            }

            state.1.clone()
        };

        // The requests which were queued while the body was streaming can go out now.
        // The state lock is released before that: the write loop takes it to flush
        if let Some(sender) = sender {
            let _ = sender.send(WriteLoopEvent::Flush(connection_id)).await;
        }

        Ok(())
    }

    /// A half written body poisons the connection - the upstream is still waiting for
    /// the rest of the chunks and nothing else can be sent through it
    pub async fn abort_streamed_request(&self, connection_id: u64) {
        self.disconnect(connection_id).await;
    }

    pub async fn upgrade_to_websocket(
        &self,
        connection_id: u64,
    ) -> Result<WriteHalf<TStream>, MyHttpClientError> {
        let mut state = self.state.lock().await;

        match &mut state.0 {
            WritePartState::Connected(context) => {
                if self.connection_id.load(Ordering::Relaxed) != connection_id {
                    return Err(MyHttpClientError::Disconnected);
                }

                let result = context.write_stream.take();

                state.0 = WritePartState::UpgradedToWebSocket(WebSocketContextModel::new(
                    self.name.clone(),
                ));

                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.upgraded_to_websocket(&self.name);
                }
                Ok(result.unwrap())
            }
            WritePartState::UpgradedToWebSocket(_) => Err(MyHttpClientError::UpgradedToWebSocket),
            WritePartState::Disconnected => Err(MyHttpClientError::Disconnected),
            WritePartState::Disposed => Err(MyHttpClientError::Disposed),
        }
    }

    /// Peeks the method of the request whose response is currently being read
    /// (the front of the FIFO queue) without popping it. The read loop needs
    /// the method to apply RFC 9112 §6.3 body framing before the body is read.
    pub fn peek_request_method(&self, connection_id: u64) -> Option<http::Method> {
        if self.connection_id.load(Ordering::Acquire) != connection_id {
            return None;
        }

        self.queue_of_requests.peek_front_method()
    }

    pub fn pop_request(
        &self,
        connection_id: u64,
        web_socket_upgrade: bool,
    ) -> Option<HttpAwaitingTask<TStream>> {
        if self.connection_id.load(Ordering::Acquire) != connection_id {
            return None;
        }

        if web_socket_upgrade {
            self.waiting_ws_upgrade.store(true, Ordering::Relaxed);
        }

        self.queue_of_requests.pop()
    }

    pub async fn flush(&self, connection_id: u64) {
        let mut state = self.state.lock().await;

        if self.connection_id.load(Ordering::Relaxed) != connection_id {
            return;
        }

        let mut has_error = false;
        if let Some((stream, payload, send_to_socket_timeout)) = state.0.get_payload_to_send() {
            for chunk in payload.chunks(1024 * 1024) {
                let future = stream.write_all(chunk);

                let result = tokio::time::timeout(send_to_socket_timeout, future).await;

                if result.is_err() {
                    has_error = true;
                    break;
                }

                let result = result.unwrap();

                if result.is_err() {
                    has_error = true;
                    break;
                }
            }
        }

        if has_error {
            self.connection_id.store(0, Ordering::Release);
            self.process_disconnect(&mut state.0, WritePartState::Disconnected)
                .await;
        }
    }

    pub async fn disconnect(&self, connection_id: u64) {
        let mut state = self.state.lock().await;

        if self.connection_id.load(Ordering::Relaxed) != connection_id {
            return;
        }

        if matches!(
            state.0,
            WritePartState::Disconnected | WritePartState::Disposed
        ) {
            return;
        }

        self.connection_id.store(0, Ordering::Release);
        self.process_disconnect(&mut state.0, WritePartState::Disconnected)
            .await;
    }

    async fn process_disconnect(
        &self,
        state: &mut WritePartState<TStream>,
        new_status: WritePartState<TStream>,
    ) {
        match &mut *state {
            WritePartState::Connected(context) => {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.tcp_disconnect(&self.name);
                }
                if let Some(mut write_stream) = context.write_stream.take() {
                    let _ = write_stream.shutdown().await;
                }
                self.queue_of_requests.notify_connection_lost();
            }
            WritePartState::UpgradedToWebSocket(_) => {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.tcp_disconnect(&self.name);
                }
            }
            _ => {}
        }

        *state = new_status;
    }

    pub async fn read_loop_stopped(&self, connection_id: u64) {
        if self.waiting_ws_upgrade.load(Ordering::Relaxed) {
            return;
        }

        let mut state = self.state.lock().await;

        if self.connection_id.load(Ordering::Relaxed) != connection_id {
            return;
        }

        if !matches!(state.0, WritePartState::Connected(_)) {
            return;
        }

        self.connection_id.store(0, Ordering::Release);
        self.process_disconnect(&mut state.0, WritePartState::Disconnected)
            .await;
    }

    pub async fn dispose(&self) {
        let mut state = self.state.lock().await;
        self.connection_id.store(0, Ordering::Release);
        self.process_disconnect(&mut state.0, WritePartState::Disposed)
            .await;

        if let Some(sender) = state.1.as_ref() {
            let _ = sender.send(WriteLoopEvent::Close).await;
        }
    }
}

impl<TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static> Drop
    for MyHttpClientInner<TStream>
{
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.instance_disposed(&self.name);
        }
    }
}

pub struct MyHttpClientDisconnection<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
> {
    inner: Arc<MyHttpClientInner<TStream>>,
    connection_id: u64,
}

impl<TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static>
    MyHttpClientDisconnection<TStream>
{
    pub fn new(inner: Arc<MyHttpClientInner<TStream>>, connection_id: u64) -> Self {
        Self {
            inner,
            connection_id,
        }
    }
}

impl<TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static>
    MyHttpClientDisconnect for MyHttpClientDisconnection<TStream>
{
    fn disconnect(&self) {
        let inner = self.inner.clone();
        let connection_id = self.connection_id;

        tokio::spawn(async move {
            inner.disconnect(connection_id).await;
        });
    }

    fn web_socket_disconnect(&self) {
        if let Some(metrics) = self.inner.metrics.as_ref() {
            metrics.websocket_is_disconnected(&self.inner.name);
        }

        let inner = self.inner.clone();
        let connection_id = self.connection_id;

        tokio::spawn(async move {
            inner.disconnect(connection_id).await;
        });
    }

    fn get_connection_id(&self) -> u64 {
        self.connection_id
    }
}
