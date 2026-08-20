use std::sync::Arc;

use tokio::io::WriteHalf;

pub struct MyHttpClientConnectionContext<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
> {
    pub write_stream: Option<WriteHalf<TStream>>,
    pub queue_to_deliver: Option<Vec<u8>>,
    pub send_to_socket_timeout: std::time::Duration,
    /// A streamed request owns the wire from its head to the terminating chunk: while it
    /// is `true` nothing else may be written, or the bytes of another request would cut
    /// into the middle of the body. The requests queued meanwhile stay in
    /// `queue_to_deliver` and go out as soon as the body is over
    pub streaming_request: bool,
}

pub struct WebSocketContextModel {
    pub name: Arc<String>,
}

impl WebSocketContextModel {
    pub fn new(name: Arc<String>) -> Self {
        Self { name }
    }
}
