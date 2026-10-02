use std::collections::VecDeque;

use bytes::Bytes;
use http::Method;
use http_body_util::combinators::BoxBody;
use parking_lot::Mutex;
use rust_extensions::{TaskCompletion, TaskCompletionAwaiter};
use tokio::io::ReadHalf;

use crate::MyHttpClientError;

pub type HttpAwaitingTask<TStream> = TaskCompletion<HttpTask<TStream>, MyHttpClientError>;

pub type HttpAwaiterTask<TStream> = TaskCompletionAwaiter<HttpTask<TStream>, MyHttpClientError>;

/// What a websocket upgrade is made of: the response, the read half of the socket and
/// the leftover of the upgrade read
pub type WebsocketUpgradeParts<TStream> = (
    hyper::Response<BoxBody<Bytes, String>>,
    ReadHalf<TStream>,
    Vec<u8>,
);

pub enum HttpTask<TStream: tokio::io::AsyncRead + Send + Sync + 'static> {
    Response(hyper::Response<BoxBody<Bytes, String>>),
    WebsocketUpgrade {
        response: hyper::Response<BoxBody<Bytes, String>>,
        read_part: ReadHalf<TStream>,
        /// Bytes which arrived in the same read() as the `101` head but past it -
        /// typically the first websocket frame the server pushed straight after
        /// the handshake. They are already consumed from the socket, so the
        /// websocket side has to replay them before reading `read_part`.
        leftover: Vec<u8>,
    },
}

impl<TStream: tokio::io::AsyncRead + Send + Sync + 'static> HttpTask<TStream> {
    pub fn unwrap_response(self) -> hyper::Response<BoxBody<Bytes, String>> {
        match self {
            HttpTask::Response(response) => response,
            HttpTask::WebsocketUpgrade { response, .. } => response,
        }
    }

    /// `None` is a task which is not a websocket upgrade.
    ///
    /// The third element is the leftover of the upgrade read - it has to be
    /// consumed before `read_part`, see [`HttpTask::WebsocketUpgrade`]
    pub fn into_websocket_upgrade(self) -> Option<WebsocketUpgradeParts<TStream>> {
        match self {
            HttpTask::WebsocketUpgrade {
                response,
                read_part,
                leftover,
            } => Some((response, read_part, leftover)),
            HttpTask::Response(_) => None,
        }
    }
}

/// A queued request awaiting its response. The `method` is retained so the read
/// loop can apply RFC 9112 §6.3 response-body framing (which depends on the
/// request method) before the request is popped.
struct QueuedRequest<TStream: tokio::io::AsyncRead + Send + Sync + 'static> {
    method: Method,
    task: HttpAwaitingTask<TStream>,
}

pub struct QueueOfRequests<TStream: tokio::io::AsyncRead + Send + Sync + 'static> {
    queue: Mutex<VecDeque<QueuedRequest<TStream>>>,
}

impl<TStream: tokio::io::AsyncRead + Send + Sync + 'static> Default for QueueOfRequests<TStream> {
    fn default() -> Self {
        Self::new()
    }
}

impl<TStream: tokio::io::AsyncRead + Send + Sync + 'static> QueueOfRequests<TStream> {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
        }
    }

    pub fn push(&self, method: Method, mut task: HttpAwaitingTask<TStream>) {
        // A task which is dropped with no result set makes its awaiter panic, unless it
        // is told what to report instead. Nothing here drops one, and if something ever
        // does, the caller gets an error
        task.set_drop_error(MyHttpClientError::CanNotExecuteRequest(
            "The request is dropped with no result".to_string(),
        ));

        self.queue.lock().push_back(QueuedRequest { method, task });
    }

    pub fn pop(&self) -> Option<HttpAwaitingTask<TStream>> {
        self.queue.lock().pop_front().map(|itm| itm.task)
    }

    /// Returns the method of the request at the front of the queue (the one
    /// whose response is being read next) without removing it. Responses are
    /// delivered in request order, so the front entry always matches the
    /// response currently on the wire.
    pub fn peek_front_method(&self) -> Option<Method> {
        self.queue.lock().front().map(|itm| itm.method.clone())
    }

    pub fn notify_connection_lost(&self) {
        let mut queue = self.queue.lock();
        while let Some(mut itm) = queue.pop_front() {
            let _ = itm.task.try_set_error(MyHttpClientError::Disconnected);
        }
    }
}
