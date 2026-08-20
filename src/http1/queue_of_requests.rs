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

pub enum HttpTask<TStream: tokio::io::AsyncRead + Send + Sync + 'static> {
    Response(hyper::Response<BoxBody<Bytes, String>>),
    WebsocketUpgrade {
        response: hyper::Response<BoxBody<Bytes, String>>,
        read_part: ReadHalf<TStream>,
    },
}

impl<TStream: tokio::io::AsyncRead + Send + Sync + 'static> HttpTask<TStream> {
    pub fn unwrap_response(self) -> hyper::Response<BoxBody<Bytes, String>> {
        match self {
            HttpTask::Response(response) => response,
            HttpTask::WebsocketUpgrade { response, .. } => response,
        }
    }

    pub fn unwrap_websocket_upgrade(
        self,
    ) -> (hyper::Response<BoxBody<Bytes, String>>, ReadHalf<TStream>) {
        match self {
            HttpTask::WebsocketUpgrade {
                response,
                read_part,
            } => (response, read_part),
            HttpTask::Response(_) => panic!("Can not unwrap as websocket upgrade"),
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

    pub fn push(&self, method: Method, task: HttpAwaitingTask<TStream>) {
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
