use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use hyper::body::{Body, Frame};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::MyHttpClientError;

/// The client closes the channel the moment the request is over, and the reason is
/// written down a tick later - this is how long [`RequestBodyPublisher::publish`] waits
/// for it before reporting the plain "the request is over"
const FAILURE_REASON_GRACE: Duration = Duration::from_millis(200);

/// Why the payload can not be published anymore
#[derive(Debug, Clone)]
pub enum PublishPayloadError {
    /// The request has failed. The typed error belongs to whoever drives the request -
    /// this is its rendering, with the retry decision already made out of it
    RequestFailed { reason: String, is_retryable: bool },
    /// The request is over and the reason is not recorded here - the call which drives
    /// the request returns it
    RequestIsOver,
}

impl PublishPayloadError {
    /// Whether it makes sense to rebuild the payload and to send the request again
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::RequestFailed { is_retryable, .. } => *is_retryable,
            Self::RequestIsOver => false,
        }
    }
}

/// Reason of the failure shared between the request and the publishing side
#[derive(Clone)]
pub struct RequestFailureReason {
    inner: Arc<RequestFailureReasonInner>,
}

struct RequestFailureReasonInner {
    reason: parking_lot::Mutex<Option<PublishPayloadError>>,
    notify: tokio::sync::Notify,
}

impl RequestFailureReason {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RequestFailureReasonInner {
                reason: parking_lot::Mutex::new(None),
                notify: tokio::sync::Notify::new(),
            }),
        }
    }

    pub fn set(&self, err: &MyHttpClientError) {
        let mut reason = self.inner.reason.lock();
        *reason = Some(PublishPayloadError::RequestFailed {
            reason: format!("{:?}", err),
            is_retryable: err.is_retryable(),
        });
        // The publisher is normally already waiting: the channel gets closed before the
        // request call has a chance to write the reason down
        self.inner.notify.notify_waiters();
    }

    async fn get_with_grace(&self) -> PublishPayloadError {
        // Subscribed before the reason is read: notify_waiters() only wakes the waiters
        // which are already registered, so a reason written down right after the check
        // below would otherwise be missed and reported only after the whole grace period
        let notified = self.inner.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        {
            let reason = self.inner.reason.lock();
            if let Some(reason) = reason.as_ref() {
                return reason.clone();
            }
        }

        let _ = tokio::time::timeout(FAILURE_REASON_GRACE, notified).await;

        let reason = self.inner.reason.lock();
        match reason.as_ref() {
            Some(reason) => reason.clone(),
            None => PublishPayloadError::RequestIsOver,
        }
    }
}

impl Default for RequestFailureReason {
    fn default() -> Self {
        Self::new()
    }
}

/// The publishing side of a streamed request body.
///
/// `publish` fails only when the request is over - as long as it returns `Ok` the chunk
/// is on its way. Dropping the publisher ends the body: that is how the client learns
/// the payload is complete, so it must not be dropped before the last chunk is pushed.
pub struct RequestBodyPublisher<TChunk: Into<Bytes> + Send + Sync + 'static> {
    sender: Sender<TChunk>,
    failure: RequestFailureReason,
}

impl<TChunk: Into<Bytes> + Send + Sync + 'static> RequestBodyPublisher<TChunk> {
    /// Pushes the next chunk of the payload. It waits while the buffer is full - that is
    /// the backpressure of a slow upstream.
    ///
    /// An error means the request is over and nothing else can be published; the payload
    /// has to be rebuilt from scratch for the next attempt
    pub async fn publish(&self, chunk: TChunk) -> Result<(), PublishPayloadError> {
        if self.sender.send(chunk).await.is_err() {
            return Err(self.failure.get_with_grace().await);
        }

        Ok(())
    }

    /// `false` means the request is over and [`Self::publish`] is going to fail
    pub fn is_open(&self) -> bool {
        !self.sender.is_closed()
    }
}

/// Request body fed by a `tokio::sync::mpsc` channel: we create the channel, the client
/// gets the consuming side, the caller keeps [`RequestBodyPublisher`] and pushes the
/// chunks into it.
///
/// The channel is what gives the backpressure - the producer waits in `publish` once
/// `buffer` chunks are queued for the socket, so a slow upstream does not turn into an
/// unbounded queue in memory.
///
/// ```no_run
/// # async fn sample<TStream, TConnector>(
/// #     client: my_http_client::http1_hyper::MyHttpHyperClient<TStream, TConnector>,
/// # ) -> Result<(), my_http_client::MyHttpClientError>
/// # where
/// #     TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
/// #     TConnector: my_http_client::MyHttpClientConnector<TStream> + Send + Sync + 'static,
/// # {
/// use my_http_client::RequestBodyStream;
///
/// let (publisher, body) = RequestBodyStream::new(4);
///
/// tokio::spawn(async move {
///     for chunk in 0..10u8 {
///         if let Err(err) = publisher.publish(vec![chunk; 1024]).await {
///             println!("The request is over: {:?}", err);
///             break;
///         }
///     }
///     // dropping the publisher ends the body
/// });
///
/// let request = my_http_client::http::Request::builder()
///     .method(my_http_client::http::Method::POST)
///     .uri("/upload")
///     .body(body)
///     .map_err(|err| my_http_client::MyHttpClientError::CanNotExecuteRequest(err.to_string()))?;
///
/// // None: the size of the payload is not known, so it goes out chunked
/// client
///     .do_streamed_request(request, None, std::time::Duration::from_secs(30))
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct RequestBodyStream<TChunk: Into<Bytes> + Send + Sync + 'static> {
    receiver: Receiver<TChunk>,
    failure: RequestFailureReason,
}

impl<TChunk: Into<Bytes> + Send + Sync + 'static> RequestBodyStream<TChunk> {
    /// `buffer` is how many chunks the producer is allowed to run ahead of the socket
    pub fn new(buffer: usize) -> (RequestBodyPublisher<TChunk>, Self) {
        let (sender, receiver) = tokio::sync::mpsc::channel(buffer);

        let failure = RequestFailureReason::new();

        (
            RequestBodyPublisher {
                sender,
                failure: failure.clone(),
            },
            Self { receiver, failure },
        )
    }

    /// Takes the consuming side of a channel which is already there. Such a body carries
    /// no failure reason back to the producing side - the sender simply gets a closed
    /// channel when the request is over
    pub fn from_receiver(receiver: Receiver<TChunk>) -> Self {
        Self {
            receiver,
            failure: RequestFailureReason::new(),
        }
    }

    pub fn get_failure_reason(&self) -> RequestFailureReason {
        self.failure.clone()
    }
}

impl<TChunk: Into<Bytes> + Send + Sync + 'static> From<Receiver<TChunk>>
    for RequestBodyStream<TChunk>
{
    fn from(receiver: Receiver<TChunk>) -> Self {
        Self::from_receiver(receiver)
    }
}

impl<TChunk: Into<Bytes> + Send + Sync + 'static> Body for RequestBodyStream<TChunk> {
    type Data = Bytes;
    type Error = String;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        // Receiver is Unpin; a closed channel gives None which ends the body
        self.get_mut()
            .receiver
            .poll_recv(cx)
            .map(|chunk| chunk.map(|chunk| Ok(Frame::data(chunk.into()))))
    }

    fn is_end_stream(&self) -> bool {
        false
    }

    // size_hint is left unknown on purpose: the payload is not there yet, so the request
    // goes out chunked
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;

    use super::*;

    #[tokio::test]
    async fn test_chunks_are_delivered_in_order() {
        let (publisher, body) = RequestBodyStream::new(2);

        tokio::spawn(async move {
            publisher.publish(b"123".to_vec()).await.unwrap();
            publisher.publish(b"456".to_vec()).await.unwrap();
        });

        let result = body.collect().await.unwrap().to_bytes();
        assert_eq!(result.as_ref(), b"123456");
    }

    #[tokio::test]
    async fn test_size_is_unknown_so_the_request_goes_out_chunked() {
        let (publisher, body) = RequestBodyStream::<Vec<u8>>::new(2);
        drop(publisher);

        assert_eq!(body.size_hint().exact(), None);
    }

    #[tokio::test]
    async fn test_dropped_publisher_ends_the_body() {
        let (publisher, body) = RequestBodyStream::<Bytes>::new(2);
        drop(publisher);

        let result = body.collect().await.unwrap().to_bytes();
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_publisher_gets_the_reason_of_the_failure() {
        let (publisher, body) = RequestBodyStream::<Vec<u8>>::new(1);

        let failure = body.get_failure_reason();

        // The request has failed: the body is dropped and the reason is written down
        drop(body);
        failure.set(&MyHttpClientError::Disconnected);

        let result = publisher.publish(b"123".to_vec()).await;

        match result {
            Ok(_) => panic!("The publish had to fail"),
            Err(err) => {
                assert!(err.is_retryable());
                match err {
                    PublishPayloadError::RequestFailed { reason, .. } => {
                        assert!(reason.contains("Disconnected"))
                    }
                    PublishPayloadError::RequestIsOver => {
                        panic!("The reason had to be there")
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_publisher_reports_the_end_of_the_request_without_a_reason() {
        let (publisher, body) = RequestBodyStream::<Vec<u8>>::new(1);

        drop(body);

        let result = publisher.publish(b"123".to_vec()).await;

        match result {
            Ok(_) => panic!("The publish had to fail"),
            Err(err) => match err {
                PublishPayloadError::RequestIsOver => {}
                PublishPayloadError::RequestFailed { .. } => {
                    panic!("Nobody has recorded a reason")
                }
            },
        }
    }
}
