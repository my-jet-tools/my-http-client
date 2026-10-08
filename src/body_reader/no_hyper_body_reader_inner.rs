use std::task::{ready, Context, Poll};

use bytes::Bytes;
use hyper::body::{Frame, SizeHint};
use tokio::sync::mpsc;

use super::{BodyEvent, BodySender, RequestDeadline, RESPONSE_BODY_CHANNEL_CAPACITY};

/// How the body of a response of the non-hyper client is read: it is received from the
/// read loop of the client, which writes it to a [`BodySender`] as it comes off the
/// socket - with the sizes of the chunks taken out when the body is chunked.
///
/// The channel between them is what gives the backpressure: once
/// [`RESPONSE_BODY_CHANNEL_CAPACITY`] pieces wait to be received the socket is not read
/// any more, so a body nobody reads is held by the upstream and not by this process.
/// **So the connection is busy with the body until it is read**, and the responses to
/// the requests pipelined behind it wait for that. A reader which is dropped before the
/// end of the body lets the connection go: what is left of the body is read past when
/// there is little of it ([`crate::http1::MAX_ABANDONED_BODY_SIZE`]) and it comes at
/// once ([`crate::http1::ABANDONED_BODY_SKIP_TIMEOUT`]), and the connection is closed
/// otherwise
pub struct NoHyperBodyReaderInner {
    state: State,
    content_length: Option<usize>,
    /// How much of the body is received already
    received: usize,
    pub(crate) request_deadline: Option<RequestDeadline>,
}

enum State {
    Reading(mpsc::Receiver<BodyEvent>),
    /// The body is read to its end
    Over,
    /// The body is over before its end, and this is why
    Failed(String),
}

impl NoHyperBodyReaderInner {
    /// A body and the sender it is written through. `content_length` is the size of
    /// the body when the response says it; `None` is a body which does not - it is
    /// chunked, or it lasts until the connection is closed
    pub fn new(content_length: Option<usize>) -> (BodySender, Self) {
        let (sender, receiver) = mpsc::channel(RESPONSE_BODY_CHANNEL_CAPACITY);

        let result = Self {
            state: State::Reading(receiver),
            content_length,
            received: 0,
            request_deadline: None,
        };

        (BodySender::new(sender), result)
    }

    /// The body of a response which has none
    pub fn empty() -> Self {
        Self {
            state: State::Over,
            content_length: Some(0),
            received: 0,
            request_deadline: None,
        }
    }

    pub fn content_length(&self) -> Option<usize> {
        self.content_length
    }

    /// How much of the body is not received yet. `None` is a body which does not say it
    pub fn remains_to_read(&self) -> Option<usize> {
        match &self.state {
            State::Reading(_) => Some(self.content_length?.saturating_sub(self.received)),
            State::Over => Some(0),
            State::Failed(_) => None,
        }
    }

    pub(crate) fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, String>>> {
        let receiver = match &mut self.state {
            State::Reading(receiver) => receiver,
            State::Over => return Poll::Ready(None),
            State::Failed(reason) => return Poll::Ready(Some(Err(reason.clone()))),
        };

        let result = match ready!(receiver.poll_recv(cx)) {
            Some(BodyEvent::Data(data)) => {
                self.received += data.len();
                Some(Ok(Frame::data(data)))
            }
            Some(BodyEvent::Completed) => {
                self.state = State::Over;
                None
            }
            Some(BodyEvent::Failed(reason)) => Some(Err(self.fail(reason))),
            // Whoever was writing the body is gone without a word about its end
            None => Some(Err(self.fail(
                "The response body is not complete: the connection is not read any more"
                    .to_string(),
            ))),
        };

        Poll::Ready(result)
    }

    pub(crate) fn is_end_stream(&self) -> bool {
        matches!(self.state, State::Over)
    }

    pub(crate) fn size_hint(&self) -> SizeHint {
        match self.remains_to_read() {
            Some(remains) => SizeHint::with_exact(remains as u64),
            None => SizeHint::default(),
        }
    }

    /// Nobody reads the body past a failure, and dropping the receiving side is what
    /// tells that to whoever writes it
    fn fail(&mut self, reason: String) -> String {
        self.state = State::Failed(reason.clone());
        reason
    }
}
