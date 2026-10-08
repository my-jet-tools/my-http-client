use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

use bytes::Bytes;
use hyper::body::{Body, Frame, Incoming, SizeHint};

use super::RequestDeadline;

/// How the body of a response is read when hyper reads the connection - the HTTP/1.1
/// client on top of hyper and the HTTP/2 one. It is a wrapper of hyper's `Incoming`:
/// the body is taken from hyper frame by frame, with nothing in between.
///
/// It is hyper which holds the body back while it is not read, the way the protocol
/// does it: an HTTP/1.1 connection is busy with a body until it is read or dropped, an
/// HTTP/2 stream has a window of its own and does not hold the others
pub struct HyperBodyReaderInner {
    state: State,
    content_length: Option<usize>,
    pub(crate) request_deadline: Option<RequestDeadline>,
}

enum State {
    Reading(Incoming),
    /// The body is read to its end
    Over,
    /// The body is over before its end, and this is why
    Failed(String),
}

impl HyperBodyReaderInner {
    pub fn new(body: Incoming) -> Self {
        // hyper knows the size of a body when its response says it
        let content_length = exact_size(&body);

        Self {
            state: State::Reading(body),
            content_length,
            request_deadline: None,
        }
    }

    pub fn content_length(&self) -> Option<usize> {
        self.content_length
    }

    /// How much of the body is not read yet. `None` is a body which does not say it
    pub fn remains_to_read(&self) -> Option<usize> {
        match &self.state {
            State::Reading(body) => exact_size(body),
            State::Over => Some(0),
            State::Failed(_) => None,
        }
    }

    pub(crate) fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, String>>> {
        let body = match &mut self.state {
            State::Reading(body) => body,
            State::Over => return Poll::Ready(None),
            State::Failed(reason) => return Poll::Ready(Some(Err(reason.clone()))),
        };

        let result = match ready!(Pin::new(body).poll_frame(cx)) {
            Some(Ok(frame)) => Some(Ok(frame)),
            Some(Err(err)) => {
                let reason = format!("The response body is not complete: {}", err);
                self.state = State::Failed(reason.clone());
                Some(Err(reason))
            }
            None => {
                self.state = State::Over;
                None
            }
        };

        Poll::Ready(result)
    }

    pub(crate) fn is_end_stream(&self) -> bool {
        match &self.state {
            State::Reading(body) => body.is_end_stream(),
            State::Over => true,
            State::Failed(_) => false,
        }
    }

    pub(crate) fn size_hint(&self) -> SizeHint {
        match &self.state {
            State::Reading(body) => body.size_hint(),
            State::Over => SizeHint::with_exact(0),
            State::Failed(_) => SizeHint::default(),
        }
    }
}

fn exact_size(body: &Incoming) -> Option<usize> {
    usize::try_from(body.size_hint().exact()?).ok()
}
