use std::{
    future::poll_fn,
    pin::Pin,
    task::{ready, Context, Poll},
};

use hyper::body::{Body, Frame, Incoming, SizeHint};
use tokio::sync::Mutex;

use super::{BodyPiece, RequestDeadline};

/// How the body of a response is read when hyper reads the connection - the HTTP/1.1
/// client on top of hyper and the HTTP/2 one. It is a wrapper of hyper's `Incoming`:
/// the body is taken from hyper frame by frame, with nothing in between.
///
/// hyper gives the data of the body: the chunks of a chunked body are taken apart by
/// the time it gets here, so a piece is the data of a frame whatever the body was
/// framed with.
///
/// It is hyper which holds the body back while it is not read, the way the protocol
/// does it: an HTTP/1.1 connection is busy with a body until it is read or dropped, an
/// HTTP/2 stream has a window of its own and does not hold the others
pub struct HyperBodyReaderInner {
    /// What changes as the body is read. It is behind a lock, so that the body can be
    /// read through a shared reference as well - by one reader at a time
    reading: Mutex<State>,
    content_length: Option<usize>,
    pub(crate) request_deadline: Option<RequestDeadline>,
    /// The piece [`super::BodyReader::next_item`] has given last
    pub(crate) current: Option<BodyPiece>,
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
            reading: Mutex::new(State::Reading(body)),
            content_length,
            request_deadline: None,
            current: None,
        }
    }

    pub fn content_length(&self) -> Option<usize> {
        self.content_length
    }

    /// How much of the body is not read yet. `None` is a body which does not say it -
    /// and a body which is being read through a shared reference right now
    pub fn remains_to_read(&self) -> Option<usize> {
        match &*self.reading.try_lock().ok()? {
            State::Reading(body) => exact_size(body),
            State::Over => Some(0),
            State::Failed(_) => None,
        }
    }

    /// The next frame for a reader which owns the body: nobody else can be reading it
    pub(crate) fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<BodyPiece>, String>>> {
        self.reading.get_mut().poll_frame(cx)
    }

    /// The next frame for a reader which shares the body. The readers take turns: the
    /// one which is waiting for a frame holds the others back until it has got it
    pub(crate) async fn next_frame(&self) -> Option<Result<Frame<BodyPiece>, String>> {
        let mut reading = self.reading.lock().await;
        poll_fn(|cx| reading.poll_frame(cx)).await
    }

    pub(crate) fn is_end_stream(&self) -> bool {
        match self.reading.try_lock().as_deref() {
            Ok(State::Reading(body)) => body.is_end_stream(),
            Ok(State::Over) => true,
            Ok(State::Failed(_)) | Err(_) => false,
        }
    }

    pub(crate) fn size_hint(&self) -> SizeHint {
        match self.reading.try_lock().as_deref() {
            Ok(State::Reading(body)) => body.size_hint(),
            Ok(State::Over) => SizeHint::with_exact(0),
            Ok(State::Failed(_)) | Err(_) => SizeHint::default(),
        }
    }
}

impl State {
    fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<BodyPiece>, String>>> {
        let body = match self {
            State::Reading(body) => body,
            State::Over => return Poll::Ready(None),
            State::Failed(reason) => return Poll::Ready(Some(Err(reason.clone()))),
        };

        let result = match ready!(Pin::new(body).poll_frame(cx)) {
            Some(Ok(frame)) => Some(Ok(frame.map_data(BodyPiece::hyper))),
            Some(Err(err)) => {
                let reason = format!("The response body is not complete: {}", err);
                *self = State::Failed(reason.clone());
                Some(Err(reason))
            }
            None => {
                *self = State::Over;
                None
            }
        };

        Poll::Ready(result)
    }
}

fn exact_size(body: &Incoming) -> Option<usize> {
    usize::try_from(body.size_hint().exact()?).ok()
}
