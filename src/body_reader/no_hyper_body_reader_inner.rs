use std::{
    future::poll_fn,
    task::{ready, Context, Poll},
};

use hyper::body::{Frame, SizeHint};
use tokio::sync::{mpsc, Mutex};

use super::{BodyEvent, BodyPiece, BodySender, RequestDeadline, RESPONSE_BODY_CHANNEL_CAPACITY};

/// How the body of a response of the non-hyper client is read: it is received from the
/// read loop of the client, which writes it to a [`BodySender`] as it comes off the
/// socket. A piece is the data one read has brought - for a chunked body the data of the
/// chunks, with what framed them cut off.
///
/// A piece is one of the two buffers the socket is read into, and the socket is not read
/// while both of them are held. That is the backpressure: a body nobody reads is held by
/// the upstream and not by this process. A reader which drops the piece it has before it
/// asks for the one after the next is never held back; a reader which keeps the pieces
/// of both buffers and asks for the next one waits for ever: nothing is read until it
/// lets go of a buffer. [`super::BodyReader::next_item`] lets go of the piece it gave
/// before on its own.
/// **So the connection is busy with the body until it is read**, and the responses to
/// the requests pipelined behind it wait for that. A reader which is dropped before the
/// end of the body lets the connection go: what is left of the body is read past when
/// there is little of it ([`crate::http1::MAX_ABANDONED_BODY_SIZE`]) and it comes at
/// once ([`crate::http1::ABANDONED_BODY_SKIP_TIMEOUT`]), and the connection is closed
/// otherwise
pub struct NoHyperBodyReaderInner {
    /// What changes as the body is read. It is behind a lock, so that the body can be
    /// read through a shared reference as well - by one reader at a time
    reading: Mutex<Reading>,
    content_length: Option<usize>,
    pub(crate) request_deadline: Option<RequestDeadline>,
    /// The piece [`super::BodyReader::next_item`] has given last
    pub(crate) current: Option<BodyPiece>,
}

struct Reading {
    state: State,
    /// How much of the data of the body is received already
    received: usize,
}

enum State {
    Receiving(mpsc::Receiver<BodyEvent>),
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
            reading: Mutex::new(Reading {
                state: State::Receiving(receiver),
                received: 0,
            }),
            content_length,
            request_deadline: None,
            current: None,
        };

        (BodySender::new(sender), result)
    }

    /// The body of a response which has none
    pub fn empty() -> Self {
        Self {
            reading: Mutex::new(Reading {
                state: State::Over,
                received: 0,
            }),
            content_length: Some(0),
            request_deadline: None,
            current: None,
        }
    }

    pub fn content_length(&self) -> Option<usize> {
        self.content_length
    }

    /// How much of the body is not received yet. `None` is a body which does not say
    /// it - and a body which is being read through a shared reference right now
    pub fn remains_to_read(&self) -> Option<usize> {
        let reading = self.reading.try_lock().ok()?;

        match &reading.state {
            State::Receiving(_) => Some(self.content_length?.saturating_sub(reading.received)),
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
        self.reading
            .try_lock()
            .is_ok_and(|reading| matches!(reading.state, State::Over))
    }

    pub(crate) fn size_hint(&self) -> SizeHint {
        match self.remains_to_read() {
            Some(remains) => SizeHint::with_exact(remains as u64),
            None => SizeHint::default(),
        }
    }
}

impl Reading {
    fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<BodyPiece>, String>>> {
        let receiver = match &mut self.state {
            State::Receiving(receiver) => receiver,
            State::Over => return Poll::Ready(None),
            State::Failed(reason) => return Poll::Ready(Some(Err(reason.clone()))),
        };

        let result = match ready!(receiver.poll_recv(cx)) {
            Some(BodyEvent::Data(piece)) => {
                self.received += piece.len();
                Some(Ok(Frame::data(BodyPiece::read(piece))))
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

    /// Nobody reads the body past a failure, and dropping the receiving side is what
    /// tells that to whoever writes it
    fn fail(&mut self, reason: String) -> String {
        self.state = State::Failed(reason.clone());
        reason
    }
}
