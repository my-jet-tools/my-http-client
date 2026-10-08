use std::{
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt};
use hyper::body::{Body, Frame, Incoming, SizeHint};

use crate::MyHttpClientError;

mod body_piece;
pub use body_piece::*;
mod body_sender;
pub use body_sender::*;
mod no_hyper_body_reader_inner;
pub use no_hyper_body_reader_inner::*;
mod hyper_body_reader_inner;
pub use hyper_body_reader_inner::*;

/// How many items of a response body of the non-hyper client may wait for its reader:
/// two pieces - a piece holds one of the two buffers the socket is read into, so there
/// are no more of them - and the end of the body behind them. So a body which is off the
/// wire does not hold the read loop back, however it is read
pub const RESPONSE_BODY_CHANNEL_CAPACITY: usize = 3;

/// When the request a body is the answer to runs out of time, and the timeout it was
/// given
pub(crate) type RequestDeadline = (tokio::time::Instant, Duration);

/// The body of a response, the same for all the clients of this crate. A response is
/// handed over as soon as its head is read, and its body is read through this reader:
///
/// * [`Self::next_item`] gives the data of the body piece by piece, as it comes over
///   the network;
/// * [`Self::into_vec`] reads the whole of it into memory.
///
/// Whoever needs nothing but the bytes of the body reads it as a
/// [`rust_extensions::AsyncBytesStream`], which the reader is as well.
///
/// A body which is not read is not held in memory - it stays with the upstream. What
/// reads the connection is what the two cases are about, and each of them has the whole
/// of its reading in its inner.
///
/// It is a `hyper::body::Body` too, so it can be handed to hyper as it is.
pub enum BodyReader {
    /// A response of the non-hyper client: the body is written by the read loop of the
    /// client as it comes off the socket, and received through a channel which holds
    /// the read loop back when nobody reads
    NoHyper(NoHyperBodyReaderInner),
    /// A response of a client which is built on hyper - the HTTP/1.1 one and the HTTP/2
    /// one: a wrapper of the body hyper gives
    Hyper(HyperBodyReaderInner),
}

impl BodyReader {
    /// A body of the non-hyper client and the sender it is written through.
    /// `content_length` is the size of the body when the response says it; `None` is a
    /// body which does not - it is chunked, or it lasts until the connection is closed
    pub fn new(content_length: Option<usize>) -> (BodySender, Self) {
        let (sender, inner) = NoHyperBodyReaderInner::new(content_length);
        (sender, Self::NoHyper(inner))
    }

    /// The body of a response which has none
    pub fn empty() -> Self {
        Self::NoHyper(NoHyperBodyReaderInner::empty())
    }

    /// A body which is read by hyper
    pub fn from_hyper(body: Incoming) -> Self {
        Self::Hyper(HyperBodyReaderInner::new(body))
    }

    /// The size of the whole body when the response says it. `None` is a body which
    /// does not: it is chunked, or it lasts until the connection is closed
    pub fn content_length(&self) -> Option<usize> {
        match self {
            Self::NoHyper(inner) => inner.content_length(),
            Self::Hyper(inner) => inner.content_length(),
        }
    }

    /// How much of the body is not read yet. `None` is a body which does not say it
    pub fn remains_to_read(&self) -> Option<usize> {
        match self {
            Self::NoHyper(inner) => inner.remains_to_read(),
            Self::Hyper(inner) => inner.remains_to_read(),
        }
    }

    /// The next piece of the body: the data of it which has come over the network
    /// since the piece before. `None` is the end of the body.
    ///
    /// The piece is the very buffer the connection is read into, and it is good until
    /// the next call: the call lets go of it, so that the connection can be read into
    /// the buffer while the piece after it is waited for. What is needed for longer is
    /// copied out of it.
    ///
    /// A body which is cut short - the connection is closed, the framing is broken,
    /// nothing has come for the read timeout of the non-hyper client - ends with an
    /// error, and keeps answering with it: what was read before is not the whole body.
    ///
    /// The timeout of the request does not bound it, so a body may last for as long as
    /// the upstream keeps sending it.
    pub async fn next_item(&mut self) -> Result<Option<&[u8]>, MyHttpClientError> {
        *self.current_mut() = None;

        let piece = self.next_piece().await?;

        let current = self.current_mut();
        *current = piece;

        Ok(current.as_deref())
    }

    /// Reads the body to its end and gives it as a whole - what is left of it, when a
    /// part is taken by [`Self::next_item`] already.
    ///
    /// `max_size` is how big the body may be to be held in memory, in bytes: a bigger
    /// one fails with [`MyHttpClientError::ResponseBodyTooLarge`] - before a byte of it
    /// is read when the response says how big it is, as soon as it grows past the limit
    /// when it does not. There is no limit but this one: `usize::MAX` takes a body of
    /// any size.
    ///
    /// The body has to be complete within the timeout of the request it is the answer
    /// to: the timeout the request was sent with covers the request, the head and this
    /// call.
    pub async fn into_vec(self, max_size: usize) -> Result<Vec<u8>, MyHttpClientError> {
        self.read_within_the_request_timeout(max_size).await
    }

    /// The next piece through a shared reference. The body is one stream, so the
    /// readers take turns: each piece goes to one of them
    async fn next_piece(&self) -> Result<Option<BodyPiece>, MyHttpClientError> {
        loop {
            let frame = match self {
                Self::NoHyper(inner) => inner.next_frame().await,
                Self::Hyper(inner) => inner.next_frame().await,
            };

            let Some(frame) = frame else {
                return Ok(None);
            };

            let frame = frame.map_err(MyHttpClientError::CanNotExecuteRequest)?;

            // The trailers of an HTTP/2 response are not a part of the body, and a
            // frame hyper gives may have nothing in it at all
            match frame.into_data() {
                Ok(piece) if !piece.is_empty() => return Ok(Some(piece)),
                _ => {}
            }
        }
    }

    async fn read_within_the_request_timeout(
        &self,
        max_size: usize,
    ) -> Result<Vec<u8>, MyHttpClientError> {
        let Some((deadline, request_timeout)) = self.request_deadline() else {
            return self.read_to_vec(max_size).await;
        };

        match tokio::time::timeout_at(deadline, self.read_to_vec(max_size)).await {
            Ok(result) => result,
            Err(_) => Err(MyHttpClientError::RequestTimeout(request_timeout)),
        }
    }

    async fn read_to_vec(&self, max_size: usize) -> Result<Vec<u8>, MyHttpClientError> {
        if self.remains_to_read().is_some_and(|size| size > max_size) {
            return Err(MyHttpClientError::ResponseBodyTooLarge { limit: max_size });
        }

        let mut result = Vec::new();

        // A body which says its size is allocated once. The size is what the response
        // says, and the limit may be none at all: a size which can not be allocated
        // must not bring the process down
        if let Some(remains) = self.remains_to_read() {
            let _ = result.try_reserve_exact(remains);
        }

        // A piece is the buffer the connection is read into, and the body is to be kept
        // by whoever asks for it: it is copied out, into a buffer of its own
        while let Some(piece) = self.next_piece().await? {
            if piece.len() > max_size - result.len() {
                return Err(MyHttpClientError::ResponseBodyTooLarge { limit: max_size });
            }

            result.extend_from_slice(&piece);
        }

        Ok(result)
    }

    fn poll_next_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<BodyPiece>, String>>> {
        match self {
            Self::NoHyper(inner) => inner.poll_frame(cx),
            Self::Hyper(inner) => inner.poll_frame(cx),
        }
    }

    fn current_mut(&mut self) -> &mut Option<BodyPiece> {
        match self {
            Self::NoHyper(inner) => &mut inner.current,
            Self::Hyper(inner) => &mut inner.current,
        }
    }

    fn request_deadline(&self) -> Option<RequestDeadline> {
        match self {
            Self::NoHyper(inner) => inner.request_deadline,
            Self::Hyper(inner) => inner.request_deadline,
        }
    }

    /// The body as the body of a [`crate::HyperResponse`], which is made of `Bytes`. A
    /// piece goes into the `Bytes` as it is: they hold the buffer it is read into, and
    /// the data is not copied
    pub(crate) fn into_bytes_body(self) -> BoxBody<Bytes, String> {
        BytesBody(self).boxed()
    }

    /// `deadline` is when the request runs out of the `request_timeout` it was sent
    /// with. `None` is a timeout too long to have one
    pub(crate) fn set_request_deadline(
        &mut self,
        deadline: Option<tokio::time::Instant>,
        request_timeout: Duration,
    ) {
        let request_deadline = deadline.map(|deadline| (deadline, request_timeout));

        match self {
            Self::NoHyper(inner) => inner.request_deadline = request_deadline,
            Self::Hyper(inner) => inner.request_deadline = request_deadline,
        }
    }
}

/// A response which is read by hyper, with its body behind the reader. `deadline` is
/// when the request runs out of the `request_timeout` it was sent with
pub(crate) fn from_hyper_response(
    response: http::Response<Incoming>,
    deadline: Option<tokio::time::Instant>,
    request_timeout: Duration,
) -> http::Response<BodyReader> {
    let mut response = response.map(BodyReader::from_hyper);

    response
        .body_mut()
        .set_request_deadline(deadline, request_timeout);

    response
}

/// The body as a source of bytes: whoever reads it this way gets the data of the body,
/// the same as [`BodyReader::next_item`] gives. The trailers of an HTTP/2 response are
/// read past.
///
/// It reads through a shared reference, so the reader can be handed over as an
/// `Arc<dyn AsyncBytesStream<MyHttpClientError, Chunk = BodyPiece> + Send + Sync>`. The
/// body is still one stream: the callers take turns, and each piece goes to one of them.
///
/// A piece is handed over as the buffer it is read into - its data is not copied. It
/// holds the buffer until it is dropped, see [`BodyPiece`].
///
/// `into_vec()` is the one the trait comes with, made of the two below. Unlike
/// [`BodyReader::into_vec`] it has no limit of the size and is not bounded by the
/// timeout of the request, and it allocates the size the response says at once.
#[async_trait::async_trait]
impl rust_extensions::AsyncBytesStream<MyHttpClientError> for BodyReader {
    type Chunk = BodyPiece;

    async fn get_next(&self) -> Result<Option<BodyPiece>, MyHttpClientError> {
        self.next_piece().await
    }

    /// The size of the whole body when the response says it. `None` is a body which
    /// does not: it is chunked, or it lasts until the connection is closed
    fn get_size(&self) -> Option<usize> {
        self.content_length()
    }
}

/// A body reader as a body of `Bytes` - see [`BodyReader::into_bytes_body`]. What it
/// says of the size of the body is what the reader says
struct BytesBody(BodyReader);

impl Body for BytesBody {
    type Data = Bytes;
    type Error = String;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, String>>> {
        let frame = ready!(Pin::new(&mut self.get_mut().0).poll_frame(cx));
        Poll::Ready(frame.map(|frame| frame.map(|frame| frame.map_data(Bytes::from_owner))))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.0.size_hint()
    }
}

/// hyper gets the data of the body, a piece of it as the buffer it is read into. The
/// trailers a body read by hyper ends with go as they are - [`BodyReader::next_item`]
/// reads past those
impl Body for BodyReader {
    type Data = BodyPiece;
    type Error = String;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        loop {
            let frame = match ready!(this.poll_next_frame(cx)) {
                Some(Ok(frame)) => frame,
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => return Poll::Ready(None),
            };

            // A frame hyper gives may have nothing in it at all
            if frame.data_ref().is_some_and(|data| data.is_empty()) {
                continue;
            }

            return Poll::Ready(Some(Ok(frame)));
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::NoHyper(inner) => inner.is_end_stream(),
            Self::Hyper(inner) => inner.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::NoHyper(inner) => inner.size_hint(),
            Self::Hyper(inner) => inner.size_hint(),
        }
    }
}
