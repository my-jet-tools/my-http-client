use std::{
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use hyper::body::{Body, Frame, Incoming, SizeHint};

use crate::MyHttpClientError;

mod body_chunk;
pub use body_chunk::*;
mod body_sender;
pub use body_sender::*;
mod no_hyper_body_reader_inner;
pub use no_hyper_body_reader_inner::*;
mod hyper_body_reader_inner;
pub use hyper_body_reader_inner::*;

/// How many pieces of a response body of the non-hyper client may wait for its reader.
/// When that many are waiting the socket is not read any more. The pieces share the two
/// buffers the socket is read into, which hold the socket back as well: this is for a
/// chunked body with many small chunks in a buffer
pub const RESPONSE_BODY_CHANNEL_CAPACITY: usize = 16;

/// When the request a body is the answer to runs out of time, and the timeout it was
/// given
pub(crate) type RequestDeadline = (tokio::time::Instant, Duration);

/// The body of a response, the same for all the clients of this crate. A response is
/// handed over as soon as its head is read, and its body is read through this reader:
///
/// * [`Self::next_item`] gives the body piece by piece, as it comes over the network.
///   A piece is a [`BodyChunk`]: it has the data of the body, and the bytes as they
///   have come;
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

    /// The next piece of the body, as it has come over the network. `None` is the end
    /// of the body.
    ///
    /// The piece gives both the data of the body and the bytes as they have come - see
    /// [`BodyChunk`]. A chunked body of the non-hyper client comes as
    /// [`BodyChunk::Chunked`] pieces, any other body as [`BodyChunk::Raw`] ones.
    ///
    /// A body which is cut short - the connection is closed, the framing is broken,
    /// nothing has come for the read timeout of the non-hyper client - ends with an
    /// error, and keeps answering with it: what was read before is not the whole body.
    ///
    /// The timeout of the request does not bound it, so a body may last for as long as
    /// the upstream keeps sending it.
    pub async fn next_item(&mut self) -> Result<Option<BodyChunk>, MyHttpClientError> {
        self.next_chunk().await
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
    async fn next_chunk(&self) -> Result<Option<BodyChunk>, MyHttpClientError> {
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
                Ok(chunk) if !chunk.as_raw_slice().is_empty() => return Ok(Some(chunk)),
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

        // The pieces share the buffer the connection is read into, and the body is to
        // be kept by whoever asks for it: it is copied out, into a buffer of its own
        while let Some(chunk) = self.next_chunk().await? {
            let data = chunk.as_slice();

            if data.len() > max_size - result.len() {
                return Err(MyHttpClientError::ResponseBodyTooLarge { limit: max_size });
            }

            result.extend_from_slice(data);
        }

        Ok(result)
    }

    fn poll_next_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<BodyChunk>, String>>> {
        match self {
            Self::NoHyper(inner) => inner.poll_frame(cx),
            Self::Hyper(inner) => inner.poll_frame(cx),
        }
    }

    fn request_deadline(&self) -> Option<RequestDeadline> {
        match self {
            Self::NoHyper(inner) => inner.request_deadline,
            Self::Hyper(inner) => inner.request_deadline,
        }
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

/// The body as a source of bytes: whoever reads it this way gets the data of the body
/// and nothing else. What a chunked body is framed with is left out, the trailers of an
/// HTTP/2 response are read past - [`BodyReader::next_item`] is for those who need the
/// pieces as they have come.
///
/// It reads through a shared reference, so the reader can be handed over as an
/// `Arc<dyn AsyncBytesStream<MyHttpClientError, Chunk = Bytes> + Send + Sync>`. The body is still one
/// stream: the callers take turns, and each piece goes to one of them.
///
/// A piece is handed over as the `Bytes` it has come in - its data is not copied.
///
/// `into_vec()` is the one the trait comes with, made of the two below. Unlike
/// [`BodyReader::into_vec`] it has no limit of the size and is not bounded by the
/// timeout of the request, and it allocates the size the response says at once.
#[async_trait::async_trait]
impl rust_extensions::AsyncBytesStream<MyHttpClientError> for BodyReader {
    type Chunk = Bytes;

    async fn get_next(&self) -> Result<Option<Bytes>, MyHttpClientError> {
        while let Some(chunk) = self.next_chunk().await? {
            let data = chunk.into_bytes();

            // The piece which ends a chunked body has no data in it
            if !data.is_empty() {
                return Ok(Some(data));
            }
        }

        Ok(None)
    }

    /// The size of the whole body when the response says it. `None` is a body which
    /// does not: it is chunked, or it lasts until the connection is closed
    fn get_size(&self) -> Option<usize> {
        self.content_length()
    }
}

/// hyper gets the data of the body: it frames what it sends on its own, so the sizes of
/// the chunks a [`BodyChunk::Chunked`] piece has come with are left out. The trailers a
/// body read by hyper ends with go as they are - [`BodyReader::next_item`] reads past
/// those
impl Body for BodyReader {
    type Data = Bytes;
    type Error = String;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        loop {
            let frame = match ready!(this.poll_next_frame(cx)) {
                Some(Ok(frame)) => frame.map_data(BodyChunk::into_bytes),
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => return Poll::Ready(None),
            };

            // The piece which ends a chunked body has no data in it
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
