use std::ops::Range;

use bytes::{Buf, Bytes};

/// A piece of a response body, as it has come over the network.
///
/// It is held the way it has come, and gives both what has come and the data of the
/// body which is in it:
///
/// | | the data of the body | as it has come |
/// | --- | --- | --- |
/// | to look at | [`Self::as_slice`] | [`Self::as_raw_slice`] |
/// | to take, sharing the buffer | [`Self::into_bytes`] | [`Self::into_raw_bytes`] |
/// | to take, as a buffer of its own | [`Self::into_vec`] | [`Self::into_raw`] |
///
/// For a [`Self::Raw`] piece the two are the same bytes. For a [`Self::Chunked`] one
/// they differ by the sizes of the chunks and their separators.
///
/// A piece is not a copy of what was read off the socket: it shares the buffer the
/// socket was read into, and so does a `Bytes` taken out of it. That buffer is read
/// into again once every piece of it is dropped - so a piece is meant to be used and
/// dropped. What has to be kept for long is better taken as a `Vec`: it is a copy of
/// its own, and holds nothing else.
#[derive(Debug, Clone)]
pub enum BodyChunk {
    /// The bytes of the body as they are: the body has a `content-length`, or lasts
    /// until the connection is closed - or it is read by hyper, which gives the data
    /// with the chunks already taken apart
    Raw(Bytes),
    /// A piece of a body in the chunked transfer coding, as it is on the wire: the data
    /// with what frames it - the size of the chunk before it, the separator after it.
    /// The pieces put together are the body exactly as the upstream has sent it, so
    /// the last one is the chunk which ends the body, with the trailers, and has no
    /// data in it
    Chunked(ChunkedData),
}

/// The bytes of a body which have nothing but its data in them
impl From<Bytes> for BodyChunk {
    fn from(data: Bytes) -> Self {
        Self::Raw(data)
    }
}

/// The bytes of a chunked body as they are on the wire, and where the data is among
/// them
#[derive(Debug, Clone)]
pub struct ChunkedData {
    raw: Bytes,
    data: Range<usize>,
}

impl BodyChunk {
    /// `data` is where the data of the body is in `raw`
    pub(crate) fn chunked(raw: Bytes, data: Range<usize>) -> Self {
        Self::Chunked(ChunkedData { raw, data })
    }

    /// The data of the body which is in the piece. For a chunked piece it is what is
    /// between the size of the chunk and its separator
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Raw(data) => data,
            Self::Chunked(chunked) => &chunked.raw[chunked.data.clone()],
        }
    }

    /// The piece as it has come over the network. For a chunked piece that is with the
    /// sizes of the chunks and their separators
    pub fn as_raw_slice(&self) -> &[u8] {
        match self {
            Self::Raw(data) => data,
            Self::Chunked(chunked) => &chunked.raw,
        }
    }

    /// The data of the body which is in the piece, as a buffer of its own. A chunked
    /// piece has what frames the data cut off
    pub fn into_vec(self) -> Vec<u8> {
        into_a_vec_of_its_own(self.into_bytes())
    }

    /// The piece as it has come over the network, as a buffer of its own
    pub fn into_raw(self) -> Vec<u8> {
        into_a_vec_of_its_own(self.into_raw_bytes())
    }

    /// The data of the body which is in the piece. Nothing is copied: it shares the
    /// buffer the piece has come in
    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Raw(data) => data,
            Self::Chunked(ChunkedData { mut raw, data }) => {
                raw.truncate(data.end);
                raw.advance(data.start);
                raw
            }
        }
    }

    /// The piece as it has come over the network. Nothing is copied: it shares the
    /// buffer the piece has come in
    pub fn into_raw_bytes(self) -> Bytes {
        match self {
            Self::Raw(data) => data,
            Self::Chunked(chunked) => chunked.raw,
        }
    }
}

/// The bytes which share a buffer are copied out of it. The ones which are the last to
/// hold their buffer are not: they become a `Vec` which takes the whole buffer over -
/// and what of it is not the data is given back here
fn into_a_vec_of_its_own(src: Bytes) -> Vec<u8> {
    let mut result: Vec<u8> = src.into();
    result.shrink_to_fit();
    result
}
