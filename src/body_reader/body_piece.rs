use std::ops::Deref;

use bytes::{Buf, Bytes};
use rust_extensions::DoubleBufferChunk;

/// A piece of a response body: the data of it, as a `&[u8]` through `Deref`.
///
/// A piece of the non-hyper client is the data one read of the connection has brought,
/// with what framed it cut off - the head before it, the sizes of the chunks and their
/// separators. It is not a copy: it is a part of one of the two buffers the connection
/// is read into, and the buffer is read into again once the piece is dropped. So a
/// piece is to be used and dropped - what is needed for long is copied out of it.
///
/// A piece of a client which is built on hyper is the data of a frame hyper gives
pub struct BodyPiece {
    data: Data,
    /// How much of the data is taken through [`Buf::advance`]
    taken: usize,
}

enum Data {
    Read(DoubleBufferChunk),
    Hyper(Bytes),
}

impl BodyPiece {
    pub(crate) fn read(chunk: DoubleBufferChunk) -> Self {
        Self {
            data: Data::Read(chunk),
            taken: 0,
        }
    }

    pub(crate) fn hyper(data: Bytes) -> Self {
        Self {
            data: Data::Hyper(data),
            taken: 0,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        let data: &[u8] = match &self.data {
            Data::Read(chunk) => chunk,
            Data::Hyper(data) => data,
        };

        &data[self.taken..]
    }
}

impl Deref for BodyPiece {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for BodyPiece {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::fmt::Debug for BodyPiece {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BodyPiece")
            .field("len", &self.as_slice().len())
            .finish()
    }
}

/// What hyper takes the data of a body as
impl Buf for BodyPiece {
    fn remaining(&self) -> usize {
        self.as_slice().len()
    }

    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }

    fn advance(&mut self, cnt: usize) {
        assert!(
            cnt <= self.remaining(),
            "advance: {} bytes are taken, and the piece has {} of them",
            cnt,
            self.remaining()
        );

        self.taken += cnt;
    }
}
