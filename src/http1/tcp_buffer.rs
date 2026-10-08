use std::{ops::Range, time::Duration};

use futures::FutureExt;
use rust_extensions::{
    BufferToRead, DoubleBuffer, DoubleBufferChunk, DoubleBufferReader, DoubleBufferWriter,
};
use tokio::io::{AsyncRead, AsyncReadExt};

use super::HttpParseError;

const CRLF: &[u8] = b"\r\n";

/// The size of the buffer the heads of the responses are read into, and of each of the
/// two buffers their bodies are read into. A piece of a body is the data of one read, so
/// that is the most of the data a piece can have - and the longest line of a head which
/// can be read
pub const BUFFER_SIZE: usize = super::MAX_RESPONSE_BODY_PIECE_SIZE;

/// What the connection is read into: the heads of the responses into a buffer of their
/// own, the bodies into the two buffers of a [`DoubleBuffer`], in turns.
///
/// A body is not copied out of the buffer it is read into: the part of the read which
/// is the data goes to the reader of the body as it is - see [`BodyBuffers::hand_over`].
/// While the reader is busy with one buffer the socket is read into the other, and with
/// both of them held the reading of the body waits until one is let go
pub struct TcpBuffer {
    head: HeadBuffer,
    body: BodyBuffers,
}

impl Default for TcpBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl TcpBuffer {
    /// The buffers of a body are not allocated until a body is read
    pub fn new() -> Self {
        let (writer, reader) = DoubleBuffer::new(BUFFER_SIZE);

        Self {
            head: HeadBuffer::new(),
            body: BodyBuffers { writer, reader },
        }
    }

    /// Nothing in the buffer of the heads is left to consume
    pub fn is_empty(&self) -> bool {
        self.head.is_empty()
    }

    /// What is in the buffer of the heads and is not consumed yet
    pub fn get_buf(&self) -> &[u8] {
        self.head.get_buf()
    }

    /// Reads the socket once into the buffer of the heads: what is read goes behind
    /// what is not consumed yet. A line which does not fit into the buffer is refused.
    ///
    /// Nothing is lost when the call is dropped half way
    pub async fn read_from<TRead: AsyncRead + Unpin + ?Sized>(
        &mut self,
        read: &mut TRead,
        read_timeout: Duration,
    ) -> Result<(), HttpParseError> {
        let Some(buffer) = self.head.room_to_read_into() else {
            return Err(HttpParseError::invalid_payload(format!(
                "Write Buffer is too small to read http headers. Size: [{}]",
                BUFFER_SIZE
            )));
        };

        let size = read_once(read, buffer, read_timeout).await?;
        self.head.read_pos += size;

        Ok(())
    }

    /// Takes the line which is next, without the CRLF it ends with. `None`: the end of
    /// the line is not read yet
    pub fn read_until_crlf(&mut self) -> Option<&[u8]> {
        self.head.read_until_crlf()
    }

    pub fn skip_exactly(&mut self, size_to_skip: usize) -> Result<(), HttpParseError> {
        if self.head.get_buf().len() < size_to_skip {
            return Err(HttpParseError::GetMoreData);
        }

        self.head.consumed_pos += size_to_skip;
        Ok(())
    }

    /// The buffer of the heads and the buffers of the bodies, to be used at once: a
    /// read of a body goes into a buffer of the body, and what of it is not the body
    /// goes to the buffer of the heads
    pub fn split(&mut self) -> (&mut HeadBuffer, &BodyBuffers) {
        (&mut self.head, &self.body)
    }
}

/// The buffer the heads of the responses are read into.
///
/// It keeps what of a read of a body is not the body, too: the beginning of the next
/// response which has come with the end of the body, and a line of the framing of a
/// chunked body which the read has cut - the beginning of the next read completes it
pub struct HeadBuffer {
    buffer: Vec<u8>,
    /// How much of the buffer is read into
    read_pos: usize,
    /// How much of what is read is consumed
    consumed_pos: usize,
}

impl HeadBuffer {
    fn new() -> Self {
        Self {
            buffer: vec![0; BUFFER_SIZE],
            read_pos: 0,
            consumed_pos: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.consumed_pos == self.read_pos
    }

    /// What is not consumed yet
    pub fn get_buf(&self) -> &[u8] {
        &self.buffer[self.consumed_pos..self.read_pos]
    }

    pub fn consume(&mut self, size: usize) {
        assert!(
            size <= self.read_pos - self.consumed_pos,
            "consume: {} bytes are consumed, and {} are not consumed yet",
            size,
            self.read_pos - self.consumed_pos
        );

        self.consumed_pos += size;
    }

    /// Puts `src` behind what is not consumed yet. What does not fit into the buffer
    /// is refused: it is a line of the framing of a body which is longer than a buffer
    pub fn keep(&mut self, src: &[u8]) -> Result<(), HttpParseError> {
        if src.is_empty() {
            return Ok(());
        }

        let Some(free) = self
            .room_to_read_into()
            .filter(|free| free.len() >= src.len())
        else {
            return Err(HttpParseError::invalid_payload(format!(
                "A line of the body framing does not fit into the buffer of [{}] bytes",
                BUFFER_SIZE
            )));
        };

        free[..src.len()].copy_from_slice(src);
        self.read_pos += src.len();

        Ok(())
    }

    /// Completes the line which is not complete here with the beginning of `src`.
    ///
    /// `Some(taken)`: the line is complete - it is what [`Self::get_buf`] gives, with
    /// no CRLF - and `taken` bytes of `src` are taken for it, the CRLF among them.
    /// `None`: `src` does not have the end of the line, and all of it is taken
    pub fn complete_line(&mut self, src: &[u8]) -> Result<Option<usize>, HttpParseError> {
        // The CR of the CRLF may be the last byte which is here already
        if self.get_buf().last() == Some(&b'\r') && src.first() == Some(&b'\n') {
            self.read_pos -= 1;
            return Ok(Some(1));
        }

        match find_crlf(src) {
            Some(line_size) => {
                self.keep(&src[..line_size])?;
                Ok(Some(line_size + CRLF.len()))
            }
            None => {
                self.keep(src)?;
                Ok(None)
            }
        }
    }

    fn read_until_crlf(&mut self) -> Option<&[u8]> {
        let line_size = find_crlf(self.get_buf())?;

        let line_start = self.consumed_pos;
        self.consumed_pos += line_size + CRLF.len();

        Some(&self.buffer[line_start..line_start + line_size])
    }

    /// Where the next read goes: the room behind what is not consumed yet, which is
    /// moved to the beginning of the buffer first. `None`: what is not consumed takes
    /// all of the buffer
    fn room_to_read_into(&mut self) -> Option<&mut [u8]> {
        if self.consumed_pos > 0 {
            self.buffer.copy_within(self.consumed_pos..self.read_pos, 0);
            self.read_pos -= self.consumed_pos;
            self.consumed_pos = 0;
        }

        if self.read_pos == self.buffer.len() {
            return None;
        }

        Some(&mut self.buffer[self.read_pos..])
    }
}

/// The two buffers the bodies are read into, in turns. A buffer is read into, the data
/// of the body which is in it is handed over to the reader of the body, and the buffer
/// is free again once the reader drops it
pub struct BodyBuffers {
    writer: DoubleBufferWriter,
    reader: DoubleBufferReader,
}

impl BodyBuffers {
    /// One of the two buffers to read into. It waits until one is free: the reader of a
    /// body holds a buffer until it is done with what was read into it
    pub async fn get_buffer_to_read(&self) -> Result<BufferToRead<'_>, HttpParseError> {
        // The reader of the buffers is this very one: it is never dropped first
        self.writer
            .get_buffer_to_read()
            .await
            .map_err(|_| HttpParseError::error("No buffer to read into"))
    }

    /// Hands `data` of what is read into `buffer` over to the reader of the body: the
    /// piece is those bytes and nothing else, and the buffer is free again once the
    /// piece is dropped. `None`: there is no data - the buffer is free again at once
    pub fn hand_over(
        &self,
        buffer: BufferToRead<'_>,
        data: Range<usize>,
    ) -> Result<Option<DoubleBufferChunk>, HttpParseError> {
        if data.is_empty() {
            return Ok(None);
        }

        buffer.send_range(data);

        let Some(Ok(Some(piece))) = self.reader.get_next().now_or_never() else {
            return Err(HttpParseError::error("What is read is not in its buffer"));
        };

        Ok(Some(piece))
    }
}

/// Reads the socket once into `buffer`, which is not empty, and gives how much is read.
/// A read of nothing is the close of the connection - an error.
///
/// Nothing is lost when the call is dropped half way
pub async fn read_once<TRead: AsyncRead + Unpin + ?Sized>(
    read: &mut TRead,
    buffer: &mut [u8],
    read_timeout: Duration,
) -> Result<usize, HttpParseError> {
    match tokio::time::timeout(read_timeout, read.read(buffer)).await {
        Ok(Ok(0)) => Err(HttpParseError::Disconnected),
        Ok(Ok(size)) => Ok(size),
        Ok(Err(err)) => Err(HttpParseError::error(err.to_string())),
        Err(_) => Err(HttpParseError::ReadingTimeout(read_timeout)),
    }
}

/// Where the first line of `src` ends
pub fn find_crlf(src: &[u8]) -> Option<usize> {
    src.windows(CRLF.len()).position(|window| window == CRLF)
}
