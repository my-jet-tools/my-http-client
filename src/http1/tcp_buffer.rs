use std::time::Duration;

use bytes::{Buf, Bytes};
use futures::FutureExt;
use rust_extensions::{DoubleBuffer, DoubleBufferReader, DoubleBufferWriter};
use tokio::io::{AsyncRead, AsyncReadExt};

use super::HttpParseError;

const CRLF: &[u8] = b"\r\n";

/// The size of each of the two buffers the socket is read into. A piece of a response
/// body is cut out of one of them, so that is the most of its data a piece can have -
/// and the longest line of a head which can be read
const BUFFER_SIZE: usize = super::MAX_RESPONSE_BODY_PIECE_SIZE;

/// What is read off the socket and not consumed yet.
///
/// The socket is read into the two buffers of a [`DoubleBuffer`], in turns, and nothing
/// else is allocated for it. A piece of a response body is not copied out of a buffer:
/// [`Self::take`] gives a piece which shares it, and the buffer is free to be read into
/// again once every piece of it is dropped. While the pieces of one buffer are with
/// whoever the body is for, the socket is read into the other one; with both of them
/// held, [`Self::read_from`] waits until one is let go.
///
/// So whoever the body is for has to let go of a piece before it asks for the piece
/// after the next one: copy it, parse it, pass it on. One which keeps the pieces of
/// both buffers and asks for more waits for ever - nothing is read until a buffer is
/// free
pub struct TcpBuffer {
    writer: DoubleBufferWriter,
    reader: DoubleBufferReader,
    /// What is read. From `consumed` on it is not consumed yet. It shares the buffer it
    /// was read into
    read: Bytes,
    consumed: usize,
}

impl Default for TcpBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl TcpBuffer {
    /// Nothing is allocated until the socket is read
    pub fn new() -> Self {
        let (writer, reader) = DoubleBuffer::new(BUFFER_SIZE);

        Self {
            writer,
            reader,
            read: Bytes::new(),
            consumed: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.consumed == self.read.len()
    }

    /// What is read off the socket and not consumed yet
    pub fn get_buf(&self) -> &[u8] {
        &self.read[self.consumed..]
    }

    /// Reads the socket once: what is read goes behind what is not consumed yet.
    ///
    /// It waits for one of the two buffers to be free, and reads into it. What is not
    /// consumed yet goes to the beginning of that buffer, so that what is parsed is one
    /// run of bytes. It is small as a rule - a line which is not complete, the beginning
    /// of the head which is next - and it is the only copy. A line which does not fit
    /// into a buffer is refused.
    ///
    /// `read_timeout` bounds the read of the socket, and not the wait for a buffer.
    /// Nothing is lost when the call is dropped half way
    pub async fn read_from<TRead: AsyncRead + Unpin + ?Sized>(
        &mut self,
        read: &mut TRead,
        read_timeout: Duration,
    ) -> Result<(), HttpParseError> {
        // What is consumed holds a buffer no more: it may be the very one to read into
        self.forget_consumed();

        let unconsumed = self.read.len();

        if unconsumed >= BUFFER_SIZE {
            return Err(HttpParseError::invalid_payload(format!(
                "Write Buffer is too small to read http headers. Size: [{}]",
                BUFFER_SIZE
            )));
        }

        // The reader of the buffers is this very one: it is never dropped first
        let Ok(mut buffer) = self.writer.get_buffer_to_read().await else {
            return Err(HttpParseError::error("No buffer to read into"));
        };

        buffer[..unconsumed].copy_from_slice(&self.read);

        let size =
            match tokio::time::timeout(read_timeout, read.read(&mut buffer[unconsumed..])).await {
                Ok(Ok(0)) => return Err(HttpParseError::Disconnected),
                Ok(Ok(size)) => size,
                Ok(Err(err)) => return Err(HttpParseError::error(err.to_string())),
                Err(_) => return Err(HttpParseError::ReadingTimeout(read_timeout)),
            };

        buffer.send(unconsumed + size);

        let Some(Ok(Some(chunk))) = self.reader.get_next().now_or_never() else {
            return Err(HttpParseError::error("What is read is not in its buffer"));
        };

        // The pieces cut out of it share the buffer, which is free again once the last
        // of them is dropped. The buffer of what was not consumed is let go of: it is
        // copied
        self.read = Bytes::from_owner(chunk);

        Ok(())
    }

    /// Takes the line which is next, without the CRLF it ends with. `None`: the end of
    /// the line is not read yet
    pub fn read_until_crlf(&mut self) -> Option<&[u8]> {
        let line_size = self
            .get_buf()
            .windows(CRLF.len())
            .position(|window| window == CRLF)?;

        let line_start = self.consumed;
        self.consumed += line_size + CRLF.len();

        Some(&self.read[line_start..line_start + line_size])
    }

    pub fn skip_exactly(&mut self, size_to_skip: usize) -> Result<(), HttpParseError> {
        if self.get_buf().len() < size_to_skip {
            return Err(HttpParseError::GetMoreData);
        }

        self.consumed += size_to_skip;
        Ok(())
    }

    /// Takes the next `size` bytes as a piece which shares the buffer they are in:
    /// nothing is copied. The buffer is not read into again until the piece is dropped.
    ///
    /// Panics when there is less than `size` to take
    pub fn take(&mut self, size: usize) -> Bytes {
        self.forget_consumed();
        self.read.split_to(size)
    }

    fn forget_consumed(&mut self) {
        if self.consumed == self.read.len() {
            // An empty `Bytes` which is left of a buffer holds it all the same
            self.read = Bytes::new();
        } else {
            self.read.advance(self.consumed);
        }

        self.consumed = 0;
    }
}
