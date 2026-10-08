use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, ReadHalf};

use crate::{
    http1::{HttpParseError, TcpBuffer, MAX_RESPONSE_BODY_PIECE_SIZE},
    BodyChunk,
};

use super::{BodyFraming, ChunksReadingMode};

const CRLF: &[u8] = b"\r\n";

/// A response body which is still on the wire: how it is framed, how far the reading of
/// it has got, and the reading side of the connection it comes through.
///
/// It gives the body the way it comes off the socket, in pieces:
///
/// * a body with a `content-length`, and a body which lasts until the connection is
///   closed, come as [`BodyChunk::Raw`] pieces - a piece is what a read() has brought;
/// * a chunked body comes as [`BodyChunk::Chunked`] pieces - the bytes as they are on
///   the wire, with the sizes of the chunks and their separators in them. A piece is
///   some data of a chunk with what frames it, so the data is always in one place of
///   the piece; the pieces put together are the body as the upstream has sent it.
///
/// Nothing waits for a chunk, let alone the body, to be complete - and nothing is
/// copied: a piece shares the block of the buffer the socket was read into
pub struct ResponseBodyOnTheWire<'s, TStream: AsyncRead> {
    read_stream: &'s mut ReadHalf<TStream>,
    tcp_buffer: &'s mut TcpBuffer,
    framing: BodyFraming,
    read_timeout: Duration,
    print_input_http_stream: bool,
}

impl<'s, TStream: AsyncRead> ResponseBodyOnTheWire<'s, TStream> {
    pub fn new(
        read_stream: &'s mut ReadHalf<TStream>,
        tcp_buffer: &'s mut TcpBuffer,
        framing: BodyFraming,
        read_timeout: Duration,
        print_input_http_stream: bool,
    ) -> Self {
        Self {
            read_stream,
            tcp_buffer,
            framing,
            read_timeout,
            print_input_http_stream,
        }
    }

    pub fn is_completed(&self) -> bool {
        matches!(self.framing, BodyFraming::Completed)
    }

    /// How much of the body is not read yet. `None` is a body which does not say it:
    /// it is chunked, or it lasts until the connection is closed
    pub fn remains_to_read(&self) -> Option<usize> {
        match self.framing {
            BodyFraming::LengthBased(remains) => Some(remains),
            BodyFraming::Completed => Some(0),
            BodyFraming::Chunked(_) | BodyFraming::UntilClose => None,
        }
    }

    /// The next piece of the body, with [`MAX_RESPONSE_BODY_PIECE_SIZE`] of its data at
    /// most. `None` is the end of the body.
    ///
    /// Nothing is lost when the call is dropped half way: what is read stays in the
    /// buffer, and the next call goes on from there
    pub async fn next_piece(&mut self) -> Result<Option<BodyChunk>, HttpParseError> {
        loop {
            match self.take_piece() {
                Err(HttpParseError::GetMoreData) => {}
                result => return result,
            }

            self.read_more().await?;
        }
    }

    /// Reads the socket once: what [`Self::take_piece`] has asked for with
    /// `GetMoreData`. With both buffers held by the pieces cut out of them, it waits for
    /// one to be free - see [`TcpBuffer::read_from`].
    ///
    /// Nothing is lost when the call is dropped half way
    pub async fn read_more(&mut self) -> Result<(), HttpParseError> {
        let result = crate::http1::read_to_buffer(
            self.read_stream,
            self.tcp_buffer,
            self.read_timeout,
            self.print_input_http_stream,
        )
        .await;

        match result {
            // The close of the connection is what ends a close-delimited body. Any
            // other body is cut short by it
            Err(HttpParseError::Disconnected)
                if matches!(self.framing, BodyFraming::UntilClose) =>
            {
                self.framing = BodyFraming::Completed;
                Ok(())
            }
            result => result,
        }
    }

    /// Reads what is left of a body nobody is going to read and throws it away, which
    /// brings the connection to the beginning of the next response. `false` is a
    /// connection which did not get there: more than `max_size` is left, the body has
    /// no end but the close of the connection, or the reading has failed
    pub async fn skip_the_rest(&mut self, max_size: usize) -> bool {
        match self.framing {
            BodyFraming::UntilClose => return false,
            BodyFraming::LengthBased(remains) if remains > max_size => return false,
            _ => {}
        }

        let mut skipped = 0;

        while let Ok(Some(piece)) = self.next_piece().await {
            skipped += piece.as_raw_slice().len();

            if skipped > max_size {
                return false;
            }
        }

        self.is_completed()
    }

    /// The next piece out of what is read off the socket already, `None` being the end
    /// of the body. `GetMoreData` is a buffer which does not have a piece in it yet:
    /// the socket has to be read - see [`Self::read_more`]
    pub fn take_piece(&mut self) -> Result<Option<BodyChunk>, HttpParseError> {
        match self.framing {
            BodyFraming::Completed => Ok(None),
            BodyFraming::LengthBased(0) => {
                self.framing = BodyFraming::Completed;
                Ok(None)
            }
            BodyFraming::LengthBased(remains) => {
                let data = self.take_data(remains)?;

                self.framing = if data.len() == remains {
                    BodyFraming::Completed
                } else {
                    BodyFraming::LengthBased(remains - data.len())
                };

                Ok(Some(BodyChunk::Raw(data)))
            }
            BodyFraming::UntilClose => Ok(Some(BodyChunk::Raw(self.take_data(usize::MAX)?))),
            BodyFraming::Chunked(mode) => self.take_chunked_piece(mode),
        }
    }

    /// Takes the data which is in the buffer, `max_size` of it at most
    fn take_data(&mut self, max_size: usize) -> Result<Bytes, HttpParseError> {
        let size = max_size
            .min(MAX_RESPONSE_BODY_PIECE_SIZE)
            .min(self.tcp_buffer.get_buf().len());

        if size == 0 {
            return Err(HttpParseError::GetMoreData);
        }

        Ok(self.tcp_buffer.take(size))
    }

    /// Takes a piece of a chunked body: the bytes of the buffer as they are, from where
    /// the reading has got to. It goes over what frames the data - the separator of
    /// the chunk before, the size of this one - takes the data which is there, and the
    /// separator behind it when the chunk is over. So a piece has the data in one place
    /// only: the next chunk is the next piece.
    ///
    /// What frames the data is not a piece on its own: it stays in the buffer until
    /// the data it frames is there. The one piece with no data is the last one - the
    /// chunk of no size which ends the body, with the trailers and the empty line
    fn take_chunked_piece(
        &mut self,
        mut mode: ChunksReadingMode,
    ) -> Result<Option<BodyChunk>, HttpParseError> {
        let buffer = self.tcp_buffer.get_buf();

        let mut size = 0;
        let mut data = None;
        let mut chunk_size_read = None;
        let mut completed = false;

        loop {
            match mode {
                ChunksReadingMode::WaitingFroChunkSize => {
                    let Some(line_size) = find_crlf(&buffer[size..]) else {
                        break;
                    };

                    let chunk_size = super::parse_chunk_size(&buffer[size..size + line_size])?;

                    chunk_size_read = Some(chunk_size);
                    size += line_size + CRLF.len();

                    mode = if chunk_size == 0 {
                        ChunksReadingMode::WaitingForEnd
                    } else {
                        ChunksReadingMode::ReadingChunk(chunk_size)
                    };
                }
                ChunksReadingMode::ReadingChunk(remains) => {
                    let data_size = remains
                        .min(buffer.len() - size)
                        .min(MAX_RESPONSE_BODY_PIECE_SIZE);

                    if data_size == 0 {
                        break;
                    }

                    data = Some(size..size + data_size);
                    size += data_size;

                    if data_size < remains {
                        mode = ChunksReadingMode::ReadingChunk(remains - data_size);
                        break;
                    }

                    mode = ChunksReadingMode::WaitingForSeparator;
                }
                ChunksReadingMode::WaitingForSeparator => {
                    if buffer.len() - size < CRLF.len() {
                        break;
                    }

                    size += CRLF.len();
                    mode = ChunksReadingMode::WaitingFroChunkSize;

                    // The chunk is over, and the piece with it
                    if data.is_some() {
                        break;
                    }
                }
                ChunksReadingMode::WaitingForEnd => {
                    // The trailers are read past: the body ends with an empty line
                    let Some(line_size) = find_crlf(&buffer[size..]) else {
                        break;
                    };

                    size += line_size + CRLF.len();

                    if line_size == 0 {
                        completed = true;
                        break;
                    }
                }
            }
        }

        if data.is_none() && !completed {
            return Err(HttpParseError::GetMoreData);
        }

        // Not earlier: the size of a chunk which waits for its data is read once more
        if let (true, Some(chunk_size)) = (self.print_input_http_stream, chunk_size_read) {
            println!("Read body chunk size: {}", chunk_size);
        }

        let piece = BodyChunk::chunked(self.tcp_buffer.take(size), data.unwrap_or(size..size));

        self.framing = if completed {
            BodyFraming::Completed
        } else {
            BodyFraming::Chunked(mode)
        };

        Ok(Some(piece))
    }
}

/// Where the first line of `src` ends
fn find_crlf(src: &[u8]) -> Option<usize> {
    src.windows(CRLF.len()).position(|window| window == CRLF)
}
