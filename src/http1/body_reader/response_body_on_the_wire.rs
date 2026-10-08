use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, ReadHalf};

use crate::http1::{HttpParseError, TcpBuffer, MAX_RESPONSE_BODY_PIECE_SIZE};

use super::{BodyFraming, ChunksReadingMode};

/// A response body which is still on the wire: how it is framed, how far the reading of
/// it has got, and the reading side of the connection it comes through.
///
/// It gives the body the way it comes off the socket. Whatever the framing is, a piece
/// is what a read() has brought: with a `content-length` or with a body which lasts
/// until the connection is closed that is the read itself, with chunks it is the read
/// with the sizes of the chunks and their separators taken out. Nothing waits for a
/// chunk, let alone the body, to be complete
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

    /// The next piece of the body, of [`MAX_RESPONSE_BODY_PIECE_SIZE`] at most. `None`
    /// is the end of the body.
    ///
    /// Nothing is lost when the call is dropped half way: what is read stays in the
    /// buffer, and the next call goes on from there
    pub async fn next_piece(&mut self) -> Result<Option<Bytes>, HttpParseError> {
        if !self.wait_for_data().await? {
            return Ok(None);
        }

        Ok(Some(Bytes::copy_from_slice(self.take())))
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

        while let Ok(true) = self.wait_for_data().await {
            skipped += self.take().len();

            if skipped > max_size {
                return false;
            }
        }

        self.is_completed()
    }

    /// `true` once some data of the body is in the buffer, for [`Self::take`] to take
    /// it; `false` when the body is over
    async fn wait_for_data(&mut self) -> Result<bool, HttpParseError> {
        loop {
            match self.skip_to_data() {
                Ok(has_data) => return Ok(has_data),
                Err(HttpParseError::GetMoreData) => {}
                Err(err) => return Err(err),
            }

            let result = crate::http1::read_to_buffer(
                self.read_stream,
                self.tcp_buffer,
                self.read_timeout,
                self.print_input_http_stream,
            )
            .await;

            match result {
                Ok(()) => {}
                // The close of the connection is what ends a close-delimited body. Any
                // other body is cut short by it
                Err(HttpParseError::Disconnected)
                    if matches!(self.framing, BodyFraming::UntilClose) =>
                {
                    self.framing = BodyFraming::Completed;
                    return Ok(false);
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Moves over what is in the buffer and is not the body itself: the sizes of the
    /// chunks, their separators, the trailers. `GetMoreData` is a buffer which does not
    /// have what it takes to tell whether there is more of the body
    fn skip_to_data(&mut self) -> Result<bool, HttpParseError> {
        loop {
            match self.framing {
                BodyFraming::Completed => return Ok(false),
                BodyFraming::LengthBased(0) => {
                    self.framing = BodyFraming::Completed;
                }
                BodyFraming::LengthBased(_)
                | BodyFraming::UntilClose
                | BodyFraming::Chunked(ChunksReadingMode::ReadingChunk(_)) => {
                    if self.tcp_buffer.is_empty() {
                        return Err(HttpParseError::GetMoreData);
                    }

                    return Ok(true);
                }
                BodyFraming::Chunked(ChunksReadingMode::WaitingFroChunkSize) => {
                    let Some(line) = self.tcp_buffer.read_until_crlf() else {
                        return Err(HttpParseError::GetMoreData);
                    };

                    let chunk_size = super::parse_chunk_size(line)?;

                    if self.print_input_http_stream {
                        println!("Read body chunk size: {}", chunk_size);
                    }

                    self.framing = BodyFraming::Chunked(if chunk_size == 0 {
                        ChunksReadingMode::WaitingForEnd
                    } else {
                        ChunksReadingMode::ReadingChunk(chunk_size)
                    });
                }
                BodyFraming::Chunked(ChunksReadingMode::WaitingForSeparator) => {
                    self.tcp_buffer.skip_exactly(2)?;
                    self.framing = BodyFraming::Chunked(ChunksReadingMode::WaitingFroChunkSize);
                }
                BodyFraming::Chunked(ChunksReadingMode::WaitingForEnd) => {
                    // The trailers are read past: the body ends with an empty line
                    let Some(line) = self.tcp_buffer.read_until_crlf() else {
                        return Err(HttpParseError::GetMoreData);
                    };

                    if line.is_empty() {
                        self.framing = BodyFraming::Completed;
                    }
                }
            }
        }
    }

    /// Takes out of the buffer the data [`Self::wait_for_data`] has found there
    fn take(&mut self) -> &[u8] {
        let remains = match self.framing {
            BodyFraming::LengthBased(remains) => remains,
            BodyFraming::Chunked(ChunksReadingMode::ReadingChunk(remains)) => remains,
            BodyFraming::UntilClose => usize::MAX,
            BodyFraming::Chunked(_) | BodyFraming::Completed => 0,
        };

        let data = self
            .tcp_buffer
            .get_as_much_as_possible(remains.min(MAX_RESPONSE_BODY_PIECE_SIZE))
            .unwrap_or_default();

        self.framing = match self.framing {
            BodyFraming::LengthBased(remains) if remains == data.len() => BodyFraming::Completed,
            BodyFraming::LengthBased(remains) => BodyFraming::LengthBased(remains - data.len()),
            BodyFraming::Chunked(ChunksReadingMode::ReadingChunk(remains))
                if remains == data.len() =>
            {
                BodyFraming::Chunked(ChunksReadingMode::WaitingForSeparator)
            }
            BodyFraming::Chunked(ChunksReadingMode::ReadingChunk(remains)) => {
                BodyFraming::Chunked(ChunksReadingMode::ReadingChunk(remains - data.len()))
            }
            framing => framing,
        };

        data
    }
}
