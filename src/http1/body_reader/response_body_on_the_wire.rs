use std::{ops::Range, time::Duration};

use rust_extensions::DoubleBufferChunk;
use tokio::io::{AsyncRead, ReadHalf};

use crate::http1::{find_crlf, read_once, HeadBuffer, HttpParseError, TcpBuffer};

use super::{BodyFraming, ChunksReadingMode};

const CRLF: &[u8] = b"\r\n";

/// A response body which is still on the wire: how it is framed, how far the reading of
/// it has got, and the reading side of the connection it comes through.
///
/// It gives the body the way it comes off the socket: a piece is the data of the body
/// which one read has brought. For a chunked body that is the data of the chunks in the
/// read, with what framed them cut off - see [`take_chunked_data`].
///
/// Nothing waits for a chunk, let alone the body, to be complete - and nothing is copied:
/// a piece is a part of the buffer the socket was read into
pub struct ResponseBodyOnTheWire<'s, TStream: AsyncRead> {
    read_stream: &'s mut ReadHalf<TStream>,
    tcp_buffer: &'s mut TcpBuffer,
    framing: BodyFraming,
    /// What was read along with the head, past its end, is the beginning of the body:
    /// it is the first read of the body
    read_with_the_head: bool,
    /// The framing is broken behind the data of the piece given last: this is what the
    /// call after it gives
    broken: Option<HttpParseError>,
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
            read_with_the_head: true,
            broken: None,
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

    /// The next piece of the body, `None` being the end of it.
    ///
    /// It is read into one of the two buffers of the body, so it waits for one of them
    /// to be free first. A read which has no data of the body in it - nothing but the
    /// size of a chunk, say - gives no piece: the socket is read once more.
    ///
    /// Nothing is lost when the call is dropped half way: what is read is handed over
    /// in the same go
    pub async fn next_piece(&mut self) -> Result<Option<DoubleBufferChunk>, HttpParseError> {
        if let Some(err) = self.broken.take() {
            return Err(err);
        }

        loop {
            let max_size = match self.framing {
                BodyFraming::Completed => return Ok(None),
                BodyFraming::LengthBased(0) => {
                    self.framing = BodyFraming::Completed;
                    return Ok(None);
                }
                // Nothing past the end of the body is read: the next response stays
                // on the wire
                BodyFraming::LengthBased(remains) => remains,
                BodyFraming::Chunked(_) | BodyFraming::UntilClose => usize::MAX,
            };

            let (head, body) = self.tcp_buffer.split();

            let mut buffer = body.get_buffer_to_read().await?;
            let max_size = max_size.min(buffer.len());

            let size = if self.read_with_the_head && !head.is_empty() {
                let read = head.get_buf();
                let size = read.len().min(max_size);

                buffer[..size].copy_from_slice(&read[..size]);
                head.consume(size);

                size
            } else {
                match read_once(self.read_stream, &mut buffer[..max_size], self.read_timeout).await
                {
                    Ok(size) => {
                        if self.print_input_http_stream {
                            println!("Resp: [{:?}]", std::str::from_utf8(&buffer[..size]));
                        }

                        size
                    }
                    // The close of the connection is what ends a close-delimited body.
                    // Any other body is cut short by it
                    Err(HttpParseError::Disconnected)
                        if matches!(self.framing, BodyFraming::UntilClose) =>
                    {
                        self.framing = BodyFraming::Completed;
                        return Ok(None);
                    }
                    Err(err) => return Err(err),
                }
            };

            self.read_with_the_head = false;

            let (data, framing) = take_data(
                &mut self.framing,
                head,
                &mut buffer[..size],
                self.print_input_http_stream,
            );

            let piece = body.hand_over(buffer, data)?;

            if let Err(err) = framing {
                // What has come before the framing broke is given first
                let Some(piece) = piece else {
                    return Err(err);
                };

                self.broken = Some(err);
                return Ok(Some(piece));
            }

            if let Some(piece) = piece {
                return Ok(Some(piece));
            }
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
            skipped += piece.len();

            if skipped > max_size {
                return false;
            }
        }

        self.is_completed()
    }
}

/// Where the data of the body is in `read`, which is what a read of the body has brought.
/// The framing goes on as far as the read gets it - or breaks, and the data which has
/// come before the break is there all the same
fn take_data(
    framing: &mut BodyFraming,
    head: &mut HeadBuffer,
    read: &mut [u8],
    print_input_http_stream: bool,
) -> (Range<usize>, Result<(), HttpParseError>) {
    match *framing {
        // Nothing more is read than what is left of the body
        BodyFraming::LengthBased(remains) => {
            let remains = remains - read.len();

            *framing = if remains == 0 {
                BodyFraming::Completed
            } else {
                BodyFraming::LengthBased(remains)
            };

            (0..read.len(), Ok(()))
        }
        BodyFraming::UntilClose => (0..read.len(), Ok(())),
        BodyFraming::Chunked(mode) => {
            let mut data = None;
            let next = take_chunked_data(mode, head, read, &mut data, print_input_http_stream);

            (data.unwrap_or(0..0), next.map(|next| *framing = next))
        }
        BodyFraming::Completed => (0..0, Ok(())),
    }
}

/// Goes over a read of a chunked body, over what frames the data: the sizes of the
/// chunks, the separators behind them, the trailers. The data of the chunks is moved
/// together, over what framed it, so it is one run of bytes - `data`, and it gives how
/// far the framing has got.
///
/// A line of the framing which the read before has cut is in `head`: the beginning of
/// this read completes it. What is left of this read past the data goes to `head` -
/// a line this read cuts, or the beginning of the next response once the body is over
fn take_chunked_data(
    mut mode: ChunksReadingMode,
    head: &mut HeadBuffer,
    read: &mut [u8],
    data: &mut Option<Range<usize>>,
    print_input_http_stream: bool,
) -> Result<BodyFraming, HttpParseError> {
    let mut pos = 0;

    if !head.is_empty() {
        let Some(taken) = head.complete_line(read)? else {
            // All of the read is a part of the line
            return Ok(BodyFraming::Chunked(mode));
        };

        let next = framing_line(mode, head.get_buf(), print_input_http_stream)?;
        head.consume(head.get_buf().len());
        pos = taken;

        match next {
            BodyFraming::Chunked(next) => mode = next,
            completed => {
                head.keep(&read[pos..])?;
                return Ok(completed);
            }
        }
    }

    let framing = loop {
        match mode {
            ChunksReadingMode::ReadingChunk(remains) => {
                let size = remains.min(read.len() - pos);

                if size == 0 {
                    break BodyFraming::Chunked(mode);
                }

                let chunk_data = pos..pos + size;

                *data = Some(match data.take() {
                    None => chunk_data,
                    // Behind the data of the chunks before, over what framed it
                    Some(data) => {
                        read.copy_within(chunk_data, data.end);
                        data.start..data.end + size
                    }
                });

                pos += size;

                mode = if size < remains {
                    ChunksReadingMode::ReadingChunk(remains - size)
                } else {
                    ChunksReadingMode::WaitingForSeparator
                };
            }
            _ => {
                let Some(line_size) = find_crlf(&read[pos..]) else {
                    // What is not a CRLF can not become one with the next read
                    if matches!(mode, ChunksReadingMode::WaitingForSeparator)
                        && !CRLF.starts_with(&read[pos..])
                    {
                        return Err(chunk_without_separator());
                    }

                    break BodyFraming::Chunked(mode);
                };

                let line = &read[pos..pos + line_size];
                pos += line_size + CRLF.len();

                match framing_line(mode, line, print_input_http_stream)? {
                    BodyFraming::Chunked(next) => mode = next,
                    completed => break completed,
                }
            }
        }
    };

    // What is left of the read: a line it cuts, or what comes after the body
    head.keep(&read[pos..])?;

    Ok(framing)
}

/// What a line of the framing of a chunked body moves the reading on to. `mode` says
/// what the line is
fn framing_line(
    mode: ChunksReadingMode,
    line: &[u8],
    print_input_http_stream: bool,
) -> Result<BodyFraming, HttpParseError> {
    let next = match mode {
        ChunksReadingMode::WaitingFroChunkSize => {
            let chunk_size = super::parse_chunk_size(line)?;

            if print_input_http_stream {
                println!("Read body chunk size: {}", chunk_size);
            }

            if chunk_size == 0 {
                ChunksReadingMode::WaitingForEnd
            } else {
                ChunksReadingMode::ReadingChunk(chunk_size)
            }
        }
        ChunksReadingMode::WaitingForSeparator => {
            if !line.is_empty() {
                return Err(chunk_without_separator());
            }

            ChunksReadingMode::WaitingFroChunkSize
        }
        // The trailers are read past: the body ends with an empty line
        ChunksReadingMode::WaitingForEnd => {
            if line.is_empty() {
                return Ok(BodyFraming::Completed);
            }

            ChunksReadingMode::WaitingForEnd
        }
        // The data of a chunk is not a line: it is taken as it is
        ChunksReadingMode::ReadingChunk(_) => mode,
    };

    Ok(BodyFraming::Chunked(next))
}

fn chunk_without_separator() -> HttpParseError {
    HttpParseError::invalid_payload("The data of a chunk is not followed by CRLF")
}
