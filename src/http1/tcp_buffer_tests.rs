//! What the connection is read into. The heads go into a buffer of their own, line by
//! line. A body goes into the two buffers of a `DoubleBuffer`, a read at a time, and a
//! piece of it is the data of a read with what framed it cut off - not a copy: a buffer
//! is not read into again until its piece is dropped. What of a read is not the body - a
//! line of the framing the read has cut, the beginning of the next response - goes to
//! the buffer of the heads.

use std::{collections::VecDeque, pin::pin, time::Duration};

use tokio::io::ReadHalf;

use super::{
    BodyFraming, ChunksReadingMode, HttpParseError, ResponseBodyOnTheWire, TcpBuffer,
    MAX_RESPONSE_BODY_PIECE_SIZE,
};

const BUFFER_SIZE: usize = MAX_RESPONSE_BODY_PIECE_SIZE;

const TIMEOUT: Duration = Duration::from_secs(5);

/// What the upstream says, a read at a time: a read gets the next of `reads` - as much of
/// it as there is room for, and the rest goes to the read after. Then the upstream is
/// silent, or it closes the connection
struct Reads {
    reads: VecDeque<Vec<u8>>,
    then_close: bool,
}

impl tokio::io::AsyncRead for Reads {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let Some(read) = self.reads.front_mut() else {
            return match self.then_close {
                true => std::task::Poll::Ready(Ok(())),
                false => std::task::Poll::Pending,
            };
        };

        let size = read.len().min(buf.remaining());

        buf.put_slice(&read[..size]);
        read.drain(..size);

        if read.is_empty() {
            self.reads.pop_front();
        }

        std::task::Poll::Ready(Ok(()))
    }
}

type Wire = tokio::io::Join<Reads, tokio::io::Sink>;

fn wire(reads: Vec<Vec<u8>>, then_close: bool) -> ReadHalf<Wire> {
    let reads = Reads {
        reads: reads.into(),
        then_close,
    };

    tokio::io::split(tokio::io::join(reads, tokio::io::sink())).0
}

fn wire_of(reads: &[&[u8]]) -> ReadHalf<Wire> {
    wire(reads.iter().map(|read| read.to_vec()).collect(), false)
}

fn chunked() -> BodyFraming {
    BodyFraming::Chunked(ChunksReadingMode::WaitingFroChunkSize)
}

/// The pieces of `body`, to its end, copied out as they come
async fn the_pieces(
    body: &mut ResponseBodyOnTheWire<'_, Wire>,
) -> Result<Vec<Vec<u8>>, HttpParseError> {
    let mut result = Vec::new();

    while let Some(piece) = body.next_piece().await? {
        result.push(piece.to_vec());
    }

    Ok(result)
}

/// The pieces of a body which comes off `reads`, with nothing read along with its head
async fn body_of(reads: &[&[u8]], framing: BodyFraming) -> Result<Vec<Vec<u8>>, HttpParseError> {
    let mut wire = wire_of(reads);
    let mut buffer = TcpBuffer::new();

    let mut body = ResponseBodyOnTheWire::new(&mut wire, &mut buffer, framing, TIMEOUT, false);

    the_pieces(&mut body).await
}

/// What a read of the socket for a head does: appends `data` to what is not consumed yet
async fn read(buffer: &mut TcpBuffer, data: &[u8]) {
    let mut socket = data;

    buffer.read_from(&mut socket, TIMEOUT).await.unwrap();

    assert!(socket.is_empty(), "One read has taken all of it");
}

/// A line is taken without the CRLF it ends with, and the next one begins behind it
#[tokio::test]
async fn the_lines_are_taken_one_by_one() {
    let mut buffer = TcpBuffer::new();

    read(&mut buffer, b"first\r\n\r\nthird\r\nnot comp").await;

    assert_eq!(buffer.read_until_crlf().unwrap(), b"first");
    assert_eq!(buffer.read_until_crlf().unwrap(), b"");
    assert_eq!(buffer.read_until_crlf().unwrap(), b"third");
    assert!(buffer.read_until_crlf().is_none());

    assert!(buffer.skip_exactly(4).is_ok());
    assert_eq!(buffer.get_buf(), b"comp");

    assert!(buffer.skip_exactly(5).is_err());
    assert_eq!(buffer.get_buf(), b"comp");
}

/// A head is read line by line, and a line which is read takes no room any more: a head
/// of any size goes through the buffer of the heads, as long as each line fits into it
#[tokio::test]
async fn a_head_bigger_than_the_buffer_is_read_line_by_line() {
    let mut buffer = TcpBuffer::new();

    let line = [b'a'; 999];
    let mut head = Vec::new();

    for _ in 0..1000 {
        head.extend_from_slice(&line);
        head.extend_from_slice(b"\r\n");
    }

    let mut lines = 0;

    for part in head.chunks(7000) {
        read(&mut buffer, part).await;

        while let Some(read_line) = buffer.read_until_crlf() {
            assert_eq!(read_line, line);
            lines += 1;
        }
    }

    assert_eq!(lines, 1000);
    assert!(buffer.is_empty());
}

/// A line which does not fit into the buffer can not be read: it is refused
#[tokio::test]
async fn a_line_which_does_not_fit_into_the_buffer_is_refused() {
    let mut buffer = TcpBuffer::new();

    let mut refused = false;

    for _ in 0..BUFFER_SIZE {
        let mut socket: &[u8] = &[b'a'; 1000];

        if let Err(err) = buffer.read_from(&mut socket, TIMEOUT).await {
            assert_eq!(
                err.as_invalid_payload(),
                Some("Write Buffer is too small to read http headers. Size: [65536]")
            );

            refused = true;
            break;
        }

        assert!(buffer.read_until_crlf().is_none());
    }

    assert!(refused);
}

/// A read of a head which is given up loses nothing: what is not consumed is where it was
#[tokio::test]
async fn a_read_of_a_head_which_is_given_up_loses_nothing() {
    let mut buffer = TcpBuffer::new();

    read(&mut buffer, b"abc").await;
    assert!(buffer.read_until_crlf().is_none());

    let (mut silent, _upstream) = tokio::io::duplex(64);

    {
        let mut given_up = pin!(buffer.read_from(&mut silent, TIMEOUT));
        assert!(futures::poll!(given_up.as_mut()).is_pending());
    }

    assert_eq!(buffer.get_buf(), b"abc");

    read(&mut buffer, b"def\r\n").await;

    assert_eq!(buffer.read_until_crlf().unwrap(), b"abcdef");
}

/// A piece of a body is what a read has brought: nothing waits for the body to be
/// complete, and nothing is put together
#[tokio::test]
async fn a_piece_is_what_a_read_has_brought() {
    let pieces = body_of(&[b"Hello", b"World"], BodyFraming::LengthBased(10)).await;
    assert_eq!(pieces.unwrap(), [b"Hello", b"World"]);

    let pieces = body_of(&[b"Hello", b"World"], BodyFraming::UntilClose).await;
    assert!(matches!(pieces, Err(HttpParseError::ReadingTimeout(_))));
}

/// A close-delimited body is over when the connection is closed
#[tokio::test]
async fn a_close_delimited_body_ends_with_the_connection() {
    let mut wire = wire(vec![b"Hello".to_vec(), b"World".to_vec()], true);
    let mut buffer = TcpBuffer::new();

    let mut body = ResponseBodyOnTheWire::new(
        &mut wire,
        &mut buffer,
        BodyFraming::UntilClose,
        TIMEOUT,
        false,
    );

    assert_eq!(the_pieces(&mut body).await.unwrap(), [b"Hello", b"World"]);
    assert!(body.is_completed());
}

/// The pieces are not copied out of the buffers the body is read into: a body of any
/// size goes through the same two buffers, and a buffer is read into again once its
/// piece is dropped
#[tokio::test]
async fn a_body_goes_through_the_same_two_buffers() {
    let reads: Vec<Vec<u8>> = (0..20u8).map(|turn| vec![turn; BUFFER_SIZE]).collect();

    let mut wire = wire(reads, false);
    let mut buffer = TcpBuffer::new();

    let mut body = ResponseBodyOnTheWire::new(
        &mut wire,
        &mut buffer,
        BodyFraming::LengthBased(20 * BUFFER_SIZE),
        TIMEOUT,
        false,
    );

    // The reader has a piece in its hands all the time: it takes the next one, and
    // only then lets go of the one it had
    let mut reading = body.next_piece().await.unwrap().unwrap();
    let mut buffers = Vec::new();

    for turn in 1..20u8 {
        let next = body.next_piece().await.unwrap().unwrap();

        assert_eq!(next.len(), BUFFER_SIZE);
        assert!(next.iter().all(|byte| *byte == turn));

        buffers.push(reading.as_ptr());
        reading = next;
    }

    buffers.sort();
    buffers.dedup();

    assert_eq!(buffers.len(), 2);
}

/// While both buffers are held there is nowhere to read into: the next piece waits for
/// one of them. Given up while it waits it loses nothing - the socket is not read
#[tokio::test]
async fn the_next_piece_waits_for_a_buffer_while_both_are_held() {
    let mut wire = wire_of(&[b"Hello", b"World", b"Again"]);
    let mut buffer = TcpBuffer::new();

    let mut body = ResponseBodyOnTheWire::new(
        &mut wire,
        &mut buffer,
        BodyFraming::LengthBased(15),
        TIMEOUT,
        false,
    );

    let hello = body.next_piece().await.unwrap().unwrap();
    let world = body.next_piece().await.unwrap().unwrap();

    {
        let mut given_up = pin!(body.next_piece());
        assert!(futures::poll!(given_up.as_mut()).is_pending());
    }

    drop(hello);

    assert_eq!(&*body.next_piece().await.unwrap().unwrap(), b"Again");
    assert_eq!(&*world, b"World");
}

/// What is read along with the head, past its end, is the first piece of the body - as
/// much of it as is the body. The rest is the next response: it stays for its head
#[tokio::test]
async fn the_body_begins_with_what_is_read_with_the_head() {
    let mut wire = wire_of(&[b"lo"]);
    let mut buffer = TcpBuffer::new();

    read(&mut buffer, b"Hel").await;

    let mut body = ResponseBodyOnTheWire::new(
        &mut wire,
        &mut buffer,
        BodyFraming::LengthBased(5),
        TIMEOUT,
        false,
    );

    assert_eq!(the_pieces(&mut body).await.unwrap(), [&b"Hel"[..], b"lo"]);

    let mut wire = wire_of(&[]);
    let mut buffer = TcpBuffer::new();

    read(&mut buffer, b"HelloHTTP/1.1 200 OK\r\n").await;

    let mut body = ResponseBodyOnTheWire::new(
        &mut wire,
        &mut buffer,
        BodyFraming::LengthBased(5),
        TIMEOUT,
        false,
    );

    assert_eq!(the_pieces(&mut body).await.unwrap(), [b"Hello"]);
    assert_eq!(buffer.get_buf(), b"HTTP/1.1 200 OK\r\n");
}

/// A body which says its size is not read past its end: the next response stays on the
/// wire, for its head to be read into the buffer of the heads
#[tokio::test]
async fn nothing_past_the_end_of_a_body_of_a_known_size_is_read() {
    let mut wire = wire_of(&[b"HelloHTTP/1.1 200 OK\r\n"]);
    let mut buffer = TcpBuffer::new();

    let mut body = ResponseBodyOnTheWire::new(
        &mut wire,
        &mut buffer,
        BodyFraming::LengthBased(5),
        TIMEOUT,
        false,
    );

    assert_eq!(the_pieces(&mut body).await.unwrap(), [b"Hello"]);
    assert!(buffer.is_empty());

    buffer.read_from(&mut wire, TIMEOUT).await.unwrap();
    assert_eq!(buffer.read_until_crlf().unwrap(), b"HTTP/1.1 200 OK");
}

/// The chunks which come in one read are one piece: their data is moved together, over
/// what framed it, and the sizes of the chunks, their separators, the chunk which ends
/// the body and the trailers are cut off
#[tokio::test]
async fn the_chunks_of_a_read_are_one_piece_of_data() {
    let pieces = body_of(
        &[b"5\r\nHello\r\n6;ext=1\r\n World\r\n1\r\n!\r\n0\r\nX-Checksum: 1\r\n\r\n"],
        chunked(),
    )
    .await;

    assert_eq!(pieces.unwrap(), [b"Hello World!"]);
}

/// A chunk does not have to be complete to be given: its data comes as it is read
#[tokio::test]
async fn a_chunk_which_comes_in_parts_is_given_in_parts() {
    let pieces = body_of(&[b"a\r\nHello", b"World\r\n0\r\n\r\n"], chunked()).await;

    assert_eq!(pieces.unwrap(), [b"Hello", b"World"]);
}

/// A read with nothing but the framing in it is no piece: there is no piece without data
#[tokio::test]
async fn a_read_of_nothing_but_the_framing_is_no_piece() {
    let pieces = body_of(&[b"5\r\n", b"Hello", b"\r\n", b"0\r\n", b"\r\n"], chunked()).await;

    assert_eq!(pieces.unwrap(), [b"Hello"]);
}

/// A line of the framing which a read cuts is completed with the read after it,
/// wherever the cut is - in the size of a chunk, between the CR and the LF of its line,
/// in the separator behind the data, in the trailers
#[tokio::test]
async fn a_line_of_the_framing_is_completed_with_the_next_read() {
    let cuts: [&[&[u8]]; 5] = [
        &[b"1", b"0\r\n0123456789abcdef\r\n0\r\n\r\n"],
        &[b"10\r", b"\n0123456789abcdef\r\n0\r\n\r\n"],
        &[b"10\r\n0123456789abcdef\r", b"\n0\r\n\r\n"],
        &[
            b"10\r\n0123456789abcdef\r\n0\r\nX-Che",
            b"cksum: 1\r",
            b"\n\r",
            b"\n",
        ],
        &[
            b"1",
            b"0",
            b"\r",
            b"\n",
            b"0123456789abcdef",
            b"\r",
            b"\n",
            b"0",
            b"\r\n\r\n",
        ],
    ];

    for reads in cuts {
        let pieces = body_of(reads, chunked()).await;

        assert_eq!(
            pieces.unwrap().concat(),
            b"0123456789abcdef",
            "Read as {:?}",
            reads
        );
    }
}

/// The data of a chunk is followed by CRLF, and by nothing else
#[tokio::test]
async fn a_chunk_which_is_not_followed_by_crlf_is_refused() {
    let cuts: [&[&[u8]]; 3] = [
        &[b"5\r\nHelloX\r\n0\r\n\r\n"],
        &[b"5\r\nHello\rX\n0\r\n\r\n"],
        &[b"5\r\nHello\r", b"X\r\n0\r\n\r\n"],
    ];

    for reads in cuts {
        let Err(err) = body_of(reads, chunked()).await else {
            panic!("A body with no separator is taken: {:?}", reads);
        };

        assert_eq!(
            err.as_invalid_payload(),
            Some("The data of a chunk is not followed by CRLF")
        );
    }
}

/// A line of the framing which does not fit into the buffer of the heads is refused
#[tokio::test]
async fn a_line_of_the_framing_which_does_not_fit_is_refused() {
    let mut line = b"5;".to_vec();
    line.extend_from_slice(&[b'x'; 2 * BUFFER_SIZE]);

    let Err(err) = body_of(&[&line, b"\r\nHello\r\n0\r\n\r\n"], chunked()).await else {
        panic!("A line bigger than the buffer is taken");
    };

    assert_eq!(
        err.as_invalid_payload(),
        Some("A line of the body framing does not fit into the buffer of [65536] bytes")
    );
}

/// What comes behind the end of a chunked body is the next response: it stays for its
/// head - whether it comes in the read which ends the body or along with the head
#[tokio::test]
async fn what_comes_after_a_chunked_body_stays_for_the_next_head() {
    let mut wire = wire_of(&[b"5\r\nHello\r\n0\r\n\r\nHTTP/1.1 200 OK\r\n"]);
    let mut buffer = TcpBuffer::new();

    let mut body = ResponseBodyOnTheWire::new(&mut wire, &mut buffer, chunked(), TIMEOUT, false);

    assert_eq!(the_pieces(&mut body).await.unwrap(), [b"Hello"]);
    assert_eq!(buffer.get_buf(), b"HTTP/1.1 200 OK\r\n");

    let mut wire = wire_of(&[]);
    let mut buffer = TcpBuffer::new();

    read(&mut buffer, b"5\r\nHello\r\n0\r\n\r\nHTTP/1.1 200 OK\r\n").await;

    let mut body = ResponseBodyOnTheWire::new(&mut wire, &mut buffer, chunked(), TIMEOUT, false);

    assert_eq!(the_pieces(&mut body).await.unwrap(), [b"Hello"]);
    assert_eq!(buffer.get_buf(), b"HTTP/1.1 200 OK\r\n");
}

/// A chunked body is not over until its last chunk: the connection which is closed
/// before it cuts the body short
#[tokio::test]
async fn a_chunked_body_which_is_cut_short_is_an_error() {
    let mut wire = wire(vec![b"5\r\nHello\r\n".to_vec()], true);
    let mut buffer = TcpBuffer::new();

    let mut body = ResponseBodyOnTheWire::new(&mut wire, &mut buffer, chunked(), TIMEOUT, false);

    assert_eq!(&*body.next_piece().await.unwrap().unwrap(), b"Hello");
    assert!(matches!(
        body.next_piece().await,
        Err(HttpParseError::Disconnected)
    ));
}

/// The framing which breaks in the middle of a read does not take the data before the
/// break along: it is given, and the break comes after it
#[tokio::test]
async fn the_data_before_a_broken_framing_is_given_first() {
    let mut wire = wire_of(&[b"5\r\nHello\r\nxyz\r\n"]);
    let mut buffer = TcpBuffer::new();

    let mut body = ResponseBodyOnTheWire::new(&mut wire, &mut buffer, chunked(), TIMEOUT, false);

    assert_eq!(&*body.next_piece().await.unwrap().unwrap(), b"Hello");

    let Err(err) = body.next_piece().await else {
        panic!("A broken framing is taken");
    };

    assert_eq!(
        err.as_invalid_payload(),
        Some("Invalid chunk size: \"xyz\"")
    );
}
