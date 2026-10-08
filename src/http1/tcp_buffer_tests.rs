//! The buffer the socket is read into gives the pieces of a response body without
//! copying them: a piece shares the buffer it is cut out of. A buffer can not be read
//! into while a piece of it is alive, so there are two of them - the two of a
//! `DoubleBuffer` - used in turns. What is checked here is that a buffer is read into
//! again once its pieces are dropped and never before that, that a read waits for a
//! buffer while both are held, and that what is not consumed yet goes on to the buffer
//! which is read into next.

use std::{pin::pin, time::Duration};

use bytes::Bytes;

use crate::BodyChunk;

use super::{TcpBuffer, MAX_RESPONSE_BODY_PIECE_SIZE};

const BUFFER_SIZE: usize = MAX_RESPONSE_BODY_PIECE_SIZE;

const TIMEOUT: Duration = Duration::from_secs(5);

/// What a read of the socket does: appends `data` to what is not consumed yet, in a
/// buffer which is free
async fn read(buffer: &mut TcpBuffer, data: &[u8]) {
    let mut socket = data;

    buffer.read_from(&mut socket, TIMEOUT).await.unwrap();

    assert!(socket.is_empty(), "One read has taken all of it");
}

/// A read which brings a buffer of `byte`, taken as one piece
async fn read_a_buffer(buffer: &mut TcpBuffer, byte: u8) -> Bytes {
    read(buffer, &vec![byte; BUFFER_SIZE]).await;
    buffer.take(BUFFER_SIZE)
}

/// A piece which is all that is left of its buffer: the buffers it was read into are
/// gone
async fn the_last_piece_of_a_buffer(data: &[u8]) -> Bytes {
    let mut buffer = TcpBuffer::new();

    read(&mut buffer, data).await;
    buffer.take(data.len())
}

#[tokio::test]
async fn a_piece_is_not_a_copy_of_what_is_read() {
    let mut buffer = TcpBuffer::new();

    read(&mut buffer, b"HelloWorld").await;

    let what_is_read = buffer.get_buf().as_ptr();

    let hello = buffer.take(5);
    let world = buffer.take(5);

    assert_eq!(hello, "Hello");
    assert_eq!(world, "World");

    // Both are the very bytes the socket was read into
    assert_eq!(hello.as_ptr(), what_is_read);
    assert_eq!(world.as_ptr(), what_is_read.wrapping_add(5));

    assert!(buffer.is_empty());
}

/// The pieces of one buffer are read while the socket is read into the other, and by
/// the time that one is read the first is free again: two buffers serve a body of any
/// size
#[tokio::test]
async fn a_buffer_is_read_into_again_once_its_pieces_are_dropped() {
    let mut buffer = TcpBuffer::new();

    let first = read_a_buffer(&mut buffer, 1).await;
    let the_first_buffer = first.as_ptr();

    // The first buffer is with its reader: the socket is read into the other one
    let second = read_a_buffer(&mut buffer, 2).await;
    let the_second_buffer = second.as_ptr();

    assert_ne!(the_second_buffer, the_first_buffer);

    drop(first);
    drop(second);

    // A reader which takes the next piece, and only then lets go of the one it had:
    // the same two buffers go round
    let mut reading = read_a_buffer(&mut buffer, 3).await;
    let mut buffers = Vec::new();

    for turn in 0..10u8 {
        let next = read_a_buffer(&mut buffer, turn).await;

        buffers.push(reading.as_ptr());
        drop(reading);

        reading = next;
    }

    buffers.sort();
    buffers.dedup();

    assert_eq!(buffers.len(), 2);
    assert!(buffers.contains(&the_first_buffer));
    assert!(buffers.contains(&the_second_buffer));
}

/// A reader which keeps a piece keeps the buffer it is in: whatever is read after that
/// goes elsewhere, and the piece stays what it was
#[tokio::test]
async fn a_piece_which_is_kept_is_never_read_over() {
    let mut buffer = TcpBuffer::new();

    read(&mut buffer, b"kept").await;
    let kept = buffer.take(4);

    for turn in 0..100u8 {
        let piece = read_a_buffer(&mut buffer, turn).await;

        assert!(piece.iter().all(|byte| *byte == turn));
        assert_eq!(kept, "kept");
    }
}

/// A read takes a buffer as a whole, however little it brings: the rest of a buffer
/// is not read into while a piece of it is alive. Two small pieces which are kept hold
/// both buffers
#[tokio::test]
async fn every_read_takes_a_buffer_of_its_own() {
    let mut buffer = TcpBuffer::new();

    read(&mut buffer, b"Hello").await;
    let hello = buffer.take(5);

    read(&mut buffer, b"World").await;
    let world = buffer.take(5);

    assert_ne!(world.as_ptr(), hello.as_ptr().wrapping_add(5));

    let mut socket: &[u8] = b"!";
    let mut third = pin!(buffer.read_from(&mut socket, TIMEOUT));

    assert!(futures::poll!(third.as_mut()).is_pending());

    drop(hello);

    assert!(futures::poll!(third.as_mut()).is_ready());
}

/// With both buffers held there is nowhere to read into, and a read waits. Once one of
/// them is let go, it is the one which is read into
#[tokio::test]
async fn a_read_waits_for_a_buffer_while_both_are_held() {
    let mut buffer = TcpBuffer::new();

    let first = read_a_buffer(&mut buffer, 1).await;
    let the_first_buffer = first.as_ptr();

    let second = read_a_buffer(&mut buffer, 2).await;

    {
        let mut socket: &[u8] = b"third";
        let mut third = pin!(buffer.read_from(&mut socket, TIMEOUT));

        // However many times it is asked
        assert!(futures::poll!(third.as_mut()).is_pending());
        assert!(futures::poll!(third.as_mut()).is_pending());

        drop(first);

        let std::task::Poll::Ready(result) = futures::poll!(third.as_mut()) else {
            panic!("The read waits for a buffer which is free");
        };

        result.unwrap();
    }

    let third = buffer.take(5);

    assert_eq!(third, "third");
    assert_eq!(third.as_ptr(), the_first_buffer);
    assert!(second.iter().all(|byte| *byte == 2));
}

/// What is consumed does not hold its buffer: a buffer which is read to its end is free
/// to be read into again
#[tokio::test]
async fn what_is_consumed_holds_no_buffer() {
    let mut buffer = TcpBuffer::new();

    let kept = read_a_buffer(&mut buffer, 1).await;

    read(&mut buffer, b"line\r\n").await;
    let the_buffer_of_the_line = buffer.get_buf().as_ptr();

    assert_eq!(buffer.read_until_crlf().unwrap(), b"line");

    {
        let mut socket: &[u8] = b"next";
        let mut next = pin!(buffer.read_from(&mut socket, TIMEOUT));

        let std::task::Poll::Ready(result) = futures::poll!(next.as_mut()) else {
            panic!("The read waits for the buffer of what is consumed");
        };

        result.unwrap();
    }

    assert_eq!(buffer.get_buf(), b"next");
    assert_eq!(buffer.get_buf().as_ptr(), the_buffer_of_the_line);
    assert!(kept.iter().all(|byte| *byte == 1));
}

/// What is read and not consumed yet - a line which is not complete - goes on to the
/// buffer the socket is read into next
#[tokio::test]
async fn what_is_not_consumed_goes_on_to_the_next_buffer() {
    let mut buffer = TcpBuffer::new();

    // A buffer which is read full, with the beginning of a line at the end of it
    let mut data = vec![7u8; BUFFER_SIZE - 3];
    data.extend_from_slice(b"abc");

    read(&mut buffer, &data).await;

    let piece = buffer.take(BUFFER_SIZE - 3);

    assert!(buffer.read_until_crlf().is_none());

    read(&mut buffer, b"def\r\nthe rest").await;

    assert_eq!(buffer.read_until_crlf().unwrap(), b"abcdef");
    assert_eq!(buffer.get_buf(), b"the rest");

    // The buffer the line began in is with its reader, as it was
    assert!(piece.iter().all(|byte| *byte == 7));
}

/// A read which is given up loses nothing: what is not consumed is where it was, and
/// the buffer which was taken for the read is free again
#[tokio::test]
async fn a_read_which_is_given_up_loses_nothing() {
    let mut buffer = TcpBuffer::new();

    read(&mut buffer, b"abc").await;
    assert!(buffer.read_until_crlf().is_none());

    let (mut silent, _upstream) = tokio::io::duplex(64);

    {
        let mut given_up = pin!(buffer.read_from(&mut silent, TIMEOUT));
        assert!(futures::poll!(given_up.as_mut()).is_pending());
    }

    assert_eq!(buffer.get_buf(), b"abc");

    // `abc` holds one buffer, and the other one is free: the read has let go of it
    {
        let mut socket: &[u8] = b"def\r\n";
        let mut next = pin!(buffer.read_from(&mut socket, TIMEOUT));

        assert!(futures::poll!(next.as_mut()).is_ready());
    }

    assert_eq!(buffer.read_until_crlf().unwrap(), b"abcdef");
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

/// A head is read line by line, and a line which is read holds no buffer: a head of
/// any size goes through the two buffers, as long as each line fits into one
#[tokio::test]
async fn a_head_bigger_than_both_buffers_is_read_line_by_line() {
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

/// A line which does not fit into a buffer can not be read: it is refused
#[tokio::test]
async fn a_line_which_does_not_fit_into_a_buffer_is_refused() {
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

/// A piece taken as a `Vec` is a buffer of its own, with nothing but the data in it -
/// the piece which is the last one of its buffer as well
#[tokio::test]
async fn a_piece_taken_as_a_vec_does_not_take_its_buffer_along() {
    let data = BodyChunk::Raw(the_last_piece_of_a_buffer(b"Hello").await).into_vec();

    assert_eq!(data, b"Hello");
    assert!(data.capacity() < 100);

    let as_it_has_come = BodyChunk::Raw(the_last_piece_of_a_buffer(b"Hello").await).into_raw();

    assert_eq!(as_it_has_come, b"Hello");
    assert!(as_it_has_come.capacity() < 100);

    let piece = the_last_piece_of_a_buffer(b"5\r\nHello\r\n").await;
    let data = BodyChunk::chunked(piece, 3..8).into_vec();

    assert_eq!(data, b"Hello");
    assert!(data.capacity() < 100);

    let piece = the_last_piece_of_a_buffer(b"5\r\nHello\r\n").await;
    let as_it_has_come = BodyChunk::chunked(piece, 3..8).into_raw();

    assert_eq!(as_it_has_come, b"5\r\nHello\r\n");
    assert!(as_it_has_come.capacity() < 100);
}
