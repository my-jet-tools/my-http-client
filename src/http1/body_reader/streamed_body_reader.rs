use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, ReadHalf};

use crate::http1::{HttpParseError, TcpBuffer};

use super::ChunksSender;

/// A response body with a `Content-Length` above this size is not read into memory
/// before the response is handed over: the head goes to the caller at once and the
/// body follows as a stream, in frames of at most this size. Smaller bodies are read
/// whole, as before. A close-delimited body always streams - nothing says how big it is.
///
/// What a streamed body may grow to is up to whoever reads it: the client holds no more
/// of it than [`STREAMED_BODY_CHANNEL_CAPACITY`] frames waiting for the reader.
pub const STREAMED_BODY_THRESHOLD: usize = 64 * 1024;

/// How many frames of a streamed body may wait for the reader before the client stops
/// reading the socket.
pub const STREAMED_BODY_CHANNEL_CAPACITY: usize = 16;

/// The response with a body which is still on its way: its frames come through the
/// sender. Fails when the builder carries an error - a head of the response it did not
/// take
pub fn create_streamed_body_response(
    builder: http::response::Builder,
) -> Result<(ChunksSender, crate::HyperResponse), http::Error> {
    super::create_body_response(builder, STREAMED_BODY_CHANNEL_CAPACITY)
}

/// Reads a body of `body_size` bytes off the socket and hands it over through `sender`.
///
/// As with a chunked body, the head is with the caller already, so a failure goes down
/// the body as its last item. A reader who gives up on the body - drops it - stops the
/// reading too: the rest of the body is left on the wire, so the connection is over.
pub async fn read_length_based_body<TStream: tokio::io::AsyncRead>(
    read_stream: &mut ReadHalf<TStream>,
    tcp_buffer: &mut TcpBuffer,
    mut sender: ChunksSender,
    body_size: usize,
    read_timeout: Duration,
) -> Result<(), HttpParseError> {
    let mut remains_to_read = body_size;

    let result = async {
        while remains_to_read > 0 {
            let frame_size = remains_to_read.min(STREAMED_BODY_THRESHOLD);

            let frame = match tcp_buffer.get_as_much_as_possible(frame_size) {
                Some(buffered) => buffered.to_vec(),
                None => read_some(read_stream, frame_size, read_timeout)
                    .await?
                    .ok_or(HttpParseError::Disconnected)?,
            };

            remains_to_read -= frame.len();
            send_frame(&mut sender, frame).await?;
        }

        Ok(())
    }
    .await;

    report_failure(&mut sender, "response", result).await
}

/// Reads a close-delimited body (RFC 9112 §6.3) off the socket until EOF and hands it
/// over through `sender`. The EOF is what completes the body; the connection is consumed
/// by it and must not be reused.
pub async fn read_body_until_close<TStream: tokio::io::AsyncRead>(
    read_stream: &mut ReadHalf<TStream>,
    tcp_buffer: &mut TcpBuffer,
    mut sender: ChunksSender,
    read_timeout: Duration,
) -> Result<(), HttpParseError> {
    let result = async {
        loop {
            let frame = match tcp_buffer.get_as_much_as_possible(STREAMED_BODY_THRESHOLD) {
                Some(buffered) => buffered.to_vec(),
                None => match read_some(read_stream, STREAMED_BODY_THRESHOLD, read_timeout).await? {
                    Some(frame) => frame,
                    None => return Ok(()),
                },
            };

            send_frame(&mut sender, frame).await?;
        }
    }
    .await;

    report_failure(&mut sender, "close-delimited", result).await
}

/// One read off the socket, of at most `max_size` bytes. `None` is the EOF
async fn read_some<TStream: tokio::io::AsyncRead>(
    read_stream: &mut ReadHalf<TStream>,
    max_size: usize,
    read_timeout: Duration,
) -> Result<Option<Vec<u8>>, HttpParseError> {
    let mut frame = vec![0u8; max_size];

    let Ok(result) = tokio::time::timeout(read_timeout, read_stream.read(&mut frame)).await else {
        return Err(HttpParseError::ReadingTimeout(read_timeout));
    };

    match result {
        Ok(0) => Ok(None),
        Ok(read) => {
            frame.truncate(read);
            Ok(Some(frame))
        }
        Err(err) => Err(HttpParseError::error(format!(
            "Error reading response body: {:?}",
            err
        ))),
    }
}

async fn send_frame(sender: &mut ChunksSender, frame: Vec<u8>) -> Result<(), HttpParseError> {
    use futures::SinkExt;

    sender
        .send(Ok(hyper::body::Frame::data(Bytes::from(frame))))
        .await
        .map_err(|err| HttpParseError::error(format!("Error sending response body: {:?}", err)))
}

async fn report_failure(
    sender: &mut ChunksSender,
    body: &str,
    result: Result<(), HttpParseError>,
) -> Result<(), HttpParseError> {
    use futures::SinkExt;

    if let Err(err) = &result {
        // Nobody is reading the body when this fails, and then there is nobody to tell
        let _ = sender
            .send(Err(super::why_the_body_is_not_complete(body, err)))
            .await;
    }

    result
}
