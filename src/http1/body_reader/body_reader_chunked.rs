use std::time::Duration;

use bytes::Bytes;

use http_body_util::{BodyExt, StreamBody};
use tokio::io::ReadHalf;

use crate::http1::{HttpParseError, TcpBuffer, MAX_CHUNK_SIZE};

/// How much of a chunk size line is repeated in the text of an error
const MAX_CHUNK_SIZE_LEN_IN_ERROR: usize = 16;

#[derive(Debug, Clone, Copy)]
pub enum ChunksReadingMode {
    WaitingFroChunkSize,
    ReadingChunk(usize),
    WaitingForSeparator,
    WaitingForEnd,
}

/// The chunks of a response body on their way to whoever reads it. The error is the
/// reason the body is over before its terminating chunk: it is what tells a body which
/// is cut short from a body which is complete
pub type ChunksSender = futures::channel::mpsc::Sender<Result<hyper::body::Frame<Bytes>, String>>;

/// Fails when the builder carries an error - a head of the response it did not take
pub fn create_chunked_body_response(
    builder: http::response::Builder,
) -> Result<(ChunksSender, crate::HyperResponse), http::Error> {
    create_body_response(builder, 1024)
}

/// The response with a body which comes through the returned sender, `capacity` frames
/// of it at most waiting for the reader
pub(crate) fn create_body_response(
    builder: http::response::Builder,
    capacity: usize,
) -> Result<(ChunksSender, crate::HyperResponse), http::Error> {
    let (sender, receiver) = futures::channel::mpsc::channel(capacity);
    let stream_body = StreamBody::new(receiver);

    let response = builder.body(stream_body.boxed())?;
    Ok((sender, response))
}

/// Reads the chunks off the socket and hands them over through `sender`.
///
/// The head of the response is already with the caller by the time the body is read, so
/// a failure can not fail the request any more. It is sent down the body instead, as
/// its last item - otherwise the body would just end, and what was read before the
/// failure would look like the whole of it
pub async fn read_chunked_body<TStream: tokio::io::AsyncRead>(
    read_stream: &mut ReadHalf<TStream>,
    tcp_buffer: &mut TcpBuffer,
    mut sender: ChunksSender,
    read_timeout: Duration,
    print_input_http_stream: bool,
) -> Result<(), HttpParseError> {
    use futures::SinkExt;

    let result = read_chunks(
        read_stream,
        tcp_buffer,
        &mut sender,
        read_timeout,
        print_input_http_stream,
    )
    .await;

    if let Err(err) = &result {
        // Nobody is reading the body when this fails, and then there is nobody to tell
        let _ = sender
            .send(Err(why_the_body_is_not_complete("chunked", err)))
            .await;
    }

    result
}

async fn read_chunks<TStream: tokio::io::AsyncRead>(
    read_stream: &mut ReadHalf<TStream>,
    tcp_buffer: &mut TcpBuffer,
    sender: &mut ChunksSender,
    read_timeout: Duration,
    print_input_http_stream: bool,
) -> Result<(), HttpParseError> {
    use futures::SinkExt;
    loop {
        let chunk_size = super::super::read_with_timeout::read_until_crlf(
            read_stream,
            tcp_buffer,
            read_timeout,
            parse_chunk_size,
            print_input_http_stream,
        )
        .await?;

        if print_input_http_stream {
            println!("Read body chunk size: {}", chunk_size);
        }

        if chunk_size == 0 {
            super::super::read_with_timeout::skip_exactly(
                read_stream,
                tcp_buffer,
                2,
                read_timeout,
                print_input_http_stream,
            )
            .await?;

            return Ok(());
        }

        if chunk_size > MAX_CHUNK_SIZE {
            return Err(HttpParseError::invalid_payload(format!(
                "Chunk size {} exceeds limit {}",
                chunk_size, MAX_CHUNK_SIZE
            )));
        }

        let mut chunk: Vec<u8> = vec![0u8; chunk_size];

        let mut read_amount = 0;

        if let Some(remains_in_buffer) = tcp_buffer.get_as_much_as_possible(chunk_size) {
            chunk[..remains_in_buffer.len()].copy_from_slice(remains_in_buffer);
            read_amount += remains_in_buffer.len();
        }

        let remains_to_read = chunk_size - read_amount;

        if remains_to_read > 0 {
            super::super::read_with_timeout::read_exact(
                read_stream,
                &mut chunk[read_amount..],
                read_timeout,
            )
            .await?;
        }

        let err = sender
            .send(Ok(hyper::body::Frame::data(chunk.into())))
            .await;

        if let Err(err) = err {
            return Err(HttpParseError::error(format!(
                "Error sending response chunk: {:?}",
                err
            )));
        }

        super::super::read_with_timeout::skip_exactly(
            read_stream,
            tcp_buffer,
            2,
            read_timeout,
            print_input_http_stream,
        )
        .await?;
    }
}

/// The last item of a streamed body cut short. `body` names which body it is
pub(crate) fn why_the_body_is_not_complete(body: &str, err: &HttpParseError) -> String {
    let reason = match err {
        HttpParseError::InvalidHttpPayload(reason) => reason.as_str().to_string(),
        HttpParseError::Error(reason) => reason.as_str().to_string(),
        HttpParseError::Disconnected => "the connection is closed".to_string(),
        HttpParseError::ReadingTimeout(timeout) => {
            format!("no data from the upstream for {:?}", timeout)
        }
        HttpParseError::GetMoreData => "the rest of it has not arrived".to_string(),
    };

    format!("The {} body is not complete: {}", body, reason)
}

fn parse_chunk_size(src: &[u8]) -> Result<usize, HttpParseError> {
    let mut end_of_hex = src.len();

    for (i, &byte) in src.iter().enumerate() {
        if !byte.is_ascii_hexdigit() {
            end_of_hex = i;
            break;
        }
    }

    if end_of_hex == 0 {
        // The line is whatever the upstream has sent: it does not have to be UTF-8 and
        // it can be as long as the read buffer
        let shown = &src[..src.len().min(MAX_CHUNK_SIZE_LEN_IN_ERROR)];

        return Err(HttpParseError::invalid_payload(format!(
            "Invalid chunk size: {:?}",
            String::from_utf8_lossy(shown)
        )));
    }

    let hex_str = std::str::from_utf8(&src[0..end_of_hex])
        .map_err(|_| HttpParseError::invalid_payload("Invalid UTF-8 in chunk size"))?;

    usize::from_str_radix(hex_str, 16).map_err(|_| {
        HttpParseError::invalid_payload(format!("Can not parse chunk size: {}", hex_str))
    })
}
