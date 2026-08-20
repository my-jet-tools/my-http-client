use std::time::Duration;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use tokio::io::{AsyncReadExt, ReadHalf};

use crate::http1::{HttpParseError, TcpBuffer, MAX_RESPONSE_BODY_SIZE};

/// Reads a close-delimited response body (RFC 9112 §6.3).
///
/// When a response has neither `Content-Length` nor `Transfer-Encoding`, its
/// body is terminated by the connection close. We drain anything already
/// buffered past the header block, then read from the socket until EOF.
///
/// A clean EOF (`read` returns `0`) is the *normal* terminator here — it
/// completes the body rather than signalling an error. The caller must not
/// return this connection to a keep-alive pool: it has been consumed by close.
pub async fn read_until_close<TStream: tokio::io::AsyncRead>(
    read_stream: &mut ReadHalf<TStream>,
    tcp_buffer: &mut TcpBuffer,
    builder: http::response::Builder,
    read_timeout: Duration,
) -> Result<http::Response<BoxBody<Bytes, String>>, HttpParseError> {
    let mut body: Vec<u8> = Vec::new();

    // Drain whatever the header parser already read into the buffer beyond the
    // header block — those bytes are the beginning of the body.
    let buffered_len = tcp_buffer.get_buf().len();
    if buffered_len > 0 {
        if let Some(buffered) = tcp_buffer.get_as_much_as_possible(buffered_len) {
            body.extend_from_slice(buffered);
        }
    }

    if body.len() > MAX_RESPONSE_BODY_SIZE {
        return Err(HttpParseError::invalid_payload(format!(
            "Close-delimited response body exceeds limit {}",
            MAX_RESPONSE_BODY_SIZE
        )));
    }

    // Heap-allocated so it does not inflate the spawned read-loop future's
    // stack frame (matches how `read_full_body` reads into a heap `Vec`).
    let mut read_buf = vec![0u8; 65536];

    loop {
        let future = read_stream.read(&mut read_buf);

        let result = tokio::time::timeout(read_timeout, future).await;

        if result.is_err() {
            return Err(HttpParseError::ReadingTimeout(read_timeout));
        }

        match result.unwrap() {
            Ok(0) => {
                // Connection closed cleanly: the body is complete.
                return Ok(crate::utils::into_body(builder, body));
            }
            Ok(read) => {
                body.extend_from_slice(&read_buf[..read]);

                if body.len() > MAX_RESPONSE_BODY_SIZE {
                    return Err(HttpParseError::invalid_payload(format!(
                        "Close-delimited response body exceeds limit {}",
                        MAX_RESPONSE_BODY_SIZE
                    )));
                }
            }
            Err(err) => {
                return Err(HttpParseError::error(format!(
                    "Error reading close-delimited body: {:?}",
                    err
                )));
            }
        }
    }
}
