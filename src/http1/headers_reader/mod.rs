use tokio::io::ReadHalf;

use super::*;

mod parse_http_response_first_line;
pub use parse_http_response_first_line::*;
mod parse_http_header;
pub use parse_http_header::*;

pub async fn read_headers<TStream: tokio::io::AsyncRead>(
    read_stream: &mut ReadHalf<TStream>,
    tcp_buffer: &mut TcpBuffer,
    read_timeout: Duration,
    print_input_http_stream: bool,
    request_method: Option<http::Method>,
) -> Result<BodyReader, HttpParseError> {
    let (status_code, version) = super::read_with_timeout::read_until_crlf(
        read_stream,
        tcp_buffer,
        read_timeout,
        parse_http_response_first_line,
        print_input_http_stream,
    )
    .await?;

    let mut builder = http::response::Builder::new()
        .status(status_code)
        .version(version);

    let mut detected_body_size = DetectedBodySize::Unknown;
    let mut headers_count: usize = 0;

    loop {
        let result = match tcp_buffer.read_until_crlf() {
            Some(line) => {
                if line.is_empty() {
                    break;
                }
                headers_count += 1;
                if headers_count > super::MAX_RESPONSE_HEADERS_COUNT {
                    return Err(HttpParseError::invalid_payload(format!(
                        "Response has more than {} headers",
                        super::MAX_RESPONSE_HEADERS_COUNT
                    )));
                }
                parse_http_header(builder, line)?
            }
            None => {
                super::read_with_timeout::read_to_buffer(
                    read_stream,
                    tcp_buffer,
                    read_timeout,
                    print_input_http_stream,
                )
                .await?;
                continue;
            }
        };

        builder = result.0;

        if !result.1.is_unknown() {
            detected_body_size = result.1;
        }
    }

    // RFC 9112 §6.3 body-length precedence. The two 101 arms come first: a 101 is
    // a 1xx status, but unlike a real interim response it switches the protocol
    // away from HTTP, so it must never be skipped as a bodyless message.
    let body_expected = response_has_body(request_method.as_ref(), status_code);

    match detected_body_size {
        #[cfg(feature = "with-websocket")]
        DetectedBodySize::WebSocketUpgrade => Ok(BodyReader::WebSocketUpgrade(
            WebSocketUpgradeBuilder::new(builder),
        )),
        // A 101 which is not handed over as a websocket: an upgrade to another
        // protocol (`Upgrade: h2c`), or any 101 when the crate is built without
        // the `with-websocket` feature. Read past as an interim response it would
        // hang the read loop until the read timeout waiting for a >= 200 answer
        // that the switched connection will never send.
        _ if status_code == http::StatusCode::SWITCHING_PROTOCOLS => {
            Ok(BodyReader::SwitchedProtocols { builder })
        }
        // A non-101 1xx is an interim response, not the final one. It must
        // be skipped without completing the request (RFC 9110 §15.2); the read
        // loop keeps reading for the real >= 200 response. Checked before the
        // bodyless arm below so a 1xx is *skipped* rather than *delivered empty*.
        _ if status_code.is_informational() => Ok(BodyReader::Interim),
        // HEAD / 204 / 304 / a 2xx to CONNECT never carry a body, regardless of
        // any Content-Length or Transfer-Encoding header.
        _ if !body_expected => Ok(BodyReader::LengthBased {
            builder,
            body_size: 0,
        }),
        DetectedBodySize::Chunked => {
            let (sender, response) = create_chunked_body_response(builder)?;
            Ok(BodyReader::Chunked { response, sender })
        }
        DetectedBodySize::Known(body_size) => Ok(BodyReader::LengthBased { builder, body_size }),
        // No length signal on a response that is allowed to have a body: the
        // body is delimited by connection close (read until EOF).
        DetectedBodySize::Unknown => Ok(BodyReader::UntilClose { builder }),
    }
}

/// Determines whether a response is allowed to carry a message body, per the
/// RFC 9112 §6.3 precedence rules that depend on the request method and the
/// response status code.
fn response_has_body(request_method: Option<&http::Method>, status: http::StatusCode) -> bool {
    // 1xx (informational), 204 (No Content) and 304 (Not Modified) never carry
    // a body, whatever headers say.
    if status.is_informational()
        || status == http::StatusCode::NO_CONTENT
        || status == http::StatusCode::NOT_MODIFIED
    {
        return false;
    }

    if let Some(method) = request_method {
        // A response to HEAD is identical to the GET response but with no body.
        if *method == http::Method::HEAD {
            return false;
        }

        // A 2xx response to CONNECT establishes a tunnel; no body follows.
        if *method == http::Method::CONNECT && status.is_success() {
            return false;
        }
    }

    true
}
