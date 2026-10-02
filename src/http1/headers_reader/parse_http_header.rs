use http::{HeaderName, HeaderValue};
use rust_extensions::slice_of_u8_utils::SliceOfU8Ext;

use crate::http1::{DetectedBodySize, HttpParseError};

/// How much of what the upstream has sent is repeated in the text of an error
const MAX_NAME_LEN_IN_ERROR: usize = 64;
const MAX_CONTENT_LENGTH_LEN_IN_ERROR: usize = 16;

pub fn parse_http_header(
    mut builder: http::response::Builder,
    src: &[u8],
) -> Result<(http::response::Builder, DetectedBodySize), HttpParseError> {
    let mut body_size = DetectedBodySize::Unknown;
    let Some(pos) = src.find_byte_pos(b':', 0) else {
        return Err(HttpParseError::invalid_payload(
            "Can not find separator between HTTP header and Http response",
        ));
    };

    let name = &src[..pos];
    let name = std::str::from_utf8(name).map_err(|_| {
        HttpParseError::invalid_payload(
            "Invalid HTTP header name. Can not convert payload to UTF8 string",
        )
    })?;

    // The builder is given the typed name and the typed value only, so it has nothing
    // to refuse: a name it does not take is reported here, as the reason the response
    // is not valid, instead of surfacing where the response is put together
    let header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
        HttpParseError::invalid_payload(format!(
            "Invalid HTTP header name: [{}]",
            head_of(name, MAX_NAME_LEN_IN_ERROR).escape_debug()
        ))
    })?;

    let value = &src[pos + 1..];
    let value_str = std::str::from_utf8(value).map_err(|_| {
        HttpParseError::invalid_payload(
            "Invalid HTTP value. Can not convert payload to UTF8 string",
        )
    })?;

    let value_str = value_str.trim();

    if name.eq_ignore_ascii_case("Content-Length") {
        match value_str.parse() {
            Ok(value) => body_size = DetectedBodySize::Known(value),
            Err(_) => {
                return Err(HttpParseError::invalid_payload(format!(
                    "Invalid Content-Length value: {}",
                    head_of(value_str, MAX_CONTENT_LENGTH_LEN_IN_ERROR)
                )));
            }
        }
    }

    if name.eq_ignore_ascii_case("Transfer-Encoding")
        && value_str.eq_ignore_ascii_case("chunked")
    {
        body_size = DetectedBodySize::Chunked;
    }

    #[cfg(feature = "with-websocket")]
    {
        if name.eq_ignore_ascii_case("upgrade") && value_str.eq_ignore_ascii_case("websocket") {
            body_size = DetectedBodySize::WebSocketUpgrade;
        }
    }

    let header_value = HeaderValue::from_str(value_str).map_err(|err| {
        HttpParseError::invalid_payload(format!(
            "Invalid Header value. {}: {}. Err: {}",
            name, value_str, err
        ))
    })?;

    builder = builder.header(header_name, header_value);

    Ok((builder, body_size))
}

/// The beginning of `src`, `max_len` bytes at most and never a part of a char: the cut
/// moves back to the nearest char boundary
fn head_of(src: &str, max_len: usize) -> &str {
    if src.len() <= max_len {
        return src;
    }

    let mut end = max_len;
    while !src.is_char_boundary(end) {
        end -= 1;
    }

    &src[..end]
}
