mod my_http_client;
use std::time::Duration;

pub use my_http_client::*;
mod detected_body_size;
pub use detected_body_size::*;
mod my_http_client_inner;
pub use my_http_client_inner::*;
mod queue_of_requests;
mod read_loop;
mod write_loop;
pub use queue_of_requests::*;
mod my_http_response;
pub use my_http_response::*;

mod my_http_request;
pub use my_http_request::*;
mod my_http_request_builder;
mod my_http_request_builder_inner;

mod my_http_client_connection_context;
pub use my_http_client_connection_context::*;

pub use my_http_request_builder::*;

mod tcp_buffer;
use rust_extensions::StrOrString;
pub use tcp_buffer::*;

mod body_reader;
pub use body_reader::*;
mod headers_reader;
pub use headers_reader::*;

mod my_http_client_metrics;
pub use my_http_client_metrics::*;

mod read_with_timeout;
pub use read_with_timeout::*;

pub mod into_hyper_request;

#[cfg(test)]
mod response_framing_tests;

#[cfg(test)]
mod streaming_body_tests;

#[cfg(test)]
mod request_input_tests;

#[cfg(test)]
mod response_input_tests;

#[cfg(test)]
mod response_body_reader_tests;

#[cfg(test)]
mod contract_tests;

const CONTENT_LENGTH_HEADER_NAME: &str = "content-length";
const TRANSFER_ENCODING_HEADER_NAME: &str = "transfer-encoding";

/// The biggest piece a response body is sent to its reader in. What a read() of the
/// socket has brought is sent as it is, unless it is bigger than that
pub const MAX_RESPONSE_BODY_PIECE_SIZE: usize = 64 * 1024;

/// How much of a response body nobody is going to read - its reader is dropped before
/// the end of it - is read past to keep the connection. A connection with more than
/// that left on the wire is closed: dialing a new one is cheaper than downloading it
pub const MAX_ABANDONED_BODY_SIZE: usize = 1024 * 1024;

/// How long the rest of an abandoned body is waited for. The requests which are sent
/// meanwhile are behind that body on the wire, so a body which does not end by then -
/// an event stream, a slow upstream - costs them the connection instead of their time
pub const ABANDONED_BODY_SKIP_TIMEOUT: Duration = Duration::from_secs(1);
pub const MAX_RESPONSE_HEADERS_COUNT: usize = 256;

/// Upper bound on consecutive interim (1xx) responses accepted before a final
/// response, guarding against a server that pins the read loop with an endless
/// stream of 1xx messages.
pub const MAX_INTERIM_RESPONSES: usize = 32;

/// Case-insensitive search for an HTTP header in a serialized request buffer.
/// Skips the request line; matches lines starting with `\r\n<name>` followed by `:` (or OWS+`:`).
pub(crate) fn headers_contains(buf: &[u8], name: &str) -> bool {
    let needle = name.as_bytes();
    if needle.is_empty() {
        return false;
    }
    let mut i = 0;
    while i + 2 + needle.len() < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            let candidate_start = i + 2;
            let candidate_end = candidate_start + needle.len();
            if candidate_end <= buf.len()
                && buf[candidate_start..candidate_end].eq_ignore_ascii_case(needle)
            {
                let mut j = candidate_end;
                while j < buf.len() && (buf[j] == b' ' || buf[j] == b'\t') {
                    j += 1;
                }
                if j < buf.len() && buf[j] == b':' {
                    return true;
                }
            }
        }
        i += 1;
    }
    false
}

#[derive(Debug)]
pub enum HttpParseError {
    GetMoreData,
    Error(Box<StrOrString<'static>>),
    ReadingTimeout(Duration),
    Disconnected,
    InvalidHttpPayload(Box<StrOrString<'static>>),
}

impl HttpParseError {
    pub fn error(src: impl Into<StrOrString<'static>>) -> Self {
        HttpParseError::Error(Box::new(src.into()))
    }

    pub fn invalid_payload(src: impl Into<StrOrString<'static>>) -> Self {
        HttpParseError::InvalidHttpPayload(Box::new(src.into()))
    }

    pub fn get_more_data(&self) -> bool {
        matches!(self, HttpParseError::GetMoreData)
    }

    pub fn as_invalid_payload(&self) -> Option<&str> {
        match self {
            HttpParseError::InvalidHttpPayload(src) => Some(src.as_str()),
            _ => None,
        }
    }
}

/// A response builder which did not take the head it was fed with: what the upstream
/// has sent is not a response this client can hand over
impl From<http::Error> for HttpParseError {
    fn from(err: http::Error) -> Self {
        HttpParseError::invalid_payload(format!("Invalid HTTP response head: {}", err))
    }
}
