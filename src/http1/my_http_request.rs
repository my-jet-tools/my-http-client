use bytes::Bytes;
use http::{Method, Version};
use http_body_util::{BodyExt, Full};

use crate::RequestBuildError;

#[derive(Clone)]
pub struct MyHttpRequest {
    pub headers: Vec<u8>,
    pub body: Bytes,
}

impl MyHttpRequest {
    /// Refuses CR, LF, NUL and a space in the path and query: written into the request
    /// line as they are, they would end it before its time and the rest of the path
    /// would be read as the next line of the request
    pub fn new<Headers: crate::MyHttpClientHeaders>(
        method: Method,
        path_and_query: &str,
        version: Version,
        headers_src: &Headers,
        body: Vec<u8>,
    ) -> Result<Self, RequestBuildError> {
        check_path_and_query(path_and_query)?;

        let mut result = Self {
            headers: create_headers(method, path_and_query, version).into_bytes(),
            body: body.into(),
        };

        headers_src.copy_to(&mut result.headers);

        if !result.body.is_empty()
            && !super::headers_contains(&result.headers, super::CONTENT_LENGTH_HEADER_NAME)
        {
            crate::headers::append_header_line(
                &mut result.headers,
                super::CONTENT_LENGTH_HEADER_NAME.as_bytes(),
                result.body.len().to_string().as_bytes(),
            );
        }

        Ok(result)
    }

    /// Head of a request whose body is streamed by
    /// [`super::MyHttpClient::do_streamed_request`]. It carries no framing header:
    /// `content-length` or `transfer-encoding: chunked` is written by the client itself,
    /// out of the `content_size` it was given.
    ///
    /// The path and query is checked the way [`Self::new`] checks it
    pub fn new_streamed<Headers: crate::MyHttpClientHeaders>(
        method: Method,
        path_and_query: &str,
        version: Version,
        headers_src: &Headers,
    ) -> Result<Self, RequestBuildError> {
        check_path_and_query(path_and_query)?;

        let mut result = Self {
            headers: create_headers(method, path_and_query, version).into_bytes(),
            body: Bytes::new(),
        };

        headers_src.copy_to(&mut result.headers);

        Ok(result)
    }

    /// Serializes the head of a streamed request: the headers, the framing which the
    /// payload is going to use, and the empty line which ends the head
    pub(crate) fn write_streamed_head_to(&self, writer: &mut Vec<u8>, content_size: Option<usize>) {
        writer.extend_from_slice(&self.headers);

        match content_size {
            Some(content_size) => {
                crate::headers::append_header_line(
                    writer,
                    super::CONTENT_LENGTH_HEADER_NAME.as_bytes(),
                    content_size.to_string().as_bytes(),
                );
            }
            None => {
                crate::headers::append_header_line(
                    writer,
                    super::TRANSFER_ENCODING_HEADER_NAME.as_bytes(),
                    b"chunked",
                );
            }
        }

        writer.extend_from_slice(crate::CL_CR);
    }

    pub async fn from_hyper_request(req: hyper::Request<Full<Bytes>>) -> Self {
        let (parts, body) = req.into_parts();

        // Nothing is checked here: the uri, the names and the values are checked by
        // their types. An `Uri` has no CR, LF, NUL or space in its path and query, a
        // `HeaderName` is a token and a `HeaderValue` has no CR, LF or NUL in it
        let headers = create_headers(
            parts.method,
            parts
                .uri
                .path_and_query()
                .map(|pq| pq.as_str())
                .unwrap_or("/"),
            parts.version,
        );

        let mut headers = headers.into_bytes();

        for (name, value) in parts.headers.iter() {
            // The value goes out byte for byte: it may carry the bytes from 0x80 up,
            // which a remote client is free to send and which are not a `&str`
            crate::headers::append_header_line(
                &mut headers,
                name.as_str().as_bytes(),
                value.as_bytes(),
            );
        }

        let body_as_bytes = match body.collect().await {
            Ok(body) => body.to_bytes(),
            // A buffered body has nothing to fail with: its error is `Infallible`
            Err(never) => match never {},
        };

        Self {
            headers,
            body: body_as_bytes,
        }
    }

    pub fn write_to(&self, writer: &mut Vec<u8>) {
        writer.extend_from_slice(&self.headers);
        writer.extend_from_slice(crate::CL_CR);
        writer.extend_from_slice(&self.body);
    }

    /// Extracts the HTTP method from the serialized request line (the first
    /// whitespace-delimited token). Used by the read loop to apply RFC 9112
    /// §6.3 response-body framing (HEAD / CONNECT never carry a body).
    pub fn get_method(&self) -> Method {
        let end = self
            .headers
            .iter()
            .position(|b| *b == b' ')
            .unwrap_or(self.headers.len());

        Method::from_bytes(&self.headers[..end]).unwrap_or(Method::GET)
    }
}

/// CR, LF, NUL and a space: what must not be written into a request line
pub(crate) fn check_path_and_query(path_and_query: &str) -> Result<(), RequestBuildError> {
    match path_and_query
        .bytes()
        .find(|b| matches!(b, b'\r' | b'\n' | 0 | b' '))
    {
        Some(b) => Err(RequestBuildError::ForbiddenByteInPath(b)),
        None => Ok(()),
    }
}

fn create_headers(method: Method, path_and_query: &str, version: Version) -> String {
    format!("{} {} {:?}\r\n", method, path_and_query, version)
}
