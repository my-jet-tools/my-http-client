use bytes::Bytes;
use http::{Method, Version};
use http_body_util::{BodyExt, Full};
use std::fmt::Write;

#[derive(Clone)]
pub struct MyHttpRequest {
    pub headers: Vec<u8>,
    pub body: Bytes,
}

impl MyHttpRequest {
    pub fn new<Headers: crate::MyHttpClientHeaders>(
        method: Method,
        path_and_query: &str,
        version: Version,
        headers_src: &Headers,
        body: Vec<u8>,
    ) -> Self {
        let mut result = Self {
            headers: create_headers(method, path_and_query, version).into_bytes(),
            body: body.into(),
        };

        headers_src.copy_to(&mut result.headers);

        if !result.body.is_empty()
            && !super::headers_contains(&result.headers, super::CONTENT_LENGTH_HEADER_NAME)
        {
            crate::headers::write_header(
                &mut result.headers,
                super::CONTENT_LENGTH_HEADER_NAME,
                result.body.len().to_string().as_str(),
            );
        }

        result
    }

    /// Head of a request whose body is streamed by
    /// [`super::MyHttpClient::do_streamed_request`]. It carries no framing header:
    /// `content-length` or `transfer-encoding: chunked` is written by the client itself,
    /// out of the `content_size` it was given
    pub fn new_streamed<Headers: crate::MyHttpClientHeaders>(
        method: Method,
        path_and_query: &str,
        version: Version,
        headers_src: &Headers,
    ) -> Self {
        let mut result = Self {
            headers: create_headers(method, path_and_query, version).into_bytes(),
            body: Bytes::new(),
        };

        headers_src.copy_to(&mut result.headers);

        result
    }

    /// Serializes the head of a streamed request: the headers, the framing which the
    /// payload is going to use, and the empty line which ends the head
    pub(crate) fn write_streamed_head_to(&self, writer: &mut Vec<u8>, content_size: Option<usize>) {
        writer.extend_from_slice(&self.headers);

        match content_size {
            Some(content_size) => {
                crate::headers::write_header(
                    writer,
                    super::CONTENT_LENGTH_HEADER_NAME,
                    content_size.to_string().as_str(),
                );
            }
            None => {
                crate::headers::write_header(
                    writer,
                    super::TRANSFER_ENCODING_HEADER_NAME,
                    "chunked",
                );
            }
        }

        writer.extend_from_slice(crate::CL_CR);
    }

    pub async fn from_hyper_request(req: hyper::Request<Full<Bytes>>) -> Self {
        let (parts, body) = req.into_parts();

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

        for header in parts.headers.iter() {
            crate::headers::write_header(
                &mut headers,
                header.0.as_str(),
                header.1.to_str().unwrap(),
            );
        }

        let body_as_bytes = body.collect().await.unwrap().to_bytes();

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

fn create_headers(method: Method, path_and_query: &str, version: Version) -> String {
    let mut headers = String::new();

    write!(
        &mut headers,
        "{} {} {:?}\r\n",
        method, path_and_query, version
    )
    .unwrap();

    headers
}
