use http::Method;

use super::MyHttpRequest;
use crate::RequestBuildError;

/// The head of a request as it is going to be written: the request line and the
/// headers taken so far. [`MyHttpRequestBuilder`] keeps one of these - or the first
/// error that came up while the request was being built - and hands every step over to
/// it
///
/// [`MyHttpRequestBuilder`]: super::MyHttpRequestBuilder
pub(super) struct MyHttpRequestBuilderInner {
    headers: Vec<u8>,
}

impl MyHttpRequestBuilderInner {
    /// Refuses CR, LF, NUL and a space in the path and query: written into the request
    /// line as they are, they would end it before its time
    pub fn new(method: Method, path_and_query: &str) -> Result<Self, RequestBuildError> {
        super::check_path_and_query(path_and_query)?;

        let mut headers = Vec::new();
        headers.extend_from_slice(method.as_str().as_bytes());
        headers.push(b' ');
        headers.extend_from_slice(path_and_query.as_bytes());
        headers.push(b' ');
        headers.extend_from_slice(b"HTTP/1.1\r\n");
        Ok(Self { headers })
    }

    /// Refuses an empty name, a name which is not an HTTP token and a value with CR, LF
    /// or NUL in it
    pub fn append_header(mut self, name: &str, value: &str) -> Result<Self, RequestBuildError> {
        crate::headers::write_header(&mut self.headers, name, value)?;
        Ok(self)
    }

    pub fn build_with_body(mut self, body: Vec<u8>) -> MyHttpRequest {
        if !body.is_empty() && !super::headers_contains(&self.headers, "content-length") {
            crate::headers::append_header_line(
                &mut self.headers,
                b"Content-Length",
                body.len().to_string().as_bytes(),
            );
        }

        MyHttpRequest {
            headers: self.headers,
            body: body.into(),
        }
    }

    pub fn build(self) -> MyHttpRequest {
        MyHttpRequest {
            headers: self.headers,
            body: Vec::new().into(),
        }
    }
}
