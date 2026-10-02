use bytes::Bytes;
use http::{request::Builder, Method, Uri, Version};
use http_body_util::Full;

use crate::RequestBuildError;

use super::*;

impl MyHttpRequest {
    /// Fails on a head which hyper does not take: no request line, a method, a path or
    /// a header it refuses. The head is a public field and nothing here trusts it
    pub fn to_hyper_h1_request(&self) -> Result<hyper::Request<Full<Bytes>>, RequestBuildError> {
        build_h1_headers(&self.headers)?
            .body(Full::new(self.body.clone()))
            .map_err(not_convertible)
    }

    /// The same as [`Self::to_hyper_h1_request`], with the `host` header turned into
    /// the authority of the uri - which makes a host hyper does not take as an
    /// authority one more reason to fail
    pub fn to_hyper_h2_request(
        &self,
        is_https: bool,
    ) -> Result<hyper::Request<Full<Bytes>>, RequestBuildError> {
        build_h2_headers(&self.headers, is_https)?
            .body(Full::new(self.body.clone()))
            .map_err(not_convertible)
    }
}

fn not_convertible(reason: impl std::fmt::Display) -> RequestBuildError {
    RequestBuildError::NotConvertibleToHyper(reason.to_string())
}

fn build_h1_headers(headers: &[u8]) -> Result<Builder, RequestBuildError> {
    let mut lines = lines(headers);

    let (http_method, uri) = extract_http_method_and_uri(lines.next())?;

    let mut builder = Builder::new().method(http_method).uri(uri);

    for line in lines {
        let (name, value) = extract_name_and_value(line);

        builder = builder.header(name.trim_ascii(), value.trim_ascii());
    }

    Ok(builder)
}

fn build_h2_headers(headers: &[u8], is_https: bool) -> Result<Builder, RequestBuildError> {
    let mut lines = lines(headers);

    let (http_method, uri) = extract_http_method_and_uri(lines.next())?;

    let mut builder = Builder::new().version(Version::HTTP_2);

    let mut host = None;

    for line in lines {
        let (name, value) = extract_name_and_value(line);

        if name.eq_ignore_ascii_case(b"host") {
            host = Some(value.trim_ascii());
        } else {
            builder = builder.header(name.trim_ascii(), value.trim_ascii());
        }
    }

    let uri = match host {
        Some(host) => Uri::builder()
            .scheme(if is_https { "https" } else { "http" })
            .authority(host)
            .path_and_query(uri)
            .build(),
        None => Uri::builder().path_and_query(uri).build(),
    }
    .map_err(not_convertible)?;

    Ok(builder.method(http_method).uri(uri))
}

/// The lines of a serialized head. Whatever follows the last CRLF is not a line
fn lines(mut head: &[u8]) -> impl Iterator<Item = &[u8]> {
    std::iter::from_fn(move || {
        let line_end = head
            .windows(crate::CL_CR.len())
            .position(|window| window == crate::CL_CR)?;

        let line = &head[..line_end];
        head = &head[line_end + crate::CL_CR.len()..];
        Some(line)
    })
}

fn extract_http_method_and_uri(
    request_line: Option<&[u8]>,
) -> Result<(Method, &[u8]), RequestBuildError> {
    let Some(request_line) = request_line else {
        return Err(not_convertible("the head has no request line"));
    };

    let mut parts = request_line.split(|b| *b == b' ');

    let method = parts.next().unwrap_or_default();

    let Some(path) = parts.next() else {
        return Err(not_convertible("the request line has no path"));
    };

    let method = Method::from_bytes(method).map_err(not_convertible)?;

    Ok((method, path))
}

fn extract_name_and_value(line: &[u8]) -> (&[u8], &[u8]) {
    match line.iter().position(|b| *b == b':') {
        Some(header_separator_index) => (
            &line[..header_separator_index],
            &line[header_separator_index + 1..],
        ),
        None => (line, &[]),
    }
}

#[cfg(test)]
mod tests {

    use http::{Method, Version};

    use crate::{http1::MyHttpRequest, MyHttpClientHeadersBuilder};

    #[test]
    fn test_converting() {
        let mut headers = MyHttpClientHeadersBuilder::new();
        headers
            .add_header("content-type", "application/json")
            .unwrap();
        headers.add_header("accept-language", "en-US").unwrap();
        let request_builder = MyHttpRequest::new(
            Method::POST,
            "/test?aaa=12",
            Version::HTTP_11,
            &headers,
            vec![0u8, 1u8, 2u8],
        )
        .unwrap();

        let body = request_builder.to_hyper_h1_request().unwrap();

        println!("{:?}", body);
    }

    #[test]
    fn test_converting_to_h2() {
        let mut headers = MyHttpClientHeadersBuilder::new();
        headers
            .add_header("content-type", "application/json")
            .unwrap();
        headers.add_header("accept-language", "en-US").unwrap();
        headers.add_header("host", "tokio.rs").unwrap();
        let request_builder = MyHttpRequest::new(
            Method::POST,
            "/test?aaa=12",
            Version::HTTP_11,
            &headers,
            vec![0u8, 1u8, 2u8],
        )
        .unwrap();

        let body = request_builder.to_hyper_h2_request(true).unwrap();

        println!("{:?}", body);
    }
}
