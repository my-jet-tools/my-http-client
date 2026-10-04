//! What the crate does with input which must not be put on the wire. The request
//! target, the header names and the header values come from the settings of a service
//! as often as from its code, and a request handed over by a hyper server carries
//! whatever the remote client has sent. None of it may panic: what is refused is
//! returned as an error.

use std::time::Duration;

use bytes::Bytes;
use http::{HeaderValue, Method, Version};
use http_body_util::{BodyExt, Full};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    validate_header_name, validate_header_value, write_header, HeaderValuePosition,
    MyHttpClientHeadersBuilder, MyHttpClientHeadersBuilderIterator, RequestBuildError,
};

use super::streaming_body_tests::TestConnector;
use super::{MyHttpClient, MyHttpRequest, MyHttpRequestBuilder};

/// The ASCII controls, the Latin-1 block and enough of what follows to have chars which
/// take two bytes in UTF-8
fn chars() -> impl Iterator<Item = char> {
    (0..=0x2ffu32).filter_map(char::from_u32)
}

fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

fn is_forbidden_in_path(c: char) -> bool {
    matches!(c, '\r' | '\n' | '\0' | ' ')
}

/// The three ways to write a header: the request builder, the headers builder and the
/// function both of them are made of. They have to refuse the very same input
fn header_errors(name: &str, value: &str) -> [Option<RequestBuildError>; 3] {
    [
        MyHttpRequestBuilder::new(Method::GET, "/")
            .append_header(name, value)
            .build()
            .err(),
        MyHttpClientHeadersBuilder::new()
            .add_header(name, value)
            .err(),
        write_header(&mut Vec::new(), name, value).err(),
    ]
}

#[test]
fn request_builder_refuses_cr_lf_nul_and_space_in_the_path() {
    for c in chars() {
        let path = format!("/a{}b", c);

        let expected = if is_forbidden_in_path(c) {
            Some(RequestBuildError::ForbiddenByteInPath(c as u8))
        } else {
            None
        };

        let error = MyHttpRequestBuilder::new(Method::GET, &path).build().err();

        assert_eq!(error, expected, "path with {:?}", c);
    }
}

#[test]
fn an_empty_header_name_is_refused() {
    for error in header_errors("", "value") {
        assert_eq!(error, Some(RequestBuildError::HeaderNameIsEmpty));
    }

    assert_eq!(
        validate_header_name(""),
        Err(RequestBuildError::HeaderNameIsEmpty)
    );
}

#[test]
fn a_header_name_which_is_not_a_token_is_refused() {
    for c in chars() {
        let name = format!("x{}y", c);

        // A char outside of ASCII is refused by its first byte
        let expected = name
            .bytes()
            .find(|b| !is_token_byte(*b))
            .map(RequestBuildError::ForbiddenByteInHeaderName);

        for error in header_errors(&name, "value") {
            assert_eq!(error, expected, "name with {:?}", c);
        }

        assert_eq!(validate_header_name(&name).err(), expected);
    }
}

#[test]
fn cr_lf_and_nul_in_a_header_value_are_refused() {
    for c in chars() {
        let value = format!("a{}b", c);

        let expected = if matches!(c, '\r' | '\n' | '\0') {
            Some(RequestBuildError::ForbiddenByteInHeaderValue(c as u8))
        } else {
            None
        };

        for error in header_errors("x-name", &value) {
            assert_eq!(error, expected, "value with {:?}", c);
        }

        assert_eq!(validate_header_value(&value).err(), expected);
    }
}

#[test]
fn a_bad_header_name_is_reported_before_a_bad_value() {
    for error in header_errors("bad name", "bad\nvalue") {
        assert_eq!(
            error,
            Some(RequestBuildError::ForbiddenByteInHeaderName(b' '))
        );
    }
}

/// The text is what ends up in the logs of a service: it tells what is wrong and which
/// byte it is, and it never repeats the input - a value is where the secrets are
#[test]
fn the_text_of_an_error_tells_what_is_wrong_and_which_byte_it_is() {
    assert_eq!(
        RequestBuildError::ForbiddenByteInPath(b'\r').to_string(),
        "Request path contains forbidden byte 0x0d (request line injection)"
    );
    assert_eq!(
        RequestBuildError::HeaderNameIsEmpty.to_string(),
        "HTTP header name must not be empty"
    );
    assert_eq!(
        RequestBuildError::ForbiddenByteInHeaderName(b' ').to_string(),
        "HTTP header name contains forbidden byte 0x20"
    );
    assert_eq!(
        RequestBuildError::ForbiddenByteInHeaderValue(b'\n').to_string(),
        "HTTP header value contains forbidden control byte 0x0a (header injection)"
    );
}

fn host_header() -> MyHttpClientHeadersBuilder {
    let mut headers = MyHttpClientHeadersBuilder::new();
    headers.add_header("host", "localhost").unwrap();
    headers
}

#[test]
fn a_header_which_is_refused_leaves_nothing_behind() {
    let mut dest = b"host: localhost\r\n".to_vec();
    assert!(write_header(&mut dest, "x-name", "bad\nvalue").is_err());
    assert!(write_header(&mut dest, "bad name", "value").is_err());
    assert_eq!(dest, b"host: localhost\r\n");

    let mut headers = host_header();
    assert!(headers.add_header("x-name", "bad\nvalue").is_err());
    assert_eq!(headers.as_str(), "host: localhost\r\n");
}

/// A step the request builder can not take does not fail on its own: the builder keeps
/// the first error it meets, skips every step after it, and the build returns that error
#[test]
fn the_request_builder_returns_its_first_error_from_the_build() {
    let builder = MyHttpRequestBuilder::new(Method::GET, "/path")
        .append_header("host", "localhost")
        .append_header("x-name", "bad\nvalue")
        .append_header("bad name", "value")
        .append_header("x-other", "value");

    let expected = RequestBuildError::ForbiddenByteInHeaderValue(b'\n');
    assert_eq!(builder.get_error(), Some(&expected));
    assert_eq!(builder.build().err(), Some(expected));

    let error = MyHttpRequestBuilder::new(Method::POST, "/a b")
        .append_header("bad name", "value")
        .build_with_body(b"body".to_vec())
        .err();
    assert_eq!(error, Some(RequestBuildError::ForbiddenByteInPath(b' ')));
}

#[test]
fn a_header_which_is_taken_is_written_as_it_is() {
    let mut written = Vec::new();
    let position = write_header(&mut written, "X-Name", "value").unwrap();

    assert_eq!(written, b"X-Name: value\r\n");
    assert_eq!(&written[position.start..position.end], b"value");

    let mut headers = MyHttpClientHeadersBuilder::new();
    let position = headers.add_header("X-Name", "caf\u{e9}").unwrap();
    assert_eq!(headers.as_str(), "X-Name: caf\u{e9}\r\n");
    assert_eq!(headers.get_value(&position), Some("caf\u{e9}"));

    let builder =
        MyHttpRequestBuilder::new(Method::POST, "/path?a=1").append_header("X-Name", "value");
    assert_eq!(builder.get_error(), None);
    assert_eq!(
        builder.build_with_body(b"body".to_vec()).unwrap().headers,
        b"POST /path?a=1 HTTP/1.1\r\nX-Name: value\r\nContent-Length: 4\r\n"
    );
}

/// A position is two public numbers, so it can be anything
#[test]
fn a_position_which_is_not_a_value_of_the_builder_gives_no_value() {
    let mut headers = MyHttpClientHeadersBuilder::new();
    let position = headers.add_header("X-Name", "caf\u{e9}").unwrap();

    let positions = [
        // Past the end of what is written
        (position.start, position.end + 100),
        (1000, 2000),
        // Upside down
        (position.end, position.start),
        // In the middle of the two bytes of the last char
        (position.start, position.end - 1),
    ];

    for (start, end) in positions {
        assert_eq!(
            headers.get_value(&HeaderValuePosition { start, end }),
            None,
            "{}..{}",
            start,
            end
        );
    }
}

/// The iterator is public and takes any bytes, not only the ones a builder has written
#[test]
fn the_iterator_of_headers_ends_at_a_header_which_is_not_utf8() {
    let mut headers = MyHttpClientHeadersBuilderIterator::new(b"a: 1\r\nb: \xff\r\nc: 3\r\n");

    assert_eq!(headers.next(), Some(("a", "1")));
    assert_eq!(headers.next(), None);
}

/// `MyHttpRequest::new` and `new_streamed` write the path into the request line the way
/// the builder does, so they refuse the same four bytes
#[test]
fn my_http_request_refuses_cr_lf_nul_and_space_in_the_path() {
    for c in chars() {
        let path = format!("/a{}b", c);

        let expected = if is_forbidden_in_path(c) {
            Some(RequestBuildError::ForbiddenByteInPath(c as u8))
        } else {
            None
        };

        let buffered =
            MyHttpRequest::new(Method::GET, &path, Version::HTTP_11, &host_header(), vec![]);
        assert_eq!(buffered.err(), expected, "path with {:?}", c);

        let streamed =
            MyHttpRequest::new_streamed(Method::POST, &path, Version::HTTP_11, &host_header());
        assert_eq!(streamed.err(), expected, "path with {:?}", c);
    }
}

/// A path which ends the request line by itself and brings a header of its own: sent
/// as it is, the upstream would read `x-injected: yes` as a header of the request
#[test]
fn a_path_which_brings_a_line_of_its_own_does_not_become_a_request() {
    const PATH_WITH_A_SECOND_LINE: &str = "/a HTTP/1.1\r\nx-injected: yes\r\nx-tail:";

    let buffered = MyHttpRequest::new(
        Method::GET,
        PATH_WITH_A_SECOND_LINE,
        Version::HTTP_11,
        &host_header(),
        vec![],
    );
    assert_eq!(
        buffered.err(),
        Some(RequestBuildError::ForbiddenByteInPath(b' '))
    );

    let streamed = MyHttpRequest::new_streamed(
        Method::POST,
        PATH_WITH_A_SECOND_LINE,
        Version::HTTP_11,
        &host_header(),
    );
    assert_eq!(
        streamed.err(),
        Some(RequestBuildError::ForbiddenByteInPath(b' '))
    );
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// `HeaderValue` takes the bytes from 0x80 up, and that is what a hyper server hands
/// over when a client sends them
const VALUE_WHICH_IS_NOT_ASCII: &[u8] = b"caf\xc3\xa9";

fn hyper_request_with_a_value_which_is_not_ascii() -> hyper::Request<Full<Bytes>> {
    hyper::Request::builder()
        .uri("/path")
        .header("host", "localhost")
        .header(
            "x-name",
            HeaderValue::from_bytes(VALUE_WHICH_IS_NOT_ASCII).unwrap(),
        )
        .body(Full::new(Bytes::new()))
        .unwrap()
}

#[tokio::test]
async fn from_hyper_request_writes_a_header_value_which_is_not_ascii_as_it_is() {
    let request = tokio::spawn(MyHttpRequest::from_hyper_request(
        hyper_request_with_a_value_which_is_not_ascii(),
    ))
    .await
    .unwrap_or_else(|_| panic!("from_hyper_request has panicked"));

    assert!(
        contains(&request.headers, b"\r\nx-name: caf\xc3\xa9\r\n"),
        "{:?}",
        String::from_utf8_lossy(&request.headers)
    );
}

/// An upstream of a single request: answers `200 OK` and gives back the head of the
/// request exactly as it came out of the socket
async fn start_upstream() -> (String, tokio::task::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let upstream = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut head = Vec::new();
        let mut buffer = [0u8; 4096];

        while !contains(&head, b"\r\n\r\n") {
            let read = socket.read(&mut buffer).await.unwrap();
            if read == 0 {
                break;
            }
            head.extend_from_slice(&buffer[..read]);
        }

        socket
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
            .await
            .unwrap();

        head
    });

    (addr, upstream)
}

#[tokio::test]
async fn a_header_value_which_is_not_ascii_reaches_the_upstream_byte_for_byte() {
    let (addr, upstream) = start_upstream().await;
    let client = MyHttpClient::new(TestConnector { host_port: addr });

    let request =
        MyHttpRequest::from_hyper_request(hyper_request_with_a_value_which_is_not_ascii()).await;

    let response = client
        .do_request(&request, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let head = upstream.await.unwrap();

    assert!(
        contains(&head, b"\r\nx-name: caf\xc3\xa9\r\n"),
        "{:?}",
        String::from_utf8_lossy(&head)
    );
}

/// A hyper server which turns every request into a `MyHttpRequest` - the way a proxy
/// does - and answers with the head it has got. Returns what the client has read
async fn send_through_a_hyper_server(raw_request: &[u8]) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();

        let service = hyper::service::service_fn(
            |request: hyper::Request<hyper::body::Incoming>| async move {
                let (parts, body) = request.into_parts();
                let body = body.collect().await.unwrap().to_bytes();

                let request = MyHttpRequest::from_hyper_request(hyper::Request::from_parts(
                    parts,
                    Full::new(body),
                ))
                .await;

                Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(Bytes::from(
                    request.headers,
                ))))
            },
        );

        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
            .await;
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(raw_request).await.unwrap();

    let mut response = Vec::new();
    // A connection reset is an answer as well: the server is gone without a response
    let _ = client.read_to_end(&mut response).await;
    response
}

#[tokio::test]
async fn a_client_header_which_is_not_ascii_does_not_kill_the_request_of_a_hyper_server() {
    let response = send_through_a_hyper_server(
        b"GET /path HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\nx-name: caf\xc3\xa9\r\n\r\n",
    )
    .await;

    assert!(
        response.starts_with(b"HTTP/1.1 200 OK\r\n"),
        "{:?}",
        String::from_utf8_lossy(&response)
    );

    // The answer of the server is the head it has built: the value is in it, byte for byte
    assert!(
        contains(&response, b"\r\nx-name: caf\xc3\xa9\r\n"),
        "{:?}",
        String::from_utf8_lossy(&response)
    );
}

fn request_with_head(head: &[u8]) -> MyHttpRequest {
    MyHttpRequest {
        headers: head.to_vec(),
        body: Bytes::new(),
    }
}

fn request_with_path(path: &str) -> MyHttpRequest {
    MyHttpRequest::new(Method::GET, path, Version::HTTP_11, &host_header(), vec![]).unwrap()
}

fn request_with_header(name: &str, value: &str) -> MyHttpRequest {
    let mut headers = MyHttpClientHeadersBuilder::new();
    headers.add_header(name, value).unwrap();
    MyHttpRequest::new(Method::GET, "/path", Version::HTTP_11, &headers, vec![]).unwrap()
}

/// The requests neither `to_hyper_h1_request` nor `to_hyper_h2_request` can convert
fn requests_hyper_does_not_take() -> Vec<(&'static str, MyHttpRequest)> {
    vec![
        ("an empty head", request_with_head(b"")),
        (
            "a head with no line end",
            request_with_head(b"GET / HTTP/1.1"),
        ),
        ("a request line with no path", request_with_head(b"GET\r\n")),
        (
            "a method which is not a token",
            request_with_head(b"G@T / HTTP/1.1\r\n"),
        ),
        (
            "a path with a byte an uri can not have",
            request_with_path("/a<b>"),
        ),
        (
            "a path which is not utf-8",
            request_with_head(b"GET /\xff\xfe HTTP/1.1\r\n"),
        ),
        (
            "a header value with a control byte",
            request_with_header("x-name", "a\x7fb"),
        ),
        (
            "a header name which is not a token",
            request_with_head(b"GET / HTTP/1.1\r\nbad name: value\r\n"),
        ),
    ]
}

fn assert_is_not_convertible<T>(result: Result<T, RequestBuildError>, case: &str) {
    match result {
        Err(RequestBuildError::NotConvertibleToHyper(_)) => {}
        Err(err) => panic!("{}: unexpected error {:?}", case, err),
        Ok(_) => panic!("{}: the request has been converted", case),
    }
}

#[test]
fn to_hyper_h1_request_reports_what_hyper_does_not_take() {
    let mut requests = requests_hyper_does_not_take();

    requests.push(("an empty path", request_with_path("")));

    for (case, request) in requests {
        assert_is_not_convertible(request.to_hyper_h1_request(), case);
    }
}

#[test]
fn to_hyper_h2_request_reports_what_hyper_does_not_take() {
    let mut requests = requests_hyper_does_not_take();

    requests.push(("a path with no leading slash", request_with_path("path")));
    requests.push((
        "a host which is not an authority",
        request_with_header("host", "local host"),
    ));
    requests.push(("an empty host", request_with_header("host", "")));

    for (case, request) in requests {
        assert_is_not_convertible(request.to_hyper_h2_request(true), case);
    }
}

#[test]
fn the_reason_a_request_is_not_convertible_is_in_the_error() {
    assert_eq!(
        request_with_head(b"").to_hyper_h1_request().err(),
        Some(RequestBuildError::NotConvertibleToHyper(
            "the head has no request line".to_string()
        ))
    );

    assert_eq!(
        request_with_head(b"GET\r\n")
            .to_hyper_h2_request(false)
            .err(),
        Some(RequestBuildError::NotConvertibleToHyper(
            "the request line has no path".to_string()
        ))
    );
}

#[test]
fn a_request_is_converted_to_a_hyper_h1_request() {
    let mut headers = MyHttpClientHeadersBuilder::new();
    headers.add_header("Host", "tokio.rs").unwrap();
    headers
        .add_header("Content-Type", "application/json")
        .unwrap();

    let request = MyHttpRequest::new(
        Method::POST,
        "/test?aaa=12",
        Version::HTTP_11,
        &headers,
        vec![0u8, 1u8, 2u8],
    )
    .unwrap()
    .to_hyper_h1_request()
    .unwrap();

    assert_eq!(request.method(), Method::POST);
    assert_eq!(request.uri(), "/test?aaa=12");
    assert_eq!(request.headers().len(), 3);
    assert_eq!(request.headers()["host"], "tokio.rs");
    assert_eq!(request.headers()["content-type"], "application/json");
    assert_eq!(request.headers()["content-length"], "3");
}

#[test]
fn a_request_is_converted_to_a_hyper_h2_request() {
    let mut headers = MyHttpClientHeadersBuilder::new();
    headers.add_header("Host", "tokio.rs").unwrap();
    headers
        .add_header("Content-Type", "application/json")
        .unwrap();

    let request = MyHttpRequest::new(
        Method::POST,
        "/test?aaa=12",
        Version::HTTP_11,
        &headers,
        vec![0u8, 1u8, 2u8],
    )
    .unwrap();

    let https = request.to_hyper_h2_request(true).unwrap();

    assert_eq!(https.method(), Method::POST);
    assert_eq!(https.version(), Version::HTTP_2);
    // The host is the authority of the uri and not a header any more
    assert_eq!(https.uri(), "https://tokio.rs/test?aaa=12");
    assert_eq!(https.headers().len(), 2);
    assert_eq!(https.headers()["content-type"], "application/json");
    assert_eq!(https.headers()["content-length"], "3");

    let http = request.to_hyper_h2_request(false).unwrap();
    assert_eq!(http.uri(), "http://tokio.rs/test?aaa=12");
}

/// What `from_hyper_request` has written byte for byte comes back out of the head byte
/// for byte: the value of a header does not have to be UTF-8
#[tokio::test]
async fn a_header_value_which_is_not_ascii_is_converted_as_it_is() {
    let request =
        MyHttpRequest::from_hyper_request(hyper_request_with_a_value_which_is_not_ascii()).await;

    let h1 = request.to_hyper_h1_request().unwrap();
    assert_eq!(h1.headers()["x-name"].as_bytes(), VALUE_WHICH_IS_NOT_ASCII);

    let h2 = request.to_hyper_h2_request(true).unwrap();
    assert_eq!(h2.headers()["x-name"].as_bytes(), VALUE_WHICH_IS_NOT_ASCII);

    // Latin-1, which is not UTF-8 at all
    let latin1 = request_with_head(b"GET / HTTP/1.1\r\nx-name: caf\xe9\r\n");
    assert_eq!(
        latin1.to_hyper_h1_request().unwrap().headers()["x-name"].as_bytes(),
        b"caf\xe9"
    );
}
