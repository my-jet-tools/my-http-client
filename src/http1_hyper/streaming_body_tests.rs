use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use rust_extensions::remote_endpoint::RemoteEndpoint;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::{TcpListener, TcpStream},
};

use crate::{MyHttpClientConnector, MyHttpClientError, PublishPayloadError, RequestBodyStream};

use super::{HyperHttpResponse, MyHttpHyperClient};

pub struct TestConnector {
    host_port: String,
}

#[async_trait::async_trait]
impl MyHttpClientConnector<TcpStream> for TestConnector {
    async fn connect(&self) -> Result<TcpStream, MyHttpClientError> {
        TcpStream::connect(self.host_port.as_str())
            .await
            .map_err(|err| MyHttpClientError::CanNotConnectToRemoteHost(err.to_string()))
    }

    fn get_remote_endpoint(&self) -> RemoteEndpoint<'_> {
        RemoteEndpoint::try_parse(self.host_port.as_str()).unwrap()
    }

    fn is_debug(&self) -> bool {
        false
    }

    fn reunite(read: ReadHalf<TcpStream>, write: WriteHalf<TcpStream>) -> TcpStream {
        read.unsplit(write)
    }
}

/// What the upstream has seen on the wire
pub struct RequestOnTheWire {
    pub headers: String,
    pub body: Vec<u8>,
}

impl RequestOnTheWire {
    pub fn is_chunked(&self) -> bool {
        self.headers
            .to_lowercase()
            .contains("transfer-encoding: chunked")
    }

    pub fn get_content_length(&self) -> Option<usize> {
        get_content_length(&self.headers.to_lowercase())
    }
}

const OK_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";

#[tokio::test]
async fn test_post_body_as_a_stream_is_sent_chunked() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        socket.write_all(OK_RESPONSE).await.unwrap();
        socket.flush().await.unwrap();
        request
    });

    // The client is not connected yet: do_streamed_request has no retry to fall back on,
    // so it has to establish the connection before it starts consuming the producer
    let client = MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    // We create the channel, the client gets the consuming side, we keep the publisher
    let (publisher, body) = RequestBodyStream::new(1);

    // The producer pushes the payload while the request is already on the wire
    tokio::spawn(async move {
        publisher.publish(b"first-".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        publisher.publish(b"second".to_vec()).await.unwrap();
        // dropping the publisher closes the channel and ends the body
    });

    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri("/upload")
        .header("host", "localhost")
        .body(body)
        .unwrap();

    let response = client
        .do_streamed_request(request, None, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(get_response_body(response).await, b"ok".to_vec());

    let request_on_the_wire = server.await.unwrap();

    // The size of a streamed body is unknown upfront, so it goes out chunked
    assert!(request_on_the_wire.is_chunked());
    assert_eq!(request_on_the_wire.get_content_length(), None);
    assert_eq!(request_on_the_wire.body, b"first-second".to_vec());
}

#[tokio::test]
async fn test_streamed_body_of_a_known_size_is_sent_with_content_length() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        socket.write_all(OK_RESPONSE).await.unwrap();
        socket.flush().await.unwrap();
        request
    });

    let client = MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    let (publisher, body) = RequestBodyStream::new(1);

    tokio::spawn(async move {
        publisher.publish(b"first-".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        publisher.publish(b"second".to_vec()).await.unwrap();
    });

    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri("/upload")
        .header("host", "localhost")
        .body(body)
        .unwrap();

    // The size is known upfront: the payload is streamed, but the framing is a
    // content-length and not chunked
    let response = client
        .do_streamed_request(request, Some(12), Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(get_response_body(response).await, b"ok".to_vec());

    let request_on_the_wire = server.await.unwrap();

    assert!(!request_on_the_wire.is_chunked());
    assert_eq!(request_on_the_wire.get_content_length(), Some(12));
    assert_eq!(request_on_the_wire.body, b"first-second".to_vec());
}

#[tokio::test]
async fn test_a_body_shorter_than_the_announced_content_length_fails() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        // The upstream would wait for the rest of the body forever
        let mut buffer = [0u8; 1024];
        let _ = socket.read(&mut buffer).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
    });

    let client = MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    let body = StreamBody::new(futures::stream::iter(vec![Ok::<_, String>(Frame::data(
        Bytes::from_static(b"12345"),
    ))]));

    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri("/upload")
        .header("host", "localhost")
        .body(body)
        .unwrap();

    let result = client
        .do_streamed_request(request, Some(10), Duration::from_secs(5))
        .await;

    // hyper refuses to send a body which does not match the announced content-length
    assert!(result.is_err());

    server.await.unwrap();
}

#[tokio::test]
async fn test_buffered_body_is_still_sent_with_content_length() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        socket.write_all(OK_RESPONSE).await.unwrap();
        socket.flush().await.unwrap();
        request
    });

    let client = MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    // Full<Bytes> as it was before the streaming support - the signature still takes it
    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri("/upload")
        .header("host", "localhost")
        .body(Full::new(Bytes::from_static(b"1234567890")))
        .unwrap();

    let response = client
        .do_request(request, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(get_response_body(response).await, b"ok".to_vec());

    let request_on_the_wire = server.await.unwrap();

    assert!(!request_on_the_wire.is_chunked());
    assert_eq!(request_on_the_wire.get_content_length(), Some(10));
    assert_eq!(request_on_the_wire.body, b"1234567890".to_vec());
}

#[tokio::test]
async fn test_buffered_body_is_replayed_after_the_upstream_dropped_the_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        // The first connection reads the request and dies without a response
        let (mut socket, _) = listener.accept().await.unwrap();
        let first_attempt = read_request(&mut socket).await;
        socket.shutdown().await.unwrap();
        drop(socket);

        // The client is expected to redial and to replay the very same request
        let (mut socket, _) = listener.accept().await.unwrap();
        let second_attempt = read_request(&mut socket).await;
        socket.write_all(OK_RESPONSE).await.unwrap();
        socket.flush().await.unwrap();

        (first_attempt, second_attempt)
    });

    let client = MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    let request = hyper::Request::builder()
        .method(hyper::Method::PUT)
        .uri("/upload")
        .header("host", "localhost")
        .body(Full::new(Bytes::from_static(b"1234567890")))
        .unwrap();

    let response = client
        .do_request(request, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(get_response_body(response).await, b"ok".to_vec());

    let (first_attempt, second_attempt) = server.await.unwrap();

    assert_eq!(first_attempt.body, b"1234567890".to_vec());
    assert_eq!(second_attempt.body, b"1234567890".to_vec());
}

#[tokio::test]
async fn test_streamed_request_is_never_retried() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        socket.shutdown().await.unwrap();
        drop(socket);

        // A second connection must not happen: the payload has been consumed by the
        // producer and the client has nothing to replay
        let second_attempt =
            tokio::time::timeout(Duration::from_millis(300), listener.accept()).await;

        (request, second_attempt.is_ok())
    });

    let client = MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    let body = StreamBody::new(futures::stream::iter(vec![Ok::<_, String>(Frame::data(
        Bytes::from_static(b"1234567890"),
    ))]));

    // Idempotent on purpose: do_request would have replayed it, do_streamed_request
    // hands the error over to the producing side instead
    let request = hyper::Request::builder()
        .method(hyper::Method::PUT)
        .uri("/upload")
        .header("host", "localhost")
        .body(body)
        .unwrap();

    let result = client
        .do_streamed_request(request, None, Duration::from_secs(5))
        .await;

    match result {
        Ok(_) => panic!("The request had to fail: the upstream died without a response"),
        Err(err) => match err {
            MyHttpClientError::CanNotExecuteRequest(_) => {}
            _ => panic!("Unexpected error: {:?}", err),
        },
    }

    let (request_on_the_wire, has_second_attempt) = server.await.unwrap();

    assert_eq!(request_on_the_wire.body, b"1234567890".to_vec());
    assert!(!has_second_attempt);
}

#[tokio::test]
async fn test_client_driven_streamed_request_delivers_the_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        socket.write_all(OK_RESPONSE).await.unwrap();
        socket.flush().await.unwrap();
        request
    });

    let client = Arc::new(MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    }));

    let (publisher, body) = RequestBodyStream::new(1);

    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri("/upload")
        .header("host", "localhost")
        .body(body)
        .unwrap();

    // The client drives the request itself, we only push the payload
    let streamed_request = client.start_streamed_request(request, None, Duration::from_secs(5));

    publisher.publish(b"first-".to_vec()).await.unwrap();
    publisher.publish(b"second".to_vec()).await.unwrap();

    // The body is over only when the publisher is gone
    drop(publisher);

    let response = streamed_request.get_response().await.unwrap();

    assert_eq!(get_response_body(response).await, b"ok".to_vec());

    let request_on_the_wire = server.await.unwrap();

    assert!(request_on_the_wire.is_chunked());
    assert_eq!(request_on_the_wire.body, b"first-second".to_vec());
}

#[tokio::test]
async fn test_client_driven_streamed_request_reports_the_failure_to_the_publisher() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        // Reads the head and the first chunks, then dies without a response
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 1024];
        let _ = socket.read(&mut buffer).await.unwrap();
        socket.shutdown().await.unwrap();
        drop(socket);
    });

    let client = Arc::new(MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    }));

    let (publisher, body) = RequestBodyStream::new(1);

    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri("/upload")
        .header("host", "localhost")
        .body(body)
        .unwrap();

    let streamed_request = client.start_streamed_request(request, None, Duration::from_secs(5));

    // The upstream is gone: the publishing side is expected to find it out, with the
    // reason of the failure and not just a closed channel
    let err = loop {
        match publisher.publish(vec![0u8; 4096]).await {
            Ok(_) => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => break err,
        }
    };

    match err {
        PublishPayloadError::RequestFailed { reason, .. } => {
            assert!(!reason.is_empty());
        }
        PublishPayloadError::RequestIsOver => {
            panic!("The reason of the failure had to reach the publisher")
        }
    }

    drop(publisher);

    let result = streamed_request.get_response().await;
    assert!(result.is_err());

    server.await.unwrap();
}

#[tokio::test]
async fn test_incoming_body_can_be_proxied_as_a_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        socket.write_all(OK_RESPONSE).await.unwrap();
        socket.flush().await.unwrap();
        request
    });

    let client = MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    // hyper::body::Incoming - what a proxied request carries - goes in as is, with no
    // collecting into memory in between
    let incoming = get_incoming_body(b"proxied-payload").await;

    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri("/upload")
        .header("host", "localhost")
        .body(incoming)
        .unwrap();

    let response = client
        .do_streamed_request(request, None, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(get_response_body(response).await, b"ok".to_vec());

    let request_on_the_wire = server.await.unwrap();
    assert_eq!(request_on_the_wire.body, b"proxied-payload".to_vec());
}

/// Produces a real hyper::body::Incoming by letting a hyper client read a response of a
/// hand written server
async fn get_incoming_body(payload: &'static [u8]) -> hyper::body::Incoming {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;

        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n",
            payload.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(payload).await.unwrap();
        socket.flush().await.unwrap();

        // Keeps the connection alive until the body is read out
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .unwrap();

    tokio::spawn(async move {
        let _ = conn.await;
    });

    let request = hyper::Request::builder()
        .method(hyper::Method::GET)
        .uri("/source")
        .header("host", "localhost")
        .body(Full::new(Bytes::new()))
        .unwrap();

    sender.send_request(request).await.unwrap().into_body()
}

async fn get_response_body(response: HyperHttpResponse) -> Vec<u8> {
    match response {
        HyperHttpResponse::Response(response) => {
            assert_eq!(response.status(), 200);
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec()
        }
        #[cfg(feature = "with-websocket")]
        HyperHttpResponse::WebSocketUpgrade { .. } => {
            panic!("Unexpected web socket upgrade")
        }
    }
}

async fn read_request(socket: &mut TcpStream) -> RequestOnTheWire {
    let mut buffer = Vec::new();

    let headers_end = loop {
        if let Some(index) = find_sub_sequence(&buffer, b"\r\n\r\n") {
            break index + 4;
        }

        read_more(socket, &mut buffer).await;
    };

    let headers = String::from_utf8(buffer[..headers_end].to_vec()).unwrap();
    let mut buffer: Vec<u8> = buffer[headers_end..].to_vec();

    let headers_lc = headers.to_lowercase();

    let body = if headers_lc.contains("transfer-encoding: chunked") {
        read_chunked_body(socket, &mut buffer).await
    } else if let Some(content_length) = get_content_length(&headers_lc) {
        while buffer.len() < content_length {
            read_more(socket, &mut buffer).await;
        }
        buffer[..content_length].to_vec()
    } else {
        vec![]
    };

    RequestOnTheWire { headers, body }
}

async fn read_chunked_body(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> Vec<u8> {
    let mut result = Vec::new();

    loop {
        let size_line = read_line(socket, buffer).await;
        let chunk_size = usize::from_str_radix(size_line.trim(), 16).unwrap();

        if chunk_size == 0 {
            // The trailing CRLF of the last chunk
            let _ = read_line(socket, buffer).await;
            break;
        }

        while buffer.len() < chunk_size + 2 {
            read_more(socket, buffer).await;
        }

        result.extend_from_slice(&buffer[..chunk_size]);
        buffer.drain(..chunk_size + 2);
    }

    result
}

async fn read_line(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> String {
    loop {
        if let Some(index) = find_sub_sequence(buffer, b"\r\n") {
            let result = String::from_utf8(buffer[..index].to_vec()).unwrap();
            buffer.drain(..index + 2);
            return result;
        }

        read_more(socket, buffer).await;
    }
}

async fn read_more(socket: &mut TcpStream, buffer: &mut Vec<u8>) {
    let mut chunk = [0u8; 1024];
    let read = socket.read(&mut chunk).await.unwrap();

    if read == 0 {
        panic!("Connection is closed before the request has been read");
    }

    buffer.extend_from_slice(&chunk[..read]);
}

fn find_sub_sequence(src: &[u8], sub_sequence: &[u8]) -> Option<usize> {
    if src.len() < sub_sequence.len() {
        return None;
    }

    (0..=src.len() - sub_sequence.len())
        .find(|index| &src[*index..*index + sub_sequence.len()] == sub_sequence)
}

fn get_content_length(headers_lc: &str) -> Option<usize> {
    for line in headers_lc.split("\r\n") {
        if let Some(value) = line.strip_prefix("content-length:") {
            return Some(value.trim().parse().unwrap());
        }
    }

    None
}
