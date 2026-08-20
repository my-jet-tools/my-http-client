use std::time::Duration;

use bytes::Bytes;
use http::{Method, Version};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use rust_extensions::remote_endpoint::RemoteEndpoint;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::{TcpListener, TcpStream},
};

use crate::{
    MyHttpClientConnector, MyHttpClientError, MyHttpClientHeadersBuilder, RequestBodyStream,
};

use super::{MyHttpClient, MyHttpRequest, MyHttpResponse};

pub struct TestConnector {
    pub host_port: String,
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

const OK_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";

fn create_headers() -> MyHttpClientHeadersBuilder {
    let mut headers = MyHttpClientHeadersBuilder::new();
    headers.add_header("host", "localhost");
    headers
}

#[tokio::test]
async fn test_streamed_body_is_sent_chunked() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket, &mut Vec::new()).await;
        socket.write_all(OK_RESPONSE).await.unwrap();
        socket.flush().await.unwrap();
        request
    });

    let client = MyHttpClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    client.connect().await.unwrap();

    let request =
        MyHttpRequest::new_streamed(Method::POST, "/upload", Version::HTTP_11, &create_headers());

    let (publisher, body) = RequestBodyStream::new(1);

    tokio::spawn(async move {
        publisher.publish(b"first-".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        publisher.publish(b"second".to_vec()).await.unwrap();
    });

    let response = client
        .do_streamed_request(&request, body, None, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(get_response_body(response).await, b"ok".to_vec());

    let request_on_the_wire = server.await.unwrap();

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
        let request = read_request(&mut socket, &mut Vec::new()).await;
        socket.write_all(OK_RESPONSE).await.unwrap();
        socket.flush().await.unwrap();
        request
    });

    let client = MyHttpClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    client.connect().await.unwrap();

    let request =
        MyHttpRequest::new_streamed(Method::POST, "/upload", Version::HTTP_11, &create_headers());

    let (publisher, body) = RequestBodyStream::new(1);

    tokio::spawn(async move {
        publisher.publish(b"first-".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        publisher.publish(b"second".to_vec()).await.unwrap();
    });

    // The size is known upfront: no chunk framing, just a content-length
    let response = client
        .do_streamed_request(&request, body, Some(12), Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(get_response_body(response).await, b"ok".to_vec());

    let request_on_the_wire = server.await.unwrap();

    assert!(!request_on_the_wire.is_chunked());
    assert_eq!(request_on_the_wire.get_content_length(), Some(12));
    assert_eq!(request_on_the_wire.body, b"first-second".to_vec());
}

#[tokio::test]
async fn test_a_body_shorter_than_the_announced_content_length_is_refused() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        // The upstream would wait for the rest of the body forever
        let mut buffer = [0u8; 1024];
        let _ = socket.read(&mut buffer).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
    });

    let client = MyHttpClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    client.connect().await.unwrap();

    let request =
        MyHttpRequest::new_streamed(Method::POST, "/upload", Version::HTTP_11, &create_headers());

    let body = StreamBody::new(futures::stream::iter(vec![Ok::<_, String>(Frame::data(
        Bytes::from_static(b"12345"),
    ))]));

    let result = client
        .do_streamed_request(&request, body, Some(10), Duration::from_secs(5))
        .await;

    match result {
        Ok(_) => panic!("The request had to be refused"),
        Err(MyHttpClientError::CanNotExecuteRequest(reason)) => {
            assert!(reason.contains("content-length"), "{}", reason)
        }
        Err(err) => panic!("Unexpected error: {:?}", err),
    }

    server.await.unwrap();
}

#[tokio::test]
async fn test_a_body_bigger_than_the_announced_content_length_is_refused() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 1024];
        let _ = socket.read(&mut buffer).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
    });

    let client = MyHttpClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    client.connect().await.unwrap();

    let request =
        MyHttpRequest::new_streamed(Method::POST, "/upload", Version::HTTP_11, &create_headers());

    // The bytes past the announced size would be read as the next request
    let body = StreamBody::new(futures::stream::iter(vec![Ok::<_, String>(Frame::data(
        Bytes::from_static(b"1234567890"),
    ))]));

    let result = client
        .do_streamed_request(&request, body, Some(5), Duration::from_secs(5))
        .await;

    match result {
        Ok(_) => panic!("The request had to be refused"),
        Err(MyHttpClientError::CanNotExecuteRequest(reason)) => {
            assert!(reason.contains("content-length"), "{}", reason)
        }
        Err(err) => panic!("Unexpected error: {:?}", err),
    }

    server.await.unwrap();
}

#[tokio::test]
async fn test_a_request_issued_while_streaming_goes_out_after_the_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut buffer = Vec::new();

        // The streamed request comes first and its body is complete before anything
        // else appears on the wire - otherwise this parser would choke
        let streamed = read_request(&mut socket, &mut buffer).await;
        socket.write_all(OK_RESPONSE).await.unwrap();

        let pipelined = read_request(&mut socket, &mut buffer).await;
        socket.write_all(OK_RESPONSE).await.unwrap();
        socket.flush().await.unwrap();

        (streamed, pipelined)
    });

    let client = std::sync::Arc::new(MyHttpClient::new(TestConnector {
        host_port: addr.to_string(),
    }));

    client.connect().await.unwrap();

    let streamed_head =
        MyHttpRequest::new_streamed(Method::POST, "/upload", Version::HTTP_11, &create_headers());

    let (publisher, body) = RequestBodyStream::new(1);

    let streaming_client = client.clone();
    let streamed_request = tokio::spawn(async move {
        streaming_client
            .do_streamed_request(&streamed_head, body, None, Duration::from_secs(5))
            .await
    });

    publisher.publish(b"first-".to_vec()).await.unwrap();

    // A regular request while the body is only half published: its bytes must not cut
    // into the chunked body
    let pipelined_client = client.clone();
    let pipelined_request = tokio::spawn(async move {
        let request = MyHttpRequest::new(
            Method::POST,
            "/pipelined",
            Version::HTTP_11,
            &create_headers(),
            b"pipelined-payload".to_vec(),
        );

        pipelined_client
            .do_request(&request, Duration::from_secs(5))
            .await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    publisher.publish(b"second".to_vec()).await.unwrap();
    drop(publisher);

    let streamed_response = streamed_request.await.unwrap().unwrap();
    assert_eq!(get_response_body(streamed_response).await, b"ok".to_vec());

    let pipelined_response = pipelined_request.await.unwrap().unwrap();
    assert_eq!(get_response_body(pipelined_response).await, b"ok".to_vec());

    let (streamed, pipelined) = server.await.unwrap();

    assert!(streamed.is_chunked());
    assert_eq!(streamed.body, b"first-second".to_vec());

    assert!(!pipelined.is_chunked());
    assert_eq!(pipelined.body, b"pipelined-payload".to_vec());
}

#[tokio::test]
async fn test_streamed_request_is_not_retried_after_the_head_reached_the_wire() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 1024];
        let _ = socket.read(&mut buffer).await.unwrap();
        socket.shutdown().await.unwrap();
        drop(socket);

        // A second connection must not happen: the payload is gone with the producer
        let second_attempt =
            tokio::time::timeout(Duration::from_millis(300), listener.accept()).await;

        second_attempt.is_ok()
    });

    let client = MyHttpClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    client.connect().await.unwrap();

    let request =
        MyHttpRequest::new_streamed(Method::POST, "/upload", Version::HTTP_11, &create_headers());

    let body = StreamBody::new(futures::stream::iter(vec![Ok::<_, String>(Frame::data(
        Bytes::from_static(b"1234567890"),
    ))]));

    let result = client
        .do_streamed_request(&request, body, None, Duration::from_secs(5))
        .await;

    assert!(result.is_err());

    let has_second_attempt = server.await.unwrap();
    assert!(!has_second_attempt);
}

#[tokio::test]
async fn test_a_head_with_content_length_is_refused() {
    let client = MyHttpClient::new(TestConnector {
        host_port: "127.0.0.1:1".to_string(),
    });

    // MyHttpRequest::new writes content-length: a streamed body would contradict it
    let request = MyHttpRequest::new(
        Method::POST,
        "/upload",
        Version::HTTP_11,
        &create_headers(),
        b"1234567890".to_vec(),
    );

    let body = StreamBody::new(futures::stream::iter(
        Vec::<Result<Frame<Bytes>, String>>::new(),
    ));

    let result = client
        .do_streamed_request(&request, body, None, Duration::from_secs(5))
        .await;

    match result {
        Ok(_) => panic!("The request had to be refused"),
        Err(MyHttpClientError::CanNotExecuteRequest(reason)) => {
            assert!(reason.contains("content-length"))
        }
        Err(err) => panic!("Unexpected error: {:?}", err),
    }
}

async fn get_response_body(response: MyHttpResponse<TcpStream>) -> Vec<u8> {
    match response {
        MyHttpResponse::Response(response) => {
            assert_eq!(response.status(), 200);
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec()
        }
        MyHttpResponse::WebSocketUpgrade { .. } => panic!("Unexpected web socket upgrade"),
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

/// `buffer` is carried between the requests of one connection on purpose: a pipelined
/// request often arrives in the same read as the tail of the previous one, and dropping
/// what is left in the buffer would lose it
async fn read_request(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> RequestOnTheWire {
    let headers_end = loop {
        if let Some(index) = find_sub_sequence(buffer, b"\r\n\r\n") {
            break index + 4;
        }

        read_more(socket, buffer).await;
    };

    let headers = String::from_utf8(buffer[..headers_end].to_vec()).unwrap();
    buffer.drain(..headers_end);

    let headers_lc = headers.to_lowercase();

    let body = if headers_lc.contains("transfer-encoding: chunked") {
        read_chunked_body(socket, buffer).await
    } else if let Some(content_length) = get_content_length(&headers_lc) {
        while buffer.len() < content_length {
            read_more(socket, buffer).await;
        }
        let body = buffer[..content_length].to_vec();
        buffer.drain(..content_length);
        body
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
