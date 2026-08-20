use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rust_extensions::remote_endpoint::RemoteEndpoint;
use tokio::{
    io::{ReadHalf, WriteHalf},
    net::{TcpListener, TcpStream},
    sync::mpsc::UnboundedReceiver,
};

use crate::{MyHttpClientConnector, MyHttpClientError, RequestBodyStream};

use super::MyHttp2Client;

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

/// What the h2 upstream has seen
pub struct RequestOnTheWire {
    pub path: String,
    pub content_length: Option<usize>,
    pub body: Vec<u8>,
}

#[tokio::test]
async fn test_streamed_body_is_sent_without_a_content_length() {
    let (addr, mut requests) = start_h2_server().await;

    let client = MyHttp2Client::new(TestConnector {
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
        .uri("https://localhost/upload")
        .body(body)
        .unwrap();

    let response = client
        .do_streamed_request(request, None, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(get_response_body(response).await, b"ok".to_vec());

    let request_on_the_wire = requests.recv().await.unwrap();

    // h2 has no chunked encoding: the size is simply not announced
    assert_eq!(request_on_the_wire.content_length, None);
    assert_eq!(request_on_the_wire.body, b"first-second".to_vec());
}

#[tokio::test]
async fn test_streamed_body_of_a_known_size_is_sent_with_content_length() {
    let (addr, mut requests) = start_h2_server().await;

    let client = MyHttp2Client::new(TestConnector {
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
        .uri("https://localhost/upload")
        .body(body)
        .unwrap();

    let response = client
        .do_streamed_request(request, Some(12), Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(get_response_body(response).await, b"ok".to_vec());

    let request_on_the_wire = requests.recv().await.unwrap();

    assert_eq!(request_on_the_wire.content_length, Some(12));
    assert_eq!(request_on_the_wire.body, b"first-second".to_vec());
}

#[tokio::test]
async fn test_a_streamed_request_does_not_block_the_other_requests() {
    let (addr, mut requests) = start_h2_server().await;

    let client = std::sync::Arc::new(MyHttp2Client::new(TestConnector {
        host_port: addr.to_string(),
    }));

    let (publisher, body) = RequestBodyStream::new(1);

    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri("https://localhost/upload")
        .body(body)
        .unwrap();

    let streaming_client = client.clone();
    let streamed_request = tokio::spawn(async move {
        streaming_client
            .do_streamed_request(request, None, Duration::from_secs(5))
            .await
    });

    publisher.publish(b"first-".to_vec()).await.unwrap();

    // The upload is only half way through - and a plain request through the very same
    // connection goes there and back, because h2 multiplexes
    let plain_request = hyper::Request::builder()
        .method(hyper::Method::GET)
        .uri("https://localhost/plain")
        .body(Full::new(Bytes::new()))
        .unwrap();

    let plain_response = client
        .do_request(&plain_request, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(get_response_body(plain_response).await, b"ok".to_vec());
    assert!(
        !streamed_request.is_finished(),
        "the plain request had to come back while the upload is still going on"
    );

    publisher.publish(b"second".to_vec()).await.unwrap();
    drop(publisher);

    let streamed_response = streamed_request.await.unwrap().unwrap();
    assert_eq!(get_response_body(streamed_response).await, b"ok".to_vec());

    let first = requests.recv().await.unwrap();
    let second = requests.recv().await.unwrap();

    // The plain request is served first even though the streamed one started earlier
    assert_eq!(first.path, "/plain");
    assert_eq!(second.path, "/upload");
    assert_eq!(second.body, b"first-second".to_vec());
}

#[tokio::test]
async fn test_a_body_shorter_than_the_announced_content_length_fails() {
    let (addr, _requests) = start_h2_server().await;

    let client = MyHttp2Client::new(TestConnector {
        host_port: addr.to_string(),
    });

    let body = StreamBody::new(futures::stream::iter(vec![Ok::<_, String>(Frame::data(
        Bytes::from_static(b"12345"),
    ))]));

    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri("https://localhost/upload")
        .body(body)
        .unwrap();

    let result = client
        .do_streamed_request(request, Some(10), Duration::from_secs(5))
        .await;

    // hyper refuses to send a body which does not match the announced content-length
    assert!(result.is_err());
}

async fn get_response_body(
    response: hyper::Response<http_body_util::combinators::BoxBody<Bytes, String>>,
) -> Vec<u8> {
    assert_eq!(response.status(), 200);
    response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec()
}

/// Serves h2 with prior knowledge - the very same thing the client speaks after its
/// handshake, no TLS/ALPN in between
async fn start_h2_server() -> (std::net::SocketAddr, UnboundedReceiver<RequestOnTheWire>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();

        let service =
            hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let sender = sender.clone();
                async move {
                    let path = req.uri().path().to_string();

                    let content_length = req
                        .headers()
                        .get(http::header::CONTENT_LENGTH)
                        .map(|value| value.to_str().unwrap().parse::<usize>().unwrap());

                    let body = req.into_body().collect().await.unwrap().to_bytes().to_vec();

                    let _ = sender.send(RequestOnTheWire {
                        path,
                        content_length,
                        body,
                    });

                    Ok::<_, hyper::Error>(hyper::Response::new(Full::new(Bytes::from_static(
                        b"ok",
                    ))))
                }
            });

        let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(socket), service)
            .await;
    });

    (addr, receiver)
}
