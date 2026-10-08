//! The calls which used to panic on a state they did not expect: a buffer nothing was
//! read into, a client which is disposed, a connection with no write loop, a task which
//! is not what it is asked to be. None of them is reachable through the client itself -
//! they are public, so they answer with an error or a `None` instead.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Version};
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use tokio::io::DuplexStream;
use tokio::net::TcpStream;

use crate::{
    http1_hyper::MyHttpHyperClient, MyHttpClientDisconnect, MyHttpClientError,
    MyHttpClientHeadersBuilder,
};

use super::streaming_body_tests::TestConnector;
use super::{
    HttpTask, MyHttpClientDisconnection, MyHttpClientInner, MyHttpRequest, QueueOfRequests,
    TcpBuffer,
};

const TIMEOUT: Duration = Duration::from_secs(5);

fn new_inner() -> Arc<MyHttpClientInner<DuplexStream>> {
    Arc::new(MyHttpClientInner::new("test".to_string(), None))
}

fn empty_response() -> hyper::Response<BoxBody<Bytes, String>> {
    hyper::Response::new(Full::new(Bytes::new()).map_err(|e| e.to_string()).boxed())
}

fn get_request() -> MyHttpRequest {
    let mut headers = MyHttpClientHeadersBuilder::new();
    headers.add_header("host", "localhost").unwrap();
    MyHttpRequest::new(Method::GET, "/", Version::HTTP_11, &headers, vec![]).unwrap()
}

#[test]
fn a_buffer_nothing_was_read_into_has_no_line() {
    let mut buffer = TcpBuffer::new();

    assert!(buffer.read_until_crlf().is_none());
}

#[tokio::test]
async fn a_task_which_is_not_a_websocket_upgrade_gives_no_upgrade() {
    let response =
        HttpTask::<DuplexStream>::Response(hyper::Response::new(crate::BodyReader::empty()));
    assert!(response.into_websocket_upgrade().is_none());

    let (client, _upstream) = tokio::io::duplex(64);
    let (read_part, _write_part) = tokio::io::split(client);

    let upgrade = HttpTask::WebsocketUpgrade {
        response: empty_response(),
        read_part,
        leftover: b"frame".to_vec(),
    };

    let (_, _, leftover) = upgrade.into_websocket_upgrade().unwrap();
    assert_eq!(leftover, b"frame");
}

#[cfg(feature = "with-websocket")]
#[test]
fn an_upgrade_response_which_is_taken_already_is_an_error() {
    let mut builder = super::WebSocketUpgradeBuilder::new(http::Response::builder().status(101));

    assert!(builder.take_upgrade_response().is_ok());
    assert!(builder.take_upgrade_response().is_err());
}

#[tokio::test]
async fn a_client_which_is_disposed_takes_no_connection() {
    let inner = new_inner();
    inner.dispose().await;

    let (client, _upstream) = tokio::io::duplex(64);
    let (_read_half, write_half) = tokio::io::split(client);

    let result = inner.new_connection(1, write_half, TIMEOUT).await;

    assert!(matches!(result, Err(MyHttpClientError::Disposed)));
}

/// The connection is set by hand here, the way nothing but a test does it: the write
/// loop is started by `MyHttpClient::connect`, and without it a request would be queued
/// with nobody to put it on the wire
#[tokio::test]
async fn a_request_is_refused_while_there_is_no_write_loop() {
    let inner = new_inner();

    let (client, _upstream) = tokio::io::duplex(64);
    let (_read_half, write_half) = tokio::io::split(client);
    inner.new_connection(1, write_half, TIMEOUT).await.unwrap();

    let result = inner.send(&get_request()).await;

    assert!(matches!(result, Err(MyHttpClientError::Disconnected)));
    assert_eq!(inner.queue_of_requests.peek_front_method(), None);
}

/// A drop is where it happens: the runtime may be gone by then
#[test]
fn disconnecting_outside_of_a_runtime_does_not_panic() {
    let disconnection = MyHttpClientDisconnection::new(new_inner(), 1);
    disconnection.disconnect();
    disconnection.web_socket_disconnect();

    let client: MyHttpHyperClient<TcpStream, TestConnector> =
        MyHttpHyperClient::new(TestConnector {
            host_port: "127.0.0.1:1".to_string(),
        });
    client.disconnect();
}

#[tokio::test]
async fn a_request_which_is_dropped_with_no_result_is_an_error_for_its_caller() {
    let queue: QueueOfRequests<DuplexStream> = QueueOfRequests::new();

    let mut task = rust_extensions::TaskCompletion::new();
    let awaiter = task.get_awaiter();
    queue.push(Method::GET, task);

    drop(queue);

    let result = tokio::spawn(awaiter.get_result())
        .await
        .unwrap_or_else(|_| panic!("The caller of the request has panicked"));

    assert!(matches!(
        result,
        Err(MyHttpClientError::CanNotExecuteRequest(_))
    ));
}
