//! A response body which is too big to be held before the caller sees it - one with a
//! `Content-Length` above `STREAMED_BODY_THRESHOLD`, or a close-delimited one - streams:
//! `do_request` returns on the head and the body follows. Smaller bodies are read whole.

use std::time::Duration;

use http::{Method, Version};
use http_body_util::BodyExt;
use hyper::body::Body;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

use crate::{MyHttpClientHeadersBuilder, MyHttpClientError};

use super::streaming_body_tests::TestConnector;
use super::{MyHttpClient, MyHttpRequest, MyHttpResponse, STREAMED_BODY_THRESHOLD};

const BIG: usize = 1024 * 1024;

fn get_request() -> MyHttpRequest {
    let mut headers = MyHttpClientHeadersBuilder::new();
    headers.add_header("host", "localhost").unwrap();
    MyHttpRequest::new(Method::GET, "/", Version::HTTP_11, &headers, vec![]).unwrap()
}

fn with_length(body: &[u8]) -> Vec<u8> {
    let mut answer = format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n", body.len()).into_bytes();
    answer.extend_from_slice(body);
    answer
}

fn body_of(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// Reads one request head off the socket. The tests send no request bodies
async fn read_request_head(socket: &mut TcpStream) {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if socket.read(&mut byte).await.unwrap() == 0 {
            return;
        }
        head.push(byte[0]);
    }
}

async fn start() -> (TcpListener, MyHttpClient<TcpStream, TestConnector>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = MyHttpClient::new(TestConnector {
        host_port: listener.local_addr().unwrap().to_string(),
    });
    (listener, client)
}

async fn do_get(
    client: &MyHttpClient<TcpStream, TestConnector>,
) -> Result<crate::HyperResponse, MyHttpClientError> {
    match client.do_request(&get_request(), Duration::from_secs(5)).await? {
        MyHttpResponse::Response(response) => Ok(response),
        MyHttpResponse::WebSocketUpgrade { .. } => panic!("Unexpected web socket upgrade"),
    }
}

async fn collect(response: crate::HyperResponse) -> Result<Vec<u8>, String> {
    Ok(response.into_body().collect().await?.to_bytes().to_vec())
}

#[tokio::test]
async fn a_small_body_is_read_whole_before_the_response_is_handed_over() {
    let (listener, client) = start().await;
    let body = body_of(STREAMED_BODY_THRESHOLD);

    let answer = with_length(&body);
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_head(&mut socket).await;
        socket.write_all(&answer).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });

    let response = do_get(&client).await.unwrap();
    assert_eq!(response.body().size_hint().exact(), Some(body.len() as u64));
    assert_eq!(collect(response).await.unwrap(), body);
}

#[tokio::test]
async fn a_big_body_streams_behind_the_head() {
    let (listener, client) = start().await;
    let body = body_of(BIG);
    let (release_sender, release) = oneshot::channel::<()>();

    let answer = with_length(&body);
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_head(&mut socket).await;

        // The head and a part of the body; the rest waits until the response is out
        let split_at = answer.len() / 2;
        socket.write_all(&answer[..split_at]).await.unwrap();
        release.await.unwrap();
        socket.write_all(&answer[split_at..]).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });

    // Would time out if the client waited for the whole body
    let response = tokio::time::timeout(Duration::from_secs(2), do_get(&client))
        .await
        .expect("the response is handed over before its body is complete")
        .unwrap();

    assert_eq!(response.status(), 200);
    release_sender.send(()).unwrap();

    assert_eq!(collect(response).await.unwrap(), body);
}

#[tokio::test]
async fn the_connection_serves_the_next_request_after_a_streamed_body() {
    let (listener, client) = start().await;
    let first = body_of(BIG);
    let second = body_of(BIG + 1);

    let answers = [with_length(&first), with_length(&second)];
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        for answer in answers {
            read_request_head(&mut socket).await;
            socket.write_all(&answer).await.unwrap();
        }
        // A second connection would be accepted here and fail the test
        tokio::time::timeout(Duration::from_millis(300), listener.accept())
            .await
            .is_err()
    });

    assert_eq!(collect(do_get(&client).await.unwrap()).await.unwrap(), first);
    assert_eq!(collect(do_get(&client).await.unwrap()).await.unwrap(), second);

    assert!(server.await.unwrap(), "the connection was not reused");
}

#[tokio::test]
async fn a_body_cut_short_fails_the_body_with_the_reason() {
    let (listener, client) = start().await;
    let body = body_of(BIG);

    let answer = with_length(&body);
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_head(&mut socket).await;
        socket.write_all(&answer[..answer.len() - 10]).await.unwrap();
        // Dropping the socket closes the connection in the middle of the body
    });

    let response = do_get(&client).await.unwrap();
    let err = collect(response).await.unwrap_err();
    assert_eq!(
        err,
        "The response body is not complete: the connection is closed"
    );
}

#[tokio::test]
async fn a_body_given_up_on_leaves_the_connection_and_the_next_request_dials_a_new_one() {
    let (listener, client) = start().await;
    let body = body_of(BIG * 8);

    let answer = with_length(&body);
    let small = with_length(b"ok");
    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        read_request_head(&mut first).await;
        // The client stops reading: the write may not go through
        let writer = tokio::spawn(async move {
            let _ = first.write_all(&answer).await;
        });

        let (mut second, _) = listener.accept().await.unwrap();
        read_request_head(&mut second).await;
        second.write_all(&small).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        writer.abort();
    });

    let response = do_get(&client).await.unwrap();
    let mut body = response.into_body();
    let first_frame = body.frame().await.unwrap().unwrap();
    assert!(first_frame.is_data());
    drop(body);

    // The read loop notices the body is gone once it has a frame to give
    let mut response = None;
    for _ in 0..50 {
        match do_get(&client).await {
            Ok(ok) => {
                response = Some(ok);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }

    let response = response.expect("a request after the body was given up on");
    assert_eq!(collect(response).await.unwrap(), b"ok");
}

#[tokio::test]
async fn a_close_delimited_body_streams_until_the_connection_is_closed() {
    let (listener, client) = start().await;
    let body = body_of(BIG);
    let (release_sender, release) = oneshot::channel::<()>();

    let rest = body[BIG / 2..].to_vec();
    let mut head_and_half = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
    head_and_half.extend_from_slice(&body[..BIG / 2]);
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_head(&mut socket).await;
        socket.write_all(&head_and_half).await.unwrap();
        release.await.unwrap();
        socket.write_all(&rest).await.unwrap();
    });

    let response = tokio::time::timeout(Duration::from_secs(2), do_get(&client))
        .await
        .expect("the response is handed over before the connection is closed")
        .unwrap();
    release_sender.send(()).unwrap();

    assert_eq!(collect(response).await.unwrap(), body);
}
