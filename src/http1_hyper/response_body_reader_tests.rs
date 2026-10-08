//! The body of a response of the HTTP/1.1 client on top of hyper is read through the
//! very same [`BodyReader`] the other clients give - here it is a wrapper of the body
//! hyper gives. Every answer is written to a real socket and read back by the real
//! client.

use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Body;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

use crate::{BodyReader, MyHttpClientError};

use super::streaming_body_tests::TestConnector;
use super::{HyperHttpResponse, MyHttpHyperClient};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

/// A body of any size is taken
const NO_LIMIT: usize = usize::MAX;

/// What the upstream does with the socket once the answer is written
#[derive(Clone, Copy)]
enum Then {
    KeepOpen,
    Close,
}

/// An upstream of a single connection which answers in two parts. The second one is
/// not written until the sender says so, and is never written when the sender is
/// dropped
async fn client_of_an_upstream_answering_in_two_parts(
    first: &'static [u8],
    second: &'static [u8],
    then: Then,
) -> (
    MyHttpHyperClient<TcpStream, TestConnector>,
    oneshot::Sender<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (send_the_second_part, the_second_part_is_due) = oneshot::channel();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut request = [0u8; 4096];
        let _ = socket.read(&mut request).await.unwrap();

        socket.write_all(first).await.unwrap();

        if the_second_part_is_due.await.is_ok() {
            socket.write_all(second).await.unwrap();
        }

        if let Then::KeepOpen = then {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    let client = MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    (client, send_the_second_part)
}

async fn body_reader_of_a_request(
    client: &MyHttpHyperClient<TcpStream, TestConnector>,
    request_timeout: Duration,
) -> BodyReader {
    let request = hyper::Request::builder()
        .method(hyper::Method::GET)
        .uri("/")
        .header("host", "localhost")
        .body(Full::new(Bytes::new()))
        .unwrap();

    match client.do_request(request, request_timeout).await.unwrap() {
        HyperHttpResponse::Response(response) => {
            assert_eq!(response.status(), 200);
            response.into_body()
        }
        #[cfg(feature = "with-websocket")]
        HyperHttpResponse::WebSocketUpgrade { .. } => panic!("Unexpected web socket upgrade"),
    }
}

/// The pieces a body comes in are up to the network, so the test asks for an amount
async fn read_exactly(body: &mut BodyReader, size: usize) -> Vec<u8> {
    let mut result = Vec::new();

    while result.len() < size {
        let piece = body.get_next().await.unwrap();
        result.extend_from_slice(&piece.expect("The body is over before its time"));
    }

    result
}

/// The first half of the body is with the caller while the second one is not even
/// written by the upstream
#[tokio::test]
async fn a_body_is_given_as_it_comes_off_the_socket() {
    let responses: [(&'static [u8], &'static [u8], Option<usize>); 2] = [
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHello",
            b"World",
            Some(10),
        ),
        (
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n",
            b"5\r\nWorld\r\n0\r\n\r\n",
            None,
        ),
    ];

    for (first, second, content_length) in responses {
        let (client, send_the_second_part) =
            client_of_an_upstream_answering_in_two_parts(first, second, Then::KeepOpen).await;

        let mut body = body_reader_of_a_request(&client, REQUEST_TIMEOUT).await;

        assert!(matches!(body, BodyReader::Hyper(_)));
        assert_eq!(body.content_length(), content_length);

        assert_eq!(read_exactly(&mut body, 5).await, b"Hello");
        assert_eq!(body.remains_to_read(), content_length.map(|_| 5));

        send_the_second_part.send(()).unwrap();

        assert_eq!(read_exactly(&mut body, 5).await, b"World");
        assert!(body.get_next().await.unwrap().is_none());

        // A body which is over stays over
        assert!(body.get_next().await.unwrap().is_none());
        assert!(body.is_end_stream());
    }
}

#[tokio::test]
async fn into_vec_gives_the_whole_body() {
    let (client, send_the_second_part) = client_of_an_upstream_answering_in_two_parts(
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHello",
        b"World",
        Then::KeepOpen,
    )
    .await;

    let body = body_reader_of_a_request(&client, REQUEST_TIMEOUT).await;

    send_the_second_part.send(()).unwrap();

    assert_eq!(body.into_vec(NO_LIMIT).await.unwrap(), b"HelloWorld");
}

#[tokio::test]
async fn into_vec_gives_what_is_left_of_a_body_which_is_read_in_part() {
    let (client, send_the_second_part) = client_of_an_upstream_answering_in_two_parts(
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHello",
        b"World",
        Then::KeepOpen,
    )
    .await;

    let mut body = body_reader_of_a_request(&client, REQUEST_TIMEOUT).await;

    assert_eq!(read_exactly(&mut body, 5).await, b"Hello");

    send_the_second_part.send(()).unwrap();

    assert_eq!(body.into_vec(NO_LIMIT).await.unwrap(), b"World");
}

/// The limit is what the caller gives: a body which says it is bigger is refused before
/// it is read, and a body of exactly that size is within it
#[tokio::test]
async fn into_vec_holds_the_body_to_the_limit_it_is_given() {
    let responses: [(&'static [u8], &'static [u8]); 2] = [
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHello",
            b"World",
        ),
        (
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n",
            b"5\r\nWorld\r\n0\r\n\r\n",
        ),
    ];

    for (first, second) in responses {
        let (client, send_the_second_part) =
            client_of_an_upstream_answering_in_two_parts(first, second, Then::KeepOpen).await;

        let body = body_reader_of_a_request(&client, REQUEST_TIMEOUT).await;

        send_the_second_part.send(()).unwrap();

        assert!(matches!(
            body.into_vec(9).await,
            Err(MyHttpClientError::ResponseBodyTooLarge { limit: 9 })
        ));

        let (client, send_the_second_part) =
            client_of_an_upstream_answering_in_two_parts(first, second, Then::KeepOpen).await;

        let body = body_reader_of_a_request(&client, REQUEST_TIMEOUT).await;

        send_the_second_part.send(()).unwrap();

        assert_eq!(body.into_vec(10).await.unwrap(), b"HelloWorld");
    }
}

/// The upstream is gone in the middle of the body: what has been read is not the whole
/// of it, and the body must not end as if it was
#[tokio::test]
async fn a_body_which_is_cut_short_ends_with_an_error() {
    let (client, send_the_second_part) = client_of_an_upstream_answering_in_two_parts(
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHello",
        b"",
        Then::Close,
    )
    .await;

    let mut body = body_reader_of_a_request(&client, REQUEST_TIMEOUT).await;

    assert_eq!(read_exactly(&mut body, 5).await, b"Hello");

    send_the_second_part.send(()).unwrap();

    let Err(MyHttpClientError::CanNotExecuteRequest(reason)) = body.get_next().await else {
        panic!("A body which is cut short has to end with an error");
    };

    assert!(
        reason.starts_with("The response body is not complete: "),
        "{}",
        reason
    );

    // It keeps failing: asking once more does not make the body complete
    let Err(MyHttpClientError::CanNotExecuteRequest(the_same_reason)) = body.get_next().await
    else {
        panic!("A body which has failed has to keep failing");
    };

    assert_eq!(the_same_reason, reason);
}

/// The timeout the request is sent with covers reading its body into memory
#[tokio::test]
async fn into_vec_is_bounded_by_the_timeout_of_the_request() {
    let (client, _the_second_part_never_comes) = client_of_an_upstream_answering_in_two_parts(
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHello",
        b"World",
        Then::KeepOpen,
    )
    .await;

    let request_timeout = Duration::from_millis(300);
    let started = tokio::time::Instant::now();

    let body = body_reader_of_a_request(&client, request_timeout).await;
    let result = body.into_vec(NO_LIMIT).await;

    assert!(
        matches!(result, Err(MyHttpClientError::RequestTimeout(timeout)) if timeout == request_timeout)
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

/// Far more than hyper, the reader and both sockets hold between them: an upstream can
/// not get rid of a body of this size unless somebody reads it
const BIG_BODY_SIZE: usize = 32 * 1024 * 1024;

/// hyper is not asked for more than the reader takes, so a body nobody reads is not
/// piled up in memory: it stays with the upstream. It goes on as soon as the body is
/// read, and nothing of it is lost on the way
#[tokio::test]
async fn a_body_nobody_reads_stays_with_the_upstream() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let the_body_is_written = std::sync::Arc::new(AtomicBool::new(false));
    let the_body_is_written_by_the_upstream = the_body_is_written.clone();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut request = [0u8; 4096];
        let _ = socket.read(&mut request).await.unwrap();

        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            BIG_BODY_SIZE
        );
        socket.write_all(head.as_bytes()).await.unwrap();

        let written = socket.write_all(&vec![7u8; BIG_BODY_SIZE]).await.is_ok();
        the_body_is_written_by_the_upstream.store(written, Ordering::SeqCst);

        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let client = MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    let mut body = body_reader_of_a_request(&client, REQUEST_TIMEOUT).await;

    // The head is here and the upstream is writing the body with all it has got
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!the_body_is_written.load(Ordering::SeqCst));

    let mut received = 0;

    while let Some(piece) = body.get_next().await.unwrap() {
        assert!(piece.iter().all(|byte| *byte == 7));
        received += piece.len();
    }

    assert_eq!(received, BIG_BODY_SIZE);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(the_body_is_written.load(Ordering::SeqCst));
}

/// The reader is dropped while the upstream is silent in the middle of the body. The
/// body of hyper is dropped with it, which is what makes hyper give the connection up
#[tokio::test]
async fn a_reader_which_is_dropped_lets_the_connection_go() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (the_connection_is_closed, is_the_connection_closed) = oneshot::channel();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut request = [0u8; 4096];
        let _ = socket.read(&mut request).await.unwrap();

        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHello")
            .await
            .unwrap();

        // The rest of the body never comes. What comes is the end of the connection
        let closed = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut request)).await;
        let _ = the_connection_is_closed.send(matches!(closed, Ok(Ok(0)) | Ok(Err(_))));
    });

    let client = MyHttpHyperClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    let mut body = body_reader_of_a_request(&client, REQUEST_TIMEOUT).await;

    assert_eq!(read_exactly(&mut body, 5).await, b"Hello");
    drop(body);

    assert!(is_the_connection_closed.await.unwrap());
}

/// The body as `rust_extensions::AsyncIterator` sees it: the portions are the pieces
/// hyper gives, read through a shared reference - the rest of them by another task
#[tokio::test]
async fn a_body_reader_is_an_async_iterator_of_bytes() {
    let (client, send_the_second_part) = client_of_an_upstream_answering_in_two_parts(
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHello",
        b"World",
        Then::KeepOpen,
    )
    .await;

    let body: std::sync::Arc<
        dyn rust_extensions::AsyncIterator<u8, MyHttpClientError> + Send + Sync + 'static,
    > = std::sync::Arc::new(body_reader_of_a_request(&client, REQUEST_TIMEOUT).await);

    let mut the_first_half = Vec::new();

    while the_first_half.len() < 5 {
        the_first_half.extend(body.get_next().await.unwrap().unwrap());
    }

    assert_eq!(the_first_half, b"Hello");

    send_the_second_part.send(()).unwrap();

    let the_rest = tokio::spawn(async move {
        let mut result = Vec::new();

        while let Some(portion) = body.get_next().await.unwrap() {
            result.extend(portion);
        }

        result
    });

    assert_eq!(the_rest.await.unwrap(), b"World");
}
