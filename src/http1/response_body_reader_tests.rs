//! How the body of a response of the non-hyper client is read. The response is handed
//! over on its head, and the body follows through the [`BodyReader`]: the read loop
//! sends it there as it comes off the socket, and stops reading the socket when nobody
//! takes it - which is why the connection is busy with a body until it is read. Every
//! answer is written to a real socket and read back by the real client.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use bytes::Bytes;
use hyper::body::Body;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

use crate::{BodyReader, MyHttpClientError};

use super::response_input_tests::{
    client_of_an_upstream_answering, get_request, read_request_heads, Then,
};
use super::streaming_body_tests::TestConnector;
use super::{
    MyHttpClient, MyHttpClientMetrics, MyHttpResponse, MAX_ABANDONED_BODY_SIZE,
    MAX_RESPONSE_BODY_PIECE_SIZE,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

/// A body of any size is taken
const NO_LIMIT: usize = usize::MAX;

/// Far more than the client and both sockets hold between them: an upstream can not
/// get rid of a body of this size unless somebody reads it
const BIG_BODY_SIZE: usize = 32 * 1024 * 1024;

/// Each of them carries `HelloWorld`, and each is cut right in the middle of it
const BODIES_IN_TWO_PARTS: [(&[u8], &[u8], Then); 3] = [
    (
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHello",
        b"World",
        Then::KeepOpen,
    ),
    // The cut is in the middle of a chunk
    (
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\na\r\nHello",
        b"World\r\n0\r\n\r\n",
        Then::KeepOpen,
    ),
    (b"HTTP/1.1 200 OK\r\n\r\nHello", b"World", Then::Close),
];

async fn upstream() -> (TcpListener, MyHttpClient<TcpStream, TestConnector>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let client = MyHttpClient::new(TestConnector {
        host_port: listener.local_addr().unwrap().to_string(),
    });

    (listener, client)
}

/// An upstream of a single connection which answers in two parts. The second one is
/// not written until the sender says so, and is never written when the sender is
/// dropped
async fn client_of_an_upstream_answering_in_two_parts(
    first: &'static [u8],
    second: &'static [u8],
    then: Then,
) -> (MyHttpClient<TcpStream, TestConnector>, oneshot::Sender<()>) {
    let (listener, client) = upstream().await;
    let (send_the_second_part, the_second_part_is_due) = oneshot::channel();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_heads(&mut socket, 1).await;
        socket.write_all(first).await.unwrap();

        if the_second_part_is_due.await.is_ok() {
            socket.write_all(second).await.unwrap();

            if let Then::Close = then {
                return;
            }
        }

        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    (client, send_the_second_part)
}

fn big_body() -> Vec<u8> {
    let pattern: Vec<u8> = (0..=250).collect();
    pattern.repeat(BIG_BODY_SIZE / pattern.len() + 1)[..BIG_BODY_SIZE].to_vec()
}

/// Writes a response with [`big_body`] in it, as far as the client takes it. `true` is
/// a body which is written to its end
async fn write_big_body(socket: &mut TcpStream) -> bool {
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
        BIG_BODY_SIZE
    );

    socket.write_all(head.as_bytes()).await.unwrap();
    socket.write_all(&big_body()).await.is_ok()
}

fn body_reader_of(response: MyHttpResponse<TcpStream>) -> BodyReader {
    match response {
        MyHttpResponse::Response(response) => {
            assert_eq!(response.status(), 200);
            response.into_body()
        }
        MyHttpResponse::WebSocketUpgrade { .. } => panic!("Unexpected web socket upgrade"),
    }
}

async fn body_reader_of_a_request(client: &MyHttpClient<TcpStream, TestConnector>) -> BodyReader {
    let response = client
        .do_request(&get_request(), REQUEST_TIMEOUT)
        .await
        .unwrap();

    body_reader_of(response)
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

fn error_of<T>(result: Result<T, MyHttpClientError>) -> String {
    match result {
        Ok(_) => panic!("An error is expected"),
        Err(err) => format!("{:?}", err),
    }
}

/// The first half of the body is with the caller while the second one is not even
/// written by the upstream: nothing waits for the body, or a chunk of it, to be complete
#[tokio::test]
async fn a_body_is_given_as_it_comes_off_the_socket() {
    for (first, second, then) in BODIES_IN_TWO_PARTS {
        let (client, send_the_second_part) =
            client_of_an_upstream_answering_in_two_parts(first, second, then).await;

        let mut body = body_reader_of_a_request(&client).await;

        assert!(matches!(body, BodyReader::NoHyper(_)));

        assert_eq!(read_exactly(&mut body, 5).await, b"Hello");

        send_the_second_part.send(()).unwrap();

        assert_eq!(read_exactly(&mut body, 5).await, b"World");
        assert!(body.get_next().await.unwrap().is_none());

        // A body which is over stays over
        assert!(body.get_next().await.unwrap().is_none());
    }
}

/// The size of the body is with its reader when the response says it
#[tokio::test]
async fn a_body_reader_knows_the_content_length_of_the_response() {
    let responses: [(&'static [u8], Then, Option<usize>); 4] = [
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHello",
            Then::KeepOpen,
            Some(5),
        ),
        (b"HTTP/1.1 204 No Content\r\n\r\n", Then::KeepOpen, Some(0)),
        (
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n0\r\n\r\n",
            Then::KeepOpen,
            None,
        ),
        (b"HTTP/1.1 200 OK\r\n\r\nHello", Then::Close, None),
    ];

    for (answer, then, expected) in responses {
        let client = client_of_an_upstream_answering(answer, then).await;

        let response = client
            .do_request(&get_request(), REQUEST_TIMEOUT)
            .await
            .unwrap();

        let MyHttpResponse::Response(response) = response else {
            panic!("Unexpected web socket upgrade");
        };

        assert_eq!(
            response.body().content_length(),
            expected,
            "{:?}",
            String::from_utf8_lossy(answer)
        );
    }
}

#[tokio::test]
async fn into_vec_gives_the_whole_body() {
    let responses: [(&'static [u8], Then, &[u8]); 5] = [
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHello",
            Then::KeepOpen,
            b"Hello",
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
            Then::KeepOpen,
            b"",
        ),
        (
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n6\r\n World\r\n0\r\n\r\n",
            Then::KeepOpen,
            b"Hello World",
        ),
        // The trailers are not a part of the body
        (
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n0\r\nX-Checksum: 1\r\n\r\n",
            Then::KeepOpen,
            b"Hello",
        ),
        (b"HTTP/1.1 200 OK\r\n\r\nHello", Then::Close, b"Hello"),
    ];

    for (response, then, expected) in responses {
        let client = client_of_an_upstream_answering(response, then).await;

        let body = body_reader_of_a_request(&client).await;

        assert_eq!(
            body.into_vec(NO_LIMIT).await.unwrap(),
            expected,
            "{:?}",
            String::from_utf8_lossy(response)
        );
    }
}

/// Bigger than the read buffer and than what a socket holds: it can not come in one
/// piece, and the upstream can not even write it until it is being read
#[tokio::test]
async fn into_vec_gives_a_body_which_is_bigger_than_the_read_buffer() {
    let (listener, client) = upstream().await;

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_heads(&mut socket, 1).await;
        write_big_body(&mut socket).await;

        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let body = body_reader_of_a_request(&client)
        .await
        .into_vec(NO_LIMIT)
        .await;

    assert!(body.unwrap() == big_body());
}

#[tokio::test]
async fn into_vec_gives_what_is_left_of_a_body_which_is_read_in_part() {
    for (first, second, then) in BODIES_IN_TWO_PARTS {
        let (client, send_the_second_part) =
            client_of_an_upstream_answering_in_two_parts(first, second, then).await;

        let mut body = body_reader_of_a_request(&client).await;

        assert_eq!(read_exactly(&mut body, 5).await, b"Hello");

        send_the_second_part.send(()).unwrap();

        assert_eq!(body.into_vec(NO_LIMIT).await.unwrap(), b"World");
    }
}

/// The client does not hold a body nobody reads: once a few pieces wait for the reader
/// the socket is not read any more, and the rest of the body stays with the upstream.
/// It goes on as soon as the body is read, and nothing of it is lost on the way
#[tokio::test]
async fn a_body_nobody_reads_stays_with_the_upstream() {
    let (listener, client) = upstream().await;

    let the_body_is_written = Arc::new(AtomicBool::new(false));
    let the_body_is_written_by_the_upstream = the_body_is_written.clone();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_heads(&mut socket, 1).await;

        let written = write_big_body(&mut socket).await;
        the_body_is_written_by_the_upstream.store(written, Ordering::SeqCst);

        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let mut body = body_reader_of_a_request(&client).await;

    // The head is here and the upstream is writing the body with all it has got
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!the_body_is_written.load(Ordering::SeqCst));

    let expected = big_body();
    let mut received = 0;

    while let Some(piece) = body.get_next().await.unwrap() {
        assert!(piece.len() <= MAX_RESPONSE_BODY_PIECE_SIZE);
        assert!(piece[..] == expected[received..received + piece.len()]);
        received += piece.len();
    }

    assert_eq!(received, BIG_BODY_SIZE);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(the_body_is_written.load(Ordering::SeqCst));
}

/// The upstream is gone in the middle of a body which says how long it is: what has
/// been read is not the whole of it, and the body must not end as if it was
#[tokio::test]
async fn a_body_which_is_cut_short_ends_with_an_error() {
    const NOT_COMPLETE: &str =
        "CanNotExecuteRequest(\"The response body is not complete: the connection is closed\")";

    let responses: [&'static [u8]; 2] = [
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHello",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\na\r\nHello",
    ];

    for response in responses {
        let client = client_of_an_upstream_answering(response, Then::Close).await;
        let mut body = body_reader_of_a_request(&client).await;

        assert_eq!(read_exactly(&mut body, 5).await, b"Hello");
        assert_eq!(error_of(body.get_next().await), NOT_COMPLETE);

        // It keeps failing: asking once more does not make the body complete
        assert_eq!(error_of(body.get_next().await), NOT_COMPLETE);
        assert_eq!(error_of(body.into_vec(NO_LIMIT).await), NOT_COMPLETE);

        let client = client_of_an_upstream_answering(response, Then::Close).await;
        let body = body_reader_of_a_request(&client).await;

        assert_eq!(error_of(body.into_vec(NO_LIMIT).await), NOT_COMPLETE);
    }
}

/// Two requests are pipelined into one connection. The answer to the second one is
/// behind the body of the first one on the wire, so it is read once that body is - and
/// by the same connection: the upstream accepts only one
#[tokio::test]
async fn the_connection_goes_on_with_the_next_response_once_the_body_is_read() {
    for (first, second, then) in BODIES_IN_TWO_PARTS {
        // A close-delimited body is the last thing its connection carries
        if let Then::Close = then {
            continue;
        }

        let (listener, client) = upstream().await;
        let (send_the_second_part, the_second_part_is_due) = oneshot::channel::<()>();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request_heads(&mut socket, 2).await;

            socket.write_all(first).await.unwrap();
            the_second_part_is_due.await.unwrap();
            socket.write_all(second).await.unwrap();

            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        client.connect().await.unwrap();

        let the_first_one = async {
            let mut body = body_reader_of_a_request(&client).await;
            let mut result = read_exactly(&mut body, 5).await;

            send_the_second_part.send(()).unwrap();

            result.extend_from_slice(&body.into_vec(NO_LIMIT).await.unwrap());
            result
        };

        let the_second_one = async {
            body_reader_of_a_request(&client)
                .await
                .into_vec(NO_LIMIT)
                .await
                .unwrap()
        };

        let (the_first_one, the_second_one) = tokio::join!(the_first_one, the_second_one);

        assert_eq!(the_first_one, b"HelloWorld");
        assert_eq!(the_second_one, b"ok");
    }
}

/// A body which is small enough to wait for its reader as a whole is off the wire at
/// once, so the connection is not busy with it: the next response is there while this
/// one is not even looked at
#[tokio::test]
async fn a_small_body_which_is_not_read_does_not_hold_the_connection() {
    let (listener, client) = upstream().await;

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_heads(&mut socket, 2).await;

        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHelloHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    client.connect().await.unwrap();

    let request = get_request();

    let (the_first_one, the_second_one) = tokio::join!(
        client.do_request(&request, REQUEST_TIMEOUT),
        client.do_request(&request, REQUEST_TIMEOUT)
    );

    let the_first_one = body_reader_of(the_first_one.unwrap());
    let the_second_one = body_reader_of(the_second_one.unwrap());

    assert_eq!(the_second_one.into_vec(NO_LIMIT).await.unwrap(), b"ok");
    assert_eq!(the_first_one.into_vec(NO_LIMIT).await.unwrap(), b"Hello");
}

/// The response is dropped with its body half way through the wire. What is left of
/// the body is read past, and the very same connection serves the next request
#[tokio::test]
async fn a_body_which_is_dropped_unread_is_read_past() {
    for (first, second, then) in BODIES_IN_TWO_PARTS {
        // There is nothing to keep: the connection is closed by the end of the body
        if let Then::Close = then {
            continue;
        }

        let (listener, client) = upstream().await;
        let (send_the_second_part, the_second_part_is_due) = oneshot::channel::<()>();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request_heads(&mut socket, 1).await;

            socket.write_all(first).await.unwrap();
            the_second_part_is_due.await.unwrap();
            socket.write_all(second).await.unwrap();

            read_request_heads(&mut socket, 1).await;

            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        let response = client
            .do_request(&get_request(), REQUEST_TIMEOUT)
            .await
            .unwrap();

        assert_eq!(response.status(), 200);
        drop(response);

        send_the_second_part.send(()).unwrap();

        let body = body_reader_of_a_request(&client).await;
        assert_eq!(body.into_vec(NO_LIMIT).await.unwrap(), b"ok");
    }
}

/// The rest of the body is not worth the connection: there is too much of it to
/// download, or it does not come - an event stream nobody listens to any more. The
/// connection is closed, and the next request is served by a new one
#[tokio::test]
async fn a_body_which_is_dropped_with_too_much_left_ends_the_connection() {
    let responses = [
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\nHello",
            MAX_ABANDONED_BODY_SIZE + 6
        ),
        // The upstream keeps the body open and sends nothing
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n".to_string(),
    ];

    for response in responses {
        let (listener, client) = upstream().await;

        tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            read_request_heads(&mut first, 1).await;
            first.write_all(response.as_bytes()).await.unwrap();

            let (mut second, _) = listener.accept().await.unwrap();
            read_request_heads(&mut second, 1).await;

            second
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(first);
        });

        let mut body = body_reader_of_a_request(&client).await;
        assert_eq!(read_exactly(&mut body, 5).await, b"Hello");
        drop(body);

        let body = body_reader_of_a_request(&client).await;
        assert_eq!(body.into_vec(NO_LIMIT).await.unwrap(), b"ok");
    }
}

/// The reader is dropped while the upstream is in the middle of sending a body which
/// is far too big to be read past. The read loop is not reading the socket then - it
/// waits for the reader to take what is read already - and it must notice that the
/// reader is gone
#[tokio::test]
async fn a_big_body_which_is_dropped_half_read_ends_the_connection() {
    let (listener, client) = upstream().await;

    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        read_request_heads(&mut first, 1).await;

        let writing_the_body = tokio::spawn(async move {
            write_big_body(&mut first).await;
        });

        let (mut second, _) = listener.accept().await.unwrap();
        read_request_heads(&mut second, 1).await;

        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(30)).await;
        drop(writing_the_body);
    });

    let mut body = body_reader_of_a_request(&client).await;
    assert!(body.get_next().await.unwrap().is_some());

    // The pieces which are read off the socket pile up, until the read loop waits
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(body);

    let body = body_reader_of_a_request(&client).await;
    assert_eq!(body.into_vec(NO_LIMIT).await.unwrap(), b"ok");
}

/// The caller has given up before the answer. The answer comes with a body nobody is
/// going to read - the read loop must not wait for a reader which does not exist, and
/// the next request must not be written into a connection which is stuck in that body
#[tokio::test]
async fn a_body_of_a_response_nobody_waits_for_does_not_hold_the_connection() {
    let (listener, client) = upstream().await;

    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        read_request_heads(&mut first, 1).await;

        // The caller times out before the answer
        tokio::time::sleep(Duration::from_millis(400)).await;

        let writing_the_body = tokio::spawn(async move {
            write_big_body(&mut first).await;
        });

        // A client which has noticed that dials again
        if let Ok(Ok((mut second, _))) =
            tokio::time::timeout(Duration::from_secs(10), listener.accept()).await
        {
            read_request_heads(&mut second, 1).await;
            second
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        }

        drop(writing_the_body);
    });

    let request = get_request();

    let result = client
        .do_request(&request, Duration::from_millis(100))
        .await;
    assert!(matches!(result, Err(MyHttpClientError::RequestTimeout(_))));

    // The answer arrives and the read loop lets the connection go
    tokio::time::sleep(Duration::from_millis(800)).await;

    let response = client
        .do_request(&request, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(
        body_reader_of(response).into_vec(NO_LIMIT).await.unwrap(),
        b"ok"
    );
}

#[derive(Default)]
struct ReadLoops {
    started: AtomicUsize,
    stopped: AtomicUsize,
}

impl MyHttpClientMetrics for ReadLoops {
    fn instance_created(&self, _name: &str) {}
    fn instance_disposed(&self, _name: &str) {}
    fn tcp_connect(&self, _name: &str) {}
    fn tcp_disconnect(&self, _name: &str) {}
    fn read_thread_start(&self, _name: &str) {
        self.started.fetch_add(1, Ordering::SeqCst);
    }
    fn read_thread_stop(&self, _name: &str) {
        self.stopped.fetch_add(1, Ordering::SeqCst);
    }
    fn write_thread_start(&self, _name: &str) {}
    fn write_thread_stop(&self, _name: &str) {}
    fn upgraded_to_websocket(&self, _name: &str) {}
    fn websocket_is_disconnected(&self, _name: &str) {}
}

/// The caller has given up and the client is disposed, and then the answer comes - with
/// a body which is too big to wait for a reader as a whole. Nobody is ever going to
/// read it: a read loop which sends it all the same waits for good, and keeps its task
/// and its socket for as long as the process lives
#[tokio::test]
async fn the_read_loop_is_over_when_a_big_response_comes_to_a_client_which_is_gone() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let read_loops = Arc::new(ReadLoops::default());

    let client = MyHttpClient::new_with_metrics(
        TestConnector {
            host_port: listener.local_addr().unwrap().to_string(),
        },
        read_loops.clone(),
    );

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_heads(&mut socket, 1).await;

        // The caller times out and the client is dropped before the answer
        tokio::time::sleep(Duration::from_millis(400)).await;
        write_big_body(&mut socket).await;
    });

    let result = client
        .do_request(&get_request(), Duration::from_millis(100))
        .await;

    assert!(matches!(result, Err(MyHttpClientError::RequestTimeout(_))));
    drop(client);

    for _ in 0..40 {
        if read_loops.stopped.load(Ordering::SeqCst) > 0 {
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(read_loops.started.load(Ordering::SeqCst), 1);
    assert_eq!(read_loops.stopped.load(Ordering::SeqCst), 1);
}

/// The timeout the request is sent with covers reading its body into memory: a body
/// which does not come is not waited for until the read timeout of the client
#[tokio::test]
async fn into_vec_is_bounded_by_the_timeout_of_the_request() {
    for (first, second, then) in BODIES_IN_TWO_PARTS {
        let (client, _the_second_part_never_comes) =
            client_of_an_upstream_answering_in_two_parts(first, second, then).await;

        let request_timeout = Duration::from_millis(300);
        let started = tokio::time::Instant::now();

        let response = client
            .do_request(&get_request(), request_timeout)
            .await
            .unwrap();

        let result = body_reader_of(response).into_vec(NO_LIMIT).await;

        assert!(
            matches!(result, Err(MyHttpClientError::RequestTimeout(timeout)) if timeout == request_timeout)
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}

/// Reading piece by piece is bounded by the read timeout of the client: the body may
/// last longer than the request was given, but not be silent for longer than that
#[tokio::test]
async fn a_body_nothing_comes_of_for_the_read_timeout_ends_with_an_error() {
    const READ_TIMEOUT: Duration = Duration::from_millis(300);

    for (first, second, then) in BODIES_IN_TWO_PARTS {
        let (mut client, _the_second_part_never_comes) =
            client_of_an_upstream_answering_in_two_parts(first, second, then).await;

        client.set_read_from_stream_timeout(READ_TIMEOUT);

        // The request has run out of its own time by the time the body fails
        let response = client
            .do_request(&get_request(), Duration::from_millis(100))
            .await
            .unwrap();

        let mut body = body_reader_of(response);

        assert_eq!(read_exactly(&mut body, 5).await, b"Hello");

        assert_eq!(
            error_of(body.get_next().await),
            "CanNotExecuteRequest(\"The response body is not complete: no data from the upstream for 300ms\")"
        );
    }
}

/// The limit is what the caller gives. A body which says it is bigger is refused before
/// a byte of it is read: the upstream has not even sent the rest of it
#[tokio::test]
async fn into_vec_refuses_a_body_which_announces_a_size_over_the_limit() {
    let (first, second, then) = BODIES_IN_TWO_PARTS[0];

    let (client, _the_second_part_never_comes) =
        client_of_an_upstream_answering_in_two_parts(first, second, then).await;

    let body = body_reader_of_a_request(&client).await;

    assert!(matches!(
        body.into_vec(9).await,
        Err(MyHttpClientError::ResponseBodyTooLarge { limit: 9 })
    ));
}

/// A body which does not say how big it is - it is chunked, or it lasts until the
/// connection is closed - is refused as soon as it grows past the limit
#[tokio::test]
async fn into_vec_refuses_a_body_which_grows_past_the_limit() {
    for (first, second, then) in BODIES_IN_TWO_PARTS {
        let (client, send_the_second_part) =
            client_of_an_upstream_answering_in_two_parts(first, second, then).await;

        let body = body_reader_of_a_request(&client).await;

        send_the_second_part.send(()).unwrap();

        assert!(matches!(
            body.into_vec(9).await,
            Err(MyHttpClientError::ResponseBodyTooLarge { limit: 9 })
        ));
    }
}

/// A body of exactly the size of the limit is within it, and what is taken by
/// `get_next` already does not count: the limit is about what is held in memory
#[tokio::test]
async fn into_vec_takes_a_body_which_is_within_the_limit() {
    for (first, second, then) in BODIES_IN_TWO_PARTS {
        let (client, send_the_second_part) =
            client_of_an_upstream_answering_in_two_parts(first, second, then).await;

        let body = body_reader_of_a_request(&client).await;

        send_the_second_part.send(()).unwrap();

        assert_eq!(body.into_vec(10).await.unwrap(), b"HelloWorld");

        let (client, send_the_second_part) =
            client_of_an_upstream_answering_in_two_parts(first, second, then).await;

        let mut body = body_reader_of_a_request(&client).await;

        assert_eq!(read_exactly(&mut body, 5).await, b"Hello");

        send_the_second_part.send(()).unwrap();

        assert_eq!(body.into_vec(5).await.unwrap(), b"World");
    }
}

/// There is no limit but the one which is given: a size the response announces and
/// nobody could allocate does not bring the process down, it is just a body which
/// does not come
#[tokio::test]
async fn into_vec_with_no_limit_survives_a_size_which_can_not_be_allocated() {
    let (listener, client) = upstream().await;

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_heads(&mut socket, 1).await;

        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\nHello",
            usize::MAX
        );
        socket.write_all(head.as_bytes()).await.unwrap();
    });

    let body = body_reader_of_a_request(&client).await;

    assert_eq!(
        error_of(body.into_vec(NO_LIMIT).await),
        "CanNotExecuteRequest(\"The response body is not complete: the connection is closed\")"
    );
}

/// Handed to hyper the body says how much of it is left, which is what lets a proxy
/// pass the content-length on instead of falling back to chunks
#[tokio::test]
async fn a_body_reader_given_to_hyper_knows_the_size_of_a_body_with_a_length() {
    use http_body_util::BodyExt;

    let (first, second, then) = BODIES_IN_TWO_PARTS[0];

    let (client, send_the_second_part) =
        client_of_an_upstream_answering_in_two_parts(first, second, then).await;

    let response = client
        .do_request(&get_request(), REQUEST_TIMEOUT)
        .await
        .unwrap();

    let mut body = response.into_response().into_body();

    assert_eq!(body.size_hint().exact(), Some(10));
    assert!(!body.is_end_stream());

    let frame = body.frame().await.unwrap().unwrap();
    assert_eq!(frame.into_data().unwrap(), "Hello");
    assert_eq!(body.size_hint().exact(), Some(5));

    send_the_second_part.send(()).unwrap();

    let frame = body.frame().await.unwrap().unwrap();
    assert_eq!(frame.into_data().unwrap(), "World");
    assert_eq!(body.size_hint().exact(), Some(0));

    assert!(body.frame().await.is_none());
    assert!(body.is_end_stream());
}

/// A writer has to say that the body is over. One which is just dropped - whoever was
/// reading the connection is gone - leaves a body which is not complete, whatever has
/// been sent before
#[tokio::test]
async fn a_sender_which_is_dropped_without_a_word_leaves_a_body_which_is_not_complete() {
    const NOT_COMPLETE: &str = "CanNotExecuteRequest(\"The response body is not complete: the connection is not read any more\")";

    let (sender, mut body) = BodyReader::new(None);

    assert!(sender.send(Bytes::from_static(b"Hello")).await);
    drop(sender);

    assert_eq!(body.get_next().await.unwrap().unwrap(), "Hello");
    assert_eq!(error_of(body.get_next().await), NOT_COMPLETE);
    assert_eq!(error_of(body.into_vec(NO_LIMIT).await), NOT_COMPLETE);

    let (sender, mut body) = BodyReader::new(None);

    assert!(sender.send(Bytes::from_static(b"Hello")).await);
    sender.complete().await;

    assert_eq!(body.get_next().await.unwrap().unwrap(), "Hello");
    assert!(!body.is_end_stream());

    assert!(body.get_next().await.unwrap().is_none());
    assert!(body.is_end_stream());
}

/// The reader counts what it has given against the size the body was made with
#[tokio::test]
async fn a_body_reader_knows_how_much_of_a_body_of_a_known_size_is_left() {
    let (sender, mut body) = BodyReader::new(Some(10));

    assert!(sender.send(Bytes::from_static(b"Hello")).await);
    assert!(sender.send(Bytes::from_static(b"World")).await);
    sender.complete().await;

    assert_eq!(body.content_length(), Some(10));
    assert_eq!(body.remains_to_read(), Some(10));

    assert_eq!(body.get_next().await.unwrap().unwrap(), "Hello");
    assert_eq!(body.remains_to_read(), Some(5));

    assert_eq!(body.into_vec(NO_LIMIT).await.unwrap(), b"World");
}

/// What the writer is told when the reader is dropped half way: it is what lets it
/// stop sending a body nobody needs
#[tokio::test]
async fn a_sender_is_told_when_the_reader_is_dropped() {
    let (sender, body) = BodyReader::new(None);

    assert!(sender.send(Bytes::from_static(b"Hello")).await);
    drop(body);

    sender.reader_is_dropped().await;
    assert!(!sender.send(Bytes::from_static(b"World")).await);
}

/// The body as `rust_extensions::AsyncIterator` sees it: a source of bytes which is read
/// in portions through a shared reference, whoever holds it
type BytesIterator =
    Arc<dyn rust_extensions::AsyncIterator<u8, MyHttpClientError> + Send + Sync + 'static>;

/// The portions are the pieces of the body as they come off the socket: the first half
/// is read while the second one is not even written. The rest is read by another task -
/// the reader is shared, and the future of `get_next` is `Send`
#[tokio::test]
async fn a_body_reader_is_an_async_iterator_of_bytes() {
    for (first, second, then) in BODIES_IN_TWO_PARTS {
        let (client, send_the_second_part) =
            client_of_an_upstream_answering_in_two_parts(first, second, then).await;

        let body: BytesIterator = Arc::new(body_reader_of_a_request(&client).await);

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

            // A body which is over stays over
            assert!(body.get_next().await.unwrap().is_none());

            result
        });

        assert_eq!(the_rest.await.unwrap(), b"World");
    }
}

/// A body which is cut short does not end as if it was complete, whichever way it is
/// read
#[tokio::test]
async fn an_async_iterator_ends_a_body_which_is_cut_short_with_an_error() {
    const NOT_COMPLETE: &str =
        "CanNotExecuteRequest(\"The response body is not complete: the connection is closed\")";

    let client = client_of_an_upstream_answering(
        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHello",
        Then::Close,
    )
    .await;

    let body: BytesIterator = Arc::new(body_reader_of_a_request(&client).await);

    assert_eq!(body.get_next().await.unwrap().unwrap(), b"Hello");
    assert_eq!(error_of(body.get_next().await), NOT_COMPLETE);
    assert_eq!(error_of(body.get_next().await), NOT_COMPLETE);
}

/// The body is one stream however many hold the reader. Two tasks read it at once: they
/// take turns, each piece goes to one of them, and nothing is lost or left waiting
#[tokio::test]
async fn the_readers_of_a_shared_body_take_turns() {
    let (listener, client) = upstream().await;

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_heads(&mut socket, 1).await;
        write_big_body(&mut socket).await;

        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let body: BytesIterator = Arc::new(body_reader_of_a_request(&client).await);

    let readers: Vec<_> = (0..2)
        .map(|_| {
            let body = body.clone();

            tokio::spawn(async move {
                let mut received = 0;

                while let Some(portion) = body.get_next().await.unwrap() {
                    received += portion.len();
                }

                received
            })
        })
        .collect();

    let mut received = 0;

    for reader in readers {
        received += reader.await.unwrap();
    }

    assert_eq!(received, BIG_BODY_SIZE);
}

/// How much is left is known between the reads, whichever way the body is read
#[tokio::test]
async fn a_body_read_as_an_async_iterator_knows_how_much_of_it_is_left() {
    use rust_extensions::AsyncIterator;

    let (sender, body) = BodyReader::new(Some(10));

    assert!(sender.send(Bytes::from_static(b"Hello")).await);
    assert!(sender.send(Bytes::from_static(b"World")).await);
    sender.complete().await;

    assert_eq!(body.remains_to_read(), Some(10));

    assert_eq!(
        AsyncIterator::get_next(&body).await.unwrap().unwrap(),
        b"Hello"
    );
    assert_eq!(body.remains_to_read(), Some(5));

    // What is left is read the other way, by a reader which owns the body
    assert_eq!(body.into_vec(NO_LIMIT).await.unwrap(), b"World");
}
