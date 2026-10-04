//! Tests for HTTP/1 response-body framing (RFC 9112 §6.3): that the request
//! method and response status are honoured when selecting the body reader.
//!
//! Each test drives [`read_headers`] over an in-memory duplex stream primed with
//! a raw server response, asserts the selected [`BodyReader`] variant, then runs
//! the matching body reader and checks the delivered body.

use std::time::Duration;

use http::Method;
use http_body_util::BodyExt;
use tokio::io::{AsyncWriteExt, DuplexStream, ReadHalf};

use super::{
    read_chunked_body, read_full_body, read_headers, read_until_close, BodyReader, TcpBuffer,
};

const TIMEOUT: Duration = Duration::from_secs(5);

/// Primes a duplex stream with `response` bytes. When `close` is set, the server
/// end is dropped after writing, so the client read half sees the buffered bytes
/// followed by EOF (simulating a `Connection: close` server). Otherwise the
/// server end is returned so it stays open for the duration of the test.
async fn setup(
    response: &[u8],
    close: bool,
) -> (ReadHalf<DuplexStream>, Option<DuplexStream>, TcpBuffer) {
    let (client, mut server) = tokio::io::duplex(1024 * 1024);
    let (mut read_half, _write_half) = tokio::io::split(client);

    server.write_all(response).await.unwrap();

    let held = if close {
        drop(server);
        None
    } else {
        Some(server)
    };

    // The read loop always fills the buffer before calling `read_headers`;
    // replicate that so the parser starts from a primed buffer.
    let mut buf = TcpBuffer::new();
    super::read_to_buffer(&mut read_half, &mut buf, TIMEOUT, false)
        .await
        .unwrap();

    (read_half, held, buf)
}

async fn collect_body(response: crate::HyperResponse) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec()
}

/// Bug A: a HEAD response carries `Content-Length: N` but no body. The reader
/// must not wait for N bytes that never arrive — it returns an empty body at
/// once. Before the fix this produced `LengthBased { body_size: N }` and hung
/// until the request timeout.
#[tokio::test]
async fn head_with_content_length_returns_empty_without_hang() {
    let (mut read_half, _held, mut buf) =
        setup(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n", false).await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::HEAD))
        .await
        .unwrap();

    let (builder, body_size) = match body_reader {
        BodyReader::LengthBased { builder, body_size } => (builder, body_size),
        other => panic!("Expected LengthBased, got {:?}", other),
    };
    assert_eq!(body_size, 0);

    let response = read_full_body(&mut read_half, &mut buf, builder, body_size, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(collect_body(response).await.is_empty());
}

/// A HEAD response with no length signal must still be treated as bodyless
/// (forced empty), never as a close-delimited stream.
#[tokio::test]
async fn head_without_length_is_empty_not_until_close() {
    let (mut read_half, _held, mut buf) = setup(b"HTTP/1.1 200 OK\r\n\r\n", false).await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::HEAD))
        .await
        .unwrap();

    match body_reader {
        BodyReader::LengthBased { body_size, .. } => assert_eq!(body_size, 0),
        other => panic!("Expected LengthBased {{ body_size: 0 }}, got {:?}", other),
    }
}

/// A HEAD response that (as some servers do) echoes `Transfer-Encoding: chunked`
/// must still be treated as bodyless. This guards the match-arm order: the
/// `!body_expected` guard must win over the `Chunked` arm, otherwise HEAD would
/// select the chunked reader and block on frames that never arrive (Bug A).
#[tokio::test]
async fn head_with_chunked_encoding_returns_empty() {
    let (mut read_half, _held, mut buf) = setup(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
        false,
    )
    .await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::HEAD))
        .await
        .unwrap();

    match body_reader {
        BodyReader::LengthBased { body_size, .. } => assert_eq!(body_size, 0),
        other => panic!("Expected LengthBased {{ body_size: 0 }}, got {:?}", other),
    }
}

/// Bug B: a 200 with neither Content-Length nor chunked encoding is delimited by
/// connection close. The full payload must be returned, not an empty body.
#[tokio::test]
async fn close_delimited_body_is_returned_in_full() {
    let (mut read_half, _held, mut buf) = setup(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nHello, close-delimited world!",
        true,
    )
    .await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();

    let builder = match body_reader {
        BodyReader::UntilClose { builder } => builder,
        other => panic!("Expected UntilClose, got {:?}", other),
    };

    let response = read_until_close(&mut read_half, &mut buf, builder, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(collect_body(response).await, b"Hello, close-delimited world!");
}

/// Bug B, exercising the actual socket read-until-EOF loop: the header block is
/// delivered and parsed first, then the body arrives over the socket in several
/// separate writes before the connection closes. This proves `read_until_close`
/// accumulates body bytes read from the stream (not just bytes pre-buffered by
/// the header parser).
#[tokio::test]
async fn close_delimited_body_read_from_socket_across_multiple_writes() {
    let (client, mut server) = tokio::io::duplex(1024 * 1024);
    let (mut read_half, _write_half) = tokio::io::split(client);

    // Only the header block is available when the parser runs.
    server
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n")
        .await
        .unwrap();

    let mut buf = TcpBuffer::new();
    super::read_to_buffer(&mut read_half, &mut buf, TIMEOUT, false)
        .await
        .unwrap();

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();

    let builder = match body_reader {
        BodyReader::UntilClose { builder } => builder,
        other => panic!("Expected UntilClose, got {:?}", other),
    };

    // Stream the body in pieces from the socket, then close to signal EOF.
    let writer = tokio::spawn(async move {
        server.write_all(b"part-one;").await.unwrap();
        tokio::task::yield_now().await;
        server.write_all(b"part-two;").await.unwrap();
        tokio::task::yield_now().await;
        server.write_all(b"part-three").await.unwrap();
        drop(server);
    });

    let response = read_until_close(&mut read_half, &mut buf, builder, TIMEOUT)
        .await
        .unwrap();
    writer.await.unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(collect_body(response).await, b"part-one;part-two;part-three");
}

/// An empty close-delimited body (headers, then immediate close) yields a
/// successful response with an empty body.
#[tokio::test]
async fn close_delimited_empty_body() {
    let (mut read_half, _held, mut buf) = setup(b"HTTP/1.1 200 OK\r\n\r\n", true).await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();

    let builder = match body_reader {
        BodyReader::UntilClose { builder } => builder,
        other => panic!("Expected UntilClose, got {:?}", other),
    };

    let response = read_until_close(&mut read_half, &mut buf, builder, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(collect_body(response).await.is_empty());
}

/// 204 No Content that (incorrectly, but observed in the wild) carries a
/// Content-Length must still return empty without hanging.
#[tokio::test]
async fn no_content_204_with_content_length_returns_empty() {
    let (mut read_half, _held, mut buf) =
        setup(b"HTTP/1.1 204 No Content\r\nContent-Length: 42\r\n\r\n", false).await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();

    let (builder, body_size) = match body_reader {
        BodyReader::LengthBased { builder, body_size } => (builder, body_size),
        other => panic!("Expected LengthBased, got {:?}", other),
    };
    assert_eq!(body_size, 0);

    let response = read_full_body(&mut read_half, &mut buf, builder, body_size, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
    assert!(collect_body(response).await.is_empty());
}

/// 304 Not Modified commonly echoes the cached representation's Content-Length
/// but sends no body. This is the classic hang; it must return empty at once.
#[tokio::test]
async fn not_modified_304_with_content_length_returns_empty() {
    let (mut read_half, _held, mut buf) = setup(
        b"HTTP/1.1 304 Not Modified\r\nContent-Length: 5120\r\n\r\n",
        false,
    )
    .await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();

    let (builder, body_size) = match body_reader {
        BodyReader::LengthBased { builder, body_size } => (builder, body_size),
        other => panic!("Expected LengthBased, got {:?}", other),
    };
    assert_eq!(body_size, 0);

    let response = read_full_body(&mut read_half, &mut buf, builder, body_size, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(response.status(), 304);
    assert!(collect_body(response).await.is_empty());
}

/// A 2xx response to CONNECT establishes a tunnel and carries no body.
#[tokio::test]
async fn connect_2xx_returns_empty() {
    let (mut read_half, _held, mut buf) =
        setup(b"HTTP/1.1 200 Connection Established\r\n\r\n", false).await;

    let body_reader = read_headers(
        &mut read_half,
        &mut buf,
        TIMEOUT,
        false,
        Some(Method::CONNECT),
    )
    .await
    .unwrap();

    match body_reader {
        BodyReader::LengthBased { body_size, .. } => assert_eq!(body_size, 0),
        other => panic!("Expected LengthBased {{ body_size: 0 }}, got {:?}", other),
    }
}

/// A normal length-delimited response still works unchanged.
#[tokio::test]
async fn length_based_body_still_works() {
    let (mut read_half, _held, mut buf) =
        setup(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHello", false).await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();

    let (builder, body_size) = match body_reader {
        BodyReader::LengthBased { builder, body_size } => (builder, body_size),
        other => panic!("Expected LengthBased, got {:?}", other),
    };
    assert_eq!(body_size, 5);

    let response = read_full_body(&mut read_half, &mut buf, builder, body_size, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(collect_body(response).await, b"Hello");
}

/// A chunked response still works unchanged.
#[tokio::test]
async fn chunked_body_still_works() {
    let (read_half, _held, buf) = setup(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n6\r\n World\r\n0\r\n\r\n",
        false,
    )
    .await;

    let mut read_half = read_half;
    let mut buf = buf;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();

    let (response, sender) = match body_reader {
        BodyReader::Chunked { response, sender } => (response, sender),
        other => panic!("Expected Chunked, got {:?}", other),
    };

    let reader = tokio::spawn(async move {
        read_chunked_body(&mut read_half, &mut buf, sender, TIMEOUT, false)
            .await
            .unwrap();
    });

    assert_eq!(response.status(), 200);
    let body = collect_body(response).await;
    reader.await.unwrap();

    assert_eq!(body, b"Hello World");
}

/// A non-websocket interim 1xx response (100 Continue) must be signalled as
/// `Interim` (to be skipped), and the following real response parsed from the
/// same stream — not delivered as the final response.
#[tokio::test]
async fn interim_100_continue_is_skipped_then_final_response_read() {
    let (mut read_half, _held, mut buf) = setup(
        b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi",
        false,
    )
    .await;

    let first = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();
    assert!(
        matches!(first, BodyReader::Interim),
        "Expected Interim for 100 Continue, got {:?}",
        first
    );

    // The real 200 is parsed next from the same stream.
    let second = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();
    let (builder, body_size) = match second {
        BodyReader::LengthBased { builder, body_size } => (builder, body_size),
        other => panic!("Expected LengthBased, got {:?}", other),
    };
    assert_eq!(body_size, 2);

    let response = read_full_body(&mut read_half, &mut buf, builder, body_size, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(collect_body(response).await, b"hi");
}

/// 103 Early Hints (with informational headers) is likewise an interim response.
#[tokio::test]
async fn early_hints_103_is_interim() {
    let (mut read_half, _held, mut buf) = setup(
        b"HTTP/1.1 103 Early Hints\r\nLink: </style.css>; rel=preload\r\n\r\n",
        false,
    )
    .await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();

    assert!(
        matches!(body_reader, BodyReader::Interim),
        "Expected Interim for 103 Early Hints, got {:?}",
        body_reader
    );
}

/// A 101 websocket upgrade must still route to the upgrade path even though 101
/// is a 1xx status (which otherwise means "no body"). This is checked before the
/// interim arm, so a real upgrade is never mistaken for a skippable 1xx.
#[cfg(feature = "with-websocket")]
#[tokio::test]
async fn websocket_upgrade_still_detected() {
    let (mut read_half, _held, mut buf) = setup(
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
        false,
    )
    .await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();

    assert!(
        matches!(body_reader, BodyReader::WebSocketUpgrade(_)),
        "Expected WebSocketUpgrade, got {:?}",
        body_reader
    );
}

/// A 101 which is not a websocket upgrade - `Upgrade: h2c` is what curl sends
/// when it tries to switch a plaintext connection to h2. It is a 1xx status, but
/// treating it as an interim response would leave the read loop waiting for a
/// >= 200 answer until the read timeout: the connection has stopped speaking
/// HTTP. It is the final answer on this exchange instead.
#[tokio::test]
async fn switching_protocols_to_a_non_websocket_protocol_is_not_interim() {
    let (mut read_half, _held, mut buf) = setup(
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n",
        false,
    )
    .await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();

    assert!(
        matches!(body_reader, BodyReader::SwitchedProtocols { .. }),
        "Expected SwitchedProtocols, got {:?}",
        body_reader
    );
}

/// Built without `with-websocket` there is nothing to hand the socket over to, so
/// even a websocket 101 is delivered as an ordinary final response rather than
/// being read past as an interim one.
#[cfg(not(feature = "with-websocket"))]
#[tokio::test]
async fn websocket_upgrade_is_a_plain_protocol_switch_without_the_feature() {
    let (mut read_half, _held, mut buf) = setup(
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
        false,
    )
    .await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, Some(Method::GET))
        .await
        .unwrap();

    assert!(
        matches!(body_reader, BodyReader::SwitchedProtocols { .. }),
        "Expected SwitchedProtocols, got {:?}",
        body_reader
    );
}

/// With no method known (empty queue / unsolicited data), a normal 200 with no
/// length signal defaults to close-delimited — the safe RFC default for
/// responses.
#[tokio::test]
async fn unknown_method_defaults_to_until_close() {
    let (mut read_half, _held, mut buf) =
        setup(b"HTTP/1.1 200 OK\r\n\r\npayload", true).await;

    let body_reader = read_headers(&mut read_half, &mut buf, TIMEOUT, false, None)
        .await
        .unwrap();

    let builder = match body_reader {
        BodyReader::UntilClose { builder } => builder,
        other => panic!("Expected UntilClose, got {:?}", other),
    };

    let response = read_until_close(&mut read_half, &mut buf, builder, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(collect_body(response).await, b"payload");
}

/// The method the framing logic relies on is derived by `MyHttpRequest::get_method`
/// from the serialized request line. Verify it round-trips for both construction
/// paths, since a wrong method here would silently reintroduce Bug A (e.g. a HEAD
/// mis-read as GET).
#[test]
fn get_method_parses_serialized_request_line() {
    use crate::http1::{MyHttpRequest, MyHttpRequestBuilder};
    use crate::MyHttpClientHeadersBuilder;
    use http::Version;

    let headers = MyHttpClientHeadersBuilder::new();
    for method in [
        Method::HEAD,
        Method::CONNECT,
        Method::GET,
        Method::POST,
        Method::DELETE,
    ] {
        let req = MyHttpRequest::new(
            method.clone(),
            "/path?x=1",
            Version::HTTP_11,
            &headers,
            vec![],
        )
        .unwrap();
        assert_eq!(req.get_method(), method, "MyHttpRequest::new path");

        let built = MyHttpRequestBuilder::new(method.clone(), "/path?x=1")
            .build()
            .unwrap();
        assert_eq!(built.get_method(), method, "MyHttpRequestBuilder path");
    }
}

/// The read loop frames each response using the method of the FIFO front entry
/// (the request whose response is on the wire). Guard that `peek_front_method`
/// peeks the front and `pop` removes that same front — the basis for correct
/// framing under pipelining.
#[test]
fn queue_peeks_front_method_in_fifo_order() {
    use crate::http1::QueueOfRequests;
    use tokio::io::DuplexStream;

    let queue: QueueOfRequests<DuplexStream> = QueueOfRequests::new();
    assert_eq!(queue.peek_front_method(), None);

    queue.push(Method::HEAD, rust_extensions::TaskCompletion::new());
    queue.push(Method::GET, rust_extensions::TaskCompletion::new());

    assert_eq!(queue.peek_front_method(), Some(Method::HEAD));
    assert!(queue.pop().is_some());
    assert_eq!(queue.peek_front_method(), Some(Method::GET));
    assert!(queue.pop().is_some());
    assert_eq!(queue.peek_front_method(), None);
}

/// End to end over a real socket: a 101 must reach the caller. Read past as an
/// interim response the call would hang until the request timeout - the peer has
/// switched the protocol and will never send the ">= 200 response" the read loop
/// would be waiting for.
#[tokio::test]
async fn a_switching_protocols_response_is_delivered_to_the_caller() {
    use super::streaming_body_tests::TestConnector;
    use super::{MyHttpClient, MyHttpRequest};
    use crate::MyHttpClientHeadersBuilder;
    use http::Version;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 1024];
        let _ = socket.read(&mut buffer).await.unwrap();
        socket
            .write_all(
                b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n",
            )
            .await
            .unwrap();

        // The socket stays open, speaking something which is not HTTP any more
        tokio::time::sleep(Duration::from_secs(10)).await;
    });

    let client = MyHttpClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    let mut headers = MyHttpClientHeadersBuilder::new();
    headers.add_header("host", "localhost").unwrap();

    let request = MyHttpRequest::new(Method::GET, "/", Version::HTTP_11, &headers, vec![]).unwrap();

    let response = client
        .do_request(&request, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(response.status(), 101);
}

/// An `AsyncRead` handing out exactly one scripted chunk per `read()` call. The
/// read boundaries are the whole point of the websocket-leftover tests, and
/// neither a duplex nor a real socket lets a test place them.
#[cfg(feature = "with-websocket")]
struct ScriptedReads {
    chunks: std::collections::VecDeque<Vec<u8>>,
}

#[cfg(feature = "with-websocket")]
impl tokio::io::AsyncRead for ScriptedReads {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Out of chunks: EOF
        let Some(chunk) = self.chunks.pop_front() else {
            return std::task::Poll::Ready(Ok(()));
        };

        assert!(
            chunk.len() <= buf.remaining(),
            "The scripted chunk does not fit into a single read"
        );
        buf.put_slice(&chunk);
        std::task::Poll::Ready(Ok(()))
    }
}

/// The upgrade request is written into a sink: nothing reads it back.
#[cfg(feature = "with-websocket")]
impl tokio::io::AsyncWrite for ScriptedReads {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// Runs the real read loop over `chunks` with one GET awaiting its response, and
/// returns the task the loop completed it with.
#[cfg(feature = "with-websocket")]
async fn read_loop_over(chunks: Vec<Vec<u8>>) -> super::HttpTask<ScriptedReads> {
    const CONNECTION_ID: u64 = 1;

    let stream = ScriptedReads {
        chunks: chunks.into(),
    };
    let (read_half, write_half) = tokio::io::split(stream);

    let inner = std::sync::Arc::new(super::MyHttpClientInner::new("test".to_string(), None));
    inner
        .new_connection(CONNECTION_ID, write_half, TIMEOUT)
        .await
        .unwrap();

    let mut task = rust_extensions::TaskCompletion::new();
    let awaiter = task.get_awaiter();
    inner.queue_of_requests.push(Method::GET, task);

    super::read_loop::read_loop(read_half, CONNECTION_ID, inner, TIMEOUT)
        .await
        .unwrap();

    awaiter.get_result().await.unwrap()
}

#[cfg(feature = "with-websocket")]
const WS_UPGRADE_HEAD: &[u8] =
    b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n";

/// An unmasked server text frame `hi`: FIN + opcode 1, length 2, payload.
#[cfg(feature = "with-websocket")]
const WS_FRAME: &[u8] = &[0x81, 0x02, b'h', b'i'];

/// The server writes its first frame right behind the `101`, so both arrive in
/// one read(). The head is parsed out of the buffer, the frame stays in it - and
/// the buffer belongs to the read loop, which is about to return. Unless the
/// remainder is handed over together with the socket, that first frame is lost
/// forever and the caller only ever sees the second one.
#[cfg(feature = "with-websocket")]
#[tokio::test]
async fn websocket_upgrade_carries_the_frame_which_arrived_with_the_101() {
    let mut single_read = WS_UPGRADE_HEAD.to_vec();
    single_read.extend_from_slice(WS_FRAME);

    let task = read_loop_over(vec![single_read]).await;

    match task {
        super::HttpTask::WebsocketUpgrade {
            response, leftover, ..
        } => {
            assert_eq!(response.status(), 101);
            assert_eq!(leftover, WS_FRAME);
        }
        super::HttpTask::Response(_) => panic!("Expected WebsocketUpgrade, got Response"),
    }
}

/// The other half of the same contract: when the `101` comes in a read of its
/// own there is nothing past the head, so there is nothing to replay.
#[cfg(feature = "with-websocket")]
#[tokio::test]
async fn websocket_upgrade_alone_in_its_read_has_no_leftover() {
    let task = read_loop_over(vec![WS_UPGRADE_HEAD.to_vec()]).await;

    match task {
        super::HttpTask::WebsocketUpgrade {
            response, leftover, ..
        } => {
            assert_eq!(response.status(), 101);
            assert!(
                leftover.is_empty(),
                "Expected no leftover, got {:?}",
                leftover
            );
        }
        super::HttpTask::Response(_) => panic!("Expected WebsocketUpgrade, got Response"),
    }
}
