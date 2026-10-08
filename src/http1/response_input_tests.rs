//! What the caller gets when the upstream answers with bytes which are not a valid
//! HTTP/1.1 response. Every answer is written to a real socket and read back by the
//! real client, so the read loop runs the way it does in production.

use std::time::Duration;

use http::{Method, Version};
use http_body_util::BodyExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{MyHttpClientError, MyHttpClientHeadersBuilder};

use super::streaming_body_tests::TestConnector;
use super::{MyHttpClient, MyHttpRequest, MyHttpResponse};

/// Long enough to tell an error which comes at once from a request which hangs until
/// its timeout
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

/// What the upstream does with the socket once the answer is written
#[derive(Clone, Copy)]
pub enum Then {
    KeepOpen,
    Close,
}

pub async fn read_request_heads(socket: &mut TcpStream, amount: usize) {
    let mut received = Vec::new();
    let mut buffer = [0u8; 4096];

    while received
        .windows(4)
        .filter(|window| *window == b"\r\n\r\n")
        .count()
        < amount
    {
        let read = socket.read(&mut buffer).await.unwrap();
        if read == 0 {
            break;
        }
        received.extend_from_slice(&buffer[..read]);
    }
}

/// An upstream of a single connection: reads a request and answers with `response`,
/// byte for byte
pub async fn client_of_an_upstream_answering(
    response: &'static [u8],
    then: Then,
) -> MyHttpClient<TcpStream, TestConnector> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request_heads(&mut socket, 1).await;
        socket.write_all(response).await.unwrap();

        if let Then::KeepOpen = then {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    MyHttpClient::new(TestConnector {
        host_port: addr.to_string(),
    })
}

pub fn get_request() -> MyHttpRequest {
    let mut headers = MyHttpClientHeadersBuilder::new();
    headers.add_header("host", "localhost").unwrap();
    MyHttpRequest::new(Method::GET, "/", Version::HTTP_11, &headers, vec![]).unwrap()
}

/// What the caller of `do_request` has got, in a form which is easy to compare
async fn outcome_of(response: &'static [u8], then: Then) -> String {
    let client = client_of_an_upstream_answering(response, then).await;

    let result = client.do_request(&get_request(), REQUEST_TIMEOUT).await;

    match result {
        Ok(response) => format!("Ok({})", response.status().as_u16()),
        Err(err) => format!("{:?}", err),
    }
}

#[tokio::test]
async fn a_header_name_which_is_not_a_token_fails_the_request_with_the_reason() {
    const BAD_HEADER: &str = "CanNotExecuteRequest(\"Invalid HTTP header name: [Bad Header]\")";
    const EMPTY_NAME: &str = "CanNotExecuteRequest(\"Invalid HTTP header name: []\")";

    let responses: [(&'static [u8], Then, &str); 6] = [
        // The body is empty
        (
            b"HTTP/1.1 200 OK\r\nBad Header: x\r\nContent-Length: 0\r\n\r\n",
            Then::KeepOpen,
            BAD_HEADER,
        ),
        // The body has a length
        (
            b"HTTP/1.1 200 OK\r\nBad Header: x\r\nContent-Length: 2\r\n\r\nok",
            Then::KeepOpen,
            BAD_HEADER,
        ),
        // The body is chunked
        (
            b"HTTP/1.1 200 OK\r\nBad Header: x\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n",
            Then::KeepOpen,
            BAD_HEADER,
        ),
        // The body lasts until the connection is closed
        (
            b"HTTP/1.1 200 OK\r\nBad Header: x\r\n\r\nok",
            Then::Close,
            BAD_HEADER,
        ),
        // The protocol is switched to something which is not a web socket
        (
            b"HTTP/1.1 101 Switching Protocols\r\nBad Header: x\r\nUpgrade: h2c\r\n\r\n",
            Then::KeepOpen,
            BAD_HEADER,
        ),
        // The name is empty
        (
            b"HTTP/1.1 200 OK\r\n: x\r\nContent-Length: 0\r\n\r\n",
            Then::KeepOpen,
            EMPTY_NAME,
        ),
    ];

    for (response, then, expected) in responses {
        assert_eq!(
            outcome_of(response, then).await,
            expected,
            "{:?}",
            String::from_utf8_lossy(response)
        );
    }
}

#[cfg(feature = "with-websocket")]
#[tokio::test]
async fn a_bad_header_name_of_a_web_socket_upgrade_fails_the_request_with_the_reason() {
    assert_eq!(
        outcome_of(
            b"HTTP/1.1 101 Switching Protocols\r\nBad Header: x\r\nUpgrade: websocket\r\n\r\n",
            Then::KeepOpen,
        )
        .await,
        "CanNotExecuteRequest(\"Invalid HTTP header name: [Bad Header]\")"
    );
}

/// `Content-Length` which is not a number is cut down to 16 bytes for the text of the
/// error, and here the cut lands in the middle of a two byte char
#[tokio::test]
async fn a_content_length_cut_in_the_middle_of_a_char_fails_the_request_with_the_reason() {
    assert_eq!(
        outcome_of(
            b"HTTP/1.1 200 OK\r\nContent-Length: 123456789012345\xc3\xa9\r\n\r\n",
            Then::KeepOpen,
        )
        .await,
        "CanNotExecuteRequest(\"Invalid Content-Length value: 123456789012345\")"
    );
}

/// The data of the body which came before it has ended, and the error it has ended
/// with. `None` is a body which is complete
async fn body_of(response: MyHttpResponse<TcpStream>) -> (Vec<u8>, Option<String>) {
    let mut body = response.into_response().into_body();
    let mut data = Vec::new();

    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(chunk) = frame.into_data() {
                    data.extend_from_slice(&chunk);
                }
            }
            Some(Err(err)) => return (data, Some(err)),
            None => return (data, None),
        }
    }
}

async fn chunked_body_of(response: &'static [u8], then: Then) -> (Vec<u8>, Option<String>) {
    let client = client_of_an_upstream_answering(response, then).await;

    let response = client
        .do_request(&get_request(), REQUEST_TIMEOUT)
        .await
        .unwrap();

    // The head is with the caller before the body is read off the socket
    assert_eq!(response.status(), 200);

    body_of(response).await
}

#[tokio::test]
async fn a_chunked_body_which_is_complete_ends_with_no_error() {
    let body = chunked_body_of(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n0\r\n\r\n",
        Then::KeepOpen,
    )
    .await;

    assert_eq!(body, (b"Hello".to_vec(), None));
}

/// The size of the second chunk starts with a byte which is neither a hex digit nor
/// UTF-8. The head is already with the caller by then, so the body is the only way
/// left to tell them: it must not end as if it was complete
#[tokio::test]
async fn a_chunk_size_which_is_not_utf8_ends_the_body_with_an_error() {
    let (data, error) = chunked_body_of(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n\xff\xfe\r\n",
        Then::KeepOpen,
    )
    .await;

    assert_eq!(data, b"Hello".to_vec());
    assert_eq!(
        error.as_deref(),
        Some("The response body is not complete: Invalid chunk size: \"\u{fffd}\u{fffd}\"")
    );
}

#[tokio::test]
async fn a_chunk_size_which_is_not_a_number_ends_the_body_with_an_error() {
    let (data, error) = chunked_body_of(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\nxyz\r\n",
        Then::KeepOpen,
    )
    .await;

    assert_eq!(data, b"Hello".to_vec());
    assert_eq!(
        error.as_deref(),
        Some("The response body is not complete: Invalid chunk size: \"xyz\"")
    );
}

/// The upstream is gone in the middle of the body: what has been read is not the
/// whole of it
#[tokio::test]
async fn a_connection_lost_in_the_middle_of_a_chunked_body_ends_it_with_an_error() {
    let (data, error) = chunked_body_of(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n",
        Then::Close,
    )
    .await;

    assert_eq!(data, b"Hello".to_vec());
    assert_eq!(
        error.as_deref(),
        Some("The response body is not complete: the connection is closed")
    );
}

/// Two requests are on the wire and both callers have given up waiting. The answer
/// which comes after that is not HTTP: there is nobody to report it to, and that must
/// not keep the connection from being marked as lost - otherwise the next request is
/// written into a socket nobody reads
#[tokio::test]
async fn a_broken_answer_to_the_callers_which_are_gone_does_not_leave_a_dead_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        read_request_heads(&mut first, 2).await;

        // Both callers time out before the answer
        tokio::time::sleep(Duration::from_millis(400)).await;
        first.write_all(b"this is not http\r\n\r\n").await.unwrap();

        // A client which has noticed the failure dials again
        if let Ok(Ok((mut second, _))) =
            tokio::time::timeout(Duration::from_secs(10), listener.accept()).await
        {
            read_request_heads(&mut second, 1).await;
            second
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        }

        drop(first);
    });

    let client = MyHttpClient::new(TestConnector {
        host_port: addr.to_string(),
    });

    client.connect().await.unwrap();

    let request = get_request();
    let short_timeout = Duration::from_millis(100);

    let (first, second) = tokio::join!(
        client.do_request(&request, short_timeout),
        client.do_request(&request, short_timeout)
    );

    assert!(matches!(first, Err(MyHttpClientError::RequestTimeout(_))));
    assert!(matches!(second, Err(MyHttpClientError::RequestTimeout(_))));

    // The broken answer arrives and the read loop stops
    tokio::time::sleep(Duration::from_millis(800)).await;

    let third = client
        .do_request(&request, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(third.status(), 200);
}

/// What a builder becomes once it is given a header it does not take
fn builder_which_carries_an_error() -> http::response::Builder {
    http::Response::builder().header("bad name", "value")
}

/// The functions which put a response together take the builder from whoever calls
/// them. The read loop never hands them one which carries an error, but they are public
#[tokio::test]
async fn a_response_builder_which_carries_an_error_is_reported() {
    assert!(crate::utils::into_empty_body(builder_which_carries_an_error()).is_err());
    assert!(crate::utils::into_body(builder_which_carries_an_error(), b"ok".to_vec()).is_err());

    #[cfg(feature = "with-websocket")]
    assert!(
        super::WebSocketUpgradeBuilder::new(builder_which_carries_an_error())
            .take_upgrade_response()
            .is_err()
    );
}
