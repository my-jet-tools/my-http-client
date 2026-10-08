//! The body of a response of the HTTP/2 client is read through the very same
//! [`BodyReader`] the other clients give - here it is a wrapper of the body hyper gives.
//! The upstream is a real h2 server, and its answer is produced frame by frame.

use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Body, Frame};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

use crate::{BodyReader, MyHttpClientError};

use super::streaming_body_tests::TestConnector;
use super::MyHttp2Client;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

/// A body of any size is taken
const NO_LIMIT: usize = usize::MAX;

/// An h2 upstream of a single request. The body of its answer is made of the frames
/// which are sent through the channel, and is over when the sender is dropped
async fn client_of_an_upstream_answering_frame_by_frame(
    content_length: Option<usize>,
) -> (
    MyHttp2Client<TcpStream, TestConnector>,
    UnboundedSender<Frame<Bytes>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let frames = Arc::new(Mutex::new(Some(receiver)));

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();

        let service =
            hyper::service::service_fn(move |_req: hyper::Request<hyper::body::Incoming>| {
                let frames: UnboundedReceiver<Frame<Bytes>> =
                    frames.lock().unwrap().take().unwrap();

                async move {
                    let body = futures::stream::unfold(frames, |mut frames| async move {
                        let frame = frames.recv().await?;
                        Some((Ok::<_, Infallible>(frame), frames))
                    });

                    let mut response = hyper::Response::builder();

                    if let Some(content_length) = content_length {
                        response = response.header(http::header::CONTENT_LENGTH, content_length);
                    }

                    Ok::<_, hyper::Error>(response.body(StreamBody::new(body)).unwrap())
                }
            });

        let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(socket), service)
            .await;
    });

    let client = MyHttp2Client::new(TestConnector {
        host_port: addr.to_string(),
    });

    (client, sender)
}

async fn response_of_a_request(
    client: &MyHttp2Client<TcpStream, TestConnector>,
    request_timeout: Duration,
) -> hyper::Response<BodyReader> {
    let request = hyper::Request::builder()
        .method(hyper::Method::GET)
        .uri("https://localhost/")
        .body(Full::new(Bytes::new()))
        .unwrap();

    let response = client.do_request(&request, request_timeout).await.unwrap();
    assert_eq!(response.status(), 200);

    response
}

fn data(payload: &'static [u8]) -> Frame<Bytes> {
    Frame::data(Bytes::from_static(payload))
}

fn trailers() -> Frame<Bytes> {
    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
    Frame::trailers(trailers)
}

/// The pieces a body comes in are up to the network, so the test asks for an amount
async fn read_exactly(body: &mut BodyReader, size: usize) -> Vec<u8> {
    let mut result = Vec::new();

    while result.len() < size {
        let piece = body.next_item().await.unwrap();
        result.extend_from_slice(piece.expect("The body is over before its time"));
    }

    result
}

/// The first half of the body is with the caller while the second one is not even
/// produced by the upstream
#[tokio::test]
async fn a_body_is_given_as_it_comes() {
    for content_length in [Some(10), None] {
        let (client, frames) = client_of_an_upstream_answering_frame_by_frame(content_length).await;

        frames.send(data(b"Hello")).unwrap();

        let mut body = response_of_a_request(&client, REQUEST_TIMEOUT)
            .await
            .into_body();

        assert!(matches!(body, BodyReader::Hyper(_)));
        assert_eq!(body.content_length(), content_length);

        assert_eq!(read_exactly(&mut body, 5).await, b"Hello");
        assert_eq!(body.remains_to_read(), content_length.map(|_| 5));

        frames.send(data(b"World")).unwrap();
        drop(frames);

        assert_eq!(read_exactly(&mut body, 5).await, b"World");
        assert!(body.next_item().await.unwrap().is_none());

        // A body which is over stays over
        assert!(body.next_item().await.unwrap().is_none());
        assert!(body.is_end_stream());
    }
}

#[tokio::test]
async fn into_vec_gives_the_whole_body() {
    let (client, frames) = client_of_an_upstream_answering_frame_by_frame(None).await;

    frames.send(data(b"Hello")).unwrap();

    let body = response_of_a_request(&client, REQUEST_TIMEOUT)
        .await
        .into_body();

    frames.send(data(b"World")).unwrap();
    drop(frames);

    assert_eq!(body.into_vec(NO_LIMIT).await.unwrap(), b"HelloWorld");
}

/// The limit is what the caller gives: a body which grows past it is refused, and a
/// body of exactly that size is within it
#[tokio::test]
async fn into_vec_holds_the_body_to_the_limit_it_is_given() {
    for content_length in [Some(10), None] {
        let (client, frames) = client_of_an_upstream_answering_frame_by_frame(content_length).await;

        frames.send(data(b"Hello")).unwrap();
        frames.send(data(b"World")).unwrap();
        drop(frames);

        let body = response_of_a_request(&client, REQUEST_TIMEOUT)
            .await
            .into_body();

        assert!(matches!(
            body.into_vec(9).await,
            Err(MyHttpClientError::ResponseBodyTooLarge { limit: 9 })
        ));

        let (client, frames) = client_of_an_upstream_answering_frame_by_frame(content_length).await;

        frames.send(data(b"Hello")).unwrap();
        frames.send(data(b"World")).unwrap();
        drop(frames);

        let body = response_of_a_request(&client, REQUEST_TIMEOUT)
            .await
            .into_body();

        assert_eq!(body.into_vec(10).await.unwrap(), b"HelloWorld");
    }
}

/// The trailers are not a part of the body: reading it piece by piece and reading it
/// as a whole both go past them
#[tokio::test]
async fn the_trailers_are_not_given_as_the_body() {
    let (client, frames) = client_of_an_upstream_answering_frame_by_frame(None).await;

    frames.send(data(b"Hello")).unwrap();
    frames.send(trailers()).unwrap();
    drop(frames);

    let mut body = response_of_a_request(&client, REQUEST_TIMEOUT)
        .await
        .into_body();

    assert_eq!(read_exactly(&mut body, 5).await, b"Hello");
    assert!(body.next_item().await.unwrap().is_none());

    let (client, frames) = client_of_an_upstream_answering_frame_by_frame(None).await;

    frames.send(data(b"Hello")).unwrap();
    frames.send(trailers()).unwrap();
    drop(frames);

    let body = response_of_a_request(&client, REQUEST_TIMEOUT)
        .await
        .into_body();

    assert_eq!(body.into_vec(NO_LIMIT).await.unwrap(), b"Hello");
}

/// Handed to hyper as a body the reader gives the frames as they are, the trailers
/// included: a proxy has to pass them on - they are where gRPC keeps its status
#[tokio::test]
async fn a_body_reader_given_to_hyper_carries_the_trailers() {
    let (client, frames) = client_of_an_upstream_answering_frame_by_frame(None).await;

    frames.send(data(b"Hello")).unwrap();
    frames.send(trailers()).unwrap();
    drop(frames);

    let mut body = response_of_a_request(&client, REQUEST_TIMEOUT)
        .await
        .into_body()
        .boxed();

    let mut received = Vec::new();
    let mut received_trailers = None;

    while let Some(frame) = body.frame().await {
        match frame.unwrap().into_data() {
            Ok(data) => received.extend_from_slice(&data),
            Err(frame) => received_trailers = frame.into_trailers().ok(),
        }
    }

    assert_eq!(received, b"Hello");
    assert_eq!(received_trailers.unwrap().get("grpc-status").unwrap(), "0");
}

/// The timeout the request is sent with covers reading its body into memory
#[tokio::test]
async fn into_vec_is_bounded_by_the_timeout_of_the_request() {
    let (client, frames) = client_of_an_upstream_answering_frame_by_frame(None).await;

    frames.send(data(b"Hello")).unwrap();

    let request_timeout = Duration::from_millis(300);
    let started = tokio::time::Instant::now();

    let body = response_of_a_request(&client, request_timeout)
        .await
        .into_body();

    // The rest of the body never comes: the upstream keeps it open
    let result = body.into_vec(NO_LIMIT).await;

    assert!(
        matches!(result, Err(MyHttpClientError::RequestTimeout(timeout)) if timeout == request_timeout)
    );
    assert!(started.elapsed() < Duration::from_secs(2));

    drop(frames);
}

/// The body as `rust_extensions::AsyncBytesStream` sees it: a source of bytes, and the
/// trailers are not among them
#[tokio::test]
async fn a_body_reader_is_an_async_bytes_stream() {
    let (client, frames) = client_of_an_upstream_answering_frame_by_frame(None).await;

    frames.send(data(b"Hello")).unwrap();
    frames.send(data(b"World")).unwrap();
    frames.send(trailers()).unwrap();
    drop(frames);

    let body: Arc<
        dyn rust_extensions::AsyncBytesStream<MyHttpClientError, Chunk = crate::BodyPiece>
            + Send
            + Sync
            + 'static,
    > = Arc::new(
        response_of_a_request(&client, REQUEST_TIMEOUT)
            .await
            .into_body(),
    );

    assert_eq!(body.get_size(), None);

    let received = tokio::spawn(async move {
        let mut result = Vec::new();

        while let Some(piece) = body.get_next().await.unwrap() {
            result.extend_from_slice(&piece);
        }

        result
    });

    assert_eq!(received.await.unwrap(), b"HelloWorld");
}
