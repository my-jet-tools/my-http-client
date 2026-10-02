# my-http-client

Low level HTTP client building blocks of the MyJetTools stack.
The crate owns the connection - it dials through a pluggable `MyHttpClientConnector`, keeps the
connection alive between the requests, and reconnects when the peer drops it.

Three clients live here, all of them generic over the stream (TCP, TLS, unix socket, ssh channel)
and over the connector which produces it:

| module         | what it is                                                                   |
| -------------- | ---------------------------------------------------------------------------- |
| `http1`        | own HTTP/1.1 implementation - this crate serializes the request and parses the response itself |
| `http1_hyper`  | HTTP/1.1 on top of `hyper::client::conn::http1`                               |
| `http2`        | HTTP/2 on top of `hyper::client::conn::http2`                                 |

This document covers the request bodies - the buffered and the streamed one. All three clients can
stream a request body through `do_streamed_request`.

## Features

| feature | what it brings |
| --- | --- |
| `with-websocket` | web socket upgrade support, and with it the `hyper-tungstenite` dependency |

Nothing is enabled by default. Without `with-websocket` the crate never takes a connection over as a
web socket: a `101 Switching Protocols` is handed to the caller as an ordinary response - see
[Web socket upgrade](#web-socket-upgrade).

## Sending a request: buffered vs streamed (`http1_hyper`)

A request body is either buffered or streamed, and the difference between them is what the client
is able to do when the attempt fails. The streamed one comes in two flavours - the caller awaits the
request, or the client drives it on its own:

| | `do_request` | `do_streamed_request` | `start_streamed_request` |
| --- | --- | --- | --- |
| body | `Full<Bytes>` | any `hyper::body::Body` | `RequestBodyStream` |
| framing on the wire | `content-length` | `content_size` picks it | `content_size` picks it |
| retries inside the client | yes | no | no |
| who awaits the request | the caller | the caller | the client (`tokio::spawn`) |
| where the failure shows up | `Err` of the call | `Err` of the call | `publish()` **and** `get_response()` |

The connection is shared by both paths: it holds a `SendRequest<HyperRequestBody>`, where
`HyperRequestBody` is `BoxBody<Bytes, String>` - the body erased to the `hyper::body::Body`
trait object. That is what lets a buffered and a streamed request travel through the very same
connection.

### Buffered body

```rust
let request = hyper::Request::builder()
    .method(hyper::Method::POST)
    .uri("/upload")
    .header("host", "localhost")
    .body(http_body_util::Full::new(bytes::Bytes::from_static(b"1234567890")))?;

let response = client.do_request(request, Duration::from_secs(30)).await?;
```

The payload stays in memory for the whole call, so the client can send it again: the request is
replayed after a `Disconnected`, after a canceled dispatch, and - for idempotent methods only -
after a timeout or a mid-flight error. Every attempt hands hyper its own copy, and there are up to
5 of them (the initial one plus 4 retries). This is the historical behaviour of the client and it
did not change when the streaming support was added.

### Streamed body

`do_streamed_request` takes anything implementing `hyper::body::Body<Data = Bytes>` whose error is
`Display` - a `RequestBodyStream`, a proxied `hyper::body::Incoming`, a `StreamBody`, a file
reader:

```rust
// proxying an incoming request body further, without collecting it into memory
let request = hyper::Request::from_parts(head, incoming);
let response = client.do_streamed_request(request, None, Duration::from_secs(30)).await?;
```

### `content_size` - the framing of a streamed payload

Streaming does not force chunked encoding: `content_size` decides how the payload is announced, and
both HTTP/1.1 clients take that parameter with the same meaning.

| `content_size` | on the wire | when to use it |
| --- | --- | --- |
| `Some(size)` | `Content-Length: size`, the pieces go out as they are | the size is known upfront - a file, a blob, a length the caller already knows |
| `None` | `Transfer-Encoding: chunked`, every piece is framed as a chunk | the size is not known until the producer is done |

`Some(size)` has to be the truth. Fewer bytes leave the upstream waiting for the rest; more would be
read as the beginning of the next request. The non-hyper client counts what it writes and refuses
both cases with `CanNotExecuteRequest`, dropping the connection; hyper refuses them on its own.

A body which knows its size - a `Full<Bytes>` - still goes out with a content-length under `None`
when it travels through `http1_hyper`: hyper takes it from the body's size hint.

**A streamed request is never retried.** The payload is consumed while it is being sent and there is
nothing left to replay, so the client hands the error over instead: the producing side rebuilds the
payload and sends the request again if it wants to. `MyHttpClientError::is_retryable()` tells whether
the request died before it reached the wire.

`request_timeout` covers the whole call - the upload of the payload included, not just the waiting
for the response head. An upload which legitimately takes minutes needs a timeout of that size.

## Producing the payload: `RequestBodyStream`

`RequestBodyStream` is a body over a `tokio::sync::mpsc` channel: we create the channel, the client
gets the consuming side, the caller keeps the `RequestBodyPublisher` and pushes the chunks into it.
A chunk is anything convertible into `Bytes` - `Vec<u8>`, `Bytes`, `String`.

The channel is what gives the backpressure: `publish` waits once `buffer` chunks are queued for the
socket, so a slow upstream does not turn into an unbounded queue in memory.

**Dropping the publisher ends the body** - that is how the client learns the payload is complete, so
it must not be dropped before the last chunk is pushed.

### Option 1: the caller drives the request

```rust
let (publisher, body) = RequestBodyStream::new(4);

tokio::spawn(async move {
    while let Some(chunk) = source.next().await {
        // Err means the request is over - nothing else can be published
        if publisher.publish(chunk).await.is_err() {
            break;
        }
    }
    // dropping the publisher ends the body
});

let request = hyper::Request::builder()
    .method(hyper::Method::POST)
    .uri("/upload")
    .body(body)?;

// the typed error of the request comes out of here
let response = client.do_streamed_request(request, Duration::from_secs(30)).await?;
```

### Option 2: the client drives the request

`start_streamed_request` spawns the request itself and gives back a `StreamedRequest`. The reason of
a failure then reaches the **publishing** side, so the producer does not have to look anywhere else
to tell "everything is published" from "the request is broken":

```rust
let client = Arc::new(MyHttpHyperClient::new(connector));

let (publisher, body) = RequestBodyStream::new(4);

let request = hyper::Request::builder()
    .method(hyper::Method::POST)
    .uri("/upload")
    .body(body)?;

let streamed_request = client.start_streamed_request(request, Duration::from_secs(30));

while let Some(chunk) = source.next().await {
    if let Err(err) = publisher.publish(chunk).await {
        // PublishPayloadError::RequestFailed { reason, is_retryable }
        println!("the request is over: {:?}, retryable: {}", err, err.is_retryable());
        break;
    }
}

// the body is over only when the publisher is gone
drop(publisher);

let response = streamed_request.get_response().await?;
```

`get_response` has to be called after the publisher is dropped - an alive publisher means the
upstream is still waiting for the rest of the payload.

`publish` fails only when the request is over:

| `PublishPayloadError` | meaning |
| --- | --- |
| `RequestFailed { reason, is_retryable }` | the request has failed and the client recorded why |
| `RequestIsOver` | the request is over and the reason belongs to whoever awaits it |

`RequestFailed` is what `start_streamed_request` produces. With `do_streamed_request` the reason is
returned by the call the caller awaits, and the publisher sees the plain `RequestIsOver`.

An existing channel is plugged in with `RequestBodyStream::from_receiver(receiver)` - such a body
carries no reason back to the producing side.

## Streaming with the non-hyper client (`http1`)

`http1::MyHttpClient` streams a body too, through `do_streamed_request`. The same
`RequestBodyStream` (or any other `hyper::body::Body`) feeds it, and the same rule applies - the
request is never retried once its head has reached the wire:

```rust
// Err: the path has a CR, LF, NUL or a space in it and can not be put into a request line
let head = MyHttpRequest::new_streamed(Method::POST, "/upload", Version::HTTP_11, &headers)?;

let (publisher, body) = RequestBodyStream::new(4);

tokio::spawn(async move {
    while let Some(chunk) = source.next().await {
        if publisher.publish(chunk).await.is_err() {
            break;
        }
    }
});

// None here, Some(size) sends the very same stream with a content-length instead
let response = client.do_streamed_request(&head, body, None, Duration::from_secs(30)).await?;
```

The head has to come from `MyHttpRequest::new_streamed`. It carries no framing header at all - the
client writes `content-length` or `transfer-encoding: chunked` itself, out of `content_size` - so a
head which already has one of them is refused, `MyHttpRequest::new` included.

**This client pipelines**, and that changes what a streamed body means for the connection. Requests
are normally serialized into one buffer and written by the write loop, but a streamed body owns the
wire from its head to its last byte - a request written in the middle of it would end up inside
somebody else's payload. So while the body is streaming:

- the requests issued meanwhile are queued in memory and go out right after the body is complete,
  which is the same order they would have had on the wire anyway;
- their responses are delayed by exactly as long as the upload takes;
- a second streamed request waits for the first one to finish.

A slow producer therefore holds the whole connection - a long upload deserves a connection of its
own. If the upload breaks half way through (a timeout, a producer error, a dead socket) the
connection is dropped: an upstream waiting for the rest of the chunks can not be reused.

Trailers are not sent - a frame which is not data is skipped.

## Streaming over HTTP/2 (`http2`)

`http2::MyHttp2Client::do_streamed_request` has the same shape and the same single-shot rule, and
this is where streaming costs the least:

```rust
let (publisher, body) = RequestBodyStream::new(4);

let request = hyper::Request::builder()
    .method(hyper::Method::POST)
    .uri("https://upstream/upload")
    .body(body)?;

let response = client.do_streamed_request(request, None, Duration::from_secs(30)).await?;
```

**A streamed request does not occupy the connection.** h2 multiplexes: the body travels as DATA
frames of its own stream while other requests keep flowing through theirs. Nothing waits for the
upload, which is exactly what the HTTP/1.1 clients can not offer.

HTTP/2 has no chunked encoding either, so `content_size` means a little less here: `Some(size)`
announces a `content-length` (hyper then refuses a body which does not deliver exactly that many
bytes), `None` sends the body with no length announced at all. Both are streamed the same way.

## Web socket upgrade

Requires the `with-websocket` feature.

All three ways of sending a request through `http1_hyper` end up with a `HyperHttpResponse`:

```rust
pub enum HyperHttpResponse {
    Response(HyperResponse),
    #[cfg(feature = "with-websocket")]
    WebSocketUpgrade { response: HyperResponse, web_socket: HyperWebsocket },
}
```

A `101 Switching Protocols` answer turns into `WebSocketUpgrade`, which needs the request to carry
the `sec-websocket-key` header - a 101 which answers a request that is not a websocket handshake
gets `UpgradedToWebSocket` instead, because the connection is consumed by the upgrade and there is
nothing to hand the socket over to.

The own `http1` client hands the upgraded connection over as `MyHttpResponse::WebSocketUpgrade`,
carrying the reunited stream itself rather than a `tungstenite` one.

### Without the feature

`101` is not an interim response: the peer stops speaking HTTP after it, so nothing more will ever
arrive on that connection. Whatever is not taken over as a web socket is therefore delivered to the
caller as the final, bodyless response, and the connection is retired instead of being read past:

* built without `with-websocket` - every 101, web socket handshake or not;
* built with it - a 101 upgrading to something else (`Upgrade: h2c`, ...).

## Retry layering

The client retries only what it can prove is safe on its own connection. Replaying a request which
may have already been executed upstream belongs to the caller: it owns the source data and knows
which requests are safe to send twice - as a rule the idempotent methods only.
