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
stream a request body through `do_streamed_request`, and all three hand a response body over the
same way - see [Reading a response body: `BodyReader`](#reading-a-response-body-bodyreader).

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

## Reading a response body: `BodyReader`

Every client returns as soon as the **head** of the response is read. The body is not read by then:
the response is an `http::Response<BodyReader>`, and the body is read through that reader.

| client | what it returns |
| --- | --- |
| `http1::MyHttpClient` | `MyHttpResponse::Response(http::Response<BodyReader>)` |
| `http1_hyper::MyHttpHyperClient` | `HyperHttpResponse::Response(http::Response<BodyReader>)` |
| `http2::MyHttp2Client` | `http::Response<BodyReader>` |

```rust
let (head, mut body) = response.into_parts();

// piece by piece, as it comes over the network - `None` is the end of the body
while let Some(data) = body.next_item().await? {
    // &[u8]: the data of the body - good until the next call
}

// or the whole of it - what is left of it, when a part is taken by next_item already.
// The limit is the caller's: a bigger body fails with ResponseBodyTooLarge
let body: Vec<u8> = body.into_vec(10 * 1024 * 1024).await?;
```

| | `next_item()` | `into_vec(max_size)` |
| --- | --- | --- |
| gives | the data which has come next - a `&[u8]`, good until the next call | the data of the whole body |
| held in memory | the pieces waiting to be taken | the body, up to `max_size` - a bigger one is refused |
| bounded in time by | nothing but the silence of the upstream (see below) | the `request_timeout` the request was sent with |

`max_size` is the only limit there is, and it is the caller's: the clients do not hold a body, so
they have no limit of their own. A body which says it is bigger than `max_size` fails with
`MyHttpClientError::ResponseBodyTooLarge { limit }` before a byte of it is read; a body which does
not say fails with it as soon as it grows past the limit. `usize::MAX` takes a body of any size.

`request_timeout` covers the request, the head and `into_vec`: a request whose body is read into
memory is bounded as a whole. A body read piece by piece may last for as long as the upstream keeps
sending it - an event stream does.

`content_length()` is the size of the body when the response says it, `remains_to_read()` is what
is left of it; both are `None` for a body which does not say - chunked, or lasting until the
connection is closed.

A body which is cut short - the connection is closed, the framing is broken - ends with an error
(`The response body is not complete: ...`) and keeps answering with it, so a part of a body never
passes for a whole one. What has come before the break is given first. The request is not sent
again: its head is with the caller already, and replaying it is the caller's decision.

`BodyReader` is a `hyper::body::Body` as well, its data being a `BodyPiece`, so it can be handed to
hyper as it is. `MyHttpResponse::into_response()` boxes it into a `HyperResponse`, whose body is
made of `Bytes`: a piece goes into the `Bytes` as it is, with no copy. It reports the exact size of
what is left when the response has a `content-length`, and carries the trailers of an HTTP/2
response, which `next_item` and `into_vec` read past.

### A piece: the data, not a copy

A piece is the data of the body - for a chunked body the data of its chunks, with what framed them
cut off: the sizes of the chunks, their separators, the chunk which ends the body and the trailers.
A piece of the non-hyper client is what one read of the connection has brought, so the chunks
which come in one read are one piece; a chunk does not have to be complete to be given - a big one
comes in several pieces. There is no piece with no data in it.

**A piece is not a copy of what was read off the socket.** It is the very buffer the socket is read
into - nothing is allocated and nothing is copied on the way from the socket to the reader. That
buffer is read into again once the piece is let go of, so `next_item()` gives a piece which is good
until the next call: the call lets go of it. What is needed for longer is copied out of it.

The trait below and hyper take a piece as a `BodyPiece` of their own - a `&[u8]` through `Deref`,
which holds its buffer until it is dropped.

### Nothing but the bytes: `AsyncBytesStream`

Whoever needs the bytes of the body and nothing else reads it as a
`rust_extensions::AsyncBytesStream<MyHttpClientError>`, which `BodyReader` is as well. Its chunk is a
`BodyPiece`:

```rust
use rust_extensions::AsyncBytesStream;

let body: Arc<dyn AsyncBytesStream<MyHttpClientError, Chunk = BodyPiece> + Send + Sync> =
    Arc::new(body);

while let Some(piece) = body.get_next().await? {
    // BodyPiece: the data of the body, as it came over the network - it is not copied
}
```

| the trait | gives |
| --- | --- |
| `get_next()` | the next piece - the same data `next_item()` gives; the trailers of an HTTP/2 response are read past |
| `get_size()` | `content_length()`: the size of the whole body when the response says it |
| `into_vec()` | the data of the whole body - what is left of it, when a part is read already. It is the one the trait comes with, made of the two above |

The trait reads through a shared reference, so the reader keeps what changes behind a lock. The
body is still one stream: the callers take turns, and each piece goes to one of them. While it is
being read that way from another task, `remains_to_read()` answers `None`.

Both the trait and the reader have an `into_vec`, and they are not the same:

| | the reader's own `into_vec(max_size)` | the trait's `into_vec()` |
| --- | --- | --- |
| the size of the body | held to `max_size` | not limited |
| the time | within the `request_timeout` | not bounded by it |
| memory | grows as the body comes | the size the response says is allocated at once |

So the trait's one is for an upstream which is trusted: the size comes from a header, and a size
nobody could allocate fails the allocation. On a reader which is owned `body.into_vec(max_size)`
is the reader's own; the trait's one is reached through the trait - `Arc<dyn AsyncBytesStream<..>>`,
a generic, or `AsyncBytesStream::into_vec(&body)`.

### `Hyper` / `NoHyper`: what reads the connection

`BodyReader` is an enum of two cases. What reads the connection differs between the clients, and
each case keeps the whole of its reading in its inner:

| case | clients | how the body gets there |
| --- | --- | --- |
| `NoHyper(NoHyperBodyReaderInner)` | `http1` | the read loop of the client writes it to a `BodySender`, and the reader receives it through a channel with backpressure |
| `Hyper(HyperBodyReaderInner)` | `http1_hyper`, `http2` | a wrapper of the body hyper gives (`Incoming`), taken frame by frame with nothing in between |

Whatever the case is, **a body which is not read is not held in memory** - it stays with the
upstream.

### `NoHyper`: the read loop writes the body

The read loop is the only reader of the socket. Having read the head it hands the response over and
goes on with the body, writing it to the `BodySender` whatever the framing is:

| framing | what is sent |
| --- | --- |
| `content-length` | what a read() of the socket has brought, until that many bytes are sent - nothing past the end of the body is read |
| `transfer-encoding: chunked` | the data of the chunks a read() has brought, moved together over what framed them |
| neither of them | what a read() has brought, until the upstream closes the connection |

A piece is `MAX_RESPONSE_BODY_PIECE_SIZE` (64 KB) at most, and a chunk does not have to be complete
to be sent. Each read is bounded by `set_read_from_stream_timeout`: a body which is silent for that
long ends with an error. The read loop ends the body explicitly; a sender which is just dropped
leaves a body which is not complete, so a read loop which is gone can not pass for the end of a
body.

**The heads and the bodies are read into buffers of their own.** A head is read into a buffer of
64 KB and parsed there, line by line - a head of any size, as long as a single line fits into the
buffer. A body is read into the two buffers of a `DoubleBuffer` of `rust-extensions`, in turns: a
buffer is read into, the part of it which is the data goes to the reader as it is - `send_range()`
cuts off what framed it - and the buffer is free again once the reader lets go of the piece. While
the reader is busy with one piece, the socket is read into the other buffer - the two go on at the
same time. A body of any size goes through the same two buffers: nothing else is allocated for it,
and nothing is copied.

What of a read is not the body goes to the buffer of the heads: the beginning of the next response
which has come along with the end of a chunked body, a line of the framing which the read has cut -
the read after it completes the line. What has come along with a head, past its end, is the
beginning of the body: it is copied into a buffer of the body, and that is the first piece.

**The body is not piled up in the client.** When the second buffer is read and the reader is not
done with the first, there is nowhere to read into: the socket is left alone until the reader lets
go of a buffer, and that buffer is what is read into next. So a body nobody is in a hurry to read
stays with the upstream - however big it is - and what is in memory is the buffers. This client
pipelines, so that has a price: **the connection is busy with a body until it is off the wire**,
and the responses to the requests issued meanwhile wait for that. A body which is off the wire
holds the buffers it was read into all the same: the head of the next response is read into its own
buffer and handed over, but its body waits for the first one to be read.

**A reader lets go of a piece before it asks for the one after the next.** `next_item()` does it on
its own. A reader of `BodyPiece`s - the trait, hyper - copies the piece, parses it as it reads, or
passes it on, and drops it. It may keep the piece it has while it asks for the next one, but a
reader which keeps the pieces of both buffers and asks for more waits for ever: nothing is read
until a buffer is free. `collect()` of hyper is such a reader for a body which does not fit into the
two buffers.

Dropping the reader before the end of the body lets the connection go. What is left of the body is
read past - up to `MAX_ABANDONED_BODY_SIZE` (1 MB), for `ABANDONED_BODY_SKIP_TIMEOUT` (1 second) -
and the connection goes on with the next response. With more than that left - a big download, an
event stream - the connection is closed instead. The requests behind that body get `Disconnected`,
which `do_request` answers by sending them again through a new connection - the way it does after
any other disconnect.

The client has to outlive the body: dropping the client shuts the connection down.

### `Hyper`: a wrapper of hyper's body

It is hyper which holds the body back while it is not read, the way the protocol does it. An
HTTP/1.1 connection is busy with a body until it is read or dropped. An HTTP/2 stream has a window
of its own, so a body which is not read holds nothing but its own stream.

Dropping the reader drops hyper's body with it: an HTTP/1.1 connection is given up, an HTTP/2
stream is reset. `BodyReader::from_hyper(incoming)` wraps a body which comes from hyper elsewhere.

## Web socket upgrade

Requires the `with-websocket` feature.

All three ways of sending a request through `http1_hyper` end up with a `HyperHttpResponse`:

```rust
pub enum HyperHttpResponse {
    Response(http::Response<BodyReader>),
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
