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
while let Some(piece) = body.next_item().await? {
    // BodyChunk: piece.as_slice() is the data of the body
}

// or the whole of it - what is left of it, when a part is taken by next_item already.
// The limit is the caller's: a bigger body fails with ResponseBodyTooLarge
let body: Vec<u8> = body.into_vec(10 * 1024 * 1024).await?;
```

| | `next_item()` | `into_vec(max_size)` |
| --- | --- | --- |
| gives | the next piece, as it came off the socket - a `BodyChunk` | the data of the whole body |
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
passes for a whole one. The request is not sent again: its head is with the caller already, and
replaying it is the caller's decision.

`BodyReader` is a `hyper::body::Body` as well, so it can be handed to hyper as it is, or boxed into
a `HyperResponse` - `MyHttpResponse::into_response()` does that. It reports the exact size of what
is left when the response has a `content-length`, and carries the trailers of an HTTP/2 response,
which `next_item` and `into_vec` read past. hyper gets the data of the body: it frames what it
sends on its own.

### `BodyChunk`: the data, and the bytes as they have come

A piece `next_item()` gives is a `BodyChunk`. It is held the way it has come over the network, and
gives both what has come and the data of the body which is in it:

| | the data of the body | as it has come |
| --- | --- | --- |
| to look at | `as_slice()` | `as_raw_slice()` |
| to take as `Bytes`, with no copy | `into_bytes()` | `into_raw_bytes()` |
| to take as a `Vec` of its own | `into_vec()` | `into_raw()` |

**A piece is not a copy of what was read off the socket.** It shares the buffer the socket was
read into, and so does a `Bytes` taken out of it - nothing is allocated and nothing is copied on
the way from the socket to the reader. That buffer is read into again once every piece of it is
dropped, so a piece is meant to be used and dropped. What has to be kept for long is better taken
as a `Vec`: it is a copy of its own and holds nothing else.

| case | when | `as_slice()` vs `as_raw_slice()` |
| --- | --- | --- |
| `BodyChunk::Raw` | the body has a `content-length` or lasts until the connection is closed - and any body read by hyper | the same bytes |
| `BodyChunk::Chunked` | a chunked body of the non-hyper client | the raw bytes are the chunked coding as it is on the wire; the data is the part of them between the size of the chunk and its separator |

A `Chunked` piece has its data in one place: it is some data of one chunk with what frames it - the
size of the chunk before it when the chunk begins there, the separator behind it when it ends
there. So `as_slice()` is a slice of what `as_raw_slice()` gives, with no copy, and `into_vec()`
cuts the framing off. A chunk does not have to be complete to be given: a big one comes in several
pieces, as it is read off the socket.

The raw bytes of the pieces put together are the body exactly as the upstream has sent it - with
the chunk of no size which ends it, and the trailers. That last piece is the only one with no data.
What frames the data is never a piece on its own: the size of a chunk waits for the first byte of
its data.

```rust
while let Some(piece) = body.next_item().await? {
    match &piece {
        // Already framed by the upstream: goes on as it is
        BodyChunk::Chunked(_) => downstream.write_all(piece.as_raw_slice()).await?,
        BodyChunk::Raw(_) => downstream.write_all(piece.as_slice()).await?,
    }
}
```

### Nothing but the bytes: `AsyncBytesStream`

Whoever needs the bytes of the body and nothing else reads it as a
`rust_extensions::AsyncBytesStream<MyHttpClientError>`, which `BodyReader` is as well. Its chunk is a
`Bytes`:

```rust
use rust_extensions::AsyncBytesStream;

let body: Arc<dyn AsyncBytesStream<MyHttpClientError, Chunk = Bytes> + Send + Sync> = Arc::new(body);

while let Some(bytes) = body.get_next().await? {
    // Bytes: the data of the body, as it came over the network - it is not copied
}
```

| the trait | gives |
| --- | --- |
| `get_next()` | the data of the next piece - what a chunked body is framed with is left out, the piece which has no data is not given, the trailers of an HTTP/2 response are read past |
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
| `content-length` | what a read() of the socket has brought, until that many bytes are sent |
| `transfer-encoding: chunked` | the same, as it is on the wire - a `BodyChunk::Chunked` piece, see above |
| neither of them | what a read() has brought, until the upstream closes the connection |

A piece is `MAX_RESPONSE_BODY_PIECE_SIZE` (64 KB) at most, and a chunk does not have to be complete
to be sent. Each read is bounded by `set_read_from_stream_timeout`: a body which is silent for that
long ends with an error. The read loop ends the body explicitly; a sender which is just dropped
leaves a body which is not complete, so a read loop which is gone can not pass for the end of a
body.

**The socket is read into the two buffers of a `DoubleBuffer`, in turns.** It is the one of
`rust-extensions`: a buffer is handed out to be read into, what is read goes on as a chunk, and the
buffer is free again once the chunk is dropped. A piece of the body is not copied out of the chunk:
it shares the buffer, so the buffer is free once every piece of it is dropped. While the reader is
busy with the pieces of one buffer, the socket is read into the other - the two go on at the same
time. With a reader which uses a piece and drops it, a body of any size goes through the same two
buffers of 64 KB: nothing else is allocated for it, and nothing is copied. A read takes a buffer as
a whole, however little it brings.

What is not consumed yet - a line which is not complete, the beginning of the head which is next -
goes to the beginning of the buffer which is read into next, so that what is parsed is one run of
bytes. That is the only copy, and it is a few bytes as a rule. A head of any size is read line by
line; a single line which does not fit into a buffer is refused.

**The body is not piled up in the client.** When the second buffer is read and the reader is not
done with the first, there is nowhere to read into: the socket is left alone until the reader lets
go of a buffer, and that buffer is what is read into next. So a body nobody is in a hurry to read
stays with the upstream - however big it is - and what is in memory is the two buffers. This client
pipelines, so that has a price: **the connection is busy with a body until it is read**, and the
responses to the requests issued meanwhile wait for that. A body which is off the wire holds the
buffers it was read into all the same: while it holds both of them, the next response waits for it
to be read.

**A reader lets go of a piece before it asks for the one after the next.** It copies the piece into
a `Vec`, parses it as it reads, or passes it on - and drops it. It may keep the piece it has while it
asks for the next one, but a reader which keeps the pieces of both buffers and asks for more waits
for ever: nothing is read until a buffer is free. `collect()` of hyper is such a reader for a body
which does not fit into the two buffers.

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
