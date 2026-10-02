use std::sync::{atomic::AtomicU64, Arc};

use http_body_util::BodyExt;

use crate::{MyHttpClientConnector, MyHttpClientError};

use super::{HttpAwaiterTask, HttpTask, MyHttpClientDisconnection, MyHttpRequest, MyHttpResponse};

use super::MyHttpClientInner;

lazy_static::lazy_static! {
    pub static ref CONNECTION_ID: Arc<AtomicU64> = {
        Arc::new(AtomicU64::new(0))
    };
}

pub struct MyHttpClient<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
    TConnector: MyHttpClientConnector<TStream> + Send + Sync + 'static,
> {
    inner: Arc<MyHttpClientInner<TStream>>,
    connector: TConnector,
    send_to_socket_timeout: std::time::Duration,
    connect_timeout: std::time::Duration,
    read_from_stream_timeout: std::time::Duration,
    // tokio::sync::Mutex by design: held for the whole streamed request, which is an
    // await point per chunk, so parking_lot does not fit
    streaming_lock: tokio::sync::Mutex<()>,
}

impl<
        TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
        TConnector: MyHttpClientConnector<TStream> + Send + Sync + 'static,
    > MyHttpClient<TStream, TConnector>
{
    pub fn new(connector: TConnector) -> Self {
        let inner = Arc::new(MyHttpClientInner::new(
            connector.get_remote_endpoint().get_host_port().to_string(),
            None,
        ));

        Self {
            inner,
            connector,
            send_to_socket_timeout: std::time::Duration::from_secs(30),
            connect_timeout: std::time::Duration::from_secs(5),
            read_from_stream_timeout: std::time::Duration::from_secs(120),
            streaming_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn new_with_metrics(
        connector: TConnector,
        metrics: Arc<dyn super::MyHttpClientMetrics + Send + Sync + 'static>,
    ) -> Self {
        let inner = Arc::new(MyHttpClientInner::new(
            connector.get_remote_endpoint().get_host_port().to_string(),
            Some(metrics),
        ));

        Self {
            inner,
            connector,
            send_to_socket_timeout: std::time::Duration::from_secs(30),
            connect_timeout: std::time::Duration::from_secs(5),
            read_from_stream_timeout: std::time::Duration::from_secs(120),
            streaming_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn set_connect_timeout(&mut self, connect_timeout: std::time::Duration) {
        self.connect_timeout = connect_timeout;
    }

    /// Per-read inactivity timeout used by the read loop. For long-lived
    /// streaming bodies with no keepalive (e.g. MCP SSE) set this large so an
    /// idle-but-healthy stream is not torn down. Must be set before `connect()`.
    pub fn set_read_from_stream_timeout(&mut self, read_from_stream_timeout: std::time::Duration) {
        self.read_from_stream_timeout = read_from_stream_timeout;
    }

    pub async fn connect(&self) -> Result<(), MyHttpClientError> {
        let connect_feature = self.connector.connect();

        let Ok(connect_result) = tokio::time::timeout(self.connect_timeout, connect_feature).await
        else {
            return Err(MyHttpClientError::CanNotConnectToRemoteHost(format!(
                "Can not connect to remote endpoint: '{}' Timeout: {:?}",
                self.connector
                    .get_remote_endpoint()
                    .get_host_port()
                    .as_str(),
                self.connect_timeout
            )));
        };

        let receiver = {
            let mut state = self.inner.state.lock().await;
            if state.1.is_none() {
                let (sender, receiver) = tokio::sync::mpsc::channel(1024);
                state.1 = Some(sender);
                Some(receiver)
            } else {
                None
            }
        };

        if let Some(receiver) = receiver {
            let inner_cloned = self.inner.clone();
            tokio::spawn(async move {
                if let Some(metrics) = &inner_cloned.metrics {
                    metrics.write_thread_start(&inner_cloned.name);
                }

                let _ = tokio::spawn(super::write_loop::write_loop(
                    inner_cloned.clone(),
                    receiver,
                ))
                .await;

                if let Some(metrics) = &inner_cloned.metrics {
                    metrics.write_thread_stop(&inner_cloned.name);
                }
            });
        }

        let stream = connect_result?;
        let current_connection_id = CONNECTION_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let (reader, writer) = tokio::io::split(stream);

        self.inner
            .new_connection(current_connection_id, writer, self.send_to_socket_timeout)
            .await?;

        let debug = self.connector.is_debug();

        let read_from_stream_timeout = self.read_from_stream_timeout;

        let inner_cloned = self.inner.clone();
        tokio::spawn(async move {
            let inner = inner_cloned.clone();

            if let Some(metrics) = &inner_cloned.metrics {
                metrics.read_thread_start(&inner.name);
            }
            let err = tokio::spawn(async move {
                let resp = super::read_loop::read_loop(
                    reader,
                    current_connection_id,
                    inner_cloned.clone(),
                    read_from_stream_timeout,
                )
                .await;

                if let Err(err) = &resp {
                    if let Some(invalid_payload_reason) = err.as_invalid_payload() {
                        let task = inner_cloned.pop_request(current_connection_id, false);

                        if let Some(mut task) = task {
                            // The caller may be gone: a request which timed out stays
                            // in the queue. Failing to tell them must not keep the
                            // connection from being dropped below
                            let _ = task.try_set_error(MyHttpClientError::CanNotExecuteRequest(
                                invalid_payload_reason.to_string(),
                            ));
                        }
                    }

                    inner_cloned.disconnect(current_connection_id).await;
                }

                resp
            })
            .await;

            match err {
                Ok(ok) => {
                    if let Err(err) = ok {
                        if debug {
                            println!("Read loop exited with error: {:?}", err);
                        }
                    }

                    // Read loop exited cleanly. This happens when the awaiter was abandoned
                    // (caller timeout) — the write half is still Connected, so without this
                    // the connection becomes a zombie: future sends are written to a socket
                    // nobody reads. read_loop_stopped transitions to Disconnected (a
                    // retryable error), so the next send reconnects instead. It no-ops on a
                    // WS upgrade or a stale connection_id.
                    inner.read_loop_stopped(current_connection_id).await;
                }
                Err(err) => {
                    if let Some(mut task) = inner.pop_request(current_connection_id, false) {
                        // The caller may be gone - see the same call above
                        let _ = task.try_set_error(MyHttpClientError::CanNotExecuteRequest(
                            "Request is panicked".to_string(),
                        ));
                    }
                    inner.disconnect(current_connection_id).await;
                    if debug {
                        println!("Read loop exited with error: {:?}", err);
                    }
                }
            }

            if let Some(metrics) = &inner.metrics {
                metrics.read_thread_stop(&inner.name);
            }
        });

        Ok(())
    }

    async fn send_payload(
        &self,
        request: &MyHttpRequest,
        request_timeout: std::time::Duration,
    ) -> Result<(HttpTask<TStream>, u64), MyHttpClientError> {
        loop {
            let err = match self.inner.send(request).await {
                Ok((awaiter, connection_id)) => {
                    let await_feature = awaiter.get_result();

                    let Ok(result) = tokio::time::timeout(request_timeout, await_feature).await
                    else {
                        return Err(MyHttpClientError::RequestTimeout(request_timeout));
                    };

                    match result {
                        Ok(response) => return Ok((response, connection_id)),
                        Err(err) => err,
                    }
                }
                Err(err) => err,
            };

            if err.is_retryable() {
                self.connect().await?;
                continue;
            }

            return Err(err);
        }
    }

    /// Sends a request whose body is produced as a stream - a [`crate::RequestBodyStream`]
    /// fed by a channel, a proxied `hyper::body::Incoming`, anything implementing
    /// `hyper::body::Body`. The head has to be built by [`MyHttpRequest::new_streamed`],
    /// which leaves the framing of the payload to this call.
    ///
    /// `content_size` is that framing:
    ///
    /// * `Some(size)` - the size is known upfront. The request goes out with
    ///   `Content-Length: size` and the pieces of the payload are written as they are.
    ///   The producer has to deliver exactly that many bytes: fewer leaves the upstream
    ///   waiting for the rest, more would be read as the beginning of the next request,
    ///   so both are refused and the connection is dropped.
    /// * `None` - the size is not known. The request goes out with
    ///   `Transfer-Encoding: chunked` and every piece is framed as a chunk.
    ///
    /// The connection writes nothing but this body until it is over, so the requests
    /// issued meanwhile wait for their turn - that is how HTTP/1.1 works, a request can
    /// not be squeezed into the middle of another one's body. A slow producer holds the
    /// whole connection, so a long upload deserves a connection of its own.
    ///
    /// Unlike [`Self::do_request`] this call is a single shot: the payload is consumed
    /// while it is being sent and there is nothing left to replay, so the request is
    /// never retried once its head has reached the wire. `request_timeout` covers the
    /// whole call - the upload included.
    pub async fn do_streamed_request<TBody>(
        &self,
        req: &MyHttpRequest,
        body: TBody,
        content_size: Option<usize>,
        request_timeout: std::time::Duration,
    ) -> Result<MyHttpResponse<TStream>, MyHttpClientError>
    where
        TBody: hyper::body::Body<Data = bytes::Bytes> + Send + Sync + 'static,
        TBody::Error: std::fmt::Display,
    {
        // The framing header is written by the client out of `content_size`; a head which
        // carries one already would end up with two, and a request with both a
        // content-length and a transfer-encoding is not a valid HTTP message
        if super::headers_contains(&req.headers, super::CONTENT_LENGTH_HEADER_NAME)
            || super::headers_contains(&req.headers, super::TRANSFER_ENCODING_HEADER_NAME)
        {
            return Err(MyHttpClientError::CanNotExecuteRequest(
                "A streamed request must be built by MyHttpRequest::new_streamed and must carry neither a content-length nor a transfer-encoding header"
                    .to_string(),
            ));
        }

        // Serializes the streamed requests of this client: the body owns the wire from
        // the head to the last byte of the payload, a second one would cut into it
        let _streaming_lock = self.streaming_lock.lock().await;

        // Nothing is consumed until the head reaches the wire - neither the head, which
        // is borrowed, nor the body, which is not polled yet - so reconnecting and
        // starting over is safe here and only here
        let mut attempt_no = 0;
        let (awaiter, connection_id) = loop {
            match self.inner.start_streamed_request(req, content_size).await {
                Ok(result) => break result,
                Err(err) => {
                    if !err.is_retryable() || attempt_no > 3 {
                        return Err(err);
                    }

                    attempt_no += 1;
                    self.connect().await?;
                }
            }
        };

        let result = tokio::time::timeout(
            request_timeout,
            self.stream_body(body, connection_id, content_size, awaiter),
        )
        .await;

        let task = match result {
            Ok(result) => result?,
            Err(_) => {
                // The body is written only half way through: the upstream is still
                // waiting for the rest of the chunks and the connection is unusable
                self.inner.abort_streamed_request(connection_id).await;
                return Err(MyHttpClientError::RequestTimeout(request_timeout));
            }
        };

        self.get_response(task, connection_id).await
    }

    async fn stream_body<TBody>(
        &self,
        body: TBody,
        connection_id: u64,
        content_size: Option<usize>,
        awaiter: HttpAwaiterTask<TStream>,
    ) -> Result<HttpTask<TStream>, MyHttpClientError>
    where
        TBody: hyper::body::Body<Data = bytes::Bytes> + Send + Sync + 'static,
        TBody::Error: std::fmt::Display,
    {
        let mut body = std::pin::pin!(body);
        let mut published = 0;

        loop {
            let frame = body.as_mut().frame().await;

            let frame = match frame {
                Some(Ok(frame)) => frame,
                Some(Err(err)) => {
                    // The producing side gave up half way through the payload
                    self.inner.abort_streamed_request(connection_id).await;
                    return Err(MyHttpClientError::CanNotExecuteRequest(format!(
                        "Can not read the body of the request: {}",
                        err
                    )));
                }
                None => break,
            };

            // Trailers are dropped: they would need a `trailer` header announced upfront
            let Ok(chunk) = frame.into_data() else {
                continue;
            };

            // With a content-length the announced amount of bytes is what the upstream
            // reads as the body; the bytes past it would be read as the next request
            if let Some(content_size) = content_size {
                if published + chunk.len() > content_size {
                    self.inner.abort_streamed_request(connection_id).await;
                    return Err(MyHttpClientError::CanNotExecuteRequest(format!(
                        "The body is bigger than the announced content-length {}",
                        content_size
                    )));
                }
            }

            self.inner
                .publish_chunk(connection_id, &chunk, content_size)
                .await?;

            published += chunk.len();
        }

        if let Some(content_size) = content_size {
            if published != content_size {
                // The upstream is still waiting for the rest of the body
                self.inner.abort_streamed_request(connection_id).await;
                return Err(MyHttpClientError::CanNotExecuteRequest(format!(
                    "The body is {} bytes while the announced content-length is {}",
                    published, content_size
                )));
            }
        }

        self.inner
            .finish_streamed_request(connection_id, content_size)
            .await?;

        awaiter.get_result().await
    }

    async fn get_response(
        &self,
        task: HttpTask<TStream>,
        connection_id: u64,
    ) -> Result<MyHttpResponse<TStream>, MyHttpClientError> {
        match task {
            HttpTask::Response(response) => Ok(MyHttpResponse::Response(response)),
            HttpTask::WebsocketUpgrade {
                response,
                read_part,
                leftover,
            } => {
                let write_part = self.inner.upgrade_to_websocket(connection_id).await?;

                let stream = TConnector::reunite(read_part, write_part);
                Ok(MyHttpResponse::WebSocketUpgrade {
                    stream,
                    response,
                    leftover,
                    disconnection: Arc::new(MyHttpClientDisconnection::new(
                        self.inner.clone(),
                        connection_id,
                    )),
                })
            }
        }
    }

    pub async fn do_request(
        &self,
        req: &MyHttpRequest,
        request_timeout: std::time::Duration,
    ) -> Result<MyHttpResponse<TStream>, MyHttpClientError> {
        let response = self.send_payload(req, request_timeout).await;

        let (task, connection_id) = match response {
            Ok(task) => task,
            Err(err) => {
                return Err(err);
            }
        };

        self.get_response(task, connection_id).await
    }
}

impl<
        TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
        TConnector: MyHttpClientConnector<TStream> + Send + Sync + 'static,
    > Drop for MyHttpClient<TStream, TConnector>
{
    fn drop(&mut self) {
        // Drop may run outside a tokio runtime (e.g. during shutdown); tokio::spawn
        // would panic there, and a panic while unwinding aborts the process
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let inner = self.inner.clone();
            handle.spawn(async move {
                inner.dispose().await;
            });
        }
    }
}

impl<
        TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
        TConnector: MyHttpClientConnector<TStream> + Send + Sync + 'static,
    > From<TConnector> for MyHttpClient<TStream, TConnector>
{
    fn from(value: TConnector) -> Self {
        Self::new(value)
    }
}
