use std::{
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use bytes::Bytes;
use http::StatusCode;
use http_body_util::{BodyExt, Full};
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    BodyReader, MyHttpClientConnector, MyHttpClientDisconnect, MyHttpClientError, RequestBodyStream,
};

use super::*;
use crate::hyper::*;

pub struct MyHttpHyperClient<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
    TConnector: MyHttpClientConnector<TStream> + Send + Sync + 'static,
> {
    connector: TConnector,
    stream: PhantomData<TStream>,
    inner: Arc<MyHttpHyperClientInner>,
    connect_timeout: Duration,
    connection_id: AtomicU64,
    // tokio::sync::Mutex by design: held across the dial (TCP connect + http
    // handshake) to serialize concurrent dialers, so parking_lot does not fit
    connect_lock: tokio::sync::Mutex<()>,
}

impl<
        TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
        TConnector: MyHttpClientConnector<TStream> + Send + Sync + 'static,
    > MyHttpHyperClient<TStream, TConnector>
{
    pub fn new(connector: TConnector) -> Self {
        Self {
            inner: Arc::new(MyHttpHyperClientInner::new(
                connector
                    .get_remote_endpoint()
                    .get_host_port()
                    .to_string()
                    .into(),
                None,
            )),
            connector,

            stream: PhantomData,
            connect_timeout: Duration::from_secs(5),
            connection_id: AtomicU64::new(0),
            connect_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn new_with_metrics(
        connector: TConnector,
        metrics: Arc<dyn MyHttpHyperClientMetrics + Send + Sync + 'static>,
    ) -> Self {
        Self {
            inner: Arc::new(MyHttpHyperClientInner::new(
                connector
                    .get_remote_endpoint()
                    .get_host_port()
                    .to_string()
                    .into(),
                Some(metrics),
            )),
            connector,

            stream: PhantomData,
            connect_timeout: Duration::from_secs(5),
            connection_id: AtomicU64::new(0),
            connect_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn set_connect_timeout(&mut self, connection_timeout: Duration) {
        self.connect_timeout = connection_timeout;
    }

    async fn get_response<TBody>(
        &self,
        req: hyper::Request<TBody>,
        response: hyper::Response<BodyReader>,
    ) -> Result<HyperHttpResponse, MyHttpClientError> {
        if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            #[cfg(feature = "with-websocket")]
            {
                self.inner.upgrade_to_websocket().await?;

                // A 101 answering a request which is not a websocket handshake has no
                // `Sec-WebSocket-Key` to derive the accept key from. The connection is
                // consumed by the upgrade either way, so there is nothing to hand the
                // socket over to
                let (handshake_response, web_socket) = hyper_tungstenite::upgrade(req, None)
                    .map_err(|_| MyHttpClientError::UpgradedToWebSocket)?;

                return Ok(HyperHttpResponse::WebSocketUpgrade {
                    response: crate::utils::into_full_body_response(handshake_response),
                    web_socket,
                });
            }

            // Built without websocket support: the 101 is the answer to this request
            // and goes back as an ordinary response. The connection is gone with the
            // protocol switch - the `with_upgrades()` task spawned in
            // wrap_http1_endpoint resolves at the upgrade and retires it by its own id
            #[cfg(not(feature = "with-websocket"))]
            {
                let _ = req;
            }
        }

        Ok(HyperHttpResponse::Response(response))
    }

    /// Sends a request with a buffered body. The payload stays in memory for the whole
    /// call, so a failed attempt can be replayed - see [`Self::do_streamed_request`] for
    /// the streaming counterpart which is a single shot
    pub async fn do_request(
        &self,
        req: hyper::Request<Full<Bytes>>,
        request_timeout: Duration,
    ) -> Result<HyperHttpResponse, MyHttpClientError> {
        let request_is_idempotent = req.method().is_idempotent();
        let mut retry_no = 0;
        loop {
            // Every attempt gets its own copy: the body is erased into a trait object
            // before it goes to hyper and can not be taken back from there
            let payload = crate::utils::to_hyper_request(req.clone());

            let err = match self.inner.send_payload(payload, request_timeout).await {
                Ok(response) => return self.get_response(req, response).await,
                Err(err) => err,
            };

            match err {
                SendHyperPayloadError::Disconnected => {
                    // The request never reached the wire, so reconnecting and retrying
                    // is safe for any method
                    if retry_no > 3 {
                        return Err(MyHttpClientError::Disconnected);
                    }
                    retry_no += 1;
                    self.connect().await?;
                }
                SendHyperPayloadError::RequestTimeout(duration) => {
                    // The connection is already dropped by send_payload: an HTTP/1.1
                    // connection with an unread response pending can not be reused.
                    // The request may have reached the upstream though, so only
                    // idempotent requests are safe to replay
                    if !request_is_idempotent || retry_no > 3 {
                        return Err(MyHttpClientError::RequestTimeout(duration));
                    }

                    self.connect().await?;
                    retry_no += 1;
                    continue;
                }
                SendHyperPayloadError::HyperError { connected, err } => {
                    if retry_no > 3 {
                        return Err(MyHttpClientError::CanNotExecuteRequest(err.to_string()));
                    }

                    if err.is_canceled() {
                        // Canceled means hyper never dispatched the request, so
                        // retrying is safe for any method. send_payload has already
                        // dropped the connection; if it died right after the
                        // handshake, pace the redial with a short sleep
                        retry_no += 1;

                        let now = DateTimeAsMicroseconds::now();
                        if now.duration_since(connected).as_positive_or_zero() < HYPER_INIT_TIMEOUT
                        {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }

                        self.connect().await?;
                        continue;
                    }

                    // Any other error: the request may have reached the upstream, so
                    // only idempotent requests are safe to replay
                    if !request_is_idempotent {
                        return Err(MyHttpClientError::CanNotExecuteRequest(err.to_string()));
                    }

                    retry_no += 1;
                    self.connect().await?;
                }
                SendHyperPayloadError::Disposed => {
                    return Err(MyHttpClientError::Disposed);
                }
                SendHyperPayloadError::UpgradedToWebsocket => {
                    return Err(MyHttpClientError::UpgradedToWebSocket);
                }
            }
        }
    }

    /// Sends a request whose body is produced as a stream - a proxied
    /// `hyper::body::Incoming`, a channel, a file reader, anything implementing
    /// `hyper::body::Body`.
    ///
    /// `content_size` is the framing of the payload:
    ///
    /// * `Some(size)` - the size is known upfront, the request goes out with
    ///   `Content-Length: size`. A body which does not deliver exactly that many bytes
    ///   kills the connection, so the number has to be the truth.
    /// * `None` - the size is not known, the request goes out with
    ///   `Transfer-Encoding: chunked`. A body which does know its size - a `Full<Bytes>` -
    ///   still goes out with a content-length: hyper takes it from the size hint.
    ///
    /// `request_timeout` covers the whole call - the upload of the payload included, not
    /// just the waiting for the response head. An upload which legitimately takes minutes
    /// needs a timeout of that size.
    ///
    /// Unlike [`Self::do_request`] this call is a single shot: the payload is consumed
    /// while it is being sent and there is nothing left to replay, so the request is
    /// never retried. On an error the producing side gets it back and decides itself
    /// whether to rebuild the stream and to call this method again -
    /// [`MyHttpClientError::is_retryable`] tells whether the connection was simply not
    /// there.
    pub async fn do_streamed_request<TBody>(
        &self,
        req: hyper::Request<TBody>,
        content_size: Option<usize>,
        request_timeout: Duration,
    ) -> Result<HyperHttpResponse, MyHttpClientError>
    where
        TBody: hyper::body::Body<Data = Bytes> + Send + Sync + 'static,
        TBody::Error: std::fmt::Display,
    {
        // There is no retry to fall back on, so the connection is established upfront.
        // It is a no-op when the client is already connected
        self.connect().await?;

        let (mut head, body) = req.into_parts();

        // The framing is announced by the headers hyper is given: an explicit
        // content-length wins over the size hint of the body, and its absence together
        // with an unknown size hint is what makes hyper send the request chunked
        match content_size {
            Some(content_size) => {
                head.headers.insert(
                    http::header::CONTENT_LENGTH,
                    http::HeaderValue::from(content_size),
                );
            }
            None => {
                head.headers.remove(http::header::CONTENT_LENGTH);
            }
        }

        // hyper_tungstenite needs the request head, and the request itself is moved into
        // hyper together with its body. Only a websocket handshake pays for that clone -
        // a plain streamed request never gets a 101 back
        #[cfg(feature = "with-websocket")]
        let websocket_head = if head.headers.contains_key(http::header::SEC_WEBSOCKET_KEY) {
            Some(head.clone())
        } else {
            None
        };

        let request = hyper::Request::from_parts(head, body.map_err(|err| err.to_string()).boxed());

        let response = self
            .inner
            .send_payload(request, request_timeout)
            .await
            .map_err(|err| match err {
                SendHyperPayloadError::Disconnected => MyHttpClientError::Disconnected,
                SendHyperPayloadError::Disposed => MyHttpClientError::Disposed,
                SendHyperPayloadError::UpgradedToWebsocket => {
                    MyHttpClientError::UpgradedToWebSocket
                }
                SendHyperPayloadError::RequestTimeout(duration) => {
                    MyHttpClientError::RequestTimeout(duration)
                }
                SendHyperPayloadError::HyperError { err, .. } => {
                    MyHttpClientError::CanNotExecuteRequest(err.to_string())
                }
            })?;

        if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            #[cfg(feature = "with-websocket")]
            {
                let Some(head) = websocket_head else {
                    // The connection is consumed by the upgrade and the request was not a
                    // websocket handshake - there is nothing to hand the socket over to
                    return Err(MyHttpClientError::UpgradedToWebSocket);
                };

                return self
                    .get_response(hyper::Request::from_parts(head, ()), response)
                    .await;
            }

            // Built without websocket support - see `get_response`: the 101 is handed
            // back as an ordinary response
        }

        Ok(HyperHttpResponse::Response(response))
    }

    /// Starts a streamed request which the client drives itself: the payload is pushed
    /// into the publisher created together with the body, and a failure of the request
    /// surfaces on the publishing side - `publish` returns the reason instead of the
    /// bare "the channel is closed".
    ///
    /// The response is picked up by [`StreamedRequest::get_response`], which has to be
    /// called after the publisher is dropped - an alive publisher means the payload is
    /// not complete yet.
    ///
    /// [`Self::do_streamed_request`] is the same thing without the spawn: there the
    /// caller awaits the request itself and gets the typed error out of it
    pub fn start_streamed_request<TChunk>(
        self: &Arc<Self>,
        req: hyper::Request<RequestBodyStream<TChunk>>,
        content_size: Option<usize>,
        request_timeout: Duration,
    ) -> StreamedRequest
    where
        TChunk: Into<Bytes> + Send + Sync + 'static,
    {
        let failure = req.body().get_failure_reason();

        let client = self.clone();

        let handle = tokio::spawn(async move {
            let result = client
                .do_streamed_request(req, content_size, request_timeout)
                .await;

            if let Err(err) = result.as_ref() {
                // The publisher is normally already blocked on a closed channel by now
                failure.set(err);
            }

            result
        });

        StreamedRequest::new(handle)
    }

    pub async fn connect(&self) -> Result<(), MyHttpClientError> {
        // Serializes dialers: a burst of failing requests produces one dial, not a
        // thundering herd. The state lock is NOT held across the dial, so concurrent
        // send_payload calls keep failing fast with Disconnected instead of queuing
        // on state.lock() for up to connect_timeout.
        let _dial_guard = self.connect_lock.lock().await;

        {
            let state = self.inner.state.lock().await;
            match &*state {
                MyHttpHyperConnectionState::Connected { .. } => return Ok(()),
                MyHttpHyperConnectionState::Disconnected => {}
                MyHttpHyperConnectionState::Disposed => return Err(MyHttpClientError::Disposed),
            }
        }

        // fetch_add returns the previous value; +1 keeps the counter equal to the id
        // stored in state, so MyHttpClientDisconnect::disconnect() (which loads the
        // counter) matches current_connection_id instead of always no-oping on the
        // off-by-one. Incremented past the early return, so no-op connect() calls do
        // not advance it; the dial guard makes this the only dialer, so the id
        // stored in state below always matches the counter.
        let connection_id = self
            .connection_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;

        let remote_host_port = self.connector.get_remote_endpoint().get_host_port();

        // The timeout covers the whole dial including the handshake: an upstream
        // that accepts TCP but never talks must not hang connect() forever
        let dial = async {
            let stream = self.connector.connect().await?;

            super::wrap_http1_endpoint::wrap_http1_endpoint(
                stream,
                remote_host_port.as_str(),
                self.inner.clone(),
                connection_id,
            )
            .await
        };

        let send_request = match tokio::time::timeout(self.connect_timeout, dial).await {
            Ok(dial_result) => dial_result?,
            Err(_) => {
                return Err(MyHttpClientError::CanNotConnectToRemoteHost(format!(
                    "Can not connect to Http1 remote endpoint: '{}' Timeout: {:?}",
                    remote_host_port.as_str(),
                    self.connect_timeout
                )));
            }
        };

        let mut state = self.inner.state.lock().await;

        // The client can be disposed while the dial was running without the state
        // lock; dropping send_request ends the freshly spawned conn task, whose
        // disconnect(connection_id) no-ops against the Disposed state
        if let MyHttpHyperConnectionState::Disposed = &*state {
            return Err(MyHttpClientError::Disposed);
        }

        *state = MyHttpHyperConnectionState::Connected {
            connected: DateTimeAsMicroseconds::now(),
            send_request,
            current_connection_id: connection_id,
            upgraded_to_websocket: false,
        };

        if let Some(metrics) = self.inner.metrics.as_ref() {
            metrics.connected(&self.inner.name);
        }

        Ok(())
    }
}

impl<
        TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
        TConnector: MyHttpClientConnector<TStream> + Send + Sync + 'static,
    > Drop for MyHttpHyperClient<TStream, TConnector>
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
    > From<TConnector> for MyHttpHyperClient<TStream, TConnector>
{
    fn from(value: TConnector) -> Self {
        Self::new(value)
    }
}

impl<
        TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
        TConnector: MyHttpClientConnector<TStream> + Send + Sync + 'static,
    > MyHttpClientDisconnect for MyHttpHyperClient<TStream, TConnector>
{
    fn disconnect(&self) {
        // It may be called outside a tokio runtime - out of a drop during shutdown, say -
        // and tokio::spawn would panic there
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let inner = self.inner.clone();
            let connection_id = self
                .connection_id
                .load(std::sync::atomic::Ordering::Relaxed);
            handle.spawn(async move { inner.disconnect(connection_id).await });
        }
    }
    fn web_socket_disconnect(&self) {
        // The connection that hosted the websocket is already released: hyper's
        // conn.with_upgrades() future resolves at the 101 upgrade, and the task in
        // wrap_http1_endpoint calls inner.disconnect for that id. Disconnecting by
        // the live counter here could only tear down a newer, unrelated connection.
    }
    fn get_connection_id(&self) -> u64 {
        self.connection_id
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}
