use std::{
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{BodyReader, MyHttpClientConnector, MyHttpClientError};

use super::{MyHttp2ClientInner, MyHttp2ConnectionState};
use crate::hyper::*;

pub struct MyHttp2Client<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
    TConnector: MyHttpClientConnector<TStream> + Send + Sync + 'static,
> {
    connector: TConnector,
    stream: PhantomData<TStream>,
    inner: Arc<MyHttp2ClientInner>,
    connect_timeout: Duration,
    connection_id: AtomicU64,
    keep_alive: Option<(Duration, Duration)>,
    // tokio::sync::Mutex by design: held across the dial (TCP connect + h2
    // handshake) to serialize concurrent dialers, so parking_lot does not fit
    connect_lock: tokio::sync::Mutex<()>,
}

impl<
        TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
        TConnector: MyHttpClientConnector<TStream> + Send + Sync + 'static,
    > MyHttp2Client<TStream, TConnector>
{
    pub fn new(connector: TConnector) -> Self {
        Self {
            inner: Arc::new(MyHttp2ClientInner::new(
                connector.get_remote_endpoint().get_host_port().to_string(),
                None,
            )),
            connector,

            stream: PhantomData,
            connect_timeout: Duration::from_secs(5),
            connection_id: AtomicU64::new(0),
            keep_alive: None,
            connect_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn new_with_metrics(
        connector: TConnector,
        metrics: Arc<dyn MyHttpHyperClientMetrics + Send + Sync + 'static>,
    ) -> Self {
        Self {
            inner: Arc::new(MyHttp2ClientInner::new(
                connector.get_remote_endpoint().get_host_port().to_string(),
                Some(metrics),
            )),
            connector,

            stream: PhantomData,
            connect_timeout: Duration::from_secs(5),
            connection_id: AtomicU64::new(0),
            keep_alive: None,
            connect_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn set_connect_timeout(&mut self, connection_timeout: Duration) {
        self.connect_timeout = connection_timeout;
    }

    /// Enables h2 keep-alive pings: a PING frame is sent every `interval`, and if the
    /// peer does not answer within `timeout` the connection is closed, which also
    /// resets [`Self::is_alive`]. Must be configured before the client is shared.
    pub fn set_keep_alive(&mut self, interval: Duration, timeout: Duration) {
        self.keep_alive = Some((interval, timeout));
    }

    /// Lock-free check whether the client holds an established connection right now.
    /// `false` does not mean the client is unusable: it connects lazily, so this is
    /// `false` before the first request and becomes `true` again after a reconnect.
    /// Without [`Self::set_keep_alive`] a silently dead peer (no FIN/RST) is only
    /// detected when a request fails, so the flag can stay stale-`true` until then.
    pub fn is_alive(&self) -> bool {
        self.inner.is_alive()
    }

    /// Sends a request with a buffered body. The payload stays in memory for the whole
    /// call, so a failed attempt can be replayed - see [`Self::do_streamed_request`] for
    /// the streaming counterpart which is a single shot
    pub async fn do_request(
        &self,
        req: &hyper::Request<Full<Bytes>>,
        request_timeout: Duration,
    ) -> Result<hyper::Response<BodyReader>, MyHttpClientError> {
        let request_is_idempotent = req.method().is_idempotent();
        let mut retry_no = 0;
        loop {
            // Every attempt gets its own copy: the body is erased into a trait object
            // before it goes to hyper and can not be taken back from there
            let payload = crate::utils::to_hyper_request(req.clone());

            let err = match self.inner.send_payload(payload, request_timeout).await {
                Ok(response) => {
                    return Ok(response);
                }
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
                    // A timeout is a slow response, not a dead connection. Replaying
                    // piles more load on a struggling upstream and can execute a
                    // non-idempotent request twice; dead connections are handled by
                    // keep-alive pings and the consecutive-timeouts limit in send_payload
                    return Err(MyHttpClientError::RequestTimeout(duration));
                }
                SendHyperPayloadError::HyperError { connected, err } => {
                    // send_payload has already disconnected this connection by id,
                    // so no force_disconnect here: it would race with a newer
                    // connection created by a concurrent request
                    if retry_no > 3 {
                        return Err(MyHttpClientError::CanNotExecuteRequest(err.to_string()));
                    }

                    if err.is_canceled() {
                        // Canceled means hyper never dispatched the request to an h2
                        // stream, so retrying is safe for any method. send_payload has
                        // already dropped the connection; if it died right after the
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

    /// Sends a request whose body is produced as a stream - a [`crate::RequestBodyStream`]
    /// fed by a channel, a proxied `hyper::body::Incoming`, anything implementing
    /// `hyper::body::Body`.
    ///
    /// HTTP/2 has no chunked encoding: the payload is a sequence of DATA frames ended by
    /// END_STREAM, so `content_size` only decides whether the length is announced upfront.
    /// `Some(size)` writes a `content-length` header - and hyper then refuses a body which
    /// does not deliver exactly that many bytes - while `None` sends the body without one.
    ///
    /// Unlike HTTP/1.1 a streamed request does not occupy the connection: h2 multiplexes,
    /// so other requests keep flowing through their own streams while this body is being
    /// uploaded.
    ///
    /// `request_timeout` covers the whole call - the upload of the payload included, not
    /// just the waiting for the response head.
    ///
    /// Unlike [`Self::do_request`] this call is a single shot: the payload is consumed
    /// while it is being sent and there is nothing left to replay, so the request is
    /// never retried. On an error the producing side gets it back and decides itself
    /// whether to rebuild the stream and to send the request again.
    pub async fn do_streamed_request<TBody>(
        &self,
        req: hyper::Request<TBody>,
        content_size: Option<usize>,
        request_timeout: Duration,
    ) -> Result<hyper::Response<BodyReader>, MyHttpClientError>
    where
        TBody: hyper::body::Body<Data = Bytes> + Send + Sync + 'static,
        TBody::Error: std::fmt::Display,
    {
        // There is no retry to fall back on, so the connection is established upfront.
        // It is a no-op when the client is already connected
        self.connect().await?;

        let (mut head, body) = req.into_parts();

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

        let request = hyper::Request::from_parts(head, body.map_err(|err| err.to_string()).boxed());

        self.inner
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
            })
    }

    pub async fn do_extended_connect(
        &self,
        path: &str,
        headers: hyper::HeaderMap,
        request_timeout: Duration,
    ) -> Result<hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>, MyHttpClientError> {
        let authority = self
            .connector
            .get_remote_endpoint()
            .get_host_port()
            .to_string();

        self.do_extended_connect_inner(&authority, path, headers, request_timeout)
            .await
    }

    /// Extended CONNECT for Unix Domain Socket transports.
    ///
    /// `:authority` over UDS is just metadata in the h2 HEADERS frame — actual routing
    /// is already done by the UDS connect. The connector's `host_port` is a filesystem
    /// path which produces an empty `:authority` (`http:///path/...`), and hyper's
    /// CONNECT validator rejects that with "invalid format". This method substitutes
    /// `localhost` as a placeholder authority.
    pub async fn do_extended_connect_unix(
        &self,
        path: &str,
        headers: hyper::HeaderMap,
        request_timeout: Duration,
    ) -> Result<hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>, MyHttpClientError> {
        self.do_extended_connect_inner("localhost", path, headers, request_timeout)
            .await
    }

    async fn do_extended_connect_inner(
        &self,
        authority: &str,
        path: &str,
        headers: hyper::HeaderMap,
        request_timeout: Duration,
    ) -> Result<hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>, MyHttpClientError> {
        self.connect().await?;

        let mut req = hyper::Request::builder()
            .method(hyper::Method::CONNECT)
            .uri(format!("http://{}{}", authority, path))
            .body(crate::utils::empty_request_body())
            .map_err(|err| MyHttpClientError::CanNotExecuteRequest(err.to_string()))?;

        *req.headers_mut() = headers;

        req.extensions_mut()
            .insert(hyper::ext::Protocol::from_static("websocket"));

        let (send_fut, current_connection_id) = {
            let mut state = self.inner.state.lock().await;
            match &mut *state {
                MyHttp2ConnectionState::Disconnected => {
                    return Err(MyHttpClientError::Disconnected);
                }
                MyHttp2ConnectionState::Connected {
                    send_request,
                    current_connection_id,
                    ..
                } => (send_request.send_request(req), *current_connection_id),
                MyHttp2ConnectionState::Disposed => {
                    return Err(MyHttpClientError::Disposed);
                }
            }
        };

        let resp_result = tokio::time::timeout(request_timeout, send_fut).await;

        let resp = match resp_result {
            Err(_) => {
                // Dropping the timed out send_fut cancels only the CONNECT stream
                // (RST_STREAM); one slow websocket handshake must not tear down the
                // connection under other multiplexed streams. Dead connections are
                // still caught by the same consecutive-timeouts policy as do_request
                self.inner
                    .register_request_timeout(current_connection_id, request_timeout)
                    .await;
                return Err(MyHttpClientError::RequestTimeout(request_timeout));
            }
            Ok(Err(err)) => {
                self.inner.disconnect(current_connection_id).await;
                return Err(MyHttpClientError::CanNotExecuteRequest(err.to_string()));
            }
            Ok(Ok(resp)) => resp,
        };

        // Any completed CONNECT round trip proves the connection alive - reset the
        // consecutive-timeouts budget the same way send_payload does on success
        self.inner
            .consecutive_timeouts
            .store(0, std::sync::atomic::Ordering::Relaxed);

        if !resp.status().is_success() {
            return Err(MyHttpClientError::CanNotExecuteRequest(format!(
                "Extended CONNECT failed with status: {}",
                resp.status()
            )));
        }

        let upgraded = hyper::upgrade::on(resp).await.map_err(|err| {
            MyHttpClientError::CanNotExecuteRequest(format!(
                "Extended CONNECT upgrade failed: {}",
                err
            ))
        })?;

        Ok(hyper_util::rt::TokioIo::new(upgraded))
    }

    pub async fn connect(&self) -> Result<(), MyHttpClientError> {
        // Serializes dialers: a burst of failing requests produces one dial, not a
        // thundering herd. The state lock is NOT held across the dial, so concurrent
        // send_payload calls keep failing fast with Disconnected (and requests on a
        // still-alive connection keep flowing) instead of queuing on state.lock()
        // for up to connect_timeout.
        let _dial_guard = self.connect_lock.lock().await;

        {
            let state = self.inner.state.lock().await;
            match &*state {
                MyHttp2ConnectionState::Connected { .. } => return Ok(()),
                MyHttp2ConnectionState::Disconnected => {}
                MyHttp2ConnectionState::Disposed => return Err(MyHttpClientError::Disposed),
            }
        }

        // Incremented past the early return, so no-op connect() calls do not advance
        // the counter; the dial guard makes this the only dialer, so the id stored
        // in state below always matches the counter; +1 mirrors the http1_hyper
        // client where the counter must equal the id stored in state
        let connection_id = self
            .connection_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;

        let remote_host_port = self.connector.get_remote_endpoint().get_host_port();

        // The timeout covers the whole dial including the h2 handshake: an upstream
        // that accepts TCP but never answers SETTINGS must not hang connect() forever
        let dial = async {
            let stream = self.connector.connect().await?;

            super::wrap_http2_endpoint::wrap_http2_endpoint(
                stream,
                remote_host_port.as_str(),
                self.inner.clone(),
                connection_id,
                self.keep_alive,
            )
            .await
        };

        let send_request = match tokio::time::timeout(self.connect_timeout, dial).await {
            Ok(dial_result) => dial_result?,
            Err(_) => {
                return Err(MyHttpClientError::CanNotConnectToRemoteHost(format!(
                    "Can not connect to Http2 remote endpoint: '{}' Timeout: {:?}",
                    remote_host_port.as_str(),
                    self.connect_timeout
                )));
            }
        };

        let mut state = self.inner.state.lock().await;

        // The client can be disposed while the dial was running without the state
        // lock; dropping send_request ends the freshly spawned conn task, whose
        // disconnect(connection_id) no-ops against the Disposed state
        if let MyHttp2ConnectionState::Disposed = &*state {
            return Err(MyHttpClientError::Disposed);
        }

        *state = MyHttp2ConnectionState::Connected {
            connected: DateTimeAsMicroseconds::now(),
            send_request,
            current_connection_id: connection_id,
        };

        self.inner
            .is_alive
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.inner
            .consecutive_timeouts
            .store(0, std::sync::atomic::Ordering::Relaxed);

        if let Some(metrics) = self.inner.metrics.as_ref() {
            metrics.connected(&self.inner.name);
        }

        Ok(())
    }
}

impl<
        TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
        TConnector: MyHttpClientConnector<TStream> + Send + Sync + 'static,
    > Drop for MyHttp2Client<TStream, TConnector>
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
    > From<TConnector> for MyHttp2Client<TStream, TConnector>
{
    fn from(value: TConnector) -> Self {
        Self::new(value)
    }
}
