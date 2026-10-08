use std::{
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

use hyper::client::conn::http2::SendRequest;
use rust_extensions::date_time::DateTimeAsMicroseconds;
use tokio::sync::Mutex;

use crate::{hyper::*, BodyReader, HyperRequest, HyperRequestBody};

/// A single timed out request is a slow stream, not a dead connection. But this many
/// timeout rounds in a row with no success in between means the connection itself is
/// likely dead (e.g. black-holed TCP while keep-alive pings are not configured), so it
/// gets dropped to let the next request reconnect.
const MAX_CONSECUTIVE_TIMEOUTS: usize = 3;

pub enum MyHttp2ConnectionState {
    Disconnected,

    Connected {
        current_connection_id: u64,
        connected: DateTimeAsMicroseconds,
        send_request: SendRequest<HyperRequestBody>,
    },
    Disposed,
}

impl MyHttp2ConnectionState {
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected { .. })
    }
}

pub struct MyHttp2ClientInner {
    pub state: Mutex<MyHttp2ConnectionState>,
    pub name: String,
    pub metrics: Option<std::sync::Arc<dyn MyHttpHyperClientMetrics + Send + Sync + 'static>>,
    pub(crate) is_alive: AtomicBool,
    pub(crate) consecutive_timeouts: AtomicUsize,
    /// Monotonic anchor for round pacing: wall clock would let an NTP step stall
    /// dead-connection detection (backwards) or split one burst of concurrent
    /// timeouts into several rounds (forwards)
    created: std::time::Instant,
    /// Micros since `created` when the last counted timeout round started; written
    /// only under the state lock
    last_counted_timeout_micros: AtomicU64,
}

impl MyHttp2ClientInner {
    pub fn new(
        name: String,
        metrics: Option<std::sync::Arc<dyn MyHttpHyperClientMetrics + Send + Sync + 'static>>,
    ) -> Self {
        if let Some(metrics) = metrics.as_ref() {
            metrics.instance_created(name.as_str());
        }

        Self {
            state: Mutex::new(MyHttp2ConnectionState::Disconnected),

            name,

            metrics,
            is_alive: AtomicBool::new(false),
            consecutive_timeouts: AtomicUsize::new(0),
            created: std::time::Instant::now(),
            last_counted_timeout_micros: AtomicU64::new(0),
        }
    }

    /// Lock-free view of whether an established connection is held right now.
    /// All writes happen under the state mutex, so Relaxed is sufficient.
    pub fn is_alive(&self) -> bool {
        self.is_alive.load(Ordering::Relaxed)
    }

    /// The body is already erased to [`crate::HyperRequestBody`] here: one connection
    /// carries both the buffered and the streamed requests, so a single body type has to
    /// travel through `SendRequest`.
    ///
    /// The response comes back on its head: its body is read through the
    /// [`BodyReader`] it carries
    pub async fn send_payload(
        &self,
        req: HyperRequest,
        request_timeout: Duration,
    ) -> Result<hyper::Response<BodyReader>, SendHyperPayloadError> {
        let deadline = tokio::time::Instant::now().checked_add(request_timeout);

        let (send_request_feature, connected, current_connection_id) = {
            let mut state = self.state.lock().await;
            match &mut *state {
                MyHttp2ConnectionState::Disconnected => {
                    return Err(SendHyperPayloadError::Disconnected);
                }
                MyHttp2ConnectionState::Connected {
                    current_connection_id,
                    connected,
                    send_request,
                } => (
                    send_request.send_request(req),
                    *connected,
                    *current_connection_id,
                ),
                MyHttp2ConnectionState::Disposed => {
                    return Err(SendHyperPayloadError::Disposed);
                }
            }
        };

        let Ok(result) = tokio::time::timeout(request_timeout, send_request_feature).await else {
            // Dropping the timed out send_request future cancels only its own h2 stream
            // (RST_STREAM), so the connection stays available to other multiplexed
            // streams instead of being torn down because of one slow response.
            self.register_request_timeout(current_connection_id, request_timeout)
                .await;
            return Err(SendHyperPayloadError::RequestTimeout(request_timeout));
        };

        match result {
            Ok(response) => {
                self.consecutive_timeouts.store(0, Ordering::Relaxed);
                Ok(crate::from_hyper_response(
                    response,
                    deadline,
                    request_timeout,
                ))
            }
            Err(err) => {
                self.disconnect(current_connection_id).await;
                Err(SendHyperPayloadError::HyperError { connected, err })
            }
        }
    }

    /// Counts a request timeout against the connection it happened on and drops the
    /// connection after MAX_CONSECUTIVE_TIMEOUTS timeout rounds in a row. Rounds, not
    /// streams: N parallel requests timing out together prove no more deadness than
    /// one, so they collapse into a single round — otherwise 3 concurrent slow
    /// responses would tear down a healthy connection under all its other multiplexed
    /// streams. A new round is counted only after `request_timeout` has elapsed since
    /// the last counted one (or after a success reset the budget to zero). Checked
    /// under the state lock so a stale timeout from a previous connection never
    /// counts against (or disconnects) the current one.
    pub(crate) async fn register_request_timeout(
        &self,
        connection_id: u64,
        request_timeout: Duration,
    ) {
        let mut state = self.state.lock().await;

        match &*state {
            MyHttp2ConnectionState::Connected {
                current_connection_id,
                ..
            } => {
                if *current_connection_id != connection_id {
                    return;
                }
            }
            MyHttp2ConnectionState::Disconnected => {
                return;
            }
            MyHttp2ConnectionState::Disposed => {
                return;
            }
        }

        let now_micros = self.created.elapsed().as_micros() as u64;

        if self.consecutive_timeouts.load(Ordering::Relaxed) > 0 {
            let since_last_counted =
                now_micros - self.last_counted_timeout_micros.load(Ordering::Relaxed);
            if since_last_counted < request_timeout.as_micros() as u64 {
                return;
            }
        }

        self.last_counted_timeout_micros
            .store(now_micros, Ordering::Relaxed);

        let timeouts = self.consecutive_timeouts.fetch_add(1, Ordering::Relaxed) + 1;
        if timeouts < MAX_CONSECUTIVE_TIMEOUTS {
            return;
        }

        if let Some(metrics) = self.metrics.as_ref() {
            metrics.disconnected(self.name.as_str());
        }

        self.is_alive.store(false, Ordering::Relaxed);
        *state = MyHttp2ConnectionState::Disconnected;
    }

    pub async fn disconnect(&self, connection_id: u64) {
        let mut state = self.state.lock().await;

        match &*state {
            MyHttp2ConnectionState::Connected {
                current_connection_id,
                ..
            } => {
                if *current_connection_id != connection_id {
                    return;
                }

                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.disconnected(self.name.as_str());
                }
            }
            MyHttp2ConnectionState::Disconnected => {
                return;
            }

            MyHttp2ConnectionState::Disposed => {
                return;
            }
        }

        self.is_alive.store(false, Ordering::Relaxed);
        *state = MyHttp2ConnectionState::Disconnected;
    }

    pub async fn dispose(&self) {
        let mut state = self.state.lock().await;

        match &*state {
            MyHttp2ConnectionState::Connected { .. } => {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.disconnected(self.name.as_str());
                }
            }
            MyHttp2ConnectionState::Disconnected => {}

            MyHttp2ConnectionState::Disposed => {}
        }

        self.is_alive.store(false, Ordering::Relaxed);
        *state = MyHttp2ConnectionState::Disposed;
    }

    pub async fn force_disconnect(&self) {
        let mut state = self.state.lock().await;

        match &*state {
            MyHttp2ConnectionState::Connected { .. } => {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.disconnected(self.name.as_str());
                }
            }
            MyHttp2ConnectionState::Disconnected => {
                return;
            }
            // Disposed is a terminal state - a forced disconnect must not resurrect
            // the client back into a connectable one
            MyHttp2ConnectionState::Disposed => {
                return;
            }
        }

        self.is_alive.store(false, Ordering::Relaxed);
        *state = MyHttp2ConnectionState::Disconnected;
    }
}

impl Drop for MyHttp2ClientInner {
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.instance_disposed(&self.name);
        }
    }
}
