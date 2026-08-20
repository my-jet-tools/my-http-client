use std::{sync::Arc, time::Duration};

use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};

use crate::{HyperRequestBody, MyHttpClientError};

use super::MyHttp2ClientInner;

pub async fn wrap_http2_endpoint<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
>(
    stream: TStream,
    remote_host: &str,
    inner: Arc<MyHttp2ClientInner>,
    connection_id: u64,
    keep_alive: Option<(Duration, Duration)>,
) -> Result<SendRequest<HyperRequestBody>, MyHttpClientError> {
    let io = TokioIo::new(stream);

    let mut builder = hyper::client::conn::http2::Builder::new(TokioExecutor::new());

    if let Some((interval, timeout)) = keep_alive {
        // hyper panics inside handshake if keep-alive is enabled without a timer
        builder.timer(TokioTimer::new());
        builder.keep_alive_interval(interval);
        builder.keep_alive_timeout(timeout);
        // Idle connections are exactly the ones a pool needs probed
        builder.keep_alive_while_idle(true);
    }

    let handshake_result = builder.handshake(io).await;

    match handshake_result {
        Ok((mut sender, conn)) => {
            tokio::task::spawn(async move {
                let _ = conn.await;
                inner.disconnect(connection_id).await;
            });

            if let Err(err) = sender.ready().await {
                return Err(MyHttpClientError::CanNotConnectToRemoteHost(format!(
                    "Can not establish Http2 connection to '{remote_host}'. Reading awaiting is finished with {}",
                    err
                )));
            }

            Ok(sender)
        }
        Err(err) => {
            Err(MyHttpClientError::CanNotConnectToRemoteHost(format!(
                "Can not establish Http2 connection to '{remote_host}'. Http2 handshake Error: {}",
                err
            )))
        }
    }
}
