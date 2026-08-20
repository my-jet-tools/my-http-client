use crate::MyHttpClientError;

use super::HyperHttpResponse;

/// A streamed request which the client drives on its own - see
/// [`super::MyHttpHyperClient::start_streamed_request`].
///
/// The payload goes into the [`crate::RequestBodyPublisher`] handed out together with
/// this one; a failure of the request surfaces on the publishing side right away, and
/// the response is picked up here
pub struct StreamedRequest {
    handle: tokio::task::JoinHandle<Result<HyperHttpResponse, MyHttpClientError>>,
}

impl StreamedRequest {
    pub fn new(
        handle: tokio::task::JoinHandle<Result<HyperHttpResponse, MyHttpClientError>>,
    ) -> Self {
        Self { handle }
    }

    /// Waits for the response. The publisher has to be dropped before that - the body is
    /// over only when its channel is closed, so an alive publisher means the upstream is
    /// still waiting for the rest of the payload
    pub async fn get_response(self) -> Result<HyperHttpResponse, MyHttpClientError> {
        match self.handle.await {
            Ok(result) => result,
            // The task is only aborted when the runtime goes down, and it does not panic:
            // do_streamed_request returns its errors
            Err(err) => Err(MyHttpClientError::CanNotExecuteRequest(format!(
                "Streamed request task is gone: {}",
                err
            ))),
        }
    }
}
