use bytes::Bytes;
use tokio::sync::mpsc;

pub(super) enum BodyEvent {
    Data(Bytes),
    /// The body is sent to its end
    Completed,
    /// The body is over before its end, and this is why
    Failed(String),
}

/// Where the body of a response is written to by whoever reads the connection - the
/// read loop of the non-hyper client. The body goes piece by piece, as it comes off the
/// socket, and has to be ended explicitly: a sender which is just dropped leaves a body
/// which is not complete
pub struct BodySender {
    sender: mpsc::Sender<BodyEvent>,
}

impl BodySender {
    pub(super) fn new(sender: mpsc::Sender<BodyEvent>) -> Self {
        Self { sender }
    }

    /// Waits while [`super::RESPONSE_BODY_CHANNEL_CAPACITY`] pieces are waiting for the
    /// reader already - that is what keeps a body nobody is in a hurry to read out of
    /// memory. `false`: the reader is dropped, nobody needs the body any more
    pub async fn send(&self, data: Bytes) -> bool {
        self.sender.send(BodyEvent::Data(data)).await.is_ok()
    }

    /// The body is sent to its end
    pub async fn complete(self) {
        let _ = self.sender.send(BodyEvent::Completed).await;
    }

    /// The body can not be sent to its end. The reason is what the reader gets instead
    /// of the rest of it
    pub async fn fail(self, reason: String) {
        let _ = self.sender.send(BodyEvent::Failed(reason)).await;
    }

    /// Resolves once the reader is dropped
    pub async fn reader_is_dropped(&self) {
        self.sender.closed().await
    }
}
