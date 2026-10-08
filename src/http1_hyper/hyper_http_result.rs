#[cfg(feature = "with-websocket")]
use hyper_tungstenite::HyperWebsocket;

pub enum HyperHttpResponse {
    /// The head of the response is read, its body is not: it is read through the
    /// [`crate::BodyReader`]
    Response(http::Response<crate::BodyReader>),
    /// A `101 Switching Protocols` answering a websocket handshake. Only built
    /// with the `with-websocket` feature - without it the 101 comes back as an
    /// ordinary `Response`.
    #[cfg(feature = "with-websocket")]
    WebSocketUpgrade {
        response: crate::HyperResponse,
        web_socket: HyperWebsocket,
    },
}
