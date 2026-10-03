#[cfg(feature = "with-websocket")]
use bytes::Bytes;
#[cfg(feature = "with-websocket")]
use http_body_util::combinators::BoxBody;

mod full_body_reader;
pub use full_body_reader::*;
mod body_reader_chunked;
pub use body_reader_chunked::*;
mod until_close_body_reader;
pub use until_close_body_reader::*;
mod streamed_body_reader;
pub use streamed_body_reader::*;

mod full_body_reader_inner;
pub use full_body_reader_inner::*;

#[derive(Debug)]
pub enum BodyReader {
    LengthBased {
        builder: http::response::Builder,
        body_size: usize,
    },
    Chunked {
        response: crate::HyperResponse,
        sender: ChunksSender,
    },
    /// A close-delimited response body (RFC 9112 §6.3): no `Content-Length` and
    /// no `Transfer-Encoding`, so the body runs until the connection is closed.
    /// Valid only on responses; the connection is consumed and must not be
    /// returned to the keep-alive pool afterwards.
    UntilClose {
        builder: http::response::Builder,
    },
    /// A non-final interim (1xx) response other than a websocket upgrade — e.g.
    /// `100 Continue` or `103 Early Hints` (RFC 9110 §15.2). It carries no body
    /// and must NOT complete the pending request: the read loop discards it and
    /// keeps reading for the real (>= 200) final response.
    Interim,
    /// A `101 Switching Protocols` which is not taken over as a websocket - the
    /// upgrade is to another protocol (`h2c`, ...) or the crate is built without
    /// the `with-websocket` feature. It is NOT an interim 1xx: the protocol is
    /// switched away from HTTP, so nothing else will ever arrive on this
    /// connection. The head is delivered as the final, bodyless response and the
    /// connection is retired instead of being read past.
    SwitchedProtocols {
        builder: http::response::Builder,
    },
    #[cfg(feature = "with-websocket")]
    WebSocketUpgrade(WebSocketUpgradeBuilder),
}

#[cfg(feature = "with-websocket")]
#[derive(Debug)]
pub struct WebSocketUpgradeBuilder {
    builder: Option<http::response::Builder>,
}

#[cfg(feature = "with-websocket")]
impl WebSocketUpgradeBuilder {
    pub fn new(builder: http::response::Builder) -> Self {
        Self {
            builder: Some(builder),
        }
    }

    /// Fails when the response is taken already, and when the builder carries an error -
    /// a head of the upgrade response it did not take
    pub fn take_upgrade_response(
        &mut self,
    ) -> Result<http::Response<BoxBody<Bytes, String>>, super::HttpParseError> {
        let Some(builder) = self.builder.take() else {
            return Err(super::HttpParseError::error(
                "WebSocket upgrade response is already taken",
            ));
        };

        Ok(crate::utils::into_empty_body(builder)?)
    }
}
