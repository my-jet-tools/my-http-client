use std::sync::Arc;

use http::{HeaderMap, StatusCode};

use crate::{BodyReader, MyHttpClientDisconnect};

pub enum MyHttpResponse<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
> {
    /// The head of the response is read, its body is not: it follows through the
    /// [`BodyReader`], and the connection is busy with it until it is read
    Response(http::Response<BodyReader>),
    WebSocketUpgrade {
        stream: TStream,
        response: crate::HyperResponse,
        disconnection: Arc<dyn MyHttpClientDisconnect + Send + Sync + 'static>,
        /// Bytes which came in the same read() as the `101` head but past it -
        /// as a rule the first websocket frame, which servers write right behind
        /// the handshake. They are already off the socket: feed them to the
        /// websocket reader before `stream`, otherwise that frame is lost.
        leftover: Vec<u8>,
    },
}

impl<TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static>
    MyHttpResponse<TStream>
{
    pub fn status(&self) -> StatusCode {
        match self {
            MyHttpResponse::Response(response) => response.status(),
            MyHttpResponse::WebSocketUpgrade { response, .. } => response.status(),
        }
    }

    pub fn headers(&self) -> &HeaderMap {
        match self {
            MyHttpResponse::Response(response) => response.headers(),
            MyHttpResponse::WebSocketUpgrade { response, .. } => response.headers(),
        }
    }

    pub fn headers_mut(&mut self) -> &HeaderMap {
        match self {
            MyHttpResponse::Response(response) => response.headers_mut(),
            MyHttpResponse::WebSocketUpgrade { response, .. } => response.headers_mut(),
        }
    }

    /// The response as hyper's: the body reader is a `hyper::body::Body`, and the body
    /// still comes frame by frame, as it is read off the socket
    pub fn into_response(self) -> crate::HyperResponse {
        match self {
            MyHttpResponse::Response(response) => response.map(BodyReader::into_bytes_body),
            MyHttpResponse::WebSocketUpgrade { response, .. } => response,
        }
    }
}
