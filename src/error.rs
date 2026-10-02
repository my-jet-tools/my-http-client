use std::time::Duration;

#[derive(Debug)]
pub enum MyHttpClientError {
    CanNotConnectToRemoteHost(String),
    UpgradedToWebSocket,
    Disconnected,
    Disposed,
    RequestTimeout(Duration),
    CanNotExecuteRequest(String),
    InvalidHttpHandshake(String),
    #[cfg(feature = "with-websocket")]
    HyperWebsocket(hyper_tungstenite::HyperWebsocket),
}

impl MyHttpClientError {
    pub fn is_web_socket_upgraded(&self) -> bool {
        matches!(self, MyHttpClientError::UpgradedToWebSocket)
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, MyHttpClientError::Disconnected)
    }
}

/// Why a request can not be built out of the input it was given. A path, the name of a
/// header and its value come from the settings of a service as often as from its code,
/// so what must not be put on the wire is reported instead of being trusted.
///
/// The offending byte is in the error and the rest of the input is not: the value of a
/// header is where the secrets are
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestBuildError {
    /// CR, LF, NUL or a space in the path and query - any of them ends the request line
    /// before its time
    ForbiddenByteInPath(u8),
    HeaderNameIsEmpty,
    /// The name of a header has to be an HTTP token
    ForbiddenByteInHeaderName(u8),
    /// CR, LF or NUL in the value of a header - any of them splits the header block
    ForbiddenByteInHeaderValue(u8),
    /// The head of the request can not be handed to hyper: it is not a request line
    /// followed by headers, or hyper refuses its method, its uri or one of its headers
    NotConvertibleToHyper(String),
}

impl std::fmt::Display for RequestBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ForbiddenByteInPath(byte) => write!(
                f,
                "Request path contains forbidden byte 0x{:02x} (request line injection)",
                byte
            ),
            Self::HeaderNameIsEmpty => f.write_str("HTTP header name must not be empty"),
            Self::ForbiddenByteInHeaderName(byte) => {
                write!(f, "HTTP header name contains forbidden byte 0x{:02x}", byte)
            }
            Self::ForbiddenByteInHeaderValue(byte) => write!(
                f,
                "HTTP header value contains forbidden control byte 0x{:02x} (header injection)",
                byte
            ),
            Self::NotConvertibleToHyper(reason) => {
                write!(
                    f,
                    "Request can not be converted to a hyper request: {}",
                    reason
                )
            }
        }
    }
}

impl std::error::Error for RequestBuildError {}
