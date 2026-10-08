pub mod http1;

mod error;
pub use error::*;

pub mod http2;
mod my_http_client_connector;
pub mod utils;
pub use my_http_client_connector::*;
mod my_http_client_disconnect;
pub use my_http_client_disconnect::*;

pub mod http1_hyper;
pub mod hyper;

pub type HyperResponse = http::Response<http_body_util::combinators::BoxBody<bytes::Bytes, String>>;

/// Body of an outgoing request as it is given to hyper. One connection serves requests
/// with different body types, so the body is erased to the `hyper::body::Body` trait
/// object - it is what makes both a buffered `Full<Bytes>` and a stream of chunks
/// travel through the very same connection
pub type HyperRequestBody = http_body_util::combinators::BoxBody<bytes::Bytes, String>;

pub type HyperRequest = http::Request<HyperRequestBody>;

mod request_body_stream;
pub use request_body_stream::*;

mod body_reader;
pub use body_reader::*;

mod headers;
pub use headers::*;

const CL_CR: &[u8] = b"\r\n";
pub extern crate http;
