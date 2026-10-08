use super::ChunksReadingMode;

/// How a response body is delimited on the wire (RFC 9112 §6.3), and how far the reading
/// of it has got
#[derive(Debug, Clone, Copy)]
pub enum BodyFraming {
    /// `Content-Length`: the amount of bytes of the body which are not read yet
    LengthBased(usize),
    Chunked(ChunksReadingMode),
    /// Neither a length nor chunks: the body runs until the connection is closed, and
    /// the connection is consumed by it
    UntilClose,
    /// The body is read to its end
    Completed,
}
