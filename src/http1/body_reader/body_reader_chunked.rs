use crate::http1::HttpParseError;

/// How much of a chunk size line is repeated in the text of an error
const MAX_CHUNK_SIZE_LEN_IN_ERROR: usize = 16;

#[derive(Debug, Clone, Copy)]
pub enum ChunksReadingMode {
    WaitingFroChunkSize,
    /// The amount of bytes of the chunk which are not read yet
    ReadingChunk(usize),
    WaitingForSeparator,
    /// The terminating chunk is read: what is left is the trailers, if there are any,
    /// and the empty line which ends the body
    WaitingForEnd,
}

pub(super) fn parse_chunk_size(src: &[u8]) -> Result<usize, HttpParseError> {
    let mut end_of_hex = src.len();

    for (i, &byte) in src.iter().enumerate() {
        if !byte.is_ascii_hexdigit() {
            end_of_hex = i;
            break;
        }
    }

    if end_of_hex == 0 {
        // The line is whatever the upstream has sent: it does not have to be UTF-8 and
        // it can be as long as the read buffer
        let shown = &src[..src.len().min(MAX_CHUNK_SIZE_LEN_IN_ERROR)];

        return Err(HttpParseError::invalid_payload(format!(
            "Invalid chunk size: {:?}",
            String::from_utf8_lossy(shown)
        )));
    }

    let hex_str = std::str::from_utf8(&src[0..end_of_hex])
        .map_err(|_| HttpParseError::invalid_payload("Invalid UTF-8 in chunk size"))?;

    usize::from_str_radix(hex_str, 16).map_err(|_| {
        HttpParseError::invalid_payload(format!("Can not parse chunk size: {}", hex_str))
    })
}
