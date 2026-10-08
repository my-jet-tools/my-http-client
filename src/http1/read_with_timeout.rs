use std::time::Duration;

use tokio::io::{AsyncReadExt, ReadHalf};

use super::{HttpParseError, TcpBuffer};

/// Reads the socket once. With both buffers of the [`TcpBuffer`] held by the pieces
/// cut out of them, it waits for one of them to be free - see [`TcpBuffer::read_from`]
pub async fn read_to_buffer<TStream: tokio::io::AsyncRead>(
    read: &mut ReadHalf<TStream>,
    tcp_buffer: &mut TcpBuffer,
    read_time_out: Duration,
    print_http_payload: bool,
) -> Result<(), HttpParseError> {
    tcp_buffer.read_from(read, read_time_out).await?;

    if print_http_payload {
        println!("Resp: [{:?}]", std::str::from_utf8(tcp_buffer.get_buf()));
    }

    Ok(())
}

pub async fn read_exact<TStream: tokio::io::AsyncRead>(
    read_stream: &mut ReadHalf<TStream>,
    buffer_to_write: &mut [u8],
    read_timeout: Duration,
) -> Result<usize, HttpParseError> {
    let mut pos = 0;
    loop {
        let feature = read_stream.read(&mut buffer_to_write[pos..]);

        let Ok(result) = tokio::time::timeout(read_timeout, feature).await else {
            return Err(HttpParseError::ReadingTimeout(read_timeout));
        };

        match result {
            Ok(result) => {
                if result == 0 {
                    return Err(HttpParseError::Disconnected);
                }

                pos += result;

                if pos == buffer_to_write.len() {
                    return Ok(result);
                }
            }
            Err(err) => {
                return Err(HttpParseError::error(format!(
                    "Error reading exact buffer: {:?}",
                    err
                )))
            }
        }
    }
}

pub async fn skip_exactly<TStream: tokio::io::AsyncRead>(
    read_stream: &mut ReadHalf<TStream>,
    tcp_buffer: &mut TcpBuffer,
    size_to_skip: usize,
    read_timeout: Duration,
    print_input_http_stream: bool,
) -> Result<(), HttpParseError> {
    loop {
        match tcp_buffer.skip_exactly(size_to_skip) {
            Ok(()) => {
                return Ok(());
            }
            Err(HttpParseError::GetMoreData) => {
                read_to_buffer(
                    read_stream,
                    tcp_buffer,
                    read_timeout,
                    print_input_http_stream,
                )
                .await?;
            }
            Err(err) => return Err(err),
        }
    }
}

pub async fn read_until_crlf<TResult, TStream: tokio::io::AsyncRead>(
    read_stream: &mut ReadHalf<TStream>,
    tcp_buffer: &mut TcpBuffer,
    read_timeout: Duration,
    conversion: impl Fn(&[u8]) -> Result<TResult, HttpParseError>,
    print_input_http_stream: bool,
) -> Result<TResult, HttpParseError> {
    loop {
        match tcp_buffer.read_until_crlf() {
            Some(as_str) => return conversion(as_str),
            None => {
                read_to_buffer(
                    read_stream,
                    tcp_buffer,
                    read_timeout,
                    print_input_http_stream,
                )
                .await?;
            }
        }
    }
}
