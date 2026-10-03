use std::{sync::Arc, time::Duration};

use super::{HttpParseError, TcpBuffer};

use super::{BodyReader, HttpTask, MyHttpClientInner};
use tokio::io::ReadHalf;

pub async fn read_loop<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
>(
    mut read_stream: ReadHalf<TStream>,
    connection_id: u64,
    inner: Arc<MyHttpClientInner<TStream>>,
    read_timeout: Duration,
) -> Result<(), HttpParseError> {
    let mut do_read_to_buffer = true;

    // Consecutive interim (1xx) responses seen before the current final response.
    let mut interim_count: usize = 0;

    let mut tcp_buffer = TcpBuffer::new();

    let print_input_http_stream = if let Ok(value) = std::env::var("DEBUG_HTTP_INPUT_STREAM") {
        println!("http_client_name: {}", inner.name.as_str());
        value.as_str() == inner.name.as_str()
    } else {
        false
    };
    while inner.is_my_connection_id(connection_id) {
        if do_read_to_buffer || tcp_buffer.is_empty() {
            super::read_with_timeout::read_to_buffer(
                &mut read_stream,
                &mut tcp_buffer,
                read_timeout,
                print_input_http_stream,
            )
            .await?;

            do_read_to_buffer = false;
        }

        // The response currently on the wire belongs to the request at the
        // front of the queue; its method drives RFC 9112 §6.3 body framing.
        let request_method = inner.peek_request_method(connection_id);

        match super::headers_reader::read_headers(
            &mut read_stream,
            &mut tcp_buffer,
            read_timeout,
            print_input_http_stream,
            request_method,
        )
        .await
        {
            Ok(body_reader) => match body_reader {
                BodyReader::Interim => {
                    // A non-final 1xx response (e.g. 100 Continue / 103 Early
                    // Hints). Discard it WITHOUT popping the request and keep
                    // reading for the real final response, which still belongs
                    // to the same front-of-queue request. Bound the count so a
                    // server cannot pin the read loop with endless 1xx messages.
                    interim_count += 1;
                    if interim_count > super::MAX_INTERIM_RESPONSES {
                        return Err(HttpParseError::invalid_payload(format!(
                            "Received more than {} consecutive interim (1xx) responses",
                            super::MAX_INTERIM_RESPONSES
                        )));
                    }
                    continue;
                }
                BodyReader::LengthBased { builder, body_size } => {
                    interim_count = 0;
                    let response = super::body_reader::read_full_body(
                        &mut read_stream,
                        &mut tcp_buffer,
                        builder,
                        body_size,
                        read_timeout,
                    )
                    .await?;

                    let request = inner.pop_request(connection_id, false);
                    if let Some(mut request) = request {
                        let result = request.try_set_ok(HttpTask::Response(response));

                        if result.is_err() {
                            return Ok(());
                        }
                    }
                }
                BodyReader::UntilClose { builder } => {
                    // Close-delimited body: read to EOF, hand back the response,
                    // then stop. The stream is consumed by the close, so this
                    // connection must not be reused for keep-alive. Returning
                    // Ok(()) lets `read_loop_stopped` transition it to
                    // Disconnected so the next send reconnects.
                    let response = super::body_reader::read_until_close(
                        &mut read_stream,
                        &mut tcp_buffer,
                        builder,
                        read_timeout,
                    )
                    .await?;

                    let request = inner.pop_request(connection_id, false);
                    if let Some(mut request) = request {
                        let _ = request.try_set_ok(HttpTask::Response(response));
                    }

                    return Ok(());
                }
                BodyReader::Chunked { response, sender } => {
                    interim_count = 0;
                    let request = inner.pop_request(connection_id, false);
                    if let Some(mut request) = request {
                        let result = request.try_set_ok(HttpTask::Response(response));

                        if result.is_err() {
                            return Ok(());
                        }
                    }

                    super::body_reader::read_chunked_body(
                        &mut read_stream,
                        &mut tcp_buffer,
                        sender,
                        read_timeout,
                        print_input_http_stream,
                    )
                    .await?;
                }
                BodyReader::SwitchedProtocols { builder } => {
                    // The protocol is switched away from HTTP: the head is the
                    // final answer, and the connection can not serve HTTP any
                    // more. Returning Ok(()) lets `read_loop_stopped` move it to
                    // Disconnected so the next send dials a new one.
                    let response = crate::utils::into_empty_body(builder)?;
                    let request = inner.pop_request(connection_id, false);
                    if let Some(mut request) = request {
                        let _ = request.try_set_ok(HttpTask::Response(response));
                    }

                    return Ok(());
                }
                #[cfg(feature = "with-websocket")]
                BodyReader::WebSocketUpgrade(mut builder) => {
                    let upgrade_response = builder.take_upgrade_response()?;

                    // The server normally writes its first websocket frame right
                    // behind the 101, so both land in the same read() and the
                    // frame is sitting in the buffer past the head. `tcp_buffer`
                    // is local to this loop and dies with it, so what is left in
                    // it has to travel with the socket - otherwise that first
                    // frame is lost forever and only the second one is seen.
                    let leftover = tcp_buffer.get_buf().to_vec();

                    let request = inner.pop_request(connection_id, true);
                    if let Some(mut request) = request {
                        let _ = request.try_set_ok(HttpTask::WebsocketUpgrade {
                            response: upgrade_response,
                            read_part: read_stream,
                            leftover,
                        });
                    }

                    return Ok(());
                }
            },
            Err(err) => match err {
                super::HttpParseError::GetMoreData => {
                    do_read_to_buffer = true;
                }
                _ => {
                    return Err(err);
                }
            },
        }
    }

    Ok(())
}
