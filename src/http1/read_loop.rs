use std::{sync::Arc, time::Duration};

use futures::future::Either;

use crate::{BodyReader, BodySender};

use super::{HttpParseError, TcpBuffer};

use super::{
    BodyFraming, ChunksReadingMode, HttpTask, MyHttpClientInner, ResponseBodyOnTheWire,
    ResponseHead,
};
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
            Ok(response_head) => match response_head {
                ResponseHead::Interim => {
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
                ResponseHead::LengthBased { builder, body_size } => {
                    interim_count = 0;

                    let connection_goes_on = if body_size == 0 {
                        let response = builder.body(BodyReader::empty())?;
                        deliver_response(&inner, connection_id, response)
                    } else {
                        let body = ResponseBodyOnTheWire::new(
                            &mut read_stream,
                            &mut tcp_buffer,
                            BodyFraming::LengthBased(body_size),
                            read_timeout,
                            print_input_http_stream,
                        );

                        send_response(&inner, connection_id, builder, body).await?
                    };

                    if !connection_goes_on {
                        return Ok(());
                    }
                }
                ResponseHead::UntilClose { builder } => {
                    // Close-delimited body: it is sent to its reader until EOF, then
                    // the loop stops. The stream is consumed by the close, so this
                    // connection must not be reused for keep-alive. Returning Ok(())
                    // lets `read_loop_stopped` transition it to Disconnected so the
                    // next send reconnects.
                    let body = ResponseBodyOnTheWire::new(
                        &mut read_stream,
                        &mut tcp_buffer,
                        BodyFraming::UntilClose,
                        read_timeout,
                        print_input_http_stream,
                    );

                    send_response(&inner, connection_id, builder, body).await?;

                    return Ok(());
                }
                ResponseHead::Chunked { builder } => {
                    interim_count = 0;

                    let body = ResponseBodyOnTheWire::new(
                        &mut read_stream,
                        &mut tcp_buffer,
                        BodyFraming::Chunked(ChunksReadingMode::WaitingFroChunkSize),
                        read_timeout,
                        print_input_http_stream,
                    );

                    if !send_response(&inner, connection_id, builder, body).await? {
                        return Ok(());
                    }
                }
                ResponseHead::SwitchedProtocols { builder } => {
                    // The protocol is switched away from HTTP: the head is the
                    // final answer, and the connection can not serve HTTP any
                    // more. Returning Ok(()) lets `read_loop_stopped` move it to
                    // Disconnected so the next send dials a new one.
                    let response = builder.body(BodyReader::empty())?;
                    deliver_response(&inner, connection_id, response);

                    return Ok(());
                }
                #[cfg(feature = "with-websocket")]
                ResponseHead::WebSocketUpgrade(mut builder) => {
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

/// Hands the response over to the request it answers. `false`: there is nobody to hand
/// it to - no request is waiting for an answer, or its caller has given up. A response
/// nobody has asked for leaves nothing to do with the connection but to close it.
///
/// The response is dropped then, and the reader of its body with it. It must not stay
/// here: a body which is sent to a reader held by the sending side waits for a reader
/// which is never going to read
fn deliver_response<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
>(
    inner: &MyHttpClientInner<TStream>,
    connection_id: u64,
    response: http::Response<BodyReader>,
) -> bool {
    let Some(mut request) = inner.pop_request(connection_id, false) else {
        return false;
    };

    request.try_set_ok(HttpTask::Response(response)).is_ok()
}

/// Hands the response over on its head, and sends its body after it to the reader the
/// response carries - piece by piece, as it comes off the socket.
///
/// `false` is a connection which can not go on with the next response: nobody waits
/// for this one, or its body is abandoned by the reader with more of it left on the
/// wire than is worth reading past - see [`super::MAX_ABANDONED_BODY_SIZE`] and
/// [`super::ABANDONED_BODY_SKIP_TIMEOUT`].
///
/// Fails when the builder carries an error - a head of the response it did not take -
/// and when the body can not be read to its end
async fn send_response<
    TStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
>(
    inner: &MyHttpClientInner<TStream>,
    connection_id: u64,
    builder: http::response::Builder,
    mut body: ResponseBodyOnTheWire<'_, TStream>,
) -> Result<bool, HttpParseError> {
    let (sender, body_reader) = BodyReader::new(body.remains_to_read());
    let response = builder.body(body_reader)?;

    if !deliver_response(inner, connection_id, response) {
        return Ok(false);
    }

    if send_body(&mut body, sender).await? {
        return Ok(true);
    }

    // The reader is dropped before the end of the body. A client which is disposed
    // meanwhile has no use for the connection
    if !inner.is_my_connection_id(connection_id) {
        return Ok(false);
    }

    let skipped = tokio::time::timeout(
        super::ABANDONED_BODY_SKIP_TIMEOUT,
        body.skip_the_rest(super::MAX_ABANDONED_BODY_SIZE),
    )
    .await;

    Ok(matches!(skipped, Ok(true)))
}

/// Sends the body to its reader. `true`: the body is sent to its end; `false`: the
/// reader is dropped, nobody needs the rest of it.
///
/// A piece is not copied out of the buffer it is read into: it is a part of one of the
/// two buffers of the body. While the reader is busy with one piece the socket is read
/// into the other buffer, and when that one is read as well the socket is not read until
/// the reader is done with the first. So the body is not piled up here, however much the
/// upstream has to give: what is in memory is the two buffers - see
/// [`super::TcpBuffer`].
///
/// The reader may be dropped while nothing comes from the upstream - an event stream
/// which is silent - so the reader is watched while the socket is read: the requests
/// behind this body must not wait for a piece of it nobody is going to take.
///
/// The head of the response is with its caller by the time the body is read, so a
/// failure can not fail the request any more. It is sent down the body instead, as its
/// last item - otherwise the body would just end, and what was read before the failure
/// would look like the whole of it
async fn send_body<TStream: tokio::io::AsyncRead>(
    body: &mut ResponseBodyOnTheWire<'_, TStream>,
    sender: BodySender,
) -> Result<bool, HttpParseError> {
    loop {
        let piece = {
            let next_piece = std::pin::pin!(body.next_piece());
            let reader_is_dropped = std::pin::pin!(sender.reader_is_dropped());

            match futures::future::select(next_piece, reader_is_dropped).await {
                Either::Left((piece, _)) => piece,
                Either::Right(_) => return Ok(false),
            }
        };

        match piece {
            Ok(Some(piece)) => {
                if !sender.send(piece).await {
                    return Ok(false);
                }
            }
            Ok(None) => {
                sender.complete().await;
                return Ok(true);
            }
            Err(err) => {
                sender.fail(why_the_body_is_not_complete(&err)).await;

                // An invalid payload is reported to the request which is next in the
                // queue, and this one is not about it: its response has not begun
                return Err(match err {
                    HttpParseError::InvalidHttpPayload(reason) => HttpParseError::Error(reason),
                    err => err,
                });
            }
        }
    }
}

fn why_the_body_is_not_complete(err: &HttpParseError) -> String {
    let reason = match err {
        HttpParseError::InvalidHttpPayload(reason) => reason.as_str().to_string(),
        HttpParseError::Error(reason) => reason.as_str().to_string(),
        HttpParseError::Disconnected => "the connection is closed".to_string(),
        HttpParseError::ReadingTimeout(timeout) => {
            format!("no data from the upstream for {:?}", timeout)
        }
        HttpParseError::GetMoreData => "the rest of it has not arrived".to_string(),
    };

    format!("The response body is not complete: {}", reason)
}
