use http_body::{Body as HttpBody, Frame};
use mppi_provider::{
    proto,
    transport::{LimitsService, request_deadline},
    wire::MAX_MESSAGE_BYTES,
};
use std::{
    collections::VecDeque,
    convert::Infallible,
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::Instant;
use tonic::{
    Code, Status,
    body::Body,
    codegen::{Bytes, Service, http},
    server::NamedService,
};

struct Frames(VecDeque<Result<Frame<Bytes>, Status>>);
impl HttpBody for Frames {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        Poll::Ready(self.0.pop_front())
    }
}

#[derive(Default)]
struct Captured {
    data: Vec<Bytes>,
    trailers: Option<http::HeaderMap>,
    error: Option<Status>,
    deadline: Option<Result<Option<Instant>, Status>>,
}

#[derive(Clone, Default)]
struct Capture {
    delay: Option<Duration>,
}
impl NamedService for Capture {
    const NAME: &'static str = "fixture.Capture";
}
impl Service<http::Request<Body>> for Capture {
    type Response = Captured;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Captured, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let delay = self.delay;
        Box::pin(async move {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let request = tonic::Request::from_http(request);
            let mut captured = Captured {
                deadline: Some(request_deadline(&request)),
                ..Captured::default()
            };
            let mut body = request.into_inner();
            while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
                match frame {
                    Ok(frame) => match frame.into_data() {
                        Ok(data) => captured.data.push(data),
                        Err(frame) => captured.trailers = frame.into_trailers().ok(),
                    },
                    Err(error) => {
                        captured.error = Some(error);
                        assert!(
                            poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                                .await
                                .is_none()
                        );
                        break;
                    }
                }
            }
            Ok(captured)
        })
    }
}

fn framed(flag: u8, payload: &[u8]) -> Bytes {
    let mut data = vec![flag];
    data.extend(u32::try_from(payload.len()).unwrap().to_be_bytes());
    data.extend(payload);
    Bytes::from(data)
}

fn request(frames: Vec<Result<Frame<Bytes>, Status>>) -> http::Request<Body> {
    http::Request::new(Body::new(Frames(frames.into())))
}

async fn capture(limit: usize, frames: Vec<Result<Frame<Bytes>, Status>>) -> Captured {
    let mut service = LimitsService::new(Capture::default(), limit).unwrap();
    service.call(request(frames)).await.unwrap()
}

#[tokio::test]
async fn split_prefixes_payloads_and_coalesced_messages_preserve_original_frames() {
    let mut combined = framed(0, b"1234").to_vec();
    combined.extend_from_slice(&framed(1, b"xy"));
    combined.extend_from_slice(&framed(0, b""));
    let combined = Bytes::from(combined);
    for split in 0..=combined.len() {
        let first = combined.slice(..split);
        let second = combined.slice(split..);
        let expected = [first.clone(), second.clone()];
        let observed = capture(4, vec![Ok(Frame::data(first)), Ok(Frame::data(second))]).await;
        assert!(observed.error.is_none(), "split {split}");
        assert_eq!(observed.data, expected);
        // The adapter forwards the same byte storage, including compression
        // flags, instead of collecting or rebuilding the message frames.
        for (actual, original) in observed.data.iter().zip(&expected) {
            assert_eq!(actual.as_ptr(), original.as_ptr());
        }
    }
}

#[tokio::test]
async fn oversized_declared_length_fails_before_payload_arrives() {
    for flag in [0, 1] {
        let header = framed(flag, b"12345").slice(..5);
        for split in 0..=5 {
            let observed = capture(
                4,
                vec![
                    Ok(Frame::data(header.slice(..split))),
                    Ok(Frame::data(header.slice(split..))),
                ],
            )
            .await;
            assert_eq!(observed.error.unwrap().code(), Code::ResourceExhausted);
        }
    }
    let mut together = framed(0, b"1234").to_vec();
    together.extend_from_slice(&[0, 0, 0, 0, 5]);
    let observed = capture(4, vec![Ok(Frame::data(Bytes::from(together)))]).await;
    assert_eq!(observed.error.unwrap().code(), Code::ResourceExhausted);
    assert!(observed.data.is_empty());
}

#[tokio::test]
async fn truncated_header_and_payload_are_invalid_at_eof_or_trailers() {
    let full = framed(0, b"1234");
    for length in 1..full.len() {
        for trailers in [false, true] {
            let mut frames = vec![Ok(Frame::data(full.slice(..length)))];
            if trailers {
                frames.push(Ok(Frame::trailers(http::HeaderMap::new())));
            }
            let observed = capture(4, frames).await;
            assert_eq!(observed.error.unwrap().code(), Code::InvalidArgument);
        }
    }
}

#[tokio::test]
async fn empty_body_trailers_and_native_errors_are_preserved() {
    let observed = capture(4, vec![]).await;
    assert!(observed.error.is_none());
    let mut trailers = http::HeaderMap::new();
    trailers.insert("fixture", http::HeaderValue::from_static("preserved"));
    let observed = capture(
        4,
        vec![
            Ok(Frame::data(framed(0, b"1234"))),
            Ok(Frame::trailers(trailers.clone())),
        ],
    )
    .await;
    assert!(observed.error.is_none());
    assert_eq!(observed.trailers.unwrap(), trailers);
    for code in [Code::Cancelled, Code::Unavailable, Code::DeadlineExceeded] {
        let observed = capture(4, vec![Err(Status::new(code, "fixture"))]).await;
        let status = observed.error.unwrap();
        assert_eq!(status.code(), code);
        assert_eq!(status.message(), "fixture");
    }
}

#[tokio::test]
async fn deadline_is_captured_before_inner_handler_work() {
    let mut service = LimitsService::new(
        Capture {
            delay: Some(Duration::from_millis(20)),
        },
        4,
    )
    .unwrap();
    let mut request = request(vec![]);
    request
        .headers_mut()
        .insert("grpc-timeout", http::HeaderValue::from_static("1m"));
    let started = Instant::now();
    let deadline = service
        .call(request)
        .await
        .unwrap()
        .deadline
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(deadline >= started);
    assert!(deadline < Instant::now());
}

#[tokio::test]
async fn timeout_units_absence_zero_and_malformed_metadata() {
    for (value, duration) in [
        ("1H", Duration::from_secs(3600)),
        ("2M", Duration::from_secs(120)),
        ("3S", Duration::from_secs(3)),
        ("4m", Duration::from_millis(4)),
        ("5u", Duration::from_micros(5)),
        ("6n", Duration::from_nanos(6)),
        ("0n", Duration::ZERO),
        ("99999999n", Duration::from_nanos(99_999_999)),
    ] {
        let mut service = LimitsService::new(Capture::default(), 4).unwrap();
        let mut request = request(vec![]);
        request
            .headers_mut()
            .insert("grpc-timeout", value.parse().unwrap());
        let earliest = Instant::now() + duration;
        let result = service.call(request).await.unwrap();
        let latest = Instant::now() + duration;
        let deadline = result.deadline.unwrap().unwrap().unwrap();
        assert!(deadline >= earliest && deadline <= latest, "{value}");
    }
    for values in [
        vec![""],
        vec!["S"],
        vec!["+1S"],
        vec!["-1S"],
        vec!["1s"],
        vec!["1 S"],
        vec!["123456789n"],
        vec!["1S,2S"],
        vec!["1S", "2S"],
    ] {
        let mut service = LimitsService::new(Capture::default(), 4).unwrap();
        let mut request = request(vec![]);
        for value in values {
            request
                .headers_mut()
                .append("grpc-timeout", value.parse().unwrap());
        }
        let result = service.call(request).await.unwrap();
        assert_eq!(
            result.deadline.unwrap().unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    let result = capture(4, vec![]).await;
    assert_eq!(result.deadline.unwrap().unwrap(), None);
    assert_eq!(request_deadline(&tonic::Request::new(())).unwrap(), None);
    assert!(LimitsService::new(Capture::default(), 0).is_err());
    assert!(LimitsService::new(Capture::default(), MAX_MESSAGE_BYTES + 1).is_err());
    assert_eq!(
        <LimitsService<Capture> as NamedService>::NAME,
        Capture::NAME
    );
}

struct Chat(Arc<AtomicUsize>);
#[tonic::async_trait]
impl proto::chat_server::Chat for Chat {
    type CompletionsStream = tonic::codegen::BoxStream<proto::ChatChunk>;
    async fn completions(
        &self,
        _request: tonic::Request<proto::CompleteRequest>,
    ) -> Result<tonic::Response<Self::CompletionsStream>, Status> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(tonic::Response::new(Box::pin(
            tonic::codegen::tokio_stream::empty(),
        )))
    }
}

#[tokio::test]
async fn native_generated_unary_service_returns_resource_exhausted_before_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let inner = proto::chat_server::ChatServer::new(Chat(calls.clone()))
        .max_decoding_message_size(4)
        .max_encoding_message_size(4);
    let mut service = LimitsService::new(inner, 4).unwrap();
    let mut request = request(vec![Ok(Frame::data(framed(0, b"12345")))]);
    *request.uri_mut() = "/mppi.v1.Chat/Completions".parse().unwrap();
    *request.method_mut() = http::Method::POST;
    request.headers_mut().insert(
        "content-type",
        http::HeaderValue::from_static("application/grpc"),
    );
    let response = service.call(request).await.unwrap();
    let status = Status::from_header_map(response.headers()).unwrap();
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
