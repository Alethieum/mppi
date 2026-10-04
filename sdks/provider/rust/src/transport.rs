//! Native service framing limits and request deadline evidence.
//!
//! The generated tonic services retain their decompressed message limits. This
//! adapter checks the gRPC length prefix before tonic allocates its message
//! buffer, preserving MPPI's RESOURCE_EXHAUSTED status for oversized messages.
//! It forwards the original body frames without collecting their payloads.

use crate::{ValidationCode, ValidationError, wire::MAX_MESSAGE_BYTES};
use http_body::{Body as HttpBody, Frame, SizeHint};
use std::{
    pin::Pin,
    task::{Context, Poll, ready},
    time::Duration,
};
use tokio::time::Instant;
use tonic::{
    Status,
    body::Body,
    codegen::{Bytes, Service, http},
    server::NamedService,
};

/// A generated service with a checked gRPC frame length and request start time.
///
/// The inner service must also enforce the same decompressed decoding bound.
/// Outgoing application messages must pass the SDK's wire validation before
/// encoding. These services compose directly into a caller-owned tonic server.
#[derive(Clone, Debug)]
pub struct LimitsService<S> {
    inner: S,
    max_message_bytes: usize,
}

impl<S> LimitsService<S> {
    pub fn new(inner: S, max_message_bytes: usize) -> Result<Self, ValidationError> {
        if !(1..=MAX_MESSAGE_BYTES).contains(&max_message_bytes) {
            return Err(ValidationError::new(
                ValidationCode::InvalidValue,
                "invalid transport message limit",
            ));
        }
        Ok(Self {
            inner,
            max_message_bytes,
        })
    }
}

impl<S: NamedService> NamedService for LimitsService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> Service<http::Request<Body>> for LimitsService<S>
where
    S: Service<http::Request<Body>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        let timing = RequestTiming(parse_deadline(request.headers(), Instant::now()));
        request.extensions_mut().insert(timing);
        let limit = self.max_message_bytes;
        self.inner
            .call(request.map(|body| Body::new(LimitedBody::new(body, limit))))
    }
}

#[derive(Clone, Debug)]
struct RequestTiming(Result<Option<Instant>, Status>);

/// Return the fixed deadline captured before native request-body decoding.
///
/// Call this before performing any admission work. Malformed or duplicate
/// grpc-timeout fields return INVALID_ARGUMENT. Direct calls to a handler that
/// bypass `LimitsService` have no captured deadline.
pub fn request_deadline<T>(request: &tonic::Request<T>) -> Result<Option<Instant>, Status> {
    request
        .extensions()
        .get::<RequestTiming>()
        .map_or(Ok(None), |timing| timing.0.clone())
}

fn parse_deadline(headers: &http::HeaderMap, started: Instant) -> Result<Option<Instant>, Status> {
    let malformed = || Status::invalid_argument("invalid grpc-timeout metadata");
    let mut values = headers.get_all("grpc-timeout").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(malformed());
    }
    let value = value.as_bytes();
    if !(2..=9).contains(&value.len()) {
        return Err(malformed());
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if !digits.iter().all(u8::is_ascii_digit) {
        return Err(malformed());
    }
    let amount = digits
        .iter()
        .fold(0u64, |amount, digit| amount * 10 + u64::from(digit - b'0'));
    let duration = match unit[0] {
        b'H' => Duration::from_secs(amount * 3600),
        b'M' => Duration::from_secs(amount * 60),
        b'S' => Duration::from_secs(amount),
        b'm' => Duration::from_millis(amount),
        b'u' => Duration::from_micros(amount),
        b'n' => Duration::from_nanos(amount),
        _ => return Err(malformed()),
    };
    // The eight-digit bound makes duration multiplication safe. A zero duration
    // is already expired, matching tonic's timeout interpretation.
    started
        .checked_add(duration)
        .map(Some)
        .ok_or_else(malformed)
}

struct LimitedBody {
    inner: Body,
    header: [u8; 5],
    header_bytes: usize,
    remaining: usize,
    limit: usize,
    finished: bool,
}

impl LimitedBody {
    fn new(inner: Body, limit: usize) -> Self {
        Self {
            inner,
            header: [0; 5],
            header_bytes: 0,
            remaining: 0,
            limit,
            finished: false,
        }
    }

    fn inspect(&mut self, mut bytes: &[u8]) -> Result<(), Status> {
        while !bytes.is_empty() {
            if self.remaining > 0 {
                let consumed = self.remaining.min(bytes.len());
                self.remaining -= consumed;
                bytes = &bytes[consumed..];
                continue;
            }
            let consumed = (5 - self.header_bytes).min(bytes.len());
            self.header[self.header_bytes..self.header_bytes + consumed]
                .copy_from_slice(&bytes[..consumed]);
            self.header_bytes += consumed;
            bytes = &bytes[consumed..];
            if self.header_bytes == 5 {
                let length = u32::from_be_bytes([
                    self.header[1],
                    self.header[2],
                    self.header[3],
                    self.header[4],
                ]) as usize;
                if length > self.limit {
                    return Err(Status::resource_exhausted("wire message limit exceeded"));
                }
                self.remaining = length;
                self.header_bytes = 0;
            }
        }
        Ok(())
    }

    fn complete(&self) -> bool {
        self.header_bytes == 0 && self.remaining == 0
    }
}

impl HttpBody for LimitedBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        let frame = ready!(Pin::new(&mut self.inner).poll_frame(cx));
        match frame {
            Some(Ok(frame)) => {
                let checked = if let Some(data) = frame.data_ref() {
                    self.inspect(data)
                } else if !self.complete() {
                    Err(Status::invalid_argument("truncated gRPC message"))
                } else {
                    Ok(())
                };
                if let Err(status) = checked {
                    self.finished = true;
                    return Poll::Ready(Some(Err(status)));
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Some(Err(status)) => {
                self.finished = true;
                Poll::Ready(Some(Err(status)))
            }
            None => {
                self.finished = true;
                if self.complete() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(Status::invalid_argument(
                        "truncated gRPC message",
                    ))))
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.finished || (self.complete() && self.inner.is_end_stream())
    }

    fn size_hint(&self) -> SizeHint {
        if self.finished {
            SizeHint::with_exact(0)
        } else {
            self.inner.size_hint()
        }
    }
}
