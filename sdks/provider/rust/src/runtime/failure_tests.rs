//! Service-level cancellation and bounded-flow checks. Provider execution is a
//! controlled stream; provider-owned final reports determine accounting.
use super::*;
use crate::interfaces::ProviderLifecycle;
use futures_core::Stream;
use proto::chat_server::Chat;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tonic::{
    body::Body,
    codegen::{Bytes, Service, http},
};

struct Flood {
    polled: Arc<AtomicU64>,
}

struct RequestBody(Option<Bytes>);
impl http_body::Body for RequestBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        Poll::Ready(self.0.take().map(|bytes| Ok(http_body::Frame::data(bytes))))
    }
}
impl Stream for Flood {
    type Item = IntegrationResult<Vec<u8>>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polled.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Some(Ok(br#"{"choices":[]}"#.to_vec())))
    }
}
#[tonic::async_trait]
impl CoverageProvider for Flood {
    async fn coverage(
        &self,
        _: &PreparedRequest,
        _: &RunContext,
    ) -> IntegrationResult<TokenCounts> {
        Ok(counts(1000))
    }
}
impl ExecutionProvider for Flood {
    fn execute(
        &self,
        _: PreparedRequest,
        _: RunContext,
        _: Arc<dyn ProviderRuns>,
        _: CancellationToken,
    ) -> ExecutionStream {
        Box::pin(Self {
            polled: self.polled.clone(),
        })
    }
}

async fn flooding() -> (Fixture, Arc<AtomicU64>) {
    let mut fixture = Fixture::new(700).await;
    let polled = Arc::new(AtomicU64::new(0));
    let binding = Arc::new(Flood {
        polled: polled.clone(),
    });
    let runtime = Arc::get_mut(&mut fixture.provider.inner).unwrap();
    runtime.coverage = binding.clone();
    runtime.execution = binding;
    fixture.voucher(1);
    (fixture, polled)
}

async fn wait_for_full_queue(polled: &AtomicU64) {
    for _ in 0..100 {
        if polled.load(Ordering::SeqCst) == 65 {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("execution did not fill the 64-message queue");
}

fn stored_context(fixture: &Fixture) -> RunContext {
    fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)]
        .context
        .clone()
}

#[tokio::test(start_paused = true)]
async fn stalled_complete_keeps_control_reports_independent_and_output_bounded() {
    let (fixture, polled) = flooding().await;
    let response = fixture
        .provider
        .completions(tonic::Request::new(fixture.request(1)))
        .await
        .unwrap();
    wait_for_full_queue(&polled).await;
    let context = stored_context(&fixture);
    let result = fixture
        .provider
        .report_usage(&context, counts(500))
        .await
        .unwrap();
    assert_eq!(result.meter_seq(), 1);
    let control_queue = fixture.provider.inner.lock().unwrap().attachments[&1]
        .queue
        .clone();
    let message =
        proto::ControlMessage::decode(control_queue.get().await.unwrap().as_slice()).unwrap();
    let Some(proto::control_message::Kind::Meter(meter)) = message.kind else {
        panic!("missing independent meter");
    };
    assert_eq!(meter.output_tokens, Some(500));
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(polled.load(Ordering::SeqCst), 65);
    drop(response);
    {
        let state = fixture.provider.inner.lock().unwrap();
        let run = &state.sessions[&fixture.sid].runs[&hash(1)];
        assert!(run.stop.is_cancelled());
        assert!(run.active());
        assert!(run.accounting.remaining_numerator() > U256::ZERO);
    }
    fixture
        .provider
        .report_terminal(&context, counts(500), RunStatus::Cancelled)
        .await
        .unwrap();
    let result = fixture
        .provider
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(result.unresolved_runs.is_empty());
    assert_eq!(polled.load(Ordering::SeqCst), 65);
}

#[tokio::test(start_paused = true)]
async fn shutdown_deadline_preserves_unfinished_reservation_and_stops_owned_stream() {
    let (fixture, polled) = flooding().await;
    let response = fixture
        .provider
        .completions(tonic::Request::new(fixture.request(1)))
        .await
        .unwrap();
    wait_for_full_queue(&polled).await;
    let context = stored_context(&fixture);
    let before =
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)].accounting;
    let result = fixture
        .provider
        .shutdown(Instant::now() + Duration::from_millis(10))
        .await
        .unwrap();
    assert_eq!(result.unresolved_session_ids, [fixture.sid]);
    assert_eq!(
        result.unresolved_runs.as_slice(),
        std::slice::from_ref(&context)
    );
    tokio::task::yield_now().await;
    {
        let state = fixture.provider.inner.lock().unwrap();
        let run = &state.sessions[&fixture.sid].runs[&hash(1)];
        assert!(run.stop.is_cancelled());
        assert_eq!(run.accounting, before);
        assert!(!state.attachments[&1].live);
    }
    assert_eq!(polled.load(Ordering::SeqCst), 65);
    assert!(
        fixture
            .provider
            .completions(tonic::Request::new(fixture.request(2)))
            .await
            .is_err()
    );
    // A definitive provider report remains valid after shutdown; stop alone
    // must never fabricate zero usage or release the funded reservation.
    fixture
        .provider
        .report_terminal(&context, counts(500), RunStatus::Cancelled)
        .await
        .unwrap();
    let state = fixture.provider.inner.lock().unwrap();
    assert_eq!(
        state.sessions[&fixture.sid].runs[&hash(1)]
            .accounting
            .remaining_numerator(),
        U256::ZERO
    );
    drop(state);
    drop(response);
}

#[tokio::test(start_paused = true)]
async fn shutdown_accepts_provider_final_report_while_waiting() {
    let (fixture, polled) = flooding().await;
    let response = fixture
        .provider
        .completions(tonic::Request::new(fixture.request(1)))
        .await
        .unwrap();
    wait_for_full_queue(&polled).await;
    let context = stored_context(&fixture);
    let sdk = fixture.provider.clone();
    let shutdown = tokio::spawn(async move {
        sdk.shutdown(Instant::now() + Duration::from_secs(10))
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    assert!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)]
            .stop
            .is_cancelled()
    );
    fixture
        .provider
        .report_terminal(&context, counts(200), RunStatus::Cancelled)
        .await
        .unwrap();
    let result = shutdown.await.unwrap();
    assert!(result.unresolved_session_ids.is_empty());
    assert!(result.unresolved_runs.is_empty());
    assert!(result.pending_operations.is_empty());
    drop(response);
}

#[tokio::test(start_paused = true)]
async fn complete_deadline_cannot_release_usage_without_provider_final_report() {
    let (fixture, polled) = flooding().await;
    // Deadlines start at the LimitsService ingress boundary, before protobuf
    // decoding; calling the Chat trait directly bypasses that boundary.
    let bytes = fixture.request(1).encode_to_vec();
    let mut frame = vec![0];
    frame.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(&bytes);
    let request = http::Request::builder()
        .method("POST")
        .uri("/mppi.v1.Chat/Completions")
        .header("content-type", "application/grpc")
        .header("grpc-timeout", "20m")
        .body(Body::new(RequestBody(Some(Bytes::from(frame)))))
        .unwrap();
    let (mut chat, _) = fixture.provider.grpc_services();
    let response = chat.call(request).await.unwrap();
    wait_for_full_queue(&polled).await;
    tokio::time::advance(Duration::from_millis(21)).await;
    tokio::task::yield_now().await;
    let context = stored_context(&fixture);
    {
        let state = fixture.provider.inner.lock().unwrap();
        let run = &state.sessions[&fixture.sid].runs[&hash(1)];
        assert!(run.stop.is_cancelled());
        assert!(run.active());
        assert!(run.unresolved);
        assert!(run.accounting.remaining_numerator() > U256::ZERO);
    }
    assert_eq!(polled.load(Ordering::SeqCst), 65);
    fixture
        .provider
        .report_terminal(&context, counts(100), RunStatus::Cancelled)
        .await
        .unwrap();
    fixture
        .provider
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    drop(response);
}
