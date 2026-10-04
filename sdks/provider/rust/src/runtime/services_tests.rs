//! Regression coverage for atomic native Control attachment transitions.
//! Locked behavior: docs/protocol.md, authenticated Control and recovery.
use super::*;
use crate::signing::ReconnectAuth;
use tonic::{
    body::Body,
    codegen::{Service, http},
};

fn voucher(fixture: &Fixture, cumulative: u64) -> proto::Voucher {
    proto::Voucher {
        session_id: Some(fixture.sid.to_vec()),
        cumulative: Some(cumulative),
        signature: Some(sign(&Voucher {
            sessionId: fixture.sid,
            cumulativeAmount: cumulative,
        })),
    }
}

fn candidate(fixture: &Fixture, queue: Arc<ByteQueue>) -> proto::ReconnectRequest {
    let nonce = hash(0x72);
    fixture.provider.inner.lock().unwrap().attachments.insert(
        2,
        Attachment {
            queue,
            nonce,
            live: true,
            sid: None,
            generation: 0,
            request: None,
            challenges: BTreeMap::new(),
            catalogs: BTreeMap::new(),
            challenge_bytes: 0,
        },
    );
    proto::ReconnectRequest {
        session_id: Some(fixture.sid.to_vec()),
        nonce: Some(nonce.to_vec()),
        signature: Some(sign(&ReconnectAuth {
            sessionId: fixture.sid,
            nonce,
        })),
        latest_voucher: Some(voucher(fixture, 2)),
        last_meter_seq: Some(0),
        pending_run_ids: vec![],
    }
}

fn assert_original_owner(fixture: &Fixture) {
    let state = fixture.provider.inner.lock().unwrap();
    let session = &state.sessions[&fixture.sid];
    assert_eq!(session.generation, 1);
    assert_eq!(session.owner, Some(1));
    assert_eq!(session.voucher.as_ref().unwrap().cumulative, Some(1));
    assert!(state.attachments[&1].live);
    assert_eq!(state.attachments[&2].generation, 0);
    assert!(state.attachments[&2].request.is_none());
    assert!(state.attachments[&2].sid.is_none());
    // A closed queue would make this fail even if the live flag survived.
    state.attachments[&1]
        .queue
        .put_nowait(b"owner still open")
        .unwrap();
}

#[tokio::test]
async fn active_reconnect_failure_preserves_owner_voucher_and_stop_token() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(1);
    let run = fixture.admit(1);
    let queue = Arc::new(ByteQueue::default());
    let request = candidate(&fixture, queue.clone());
    let error = fixture.provider.inner.attach(2, request).unwrap_err();
    assert_eq!(reason(&error).as_deref(), Some("STATE_UNAVAILABLE"));
    assert_original_owner(&fixture);
    assert!(!run.stop.is_cancelled());
    assert_eq!(queue.buffered().unwrap(), (0, 0));
    fixture
        .provider
        .inner
        .recheck_dispatch(&run.context)
        .unwrap();
}

#[tokio::test]
async fn reconnect_queue_failure_preserves_owner_and_authorization() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(1);
    let queue = Arc::new(ByteQueue::new(1, wire::MAX_MESSAGE_BYTES, 1000).unwrap());
    queue.put_nowait(b"occupied").unwrap();
    let request = candidate(&fixture, queue.clone());
    let error = fixture.provider.inner.attach(2, request).unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert_original_owner(&fixture);
    assert_eq!(queue.buffered().unwrap(), (1, 8));
}

#[tokio::test]
async fn unrepresentable_reconnect_preserves_owner_and_authorization() {
    let mut fixture = Fixture::new(700).await;
    fixture.voucher(1);
    Arc::get_mut(&mut fixture.provider.inner)
        .unwrap()
        .limits
        .wire = WireLimits::new(
        128,
        128,
        128,
        crate::DEFAULT_JSON_DEPTH,
        wire::MAX_PENDING_RUNS,
        wire::MAX_ERROR_BYTES,
    )
    .unwrap();
    let queue = Arc::new(ByteQueue::default());
    let request = candidate(&fixture, queue.clone());
    let error = fixture.provider.inner.attach(2, request).unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert_original_owner(&fixture);
    assert_eq!(queue.buffered().unwrap(), (0, 0));
}

#[tokio::test]
async fn invalid_reconnect_voucher_preserves_owner_and_authorization() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(1);
    for invalid in [false, true] {
        let mut request = candidate(&fixture, Arc::new(ByteQueue::default()));
        if invalid {
            request
                .latest_voucher
                .as_mut()
                .unwrap()
                .signature
                .as_mut()
                .unwrap()[0] ^= 1;
        } else {
            request.latest_voucher = Some(voucher(&fixture, 101));
        }
        assert!(fixture.provider.inner.attach(2, request).is_err());
        assert_original_owner(&fixture);
    }
}

#[tokio::test]
async fn successful_reconnect_commits_staged_snapshot_once() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(1);
    let queue = Arc::new(ByteQueue::default());
    let request = candidate(&fixture, queue.clone());
    fixture.provider.inner.attach(2, request.clone()).unwrap();
    {
        let state = fixture.provider.inner.lock().unwrap();
        let session = &state.sessions[&fixture.sid];
        assert_eq!(session.generation, 2);
        assert_eq!(session.owner, Some(2));
        assert_eq!(session.voucher, request.latest_voucher);
        assert!(!state.attachments[&1].live);
        assert_eq!(state.attachments[&2].request.as_ref(), Some(&request));
    }
    let first = proto::ControlMessage::decode(queue.get().await.unwrap().as_slice()).unwrap();
    let Some(proto::control_message::Kind::SessionState(snapshot)) = &first.kind else {
        panic!("snapshot")
    };
    assert_eq!(snapshot.control_generation, Some(2));
    assert_eq!(snapshot.accepted_voucher, request.latest_voucher);
    assert_eq!(snapshot.ready, Some(true));
    fixture.provider.inner.attach(2, request).unwrap();
    let repeated = proto::ControlMessage::decode(queue.get().await.unwrap().as_slice()).unwrap();
    assert_eq!(repeated, first);
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].generation,
        2
    );
}

#[tokio::test]
async fn failed_initial_challenge_retains_no_attachment_or_identifier() {
    let mut fixture = Fixture::new(700).await;
    {
        let runtime = Arc::get_mut(&mut fixture.provider.inner).unwrap();
        runtime.limits = RuntimeLimits::new(
            1,
            1,
            WireLimits::new(
                wire::MAX_MESSAGE_BYTES,
                1,
                crate::DEFAULT_CATALOG_BYTES,
                crate::DEFAULT_JSON_DEPTH,
                wire::MAX_PENDING_RUNS,
                wire::MAX_ERROR_BYTES,
            )
            .unwrap(),
        )
        .unwrap();
        runtime.lock().unwrap().attachments.clear();
    }
    let mut service = proto::control_server::ControlServer::new(fixture.provider.clone());
    for _ in 0..3 {
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri("/mppi.v1.Control/Session")
            .header("content-type", "application/grpc")
            .body(Body::empty())
            .unwrap();
        let response = service.call(request).await.unwrap();
        let error = Status::from_header_map(response.headers()).unwrap();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        let state = fixture.provider.inner.lock().unwrap();
        assert!(state.attachments.is_empty());
        assert_eq!(state.next_attachment, 0);
    }
}

struct CountedExecution {
    calls: Arc<AtomicU64>,
}
struct PendingOutput;
impl futures_core::Stream for PendingOutput {
    type Item = IntegrationResult<Vec<u8>>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Pending
    }
}
#[tonic::async_trait]
impl CoverageProvider for CountedExecution {
    async fn coverage(
        &self,
        _: &PreparedRequest,
        _: &RunContext,
    ) -> IntegrationResult<TokenCounts> {
        Ok(counts(1000))
    }
}
impl ExecutionProvider for CountedExecution {
    fn execute(
        &self,
        _: PreparedRequest,
        _: RunContext,
        _: Arc<dyn ProviderRuns>,
        _: CancellationToken,
    ) -> ExecutionStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(PendingOutput)
    }
}

#[tokio::test(start_paused = true)]
async fn sufficient_voucher_after_authorization_deadline_never_dispatches() {
    use proto::chat_server::Chat;
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };
    let mut fixture = Fixture::new(700).await;
    let calls = Arc::new(AtomicU64::new(0));
    let integration = Arc::new(CountedExecution {
        calls: calls.clone(),
    });
    {
        let runtime = Arc::get_mut(&mut fixture.provider.inner).unwrap();
        runtime.coverage = integration.clone();
        runtime.execution = integration;
    }
    let future = fixture
        .provider
        .completions(tonic::Request::new(fixture.request(1)));
    tokio::pin!(future);
    // Drive the real service through its first insufficient-authority result.
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let queue = fixture.provider.inner.lock().unwrap().attachments[&1]
        .queue
        .clone();
    let guidance = proto::ControlMessage::decode(queue.get().await.unwrap().as_slice()).unwrap();
    assert!(matches!(
        guidance.kind,
        Some(proto::control_message::Kind::LowCredit(_))
    ));
    // Tokio 1.53.1 time/clock.rs:200: advance changes Instant::now while
    // leaving this unspawned service future unpolled until the voucher is set.
    tokio::time::advance(Duration::from_millis(10_001)).await;
    fixture.voucher(1);
    fixture.provider.inner.changed.notify_waiters();
    let error = match future.await {
        Err(error) => error,
        Ok(_) => panic!("expired authorization wait granted paid execution"),
    };
    assert_eq!(reason(&error).as_deref(), Some("AUTHORIZATION_REQUIRED"));
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let state = fixture.provider.inner.lock().unwrap();
    let run = &state.sessions[&fixture.sid].runs[&hash(1)];
    assert!(matches!(run.phase, RunPhase::NotAdmitted));
    assert_eq!(run.accounting.remaining_numerator(), U256::ZERO);
    assert_eq!(run.accounting.counts(), zero_counts());
    assert_eq!(state.retained_request_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn atomic_admission_rejects_expired_authorization_before_reserving() {
    let fixture = Fixture::new(700).await;
    let run = fixture.begin(1);
    assert!(matches!(
        fixture
            .provider
            .inner
            .attempt_admission_before(&run, counts(1000), None)
            .unwrap(),
        Admission::Wait(_)
    ));
    let deadline = Instant::now() + Duration::from_millis(10_000);
    tokio::time::advance(Duration::from_millis(10_001)).await;
    fixture.voucher(1);
    let error = fixture
        .provider
        .inner
        .attempt_admission_before(&run, counts(1000), Some(deadline))
        .err()
        .unwrap();
    assert_eq!(reason(&error).as_deref(), Some("AUTHORIZATION_REQUIRED"));
    let state = fixture.provider.inner.lock().unwrap();
    let stored = &state.sessions[&fixture.sid].runs[&hash(1)];
    assert!(matches!(stored.phase, RunPhase::Preparing));
    assert_eq!(stored.accounting.remaining_numerator(), U256::ZERO);
    assert!(!run.stop.is_cancelled());
}
