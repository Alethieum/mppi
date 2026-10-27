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
    {
        let state = fixture.provider.inner.lock().unwrap();
        fixture.provider.inner.wake_locked(&state, fixture.sid);
    }
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
        Admission::Authorizing
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

/// The first run needs this many output tokens; every later one needs 1,000.
struct SteadyLoad(u64);
#[tonic::async_trait]
impl CoverageProvider for SteadyLoad {
    async fn coverage(
        &self,
        _: &PreparedRequest,
        context: &RunContext,
    ) -> IntegrationResult<TokenCounts> {
        Ok(counts(if context.run_id() == hash(1) {
            self.0
        } else {
            1000
        }))
    }
}
impl ExecutionProvider for SteadyLoad {
    fn execute(
        &self,
        _: PreparedRequest,
        _: RunContext,
        _: Arc<dyn ProviderRuns>,
        _: CancellationToken,
    ) -> ExecutionStream {
        Box::pin(PendingOutput) // Admitted runs keep streaming and reserving.
    }
}

#[tokio::test(start_paused = true)]
async fn concurrent_runs_are_admitted_in_the_order_they_first_waited_under_steady_load() {
    use proto::chat_server::Chat;
    // The agent answers each coverage request one round trip later, signing
    // exactly its target, while a new run arrives every round trip.
    const RTT: Duration = Duration::from_millis(100);
    let mut fixture = Fixture::new(1000).await; // 1,000 output tokens cost one unit.
    {
        let runtime = Arc::get_mut(&mut fixture.provider.inner).unwrap();
        runtime.coverage = Arc::new(SteadyLoad(3000));
        runtime.execution = Arc::new(SteadyLoad(3000));
    }
    fixture.voucher(1);
    let sid = fixture.sid;
    let queue = fixture.provider.inner.lock().unwrap().attachments[&1]
        .queue
        .clone();
    let asked = Arc::new(Mutex::new(BTreeMap::<Vec<u8>, usize>::new()));
    let agent = {
        let (provider, asked) = (fixture.provider.clone(), asked.clone());
        tokio::spawn(async move {
            while let Ok(bytes) = queue.get().await {
                let message = proto::ControlMessage::decode(bytes.as_slice()).unwrap();
                let Some(proto::control_message::Kind::LowCredit(notice)) = message.kind else {
                    continue;
                };
                let request = notice.coverage_request.unwrap();
                let tokens = request
                    .required_by_price
                    .iter()
                    .map(|row| row.output_tokens.unwrap())
                    .sum::<u64>();
                *asked
                    .lock()
                    .unwrap()
                    .entry(request.run_id.unwrap())
                    .or_default() += 1;
                let provider = provider.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(RTT).await;
                    let target = tokens.div_ceil(1000);
                    {
                        let mut state = provider.inner.lock().unwrap();
                        let session = state.sessions.get_mut(&sid).unwrap();
                        if session.voucher.as_ref().and_then(|v| v.cumulative) < Some(target) {
                            session.voucher = Some(proto::Voucher {
                                session_id: Some(sid.to_vec()),
                                cumulative: Some(target),
                                signature: Some(sign(&Voucher {
                                    sessionId: sid,
                                    cumulativeAmount: target,
                                })),
                            });
                        }
                        provider.inner.wake_locked(&state, sid);
                    }
                });
            }
        })
    };
    let start = Instant::now();
    let calls = (1..=21u8)
        .map(|id| {
            let arrival = match id {
                1 => start,
                _ => start + Duration::from_millis(10) + RTT * u32::from(id - 2),
            };
            let (provider, request) = (fixture.provider.clone(), fixture.request(id));
            tokio::spawn(async move {
                tokio::time::sleep_until(arrival).await;
                let response = provider.completions(tonic::Request::new(request)).await;
                (arrival, Instant::now(), response)
            })
        })
        .collect::<Vec<_>>();
    let mut admitted = Vec::new();
    let mut streams = Vec::new(); // Dropping one would cancel its run.
    for call in calls {
        let (arrival, at, response) = call.await.unwrap();
        assert!(
            response.is_ok(),
            "a run was refused after {:?}",
            at - arrival
        );
        admitted.push((at - arrival, at));
        streams.push(response);
    }
    agent.abort();
    for (index, (wait, _)) in admitted.iter().enumerate() {
        assert!(*wait <= 2 * RTT, "run {} waited {wait:?}", index + 1);
    }
    assert!(admitted.windows(2).all(|pair| pair[0].1 < pair[1].1));
    // The first run's target never moved, so it was asked for only once.
    assert_eq!(asked.lock().unwrap()[hash(1).as_slice()], 1);
}

#[tokio::test(start_paused = true)]
async fn a_covered_run_behind_a_silent_head_is_admitted_within_its_turn() {
    use proto::chat_server::Chat;
    // Nobody answers the first run's coverage request, as when an agent will
    // not sign it. Later runs the voucher covers must not wait on it for long,
    // and must never fail for it.
    let mut fixture = Fixture::new(1000).await; // 1,000 output tokens cost one unit.
    {
        let runtime = Arc::get_mut(&mut fixture.provider.inner).unwrap();
        runtime.coverage = Arc::new(SteadyLoad(9000));
        runtime.execution = Arc::new(SteadyLoad(9000));
    }
    fixture.voucher(5);
    let start = Instant::now();
    let calls = [(1u8, 0u64), (2, 0), (3, 1), (4, 1000), (5, 5000)].map(|(id, delay)| {
        let (provider, request) = (fixture.provider.clone(), fixture.request(id));
        let arrival = start + Duration::from_millis(delay);
        tokio::spawn(async move {
            tokio::time::sleep_until(arrival).await;
            let response = provider.completions(tonic::Request::new(request)).await;
            (arrival, Instant::now(), response)
        })
    });
    let mut results = Vec::new();
    for call in calls {
        results.push(call.await.unwrap());
    }
    let (_, _, head) = &results[0];
    let error = head
        .as_ref()
        .err()
        .expect("the silent head is never covered");
    assert_eq!(reason(error).as_deref(), Some("AUTHORIZATION_REQUIRED"));
    for (index, (arrival, at, response)) in results.iter().enumerate().skip(1) {
        assert!(response.is_ok(), "covered run {} failed", index + 1);
        let waited = *at - *arrival;
        assert!(
            waited <= Duration::from_millis(2050),
            "run {} waited {waited:?}",
            index + 1
        );
    }
}

#[tokio::test(start_paused = true)]
async fn control_loss_ends_every_run_waiting_in_the_queue_at_once() {
    use proto::chat_server::Chat;
    let mut fixture = Fixture::new(1000).await; // 1,000 output tokens cost one unit.
    {
        let runtime = Arc::get_mut(&mut fixture.provider.inner).unwrap();
        runtime.coverage = Arc::new(SteadyLoad(9000));
        runtime.execution = Arc::new(SteadyLoad(9000));
    }
    fixture.voucher(5);
    let calls = [1u8, 2].map(|id| {
        let (provider, request) = (fixture.provider.clone(), fixture.request(id));
        tokio::spawn(async move { provider.completions(tonic::Request::new(request)).await })
    });
    tokio::time::sleep(Duration::from_millis(100)).await; // Head waits, the next defers.
    let lost = Instant::now();
    {
        let mut state = fixture.provider.inner.lock().unwrap();
        fixture.provider.inner.detach_locked(&mut state, 1);
    }
    for call in calls {
        let error = call
            .await
            .unwrap()
            .err()
            .expect("no run is admitted after Control loss");
        assert_eq!(reason(&error).as_deref(), Some("CONTROL_NOT_READY"));
    }
    assert!(Instant::now() - lost <= Duration::from_millis(100));
    let state = fixture.provider.inner.lock().unwrap();
    assert!(
        state.sessions[&fixture.sid]
            .runs
            .values()
            .all(|run| run.waiting().is_none())
    );
}

/// Asks for more coverage than the deposit holds, or none, then fails with a
/// host error of its own, as a host whose renewal was refused does.
struct RefusedRenewal(bool);
struct Failing(
    Option<
        std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::interfaces::IntegrationError> + Send>,
        >,
    >,
);
impl futures_core::Stream for Failing {
    type Item = IntegrationResult<Vec<u8>>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let Some(failure) = self.0.as_mut() else {
            return std::task::Poll::Ready(None);
        };
        let error = std::task::ready!(failure.as_mut().poll(cx));
        self.0 = None;
        std::task::Poll::Ready(Some(Err(error)))
    }
}
#[tonic::async_trait]
impl CoverageProvider for RefusedRenewal {
    async fn coverage(
        &self,
        _: &PreparedRequest,
        _: &RunContext,
    ) -> IntegrationResult<TokenCounts> {
        Ok(counts(1000))
    }
}
impl ExecutionProvider for RefusedRenewal {
    fn execute(
        &self,
        _: PreparedRequest,
        context: RunContext,
        runs: Arc<dyn ProviderRuns>,
        _: CancellationToken,
    ) -> ExecutionStream {
        let renew = self.0;
        Box::pin(Failing(Some(Box::pin(async move {
            if renew {
                // 1,000,000 output tokens at 700 per million is 700 > the 100 deposit.
                let renewal = runs.require_coverage(&context, counts(1_000_000)).await;
                assert!(!renewal.unwrap().granted());
            }
            Box::new(std::io::Error::other("host stream failed"))
                as crate::interfaces::IntegrationError
        }))))
    }
}

#[tokio::test]
async fn a_host_error_after_a_refused_renewal_ends_the_run_with_that_refusal() {
    use futures_core::Stream;
    use proto::chat_server::Chat;
    use std::future::poll_fn;
    for (renew, expected) in [
        (true, PaymentCode::InsufficientDeposit),
        (false, PaymentCode::SessionNotReady),
    ] {
        let mut fixture = Fixture::new(700).await;
        {
            let runtime = Arc::get_mut(&mut fixture.provider.inner).unwrap();
            runtime.coverage = Arc::new(RefusedRenewal(renew));
            runtime.execution = Arc::new(RefusedRenewal(renew));
        }
        fixture.voucher(1);
        let mut output = fixture
            .provider
            .completions(tonic::Request::new(fixture.request(1)))
            .await
            .unwrap()
            .into_inner();
        let status = poll_fn(|cx| std::pin::Pin::new(&mut output).poll_next(cx))
            .await
            .unwrap()
            .unwrap_err();
        let failure = wire::parse_payment_error(&status).unwrap().unwrap();
        assert_eq!(failure.code(), expected);
        if !renew {
            // An arbitrary host error stays sanitized.
            assert_eq!(failure.reason(), Some("STATE_UNAVAILABLE"));
        }
        assert_eq!(failure.run_id(), Some(hash(1)));
    }
}
