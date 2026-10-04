//! Untrusted Control clients cannot retain SDK attachments without a deadline.
use super::*;
use crate::interfaces::ProviderLifecycle;
use crate::signing::ReconnectAuth;
use crate::types::OperationState;
use http_body::Body as _;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc;
use tonic::{
    body::Body,
    codegen::{Bytes, Service, http},
};

struct OpenBody(mpsc::Receiver<Bytes>);
impl http_body::Body for OpenBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        self.0
            .poll_recv(cx)
            .map(|frame| frame.map(|bytes| Ok(http_body::Frame::data(bytes))))
    }
}

async fn open(
    fixture: &Fixture,
    timeout: Option<&str>,
) -> (mpsc::Sender<Bytes>, http::Response<Body>) {
    let (send, receive) = mpsc::channel(4);
    let mut request = http::Request::builder()
        .method("POST")
        .uri("/mppi.v1.Control/Session")
        .header("content-type", "application/grpc");
    if let Some(timeout) = timeout {
        request = request.header("grpc-timeout", timeout);
    }
    let (_, mut service) = fixture.provider.grpc_services();
    let response = service
        .call(request.body(Body::new(OpenBody(receive))).unwrap())
        .await
        .unwrap();
    (send, response)
}

async fn bounded_fixture() -> Fixture {
    let mut fixture = Fixture::new(700).await;
    let runtime = Arc::get_mut(&mut fixture.provider.inner).unwrap();
    runtime.limits.max_control_streams = 2;
    runtime.lock().unwrap().attachments.clear();
    fixture
}

async fn yields() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

async fn send(sender: &mpsc::Sender<Bytes>, kind: proto::control_message::Kind) {
    let bytes = proto::ControlMessage { kind: Some(kind) }.encode_to_vec();
    let mut frame = vec![0];
    frame.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(&bytes);
    sender.send(Bytes::from(frame)).await.unwrap();
}

async fn message(response: &mut http::Response<Body>) -> proto::control_message::Kind {
    let frame = std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    let data = frame.into_data().unwrap();
    assert_eq!(data[0], 0);
    assert_eq!(
        u32::from_be_bytes(data[1..5].try_into().unwrap()) as usize,
        data.len() - 5
    );
    proto::ControlMessage::decode(&data[5..])
        .unwrap()
        .kind
        .unwrap()
}

async fn nonce(response: &mut http::Response<Body>) -> B256 {
    let proto::control_message::Kind::ReconnectChallenge(value) = message(response).await else {
        panic!("missing initial nonce");
    };
    B256::from_slice(value.nonce.as_deref().unwrap())
}

fn proof(fixture: &Fixture, nonce: B256) -> proto::control_message::Kind {
    proto::control_message::Kind::ReconnectRequest(proto::ReconnectRequest {
        session_id: Some(fixture.sid.to_vec()),
        nonce: Some(nonce.to_vec()),
        signature: Some(sign(&ReconnectAuth {
            sessionId: fixture.sid,
            nonce,
        })),
        latest_voucher: None,
        last_meter_seq: Some(0),
        pending_run_ids: vec![],
    })
}

fn challenge(id: u8) -> proto::control_message::Kind {
    proto::control_message::Kind::ChallengeRequest(proto::ChallengeRequest {
        request_id: Some(hash(id).to_vec()),
        model: Some("example/model".into()),
    })
}

async fn stop(fixture: &Fixture) {
    fixture
        .provider
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn r7_hooks_confirmed_opening_and_denied_coverage_deliver_owned_payloads() {
    let mut fixture = Fixture::new(1000).await;
    let (opened, opening_events) = std::sync::mpsc::channel();
    let (drained, drain_events) = std::sync::mpsc::channel();
    let payment_events = Arc::new(AtomicU64::new(0));
    let settled = payment_events.clone();
    let closed = payment_events.clone();
    Arc::get_mut(&mut fixture.provider.inner).unwrap().observers = hooks::Observers::new(Hooks {
        on_session_open: Some(Arc::new(move |notice| opened.send(notice).unwrap())),
        on_drain: Some(Arc::new(move |notice| drained.send(notice).unwrap())),
        on_settle: Some(Arc::new(move |_| {
            settled.fetch_add(1, Ordering::SeqCst);
        })),
        on_close: Some(Arc::new(move |_| {
            closed.fetch_add(1, Ordering::SeqCst);
        })),
        ..Hooks::default()
    });
    fixture.provider.inner.lock().unwrap().sessions.clear();
    fixture.provider.inner.lock().unwrap().attachments.clear();
    let (sender, mut response) = open(&fixture, None).await;
    let nonce = nonce(&mut response).await;
    let notice = proto::SessionOpen {
        session_id: Some(fixture.sid.to_vec()),
        open_tx: Some(hash(0xaa).to_vec()),
        agent_id: None,
    };
    send(
        &sender,
        proto::control_message::Kind::SessionOpen(notice.clone()),
    )
    .await;
    send(&sender, challenge(12)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::Challenge(_)
    ));
    let mut observed = opening_events.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(observed, notice);
    // Changing the callback's owned payload cannot alter the retained opening.
    observed.session_id = Some(hash(0xff).to_vec());
    {
        let state = fixture.provider.inner.lock().unwrap();
        let session = &state.sessions[&fixture.sid];
        assert_eq!(session.opening.tx_hash(), hash(0xaa));
        assert_eq!(session.opening.block().observation().block_number(), 100);
        assert_eq!(session.generation, 0);
        assert!(
            session.owner.is_none(),
            "opening notice is not attachment authority"
        );
    }
    send(&sender, proof(&fixture, nonce)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::SessionState(_)
    ));
    fixture.voucher(1);
    let run = fixture.admit(1);
    let denied = fixture
        .provider
        .runs()
        .require_coverage(&run.context, counts(2000))
        .await
        .unwrap();
    assert!(!denied.granted());
    let proto::control_message::Kind::Drain(wire_drain) = message(&mut response).await else {
        panic!("coverage denial did not deliver Drain");
    };
    let callback_drain = drain_events.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(callback_drain, wire_drain);
    assert_eq!(callback_drain.session_id, Some(fixture.sid.to_vec()));
    assert_eq!(callback_drain.run_id, Some(run.context.run_id().to_vec()));
    assert_eq!(
        callback_drain.reason,
        Some(proto::DrainReason::AuthorizationExhausted as i32)
    );
    assert!(run.stop.is_cancelled());
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)]
            .accounting
            .remaining_numerator(),
        U256::from(1_000_000u64)
    );
    fixture
        .provider
        .runs()
        .report_terminal(&run.context, counts(0), RunStatus::Cancelled)
        .await
        .unwrap();
    let shutdown = fixture
        .provider
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(shutdown.unresolved_runs.is_empty());
    assert!(shutdown.pending_operations.is_empty());
    assert_eq!(payment_events.load(Ordering::SeqCst), 0);
    // The fixture wallet rejects any custody call. Canonical state remains open
    // and funded after both an execution drain and resource shutdown.
    let block = fixture.provider.inner.reader.block(None).await.unwrap();
    let chain_session = fixture
        .provider
        .inner
        .reader
        .session(fixture.sid, &block)
        .await
        .unwrap();
    assert_eq!(chain_session.status(), SessionStatus::Open);
    assert_eq!(chain_session.deposit(), 100);
    assert_eq!(chain_session.settled_cumulative(), 0);
    drop((sender, response));
}

#[tokio::test(start_paused = true)]
async fn silent_unauthenticated_control_releases_budget_without_client_deadline() {
    let fixture = bounded_fixture().await;
    let first = open(&fixture, None).await;
    let second = open(&fixture, None).await;
    let blocked = open(&fixture, None).await;
    assert_eq!(
        Status::from_header_map(blocked.1.headers()).unwrap().code(),
        tonic::Code::ResourceExhausted
    );
    // Keep both request senders and response bodies alive and unread. Cleanup
    // must not depend on the attacker polling a response or closing its stream.
    tokio::time::advance(Duration::from_secs(61)).await;
    yields().await;
    assert!(
        fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .attachments
            .is_empty(),
        "unauthenticated clients retained the entire attachment budget past the server setup limit"
    );
    let next = open(&fixture, None).await;
    assert!(Status::from_header_map(next.1.headers()).is_none());
    drop((first, second, blocked, next));
    stop(&fixture).await;
}

#[tokio::test(start_paused = true)]
async fn huge_client_timeout_cannot_extend_server_authentication_limit() {
    let fixture = bounded_fixture().await;
    let client = open(&fixture, Some("99999999H")).await;
    assert!(Status::from_header_map(client.1.headers()).is_none());
    tokio::time::advance(Duration::from_secs(61)).await;
    yields().await;
    assert!(
        fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .attachments
            .is_empty()
    );
    drop(client);
    stop(&fixture).await;
}

#[tokio::test(start_paused = true)]
async fn malformed_and_expired_control_deadlines_retain_no_attachment() {
    let fixture = bounded_fixture().await;
    for (timeout, code) in [
        ("forever", tonic::Code::InvalidArgument),
        ("999999999S", tonic::Code::InvalidArgument),
        ("0S", tonic::Code::DeadlineExceeded),
    ] {
        let client = open(&fixture, Some(timeout)).await;
        assert_eq!(
            Status::from_header_map(client.1.headers()).unwrap().code(),
            code
        );
        assert!(
            fixture
                .provider
                .inner
                .lock()
                .unwrap()
                .attachments
                .is_empty()
        );
    }
    let client = open(&fixture, Some("5m")).await;
    tokio::time::advance(Duration::from_millis(6)).await;
    yields().await;
    assert!(
        fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .attachments
            .is_empty()
    );
    drop(client);
    stop(&fixture).await;
}

#[tokio::test(start_paused = true)]
async fn signed_attachment_remains_live_after_setup_limit() {
    let fixture = bounded_fixture().await;
    let (sender, mut response) = open(&fixture, None).await;
    let nonce = nonce(&mut response).await;
    send(&sender, proof(&fixture, nonce)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::SessionState(_)
    ));
    tokio::time::advance(Duration::from_secs(121)).await;
    yields().await;
    assert!(fixture.provider.inner.lock().unwrap().attachments[&1].live);
    send(&sender, challenge(1)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::Challenge(_)
    ));
    drop((sender, response));
    stop(&fixture).await;
}

#[tokio::test(start_paused = true)]
async fn challenges_and_session_notices_never_authenticate_or_reset_setup_limit() {
    let fixture = bounded_fixture().await;
    let (sender, mut response) = open(&fixture, None).await;
    nonce(&mut response).await;
    tokio::time::advance(Duration::from_secs(30)).await;
    send(&sender, challenge(1)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::Challenge(_)
    ));
    send(
        &sender,
        proto::control_message::Kind::SessionOpen(proto::SessionOpen {
            session_id: Some(fixture.sid.to_vec()),
            open_tx: Some(hash(0xaa).to_vec()),
            agent_id: None,
        }),
    )
    .await;
    yields().await;
    assert_eq!(
        fixture.provider.inner.lock().unwrap().attachments[&1].sid,
        Some(fixture.sid)
    );
    tokio::time::advance(Duration::from_secs(28)).await;
    send(&sender, challenge(2)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::Challenge(_)
    ));
    tokio::time::advance(Duration::from_secs(3)).await;
    yields().await;
    assert!(
        fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .attachments
            .is_empty()
    );
    drop((sender, response));
    stop(&fixture).await;
}

#[tokio::test(start_paused = true)]
async fn pending_opening_work_is_cancelled_at_setup_limit() {
    let fixture = bounded_fixture().await;
    let held = fixture.provider.inner.openings.lock().await;
    let (sender, response) = open(&fixture, None).await;
    send(
        &sender,
        proto::control_message::Kind::SessionOpen(proto::SessionOpen {
            session_id: Some(fixture.sid.to_vec()),
            open_tx: Some(hash(0xaa).to_vec()),
            agent_id: None,
        }),
    )
    .await;
    yields().await;
    tokio::time::advance(Duration::from_secs(61)).await;
    yields().await;
    assert!(
        fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .attachments
            .is_empty()
    );
    drop((sender, response, held));
    stop(&fixture).await;
}

#[tokio::test(start_paused = true)]
async fn ready_late_proof_cannot_acquire_ownership() {
    let fixture = bounded_fixture().await;
    let (sender, mut response) = open(&fixture, None).await;
    let nonce = nonce(&mut response).await;
    send(&sender, proof(&fixture, nonce)).await;
    // The queued proof is ready when the timer is next polled. The explicit
    // monotonic check must win even though Tokio polls the inner future first.
    tokio::time::advance(Duration::from_secs(60)).await;
    yields().await;
    {
        let state = fixture.provider.inner.lock().unwrap();
        assert!(state.attachments.is_empty());
        assert_eq!(state.sessions[&fixture.sid].generation, 1);
    }
    drop((sender, response));
    stop(&fixture).await;
}

#[test]
fn setup_policy_is_finite_and_independent_of_complete_authorization() {
    assert_eq!(
        RuntimeLimits::default().control_auth_timeout(),
        Duration::from_secs(60)
    );
    assert!(
        RuntimeLimits::default()
            .with_control_auth_timeout(Duration::ZERO)
            .is_err()
    );
    assert!(
        RuntimeLimits::default()
            .with_control_auth_timeout(Duration::from_secs(3601))
            .is_err()
    );
    let policy = RuntimeLimits::default()
        .with_control_auth_timeout(Duration::from_millis(125))
        .unwrap();
    assert_eq!(policy.control_auth_timeout(), Duration::from_millis(125));
}

#[tokio::test]
async fn authenticated_topup_notice_cannot_grant_from_unconfirmed_funding() {
    let fixture = Fixture::new(100_000).await;
    fixture.provider.inner.lock().unwrap().attachments.clear();
    fixture.voucher(100);
    let (sender, mut response) = open(&fixture, None).await;
    let nonce = nonce(&mut response).await;
    send(&sender, proof(&fixture, nonce)).await;
    let proto::control_message::Kind::SessionState(snapshot) = message(&mut response).await else {
        panic!("missing authenticated state");
    };
    let generation = snapshot.control_generation.unwrap();
    let auth = crate::signing::RunAuth {
        sessionId: fixture.sid,
        controlGeneration: generation,
        runId: hash(1),
        model: "example/model".into(),
        rateCardHash: fixture.rpc.initial_hash,
    };
    let mut request = fixture.request(1);
    request.control_generation = Some(generation);
    request.auth_signature = Some(sign(&auth));
    let run = fixture.provider.inner.begin_run(request).unwrap();
    assert!(matches!(
        fixture
            .provider
            .inner
            .attempt_admission(&run, counts(1000))
            .unwrap(),
        Admission::Granted
    ));
    send(
        &sender,
        proto::control_message::Kind::TopUpNotice(proto::TopUpNotice {
            session_id: Some(fixture.sid.to_vec()),
            tx: Some(hash(0xdd).to_vec()),
            model: None,
            rate_card_hash: None,
            tokens: None,
        }),
    )
    .await;
    // A subsequent challenge is a Control-processing barrier, not a funding ACK.
    send(&sender, challenge(7)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::Challenge(_)
    ));
    assert_eq!(
        fixture.provider.inner.chain.snapshot().unwrap().sessions()[&fixture.sid].deposit(),
        100
    );
    let denied = fixture
        .provider
        .inner
        .require_coverage_sync(&run.context, counts(1001))
        .unwrap();
    assert!(!denied.granted());
    assert_eq!(
        denied.failure().unwrap().code(),
        PaymentCode::InsufficientDeposit
    );
    assert!(run.stop.is_cancelled());
    {
        let state = fixture.provider.inner.lock().unwrap();
        let session = &state.sessions[&fixture.sid];
        assert_eq!(session.voucher.as_ref().unwrap().cumulative, Some(100));
        assert_eq!(
            session.runs[&hash(1)].accounting.remaining_numerator(),
            U256::from(100_000_000u64)
        );
    }
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(0), Some(RunStatus::Cancelled))
        .unwrap();
    drop((sender, response));
    stop(&fixture).await;
}

fn voucher_message(fixture: &Fixture, cumulative: u64) -> proto::control_message::Kind {
    proto::control_message::Kind::Voucher(proto::Voucher {
        session_id: Some(fixture.sid.to_vec()),
        cumulative: Some(cumulative),
        signature: Some(sign(&Voucher {
            sessionId: fixture.sid,
            cumulativeAmount: cumulative,
        })),
    })
}

async fn authenticated(fixture: &Fixture) -> (mpsc::Sender<Bytes>, http::Response<Body>) {
    fixture.provider.inner.lock().unwrap().attachments.clear();
    let (sender, mut response) = open(fixture, None).await;
    let nonce = nonce(&mut response).await;
    send(&sender, proof(fixture, nonce)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::SessionState(_)
    ));
    (sender, response)
}

fn retained_close(fixture: &Fixture) -> proto::CloseResult {
    fixture.voucher(1);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(1000), Some(RunStatus::Completed))
        .unwrap();
    let signature = sign(&Voucher {
        sessionId: fixture.sid,
        cumulativeAmount: 1,
    });
    let prepared = fixture
        .provider
        .inner
        .contract
        .close(&descriptor(), 1, 1, &signature)
        .unwrap();
    let result = OperationResult::new(
        "close:original-confirmed",
        OperationState::Confirmed,
        Some("original-wallet-operation".into()),
        Some(hash(0xdd)),
        Some(
            *fixture
                .provider
                .inner
                .chain
                .snapshot()
                .unwrap()
                .block()
                .observation(),
        ),
        vec![],
    )
    .unwrap();
    let message = proto::CloseResult {
        session_id: Some(fixture.sid.to_vec()),
        tx_hash: Some(hash(0xdd).to_vec()),
        capture_amount: Some(1),
        refunded: Some(99),
    };
    // Supply the already-confirmed outcome at the delivery boundary. Canonical
    // receipt validation has separate chain tests; the fixture wallet panics
    // on any execute or reconcile, proving replay adds no custody calls.
    let mut state = fixture.provider.inner.lock().unwrap();
    let session = state.sessions.get_mut(&fixture.sid).unwrap();
    session.close_pending = true;
    session.close_reference = Some(result.operation_ref().into());
    session.operation = Some(PendingOperation {
        prepared: Arc::new(prepared),
        result: Arc::new(result),
        units: Arc::new(BTreeMap::from([(fixture.sid, 1000)])),
    });
    session.close_result = Some(message.clone());
    message
}

fn close_request(fixture: &Fixture) -> proto::control_message::Kind {
    proto::control_message::Kind::CloseRequest(proto::CloseRequest {
        session_id: Some(fixture.sid.to_vec()),
        signature: Some(sign(&crate::signing::CloseAuth {
            sessionId: fixture.sid,
        })),
    })
}

#[tokio::test]
async fn retained_close_result_replays_after_authenticated_reconnect_without_new_custody() {
    let mut fixture = Fixture::new(1000).await;
    let (events, received) = std::sync::mpsc::channel();
    let closed = events.clone();
    Arc::get_mut(&mut fixture.provider.inner).unwrap().observers = hooks::Observers::new(Hooks {
        on_close: Some(Arc::new(move |_| closed.send("close").unwrap())),
        on_challenge: Some(Arc::new(move |_| events.send("barrier").unwrap())),
        ..Hooks::default()
    });
    let expected = retained_close(&fixture);
    let (sender, response) = authenticated(&fixture).await;
    let (original, generation) = {
        let mut state = fixture.provider.inner.lock().unwrap();
        let session = &state.sessions[&fixture.sid];
        let original = session.operation.clone().unwrap();
        let generation = session.generation;
        let owner = session.owner.unwrap();
        fixture
            .provider
            .inner
            .send_locked(
                &mut state,
                owner,
                control(proto::control_message::Kind::CloseResult(expected.clone())),
            )
            .unwrap();
        (original, generation)
    };
    fixture
        .provider
        .inner
        .observers
        .emit(hooks::Event::Close(expected.clone()));
    assert_eq!(
        received.recv_timeout(Duration::from_secs(1)).unwrap(),
        "close"
    );
    // Drop the original response with its result still queued, then use a real
    // fresh Control request body and fixed-signer reconnect proof.
    drop((sender, response));
    yields().await;
    let (sender, mut response) = open(&fixture, None).await;
    let nonce = nonce(&mut response).await;
    send(&sender, proof(&fixture, nonce)).await;
    let proto::control_message::Kind::SessionState(snapshot) = message(&mut response).await else {
        panic!("missing recovered attachment");
    };
    assert_eq!(snapshot.control_generation, Some(generation + 1));
    assert_eq!(snapshot.close_pending, Some(true));
    assert_eq!(snapshot.ready, Some(false));
    for _ in 0..2 {
        send(&sender, close_request(&fixture)).await;
        let proto::control_message::Kind::CloseResult(replayed) =
            tokio::time::timeout(Duration::from_secs(1), message(&mut response))
                .await
                .unwrap()
        else {
            panic!("authenticated close retry did not return its original CloseResult");
        };
        assert_eq!(replayed.encode_to_vec(), expected.encode_to_vec());
    }
    {
        let state = fixture.provider.inner.lock().unwrap();
        let session = &state.sessions[&fixture.sid];
        let current = session.operation.as_ref().unwrap();
        assert_eq!(
            session.close_reference.as_deref(),
            Some(original.result.operation_ref())
        );
        assert_eq!(current.prepared.call(), original.prepared.call());
        assert!(Arc::ptr_eq(&current.result, &original.result));
        assert!(state.collecting.is_empty());
    }
    // The serial observer barrier proves replay did not emit another on_close.
    send(&sender, challenge(9)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::Challenge(_)
    ));
    assert_eq!(
        received.recv_timeout(Duration::from_secs(1)).unwrap(),
        "barrier"
    );
    assert!(received.try_recv().is_err());
    drop((sender, response));
    stop(&fixture).await;
}

#[tokio::test]
async fn retained_close_result_still_requires_owner_and_valid_close_proof() {
    for invalid_signature in [false, true] {
        let fixture = Fixture::new(1000).await;
        let expected = retained_close(&fixture);
        let (sender, mut response) = if invalid_signature {
            authenticated(&fixture).await
        } else {
            fixture.provider.inner.lock().unwrap().attachments.clear();
            let (sender, mut response) = open(&fixture, None).await;
            nonce(&mut response).await;
            (sender, response)
        };
        let mut request = close_request(&fixture);
        if invalid_signature {
            let proto::control_message::Kind::CloseRequest(value) = &mut request else {
                unreachable!()
            };
            value.signature.as_mut().unwrap()[0] ^= 1;
        }
        send(&sender, request).await;
        let frame = tokio::time::timeout(
            Duration::from_secs(1),
            std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let trailers = frame
            .into_trailers()
            .expect("rejected close emitted a result");
        assert_eq!(
            Status::from_header_map(&trailers).unwrap().code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].close_result,
            Some(expected)
        );
        drop((sender, response));
        stop(&fixture).await;
    }
}

#[tokio::test]
async fn confirmed_topup_voucher_refreshes_lagging_cache_before_acceptance() {
    let fixture = Fixture::new(700).await;
    let (sender, mut response) = authenticated(&fixture).await;
    assert_eq!(
        fixture.provider.inner.chain.snapshot().unwrap().sessions()[&fixture.sid].deposit(),
        100
    );
    fixture.rpc.deposit.store(200, Ordering::SeqCst);
    // No TopUpNotice and no manual refresh: the real Control handler receives
    // the genuine fixed-signer voucher while its latest cache still says 100.
    send(&sender, voucher_message(&fixture, 150)).await;
    yields().await;
    {
        let state = fixture.provider.inner.lock().unwrap();
        assert_eq!(
            state.sessions[&fixture.sid]
                .voucher
                .as_ref()
                .map(|v| v.cumulative),
            Some(Some(150)),
            "confirmed top-up was rejected solely because the follower cache lagged"
        );
    }
    send(&sender, challenge(9)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::Challenge(_)
    ));
    assert_eq!(
        fixture.provider.inner.chain.snapshot().unwrap().sessions()[&fixture.sid].deposit(),
        200
    );
    drop((sender, response));
    stop(&fixture).await;
}

#[tokio::test]
async fn unauthenticated_notice_disconnect_preserves_recovered_zero_generation() {
    use crate::interfaces::ProviderSessions;
    for close_intent in [false, true] {
        let mut fixture = Fixture::new(700).await;
        {
            let mut state = fixture.provider.inner.lock().unwrap();
            let opening = state.sessions[&fixture.sid].opening.clone();
            let mut unused = Session::new(fixture.sid, opening, true);
            if close_intent {
                unused.close_pending = true;
                unused.close_reference = Some("retained-unused-close".into());
            }
            state.sessions.insert(fixture.sid, unused);
        }
        let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
        let original = fixture.provider.clone();
        let source = &original.inner;
        fixture.provider = Mppi::new(
            source.config.clone(),
            source.chain.clone(),
            source.contract.clone(),
            source.wallet.clone(),
            source.coverage.clone(),
            source.execution.clone(),
        )
        .unwrap();
        {
            let mut state = fixture.provider.inner.lock().unwrap();
            state.started = true;
            state.start_block = Some(100);
        }
        fixture.provider.recover(evidence.clone()).await.unwrap();
        let (sender, mut response) = open(&fixture, None).await;
        nonce(&mut response).await;
        send(
            &sender,
            proto::control_message::Kind::SessionOpen(proto::SessionOpen {
                session_id: Some(fixture.sid.to_vec()),
                open_tx: Some(hash(0xaa).to_vec()),
                agent_id: None,
            }),
        )
        .await;
        send(&sender, challenge(8)).await;
        assert!(matches!(
            message(&mut response).await,
            proto::control_message::Kind::Challenge(_)
        ));
        drop(sender);
        yields().await;
        assert!(
            fixture
                .provider
                .inner
                .lock()
                .unwrap()
                .sessions
                .contains_key(&fixture.sid),
            "unauthenticated EOF erased authoritative recovered state"
        );
        let retained = fixture.provider.checkpoint(fixture.sid).await.unwrap();
        assert_eq!(retained.snapshot, evidence.snapshot);
        assert_eq!(retained.close_reference, evidence.close_reference);
        assert_eq!(retained.publications, evidence.publications);
        drop(response);
        let (sender, mut response) = open(&fixture, None).await;
        let nonce = nonce(&mut response).await;
        send(&sender, proof(&fixture, nonce)).await;
        let proto::control_message::Kind::SessionState(state) = message(&mut response).await else {
            panic!("missing legitimate attachment response")
        };
        assert_eq!(state.control_generation, Some(1));
        assert_eq!(state.close_pending, Some(close_intent));
        assert_eq!(state.ready, Some(!close_intent));
        drop((sender, response));
        stop(&fixture).await;
    }
}

#[derive(Default)]
struct RefreshControl {
    armed: std::sync::atomic::AtomicBool,
    hold: std::sync::atomic::AtomicBool,
    fail: std::sync::atomic::AtomicBool,
    reads: std::sync::atomic::AtomicUsize,
    hold_restore: std::sync::atomic::AtomicBool,
    restore_reads: std::sync::atomic::AtomicUsize,
    release: Notify,
}
struct ControlledRefreshRpc {
    base: Arc<TestRpc>,
    control: Arc<RefreshControl>,
}
#[tonic::async_trait]
impl Rpc for ControlledRefreshRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        if method == "eth_getTransactionReceipt"
            && self.control.armed.load(Ordering::SeqCst)
            && self.control.hold_restore.swap(false, Ordering::SeqCst)
        {
            self.control.restore_reads.fetch_add(1, Ordering::SeqCst);
            self.control.release.notified().await;
        }
        if method == "eth_getBlockByNumber"
            && params[0] == "latest"
            && self.control.armed.load(Ordering::SeqCst)
        {
            self.control.reads.fetch_add(1, Ordering::SeqCst);
            if self.control.hold.load(Ordering::SeqCst) {
                self.control.release.notified().await;
            }
            if self.control.fail.load(Ordering::SeqCst) {
                return Err(ChainError::InvalidEvidence("controlled unavailable RPC"));
            }
        }
        self.base.call(method, params).await
    }
}
async fn controlled() -> (Fixture, Arc<RefreshControl>) {
    let control = Arc::new(RefreshControl::default());
    let fixture = Fixture::with_wallet(700, Arc::new(Bindings), {
        let control = control.clone();
        move |base| Arc::new(ControlledRefreshRpc { base, control })
    })
    .await;
    (fixture, control)
}
fn arm(control: &RefreshControl, hold: bool) {
    control.hold.store(hold, Ordering::SeqCst);
    control.armed.store(true, Ordering::SeqCst);
}
fn no_voucher(fixture: &Fixture) {
    assert!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid]
            .voucher
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn covered_and_duplicate_vouchers_use_cache_without_refresh() {
    let (fixture, control) = controlled().await;
    let (sender, mut response) = authenticated(&fixture).await;
    arm(&control, true);
    for cumulative in [100, 100, 90] {
        send(&sender, voucher_message(&fixture, cumulative)).await;
        send(&sender, challenge(cumulative as u8)).await;
        assert!(matches!(
            message(&mut response).await,
            proto::control_message::Kind::Challenge(_)
        ));
    }
    assert_eq!(control.reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid]
            .voucher
            .as_ref()
            .unwrap()
            .cumulative,
        Some(100)
    );
    drop((sender, response));
    stop(&fixture).await;
}

#[tokio::test(start_paused = true)]
async fn forged_or_unowned_above_deposit_voucher_cannot_trigger_refresh() {
    for authenticated_client in [false, true] {
        let (fixture, control) = controlled().await;
        let (sender, response) = if authenticated_client {
            authenticated(&fixture).await
        } else {
            open(&fixture, None).await
        };
        let mut value = voucher_message(&fixture, 150);
        if authenticated_client {
            let proto::control_message::Kind::Voucher(voucher) = &mut value else {
                unreachable!()
            };
            voucher.signature.as_mut().unwrap()[0] ^= 1;
        }
        arm(&control, true);
        send(&sender, value).await;
        yields().await;
        assert_eq!(control.reads.load(Ordering::SeqCst), 0);
        no_voucher(&fixture);
        drop((sender, response));
        stop(&fixture).await;
    }
}

#[tokio::test(start_paused = true)]
async fn unconfirmed_and_failed_refresh_never_authorize_voucher() {
    for fail in [false, true] {
        let (fixture, control) = controlled().await;
        let (sender, response) = authenticated(&fixture).await;
        control.fail.store(fail, Ordering::SeqCst);
        arm(&control, false);
        send(&sender, voucher_message(&fixture, 150)).await;
        yields().await;
        assert_eq!(control.reads.load(Ordering::SeqCst), 1);
        no_voucher(&fixture);
        assert!(
            fixture
                .provider
                .inner
                .lock()
                .unwrap()
                .attachments
                .is_empty()
        );
        drop((sender, response));
        stop(&fixture).await;
    }
}

#[tokio::test(start_paused = true)]
async fn above_deposit_refresh_timeout_is_bounded_and_fails_closed() {
    let (fixture, control) = controlled().await;
    let (sender, response) = authenticated(&fixture).await;
    fixture.rpc.deposit.store(200, Ordering::SeqCst);
    arm(&control, true);
    send(&sender, voucher_message(&fixture, 150)).await;
    yields().await;
    assert_eq!(control.reads.load(Ordering::SeqCst), 1);
    // The original accepted state remains unchanged throughout the RPC wait.
    no_voucher(&fixture);
    tokio::time::advance(Duration::from_millis(1001)).await;
    yields().await;
    no_voucher(&fixture);
    assert!(
        fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .attachments
            .is_empty()
    );
    assert!(fixture.provider.inner.chain.snapshot().is_err());
    drop((sender, response));
    stop(&fixture).await;
}

#[tokio::test(start_paused = true)]
async fn refresh_rechecks_control_ownership_close_and_pause_before_acceptance() {
    for change in ["owner", "close", "pause"] {
        let (fixture, control) = controlled().await;
        let (sender, response) = authenticated(&fixture).await;
        fixture.rpc.deposit.store(200, Ordering::SeqCst);
        arm(&control, true);
        send(&sender, voucher_message(&fixture, 150)).await;
        yields().await;
        assert_eq!(control.reads.load(Ordering::SeqCst), 1);
        no_voucher(&fixture);
        let mut replacement = None;
        match change {
            "owner" => {
                let (new_sender, mut new_response) = open(&fixture, None).await;
                let nonce = nonce(&mut new_response).await;
                send(&new_sender, proof(&fixture, nonce)).await;
                assert!(matches!(
                    message(&mut new_response).await,
                    proto::control_message::Kind::SessionState(_)
                ));
                replacement = Some((new_sender, new_response));
            }
            "close" => {
                let mut state = fixture.provider.inner.lock().unwrap();
                let session = state.sessions.get_mut(&fixture.sid).unwrap();
                session.close_pending = true;
                session.close_reference = Some("concurrent-close".into());
            }
            "pause" => fixture.rpc.pause.store(1, Ordering::SeqCst),
            _ => unreachable!(),
        }
        control.release.notify_one();
        yields().await;
        no_voucher(&fixture);
        assert_eq!(control.reads.load(Ordering::SeqCst), 1);
        drop((sender, response, replacement));
        stop(&fixture).await;
    }
}

#[tokio::test]
async fn provisional_opening_cleanup_preserves_authoritative_replacement_and_watch() {
    use crate::interfaces::ProviderSessions;
    for restore in [false, true] {
        let fixture = Fixture::new(700).await;
        fixture.provider.inner.lock().unwrap().sessions.clear();
        fixture.provider.inner.lock().unwrap().attachments.clear();
        let (sender, mut response) = open(&fixture, None).await;
        nonce(&mut response).await;
        send(
            &sender,
            proto::control_message::Kind::SessionOpen(proto::SessionOpen {
                session_id: Some(fixture.sid.to_vec()),
                open_tx: Some(hash(0xaa).to_vec()),
                agent_id: None,
            }),
        )
        .await;
        send(&sender, challenge(9)).await;
        assert!(matches!(
            message(&mut response).await,
            proto::control_message::Kind::Challenge(_)
        ));
        let checkpoint = fixture.provider.checkpoint(fixture.sid).await.unwrap();
        assert_eq!(checkpoint.snapshot.control_generation, Some(0));
        if restore {
            fixture.provider.recover(checkpoint).await.unwrap();
        }
        drop(sender);
        yields().await;
        assert_eq!(
            fixture
                .provider
                .inner
                .lock()
                .unwrap()
                .sessions
                .contains_key(&fixture.sid),
            restore
        );
        // Force a fresh follower snapshot to prove cleanup did not remove a
        // replacement's watch (the old snapshot alone would hide this race).
        let current = fixture.provider.inner.chain.refresh().await.unwrap();
        assert_eq!(current.sessions().contains_key(&fixture.sid), restore);
        drop(response);
        stop(&fixture).await;
    }
}

#[tokio::test(start_paused = true)]
async fn recovery_watch_survives_provisional_eof_during_refresh() {
    use crate::interfaces::ProviderSessions;
    let (fixture, control) = controlled().await;
    fixture.provider.inner.lock().unwrap().sessions.clear();
    fixture.provider.inner.lock().unwrap().attachments.clear();
    let (sender, mut response) = open(&fixture, None).await;
    nonce(&mut response).await;
    send(
        &sender,
        proto::control_message::Kind::SessionOpen(proto::SessionOpen {
            session_id: Some(fixture.sid.to_vec()),
            open_tx: Some(hash(0xaa).to_vec()),
            agent_id: None,
        }),
    )
    .await;
    send(&sender, challenge(10)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::Challenge(_)
    ));
    let checkpoint = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    arm(&control, true);
    let recovery = tokio::spawn({
        let provider = fixture.provider.clone();
        async move { provider.recover(checkpoint).await }
    });
    yields().await;
    assert_eq!(
        control.reads.load(Ordering::SeqCst),
        1,
        "recovery is held after refresh cloned its watcher set"
    );
    drop(sender);
    yields().await;
    assert!(
        !fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .sessions
            .contains_key(&fixture.sid)
    );
    control.release.notify_one();
    let restored = recovery.await.unwrap().unwrap();
    assert_eq!(restored.session_id(), fixture.sid);
    control.armed.store(false, Ordering::SeqCst);
    let fresh = fixture.provider.inner.chain.refresh().await.unwrap();
    assert!(
        fresh.sessions().contains_key(&fixture.sid),
        "recovery installed state after unauthenticated cleanup revoked its watch"
    );
    drop(response);
    let (sender, mut response) = open(&fixture, None).await;
    let nonce = nonce(&mut response).await;
    send(&sender, proof(&fixture, nonce)).await;
    let proto::control_message::Kind::SessionState(state) = message(&mut response).await else {
        panic!("first legitimate attachment failed")
    };
    assert_eq!(state.control_generation, Some(1));
    assert_eq!(state.ready, Some(true));
    drop((sender, response));
    stop(&fixture).await;
}

async fn provisional(
    fixture: &Fixture,
) -> (
    mpsc::Sender<Bytes>,
    http::Response<Body>,
    crate::interfaces::RecoveryEvidence,
) {
    use crate::interfaces::ProviderSessions;
    fixture.provider.inner.lock().unwrap().sessions.clear();
    fixture.provider.inner.lock().unwrap().attachments.clear();
    let (sender, mut response) = open(fixture, None).await;
    nonce(&mut response).await;
    send(
        &sender,
        proto::control_message::Kind::SessionOpen(proto::SessionOpen {
            session_id: Some(fixture.sid.to_vec()),
            open_tx: Some(hash(0xaa).to_vec()),
            agent_id: None,
        }),
    )
    .await;
    send(&sender, challenge(11)).await;
    assert!(matches!(
        message(&mut response).await,
        proto::control_message::Kind::Challenge(_)
    ));
    let checkpoint = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    (sender, response, checkpoint)
}

#[tokio::test(start_paused = true)]
async fn failed_recovery_after_provisional_eof_releases_orphan_watch() {
    use crate::interfaces::ProviderSessions;
    let (fixture, control) = controlled().await;
    let (sender, response, checkpoint) = provisional(&fixture).await;
    control.hold_restore.store(true, Ordering::SeqCst);
    control.fail.store(true, Ordering::SeqCst);
    arm(&control, false);
    let recovery = tokio::spawn({
        let provider = fixture.provider.clone();
        async move { provider.recover(checkpoint).await }
    });
    yields().await;
    assert_eq!(control.restore_reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        control.reads.load(Ordering::SeqCst),
        0,
        "held before watch_session can add its watch"
    );
    drop(sender);
    yields().await;
    assert!(
        !fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .sessions
            .contains_key(&fixture.sid)
    );
    control.release.notify_one();
    assert!(recovery.await.unwrap().is_err());
    assert_eq!(control.reads.load(Ordering::SeqCst), 1);
    control.armed.store(false, Ordering::SeqCst);
    let current = fixture.provider.inner.chain.refresh().await.unwrap();
    assert!(
        !current.sessions().contains_key(&fixture.sid),
        "failed recovery leaked a watch based on stale existed state"
    );
    drop(response);
    stop(&fixture).await;
}

#[tokio::test(start_paused = true)]
async fn cancelled_recovery_after_provisional_eof_releases_watch_and_serialization() {
    use crate::interfaces::ProviderSessions;
    let (fixture, control) = controlled().await;
    let (sender, response, checkpoint) = provisional(&fixture).await;
    arm(&control, true);
    let recovery = tokio::spawn({
        let provider = fixture.provider.clone();
        async move { provider.recover(checkpoint).await }
    });
    yields().await;
    assert_eq!(control.reads.load(Ordering::SeqCst), 1);
    drop(sender);
    yields().await;
    recovery.abort();
    assert!(recovery.await.unwrap_err().is_cancelled());
    control.armed.store(false, Ordering::SeqCst);
    let current = fixture.provider.inner.chain.refresh().await.unwrap();
    assert!(!current.sessions().contains_key(&fixture.sid));
    drop(response);
    // A fresh valid setup can acquire the existing openings mutex and interest.
    let (sender, response, _) = provisional(&fixture).await;
    drop((sender, response));
    stop(&fixture).await;
}

#[tokio::test(start_paused = true)]
async fn pending_watch_for_another_session_does_not_block_provisional_cleanup() {
    let (fixture, _) = controlled().await;
    let (sender, response, _) = provisional(&fixture).await;
    let serial = fixture.provider.inner.openings.lock().await;
    let interest = PendingWatch::new(&fixture.provider.inner, hash(0x19)).unwrap();
    drop(sender);
    yields().await;
    assert!(
        !fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .sessions
            .contains_key(&fixture.sid)
    );
    let current = fixture.provider.inner.chain.refresh().await.unwrap();
    assert!(!current.sessions().contains_key(&fixture.sid));
    drop((interest, serial, response));
    stop(&fixture).await;
}
