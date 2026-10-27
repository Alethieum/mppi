//! Closed sessions retire instead of counting against a limit. A closed session
//! is terminal on chain (MPPI.sol `_existingSession`), so one with nothing left
//! to serve, collect or reconcile retires after `closed_retention` and is never
//! reinstalled. The fixture chain adds openings and closures to the core one.
use super::*;
use crate::{
    interfaces::{IntegrationError, ProviderSessions},
    signing::ReconnectAuth,
    types::OperationState,
};
use std::{sync::atomic::AtomicBool, time::SystemTime};

const DAY: Duration = Duration::from_secs(86400);

#[derive(Default)]
pub(super) struct Chain {
    pub(super) openings: Mutex<BTreeMap<B256, SessionDescriptor>>,
    /// Closed sessions with their `closeRequestedAt`: set before a withdrawal,
    /// zero for an idle release.
    closed: Mutex<BTreeMap<B256, u64>>,
    /// Milliseconds each exchange with the RPC takes, one batch or one call.
    pub(super) round_trip: AtomicU64,
}
struct ChainRpc {
    base: Arc<TestRpc>,
    chain: Arc<Chain>,
}
fn word_at(value: &str, from: usize) -> B256 {
    B256::from_slice(&hex::decode(&value[from..from + 64]).unwrap())
}
impl ChainRpc {
    async fn exchange(&self) {
        let millis = self.chain.round_trip.load(Ordering::SeqCst);
        if millis > 0 {
            tokio::time::sleep(Duration::from_millis(millis)).await;
        }
    }
    async fn answer(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        if method == "eth_getTransactionReceipt" {
            let tx = word_at(params[0].as_str().unwrap(), 2);
            if let Some(opened) = self.chain.openings.lock().unwrap().get(&tx) {
                return Ok(self.base.opened(opened, tx));
            }
        }
        let session = params[0]["data"]
            .as_str()
            .filter(|data| data.starts_with(&hx(&keccak256("getSession(bytes32)")[..4])))
            .map(|data| word_at(data, 10));
        let answer = self.base.call(method, params).await?;
        let Some(requested) =
            session.and_then(|sid| self.chain.closed.lock().unwrap().get(&sid).copied())
        else {
            return Ok(answer);
        };
        let mut state = hex::decode(answer.as_str().unwrap().trim_start_matches("0x")).unwrap();
        state[64..96].copy_from_slice(&word(2)); // SessionStatus.Closed
        state[96..128].copy_from_slice(&word(requested));
        Ok(json!(hx(state)))
    }
}
#[tonic::async_trait]
impl Rpc for ChainRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        self.exchange().await;
        self.answer(method, params).await
    }
    async fn call_many(&self, calls: &[(&str, Value)]) -> Result<Vec<Value>, ChainError> {
        self.exchange().await;
        let mut results = Vec::with_capacity(calls.len());
        for (method, params) in calls {
            results.push(self.answer(method, params.clone()).await?);
        }
        Ok(results)
    }
}
/// Retirement neither submits nor learns anything from the wallet.
struct Quiet;
#[tonic::async_trait]
impl TransactionWallet for Quiet {
    fn address(&self) -> Address {
        addr(0x11)
    }
    async fn execute(&self, _: &WalletCall, _: &str) -> Result<OperationResult, WalletError> {
        panic!("retirement never submits a transaction")
    }
    async fn reconcile(&self, reference: &str) -> Result<OperationResult, WalletError> {
        Ok(
            OperationResult::new(reference, OperationState::Unknown, None, None, None, vec![])
                .unwrap(),
        )
    }
}

pub(super) async fn fixture() -> (Fixture, Arc<Chain>) {
    fixture_with(Arc::new(Quiet)).await
}
async fn fixture_with(wallet: Arc<dyn TransactionWallet>) -> (Fixture, Arc<Chain>) {
    let chain = Arc::new(Chain::default());
    let shared = chain.clone();
    let fixture = Fixture::with_wallet(700, wallet, move |base| {
        Arc::new(ChainRpc {
            base,
            chain: shared,
        })
    })
    .await;
    (fixture, chain)
}
pub(super) fn descriptor_for(n: u64) -> SessionDescriptor {
    SessionDescriptor {
        salt: B256::from(U256::from(n).to_be_bytes::<32>()),
        ..descriptor()
    }
}
pub(super) fn sid_of(n: u64) -> B256 {
    domain().signing_context().session_id(&descriptor_for(n))
}
pub(super) fn notice(sid: B256, tx: B256) -> proto::SessionOpen {
    proto::SessionOpen {
        session_id: Some(sid.to_vec()),
        open_tx: Some(tx.to_vec()),
        agent_id: None,
    }
}
pub(super) fn reconnect(sid: B256, nonce: B256, pending: &[B256]) -> proto::ReconnectRequest {
    proto::ReconnectRequest {
        session_id: Some(sid.to_vec()),
        nonce: Some(nonce.to_vec()),
        signature: Some(sign(&ReconnectAuth {
            sessionId: sid,
            nonce,
        })),
        latest_voucher: None,
        last_meter_seq: Some(0),
        pending_run_ids: pending.iter().map(|rid| rid.to_vec()).collect(),
    }
}
pub(super) fn stream(fixture: &Fixture, id: u64) -> Arc<ByteQueue> {
    let queue = Arc::new(ByteQueue::default());
    fixture.provider.inner.lock().unwrap().attachments.insert(
        id,
        Attachment {
            queue: queue.clone(),
            nonce: keccak256(id.to_be_bytes()),
            live: true,
            sid: None,
            generation: 0,
            request: None,
            challenges: BTreeMap::new(),
            catalogs: BTreeMap::new(),
            challenge_bytes: 0,
        },
    );
    queue
}
/// Session `n` opened and authenticated on its own Control stream, which then
/// ends, as an agent's does when it is done.
pub(super) async fn attached(
    fixture: &Fixture,
    chain: &Chain,
    n: u64,
) -> Result<(B256, B256), Status> {
    let sid = sid_of(n);
    let tx = keccak256(sid);
    chain.openings.lock().unwrap().insert(tx, descriptor_for(n));
    let id = 1000 + n;
    stream(fixture, id);
    let runtime = &fixture.provider.inner;
    runtime.opening(id, notice(sid, tx)).await?;
    runtime.attach(id, reconnect(sid, keccak256(id.to_be_bytes()), &[]))?;
    runtime.lock().unwrap().attachments.remove(&id);
    Ok((sid, tx))
}
async fn close(fixture: &Fixture, chain: &Chain, sid: B256, requested_at: u64) {
    chain.closed.lock().unwrap().insert(sid, requested_at);
    fixture.provider.inner.chain.refresh().await.unwrap();
}
fn retained(fixture: &Fixture, sid: B256) -> bool {
    fixture
        .provider
        .inner
        .lock()
        .unwrap()
        .sessions
        .contains_key(&sid)
}
/// One maintenance pass, then every collection it started has finished.
async fn maintain(fixture: &Fixture) {
    fixture.provider.inner.maintain_once();
    while !fixture.provider.inner.lock().unwrap().collecting.is_empty() {
        tokio::task::yield_now().await;
    }
}
fn watched(fixture: &Fixture) -> Vec<B256> {
    let snapshot = fixture.provider.inner.chain.snapshot().unwrap();
    snapshot.sessions().keys().copied().collect()
}
fn failure(error: &IntegrationError) -> Option<String> {
    reason(error.downcast_ref::<Status>().unwrap())
}

#[tokio::test(start_paused = true)]
async fn closed_sessions_retire_so_openings_never_stop_at_a_session_count() {
    let (fixture, chain) = fixture().await;
    // A hundred more than the 1,024 sessions a process once held for good.
    for n in 0..1124 {
        let (sid, _) = attached(&fixture, &chain, n)
            .await
            .unwrap_or_else(|error| panic!("opening {n} refused: {error}"));
        close(&fixture, &chain, sid, 0).await;
        maintain(&fixture).await;
        tokio::time::advance(DAY).await;
        maintain(&fixture).await;
    }
    let retained = fixture.provider.inner.lock().unwrap().sessions.len();
    assert_eq!(retained, 1, "only the fixture's open session remains");
    fixture.provider.inner.chain.refresh().await.unwrap();
    assert_eq!(
        watched(&fixture),
        [fixture.sid],
        "retired sessions stay watched"
    );
}

#[tokio::test(start_paused = true)]
async fn a_resent_opening_for_a_closed_session_is_refused_and_never_reinstalled() {
    let (fixture, chain) = fixture().await;
    let runtime = &fixture.provider.inner;
    let (sid, tx) = attached(&fixture, &chain, 1).await.unwrap();
    close(&fixture, &chain, sid, 0).await;
    // As after its retirement: this process no longer holds the session.
    runtime.lock().unwrap().sessions.remove(&sid);
    let queue = stream(&fixture, 7);
    let error = runtime.opening(7, notice(sid, tx)).await.unwrap_err();
    assert_eq!(reason(&error).as_deref(), Some("SESSION_CLOSED"));
    assert!(!retained(&fixture, sid));
    // A reattachment naming a run served before then gets no snapshot at all,
    // so never NOT_ADMITTED for it.
    let error = runtime
        .attach(7, reconnect(sid, keccak256(7u64.to_be_bytes()), &[hash(9)]))
        .unwrap_err();
    assert_eq!(reason(&error).as_deref(), Some("STATE_UNAVAILABLE"));
    assert_eq!(queue.buffered().unwrap(), (0, 0));
    runtime.chain.refresh().await.unwrap();
    assert_eq!(
        watched(&fixture),
        [fixture.sid],
        "a refused opening keeps its watch"
    );
}

#[tokio::test(start_paused = true)]
async fn a_closed_session_that_opened_before_the_start_is_refused_session_closed() {
    let (fixture, chain) = fixture().await;
    let runtime = &fixture.provider.inner;
    let (closed, closed_tx) = attached(&fixture, &chain, 1).await.unwrap();
    let (open, open_tx) = attached(&fixture, &chain, 2).await.unwrap();
    close(&fixture, &chain, closed, 0).await;
    {
        // As in a process started after both openings, holding neither.
        let mut state = runtime.lock().unwrap();
        state.sessions.remove(&closed);
        state.sessions.remove(&open);
        state.start_block = Some(u64::MAX);
    }
    for id in [7, 8] {
        stream(&fixture, id);
    }
    // The closed check comes first, as in the Python runtime: the agent learns
    // the session closed, not that its state is unavailable.
    let error = runtime
        .opening(7, notice(closed, closed_tx))
        .await
        .unwrap_err();
    assert_eq!(reason(&error).as_deref(), Some("SESSION_CLOSED"));
    // An open session from before the start still needs its recovery.
    let error = runtime.opening(8, notice(open, open_tx)).await.unwrap_err();
    assert_eq!(reason(&error).as_deref(), Some("STATE_UNAVAILABLE"));
    assert!(!retained(&fixture, closed) && !retained(&fixture, open));
    runtime.chain.refresh().await.unwrap();
    assert_eq!(
        watched(&fixture),
        [fixture.sid],
        "a refused opening keeps its watch"
    );
}

#[tokio::test(start_paused = true)]
async fn a_closed_session_is_retained_for_its_window_and_while_anything_remains() {
    let (mut fixture, chain) = fixture().await;
    let (sent, retirements) = std::sync::mpsc::channel();
    Arc::get_mut(&mut fixture.provider.inner).unwrap().observers = hooks::Observers::new(Hooks {
        on_session_retired: Some(Arc::new(move |sid| sent.send(sid).unwrap())),
        ..Hooks::default()
    });
    let runtime = &fixture.provider.inner;
    // Within closed_retention an agent can still read it, and the host can
    // still take its final checkpoint.
    let (sid, _) = attached(&fixture, &chain, 1).await.unwrap();
    close(&fixture, &chain, sid, 0).await;
    maintain(&fixture).await;
    tokio::time::advance(DAY - Duration::from_secs(1)).await;
    maintain(&fixture).await;
    assert!(retained(&fixture, sid));
    fixture.provider.checkpoint(sid).await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    maintain(&fixture).await;
    assert!(
        !retained(&fixture, sid),
        "a final session outlived its window"
    );
    let retired = retirements.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(retired, sid);
    assert_eq!(
        failure(&fixture.provider.get(sid).await.unwrap_err()).as_deref(),
        Some("STATE_UNAVAILABLE")
    );
    assert_eq!(
        failure(&fixture.provider.checkpoint(sid).await.unwrap_err()).as_deref(),
        Some("STATE_UNAVAILABLE")
    );

    let observation = ChainObservation::new(CHAIN, addr(0x99), 100, hash(0xbb)).unwrap();
    let operation = |n: u64, close: bool, state: OperationState| {
        let descriptor = descriptor_for(n);
        let signature = sign(&Voucher {
            sessionId: sid_of(n),
            cumulativeAmount: 1,
        });
        let prepared = if close {
            runtime.contract.close(&descriptor, 1, 1, &signature)
        } else {
            runtime.contract.settle(&descriptor, 1, 1, &signature)
        };
        let confirmed = state == OperationState::Confirmed;
        PendingOperation {
            prepared: Arc::new(prepared.unwrap()),
            result: Arc::new(
                OperationResult::new(
                    if close {
                        "close:retire"
                    } else {
                        "settle:retire"
                    },
                    state,
                    None,
                    confirmed.then_some(hash(0xcc)),
                    confirmed.then_some(observation),
                    vec![],
                )
                .unwrap(),
            ),
            units: Arc::new(BTreeMap::from([(sid_of(n), 0)])),
        }
    };
    type Change<'a> = Box<dyn Fn(&mut Session, u64) + 'a>;
    let cases: Vec<(&str, Change<'_>, Change<'_>)> = vec![
        (
            "an unresolved operation",
            Box::new(|session, n| {
                session.operation = Some(operation(n, false, OperationState::Pending))
            }),
            Box::new(|session, n| {
                session.operation = Some(operation(n, false, OperationState::Failed))
            }),
        ),
        (
            "an unresolved baseline",
            Box::new(|session, n| {
                session.baseline = Some(operation(n, false, OperationState::Unknown))
            }),
            Box::new(|session, _| session.baseline = None),
        ),
        (
            "a close in progress",
            Box::new(|session, n| {
                session.close_pending = true;
                session.close_reference = Some("close:retire".into());
                session.operation = Some(operation(n, true, OperationState::Pending));
            }),
            Box::new(|session, n| {
                session.operation = Some(operation(n, true, OperationState::Failed))
            }),
        ),
        (
            "a confirmed close without its result",
            Box::new(|session, n| {
                session.close_pending = true;
                session.close_reference = Some("close:retire".into());
                session.operation = Some(operation(n, true, OperationState::Confirmed));
            }),
            Box::new(|session, n| {
                session.close_result = Some(proto::CloseResult {
                    session_id: Some(sid_of(n).to_vec()),
                    tx_hash: Some(hash(0xcc).to_vec()),
                    capture_amount: Some(1),
                    refunded: Some(99),
                })
            }),
        ),
        (
            "the inconsistent flag",
            Box::new(|session, _| session.inconsistent = true),
            Box::new(|session, _| session.inconsistent = false),
        ),
    ];
    for (n, (blocker, hold, release)) in (2..).zip(cases) {
        let (sid, _) = attached(&fixture, &chain, n).await.unwrap();
        hold(runtime.lock().unwrap().sessions.get_mut(&sid).unwrap(), n);
        close(&fixture, &chain, sid, 0).await;
        for _ in 0..2 {
            maintain(&fixture).await;
            tokio::time::advance(DAY).await;
        }
        maintain(&fixture).await;
        assert!(retained(&fixture, sid), "retired with {blocker}");
        release(runtime.lock().unwrap().sessions.get_mut(&sid).unwrap(), n);
        maintain(&fixture).await;
        tokio::time::advance(DAY).await;
        maintain(&fixture).await;
        assert!(!retained(&fixture, sid), "kept after {blocker} ended");
    }

    // A live owner holds a final session past its window, until it detaches.
    let (sid, _) = attached(&fixture, &chain, 9).await.unwrap();
    stream(&fixture, 9);
    {
        let mut state = runtime.lock().unwrap();
        state.attachments.get_mut(&9).unwrap().sid = Some(sid);
        state.sessions.get_mut(&sid).unwrap().owner = Some(9);
    }
    close(&fixture, &chain, sid, 0).await;
    maintain(&fixture).await;
    tokio::time::advance(DAY).await;
    maintain(&fixture).await;
    assert!(retained(&fixture, sid), "retired with a live owner");
    runtime.lock().unwrap().attachments.remove(&9);
    maintain(&fixture).await;
    assert!(!retained(&fixture, sid), "kept after its owner detached");

    // An admitted run keeps its session until the host reports its final.
    fixture.voucher(1);
    let run = fixture.admit(1);
    runtime.lock().unwrap().attachments.remove(&1);
    close(&fixture, &chain, fixture.sid, 0).await;
    maintain(&fixture).await;
    tokio::time::advance(DAY).await;
    maintain(&fixture).await;
    assert!(
        retained(&fixture, fixture.sid),
        "retired with an active run"
    );
    runtime
        .report_sync(&run.context, counts(10), Some(RunStatus::Completed))
        .unwrap();
    maintain(&fixture).await;
    tokio::time::advance(DAY).await;
    maintain(&fixture).await;
    assert!(!retained(&fixture, fixture.sid), "kept after its run ended");
}

#[tokio::test(start_paused = true)]
async fn a_pending_close_another_exit_completed_is_final_and_retires() {
    let (fixture, chain) = fixture().await;
    let runtime = &fixture.provider.inner;
    // The agent withdrew after requesting a close, or the session was released
    // idle, before the provider's own close: nothing remains to capture.
    for (n, requested_at, failed) in [(1, TIME - 3600, false), (2, 0, true)] {
        let (sid, _) = attached(&fixture, &chain, n).await.unwrap();
        {
            let mut state = runtime.lock().unwrap();
            let session = state.sessions.get_mut(&sid).unwrap();
            session.close_pending = true;
            session.close_reference = Some(format!("close:{n}"));
        }
        if failed {
            // Its own close reverted: the contract already held it closed.
            let descriptor = descriptor_for(n);
            let prepared = runtime.contract.close(&descriptor, 0, 0, &[]).unwrap();
            runtime
                .lock()
                .unwrap()
                .sessions
                .get_mut(&sid)
                .unwrap()
                .operation = Some(PendingOperation {
                prepared: Arc::new(prepared),
                result: Arc::new(
                    OperationResult::new(
                        format!("close:{n}"),
                        OperationState::Failed,
                        None,
                        None,
                        None,
                        vec![],
                    )
                    .unwrap(),
                ),
                units: Arc::new(BTreeMap::from([(sid, 0)])),
            });
        }
        close(&fixture, &chain, sid, requested_at).await;
        maintain(&fixture).await;
        let due = fixture.provider.collections_due().unwrap();
        for (reserved, _) in &due {
            fixture.provider.collection_finished(*reserved);
        }
        assert!(
            !due.iter().any(|(due, _)| *due == sid),
            "a final close is retried"
        );
        tokio::time::advance(DAY).await;
        maintain(&fixture).await;
        assert!(
            !retained(&fixture, sid),
            "a close another exit completed stays pending"
        );
    }
}

#[tokio::test]
async fn final_evidence_of_a_closed_session_is_refused_and_unfinished_evidence_recovers() {
    let (fixture, chain) = fixture().await;
    fixture.voucher(100);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(500), None)
        .unwrap();
    close(&fixture, &chain, fixture.sid, 0).await;
    let unfinished = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(750), Some(RunStatus::Completed))
        .unwrap();
    let finished = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    assert!(finished.is_final() && !unfinished.is_final());
    let provider = super::recovery_tests::restarted(&fixture);
    let error = provider.recover(finished.clone()).await.unwrap_err();
    assert_eq!(failure(&error).as_deref(), Some("SESSION_CLOSED"));
    let error = provider.recover_current(finished).await.unwrap_err();
    assert_eq!(failure(&error).as_deref(), Some("SESSION_CLOSED"));
    assert!(provider.inner.lock().unwrap().sessions.is_empty());
    // Its reservation is unresolved: recovered, then retired once it ends.
    provider.recover(unfinished).await.unwrap();
    assert!(provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)].unresolved);
}

#[tokio::test(start_paused = true)]
async fn closed_retention_is_one_hour_to_seven_days_and_is_applied() {
    let hour = Duration::from_secs(3600);
    assert_eq!(RuntimeLimits::default().closed_retention(), DAY);
    for retention in [hour, 7 * DAY] {
        let limits = RuntimeLimits::default().with_closed_retention(retention);
        assert_eq!(limits.unwrap().closed_retention(), retention);
    }
    for retention in [
        hour - Duration::from_millis(1),
        7 * DAY + Duration::from_millis(1),
    ] {
        let limits = RuntimeLimits::default().with_closed_retention(retention);
        assert_eq!(limits.unwrap_err().code(), ValidationCode::InvalidValue);
    }
    let (mut fixture, chain) = fixture().await;
    Arc::get_mut(&mut fixture.provider.inner).unwrap().limits = RuntimeLimits::default()
        .with_closed_retention(hour)
        .unwrap();
    let (sid, _) = attached(&fixture, &chain, 1).await.unwrap();
    close(&fixture, &chain, sid, 0).await;
    maintain(&fixture).await;
    tokio::time::advance(hour).await;
    maintain(&fixture).await;
    assert!(!retained(&fixture, sid));
}

/// Records each call to forget references, with the time from which it may.
/// Its collections never reach a wallet service, unless `lose` leaves their
/// outcome unknown. Its record of a reference names the transaction `bound`
/// gives it, if any; `looked_up` lists each reference it was asked about.
#[derive(Default)]
struct Forgetting {
    lose: AtomicBool,
    refuse: AtomicBool,
    near: AtomicBool,
    attempts: AtomicU64,
    retired: Mutex<Vec<(BTreeSet<String>, SystemTime)>>,
    bound: Mutex<BTreeMap<String, B256>>,
    looked_up: Mutex<Vec<String>>,
}
impl Forgetting {
    fn retired(&self) -> Vec<BTreeSet<String>> {
        let calls = self.retired.lock().unwrap();
        calls
            .iter()
            .map(|(references, _)| references.clone())
            .collect()
    }
    fn at(&self, call: usize) -> SystemTime {
        self.retired.lock().unwrap()[call].1
    }
}
#[tonic::async_trait]
impl TransactionWallet for Forgetting {
    fn address(&self) -> Address {
        addr(0x11)
    }
    async fn execute(
        &self,
        _: &WalletCall,
        reference: &str,
    ) -> Result<OperationResult, WalletError> {
        let state = if self.lose.load(Ordering::SeqCst) {
            OperationState::Unknown
        } else {
            OperationState::Failed
        };
        Ok(OperationResult::new(reference, state, None, None, None, vec![]).unwrap())
    }
    async fn reconcile(&self, reference: &str) -> Result<OperationResult, WalletError> {
        self.looked_up.lock().unwrap().push(reference.into());
        let tx = self.bound.lock().unwrap().get(reference).copied();
        Ok(
            OperationResult::new(reference, OperationState::Unknown, None, tx, None, vec![])
                .unwrap(),
        )
    }
    async fn retire(&self, operation_refs: &[String], at: SystemTime) -> Result<(), WalletError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.refuse.load(Ordering::SeqCst) {
            return Err(WalletError);
        }
        let references = operation_refs.iter().cloned().collect();
        self.retired.lock().unwrap().push((references, at));
        Ok(())
    }
    fn near_capacity(&self) -> bool {
        self.near.load(Ordering::SeqCst)
    }
}
/// Session `sid` may capture nothing: its voucher authorizes zero.
fn authorize(fixture: &Fixture, sid: B256) {
    let voucher = proto::Voucher {
        session_id: Some(sid.to_vec()),
        cumulative: Some(0),
        signature: Some(sign(&Voucher {
            sessionId: sid,
            cumulativeAmount: 0,
        })),
    };
    let mut state = fixture.provider.inner.lock().unwrap();
    state.sessions.get_mut(&sid).unwrap().voucher = Some(voucher);
}
/// One maintenance pass, then the wallet call it started, if any, has ended.
async fn forget(fixture: &Fixture) {
    maintain(fixture).await;
    while fixture.provider.inner.lock().unwrap().retiring {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_reference_retires_from_the_wallet_once_every_session_it_covers_retired() {
    let wallet = Arc::new(Forgetting::default());
    let (fixture, chain) = fixture_with(wallet.clone()).await;
    let runtime = &fixture.provider.inner;
    let (first, _) = attached(&fixture, &chain, 1).await.unwrap();
    let (second, _) = attached(&fixture, &chain, 2).await.unwrap();
    let (stuck, _) = attached(&fixture, &chain, 3).await.unwrap();
    for sid in [first, second, stuck] {
        authorize(&fixture, sid);
    }
    // Two captures of the first session: the earlier one is superseded.
    let earlier = fixture.provider.settle(first).await.unwrap();
    let later = fixture.provider.settle(first).await.unwrap();
    assert_ne!(earlier.operation_ref(), later.operation_ref());
    let other = fixture.provider.settle(second).await.unwrap();
    // The stuck session's capture is lost, so it stays unresolved.
    wallet.lose.store(true, Ordering::SeqCst);
    let lost = fixture.provider.settle(stuck).await.unwrap();
    wallet.lose.store(false, Ordering::SeqCst);
    assert_eq!(lost.state(), OperationState::Unknown);
    // A batch reference covering the first two sessions (as a batch
    // collection retains it, `sessions::batch_tests`).
    let shared = "settleBatch:shared".to_owned();
    runtime.lock().unwrap().references.insert(
        shared.clone(),
        BTreeMap::from([(first, None), (second, None)]),
    );
    let references = |list: &[&OperationResult]| {
        list.iter()
            .map(|result| result.operation_ref().to_owned())
            .collect::<BTreeSet<_>>()
    };

    // The first session retires: its own captures go, from the time it
    // retired, and the shared batch stays.
    close(&fixture, &chain, first, 0).await;
    forget(&fixture).await;
    tokio::time::advance(DAY).await;
    let before = SystemTime::now();
    forget(&fixture).await;
    let after = SystemTime::now();
    assert!(!retained(&fixture, first));
    assert_eq!(wallet.retired(), [references(&[&earlier, &later])]);
    assert!((before..=after).contains(&wallet.at(0)));

    // A wallet that cannot forget them now keeps them for a later pass, at
    // most once per settlement interval.
    for sid in [second, stuck] {
        close(&fixture, &chain, sid, 0).await;
    }
    forget(&fixture).await;
    tokio::time::advance(DAY).await;
    wallet.refuse.store(true, Ordering::SeqCst);
    forget(&fixture).await;
    assert!(!retained(&fixture, second));
    assert_eq!(wallet.retired.lock().unwrap().len(), 1);
    wallet.refuse.store(false, Ordering::SeqCst);
    let attempts = wallet.attempts.load(Ordering::SeqCst);
    for _ in 0..3 {
        forget(&fixture).await;
    }
    assert_eq!(
        wallet.attempts.load(Ordering::SeqCst),
        attempts,
        "a refused retire spins"
    );
    let interval = Duration::from_millis(runtime.config.settlement().interval_ms());
    tokio::time::advance(interval).await;
    forget(&fixture).await;
    let mut expected = references(&[&other]);
    expected.insert(shared);
    assert_eq!(wallet.retired()[1..], [expected]);

    // An unresolved capture keeps its session, so its record is never forgotten.
    for _ in 0..3 {
        tokio::time::advance(DAY).await;
        forget(&fixture).await;
    }
    assert!(retained(&fixture, stuck));
    assert_eq!(wallet.retired.lock().unwrap().len(), 2);
    let state = runtime.lock().unwrap();
    assert_eq!(
        state.references.keys().collect::<Vec<_>>(),
        [lost.operation_ref()]
    );
}

#[tokio::test(start_paused = true)]
async fn each_rise_of_the_wallets_capacity_is_reported_once() {
    let wallet = Arc::new(Forgetting::default());
    let (mut fixture, _) = fixture_with(wallet.clone()).await;
    let (sent, errors) = std::sync::mpsc::channel();
    Arc::get_mut(&mut fixture.provider.inner).unwrap().observers = hooks::Observers::new(Hooks {
        on_error: Some(Arc::new(move |error: HookError| {
            sent.send((error.operation, error.code)).unwrap()
        })),
        ..Hooks::default()
    });
    let alert = ("wallet", tonic::Code::ResourceExhausted);
    maintain(&fixture).await;
    wallet.near.store(true, Ordering::SeqCst);
    maintain(&fixture).await;
    maintain(&fixture).await;
    assert_eq!(errors.recv_timeout(Duration::from_secs(1)).unwrap(), alert);
    wallet.near.store(false, Ordering::SeqCst);
    maintain(&fixture).await;
    wallet.near.store(true, Ordering::SeqCst);
    maintain(&fixture).await;
    assert_eq!(errors.recv_timeout(Duration::from_secs(1)).unwrap(), alert);
    assert!(errors.recv_timeout(Duration::from_millis(100)).is_err());
}

#[tokio::test(start_paused = true)]
async fn a_batch_reference_is_due_at_the_latest_of_its_sessions_times() {
    let wallet = Arc::new(Forgetting::default());
    let (fixture, chain) = fixture_with(wallet.clone()).await;
    let runtime = &fixture.provider.inner;
    let (first, _) = attached(&fixture, &chain, 1).await.unwrap();
    let (second, _) = attached(&fixture, &chain, 2).await.unwrap();
    // The second session's later confirmed collections superseded both
    // batches: one is due after the first session retires, one before.
    let later = SystemTime::now() + 2 * DAY;
    let earlier = SystemTime::UNIX_EPOCH + DAY;
    {
        let mut state = runtime.lock().unwrap();
        for (reference, superseded) in [
            ("settleBatch:later", later),
            ("settleBatch:earlier", earlier),
        ] {
            state.references.insert(
                reference.into(),
                BTreeMap::from([(first, None), (second, Some(superseded))]),
            );
        }
    }
    close(&fixture, &chain, first, 0).await;
    forget(&fixture).await;
    tokio::time::advance(DAY).await;
    let before = SystemTime::now();
    forget(&fixture).await;
    let after = SystemTime::now();
    // Retirement counts as no delay; the latest time is the one each waits for.
    let calls = wallet.retired.lock().unwrap().clone();
    assert_eq!(calls.len(), 2, "one call for each second");
    for (references, at) in calls {
        if references.contains("settleBatch:later") {
            assert_eq!(at, later);
        } else {
            assert_eq!(
                references,
                BTreeSet::from(["settleBatch:earlier".to_owned()])
            );
            assert!((before..=after).contains(&at));
        }
    }
    assert!(runtime.lock().unwrap().references.is_empty());
}

/// References due within the same second share one call, at the latest of
/// their times, and those due in a later second another: for any wallet never
/// before a reference is due, and less than a second after (the Circle adapter
/// keeps whole seconds, so there each gets the mark its own time would).
#[tokio::test(start_paused = true)]
async fn references_due_within_one_second_share_one_retire_call() {
    let wallet = Arc::new(Forgetting::default());
    let (fixture, _) = fixture_with(wallet.clone()).await;
    let second = SystemTime::UNIX_EPOCH + Duration::from_secs(1_900_000_000);
    let at = |millis| second + Duration::from_millis(millis);
    fixture.provider.inner.lock().unwrap().retirable.extend(
        [
            ("settle:a", 200),
            ("settle:b", 700),
            ("settle:c", 1000),
            ("settle:d", 1500),
        ]
        .map(|(reference, millis)| (reference.to_owned(), at(millis))),
    );
    forget(&fixture).await;
    let named = |list: &[&str]| list.iter().map(|name| name.to_string()).collect();
    assert_eq!(
        *wallet.retired.lock().unwrap(),
        [
            (named(&["settle:a", "settle:b"]), at(700)),
            (named(&["settle:c", "settle:d"]), at(1500)),
        ]
    );
}

/// A session final but not yet retired when its process stopped had its final
/// evidence refused after the restart, so its references were never learned
/// and never forgotten.
#[tokio::test(start_paused = true)]
async fn a_final_record_refused_at_recovery_hands_its_references_to_retirement() {
    let wallet = Arc::new(Forgetting::default());
    let (fixture, chain) = fixture_with(wallet.clone()).await;
    let runtime = &fixture.provider.inner;
    let (sid, _) = attached(&fixture, &chain, 1).await.unwrap();
    let (other, _) = attached(&fixture, &chain, 2).await.unwrap();
    let observation = ChainObservation::new(CHAIN, addr(0x99), 100, hash(0xbb)).unwrap();
    let confirmed = |reference: &str, prepared: crate::contract::PreparedOperation| {
        let units = prepared
            .session_ids()
            .into_iter()
            .map(|id| (id, 0))
            .collect();
        PendingOperation {
            prepared: Arc::new(prepared),
            result: Arc::new(
                OperationResult::new(
                    reference,
                    OperationState::Confirmed,
                    None,
                    Some(hash(0xcc)),
                    Some(observation),
                    vec![],
                )
                .unwrap(),
            ),
            units: Arc::new(units),
        }
    };
    let signature = |id| {
        sign(&Voucher {
            sessionId: id,
            cumulativeAmount: 1,
        })
    };
    let (own, shared) = (descriptor_for(1), descriptor_for(2));
    let settle = runtime
        .contract
        .settle(&own, 1, 1, &signature(sid))
        .unwrap();
    let batch = runtime
        .contract
        .settle_batch(&[
            (&own, 1, 1, &signature(sid)[..]),
            (&shared, 1, 1, &signature(other)[..]),
        ])
        .unwrap();
    {
        let mut state = runtime.lock().unwrap();
        let session = state.sessions.get_mut(&sid).unwrap();
        session.baseline = Some(confirmed("settle:own", settle));
        session.operation = Some(confirmed("settleBatch:shared", batch));
    }
    // The wallet's own record of each reference names the record's transaction.
    for reference in ["settle:own", "settleBatch:shared"] {
        wallet
            .bound
            .lock()
            .unwrap()
            .insert(reference.into(), hash(0xcc));
    }
    close(&fixture, &chain, sid, 0).await;
    let evidence = fixture.provider.checkpoint(sid).await.unwrap();
    assert!(evidence.is_final());
    let provider = super::recovery_tests::restarted(&fixture);
    let before = SystemTime::now();
    let error = provider.recover(evidence).await.unwrap_err();
    let after = SystemTime::now();
    assert_eq!(failure(&error).as_deref(), Some("SESSION_CLOSED"));
    provider.inner.maintain_once();
    while provider.inner.lock().unwrap().retiring {
        tokio::task::yield_now().await;
    }
    // Its own capture goes from now; the batch waits for the other session.
    assert_eq!(
        wallet.retired(),
        [BTreeSet::from(["settle:own".to_owned()])]
    );
    assert!((before..=after).contains(&wallet.at(0)));
    let state = provider.inner.lock().unwrap();
    let covered = &state.references["settleBatch:shared"];
    assert!(covered[&sid].is_some() && covered[&other].is_none());
}

/// A final record hands over only a reference the wallet's own record binds to
/// it: both name the same transaction, or neither names one. A mixed-up or
/// corrupt record could pair this session's call with another session's
/// reference, whose record would then be forgotten; and a wallet record with a
/// transaction the record lacks is traceable, so it stays for the wallet's
/// reconciliation. The wallet is asked about each; one not handed over is
/// kept, which is always safe.
#[tokio::test(start_paused = true)]
async fn a_final_record_hands_over_only_references_the_wallet_binds_to_it() {
    let observation = ChainObservation::new(CHAIN, addr(0x99), 100, hash(0xbb)).unwrap();
    for (case, saved, recorded, handed) in [
        (
            "the record's transaction",
            Some(hash(0xcc)),
            Some(hash(0xcc)),
            true,
        ),
        (
            "another transaction",
            Some(hash(0xcc)),
            Some(hash(0xdd)),
            false,
        ),
        ("no transaction", Some(hash(0xcc)), None, false),
        (
            "a transaction the record lacks",
            None,
            Some(hash(0xcc)),
            false,
        ),
        ("no transaction on either side", None, None, true),
    ] {
        let wallet = Arc::new(Forgetting::default());
        let (fixture, chain) = fixture_with(wallet.clone()).await;
        let runtime = &fixture.provider.inner;
        let (sid, _) = attached(&fixture, &chain, 1).await.unwrap();
        // This session's capture, under another session's reference.
        let reference = "settle:another-session";
        let prepared = runtime
            .contract
            .settle(
                &descriptor_for(1),
                1,
                1,
                &sign(&Voucher {
                    sessionId: sid,
                    cumulativeAmount: 1,
                }),
            )
            .unwrap();
        let result = match saved {
            Some(tx) => OperationResult::new(
                reference,
                OperationState::Confirmed,
                None,
                Some(tx),
                Some(observation),
                vec![],
            ),
            None => {
                OperationResult::new(reference, OperationState::Failed, None, None, None, vec![])
            }
        }
        .unwrap();
        runtime
            .lock()
            .unwrap()
            .sessions
            .get_mut(&sid)
            .unwrap()
            .operation = Some(PendingOperation {
            prepared: Arc::new(prepared),
            result: Arc::new(result),
            units: Arc::new(BTreeMap::from([(sid, 0)])),
        });
        if let Some(tx) = recorded {
            wallet.bound.lock().unwrap().insert(reference.into(), tx);
        }
        close(&fixture, &chain, sid, 0).await;
        let evidence = fixture.provider.checkpoint(sid).await.unwrap();
        assert!(evidence.is_final(), "{case}");
        let provider = super::recovery_tests::restarted(&fixture);
        let error = provider.recover(evidence).await.unwrap_err();
        assert_eq!(failure(&error).as_deref(), Some("SESSION_CLOSED"), "{case}");
        provider.inner.maintain_once();
        while provider.inner.lock().unwrap().retiring {
            tokio::task::yield_now().await;
        }
        let expected = if handed {
            vec![BTreeSet::from([reference.to_owned()])]
        } else {
            vec![]
        };
        assert_eq!(wallet.retired(), expected, "{case}");
        assert!(
            wallet
                .looked_up
                .lock()
                .unwrap()
                .iter()
                .any(|known| known == reference),
            "{case}: the wallet was not asked"
        );
        assert!(
            provider.inner.lock().unwrap().references.is_empty(),
            "{case}"
        );
    }
}

/// A record restored without a transaction hash, as a settlement that never
/// began is, was not looked up in the wallet at recovery, and no wallet answer
/// in this process has matched it since. Its hand-over takes recovery's wallet
/// check: a wallet record with a transaction the record lacks is traceable and
/// kept; one that agrees goes.
#[tokio::test(start_paused = true)]
async fn a_reference_restored_without_a_wallet_lookup_is_retired_only_if_the_wallet_agrees() {
    for (case, recorded, retired) in [
        ("a transaction the record lacks", Some(hash(0xcc)), false),
        ("no transaction on either side", None, true),
    ] {
        let wallet = Arc::new(Forgetting::default());
        let (fixture, chain) = fixture_with(wallet.clone()).await;
        let runtime = &fixture.provider.inner;
        let (sid, _) = attached(&fixture, &chain, 1).await.unwrap();
        let reference = "settle:never-began";
        let prepared = runtime
            .contract
            .settle(
                &descriptor_for(1),
                0,
                0,
                &sign(&Voucher {
                    sessionId: sid,
                    cumulativeAmount: 0,
                }),
            )
            .unwrap();
        let never_began =
            OperationResult::new(reference, OperationState::Failed, None, None, None, vec![])
                .unwrap();
        runtime
            .lock()
            .unwrap()
            .sessions
            .get_mut(&sid)
            .unwrap()
            .operation = Some(PendingOperation {
            prepared: Arc::new(prepared),
            result: Arc::new(never_began),
            units: Arc::new(BTreeMap::from([(sid, 0)])),
        });
        let evidence = fixture.provider.checkpoint(sid).await.unwrap();
        let provider = super::recovery_tests::restarted(&fixture);
        provider.recover(evidence).await.unwrap();
        assert!(wallet.looked_up.lock().unwrap().is_empty(), "{case}");
        if let Some(tx) = recorded {
            wallet.bound.lock().unwrap().insert(reference.into(), tx);
        }
        // The session closes and retires here, handing its references over.
        close(&fixture, &chain, sid, 0).await;
        provider.inner.maintain_once();
        tokio::time::advance(DAY).await;
        provider.inner.maintain_once();
        while provider.inner.lock().unwrap().retiring {
            tokio::task::yield_now().await;
        }
        assert!(
            !provider.inner.lock().unwrap().sessions.contains_key(&sid),
            "{case}: the session did not retire"
        );
        let expected = if retired {
            vec![BTreeSet::from([reference.to_owned()])]
        } else {
            vec![]
        };
        assert_eq!(wallet.retired(), expected, "{case}");
        assert!(
            wallet
                .looked_up
                .lock()
                .unwrap()
                .iter()
                .any(|known| known == reference),
            "{case}: the wallet was not asked"
        );
    }
}
