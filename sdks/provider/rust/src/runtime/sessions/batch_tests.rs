//! Runtime batch tests exercise the public session API with canonical ABI/RPC
//! evidence, independent wallet handles and provider-reported usage.
use super::super::tests::{Fixture, TestRpc};
use super::*;
use crate::{
    capabilities::{WalletCall, WalletError},
    chain::{ChainError, Rpc},
    contract::abi::IMPPI,
    interfaces::ProviderLifecycle,
    signing::SessionDescriptor,
};
use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::SolCall;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

fn hx(value: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(value))
}
fn word(value: u64) -> Vec<u8> {
    U256::from(value).to_be_bytes::<32>().to_vec()
}
fn aword(value: Address) -> Vec<u8> {
    [vec![0; 12], value.to_vec()].concat()
}
fn tx(number: u64) -> B256 {
    B256::repeat_byte(0xd0 + number as u8)
}
#[derive(Default)]
struct Probe {
    calls: Mutex<Vec<(WalletCall, String)>>,
    lookups: AtomicU64,
    lookup_known: AtomicBool,
    simulation: AtomicU64,
    fail_simulation: AtomicBool,
    block_simulation: AtomicBool,
    simulation_entered: Notify,
    mode: AtomicU64,
    block_elapsed: AtomicU64,
    receipt_entered: Notify,
    block_wallet: AtomicBool,
    wallet_entered: Notify,
    wallet_release: Notify,
    settled: Mutex<BTreeMap<B256, u64>>,
    openings: Mutex<BTreeMap<B256, SessionDescriptor>>,
    /// Holds a read of the first session's opening until released.
    block_opening: AtomicBool,
    opening_entered: Notify,
    opening_release: Notify,
    logs: Mutex<Vec<(SessionDescriptor, u64, u64)>>,
    /// Each retire call, and the earliest time each reference was marked to
    /// be forgotten from, as a wallet keeps it durably.
    retired: Mutex<Vec<(Vec<String>, std::time::SystemTime)>>,
    marks: Mutex<BTreeMap<String, std::time::SystemTime>>,
    /// When set, a lookup of a reference withdraws its mark, as every wallet
    /// must (docs/sdk-interfaces.md, Wallet requirements).
    withdraws: AtomicBool,
    /// When set, the settled amount a `getSession` read at any block but
    /// block 101 reports, as an archive read of an earlier block does; a read
    /// at block 101, once the chain has moved on to it, reports the current
    /// amount.
    historical: Mutex<Option<u64>>,
}
impl Probe {
    /// What a durable wallet's next save forgets once `now` has come.
    fn save(&self, now: std::time::SystemTime) {
        self.marks.lock().unwrap().retain(|_, at| *at > now);
    }
}
#[tonic::async_trait]
impl TransactionWallet for Probe {
    fn address(&self) -> Address {
        Address::repeat_byte(0x11)
    }
    async fn execute(
        &self,
        call: &WalletCall,
        reference: &str,
    ) -> Result<OperationResult, WalletError> {
        let number = {
            let mut calls = self.calls.lock().unwrap();
            calls.push((call.clone(), reference.into()));
            calls.len() as u64
        };
        self.wallet_entered.notify_one();
        if self.block_wallet.load(Ordering::SeqCst) {
            self.wallet_release.notified().await;
        }
        Ok(OperationResult::new(
            reference,
            OperationState::Pending,
            Some(format!("batch-wallet-{number}")),
            Some(tx(number)),
            None,
            vec![],
        )
        .unwrap())
    }
    async fn reconcile(&self, reference: &str) -> Result<OperationResult, WalletError> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        if self.withdraws.load(Ordering::SeqCst) {
            self.marks.lock().unwrap().remove(reference);
        }
        if self.lookup_known.load(Ordering::SeqCst) {
            let calls = self.calls.lock().unwrap();
            if let Some(index) = calls.iter().position(|(_, known)| known == reference) {
                let number = index as u64 + 1;
                return Ok(OperationResult::new(
                    reference,
                    OperationState::Pending,
                    Some(format!("batch-wallet-{number}")),
                    Some(tx(number)),
                    None,
                    vec![],
                )
                .unwrap());
            }
        }
        // A wallet lookup outage must not erase already returned handles.
        Ok(unknown(reference).unwrap())
    }
    async fn retire(
        &self,
        operation_refs: &[String],
        at: std::time::SystemTime,
    ) -> Result<(), WalletError> {
        self.retired
            .lock()
            .unwrap()
            .push((operation_refs.to_vec(), at));
        let mut marks = self.marks.lock().unwrap();
        for reference in operation_refs {
            let mark = marks.entry(reference.clone()).or_insert(at);
            *mark = (*mark).min(at);
        }
        Ok(())
    }
}
struct BatchRpc {
    base: Arc<TestRpc>,
    probe: Arc<Probe>,
}
#[tonic::async_trait]
impl Rpc for BatchRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        if method == "eth_getBlockByNumber" {
            let elapsed = self.probe.block_elapsed.load(Ordering::SeqCst);
            let mut block = self.base.call(method, params.clone()).await?;
            if elapsed > 0 && params[0] != "0x64" {
                block["number"] = json!("0x65");
                block["hash"] = json!(hx(B256::repeat_byte(0xbd)));
                block["parentHash"] = json!(hx(B256::repeat_byte(0xbb)));
                block["timestamp"] = json!(format!("0x{:x}", super::super::tests::TIME + elapsed));
            }
            return Ok(block);
        }
        if method == "eth_estimateGas" {
            return Ok(json!("0x186a0"));
        }
        if method == "eth_call" && params[0].get("from").is_some() {
            self.probe.simulation.fetch_add(1, Ordering::SeqCst);
            self.probe.simulation_entered.notify_one();
            if self.probe.block_simulation.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            assert_eq!(params[0]["from"], hx(Address::repeat_byte(0x11)));
            assert_eq!(params[0]["to"], hx(Address::repeat_byte(0x99)));
            if self.probe.fail_simulation.load(Ordering::SeqCst) {
                return Err(ChainError::Unavailable);
            }
            return Ok(json!("0x"));
        }
        if method == "eth_getTransactionReceipt" {
            let requested = params[0].as_str().unwrap();
            if requested == hx(B256::repeat_byte(0xaa))
                && self.probe.block_opening.load(Ordering::SeqCst)
            {
                self.probe.opening_entered.notify_one();
                self.probe.opening_release.notified().await;
            }
            let opening = self
                .probe
                .openings
                .lock()
                .unwrap()
                .iter()
                .find(|(tx, _)| requested == hx(tx))
                .map(|(_, descriptor)| descriptor.clone());
            if let Some(descriptor) = opening {
                let mut receipt = self.base.call(method, params.clone()).await?;
                let domain =
                    crate::types::Domain::new(5_042_002, Address::repeat_byte(0x99)).unwrap();
                receipt["transactionHash"] = json!(requested);
                receipt["logs"][0]["transactionHash"] = json!(requested);
                receipt["logs"][0]["topics"][1] =
                    json!(hx(domain.signing_context().session_id(&descriptor)));
                let mut data = hex::decode(
                    receipt["logs"][0]["data"]
                        .as_str()
                        .unwrap()
                        .trim_start_matches("0x"),
                )
                .unwrap();
                data[128..160].copy_from_slice(descriptor.salt.as_slice());
                receipt["logs"][0]["data"] = json!(hx(data));
                return Ok(receipt);
            }
            if (1..=16).any(|n| requested == hx(tx(n))) {
                self.probe.receipt_entered.notify_one();
                match self.probe.mode.load(Ordering::SeqCst) {
                    0 => return Ok(Value::Null),
                    3 => return Err(ChainError::Unavailable),
                    4 => std::future::pending::<()>().await,
                    _ => {}
                }
                let domain =
                    crate::types::Domain::new(5_042_002, Address::repeat_byte(0x99)).unwrap();
                let logs = self.probe.logs.lock().unwrap().iter().enumerate().map(|(index, (d, capture, delta))| {
                    json!({"address":hx(Address::repeat_byte(0x99)),
                        "topics":[hx(keccak256("Settled(bytes32,address,address,uint64,uint64,uint64,uint64)")),
                            hx(domain.signing_context().session_id(d)), hx(aword(d.agent)), hx(aword(d.recipient))],
                        "data":hx([word(100),word(*capture),word(*delta),word(*capture)].concat()),
                        "logIndex":format!("0x{index:x}"),"transactionIndex":"0x0", "transactionHash":requested,
                        "blockNumber":"0x64","blockHash":hx(B256::repeat_byte(0xbb)),"removed":false})
                }).collect::<Vec<_>>();
                return Ok(
                    json!({"transactionHash":requested,"blockHash":hx(B256::repeat_byte(0xbb)),
                    "blockNumber":"0x64","transactionIndex":"0x0",
                    "status":if self.probe.mode.load(Ordering::SeqCst)==2 {"0x0"} else {"0x1"},"logs":logs}),
                );
            }
        }
        if method == "eth_call" {
            let raw = params[0]["data"].as_str().unwrap();
            if raw.starts_with(&hx(&keccak256("getSession(bytes32)")[..4])) {
                let sid = B256::from_slice(&hex::decode(&raw[10..]).unwrap());
                let newest = params[1]["blockHash"] == json!(hx(B256::repeat_byte(0xbd)));
                let mut value = self.base.call(method, params).await?;
                let mut data =
                    hex::decode(value.as_str().unwrap().trim_start_matches("0x")).unwrap();
                let historical = *self.probe.historical.lock().unwrap();
                let settled = historical.filter(|_| !newest).unwrap_or_else(|| {
                    self.probe
                        .settled
                        .lock()
                        .unwrap()
                        .get(&sid)
                        .copied()
                        .unwrap_or(0)
                });
                data[32..64].copy_from_slice(&word(settled));
                value = json!(hx(data));
                return Ok(value);
            }
        }
        self.base.call(method, params).await
    }
}
/// The probe, stating its own call bound.
struct Bounded(Arc<Probe>, usize);
#[tonic::async_trait]
impl TransactionWallet for Bounded {
    fn address(&self) -> Address {
        self.0.address()
    }
    async fn execute(
        &self,
        call: &WalletCall,
        reference: &str,
    ) -> Result<OperationResult, WalletError> {
        self.0.execute(call, reference).await
    }
    async fn reconcile(&self, reference: &str) -> Result<OperationResult, WalletError> {
        self.0.reconcile(reference).await
    }
    fn max_call_bytes(&self) -> usize {
        self.1
    }
}
async fn fixture() -> (Fixture, Arc<Probe>) {
    fixture_with(|probe| probe).await
}
async fn fixture_with(
    wallet: impl FnOnce(Arc<Probe>) -> Arc<dyn TransactionWallet>,
) -> (Fixture, Arc<Probe>) {
    let probe = Arc::new(Probe::default());
    let rpc_probe = probe.clone();
    let fixture = Fixture::with_wallet(1000, wallet(probe.clone()), move |base| {
        Arc::new(BatchRpc {
            base,
            probe: rpc_probe,
        })
    })
    .await;
    fixture.voucher(100);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(
            &run.context,
            TokenCounts::new(0, 0, 1000).unwrap(),
            Some(RunStatus::Completed),
        )
        .unwrap();
    (fixture, probe)
}
async fn add_session(fixture: &mut Fixture, probe: &Probe) -> B256 {
    open_session(
        fixture,
        probe,
        B256::repeat_byte(0x66),
        B256::repeat_byte(0xac),
        2,
    )
    .await
}
/// Another attached session with usage to collect, opened by `opening_tx`
/// and owned by attachment `id`.
async fn open_session(
    fixture: &mut Fixture,
    probe: &Probe,
    salt: B256,
    opening_tx: B256,
    id: u64,
) -> B256 {
    let first = fixture.sid;
    let runtime = &fixture.provider.inner;
    let mut descriptor = runtime.lock().unwrap().sessions[&first]
        .opening
        .descriptor()
        .clone();
    descriptor.salt = salt;
    let sid = runtime
        .contract
        .domain()
        .signing_context()
        .session_id(&descriptor);
    probe
        .openings
        .lock()
        .unwrap()
        .insert(opening_tx, descriptor);
    runtime.chain.watch_session(sid).await.unwrap();
    let opening = runtime
        .reader
        .opening(opening_tx, sid, Some(runtime.config.catalog()))
        .await
        .unwrap();
    {
        let mut state = runtime.lock().unwrap();
        let mut session = Session::new(sid, opening, true);
        session.generation = 1;
        session.owner = Some(id);
        state.sessions.insert(sid, session);
        state.attachments.insert(
            id,
            Attachment {
                queue: Arc::new(ByteQueue::default()),
                nonce: B256::repeat_byte(0x72),
                live: true,
                sid: Some(sid),
                generation: 1,
                request: None,
                challenges: BTreeMap::new(),
                catalogs: BTreeMap::new(),
                challenge_bytes: 0,
            },
        );
    }
    fixture.sid = sid;
    fixture.voucher(100);
    let run = fixture.admit(2);
    fixture
        .provider
        .inner
        .report_sync(
            &run.context,
            TokenCounts::new(0, 0, 1000).unwrap(),
            Some(RunStatus::Completed),
        )
        .unwrap();
    let run = fixture.admit(3);
    fixture
        .provider
        .inner
        .report_sync(
            &run.context,
            TokenCounts::new(0, 0, 1000).unwrap(),
            Some(RunStatus::Completed),
        )
        .unwrap();
    fixture.sid = first;
    sid
}
fn descriptor(fixture: &Fixture, sid: B256) -> SessionDescriptor {
    fixture.provider.inner.lock().unwrap().sessions[&sid]
        .opening
        .descriptor()
        .clone()
}

#[tokio::test]
async fn ordered_batch_retains_duplicates_exact_calldata_and_shared_intent() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    let ids = [other, fixture.sid, other];
    let result = fixture.provider.settle_batch(&ids).await.unwrap();
    assert_eq!(result.state(), OperationState::Pending);
    assert_eq!(probe.simulation.load(Ordering::SeqCst), 1);
    let calls = probe.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let call = IMPPI::settleBatchCall::abi_decode(calls[0].0.data()).unwrap();
    assert_eq!(call.captureAmounts, [2, 1, 2]);
    assert_eq!(
        call.d.iter().map(|d| d.salt).collect::<Vec<_>>(),
        [
            B256::repeat_byte(0x66),
            B256::repeat_byte(0x55),
            B256::repeat_byte(0x66)
        ]
    );
    drop(calls);
    let state = fixture.provider.inner.lock().unwrap();
    let first = state.sessions[&fixture.sid].operation.as_ref().unwrap();
    let second = state.sessions[&other].operation.as_ref().unwrap();
    assert_eq!(first.prepared.session_ids(), ids);
    assert_eq!(first.units[&fixture.sid], 1000);
    assert_eq!(first.units[&other], 2000);
    assert_eq!(first.result.operation_ref(), second.result.operation_ref());
    assert!(Arc::ptr_eq(&first.prepared, &second.prepared));
    assert!(Arc::ptr_eq(&first.result, &second.result));
}

#[tokio::test]
async fn intersecting_single_and_batch_reconcile_once_without_resubmission() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    let original = fixture
        .provider
        .settle_batch(&[fixture.sid, other])
        .await
        .unwrap();
    let reversed_ids = [other, fixture.sid];
    let (single, reversed) = tokio::join!(
        fixture.provider.settle(other),
        fixture.provider.settle_batch(&reversed_ids)
    );
    for result in [single.unwrap(), reversed.unwrap()] {
        assert_eq!(result.operation_ref(), original.operation_ref());
        assert_eq!(result.tx_hash(), original.tx_hash());
        assert_eq!(result.wallet_operation_id(), original.wallet_operation_id());
    }
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
    assert_eq!(probe.lookups.load(Ordering::SeqCst), 2);
    let state = fixture.provider.inner.lock().unwrap();
    assert_eq!(
        state.sessions[&fixture.sid]
            .operation
            .as_ref()
            .unwrap()
            .result,
        state.sessions[&other].operation.as_ref().unwrap().result
    );
}

#[tokio::test]
async fn partial_success_credits_only_event_and_only_covered_baseline_then_retries_eligible_item() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    probe.mode.store(1, Ordering::SeqCst);
    probe.settled.lock().unwrap().insert(fixture.sid, 1);
    probe
        .logs
        .lock()
        .unwrap()
        .push((descriptor(&fixture, fixture.sid), 1, 1));
    let result = fixture
        .provider
        .settle_batch(&[fixture.sid, other])
        .await
        .unwrap();
    assert_eq!(result.state(), OperationState::Confirmed);
    assert_eq!(
        result.sessions()[0].settlement_reference().unwrap().delta(),
        1
    );
    assert_eq!(result.sessions()[0].target_covered(), Some(true));
    assert_eq!(result.sessions()[1].target_covered(), Some(false));
    assert!(result.sessions()[1].settlement_reference().is_none());
    {
        let state = fixture.provider.inner.lock().unwrap();
        assert_eq!(state.sessions[&fixture.sid].confirmed_units, 1000);
        assert_eq!(state.sessions[&other].confirmed_units, 0);
        assert!(state.sessions[&other].baseline.is_none());
    }
    probe.mode.store(0, Ordering::SeqCst);
    let retry = fixture.provider.settle(other).await.unwrap();
    assert_ne!(retry.operation_ref(), result.operation_ref());
    assert_eq!(probe.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn silent_covered_targets_do_not_invent_delta_and_exact_retry_is_read_only() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    probe.mode.store(1, Ordering::SeqCst);
    probe
        .settled
        .lock()
        .unwrap()
        .extend([(fixture.sid, 1), (other, 2)]);
    let first = fixture
        .provider
        .settle_batch(&[other, fixture.sid, other])
        .await
        .unwrap();
    assert_eq!(first.state(), OperationState::Unknown);
    assert!(first.observation().is_some());
    assert_eq!(first.sessions().len(), 3);
    assert!(
        first.sessions().iter().all(
            |item| item.target_covered() == Some(true) && item.settlement_reference().is_none()
        )
    );
    let repeated = fixture
        .provider
        .settle_batch(&[other, fixture.sid, other])
        .await
        .unwrap();
    assert_eq!(repeated, first);
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&other].confirmed_units,
        2000
    );
}

#[tokio::test]
async fn silent_uncovered_execution_remains_unpaid_but_can_prepare_a_current_retry() {
    let (fixture, probe) = fixture().await;
    probe.mode.store(1, Ordering::SeqCst);
    let first = fixture.provider.settle_batch(&[fixture.sid]).await.unwrap();
    assert_eq!(first.state(), OperationState::Unknown);
    assert!(first.observation().is_some());
    assert_eq!(first.sessions()[0].target_covered(), Some(false));
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].confirmed_units,
        0
    );
    probe.mode.store(0, Ordering::SeqCst);
    let second = fixture.provider.settle_batch(&[fixture.sid]).await.unwrap();
    assert_ne!(second.operation_ref(), first.operation_ref());
    assert_eq!(probe.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn simulation_failure_is_not_exported_as_wallet_intent_and_allows_safe_retry() {
    let (fixture, probe) = fixture().await;
    probe.fail_simulation.store(true, Ordering::SeqCst);
    assert!(fixture.provider.settle_batch(&[fixture.sid]).await.is_err());
    assert!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid]
            .operation
            .is_none()
    );
    assert!(probe.calls.lock().unwrap().is_empty());
    probe.fail_simulation.store(false, Ordering::SeqCst);
    let retry = fixture.provider.settle_batch(&[fixture.sid]).await.unwrap();
    assert_eq!(retry.state(), OperationState::Pending);
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn caller_cancellation_leaves_owned_batch_running_and_retains_returned_handles() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    probe.block_wallet.store(true, Ordering::SeqCst);
    let sdk = fixture.provider.clone();
    let sid = fixture.sid;
    let caller = tokio::spawn(async move { sdk.settle_batch(&[sid, other]).await });
    probe.wallet_entered.notified().await;
    caller.abort();
    let _ = caller.await;
    probe.wallet_release.notify_one();
    probe.receipt_entered.notified().await;
    let _operations = fixture.provider.inner.operations.lock().await;
    let state = fixture.provider.inner.lock().unwrap();
    for sid in [fixture.sid, other] {
        let operation = state.sessions[&sid].operation.as_ref().unwrap();
        assert_eq!(operation.result.tx_hash(), Some(tx(1)));
        assert_eq!(
            operation.result.wallet_operation_id(),
            Some("batch-wallet-1")
        );
    }
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn shutdown_deduplicates_shared_pending_batch_and_retains_empty_batch() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    let payment = fixture
        .provider
        .settle_batch(&[fixture.sid, other])
        .await
        .unwrap();
    let empty = fixture.provider.settle_batch(&[]).await.unwrap();
    let report = fixture
        .provider
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(report.pending_operations.len(), 2);
    assert!(
        report
            .pending_operations
            .iter()
            .any(|item| item.operation_ref() == payment.operation_ref())
    );
    assert!(
        report
            .pending_operations
            .iter()
            .any(|item| item.operation_ref() == empty.operation_ref())
    );
    assert_eq!(probe.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn shutdown_during_chain_read_keeps_batch_wallet_identity_on_every_member() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    probe.mode.store(4, Ordering::SeqCst);
    let sdk = fixture.provider.clone();
    let sid = fixture.sid;
    let caller = tokio::spawn(async move { sdk.settle_batch(&[sid, other]).await });
    probe.receipt_entered.notified().await;
    let report = fixture
        .provider
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(caller.await.unwrap().is_err());
    assert_eq!(report.pending_operations.len(), 1);
    assert_eq!(report.pending_operations[0].tx_hash(), Some(tx(1)));
    assert_eq!(
        report.pending_operations[0].wallet_operation_id(),
        Some("batch-wallet-1")
    );
    let state = fixture.provider.inner.lock().unwrap();
    for sid in [fixture.sid, other] {
        assert_eq!(
            state.sessions[&sid]
                .operation
                .as_ref()
                .unwrap()
                .result
                .tx_hash(),
            Some(tx(1))
        );
    }
}

#[tokio::test]
async fn empty_success_is_silent_retained_and_repeated_without_another_wallet_call() {
    let (fixture, probe) = fixture().await;
    probe.mode.store(1, Ordering::SeqCst);
    let first = fixture.provider.settle_batch(&[]).await.unwrap();
    assert_eq!(first.state(), OperationState::Unknown);
    assert!(first.observation().is_some());
    assert!(first.sessions().is_empty());
    assert_eq!(fixture.provider.settle_batch(&[]).await.unwrap(), first);
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn resource_limit_and_unknown_session_reject_before_simulation_and_wallet() {
    let (fixture, probe) = fixture().await;
    assert!(
        fixture
            .provider
            .settle_batch(&vec![fixture.sid; wire::MAX_MESSAGE_BYTES / 512 + 1])
            .await
            .is_err()
    );
    assert!(
        fixture
            .provider
            .settle_batch(&[B256::repeat_byte(0xfe)])
            .await
            .is_err()
    );
    assert_eq!(probe.simulation.load(Ordering::SeqCst), 0);
    assert!(probe.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn reconciling_one_member_confirms_all_members_using_original_usage_snapshot() {
    let (mut fixture, probe) = fixture().await;
    let (settled, settlement_events) = std::sync::mpsc::channel();
    Arc::get_mut(&mut fixture.provider.inner).unwrap().observers = hooks::Observers::new(Hooks {
        on_settle: Some(Arc::new(move |result| settled.send(result).unwrap())),
        ..Hooks::default()
    });
    let other = add_session(&mut fixture, &probe).await;
    let original = fixture
        .provider
        .settle_batch(&[fixture.sid, other])
        .await
        .unwrap();
    assert_ne!(original.state(), OperationState::Confirmed);
    assert!(
        settlement_events.try_recv().is_err(),
        "pending custody is not a settlement event"
    );
    let run = fixture.admit(4);
    fixture
        .provider
        .inner
        .report_sync(
            &run.context,
            TokenCounts::new(0, 0, 1000).unwrap(),
            Some(RunStatus::Completed),
        )
        .unwrap();
    probe
        .settled
        .lock()
        .unwrap()
        .extend([(fixture.sid, 1), (other, 2)]);
    probe.logs.lock().unwrap().extend([
        (descriptor(&fixture, fixture.sid), 1, 1),
        (descriptor(&fixture, other), 2, 2),
    ]);
    probe.mode.store(1, Ordering::SeqCst);
    let result = fixture.provider.settle(other).await.unwrap();
    assert_eq!(result.operation_ref(), original.operation_ref());
    assert_eq!(result.state(), OperationState::Confirmed);
    let observed = settlement_events
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        observed, result,
        "hook must carry the canonically reconciled result"
    );
    assert_eq!(observed.tx_hash(), original.tx_hash());
    assert_eq!(
        observed.wallet_operation_id(),
        original.wallet_operation_id()
    );
    assert_eq!(observed.observation().unwrap().block_number(), 100);
    assert_eq!(
        observed
            .sessions()
            .iter()
            .map(|item| item.session_id())
            .collect::<Vec<_>>(),
        vec![fixture.sid, other]
    );
    assert!(
        observed.sessions().iter().all(
            |item| item.target_covered() == Some(true) && item.settlement_reference().is_some()
        )
    );
    let state = fixture.provider.inner.lock().unwrap();
    assert_eq!(state.sessions[&fixture.sid].units().unwrap(), 2000);
    assert_eq!(state.sessions[&fixture.sid].confirmed_units, 1000);
    assert_eq!(state.sessions[&other].confirmed_units, 2000);
    assert_eq!(
        state.sessions[&fixture.sid]
            .operation
            .as_ref()
            .unwrap()
            .result
            .as_ref(),
        &result
    );
    assert_eq!(
        state.sessions[&other]
            .operation
            .as_ref()
            .unwrap()
            .result
            .as_ref(),
        &result
    );
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn uncovered_batch_cannot_retry_after_collection_deadline() {
    let (fixture, probe) = fixture().await;
    probe.mode.store(1, Ordering::SeqCst);
    let first = fixture.provider.settle_batch(&[fixture.sid]).await.unwrap();
    assert_eq!(first.sessions()[0].target_covered(), Some(false));
    probe.block_elapsed.store(86401, Ordering::SeqCst);
    fixture.advance_clock(86401 * 1000);
    fixture.provider.inner.chain.refresh().await.unwrap();
    let error = fixture
        .provider
        .settle_batch(&[fixture.sid])
        .await
        .unwrap_err();
    let failure = wire::parse_payment_error(error.downcast_ref::<Status>().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(failure.reason(), Some("CLAIM_DEADLINE"));
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
    assert_eq!(probe.simulation.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid]
            .usage()
            .unwrap(),
        1
    );
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].confirmed_units,
        0
    );
}

#[tokio::test]
async fn close_reconciles_intersecting_batch_without_claiming_close_completion() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    let original = fixture
        .provider
        .settle_batch(&[fixture.sid, other])
        .await
        .unwrap();
    let closing = fixture.provider.close(other).await.unwrap();
    assert_eq!(closing.state(), OperationState::Unknown);
    assert_ne!(closing.operation_ref(), original.operation_ref());
    assert!(closing.tx_hash().is_none());
    let state = fixture.provider.inner.lock().unwrap();
    assert!(state.sessions[&other].close_pending);
    assert!(state.sessions[&other].close_result.is_none());
    assert_eq!(
        state.sessions[&other]
            .operation
            .as_ref()
            .unwrap()
            .result
            .operation_ref(),
        original.operation_ref()
    );
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn shutdown_during_simulation_discards_known_unsubmitted_batch() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    probe.block_simulation.store(true, Ordering::SeqCst);
    let sdk = fixture.provider.clone();
    let sid = fixture.sid;
    let caller = tokio::spawn(async move { sdk.settle_batch(&[sid, other]).await });
    probe.simulation_entered.notified().await;
    let report = fixture
        .provider
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(caller.await.unwrap().is_err());
    assert!(probe.calls.lock().unwrap().is_empty());
    assert!(
        report.pending_operations.is_empty(),
        "simulation cannot have submitted a wallet operation"
    );
    let state = fixture.provider.inner.lock().unwrap();
    for sid in [fixture.sid, other] {
        assert!(state.sessions[&sid].operation.is_none());
    }
}

#[tokio::test]
async fn recovery_rejects_omitted_pending_batch_identity_retained_by_another_member() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    let pending = fixture
        .provider
        .settle_batch(&[fixture.sid, other])
        .await
        .unwrap();
    fixture
        .provider
        .inner
        .detach_locked(&mut fixture.provider.inner.lock().unwrap(), 1);
    let mut evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    evidence.operation = None;
    // Omission in the target session cannot hide another retained member's
    // unresolved intent for the same funds and ordered capture targets.
    fixture
        .provider
        .inner
        .lock()
        .unwrap()
        .sessions
        .get_mut(&fixture.sid)
        .unwrap()
        .operation = None;
    let before = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    let other_before = fixture.provider.checkpoint(other).await.unwrap();
    let error = fixture.provider.recover(evidence).await.unwrap_err();
    let failure = wire::parse_payment_error(error.downcast_ref::<Status>().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(failure.reason(), Some("STATE_UNAVAILABLE"));
    let state = fixture.provider.inner.lock().unwrap();
    assert!(state.sessions[&fixture.sid].operation.is_none());
    assert_eq!(
        state.sessions[&fixture.sid].generation,
        before.snapshot.control_generation.unwrap()
    );
    assert_eq!(
        state.sessions[&other].generation,
        other_before.snapshot.control_generation.unwrap()
    );
    assert_eq!(
        state.sessions[&other]
            .operation
            .as_ref()
            .unwrap()
            .result
            .operation_ref(),
        pending.operation_ref()
    );
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
}

fn due(fixture: &Fixture) {
    let mut state = fixture.provider.inner.lock().unwrap();
    for session in state.sessions.values_mut() {
        session.last_collection = Instant::now() - Duration::from_secs(60);
    }
}
async fn wait_for_maintenance(fixture: &Fixture) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if fixture.provider.inner.lock().unwrap().collecting.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn maintenance_groups_due_sessions_into_one_ordered_batch() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    due(&fixture);
    fixture.provider.inner.maintain_once();
    wait_for_maintenance(&fixture).await;
    let calls = probe.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let batch = IMPPI::settleBatchCall::abi_decode(calls[0].0.data()).unwrap();
    assert_eq!(batch.d.len(), 2);
    let mut ids = [fixture.sid, other];
    ids.sort();
    let state = fixture.provider.inner.lock().unwrap();
    assert_eq!(
        state.sessions[&fixture.sid]
            .operation
            .as_ref()
            .unwrap()
            .prepared
            .session_ids(),
        ids
    );
    assert_eq!(probe.simulation.load(Ordering::SeqCst), 1);
    assert_eq!(
        state.sessions[&fixture.sid]
            .operation
            .as_ref()
            .unwrap()
            .result
            .operation_ref(),
        state.sessions[&other]
            .operation
            .as_ref()
            .unwrap()
            .result
            .operation_ref()
    );
}

#[tokio::test]
async fn maintenance_splits_known_unsubmitted_simulation_failure_into_eligible_single_calls() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    probe.fail_simulation.store(true, Ordering::SeqCst);
    due(&fixture);
    fixture.provider.inner.maintain_once();
    wait_for_maintenance(&fixture).await;
    let calls = probe.calls.lock().unwrap();
    assert_eq!(probe.simulation.load(Ordering::SeqCst), 1);
    assert_eq!(calls.len(), 2);
    let mut targets = calls
        .iter()
        .map(|(call, _)| {
            IMPPI::settleCall::abi_decode(call.data())
                .unwrap()
                .captureAmount
        })
        .collect::<Vec<_>>();
    targets.sort();
    assert_eq!(targets, [1, 2]);
    assert_ne!(calls[0].1, calls[1].1);
    let state = fixture.provider.inner.lock().unwrap();
    for sid in [fixture.sid, other] {
        let operation = state.sessions[&sid].operation.as_ref().unwrap();
        assert_eq!(operation.prepared.kind(), OperationKind::Settle);
        assert_eq!(operation.result.state(), OperationState::Pending);
        assert_eq!(state.sessions[&sid].confirmed_units, 0);
    }
}

#[tokio::test]
async fn recovery_preserves_failed_close_identity_and_a_repeated_close_waits_its_interval() {
    let (fixture, probe) = fixture().await;
    probe.lookup_known.store(true, Ordering::SeqCst);
    probe.mode.store(2, Ordering::SeqCst);
    let original = fixture.provider.close(fixture.sid).await.unwrap();
    assert_eq!(original.state(), OperationState::Failed);
    fixture
        .provider
        .inner
        .detach_locked(&mut fixture.provider.inner.lock().unwrap(), 1);
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    let mut omitted = evidence.clone();
    omitted.operation = None;
    assert!(fixture.provider.recover(omitted).await.is_err());
    let mut changed = evidence.clone();
    changed.operation.as_mut().unwrap().result = OperationResult::new(
        "close:replacement",
        OperationState::Failed,
        original.wallet_operation_id().map(str::to_owned),
        original.tx_hash(),
        original.observation(),
        original.sessions().to_vec(),
    )
    .unwrap();
    assert!(fixture.provider.recover(changed).await.is_err());
    fixture.provider.recover(evidence).await.unwrap();
    assert_eq!(fixture.provider.close(fixture.sid).await.unwrap(), original);
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
}

/// A runtime restarted over the same chain, wallet and configuration.
fn restarted(fixture: &Fixture) -> Mppi {
    let source = &fixture.provider.inner;
    let provider = Mppi::new(
        source.config.clone(),
        source.chain.clone(),
        source.contract.clone(),
        source.wallet.clone(),
        source.coverage.clone(),
        source.execution.clone(),
    )
    .unwrap();
    {
        let mut state = provider.inner.lock().unwrap();
        state.started = true;
        state.start_block = Some(99);
    }
    provider
}
/// The session's last collection one settlement interval ago.
fn interval_passed(provider: &Mppi, sid: B256) {
    let interval = Duration::from_millis(provider.inner.config.settlement().interval_ms());
    let mut state = provider.inner.lock().unwrap();
    state.sessions.get_mut(&sid).unwrap().last_collection = Instant::now() - interval;
}

#[tokio::test]
async fn a_close_mined_and_reverted_is_retried_under_its_successor_reference() {
    let (fixture, probe) = fixture().await;
    probe.lookup_known.store(true, Ordering::SeqCst);
    probe.mode.store(2, Ordering::SeqCst); // Its receipt shows a revert.
    let reverted = fixture.provider.close(fixture.sid).await.unwrap();
    assert_eq!(reverted.state(), OperationState::Failed);
    assert!(reverted.tx_hash().is_some());
    let first = reverted.operation_ref().to_owned();
    fixture
        .provider
        .inner
        .detach_locked(&mut fixture.provider.inner.lock().unwrap(), 1);
    let before = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    // At most once per settlement interval, by a call or by maintenance.
    assert_eq!(fixture.provider.close(fixture.sid).await.unwrap(), reverted);
    assert!(fixture.provider.collections_due().unwrap().is_empty());
    interval_passed(&fixture.provider, fixture.sid);
    assert_eq!(
        fixture.provider.collections_due().unwrap(),
        vec![(fixture.sid, true)]
    );
    fixture.provider.collection_finished(fixture.sid);
    probe.mode.store(0, Ordering::SeqCst); // The retry is still pending.
    let retried = fixture.provider.close(fixture.sid).await.unwrap();
    let successor = format!("{first}.1");
    assert_eq!(retried.operation_ref(), successor);
    assert_eq!(retried.state(), OperationState::Pending);
    let calls = probe.calls.lock().unwrap().clone();
    assert_eq!(
        calls
            .iter()
            .map(|(_, reference)| reference.as_str())
            .collect::<Vec<_>>(),
        [first.as_str(), successor.as_str()]
    );
    // The close intent names its current reference, so its checkpoint recovers.
    let after = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    assert_eq!(after.close_reference.as_deref(), Some(successor.as_str()));
    assert_eq!(
        after.operation.as_ref().unwrap().result.operation_ref(),
        successor
    );
    restarted(&fixture).recover(after).await.unwrap();
    // A runtime restored from a checkpoint older than the retry derives the
    // same successor, so the wallet's binding of it continues that retry.
    probe.mode.store(2, Ordering::SeqCst);
    let older = restarted(&fixture);
    older.recover(before).await.unwrap();
    interval_passed(&older, fixture.sid);
    probe.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        older.close(fixture.sid).await.unwrap().operation_ref(),
        successor
    );
    assert_eq!(probe.calls.lock().unwrap()[2].1, successor);
}

/// A close mined and reverted is retried only once its successor simulates.
/// While the simulation fails, as it would for a close that keeps reverting,
/// the runtime records no intent and sends nothing, and the next attempt waits
/// its interval; once the simulation passes, the successor is sent.
#[tokio::test]
async fn a_reverted_close_is_sent_again_only_when_its_successor_simulates() {
    let (fixture, probe) = fixture().await;
    let sid = fixture.sid;
    probe.lookup_known.store(true, Ordering::SeqCst);
    probe.mode.store(2, Ordering::SeqCst); // Its receipt shows a revert.
    let reverted = fixture.provider.close(sid).await.unwrap();
    assert_eq!(reverted.state(), OperationState::Failed);
    let first = reverted.operation_ref().to_owned();
    let successor = format!("{first}.1");
    probe.fail_simulation.store(true, Ordering::SeqCst);
    for _ in 0..3 {
        interval_passed(&fixture.provider, sid);
        let simulated = probe.simulation.load(Ordering::SeqCst);
        assert!(fixture.provider.close(sid).await.is_err());
        assert_eq!(probe.simulation.load(Ordering::SeqCst), simulated + 1);
        {
            let state = fixture.provider.inner.lock().unwrap();
            let session = &state.sessions[&sid];
            assert_eq!(session.close_reference.as_deref(), Some(first.as_str()));
            assert_eq!(
                session.operation.as_ref().unwrap().result.as_ref(),
                &reverted
            );
            assert!(!state.references.contains_key(&successor));
        }
        // The attempt counts: the next waits its interval, by a call or by
        // maintenance.
        assert_eq!(fixture.provider.close(sid).await.unwrap(), reverted);
        assert!(fixture.provider.collections_due().unwrap().is_empty());
    }
    assert_eq!(
        probe.calls.lock().unwrap().len(),
        1,
        "a failing simulation sent the close"
    );
    probe.fail_simulation.store(false, Ordering::SeqCst);
    probe.mode.store(0, Ordering::SeqCst); // The retry is still pending.
    interval_passed(&fixture.provider, sid);
    let simulated = probe.simulation.load(Ordering::SeqCst);
    let retried = fixture.provider.close(sid).await.unwrap();
    assert_eq!(retried.operation_ref(), successor);
    assert_eq!(retried.state(), OperationState::Pending);
    assert_eq!(probe.simulation.load(Ordering::SeqCst), simulated + 1);
    let calls = probe.calls.lock().unwrap().clone();
    assert_eq!(
        calls
            .iter()
            .map(|(_, reference)| reference.as_str())
            .collect::<Vec<_>>(),
        [first.as_str(), successor.as_str()]
    );
}

/// A capture whose wallet lookup lost its transaction hash is still confirmed
/// from the chain by the hash this process kept, so no wallet answer in this
/// process matches its record. Its hand-over takes recovery's wallet check: a
/// wallet that still cannot show that hash keeps the record; one whose record
/// agrees again lets it go.
#[tokio::test]
async fn a_reference_whose_last_wallet_answer_disagreed_is_retired_only_if_the_wallet_agrees() {
    for agrees in [false, true] {
        let (mut fixture, probe) = fixture().await;
        let sid = fixture.sid;
        probe.mode.store(0, Ordering::SeqCst); // No receipt yet.
        let pending = fixture.provider.settle(sid).await.unwrap();
        assert_eq!(pending.state(), OperationState::Pending);
        // The wallet's lookup shows no hash; the chain confirms the one kept.
        settles(&fixture, &probe, &[(sid, 1, 1)]);
        let first = fixture.provider.settle(sid).await.unwrap();
        assert_eq!(first.state(), OperationState::Confirmed);
        assert_eq!(first.tx_hash(), pending.tx_hash());
        // A later capture supersedes it, and maintenance hands it over.
        spend(&mut fixture, sid, 4);
        settles(&fixture, &probe, &[(sid, 2, 1)]);
        let later = fixture.provider.settle(sid).await.unwrap();
        assert_eq!(later.state(), OperationState::Confirmed);
        probe.lookup_known.store(agrees, Ordering::SeqCst);
        let lookups = probe.lookups.load(Ordering::SeqCst);
        retire_pass(&fixture.provider).await;
        let retired = probe
            .retired
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(references, _)| references.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            retired.iter().any(|known| known == first.operation_ref()),
            agrees,
            "agrees {agrees}"
        );
        assert!(
            probe.lookups.load(Ordering::SeqCst) > lookups,
            "agrees {agrees}: the wallet was not asked"
        );
    }
}

/// The probe, whose next `retire` call waits until released, as a slow
/// wallet's call is under way.
struct Pausing {
    probe: Arc<Probe>,
    pause: AtomicBool,
    entered: Notify,
    release: Notify,
}
#[tonic::async_trait]
impl TransactionWallet for Pausing {
    fn address(&self) -> Address {
        self.probe.address()
    }
    async fn execute(
        &self,
        call: &WalletCall,
        reference: &str,
    ) -> Result<OperationResult, WalletError> {
        self.probe.execute(call, reference).await
    }
    async fn reconcile(&self, reference: &str) -> Result<OperationResult, WalletError> {
        self.probe.reconcile(reference).await
    }
    async fn retire(
        &self,
        operation_refs: &[String],
        at: std::time::SystemTime,
    ) -> Result<(), WalletError> {
        if self.pause.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.probe.retire(operation_refs, at).await
    }
}

/// A reference handed over for the wallet's `retire`, still queued (as a
/// refused call leaves it, for a settlement interval) or in a `retire` task
/// already under way, is not passed to `retire` once a recovery has installed a
/// session that names it again: the install takes it off the queue, and the
/// task checks again, just before each wallet call, that no session needs it.
#[tokio::test]
async fn a_recovery_naming_a_reference_due_for_retire_keeps_it_from_retire() {
    for in_flight in [false, true] {
        let mut pausing = None;
        let (fixture, probe) = fixture_with(|probe| {
            let wallet = Arc::new(Pausing {
                probe,
                pause: AtomicBool::new(false),
                entered: Notify::new(),
                release: Notify::new(),
            });
            pausing = Some(wallet.clone());
            wallet
        })
        .await;
        let pausing = pausing.unwrap();
        let sid = fixture.sid;
        settles(&fixture, &probe, &[(sid, 1, 1)]);
        let first = fixture.provider.settle(sid).await.unwrap();
        assert_eq!(first.state(), OperationState::Confirmed);
        let reference = first.operation_ref().to_owned();
        fixture
            .provider
            .inner
            .detach_locked(&mut fixture.provider.inner.lock().unwrap(), 1);
        let evidence = fixture.provider.checkpoint(sid).await.unwrap();
        // Handed over, as a supersede followed by a refused call leaves it.
        let due = std::time::SystemTime::now() + DAY;
        {
            let mut state = fixture.provider.inner.lock().unwrap();
            state.references.remove(&reference);
            if in_flight {
                // An earlier second's call is under way as the recovery installs.
                state
                    .retirable
                    .push(("settle:earlier".into(), due - Duration::from_secs(10)));
            }
            state.retirable.push((reference.clone(), due));
        }
        if in_flight {
            pausing.pause.store(true, Ordering::SeqCst);
            fixture.provider.inner.maintain_once();
            pausing.entered.notified().await;
        }
        probe.lookup_known.store(true, Ordering::SeqCst);
        fixture.provider.recover(evidence).await.unwrap();
        assert!(
            fixture
                .provider
                .inner
                .lock()
                .unwrap()
                .references
                .contains_key(&reference),
            "the recovery marks it needed again"
        );
        pausing.release.notify_one();
        while fixture.provider.inner.lock().unwrap().retiring {
            tokio::task::yield_now().await;
        }
        retire_pass(&fixture.provider).await;
        let retired = probe
            .retired
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(references, _)| references.clone())
            .collect::<Vec<_>>();
        assert!(
            !retired.contains(&reference),
            "in flight {in_flight}: a reference the installed session names was handed to retire"
        );
        assert_eq!(
            retired.iter().any(|known| known == "settle:earlier"),
            in_flight
        );
    }
}

#[test]
fn a_close_reference_counts_its_retries() {
    for (reference, next) in [
        ("close:ab", "close:ab.1"),
        ("close:ab.1", "close:ab.2"),
        ("close:ab.9", "close:ab.10"),
        ("close:ab.01", "close:ab.01.1"),
        ("close:ab.+1", "close:ab.+1.1"),
        ("close:ab.x", "close:ab.x.1"),
        (
            "close:ab.18446744073709551615",
            "close:ab.18446744073709551615.1",
        ),
    ] {
        assert_eq!(super::successor(reference), next);
    }
}

/// The next collection's receipt shows these `(session, capture, delta)`
/// settlements, and the chain the captures as settled.
fn settles(fixture: &Fixture, probe: &Probe, items: &[(B256, u64, u64)]) {
    probe.mode.store(1, Ordering::SeqCst);
    let mut settled = probe.settled.lock().unwrap();
    let mut logs = probe.logs.lock().unwrap();
    logs.clear();
    for (sid, capture, delta) in items {
        settled.insert(*sid, *capture);
        logs.push((descriptor(fixture, *sid), *capture, *delta));
    }
}
/// One more completed run of `sid`, adding one unit of usage.
fn spend(fixture: &mut Fixture, sid: B256, run: u8) {
    let first = std::mem::replace(&mut fixture.sid, sid);
    let handle = fixture.admit(run);
    fixture
        .provider
        .inner
        .report_sync(
            &handle.context,
            TokenCounts::new(0, 0, 1000).unwrap(),
            Some(RunStatus::Completed),
        )
        .unwrap();
    fixture.sid = first;
}
/// One maintenance pass, then the retire call it started, if any, has ended.
async fn retire_pass(provider: &Mppi) {
    provider.inner.maintain_once();
    while provider.inner.lock().unwrap().retiring {
        tokio::task::yield_now().await;
    }
}
const DAY: Duration = Duration::from_secs(86400);

#[tokio::test]
async fn a_reference_superseded_by_a_later_confirmed_collection_is_handed_over_due_a_day_later() {
    let (mut fixture, probe) = fixture().await;
    let sid = fixture.sid;
    settles(&fixture, &probe, &[(sid, 1, 1)]);
    let first = fixture.provider.settle(sid).await.unwrap();
    assert_eq!(first.state(), OperationState::Confirmed);
    spend(&mut fixture, sid, 4);
    settles(&fixture, &probe, &[(sid, 2, 1)]);
    let before = std::time::SystemTime::now();
    let second = fixture.provider.settle(sid).await.unwrap();
    let after = std::time::SystemTime::now();
    assert_eq!(second.state(), OperationState::Confirmed);
    // Handed over at once, due closed_retention after it was superseded.
    retire_pass(&fixture.provider).await;
    let calls = probe.retired.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, [first.operation_ref()]);
    assert!((before + DAY..=after + DAY).contains(&calls[0].1));
    let state = fixture.provider.inner.lock().unwrap();
    assert_eq!(
        state.references.keys().collect::<Vec<_>>(),
        [second.operation_ref()]
    );
}

#[tokio::test]
async fn a_restart_before_a_superseded_reference_falls_due_keeps_its_mark() {
    let (mut fixture, probe) = fixture().await;
    let sid = fixture.sid;
    settles(&fixture, &probe, &[(sid, 1, 1)]);
    let first = fixture.provider.settle(sid).await.unwrap();
    spend(&mut fixture, sid, 4);
    settles(&fixture, &probe, &[(sid, 2, 1)]);
    let second = fixture.provider.settle(sid).await.unwrap();
    retire_pass(&fixture.provider).await;
    let at = probe.marks.lock().unwrap()[first.operation_ref()];
    // The process stops before the mark falls due; the next one recovers the
    // session from its current checkpoint, which names only the later capture.
    let evidence = fixture.provider.checkpoint(sid).await.unwrap();
    probe.lookup_known.store(true, Ordering::SeqCst);
    let provider = restarted(&fixture);
    provider.recover(evidence).await.unwrap();
    for _ in 0..3 {
        retire_pass(&provider).await;
    }
    {
        let state = provider.inner.lock().unwrap();
        assert_eq!(
            state.references.keys().collect::<Vec<_>>(),
            [second.operation_ref()]
        );
        assert!(state.retirable.is_empty());
    }
    assert_eq!(
        probe.retired.lock().unwrap().len(),
        1,
        "the reference was learned again"
    );
    // The wallet kept the mark it was given before the restart, so its record
    // still goes when that time comes. (The Circle adapter keeps it in its
    // journal: the next test.)
    probe.save(at - Duration::from_secs(1));
    assert!(
        probe
            .marks
            .lock()
            .unwrap()
            .contains_key(first.operation_ref())
    );
    probe.save(at);
    assert!(probe.marks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_superseded_reference_marked_in_the_circle_journal_is_forgotten_after_a_restart() {
    use crate::circle::Clock as _;
    use std::time::{SystemTime, UNIX_EPOCH};
    let seconds = |time: SystemTime| time.duration_since(UNIX_EPOCH).unwrap().as_secs();
    // The runtime collects through the real Circle adapter, which drives the
    // adapter tests' fake CLI on its own journal and reads their clock.
    let circle = crate::circle::tests::Fixture::new(5_042_002);
    circle.service.distinct.store(true, Ordering::SeqCst);
    let wallet: Arc<dyn TransactionWallet> = Arc::new(circle.wallet(false, false).unwrap());
    let adapter = Arc::downgrade(&wallet);
    let (mut fixture, probe) = fixture_with(|_| wallet).await;
    let sid = fixture.sid;
    *circle.service.hash.lock().unwrap() = Some(tx(1));
    settles(&fixture, &probe, &[(sid, 1, 1)]);
    let first = fixture.provider.settle(sid).await.unwrap();
    spend(&mut fixture, sid, 4);
    *circle.service.hash.lock().unwrap() = Some(tx(2));
    settles(&fixture, &probe, &[(sid, 2, 1)]);
    let before = SystemTime::now();
    let second = fixture.provider.settle(sid).await.unwrap();
    let after = SystemTime::now();
    assert_eq!(
        (first.state(), second.state()),
        (OperationState::Confirmed, OperationState::Confirmed)
    );
    // The first capture is handed over due a day later, and the adapter
    // keeps that time in its journal.
    retire_pass(&fixture.provider).await;
    let operations = &circle.row()["operations"];
    let mark = operations[first.operation_ref()]["retire_at"]
        .as_u64()
        .unwrap();
    assert!((seconds(before + DAY)..=seconds(after + DAY)).contains(&mark));
    assert_eq!(operations[second.operation_ref()]["retire_at"], Value::Null);
    // The process stops before the mark falls due: its runtime and adapter
    // are gone.
    let evidence = fixture.provider.checkpoint(sid).await.unwrap();
    let stopped = fixture.provider;
    let source = &stopped.inner;
    let (config, chain, contract, coverage, execution) = (
        source.config.clone(),
        source.chain.clone(),
        source.contract.clone(),
        source.coverage.clone(),
        source.execution.clone(),
    );
    drop(stopped);
    assert!(
        adapter.upgrade().is_none(),
        "the adapter outlived its runtime"
    );
    // The next process opens a new adapter on the same journal.
    let provider = Mppi::new(
        config,
        chain,
        contract,
        Arc::new(circle.wallet(false, true).unwrap()),
        coverage,
        execution,
    )
    .unwrap();
    {
        let mut state = provider.inner.lock().unwrap();
        state.started = true;
        state.start_block = Some(99);
    }
    // Once the adapter's clock reaches the mark, recovering the session from
    // its current checkpoint, which names only the later capture, looks that
    // capture up in the adapter, whose save forgets the first.
    let reached = UNIX_EPOCH + Duration::from_secs(mark);
    circle
        .clock
        .advance(reached.duration_since(circle.clock.wall()).unwrap());
    provider.recover(evidence).await.unwrap();
    for _ in 0..3 {
        retire_pass(&provider).await;
    }
    {
        let state = provider.inner.lock().unwrap();
        assert_eq!(
            state.references.keys().collect::<Vec<_>>(),
            [second.operation_ref()]
        );
        assert!(state.retirable.is_empty());
    }
    let kept = circle.row()["operations"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(kept, [second.operation_ref()]);
}

/// The runtime stops and the next process starts on the same configuration,
/// chain and collaborators, with `wallet`: a new adapter on the same journal,
/// or the same wallet.
fn restart(fixture: &mut Fixture, wallet: Arc<dyn TransactionWallet>, limits: &RuntimeLimits) {
    let source = &fixture.provider.inner;
    let provider = Mppi::with_limits(
        source.config.clone(),
        source.chain.clone(),
        source.contract.clone(),
        wallet,
        source.coverage.clone(),
        source.execution.clone(),
        limits.clone(),
    )
    .unwrap();
    {
        let mut state = provider.inner.lock().unwrap();
        state.started = true;
        state.start_block = Some(99);
    }
    drop(std::mem::replace(&mut fixture.provider, provider));
}
/// The session's agent attached again, as the fixture's attachment 1, so its
/// runs are admitted.
fn reattach(fixture: &Fixture) {
    let mut state = fixture.provider.inner.lock().unwrap();
    let session = state.sessions.get_mut(&fixture.sid).unwrap();
    session.generation = 1;
    session.owner = Some(1);
    state.attachments.insert(
        1,
        Attachment {
            queue: Arc::new(ByteQueue::default()),
            nonce: B256::repeat_byte(0x71),
            live: true,
            sid: Some(fixture.sid),
            generation: 1,
            request: None,
            challenges: BTreeMap::new(),
            catalogs: BTreeMap::new(),
            challenge_bytes: 0,
        },
    );
}

/// A host checkpoint taken before a later confirmed collection superseded
/// its capture names that capture again when the session is recovered from
/// it: the wallet lookup withdraws the capture's retirement mark, so the
/// session's checkpoint still recovers after the time it was marked for, and
/// a collection that supersedes it again hands it over again.
async fn a_recovery_from_an_older_checkpoint_withdraws_the_mark(retention: Duration) {
    use crate::circle::{Clock as _, tests::TestClock};
    use std::time::{SystemTime, UNIX_EPOCH};
    let limits = RuntimeLimits::default()
        .with_closed_retention(retention)
        .unwrap();
    let circle = crate::circle::tests::Fixture::new(5_042_002);
    circle.service.distinct.store(true, Ordering::SeqCst);
    let wallet: Arc<dyn TransactionWallet> = Arc::new(circle.wallet(false, false).unwrap());
    let (mut fixture, probe) = fixture_with(|_| wallet).await;
    Arc::get_mut(&mut fixture.provider.inner)
        .expect("the only handle to the runtime")
        .limits = limits.clone();
    let sid = fixture.sid;
    let journal = || circle.row()["operations"].clone();
    *circle.service.hash.lock().unwrap() = Some(tx(1));
    settles(&fixture, &probe, &[(sid, 1, 1)]);
    let first = fixture.provider.settle(sid).await.unwrap();
    spend(&mut fixture, sid, 4);
    // The host's last saved checkpoint names the first capture.
    let older = fixture.provider.checkpoint(sid).await.unwrap();
    *circle.service.hash.lock().unwrap() = Some(tx(2));
    settles(&fixture, &probe, &[(sid, 2, 1)]);
    let second = fixture.provider.settle(sid).await.unwrap();
    assert_eq!(
        (first.state(), second.state()),
        (OperationState::Confirmed, OperationState::Confirmed)
    );
    // The second supersedes the first, which is handed over due
    // `retention` later and marked in the journal.
    retire_pass(&fixture.provider).await;
    let mark = journal()[first.operation_ref()]["retire_at"]
        .as_u64()
        .unwrap();
    // The chain moves on to block 101, and the process stops before the host
    // saves a checkpoint naming the second capture.
    probe.block_elapsed.store(1, Ordering::SeqCst);
    fixture.advance_clock(1000);
    fixture.provider.inner.chain.refresh().await.unwrap();
    restart(
        &mut fixture,
        Arc::new(circle.wallet(false, true).unwrap()),
        &limits,
    );
    // The next process recovers the session from that older checkpoint: the
    // first capture's receipt, and the session at the checkpoint's block as
    // an archive read shows it.
    *probe.logs.lock().unwrap() = vec![(descriptor_of(&older), 1, 1)];
    *probe.historical.lock().unwrap() = older.snapshot.settled_cumulative;
    fixture.provider.recover(older).await.unwrap();
    *probe.historical.lock().unwrap() = None;
    // Its lookup of the first capture withdrew the mark.
    let row = journal()[first.operation_ref()].clone();
    assert!(row.is_object(), "the first capture was forgotten");
    assert_eq!(row["retire_at"], Value::Null);
    // Another process's save after the time it was marked for keeps it.
    let elsewhere = Arc::new(TestClock::now());
    elsewhere.advance(
        (UNIX_EPOCH + Duration::from_secs(mark))
            .duration_since(elsewhere.wall())
            .unwrap(),
    );
    let other = circle
        .wallet(false, true)
        .unwrap()
        .with_clock(elsewhere.clone());
    TransactionWallet::retire(&other, &[], elsewhere.wall())
        .await
        .unwrap();
    assert!(journal().get(first.operation_ref()).is_some());
    // So the session's current checkpoint, which names it, still recovers.
    let current = fixture.provider.checkpoint(sid).await.unwrap();
    let named = |evidence: &RecoveryEvidence| {
        evidence
            .operation
            .as_ref()
            .map(|operation| operation.result.operation_ref().to_owned())
    };
    assert_eq!(named(&current).as_deref(), Some(first.operation_ref()));
    restart(
        &mut fixture,
        Arc::new(circle.wallet(false, true).unwrap()),
        &limits,
    );
    fixture.provider.recover(current).await.unwrap();
    // A later confirmed collection supersedes the first capture again: it
    // is handed over again, and forgotten once due.
    reattach(&fixture);
    fixture.voucher(100);
    spend(&mut fixture, sid, 5);
    *circle.service.hash.lock().unwrap() = Some(tx(3));
    settles(&fixture, &probe, &[(sid, 3, 1)]);
    let third = fixture.provider.settle(sid).await.unwrap();
    assert_eq!(third.state(), OperationState::Confirmed);
    let before = SystemTime::now();
    retire_pass(&fixture.provider).await;
    let again = journal()[first.operation_ref()]["retire_at"]
        .as_u64()
        .unwrap();
    let seconds = |time: SystemTime| time.duration_since(UNIX_EPOCH).unwrap().as_secs();
    assert!(again >= seconds(before + retention));
    circle.clock.advance(
        (UNIX_EPOCH + Duration::from_secs(again))
            .duration_since(circle.clock.wall())
            .unwrap(),
    );
    let reopened = circle.wallet(false, true).unwrap();
    TransactionWallet::retire(&reopened, &[], circle.clock.wall())
        .await
        .unwrap();
    let kept = journal()
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    // The second capture, which the recovered runtime never learned, stays.
    assert_eq!(
        kept,
        BTreeSet::from([
            second.operation_ref().to_owned(),
            third.operation_ref().to_owned()
        ])
    );
}
/// The session descriptor a recovery record names.
fn descriptor_of(evidence: &RecoveryEvidence) -> SessionDescriptor {
    crate::wire::descriptor_from_wire(evidence.snapshot.descriptor.as_ref().unwrap()).unwrap()
}

#[tokio::test]
async fn a_recovery_from_an_older_checkpoint_withdraws_the_mark_of_the_reference_it_names() {
    a_recovery_from_an_older_checkpoint_withdraws_the_mark(DAY).await;
}

#[tokio::test]
async fn a_recovery_from_an_older_checkpoint_withdraws_the_mark_within_an_hours_retention() {
    a_recovery_from_an_older_checkpoint_withdraws_the_mark(Duration::from_secs(3600)).await;
}

/// The runtime's half of the test above, with the suites' generic fake
/// wallet rather than the Circle adapter. Recovering a session from a
/// checkpoint taken before a later confirmed collection superseded its
/// capture looks that capture up in the wallet, so a wallet that withdraws a
/// reference's mark on lookup, as every wallet must, keeps it past the time
/// it was marked for; a collection that supersedes it again hands it over
/// again, and the later capture, never learned again, is never handed over.
async fn a_recovery_from_an_older_checkpoint_asks_the_wallet(retention: Duration) {
    use std::time::SystemTime;
    let limits = RuntimeLimits::default()
        .with_closed_retention(retention)
        .unwrap();
    let (mut fixture, probe) = fixture().await;
    Arc::get_mut(&mut fixture.provider.inner)
        .expect("the only handle to the runtime")
        .limits = limits.clone();
    probe.lookup_known.store(true, Ordering::SeqCst);
    probe.withdraws.store(true, Ordering::SeqCst);
    let sid = fixture.sid;
    settles(&fixture, &probe, &[(sid, 1, 1)]);
    let first = fixture.provider.settle(sid).await.unwrap();
    spend(&mut fixture, sid, 4);
    // The host's last saved checkpoint names the first capture.
    let older = fixture.provider.checkpoint(sid).await.unwrap();
    settles(&fixture, &probe, &[(sid, 2, 1)]);
    let second = fixture.provider.settle(sid).await.unwrap();
    assert_eq!(
        (first.state(), second.state()),
        (OperationState::Confirmed, OperationState::Confirmed)
    );
    // The second supersedes the first, which is handed over due `retention`
    // later and marked.
    retire_pass(&fixture.provider).await;
    let marked = |reference: &str| probe.marks.lock().unwrap().contains_key(reference);
    assert!(marked(first.operation_ref()));
    // The chain moves on to block 101, and the process stops before the host
    // saves a checkpoint naming the second capture.
    probe.block_elapsed.store(1, Ordering::SeqCst);
    fixture.advance_clock(1000);
    fixture.provider.inner.chain.refresh().await.unwrap();
    let wallet: Arc<dyn TransactionWallet> = probe.clone();
    restart(&mut fixture, wallet.clone(), &limits);
    // The next process recovers the session from that older checkpoint: the
    // first capture's receipt, and the session at the checkpoint's block as
    // an archive read shows it.
    *probe.logs.lock().unwrap() = vec![(descriptor_of(&older), 1, 1)];
    *probe.historical.lock().unwrap() = older.snapshot.settled_cumulative;
    fixture.provider.recover(older).await.unwrap();
    *probe.historical.lock().unwrap() = None;
    // Its lookup of the first capture withdrew the mark: the wallet keeps
    // the record past the time it was marked for.
    assert!(!marked(first.operation_ref()));
    // So the session's current checkpoint, which names it, still recovers.
    let current = fixture.provider.checkpoint(sid).await.unwrap();
    assert_eq!(
        current
            .operation
            .as_ref()
            .map(|operation| operation.result.operation_ref()),
        Some(first.operation_ref())
    );
    restart(&mut fixture, wallet, &limits);
    fixture.provider.recover(current).await.unwrap();
    // A later confirmed collection supersedes the first capture again: it
    // is handed over again, due `retention` after that collection.
    reattach(&fixture);
    fixture.voucher(100);
    spend(&mut fixture, sid, 5);
    settles(&fixture, &probe, &[(sid, 3, 1)]);
    let before = SystemTime::now();
    let third = fixture.provider.settle(sid).await.unwrap();
    let after = SystemTime::now();
    assert_eq!(third.state(), OperationState::Confirmed);
    retire_pass(&fixture.provider).await;
    let calls = probe.retired.lock().unwrap().clone();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, [first.operation_ref()]);
    assert!((before + retention..=after + retention).contains(&calls[1].1));
    assert!(
        calls
            .iter()
            .all(|(references, _)| !references.contains(&second.operation_ref().to_owned()))
    );
}

#[tokio::test]
async fn a_recovery_from_an_older_checkpoint_asks_the_wallet_about_the_reference_it_names() {
    a_recovery_from_an_older_checkpoint_asks_the_wallet(DAY).await;
}

#[tokio::test]
async fn a_recovery_from_an_older_checkpoint_asks_the_wallet_within_an_hours_retention() {
    a_recovery_from_an_older_checkpoint_asks_the_wallet(Duration::from_secs(3600)).await;
}

#[tokio::test]
async fn a_reference_still_named_as_a_baseline_waits_and_a_batch_waits_for_every_session() {
    let (mut fixture, probe) = fixture().await;
    let sid = fixture.sid;
    settles(&fixture, &probe, &[(sid, 1, 1)]);
    let own = fixture.provider.settle(sid).await.unwrap();
    let other = add_session(&mut fixture, &probe).await;
    // The batch confirms, but the first session's target did not rise: its
    // capture stays its baseline, which its checkpoint still names.
    settles(&fixture, &probe, &[(other, 2, 2)]);
    let batch = fixture.provider.settle_batch(&[sid, other]).await.unwrap();
    assert_eq!(batch.state(), OperationState::Confirmed);
    retire_pass(&fixture.provider).await;
    assert!(
        probe.retired.lock().unwrap().is_empty(),
        "a named baseline was handed over"
    );
    {
        let state = fixture.provider.inner.lock().unwrap();
        let baseline = state.sessions[&sid].baseline.as_ref().unwrap();
        assert_eq!(baseline.result.operation_ref(), own.operation_ref());
    }
    // A later capture of the first session supersedes both for it; the batch
    // still covers the other session, whose baseline it is.
    spend(&mut fixture, sid, 5);
    settles(&fixture, &probe, &[(sid, 2, 1)]);
    fixture.provider.settle(sid).await.unwrap();
    retire_pass(&fixture.provider).await;
    let calls = probe.retired.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, [own.operation_ref()]);
    // The other session's later capture supersedes the batch for it too: the
    // batch is due a day after the later of the two.
    spend(&mut fixture, other, 6);
    settles(&fixture, &probe, &[(other, 3, 1)]);
    let before = std::time::SystemTime::now();
    fixture.provider.settle(other).await.unwrap();
    let after = std::time::SystemTime::now();
    retire_pass(&fixture.provider).await;
    let calls = probe.retired.lock().unwrap().clone();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, [batch.operation_ref()]);
    assert!((before + DAY..=after + DAY).contains(&calls[1].1));
}

#[tokio::test]
async fn shutdown_during_empty_batch_simulation_has_no_pending_wallet_operation() {
    let (fixture, probe) = fixture().await;
    probe.block_simulation.store(true, Ordering::SeqCst);
    let sdk = fixture.provider.clone();
    let caller = tokio::spawn(async move { sdk.settle_batch(&[]).await });
    probe.simulation_entered.notified().await;
    let report = fixture
        .provider
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(caller.await.unwrap().is_err());
    assert!(report.pending_operations.is_empty());
    assert!(probe.calls.lock().unwrap().is_empty());
    assert!(fixture.provider.inner.lock().unwrap().empty_batch.is_none());
}

#[tokio::test]
async fn shutdown_after_batch_wallet_submission_started_preserves_unknown_intent() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    probe.block_wallet.store(true, Ordering::SeqCst);
    let sdk = fixture.provider.clone();
    let sid = fixture.sid;
    let caller = tokio::spawn(async move { sdk.settle_batch(&[sid, other]).await });
    probe.wallet_entered.notified().await;
    let report = fixture
        .provider
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(caller.await.unwrap().is_err());
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
    assert_eq!(report.pending_operations.len(), 1);
    assert_eq!(
        report.pending_operations[0].state(),
        OperationState::Unknown
    );
    let state = fixture.provider.inner.lock().unwrap();
    for sid in [fixture.sid, other] {
        assert_eq!(
            state.sessions[&sid]
                .operation
                .as_ref()
                .unwrap()
                .result
                .state(),
            OperationState::Unknown
        );
    }
}

async fn exhausted_shutdown_during_simulation(empty: bool, expired: bool) {
    let (fixture, probe) = fixture().await;
    probe.block_simulation.store(true, Ordering::SeqCst);
    let sdk = fixture.provider.clone();
    let sid = fixture.sid;
    let caller = tokio::spawn(async move {
        let ids = if empty { vec![] } else { vec![sid] };
        sdk.settle_batch(&ids).await
    });
    probe.simulation_entered.notified().await;
    assert!(probe.calls.lock().unwrap().is_empty());
    let deadline = if expired {
        Instant::now() - Duration::from_secs(1)
    } else {
        Instant::now()
    };
    let report = fixture.provider.shutdown(deadline).await.unwrap();
    // Inspect before yielding: the report must not depend on when abort_all
    // subsequently drops the suspended simulation future.
    assert!(
        report.pending_operations.is_empty(),
        "an exhausted shutdown deadline must not export a never-submitted intent"
    );
    assert!(probe.calls.lock().unwrap().is_empty());
    {
        let state = fixture.provider.inner.lock().unwrap();
        assert!(state.empty_batch.is_none());
        assert!(state.sessions[&sid].operation.is_none());
    }
    assert!(caller.await.unwrap().is_err());
}

#[tokio::test]
async fn immediate_shutdown_omits_unsubmitted_nonempty_batch() {
    exhausted_shutdown_during_simulation(false, false).await;
}

#[tokio::test]
async fn expired_shutdown_omits_unsubmitted_nonempty_batch() {
    exhausted_shutdown_during_simulation(false, true).await;
}

#[tokio::test]
async fn immediate_shutdown_omits_unsubmitted_empty_batch() {
    exhausted_shutdown_during_simulation(true, false).await;
}

#[tokio::test]
async fn expired_shutdown_omits_unsubmitted_empty_batch() {
    exhausted_shutdown_during_simulation(true, true).await;
}

async fn expired_shutdown_retains_started_wallet(empty: bool) {
    let (fixture, probe) = fixture().await;
    probe.block_wallet.store(true, Ordering::SeqCst);
    let sdk = fixture.provider.clone();
    let sid = fixture.sid;
    let caller = tokio::spawn(async move {
        let ids = if empty { vec![] } else { vec![sid] };
        sdk.settle_batch(&ids).await
    });
    probe.wallet_entered.notified().await;
    let report = fixture
        .provider
        .shutdown(Instant::now() - Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
    assert_eq!(report.pending_operations.len(), 1);
    assert_eq!(
        report.pending_operations[0].state(),
        OperationState::Unknown
    );
    {
        let state = fixture.provider.inner.lock().unwrap();
        let pending = if empty {
            state.empty_batch.as_ref()
        } else {
            state.sessions[&sid].operation.as_ref()
        }
        .unwrap();
        assert_eq!(pending.result.as_ref(), &report.pending_operations[0]);
    }
    assert!(caller.await.unwrap().is_err());
}

#[tokio::test]
async fn expired_shutdown_keeps_wallet_started_nonempty_batch_unknown() {
    expired_shutdown_retains_started_wallet(false).await;
}

#[tokio::test]
async fn expired_shutdown_keeps_wallet_started_empty_batch_unknown() {
    expired_shutdown_retains_started_wallet(true).await;
}

#[tokio::test]
async fn a_collection_proceeds_while_a_recovery_reads_and_its_intent_still_refuses_it() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    let runtime = fixture.provider.inner.clone();
    // The session's Control has ended, so recovery may replace it.
    runtime.detach_locked(&mut runtime.lock().unwrap(), 1);
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    probe.block_opening.store(true, Ordering::SeqCst);
    let recovery = {
        let provider = fixture.provider.clone();
        tokio::spawn(async move { provider.recover(evidence).await })
    };
    probe.opening_entered.notified().await;
    // While the recovery reads, a collection includes the session in a batch
    // whose outcome is still unknown.
    let pending = tokio::time::timeout(
        Duration::from_secs(1),
        fixture.provider.settle_batch(&[fixture.sid, other]),
    )
    .await
    .expect("a collection waited for a recovery's chain reads")
    .unwrap();
    assert_eq!(pending.state(), OperationState::Pending);
    probe.block_opening.store(false, Ordering::SeqCst);
    probe.opening_release.notify_one();
    let error = recovery.await.unwrap().unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().message(),
        "SESSION_NOT_READY: the evidence lacks the held session's unresolved or close operation"
    );
    let state = runtime.lock().unwrap();
    for sid in [fixture.sid, other] {
        let retained = state.sessions[&sid].operation.as_ref().unwrap();
        assert_eq!(retained.result.operation_ref(), pending.operation_ref());
    }
    assert_eq!(probe.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_batch_reference_is_kept_for_every_session_it_covers_even_one_not_yet_restored() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    let batch = fixture
        .provider
        .settle_batch(&[other, fixture.sid, other])
        .await
        .unwrap();
    let covered = BTreeMap::from([(fixture.sid, None), (other, None)]);
    assert_eq!(
        fixture.provider.inner.lock().unwrap().references[batch.operation_ref()],
        covered
    );
    // As after a restart: the first member restored brings the reference with
    // both sessions, so it waits for the one not restored yet.
    probe.lookup_known.store(true, Ordering::SeqCst);
    fixture
        .provider
        .inner
        .detach_locked(&mut fixture.provider.inner.lock().unwrap(), 1);
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    {
        let mut state = fixture.provider.inner.lock().unwrap();
        state.sessions.clear();
        state.references.clear();
    }
    fixture.provider.recover(evidence).await.unwrap();
    let state = fixture.provider.inner.lock().unwrap();
    assert!(!state.sessions.contains_key(&other));
    assert_eq!(state.references[batch.operation_ref()], covered);
}

/// The sessions each collection call covered, in order.
fn collected(probe: &Probe) -> Vec<Vec<B256>> {
    let context = crate::types::Domain::new(5_042_002, Address::repeat_byte(0x99))
        .unwrap()
        .signing_context();
    let session = |d: &IMPPI::SessionDescriptor| {
        context.session_id(&SessionDescriptor {
            agent: d.agent,
            sessionSigner: d.sessionSigner,
            providerId: d.providerId,
            recipient: d.recipient,
            operator: d.operator,
            token: d.token,
            salt: d.salt,
        })
    };
    let calls = probe.calls.lock().unwrap();
    calls
        .iter()
        .map(
            |(call, _)| match IMPPI::settleBatchCall::abi_decode(call.data()) {
                Ok(batch) => batch.d.iter().map(session).collect(),
                Err(_) => vec![session(
                    &IMPPI::settleCall::abi_decode(call.data()).unwrap().d,
                )],
            },
        )
        .collect()
}
/// `count` more sessions with usage, every one due.
async fn many(fixture: &mut Fixture, probe: &Probe, count: u64) -> BTreeSet<B256> {
    let mut ids = BTreeSet::from([fixture.sid]);
    for n in 0..count {
        let salt = keccak256(n.to_be_bytes());
        ids.insert(open_session(fixture, probe, salt, keccak256(salt), 100 + n).await);
    }
    due(fixture);
    ids
}

#[test]
fn collection_calldata_is_as_the_batch_bound_assumes() {
    let calls = ContractCalls::new(
        crate::types::Domain::new(5_042_002, Address::repeat_byte(0x99)).unwrap(),
        "USDC".into(),
        "2".into(),
    )
    .unwrap();
    let descriptor = |n: u64| SessionDescriptor {
        agent: Address::repeat_byte(0x44),
        sessionSigner: Address::repeat_byte(0x45),
        providerId: U256::from(42),
        recipient: Address::repeat_byte(0x22),
        operator: Address::repeat_byte(0x11),
        token: crate::contract::ARC_USDC,
        salt: B256::from(U256::from(n)),
    };
    let signature = [0xff; 65];
    let settle = calls.settle(&descriptor(0), u64::MAX, u64::MAX, &signature);
    assert_eq!(settle.unwrap().call().data().len(), SETTLE_CALL_BYTES);
    let close = calls.close(&descriptor(0), u64::MAX, u64::MAX, &signature);
    assert_eq!(close.unwrap().call().data().len(), SETTLE_CALL_BYTES);
    let descriptors = (0..137).map(descriptor).collect::<Vec<_>>();
    for n in [0, 1, 2, 136, 137] {
        let items = descriptors[..n]
            .iter()
            .map(|d| (d, u64::MAX, u64::MAX, &signature[..]))
            .collect::<Vec<_>>();
        let batch = calls.settle_batch(&items).unwrap();
        assert_eq!(batch.call().data().len(), 260 + 448 * n);
    }
    assert_eq!(batch_sessions(61_440), 136);
    assert_eq!(260 + 448 * 136, 61_188);
    assert_eq!(260 + 448 * 137, 61_636);
    assert_eq!(batch_sessions(SETTLE_CALL_BYTES), 1);
}

#[tokio::test]
async fn maintenance_batches_fit_the_wallets_call_bound_and_collect_every_due_session() {
    let (mut fixture, probe) = fixture().await;
    let ids = many(&mut fixture, &probe, 140).await;
    fixture.provider.inner.maintain_once();
    wait_for_maintenance(&fixture).await;
    for (call, _) in probe.calls.lock().unwrap().iter() {
        assert!(call.data().len() <= 61_440, "{} bytes", call.data().len());
    }
    let groups = collected(&probe);
    assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(), [136, 5]);
    assert_eq!(groups.concat().into_iter().collect::<BTreeSet<_>>(), ids);
    let state = fixture.provider.inner.lock().unwrap();
    for sid in &ids {
        let operation = state.sessions[sid].operation.as_ref().unwrap();
        assert!(
            operation.result.tx_hash().is_some(),
            "a due session was not collected"
        );
    }
}

#[tokio::test]
async fn a_wallet_stating_a_smaller_call_bound_gets_batches_split_to_fit_it() {
    // Three sessions' batch is 1,604 bytes; four would take 2,052.
    for (bound, sizes) in [(1_604, vec![3, 2]), (SETTLE_CALL_BYTES, vec![1; 5])] {
        let (mut fixture, probe) =
            fixture_with(|probe| Arc::new(Bounded(probe, bound)) as Arc<dyn TransactionWallet>)
                .await;
        let ids = many(&mut fixture, &probe, 4).await;
        fixture.provider.inner.maintain_once();
        wait_for_maintenance(&fixture).await;
        for (call, _) in probe.calls.lock().unwrap().iter() {
            assert!(call.data().len() <= bound, "{} bytes", call.data().len());
        }
        let groups = collected(&probe);
        assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(), sizes);
        assert_eq!(groups.concat().into_iter().collect::<BTreeSet<_>>(), ids);
    }
}

/// A wallet refusing calldata over its bound left the batch Unknown with no
/// handle, and every member only reconciled it.
#[tokio::test]
async fn a_batch_over_the_wallets_call_bound_is_refused_before_any_intent() {
    let (mut fixture, probe) = fixture().await;
    let other = add_session(&mut fixture, &probe).await;
    let mut ids = vec![fixture.sid; 68];
    ids.extend(vec![other; 69]);
    let error = fixture.provider.settle_batch(&ids).await.unwrap_err();
    let status = error.downcast_ref::<Status>().unwrap();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert_eq!(probe.simulation.load(Ordering::SeqCst), 0);
    assert!(probe.calls.lock().unwrap().is_empty());
    for sid in [fixture.sid, other] {
        assert!(
            fixture.provider.inner.lock().unwrap().sessions[&sid]
                .operation
                .is_none()
        );
        let single = fixture.provider.settle(sid).await.unwrap();
        assert_eq!(single.state(), OperationState::Pending);
    }
}

#[tokio::test]
async fn a_call_bound_below_one_settlement_is_refused_at_construction() {
    let (fixture, probe) = fixture().await;
    let source = &fixture.provider.inner;
    for (bound, accepted) in [
        (0, false),
        (SETTLE_CALL_BYTES - 1, false),
        (SETTLE_CALL_BYTES, true),
    ] {
        let built = Mppi::new(
            source.config.clone(),
            source.chain.clone(),
            source.contract.clone(),
            Arc::new(Bounded(probe.clone(), bound)),
            source.coverage.clone(),
            source.execution.clone(),
        );
        assert_eq!(built.is_ok(), accepted, "bound {bound}");
        if let Err(error) = built {
            assert_eq!(error.code(), ValidationCode::InvalidValue);
        }
    }
}
