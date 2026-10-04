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
    logs: Mutex<Vec<(SessionDescriptor, u64, u64)>>,
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
                let mut value = self.base.call(method, params).await?;
                let mut data =
                    hex::decode(value.as_str().unwrap().trim_start_matches("0x")).unwrap();
                let settled = self
                    .probe
                    .settled
                    .lock()
                    .unwrap()
                    .get(&sid)
                    .copied()
                    .unwrap_or(0);
                data[32..64].copy_from_slice(&word(settled));
                value = json!(hx(data));
                return Ok(value);
            }
        }
        self.base.call(method, params).await
    }
}
async fn fixture() -> (Fixture, Arc<Probe>) {
    let probe = Arc::new(Probe::default());
    let rpc_probe = probe.clone();
    let fixture = Fixture::with_wallet(1000, probe.clone(), move |base| {
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
    let first = fixture.sid;
    let runtime = &fixture.provider.inner;
    let mut descriptor = runtime.lock().unwrap().sessions[&first]
        .opening
        .descriptor()
        .clone();
    descriptor.salt = B256::repeat_byte(0x66);
    let sid = runtime
        .contract
        .domain()
        .signing_context()
        .session_id(&descriptor);
    let opening_tx = B256::repeat_byte(0xac);
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
        session.owner = Some(2);
        state.sessions.insert(sid, session);
        state.attachments.insert(
            2,
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
async fn recovery_preserves_failed_close_identity_and_repeated_close_never_resubmits() {
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
