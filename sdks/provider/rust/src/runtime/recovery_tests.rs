//! Recovery tests share the deterministic signed chain fixture, but install
//! evidence into a distinct runtime with no live sessions or attachments.
use super::*;
use crate::{
    interfaces::{ProviderSessions, RecoveryEvidence, RecoveryOperation},
    signing::RunAuth,
    types::OperationState,
};

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
        state.start_block = Some(100);
    }
    provider
}

async fn final_history() -> (Fixture, RecoveryEvidence) {
    let fixture = Fixture::new(7000).await;
    fixture.voucher(100);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(750), Some(RunStatus::Completed))
        .unwrap();
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    (fixture, evidence)
}

#[tokio::test]
async fn completed_history_restores_amounts_and_original_proofs_without_attachment() {
    let (fixture, mut evidence) = final_history().await;
    evidence.snapshot.ready = Some(true); // A saved flag is not authority.
    let provider = restarted(&fixture);
    let view = provider.recover(evidence.clone()).await.unwrap();
    assert!(!view.readiness().ready());
    let state = provider.inner.lock().unwrap();
    let recovered = &state.sessions[&fixture.sid];
    assert_eq!(recovered.usage().unwrap(), 5);
    assert_eq!(recovered.owner, None);
    assert_eq!(recovered.generation, 1);
    assert_eq!(recovered.runs.len(), 1);
    assert_eq!(
        recovered.runs[&hash(1)].signature,
        evidence.runs[0].signature
    );
    assert!(recovered.runs[&hash(1)].stop.is_cancelled());
    assert!(state.attachments.is_empty());
    assert_eq!(state.retained_request_bytes, 0);
}

#[tokio::test]
async fn unresolved_recovery_keeps_reservation_until_original_final_report_once() {
    let fixture = Fixture::new(7000).await;
    fixture.voucher(100);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(500), None)
        .unwrap();
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    let reserved = evidence.runs[0].accounting.remaining_numerator();
    let provider = restarted(&fixture);
    let view = provider.recover(evidence).await.unwrap();
    assert!(!view.readiness().ready());
    {
        let state = provider.inner.lock().unwrap();
        let restored = &state.sessions[&fixture.sid].runs[&hash(1)];
        assert!(restored.stop.is_cancelled());
        assert!(restored.unresolved);
        assert_eq!(restored.accounting.remaining_numerator(), reserved);
    }
    assert!(
        !provider
            .inner
            .require_coverage_sync(&run.context, counts(1000))
            .unwrap()
            .granted()
    );
    let first = provider
        .inner
        .report_sync(&run.context, counts(750), Some(RunStatus::Cancelled))
        .unwrap();
    let duplicate = provider
        .inner
        .report_sync(&run.context, counts(750), Some(RunStatus::Cancelled))
        .unwrap();
    assert_eq!(first.meter_seq(), duplicate.meter_seq());
    assert!(duplicate.duplicate());
    let state = provider.inner.lock().unwrap();
    let restored = &state.sessions[&fixture.sid].runs[&hash(1)];
    assert!(!restored.unresolved);
    assert_eq!(restored.accounting.remaining_numerator(), U256::ZERO);
    assert_eq!(state.sessions[&fixture.sid].usage().unwrap(), 5);
}

#[tokio::test]
async fn malformed_or_incomplete_recovery_never_installs_partial_state() {
    let (fixture, valid) = final_history().await;
    let mut cases = Vec::new();
    let mut bad = valid.clone();
    bad.runs[0].signature[0] ^= 1;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.runs.push(bad.runs[0].clone());
    cases.push(bad);
    let mut bad = valid.clone();
    bad.runs.clear();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.usage_by_price[0].output_tokens = Some(749);
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.run_states.clear();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.run_states[0].status = Some(proto::RunStatus::Failed as i32);
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot
        .accepted_voucher
        .as_mut()
        .unwrap()
        .signature
        .as_mut()
        .unwrap()[0] ^= 1;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.accepted_voucher = None;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.meter_seq = Some(2);
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.deposit = Some(101);
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.initial_rate_card_hash = Some(hash(0xee).to_vec());
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.descriptor.as_mut().unwrap().operator = Some(addr(0xee).to_vec());
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.close_pending = Some(true);
    cases.push(bad);
    let mut bad = valid.clone();
    bad.reservations
        .push((bad.runs[0].context.clone(), bad.runs[0].accounting));
    cases.push(bad);
    let mut bad = valid.clone();
    bad.publications.push(bad.publications[0].clone());
    cases.push(bad);
    let mut bad = valid.clone();
    bad.confirmed_units = 1;
    cases.push(bad);
    for (index, bad) in cases.into_iter().enumerate() {
        let provider = restarted(&fixture);
        assert!(provider.recover(bad).await.is_err(), "corrupt case {index}");
        let state = provider.inner.lock().unwrap();
        assert!(
            state.sessions.is_empty(),
            "corrupt case {index} installed history"
        );
        assert_eq!(state.retained_request_bytes, 0);
    }
}

#[tokio::test]
async fn unresolved_reservation_missing_duplicate_and_underfunded_fail_closed() {
    let fixture = Fixture::new(7000).await;
    fixture.voucher(7);
    let run = fixture.admit(1);
    let valid = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    let mut cases = Vec::new();
    let mut bad = valid.clone();
    bad.reservations.clear();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.reservations.push(bad.reservations[0].clone());
    cases.push(bad);
    let mut bad = valid.clone();
    bad.runs[0].accounting =
        RunAccounting::new(zero_counts(), U256::from(7_000_001), None).unwrap();
    bad.reservations[0].1 = bad.runs[0].accounting;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.accepted_voucher = Some(proto::Voucher {
        session_id: Some(fixture.sid.to_vec()),
        cumulative: Some(6),
        signature: Some(sign(&Voucher {
            sessionId: fixture.sid,
            cumulativeAmount: 6,
        })),
    });
    cases.push(bad);
    for bad in cases {
        let provider = restarted(&fixture);
        assert!(provider.recover(bad).await.is_err());
        assert!(provider.inner.lock().unwrap().sessions.is_empty());
    }
    assert_eq!(run.context, valid.runs[0].context);
}

#[tokio::test]
async fn not_admitted_history_requires_original_authenticated_binding_and_zero_accounting() {
    let fixture = Fixture::new(7000).await;
    let run = fixture.begin(1);
    fixture.provider.inner.cancel_run(&run.context);
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    assert_eq!(
        evidence.snapshot.run_states[0].status,
        Some(proto::RunStatus::NotAdmitted as i32)
    );
    assert!(evidence.snapshot.accepted_voucher.is_none());
    let provider = restarted(&fixture);
    provider.recover(evidence.clone()).await.unwrap();
    assert_eq!(
        provider.inner.lock().unwrap().sessions[&fixture.sid]
            .usage()
            .unwrap(),
        0
    );
    let mut missing = evidence.clone();
    missing.runs.clear();
    assert!(restarted(&fixture).recover(missing).await.is_err());
    let mut forged = evidence;
    forged.runs[0].accounting = RunAccounting::new(counts(1), U256::ZERO, None).unwrap();
    assert!(restarted(&fixture).recover(forged).await.is_err());
}

#[tokio::test]
async fn recovery_rejects_live_owner_active_state_and_retained_cursor_rollback() {
    let (fixture, evidence) = final_history().await;
    assert!(fixture.provider.recover(evidence.clone()).await.is_err());
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].owner,
        Some(1)
    );
    fixture
        .provider
        .inner
        .detach_locked(&mut fixture.provider.inner.lock().unwrap(), 1);
    fixture.provider.recover(evidence.clone()).await.unwrap();
    {
        let mut state = fixture.provider.inner.lock().unwrap();
        state.sessions.get_mut(&fixture.sid).unwrap().generation = 2;
    }
    assert!(fixture.provider.recover(evidence).await.is_err());
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].generation,
        2
    );

    let fixture = Fixture::new(7000).await;
    fixture.voucher(100);
    fixture.admit(1);
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    fixture
        .provider
        .inner
        .detach_locked(&mut fixture.provider.inner.lock().unwrap(), 1);
    assert!(fixture.provider.recover(evidence).await.is_err());
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid]
            .active()
            .count(),
        1
    );
}

#[tokio::test]
async fn recovery_preserves_unknown_wallet_identity_and_rejects_omission_on_replacement() {
    let (fixture, mut evidence) = final_history().await;
    let voucher = evidence.snapshot.accepted_voucher.as_ref().unwrap();
    let prepared = fixture
        .provider
        .inner
        .contract
        .settle(&descriptor(), 100, 5, voucher.signature.as_deref().unwrap())
        .unwrap();
    evidence.operation = Some(RecoveryOperation {
        prepared,
        result: OperationResult::new(
            "settle:original",
            OperationState::Unknown,
            Some("wallet-original".into()),
            None,
            None,
            vec![],
        )
        .unwrap(),
        units: vec![(fixture.sid, 750)],
    });
    let provider = restarted(&fixture);
    provider.recover(evidence.clone()).await.unwrap();
    let retained = provider.checkpoint(fixture.sid).await.unwrap();
    let operation = retained.operation.unwrap();
    assert_eq!(operation.result.operation_ref(), "settle:original");
    assert_eq!(
        operation.result.wallet_operation_id(),
        Some("wallet-original")
    );
    assert_eq!(operation.result.state(), OperationState::Unknown);
    let mut missing = evidence.clone();
    missing.operation = None;
    assert!(provider.recover(missing).await.is_err());
    evidence.operation.as_mut().unwrap().result = OperationResult::new(
        "settle:original",
        OperationState::Unknown,
        None,
        None,
        None,
        vec![],
    )
    .unwrap();
    assert!(provider.recover(evidence).await.is_err());
    assert_eq!(
        provider
            .checkpoint(fixture.sid)
            .await
            .unwrap()
            .operation
            .unwrap()
            .result
            .wallet_operation_id(),
        Some("wallet-original")
    );
}

#[tokio::test]
async fn recovery_rejects_uncorroborated_baseline_and_duplicate_operation_units() {
    let wallet = Arc::new(RecoveryWallet::default());
    let fixture = Fixture::with_wallet(7000, wallet.clone(), |rpc| rpc).await;
    fixture.voucher(100);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(750), Some(RunStatus::Completed))
        .unwrap();
    let mut evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    let voucher = evidence.snapshot.accepted_voucher.as_ref().unwrap();
    let prepared = fixture
        .provider
        .inner
        .contract
        .settle(&descriptor(), 100, 5, voucher.signature.as_deref().unwrap())
        .unwrap();
    let operation = RecoveryOperation {
        prepared,
        result: OperationResult::new(
            "settle:baseline",
            OperationState::Confirmed,
            Some("wallet-baseline".into()),
            Some(hash(0xee)),
            Some(evidence.observation),
            vec![],
        )
        .unwrap(),
        units: vec![(fixture.sid, 750)],
    };
    *wallet.answer.lock().unwrap() = Some(Ok(operation.result.clone()));
    evidence.operation = Some(operation.clone());
    // The caller's terminal claim must be discarded: this transaction has no
    // matching canonical receipt/call. Its known identifiers remain unresolved.
    let provider = restarted(&fixture);
    provider.recover(evidence.clone()).await.unwrap();
    let restored = provider.checkpoint(fixture.sid).await.unwrap();
    assert_eq!(
        restored.operation.unwrap().result.state(),
        OperationState::Unknown
    );
    let mut baseline = evidence.clone();
    baseline.baseline = Some(operation);
    baseline.confirmed_units = 750;
    assert!(restarted(&fixture).recover(baseline).await.is_err());
    evidence
        .operation
        .as_mut()
        .unwrap()
        .units
        .push((fixture.sid, 750));
    assert!(restarted(&fixture).recover(evidence).await.is_err());
}

#[derive(Default)]
struct RecoveryWallet {
    executions: AtomicU64,
    lookups: Mutex<Vec<String>>,
    answer: Mutex<Option<Result<OperationResult, WalletError>>>,
}
#[tonic::async_trait]
impl TransactionWallet for RecoveryWallet {
    fn address(&self) -> Address {
        addr(0x11)
    }
    async fn execute(
        &self,
        _: &WalletCall,
        reference: &str,
    ) -> Result<OperationResult, WalletError> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let result = OperationResult::new(
            reference,
            OperationState::Pending,
            Some("wallet-original".into()),
            Some(hash(0xdd)),
            None,
            vec![],
        )
        .unwrap();
        *self.answer.lock().unwrap() = Some(Ok(result.clone()));
        Ok(result)
    }
    async fn reconcile(&self, reference: &str) -> Result<OperationResult, WalletError> {
        self.lookups.lock().unwrap().push(reference.into());
        self.answer
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(Err(WalletError))
    }
}

struct RecoveryReceiptRpc {
    base: Arc<TestRpc>,
}
#[tonic::async_trait]
impl Rpc for RecoveryReceiptRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        if method == "eth_getTransactionReceipt" && params[0] == hx(hash(0xee)) {
            // Canonical status-zero receipt, unrelated to the wallet's original
            // transaction. The receipt alone cannot prove its operation identity.
            let mut receipt = self.base.receipt();
            receipt["transactionHash"] = json!(hx(hash(0xee)));
            receipt["status"] = json!("0x0");
            receipt["logs"] = json!([]);
            return Ok(receipt);
        }
        if method == "eth_getTransactionReceipt" && params[0] == hx(hash(0xdd)) {
            return Ok(Value::Null);
        }
        self.base.call(method, params).await
    }
}

async fn original_pending_operation() -> (Fixture, Arc<RecoveryWallet>, RecoveryEvidence) {
    let wallet = Arc::new(RecoveryWallet::default());
    let fixture = Fixture::with_wallet(7000, wallet.clone(), |base| {
        Arc::new(RecoveryReceiptRpc { base })
    })
    .await;
    fixture.voucher(100);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(750), Some(RunStatus::Completed))
        .unwrap();
    assert_eq!(
        fixture.provider.settle(fixture.sid).await.unwrap().state(),
        OperationState::Pending
    );
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
    (fixture, wallet, evidence)
}

#[tokio::test]
async fn recovery_rejects_unrelated_reverted_hash_without_second_wallet_submission() {
    let (fixture, wallet, original) = original_pending_operation().await;
    let mut forged = original.clone();
    let operation = forged.operation.as_mut().unwrap();
    let reference = operation.result.operation_ref().to_owned();
    operation.result = OperationResult::new(
        &reference,
        OperationState::Unknown,
        Some("wallet-original".into()),
        Some(hash(0xee)),
        None,
        vec![],
    )
    .unwrap();
    let provider = restarted(&fixture);
    let outcome = provider.recover(forged).await;
    if outcome.is_ok() {
        // The vulnerable code installs Failed from the unrelated receipt and
        // starts a second payment while the genuine operation is still pending.
        provider.settle(fixture.sid).await.unwrap();
    }
    assert_eq!(
        wallet.executions.load(Ordering::SeqCst),
        1,
        "an unrelated reverted transaction must not authorize a second submission"
    );
    assert!(outcome.is_err());
    assert!(provider.inner.lock().unwrap().sessions.is_empty());
    provider.recover(original).await.unwrap();
    assert_eq!(
        provider.settle(fixture.sid).await.unwrap().state(),
        OperationState::Pending
    );
    assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
    assert!(
        wallet
            .lookups
            .lock()
            .unwrap()
            .iter()
            .all(|known| known == &reference)
    );
}

#[tokio::test]
async fn recovery_rejects_pending_hash_with_unavailable_or_conflicting_wallet_binding() {
    let (fixture, wallet, evidence) = original_pending_operation().await;
    let known = &evidence.operation.as_ref().unwrap().result;
    let reference = known.operation_ref();
    let cases = [
        Err(WalletError),
        Ok(OperationResult::new(
            "wrong-reference",
            OperationState::Pending,
            Some("wallet-original".into()),
            Some(hash(0xdd)),
            None,
            vec![],
        )
        .unwrap()),
        Ok(OperationResult::new(
            reference,
            OperationState::Pending,
            Some("wrong-wallet".into()),
            Some(hash(0xdd)),
            None,
            vec![],
        )
        .unwrap()),
        Ok(OperationResult::new(
            reference,
            OperationState::Pending,
            None,
            Some(hash(0xdd)),
            None,
            vec![],
        )
        .unwrap()),
        Ok(OperationResult::new(
            reference,
            OperationState::Unknown,
            Some("wallet-original".into()),
            None,
            None,
            vec![],
        )
        .unwrap()),
    ];
    for (index, answer) in cases.into_iter().enumerate() {
        *wallet.answer.lock().unwrap() = Some(answer);
        let provider = restarted(&fixture);
        assert!(
            provider.recover(evidence.clone()).await.is_err(),
            "binding case {index}"
        );
        assert!(provider.inner.lock().unwrap().sessions.is_empty());
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn recovery_accepts_wallet_bound_revert_and_preserves_original_identity() {
    let (fixture, wallet, mut evidence) = original_pending_operation().await;
    let operation = evidence.operation.as_mut().unwrap();
    operation.result = OperationResult::new(
        operation.result.operation_ref(),
        OperationState::Unknown,
        Some("wallet-original".into()),
        Some(hash(0xee)),
        None,
        vec![],
    )
    .unwrap();
    *wallet.answer.lock().unwrap() = Some(Ok(operation.result.clone()));
    let provider = restarted(&fixture);
    provider.recover(evidence.clone()).await.unwrap();
    let restored = provider
        .checkpoint(fixture.sid)
        .await
        .unwrap()
        .operation
        .unwrap();
    assert_eq!(restored.result.state(), OperationState::Failed);
    assert_eq!(
        restored.result.operation_ref(),
        evidence.operation.unwrap().result.operation_ref()
    );
    assert_eq!(restored.result.tx_hash(), Some(hash(0xee)));
    assert_eq!(
        restored.result.wallet_operation_id(),
        Some("wallet-original")
    );
    assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
}

struct HistoricalRpc {
    base: Arc<TestRpc>,
    height: Arc<AtomicU64>,
    later: Option<Catalog>,
}
#[tonic::async_trait]
impl Rpc for HistoricalRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        if method == "eth_getBlockByNumber" {
            let height = if params[0] == "latest" {
                self.height.load(Ordering::SeqCst)
            } else {
                u64::from_str_radix(params[0].as_str().unwrap().trim_start_matches("0x"), 16)
                    .unwrap()
            };
            if height == 101 {
                return Ok(
                    json!({"number":"0x65","hash":hx(hash(0xcd)),"parentHash":hx(hash(0xbb)),"timestamp":format!("0x{TIME:x}"),"gasLimit":"0x1c9c380"}),
                );
            }
        }
        if method == "eth_call"
            && params[1]["blockHash"] == hx(hash(0xcd))
            && params[0]["data"]
                .as_str()
                .unwrap()
                .starts_with(&hx(&keccak256("getMetadata(uint256,string)")[..4]))
        {
            return Ok(json!(hx(dynamic(
                self.later
                    .as_ref()
                    .map_or(&[][..], Catalog::canonical_bytes)
            ))));
        }
        self.base.call(method, params).await
    }
}

#[tokio::test]
async fn concurrent_original_catalogs_recover_shared_floor_and_remaining_coverage() {
    let later = publish_catalog(
        [CatalogRow::new("example/model", Rates::new(0, 900, None)).unwrap()],
        crate::DEFAULT_CATALOG_BYTES,
    )
    .unwrap();
    let height = Arc::new(AtomicU64::new(100));
    let fixture = Fixture::with_wallet(700, Arc::new(Bindings), {
        let height = height.clone();
        let later = Some(later.clone());
        move |base| {
            Arc::new(HistoricalRpc {
                base,
                height,
                later,
            })
        }
    })
    .await;
    fixture.voucher(2);
    let first = fixture.admit(1); // 0.7 exact USD base units reserved.
    height.store(101, Ordering::SeqCst);
    fixture.provider.inner.chain.refresh().await.unwrap();
    let mut request = fixture.request(2);
    request.rate_card_hash = Some(later.rate_card_hash().to_vec());
    request.auth_signature = Some(sign(&RunAuth {
        sessionId: fixture.sid,
        controlGeneration: 1,
        runId: hash(2),
        model: "example/model".into(),
        rateCardHash: later.rate_card_hash(),
    }));
    let second = fixture.provider.inner.begin_run(request).unwrap();
    assert!(matches!(
        fixture
            .provider
            .inner
            .attempt_admission(&second, counts(1000))
            .unwrap(),
        Admission::Granted
    ));
    fixture
        .provider
        .inner
        .report_sync(&first.context, counts(800), None)
        .unwrap();
    fixture
        .provider
        .inner
        .report_sync(&second.context, counts(800), None)
        .unwrap();
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    assert_eq!(evidence.snapshot.catalogs.len(), 2);
    assert_eq!(evidence.snapshot.usage_by_price.len(), 2);
    let provider = restarted(&fixture);
    provider.recover(evidence.clone()).await.unwrap();
    {
        let state = provider.inner.lock().unwrap();
        let session = &state.sessions[&fixture.sid];
        assert_eq!(session.usage().unwrap(), 1); // floor(0.56 + 0.72), not two floors.
        assert_eq!(session.runs[&hash(1)].rates.output(), 700);
        assert_eq!(session.runs[&hash(2)].rates.output(), 900);
        assert_eq!(
            required_coverage(
                std::iter::once(session.usage_numerator().unwrap()).chain(
                    session
                        .active()
                        .map(|run| run.accounting.remaining_numerator())
                )
            )
            .unwrap(),
            2
        );
    }
    let mut missing = evidence.clone();
    missing
        .publications
        .retain(|(hash, _)| hash != later.rate_card_hash().as_slice());
    assert!(restarted(&fixture).recover(missing).await.is_err());
    let mut wrong = evidence;
    let published = wrong
        .publications
        .iter_mut()
        .find(|(hash, _)| hash == later.rate_card_hash().as_slice())
        .unwrap();
    published.1 = ChainObservation::new(CHAIN, addr(0x99), 100, hash(0xbb)).unwrap();
    assert!(restarted(&fixture).recover(wrong).await.is_err());
}

#[tokio::test]
async fn unavailable_current_catalog_preserves_historical_recovery_and_accounting() {
    let height = Arc::new(AtomicU64::new(100));
    let fixture = Fixture::with_wallet(7000, Arc::new(Bindings), {
        let height = height.clone();
        move |base| {
            Arc::new(HistoricalRpc {
                base,
                height,
                later: None,
            })
        }
    })
    .await;
    fixture.voucher(100);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(750), Some(RunStatus::Completed))
        .unwrap();
    height.store(101, Ordering::SeqCst);
    fixture.provider.inner.chain.refresh().await.unwrap();
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    assert!(evidence.snapshot.current_rate_card_hash.is_none());
    let provider = restarted(&fixture);
    let view = provider.recover(evidence).await.unwrap();
    assert!(!view.readiness().ready());
    let retained = provider.checkpoint(fixture.sid).await.unwrap();
    assert!(retained.snapshot.current_rate_card_hash.is_none());
    assert_eq!(retained.snapshot.usage_by_price[0].output_tokens, Some(750));
    assert_eq!(
        provider.inner.lock().unwrap().sessions[&fixture.sid]
            .usage()
            .unwrap(),
        5
    );
}

#[tokio::test]
async fn stated_current_catalog_requires_its_exact_bytes_even_without_usage_in_that_version() {
    let later = publish_catalog(
        [CatalogRow::new("example/model", Rates::new(0, 900, None)).unwrap()],
        crate::DEFAULT_CATALOG_BYTES,
    )
    .unwrap();
    let height = Arc::new(AtomicU64::new(100));
    let fixture = Fixture::with_wallet(700, Arc::new(Bindings), {
        let height = height.clone();
        let later = Some(later.clone());
        move |base| {
            Arc::new(HistoricalRpc {
                base,
                height,
                later,
            })
        }
    })
    .await;
    height.store(101, Ordering::SeqCst);
    fixture.provider.inner.chain.refresh().await.unwrap();
    let mut evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    assert_eq!(
        evidence.snapshot.current_rate_card_hash,
        Some(later.rate_card_hash().to_vec())
    );
    restarted(&fixture).recover(evidence.clone()).await.unwrap();
    evidence
        .snapshot
        .catalogs
        .retain(|row| row.rate_card_hash.as_deref() != Some(later.rate_card_hash().as_slice()));
    evidence
        .publications
        .retain(|(hash, _)| hash != later.rate_card_hash().as_slice());
    assert!(restarted(&fixture).recover(evidence).await.is_err());
}

#[tokio::test]
async fn simulation_rejection_checkpoint_does_not_restore_phantom_wallet_intent() {
    let (fixture, _) = final_history().await;
    // TestRpc rejects the batch simulation's eth_call; Bindings panics if a
    // wallet call is made. Thus this exercises a real known-unsubmitted failure.
    assert!(fixture.provider.settle_batch(&[fixture.sid]).await.is_err());
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    let provider = restarted(&fixture);
    provider.recover(evidence).await.unwrap();
    let restored = provider.checkpoint(fixture.sid).await.unwrap();
    assert!(
        restored.operation.is_none(),
        "simulation never reached wallet submission; recovery must not invent a pending wallet intent"
    );
    assert_eq!(
        provider.inner.lock().unwrap().sessions[&fixture.sid]
            .usage()
            .unwrap(),
        5
    );
}
async fn unused_history() -> (Fixture, RecoveryEvidence) {
    let fixture = Fixture::new(7000).await;
    {
        // This fixture normally seeds an attached session. Construct instead
        // the exact new-opening state; no attachment handshake has occurred.
        let mut state = fixture.provider.inner.lock().unwrap();
        let opening = state.sessions[&fixture.sid].opening.clone();
        state
            .sessions
            .insert(fixture.sid, Session::new(fixture.sid, opening, true));
        state.attachments.clear();
    }
    let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    (fixture, evidence)
}

fn unused_attachment(provider: &Mppi, sid: B256, id: u64, nonce: B256) -> proto::ReconnectRequest {
    provider.inner.lock().unwrap().attachments.insert(
        id,
        Attachment {
            queue: Arc::new(ByteQueue::default()),
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
        session_id: Some(sid.to_vec()),
        nonce: Some(nonce.to_vec()),
        signature: Some(sign(&crate::signing::ReconnectAuth {
            sessionId: sid,
            nonce,
        })),
        latest_voucher: None,
        last_meter_seq: Some(0),
        pending_run_ids: vec![],
    }
}

#[tokio::test]
async fn unattached_checkpoint_restores_zero_until_first_authenticated_control() {
    let (fixture, evidence) = unused_history().await;
    assert_eq!(evidence.snapshot.control_generation, Some(0));
    assert_eq!(evidence.snapshot.ready, Some(false));
    assert!(evidence.snapshot.accepted_voucher.is_none());
    assert!(evidence.runs.is_empty() && evidence.reservations.is_empty());
    let provider = restarted(&fixture);
    let view = provider.recover(evidence.clone()).await.unwrap();
    assert!(!view.readiness().ready());
    {
        let state = provider.inner.lock().unwrap();
        let session = &state.sessions[&fixture.sid];
        assert_eq!(session.generation, 0);
        assert_eq!(session.owner, None);
        assert_eq!(session.usage().unwrap(), 0);
    }
    let mut exported = provider.checkpoint(fixture.sid).await.unwrap();
    assert_eq!(exported.snapshot, evidence.snapshot);
    exported.snapshot.deposit = Some(999);
    assert_eq!(
        provider.checkpoint(fixture.sid).await.unwrap().snapshot,
        evidence.snapshot
    );
    assert!(provider.inner.begin_run(fixture.request(1)).is_err());
    let request = unused_attachment(&provider, fixture.sid, 1, hash(0x72));
    let mut stale = request.clone();
    stale.nonce = Some(hash(0x71).to_vec());
    stale.signature = Some(sign(&crate::signing::ReconnectAuth {
        sessionId: fixture.sid,
        nonce: hash(0x71),
    }));
    assert!(provider.inner.attach(1, stale).is_err());
    assert_eq!(
        provider.inner.lock().unwrap().sessions[&fixture.sid].generation,
        0
    );
    provider.inner.attach(1, request.clone()).unwrap();
    provider.inner.attach(1, request.clone()).unwrap();
    assert_eq!(
        provider.inner.lock().unwrap().sessions[&fixture.sid].generation,
        1
    );
    let second = unused_attachment(&provider, fixture.sid, 2, hash(0x73));
    assert!(provider.inner.attach(2, request).is_err());
    provider.inner.attach(2, second).unwrap();
    assert_eq!(
        provider.inner.lock().unwrap().sessions[&fixture.sid].generation,
        2
    );
}

#[tokio::test]
async fn unattached_checkpoint_is_never_a_valid_wire_snapshot() {
    let (_, evidence) = unused_history().await;
    wire::validate_recovery_snapshot(&evidence.snapshot, &WireLimits::default()).unwrap();
    assert!(wire::validate_message(&evidence.snapshot).is_err());
    let message = proto::ControlMessage {
        kind: Some(proto::control_message::Kind::SessionState(
            evidence.snapshot,
        )),
    };
    assert!(wire::validate_message(&message).is_err());
}

#[tokio::test]
async fn unattached_recovery_rejects_contradictory_history_atomically() {
    let (fixture, valid) = unused_history().await;
    let (_, used) = final_history().await;
    let mut cases = Vec::new();
    let mut bad = valid.clone();
    bad.snapshot.ready = Some(true);
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.nonce = Some(hash(1).to_vec());
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.meter_seq = Some(1);
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.settled_cumulative = Some(1);
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.accepted_voucher = used.snapshot.accepted_voucher.clone();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.usage_by_price = used.snapshot.usage_by_price.clone();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.run_states = used.snapshot.run_states.clone();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.runs = used.runs.clone();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.reservations
        .push((used.runs[0].context.clone(), used.runs[0].accounting));
    cases.push(bad);
    let mut bad = valid.clone();
    bad.confirmed_units = 1;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.control_generation = None;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.catalogs.push(bad.snapshot.catalogs[0].clone());
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.descriptor.as_mut().unwrap().agent = Some(addr(0x12).to_vec());
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.chain_block_hash = Some(hash(0x12).to_vec());
    cases.push(bad);
    let mut bad = valid.clone();
    bad.snapshot.deposit = Some(99);
    cases.push(bad);
    for (index, bad) in cases.into_iter().enumerate() {
        let provider = restarted(&fixture);
        assert!(
            provider.recover(bad).await.is_err(),
            "corrupt unused case {index}"
        );
        assert!(provider.inner.lock().unwrap().sessions.is_empty());
    }
}

#[tokio::test]
async fn unattached_checkpoint_cannot_replace_later_generation_or_close_intent() {
    let (fixture, evidence) = unused_history().await;
    {
        let mut state = fixture.provider.inner.lock().unwrap();
        let session = state.sessions.get_mut(&fixture.sid).unwrap();
        session.close_pending = true;
        session.close_reference = Some("retained-close".into());
    }
    assert!(fixture.provider.recover(evidence.clone()).await.is_err());
    let closing = fixture.provider.checkpoint(fixture.sid).await.unwrap();
    let restored = restarted(&fixture);
    assert!(!restored.recover(closing).await.unwrap().readiness().ready());
    assert_eq!(
        restored.inner.lock().unwrap().sessions[&fixture.sid]
            .close_reference
            .as_deref(),
        Some("retained-close")
    );
    let provider = restarted(&fixture);
    provider.recover(evidence.clone()).await.unwrap();
    let request = unused_attachment(&provider, fixture.sid, 1, hash(0x72));
    provider.inner.attach(1, request).unwrap();
    assert!(provider.recover(evidence.clone()).await.is_err());
    provider
        .inner
        .detach_locked(&mut provider.inner.lock().unwrap(), 1);
    assert!(provider.recover(evidence).await.is_err());
    assert_eq!(
        provider.inner.lock().unwrap().sessions[&fixture.sid].generation,
        1
    );
}

#[tokio::test]
async fn unattached_checkpoint_requires_authoritative_private_history() {
    let (fixture, _) = unused_history().await;
    fixture
        .provider
        .inner
        .lock()
        .unwrap()
        .sessions
        .get_mut(&fixture.sid)
        .unwrap()
        .history_available = false;
    assert!(fixture.provider.checkpoint(fixture.sid).await.is_err());
    let request = unused_attachment(&fixture.provider, fixture.sid, 1, hash(0x72));
    assert!(fixture.provider.inner.attach(1, request).is_err());
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].generation,
        0
    );
}

#[tokio::test]
async fn unattached_recovery_preserves_refund_only_operation_identity() {
    let (fixture, mut evidence) = unused_history().await;
    evidence.snapshot.close_pending = Some(true);
    evidence.close_reference = Some("unused-close".into());
    let prepared = fixture
        .provider
        .inner
        .contract
        .close(&descriptor(), 0, 0, &[])
        .unwrap();
    evidence.operation = Some(RecoveryOperation {
        prepared: prepared.clone(),
        result: OperationResult::new(
            "unused-close",
            OperationState::Unknown,
            None,
            None,
            None,
            vec![],
        )
        .unwrap(),
        units: vec![(fixture.sid, 0)],
    });
    let provider = restarted(&fixture);
    assert!(
        !provider
            .recover(evidence)
            .await
            .unwrap()
            .readiness()
            .ready()
    );
    let retained = provider.checkpoint(fixture.sid).await.unwrap();
    assert_eq!(retained.snapshot.control_generation, Some(0));
    assert_eq!(retained.snapshot.close_pending, Some(true));
    assert_eq!(retained.close_reference.as_deref(), Some("unused-close"));
    let operation = retained.operation.unwrap();
    assert_eq!(operation.prepared.call(), prepared.call());
    assert_eq!(operation.result.operation_ref(), "unused-close");
    assert_eq!(operation.result.state(), OperationState::Unknown);
    assert_eq!(operation.units, vec![(fixture.sid, 0)]);
}
