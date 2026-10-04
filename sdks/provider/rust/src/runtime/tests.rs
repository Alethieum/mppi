//! Core tests use real signing, ABI readers and follower verification with an
//! independent deterministic RPC fixture. They do not execute an inference engine.
use super::*;
use crate::{
    Address, CatalogRow,
    capabilities::{WalletCall, WalletError},
    chain::{ChainError, CodePin, DeploymentProfile, Rpc},
    follower::Clock,
    interfaces::ExecutionStream,
    publish_catalog,
    signing::{SessionDescriptor, SessionProof, Voucher},
    types::Domain,
};
use alloy_primitives::{hex, keccak256};
use k256::ecdsa::SigningKey;
use serde_json::{Value, json};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const CHAIN: u64 = 5_042_002;
pub(super) const TIME: u64 = 1_800_000_000;
fn addr(n: u8) -> Address {
    Address::repeat_byte(n)
}
fn hash(n: u8) -> B256 {
    B256::repeat_byte(n)
}
fn hx(bytes: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(bytes))
}
fn word(n: u64) -> Vec<u8> {
    U256::from(n).to_be_bytes::<32>().to_vec()
}
fn aword(address: Address) -> Vec<u8> {
    [vec![0; 12], address.to_vec()].concat()
}
fn dynamic(bytes: &[u8]) -> Vec<u8> {
    let mut result = [word(32), word(bytes.len() as u64), bytes.to_vec()].concat();
    result.resize(64 + bytes.len().div_ceil(32) * 32, 0);
    result
}
fn domain() -> Domain {
    Domain::new(CHAIN, addr(0x99)).unwrap()
}
fn key() -> SigningKey {
    SigningKey::from_slice(&[7; 32]).unwrap()
}
fn sign<T: SessionProof>(proof: &T) -> Vec<u8> {
    // ecdsa 0.16.9 recovery.rs:180 signs the exact supplied prehash.
    let (signature, recovery) = key()
        .sign_prehash_recoverable(domain().signing_context().digest(proof).as_slice())
        .unwrap();
    let mut result = signature.to_bytes().to_vec();
    result.push(27 + recovery.to_byte());
    result
}
fn descriptor() -> SessionDescriptor {
    let public = key().verifying_key().to_encoded_point(false);
    SessionDescriptor {
        agent: addr(0x44),
        sessionSigner: Address::from_slice(&keccak256(&public.as_bytes()[1..])[12..]),
        providerId: U256::from(42),
        recipient: addr(0x22),
        operator: addr(0x11),
        token: crate::contract::ARC_USDC,
        salt: hash(0x55),
    }
}
struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn monotonic(&self) -> Duration {
        Duration::from_millis(self.0.load(Ordering::SeqCst))
    }
    fn wall(&self) -> Result<Duration, ChainError> {
        Ok(Duration::from_secs(TIME) + self.monotonic())
    }
}
pub(super) struct TestRpc {
    catalog: Mutex<Catalog>,
    initial_hash: B256,
    deposit: AtomicU64,
    pause: AtomicU64,
    pub(super) last_activity: AtomicU64,
}
impl TestRpc {
    fn sid(&self) -> B256 {
        domain().signing_context().session_id(&descriptor())
    }
    fn profile(&self) -> DeploymentProfile {
        let pin = |n| CodePin::new(addr(n), keccak256([0x60, 0])).unwrap();
        DeploymentProfile::new(
            domain(),
            addr(0x97),
            pin(0x99),
            pin(0x98),
            pin(0x97),
            pin(0x96),
        )
        .unwrap()
    }
    fn block(&self) -> Value {
        json!({"number":"0x64","hash":hx(hash(0xbb)),"parentHash":hx(hash(0xbc)),"timestamp":format!("0x{TIME:x}"),"gasLimit":"0x1c9c380"})
    }
    fn receipt(&self) -> Value {
        let d = descriptor();
        let topics = vec![
            hx(keccak256(
                "SessionOpened(bytes32,address,address,uint256,address,address,address,bytes32,uint64,bytes32)",
            )),
            hx(self.sid()),
            hx(aword(d.agent)),
            hx(d.providerId.to_be_bytes::<32>()),
        ];
        let data = [
            aword(d.sessionSigner),
            aword(d.recipient),
            aword(d.operator),
            aword(d.token),
            d.salt.to_vec(),
            word(100),
            self.initial_hash.to_vec(),
        ]
        .concat();
        json!({"transactionHash":hx(hash(0xaa)),"blockHash":hx(hash(0xbb)),"blockNumber":"0x64","transactionIndex":"0x0","status":"0x1","logs":[{
            "address":hx(addr(0x99)),"topics":topics,"data":hx(data),"logIndex":"0x0","transactionIndex":"0x0","transactionHash":hx(hash(0xaa)),"blockNumber":"0x64","blockHash":hx(hash(0xbb)),"removed":false
        }]})
    }
}
#[tonic::async_trait]
impl Rpc for TestRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        match method {
            "eth_chainId" => Ok(json!(format!("0x{CHAIN:x}"))),
            "eth_getBlockByNumber" => Ok(self.block()),
            "eth_getCode" => Ok(json!("0x6000")),
            "eth_getStorageAt" => Ok(json!(hx(aword(if params[0] == hx(addr(0x99)) {
                addr(0x98)
            } else {
                addr(0x96)
            })))),
            "eth_getTransactionReceipt" => Ok(self.receipt()),
            "eth_call" => {
                let raw = params[0]["data"].as_str().unwrap();
                let is = |signature: &str| raw.starts_with(&hx(&keccak256(signature)[..4]));
                let bytes = if is("getSession(bytes32)") {
                    [
                        word(self.deposit.load(Ordering::SeqCst)),
                        word(0),
                        word(0),
                        word(0),
                        word(self.last_activity.load(Ordering::SeqCst)),
                        self.initial_hash.to_vec(),
                    ]
                    .concat()
                } else if is("voucherDigest(bytes32,uint64)") {
                    domain()
                        .signing_context()
                        .digest(&Voucher {
                            sessionId: B256::ZERO,
                            cumulativeAmount: 0,
                        })
                        .to_vec()
                } else if is("pauseLevel()") {
                    word(self.pause.load(Ordering::SeqCst))
                } else if is("ownerOf(uint256)") {
                    aword(addr(0x11))
                } else if is("getAgentWallet(uint256)") {
                    aword(addr(0x22))
                } else if is("tokenURI(uint256)") {
                    dynamic(b"https://provider.example/discover")
                } else if is("getMetadata(uint256,string)") {
                    dynamic(self.catalog.lock().unwrap().canonical_bytes())
                } else {
                    return Err(ChainError::InvalidEvidence("unplanned runtime test call"));
                };
                Ok(json!(hx(bytes)))
            }
            _ => Err(ChainError::InvalidEvidence("unplanned runtime test method")),
        }
    }
}
struct Bindings;
#[tonic::async_trait]
impl TransactionWallet for Bindings {
    fn address(&self) -> Address {
        addr(0x11)
    }
    async fn execute(&self, _: &WalletCall, _: &str) -> Result<OperationResult, WalletError> {
        panic!("core must never submit a transaction")
    }
    async fn reconcile(&self, _: &str) -> Result<OperationResult, WalletError> {
        panic!("core must never query wallet")
    }
}
#[tonic::async_trait]
impl CoverageProvider for Bindings {
    async fn coverage(
        &self,
        _: &PreparedRequest,
        _: &RunContext,
    ) -> IntegrationResult<TokenCounts> {
        panic!("callbacks belong outside atomic core")
    }
}
impl ExecutionProvider for Bindings {
    fn execute(
        &self,
        _: PreparedRequest,
        _: RunContext,
        _: Arc<dyn ProviderRuns>,
        _: CancellationToken,
    ) -> ExecutionStream {
        panic!("callbacks belong outside atomic core")
    }
}
pub(super) struct Fixture {
    pub(super) provider: Mppi,
    pub(super) rpc: Arc<TestRpc>,
    clock: Arc<TestClock>,
    pub(super) sid: B256,
}
impl Fixture {
    pub(super) fn advance_clock(&self, millis: u64) {
        self.clock.0.fetch_add(millis, Ordering::SeqCst);
    }

    async fn new(rate: u64) -> Self {
        Self::with_wallet(rate, Arc::new(Bindings), |rpc| rpc).await
    }
    pub(super) async fn with_wallet(
        rate: u64,
        wallet: Arc<dyn TransactionWallet>,
        wrap: impl FnOnce(Arc<TestRpc>) -> Arc<dyn Rpc>,
    ) -> Self {
        let values = json!({
            "provider":{"name":"Runtime fixture","description":"Core payment checks","public_origin":"https://provider.example","recipient":hx(addr(0x22))},
            "identity":{"provider_id":"42","wallet_address":hx(addr(0x11))},
            "chain":{"network":"testnet","max_age_ms":1000},
            "upstream":{"url":"http://127.0.0.1:8080/v1"},
            "models":[{"model":"example/model","description":"Text generation","context_window":128000,"max_output":8192,"modalities":["text"],"embeddings":false,"streaming":true,"tools":true,"structured_output":true,"rates":{"input":"0","output":rate.to_string()}}],
            "payment":{"collection_margin_ms":60000,"authorization_wait_ms":10000},
            "settlement":{"interval_ms":30000,"amount":"1000000"}
        });
        let config = ProviderConfig::from_mapping(&values, None).unwrap();
        let rpc = Arc::new(TestRpc {
            catalog: Mutex::new(config.catalog().clone()),
            initial_hash: config.catalog().rate_card_hash(),
            deposit: AtomicU64::new(100),
            pause: AtomicU64::new(0),
            last_activity: AtomicU64::new(TIME),
        });
        let reader = ChainReader::new(wrap(rpc.clone()), rpc.profile());
        let clock = Arc::new(TestClock(AtomicU64::new(0)));
        let follower = Arc::new(
            ChainFollower::with_clock(reader.clone(), 1000, 60000, Some(250), clock.clone())
                .unwrap(),
        );
        follower.start().await.unwrap();
        follower.watch_provider(U256::from(42)).await.unwrap();
        follower.watch_session(rpc.sid()).await.unwrap();
        let opening = reader
            .opening(hash(0xaa), rpc.sid(), Some(config.catalog()))
            .await
            .unwrap();
        let calls = ContractCalls::new(domain(), "USDC".into(), "2".into()).unwrap();
        let provider = Mppi::new(
            config,
            follower,
            calls,
            wallet,
            Arc::new(Bindings),
            Arc::new(Bindings),
        )
        .unwrap();
        let sid = rpc.sid();
        {
            let mut state = provider.inner.lock().unwrap();
            state.started = true;
            state.start_block = Some(99);
            let mut session = Session::new(sid, opening, true);
            session.generation = 1;
            session.owner = Some(1);
            state.sessions.insert(sid, session);
            state.attachments.insert(
                1,
                Attachment {
                    queue: Arc::new(ByteQueue::default()),
                    nonce: hash(0x71),
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
        Self {
            provider,
            rpc,
            clock,
            sid,
        }
    }
    fn request(&self, id: u8) -> proto::CompleteRequest {
        let proof = crate::signing::RunAuth {
            sessionId: self.sid,
            controlGeneration: 1,
            runId: hash(id),
            model: "example/model".into(),
            rateCardHash: self.rpc.initial_hash,
        };
        proto::CompleteRequest {
            model: Some(proof.model.clone()),
            stream: Some(true),
            session_id: Some(self.sid.to_vec()),
            run_id: Some(proof.runId.to_vec()),
            control_generation: Some(1),
            rate_card_hash: Some(proof.rateCardHash.to_vec()),
            auth_signature: Some(sign(&proof)),
            payload: Some(
                br#"{"model":"example/model","stream":true,"large_integer":9007199254740993}"#
                    .to_vec(),
            ),
        }
    }
    fn begin(&self, id: u8) -> RunHandle {
        self.provider.inner.begin_run(self.request(id)).unwrap()
    }
    pub(super) fn voucher(&self, amount: u64) {
        let voucher = proto::Voucher {
            session_id: Some(self.sid.to_vec()),
            cumulative: Some(amount),
            signature: Some(sign(&Voucher {
                sessionId: self.sid,
                cumulativeAmount: amount,
            })),
        };
        self.provider
            .inner
            .lock()
            .unwrap()
            .sessions
            .get_mut(&self.sid)
            .unwrap()
            .voucher = Some(voucher);
    }
    pub(super) fn admit(&self, id: u8) -> RunHandle {
        let run = self.begin(id);
        assert!(matches!(
            self.provider
                .inner
                .attempt_admission(&run, counts(1000))
                .unwrap(),
            Admission::Granted
        ));
        run
    }
}
fn counts(output: u64) -> TokenCounts {
    TokenCounts::new(0, 0, output).unwrap()
}
fn reason(error: &Status) -> Option<String> {
    wire::parse_payment_error(error)
        .unwrap()
        .and_then(|failure| failure.reason().map(str::to_owned))
}

#[tokio::test]
async fn signed_admission_preserves_payload_and_deduplicates_before_callbacks() {
    let fixture = Fixture::new(700).await;
    let mut invalid = fixture.request(1);
    invalid.auth_signature.as_mut().unwrap()[0] ^= 1;
    assert_eq!(
        fixture
            .provider
            .inner
            .begin_run(invalid)
            .err()
            .unwrap()
            .code(),
        tonic::Code::Unauthenticated
    );
    let run = fixture.begin(1);
    assert!(
        std::str::from_utf8(run.request.payload())
            .unwrap()
            .contains("9007199254740993")
    );
    assert_eq!(
        fixture
            .provider
            .inner
            .begin_run(fixture.request(1))
            .err()
            .unwrap()
            .code(),
        tonic::Code::AlreadyExists
    );
    assert!(matches!(
        fixture
            .provider
            .inner
            .attempt_admission(&run, counts(1000))
            .unwrap(),
        Admission::Wait(_)
    ));
    assert!(
        !fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)].admitted()
    );
    fixture.voucher(1);
    assert!(matches!(
        fixture
            .provider
            .inner
            .attempt_admission(&run, counts(1000))
            .unwrap(),
        Admission::Granted
    ));
    fixture
        .provider
        .inner
        .recheck_dispatch(&run.context)
        .unwrap();
    assert!(
        fixture
            .provider
            .inner
            .recheck_dispatch(&run.context)
            .is_err()
    );
}

#[tokio::test]
async fn concurrent_pending_candidates_cannot_reuse_authorized_headroom() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(1);
    let first = fixture.begin(1);
    let second = fixture.begin(2);
    assert!(matches!(
        fixture
            .provider
            .inner
            .attempt_admission(&first, counts(1000))
            .unwrap(),
        Admission::Granted
    ));
    let Admission::Wait(guidance) = fixture
        .provider
        .inner
        .attempt_admission(&second, counts(1000))
        .unwrap()
    else {
        panic!("combined coverage requires two units")
    };
    let Some(proto::control_message::Kind::LowCredit(guidance)) = guidance.kind else {
        panic!("coverage guidance")
    };
    assert_eq!(
        guidance.coverage_request.unwrap().required_by_price[0].output_tokens,
        Some(2000)
    );
    fixture.voucher(2);
    assert!(matches!(
        fixture
            .provider
            .inner
            .attempt_admission(&second, counts(1000))
            .unwrap(),
        Admission::Granted
    ));
    let first_final = fixture
        .provider
        .inner
        .report_sync(&first.context, counts(1000), Some(RunStatus::Completed))
        .unwrap();
    assert_eq!(first_final.session_usage_amount(), 0);
    let second_final = fixture
        .provider
        .inner
        .report_sync(&second.context, counts(1000), Some(RunStatus::Completed))
        .unwrap();
    assert_eq!(second_final.session_usage_amount(), 1);
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(2)]
            .terminal
            .as_ref()
            .unwrap()
            .amount,
        Some(0)
    );
}

#[tokio::test]
async fn cumulative_reports_and_final_repeat_keep_accounting_and_cursor() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(2);
    let run = fixture.admit(1);
    let first = fixture
        .provider
        .inner
        .report_sync(&run.context, counts(400), None)
        .unwrap();
    let duplicate = fixture
        .provider
        .inner
        .report_sync(&run.context, counts(400), None)
        .unwrap();
    assert!(duplicate.duplicate());
    assert_eq!(duplicate.meter_seq(), first.meter_seq());
    let final_report = fixture
        .provider
        .inner
        .report_sync(&run.context, counts(800), Some(RunStatus::Completed))
        .unwrap();
    let repeated = fixture
        .provider
        .inner
        .report_sync(&run.context, counts(800), Some(RunStatus::Completed))
        .unwrap();
    assert!(repeated.duplicate());
    assert_eq!(repeated.meter_seq(), final_report.meter_seq());
    let terminal = fixture
        .provider
        .inner
        .finish_stream(&run.context)
        .unwrap()
        .unwrap();
    let queue = fixture.provider.inner.lock().unwrap().attachments[&1]
        .queue
        .clone();
    let mut found = None;
    while queue.buffered().unwrap().0 > 0 {
        let message = proto::ControlMessage::decode(queue.get().await.unwrap().as_slice()).unwrap();
        if let Some(proto::control_message::Kind::Receipt(receipt)) = message.kind {
            found = Some(receipt);
        }
    }
    let receipt = found.unwrap();
    assert_eq!(receipt.final_meter_seq, terminal.final_meter_seq);
    assert_eq!(receipt.amount, terminal.amount);
    assert_eq!(receipt.output_tokens, terminal.output_tokens);
    assert_eq!(receipt.status, terminal.status);
    assert_eq!(
        fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .retained_request_bytes,
        0
    );
    assert!(
        fixture
            .provider
            .inner
            .report_sync(&run.context, counts(801), Some(RunStatus::Completed))
            .is_err()
    );
    assert!(fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].inconsistent);
}

#[tokio::test]
async fn uncovered_report_freezes_session_without_accounting_change_or_release() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(2);
    let run = fixture.admit(1);
    let old =
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)].accounting;
    assert!(
        fixture
            .provider
            .inner
            .report_sync(&run.context, counts(1001), Some(RunStatus::Completed))
            .is_err()
    );
    let state = fixture.provider.inner.lock().unwrap();
    assert_eq!(state.sessions[&fixture.sid].runs[&hash(1)].accounting, old);
    assert!(state.sessions[&fixture.sid].inconsistent);
    assert!(run.stop.is_cancelled());
}

#[tokio::test]
async fn lost_control_delivery_retains_committed_usage_and_accepts_definitive_stop_report() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(2);
    let run = fixture.admit(1);
    let queue = fixture.provider.inner.lock().unwrap().attachments[&1]
        .queue
        .clone();
    for _ in 0..64 {
        queue.put_nowait(&[0]).unwrap();
    }
    let report = fixture
        .provider
        .inner
        .report_sync(&run.context, counts(500), None)
        .unwrap();
    {
        let state = fixture.provider.inner.lock().unwrap();
        assert!(!state.attachments[&1].live);
        assert_eq!(
            state.sessions[&fixture.sid].runs[&hash(1)]
                .accounting
                .counts(),
            counts(500)
        );
        assert_eq!(state.sessions[&fixture.sid].meter_seq, report.meter_seq());
        assert!(run.stop.is_cancelled());
    }
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(500), Some(RunStatus::Cancelled))
        .unwrap();
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)]
            .accounting
            .remaining_numerator(),
        U256::ZERO
    );
}

#[tokio::test]
async fn exhausted_meter_sequence_cannot_commit_counts_or_release_reservation() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(2);
    let run = fixture.admit(1);
    let previous = {
        let mut state = fixture.provider.inner.lock().unwrap();
        let session = state.sessions.get_mut(&fixture.sid).unwrap();
        session.meter_seq = u64::MAX;
        session.runs[&hash(1)].accounting
    };
    let error = fixture
        .provider
        .inner
        .report_sync(&run.context, counts(500), Some(RunStatus::Completed))
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    let state = fixture.provider.inner.lock().unwrap();
    assert_eq!(
        state.sessions[&fixture.sid].runs[&hash(1)].accounting,
        previous
    );
    assert_eq!(state.sessions[&fixture.sid].meter_seq, u64::MAX);
    assert!(state.sessions[&fixture.sid].inconsistent);
}

#[tokio::test]
async fn renewal_never_shrinks_and_missing_final_retains_reservation() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(1);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(500), None)
        .unwrap();
    let before =
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)].accounting;
    assert!(
        fixture
            .provider
            .inner
            .require_coverage_sync(&run.context, counts(1))
            .unwrap()
            .granted()
    );
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)].accounting,
        before
    );
    let denied = fixture
        .provider
        .inner
        .require_coverage_sync(&run.context, counts(2000))
        .unwrap();
    assert!(!denied.granted());
    assert_eq!(
        denied.failure().unwrap().reason(),
        Some("AUTHORIZATION_REQUIRED")
    );
    assert!(
        fixture
            .provider
            .inner
            .finish_stream(&run.context)
            .unwrap()
            .is_none()
    );
    {
        let state = fixture.provider.inner.lock().unwrap();
        assert!(state.sessions[&fixture.sid].runs[&hash(1)].unresolved);
        assert_eq!(
            state.sessions[&fixture.sid].runs[&hash(1)].accounting,
            before
        );
    }
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(500), Some(RunStatus::Cancelled))
        .unwrap();
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)]
            .accounting
            .remaining_numerator(),
        U256::ZERO
    );
}

#[tokio::test]
async fn generation_fence_denies_grants_but_allows_original_final() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(2);
    let run = fixture.admit(1);
    {
        let mut state = fixture.provider.inner.lock().unwrap();
        state.sessions.get_mut(&fixture.sid).unwrap().generation = 2;
        state.attachments.get_mut(&1).unwrap().generation = 2;
    }
    assert_eq!(
        reason(
            &fixture
                .provider
                .inner
                .recheck_dispatch(&run.context)
                .unwrap_err()
        )
        .as_deref(),
        Some("CONTROL_NOT_READY")
    );
    assert!(
        !fixture
            .provider
            .inner
            .require_coverage_sync(&run.context, counts(1000))
            .unwrap()
            .granted()
    );
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(200), Some(RunStatus::Cancelled))
        .unwrap();
}

#[tokio::test]
async fn zero_price_requires_explicit_zero_voucher_and_checks_group_counts() {
    let fixture = Fixture::new(0).await;
    let first = fixture.begin(1);
    assert!(matches!(
        fixture
            .provider
            .inner
            .attempt_admission(&first, counts(1000))
            .unwrap(),
        Admission::Wait(_)
    ));
    fixture.voucher(0);
    assert!(matches!(
        fixture
            .provider
            .inner
            .attempt_admission(&first, counts(1000))
            .unwrap(),
        Admission::Granted
    ));
    let second = fixture.admit(2);
    fixture
        .provider
        .inner
        .report_sync(
            &first.context,
            counts(u64::MAX - 1000),
            Some(RunStatus::Completed),
        )
        .unwrap();
    assert!(
        fixture
            .provider
            .inner
            .report_sync(&second.context, counts(1001), Some(RunStatus::Completed))
            .is_err()
    );
    assert!(fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].inconsistent);
}

#[path = "failure_tests.rs"]
mod failure_tests;
#[path = "recovery_tests.rs"]
mod recovery_tests;
#[path = "services_tests.rs"]
mod services_tests;

#[path = "control_auth_tests.rs"]
mod control_auth_tests;
#[path = "midstream_tests.rs"]
mod midstream_tests;

#[tokio::test]
async fn stale_chain_pause_and_catalog_change_fail_before_admission() {
    let fixture = Fixture::new(700).await;
    fixture.voucher(2);
    let run = fixture.begin(1);
    fixture.rpc.pause.store(1, Ordering::SeqCst);
    fixture.provider.inner.chain.refresh().await.unwrap();
    assert_eq!(
        reason(
            &fixture
                .provider
                .inner
                .attempt_admission(&run, counts(1000))
                .err()
                .unwrap()
        )
        .as_deref(),
        Some("CONTRACT_PAUSED")
    );
    fixture.rpc.pause.store(0, Ordering::SeqCst);
    *fixture.rpc.catalog.lock().unwrap() = publish_catalog(
        vec![CatalogRow::new("example/model", Rates::new(0, 900, None)).unwrap()],
        crate::DEFAULT_CATALOG_BYTES,
    )
    .unwrap();
    fixture.provider.inner.chain.refresh().await.unwrap();
    assert_eq!(
        reason(
            &fixture
                .provider
                .inner
                .attempt_admission(&run, counts(1000))
                .err()
                .unwrap()
        )
        .as_deref(),
        Some("PRICE_CHANGED")
    );
    fixture.clock.0.store(1000, Ordering::SeqCst);
    assert_eq!(
        reason(
            &fixture
                .provider
                .inner
                .attempt_admission(&run, counts(1000))
                .err()
                .unwrap()
        )
        .as_deref(),
        Some("CHAIN_STATE_STALE")
    );
}

#[tokio::test]
async fn insufficient_deposit_fails_without_reservation_and_cancelled_waiter_stays_rejected() {
    let fixture = Fixture::new(700).await;
    let run = fixture.begin(1);
    let error = fixture
        .provider
        .inner
        .attempt_admission(&run, counts(200_000))
        .err()
        .unwrap();
    assert_eq!(
        wire::parse_payment_error(&error).unwrap().unwrap().code(),
        PaymentCode::InsufficientDeposit
    );
    fixture.provider.inner.cancel_run(&run.context);
    fixture.voucher(100);
    assert!(
        fixture
            .provider
            .inner
            .attempt_admission(&run, counts(1000))
            .is_err()
    );
    let state = fixture.provider.inner.lock().unwrap();
    assert_eq!(
        state.sessions[&fixture.sid].runs[&hash(1)]
            .accounting
            .remaining_numerator(),
        U256::ZERO
    );
    assert_eq!(state.retained_request_bytes, 0);
    assert!(state.sessions[&fixture.sid].runs[&hash(1)].phase == RunPhase::NotAdmitted);
}

#[tokio::test]
async fn blocked_meter_observer_cannot_delay_accounting_or_change_committed_counts() {
    let (entered, waiting) = std::sync::mpsc::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let blocked = Mutex::new(blocked);
    let mut fixture = Fixture::new(1000).await;
    Arc::get_mut(&mut fixture.provider.inner).unwrap().observers = hooks::Observers::new(Hooks {
        on_meter: Some(Arc::new(move |mut meter| {
            meter.output_tokens = Some(u64::MAX);
            entered.send(()).unwrap();
            let _ = blocked.lock().unwrap().recv_timeout(Duration::from_secs(2));
        })),
        ..Hooks::default()
    });
    fixture.voucher(100);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(10), None)
        .unwrap();
    waiting.recv_timeout(Duration::from_secs(1)).unwrap();
    let second = fixture
        .provider
        .inner
        .report_sync(&run.context, counts(20), None)
        .unwrap();
    assert_eq!(second.counts(), counts(20));
    assert_eq!(fixture.provider.hook_stats().queued, 1);
    fixture.provider.inner.observers.close();
    release.send(()).unwrap();
    assert_eq!(
        fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].runs[&hash(1)]
            .accounting
            .counts(),
        counts(20)
    );
    fixture.provider.inner.chain.stop().await.unwrap();
}
