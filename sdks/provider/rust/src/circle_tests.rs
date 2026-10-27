//! Exact synthetic CLI responses; no Circle account, chain read or network.
use super::*;
use crate::{
    chain::{ChainError, CodePin, DeploymentProfile, Rpc},
    types::Domain,
};
use alloy_primitives::keccak256;
use std::sync::{
    Mutex as StdMutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};

fn operator() -> Address {
    Address::repeat_byte(0x11)
}
fn profile(chain: u64) -> DeploymentProfile {
    let pin = |n| CodePin::new(Address::repeat_byte(n), keccak256([0x60, 0])).unwrap();
    DeploymentProfile::new(
        Domain::new(chain, Address::repeat_byte(1)).unwrap(),
        Address::repeat_byte(3),
        pin(1),
        pin(2),
        pin(3),
        pin(4),
    )
    .unwrap()
}
/// The adapter pins no account code, so it never reads the chain.
struct TestRpc {
    chain: u64,
    reads: AtomicUsize,
}
#[tonic::async_trait]
impl Rpc for TestRpc {
    async fn call(&self, method: &str, _params: Value) -> Result<Value, ChainError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        panic!("unexpected RPC {method}")
    }
}
const ID: &str = "11111111-1111-4111-8111-111111111111";
/// A command, a channel it reports on as it starts and one it then waits on.
type Hold = (&'static str, mpsc::Sender<()>, mpsc::Receiver<()>);
/// What an execute notifies as it starts, and what it then waits for.
type Gate = (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
#[derive(Default)]
pub(crate) struct Service {
    chain: u64,
    fault: StdMutex<Option<&'static str>>,
    commands: StdMutex<Vec<(Vec<String>, bool)>>,
    transaction: StdMutex<Option<Value>>,
    /// The next execute notifies the first as it starts and then waits for
    /// the second before answering, without blocking its thread: Circle still
    /// executing.
    gate: StdMutex<Option<Gate>>,
    /// Each execute gets its own transaction id, as Circle's do, instead of ID.
    pub(crate) distinct: AtomicBool,
    /// The transaction hash each execute reports, instead of 0x08...08.
    pub(crate) hash: StdMutex<Option<B256>>,
    executed: AtomicUsize,
    /// A command, such as "wallet execute", that exits 1 with this stdout.
    failed: StdMutex<Option<(&'static str, &'static [u8])>>,
    /// A command that, the next time it runs, reports on the sender and then
    /// waits on the receiver before answering: a slow Circle call.
    held: StdMutex<Option<Hold>>,
}
#[tonic::async_trait]
impl CircleCommand for Service {
    async fn run(&self, args: Vec<String>, mutating: bool) -> Result<CommandOutput, CircleError> {
        self.commands.lock().unwrap().push((args.clone(), mutating));
        let held = {
            let mut held = self.held.lock().unwrap();
            if held
                .as_ref()
                .is_some_and(|(command, ..)| args.len() > 1 && args[..2].join(" ") == *command)
            {
                held.take()
            } else {
                None
            }
        };
        if let Some((_, entered, release)) = held {
            // Blocks this caller's thread, which the test gives its own runtime.
            entered.send(()).unwrap();
            release.recv_timeout(Duration::from_secs(30)).unwrap();
        }
        let fault = *self.fault.lock().unwrap();
        if args == ["--version"] {
            return Ok(CommandOutput {
                success: true,
                stdout: if fault == Some("version") {
                    b"1.1.5".to_vec()
                } else {
                    b"1.1.4\n".to_vec()
                },
            });
        }
        if fault == Some("auth") {
            return Ok(CommandOutput {
                success: false,
                stdout: br#"{"error":{"code":"AUTH_EXPIRED","message":"SECRET CLI OUTPUT"}}"#
                    .to_vec(),
            });
        }
        if let Some((command, stdout)) = *self.failed.lock().unwrap()
            && args[..2].join(" ") == command
        {
            return Ok(CommandOutput {
                success: false,
                stdout: stdout.to_vec(),
            });
        }
        let get = |name: &str| {
            args.iter()
                .position(|v| v == name)
                .map(|i| args[i + 1].clone())
                .unwrap()
        };
        assert_eq!(get("--chain"), blockchain(self.chain).unwrap());
        let data = if args[..2] == ["wallet", "list"] {
            assert!(!mutating);
            json!({"wallets":[{"address":encoded(if fault==Some("wallet") {Address::repeat_byte(9)} else {operator()}),"type":if fault==Some("type") {"local"} else {"agent"},"blockchain":blockchain(self.chain).unwrap()}]})
        } else if args[..2] == ["wallet", "execute"] {
            assert!(mutating);
            let gate = self.gate.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                entered.notify_one();
                release.notified().await;
            }
            if fault == Some("lost") {
                return Err(CircleError::Unavailable);
            }
            let direct = args[2] == "register()";
            let n = self.executed.fetch_add(1, Ordering::SeqCst);
            let id = if self.distinct.load(Ordering::SeqCst) {
                format!("{n:08x}-1111-4111-8111-111111111111")
            } else {
                ID.into()
            };
            let row = json!({"id":id,"idempotencyKey":get("--idempotency-key"),"state":"COMPLETE","blockchain":blockchain(self.chain).unwrap(),"operation":"CONTRACT_EXECUTION","transactionType":"OUTBOUND","sourceAddress":get("--address"),"contractAddress":get("--contract"),"abiFunctionSignature":args[2],"abiParameters":if direct {Value::Null} else {json!([args[3],args[4],args[5]])},"txHash":encoded(self.hash.lock().unwrap().unwrap_or(B256::repeat_byte(8)))});
            let mut row = row;
            if fault == Some("key") {
                row["idempotencyKey"] = json!("22222222-2222-4222-8222-222222222222");
            }
            if fault == Some("failed") {
                row["state"] = json!("FAILED");
                row["txHash"] = Value::Null;
            }
            *self.transaction.lock().unwrap() = Some(row.clone());
            row
        } else if args[..2] == ["transaction", "list"] {
            assert!(!mutating);
            let row = self.transaction.lock().unwrap().clone().unwrap();
            json!({"transactions":if fault==Some("duplicate") {vec![row.clone(),row]} else {vec![row]}})
        } else if args[..3] == ["wallet", "sign", "typed-data"] {
            assert!(mutating);
            json!({"signature":encoded(vec![0xab;97])})
        } else {
            panic!("unexpected CLI args {args:?}")
        };
        Ok(CommandOutput {
            success: true,
            stdout: serde_json::to_vec(&json!({"data":data})).unwrap(),
        })
    }
}
/// A clock the test moves: its monotonic and wall times advance together.
pub(crate) struct TestClock {
    monotonic: Instant,
    wall: SystemTime,
    elapsed: StdMutex<Duration>,
}
impl TestClock {
    /// A clock at the current time, as a process starting now reads it.
    pub(crate) fn now() -> Self {
        Self {
            monotonic: Instant::now(),
            wall: SystemTime::now(),
            elapsed: StdMutex::new(Duration::ZERO),
        }
    }
    pub(crate) fn advance(&self, by: Duration) {
        *self.elapsed.lock().unwrap() += by;
    }
}
impl Clock for TestClock {
    fn monotonic(&self) -> Instant {
        self.monotonic + *self.elapsed.lock().unwrap()
    }
    fn wall(&self) -> SystemTime {
        self.wall + *self.elapsed.lock().unwrap()
    }
}
/// A retirement mark as the journal keeps it: whole Unix seconds.
fn mark(time: SystemTime) -> Value {
    json!(unix_seconds(time).unwrap())
}
pub(crate) struct Fixture {
    directory: PathBuf,
    path: PathBuf,
    rpc: Arc<TestRpc>,
    pub(crate) service: Arc<Service>,
    pub(crate) clock: Arc<TestClock>,
}
impl Fixture {
    pub(crate) fn new(chain: u64) -> Self {
        let directory =
            std::env::temp_dir().join(format!("mppi-circle-agent-{}", fresh_key().unwrap()));
        std::fs::create_dir(&directory).unwrap();
        Self {
            path: directory.join("operations.json"),
            directory,
            rpc: Arc::new(TestRpc {
                chain,
                reads: AtomicUsize::new(0),
            }),
            service: Arc::new(Service {
                chain,
                ..Service::default()
            }),
            clock: Arc::new(TestClock::now()),
        }
    }
    /// A wallet on this fixture's journal and clock; each call makes a new one,
    /// as a restarted or second process would.
    pub(crate) fn wallet(
        &self,
        activate: bool,
        existing: bool,
    ) -> Result<CircleAgentWallet, CircleError> {
        self.wallet_with(
            CircleOptions {
                allow_registration_activation: activate,
                ..CircleOptions::default()
            },
            existing,
        )
    }
    /// A wallet whose CLI timeout is `timeout`, the longest its execute and
    /// reconcile wait for the journal's lock.
    fn wallet_within(
        &self,
        timeout: Duration,
        existing: bool,
    ) -> Result<CircleAgentWallet, CircleError> {
        self.wallet_with(
            CircleOptions {
                read_timeout_ms: timeout.as_millis() as u64,
                ..CircleOptions::default()
            },
            existing,
        )
    }
    fn wallet_with(
        &self,
        options: CircleOptions,
        existing: bool,
    ) -> Result<CircleAgentWallet, CircleError> {
        CircleAgentWallet::with_command(
            Arc::new(ChainReader::new(self.rpc.clone(), profile(self.rpc.chain))),
            operator(),
            &self.path,
            options,
            existing,
            self.service.clone(),
        )
        .map(|wallet| wallet.with_clock(self.clock.clone()))
    }
    /// The commands run since the last call, each by its first two words.
    fn verbs(&self) -> Vec<String> {
        let mut commands = self.service.commands.lock().unwrap();
        let verbs = commands
            .iter()
            .map(|(args, _)| args.iter().take(2).cloned().collect::<Vec<_>>().join(" "))
            .collect();
        commands.clear();
        verbs
    }
    fn executions(&self) -> usize {
        self.service
            .commands
            .lock()
            .unwrap()
            .iter()
            .filter(|(args, _)| args.get(1).is_some_and(|s| s == "execute"))
            .count()
    }
    pub(crate) fn row(&self) -> Value {
        serde_json::from_slice(&std::fs::read(&self.path).unwrap()).unwrap()
    }
    fn call(&self) -> WalletCall {
        WalletCall::new(
            self.rpc.chain,
            Address::repeat_byte(7),
            vec![0xde, 0xad, 0xbe, 0xef],
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}
/// `row` as releases before the digest kept it: the whole call, no
/// retirement mark, and before route selection no route.
fn whole(call: &WalletCall, row: &Value) -> Value {
    let mut row = row.clone();
    let fields = row.as_object_mut().unwrap();
    fields.remove("call_digest");
    fields.remove("retire_at");
    fields.remove("route");
    fields.insert(
        "call".into(),
        json!({"chain_id":call.chain_id(),"to":encoded(call.to()),"data":encoded(call.data())}),
    );
    row
}

#[tokio::test]
async fn exact_execution_on_both_networks_retains_id_and_never_reexecutes() {
    for chain in [5042, 5_042_002] {
        let f = Fixture::new(chain);
        let wallet = f.wallet(false, false).unwrap();
        let result = wallet.execute(&f.call(), "original").await.unwrap();
        assert_eq!(result.state(), OperationState::Pending);
        assert_eq!(result.wallet_operation_id(), Some(ID));
        assert_eq!(result.tx_hash(), Some(B256::repeat_byte(8)));
        let row = f.row();
        assert_eq!(row["kind"], "mppi-circle-agent-wallet");
        assert_eq!(
            row["operations"]["original"]["call_digest"],
            json!(encoded(digest(&f.call())))
        );
        uuid(
            row["operations"]["original"]["idempotency_key"]
                .as_str()
                .unwrap(),
            true,
        )
        .unwrap();
        let reopened = f.wallet(false, true).unwrap();
        assert_eq!(
            reopened
                .execute(&f.call(), "original")
                .await
                .unwrap()
                .tx_hash(),
            result.tx_hash()
        );
        assert_eq!(f.executions(), 1);
        let changed = WalletCall::new(chain, f.call().to(), vec![1]).unwrap();
        assert!(reopened.execute(&changed, "original").await.is_err());
        assert_eq!(f.executions(), 1);
        let wrong = WalletCall::new(
            if chain == 5042 { 5_042_002 } else { 5042 },
            f.call().to(),
            vec![1],
        )
        .unwrap();
        let refused = reopened.execute(&wrong, "other").await.unwrap();
        assert_eq!(refused.state(), OperationState::Failed);
        assert_eq!(f.executions(), 1);
    }
}
#[test]
fn both_adapters_bind_a_call_by_the_same_digest() {
    // The Python adapter's tests check this vector too: both keep one journal.
    let call = WalletCall::new(5042, Address::repeat_byte(7), vec![0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(
        encoded(digest(&call.unwrap())),
        "0xe4bdaec23c3fd8baf664d58d25c7b5f93f9ef58b77c4ce4902c6846b1289abf3"
    );
}
#[tokio::test]
async fn registration_activation_sends_only_its_exact_call_directly() {
    let f = Fixture::new(5042);
    let wallet = f.wallet(true, false).unwrap();
    wallet.verify_registration_wallet().await.unwrap();
    let string_register = RegistryCalls::new(5042, profile(5042).registry_address())
        .unwrap()
        .register(operator(), Some("https://provider.example/discover"))
        .unwrap();
    wallet
        .execute(string_register.call(), "uri-register")
        .await
        .unwrap();
    assert_eq!(f.row()["operations"]["uri-register"]["route"], "self");
    let f = Fixture::new(5042);
    let wallet = f.wallet(true, false).unwrap();
    let result = wallet
        .execute(&wallet.registration_call().unwrap(), "registration")
        .await
        .unwrap();
    assert_eq!(result.wallet_operation_id(), Some(ID));
    assert_eq!(
        f.row()["operations"]["registration"]["route"],
        "direct_register"
    );
    let unopted = f.wallet(false, false).unwrap();
    assert!(unopted.verify_registration_wallet().await.is_err());
    unopted.verify_wallet().await.unwrap(); // Any account generation, deployed or not.
    assert_eq!(f.executions(), 1);
    assert_eq!(f.rpc.reads.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn lost_or_mismatched_submission_result_never_acquires_a_history_guess() {
    for fault in ["lost", "key"] {
        let f = Fixture::new(5042);
        *f.service.fault.lock().unwrap() = Some(fault);
        let wallet = f.wallet(false, false).unwrap();
        let result = wallet.execute(&f.call(), "original").await.unwrap();
        assert_eq!(result.state(), OperationState::Unknown);
        assert_eq!(result.wallet_operation_id(), None);
        let cause = if fault == "lost" {
            "Unavailable"
        } else {
            "InvalidEvidence"
        };
        assert_eq!(result.cause(), Some(cause));
        *f.service.fault.lock().unwrap() = None;
        wallet.execute(&f.call(), "original").await.unwrap();
        wallet.reconcile("original").await.unwrap();
        assert_eq!(f.executions(), 1);
        assert!(
            f.service
                .commands
                .lock()
                .unwrap()
                .iter()
                .all(|(args, _)| args[0] != "transaction")
        );
    }
}
#[tokio::test]
async fn a_failed_command_keeps_only_its_listed_cli_code_as_the_cause() {
    // CLI 1.1.4 prints its error as JSON on stdout and exits 1 (dist/index.js:732-750).
    const TIMEOUT: &[u8] =
        br#"{"error":{"code":"TIMEOUT","message":"SECRET https://u:p@example.test"}}"#;
    const UNLISTED: &[u8] = br#"{"error":{"code":"SECRET_CODE","message":"SECRET CLI OUTPUT"}}"#;
    for (stdout, cause) in [
        (TIMEOUT, "Unavailable (TIMEOUT)"),
        (UNLISTED, "Unavailable"),
        (&b"Error: SECRET CLI OUTPUT"[..], "Unavailable"),
    ] {
        let f = Fixture::new(5042);
        *f.service.failed.lock().unwrap() = Some(("wallet execute", stdout));
        let wallet = f.wallet(false, false).unwrap();
        let result = wallet.execute(&f.call(), "original").await.unwrap();
        assert_eq!(result.state(), OperationState::Unknown);
        assert_eq!(
            (result.wallet_operation_id(), result.tx_hash()),
            (None, None)
        );
        assert_eq!(result.cause(), Some(cause));
        assert!(!format!("{result:?}").contains("SECRET"));
        assert_eq!(f.executions(), 1);
    }
    // A lookup refused for an expired login keeps the original handles.
    let f = Fixture::new(5042);
    let wallet = f.wallet(false, false).unwrap();
    let original = wallet.execute(&f.call(), "original").await.unwrap();
    *f.service.fault.lock().unwrap() = Some("auth");
    let result = wallet.reconcile("original").await.unwrap();
    assert_eq!(result.state(), OperationState::Unknown);
    assert_eq!(result.wallet_operation_id(), original.wallet_operation_id());
    assert_eq!(result.tx_hash(), original.tx_hash());
    assert_eq!(result.cause(), Some("Authentication (AUTH_EXPIRED)"));
    assert!(!format!("{result:?}").contains("SECRET"));
    // A failed listing without JSON is an unavailable command, not wrong evidence.
    *f.service.fault.lock().unwrap() = None;
    *f.service.failed.lock().unwrap() = Some(("wallet list", b"Error: SECRET CLI OUTPUT"));
    assert_eq!(wallet.verify_wallet().await, Err(CircleError::Unavailable));
    assert_eq!(f.executions(), 1);
}
#[tokio::test]
async fn an_execute_checks_the_cli_and_wallet_unless_a_check_began_within_60_seconds() {
    // CLI 1.1.4 starts a Node process per command and has no long-lived mode.
    const EXPIRED: &[u8] = br#"{"error":{"code":"AUTH_REQUIRED","message":"SECRET CLI OUTPUT"}}"#;
    let f = Fixture::new(5042);
    f.service.distinct.store(true, Ordering::SeqCst);
    let wallet = f.wallet(false, false).unwrap();
    let typed = json!({"domain":{"chainId":5042}});
    wallet.verify_wallet().await.unwrap();
    wallet.sign_typed_data(&typed).await.unwrap();
    f.clock.advance(Duration::from_secs(59));
    wallet.execute(&f.call(), "first").await.unwrap();
    assert_eq!(
        f.verbs(),
        ["--version", "wallet list", "wallet sign", "wallet execute"]
    );
    // Once the check is 60 seconds old an execute checks first; clones share
    // the record it sets.
    f.clock.advance(Duration::from_secs(1));
    wallet.clone().execute(&f.call(), "second").await.unwrap();
    wallet.execute(&f.call(), "third").await.unwrap();
    assert_eq!(
        f.verbs(),
        [
            "--version",
            "wallet list",
            "wallet execute",
            "wallet execute"
        ]
    );
    // Signing and verify_wallet accept the last passing check whatever its age.
    f.clock.advance(Duration::from_secs(3600));
    wallet.verify_wallet().await.unwrap();
    wallet.sign_typed_data(&typed).await.unwrap();
    assert_eq!(f.verbs(), ["wallet sign"]);
    // A login lost before the check is due fails the execute's own check,
    // before its journal save: definitely not submitted.
    *f.service.failed.lock().unwrap() = Some(("wallet list", EXPIRED));
    let result = wallet.execute(&f.call(), "fourth").await.unwrap();
    assert_eq!(result.state(), OperationState::Failed);
    assert_eq!(
        result.cause(),
        Some("NotSubmittedError: Authentication (AUTH_REQUIRED)")
    );
    assert_eq!(f.verbs(), ["--version", "wallet list"]);
    assert!(f.row()["operations"].get("fourth").is_none());
    // A login lost within 60 seconds of a passing check reaches the CLI's own
    // refusal after the save: unknown, with no transaction id to reconcile.
    *f.service.failed.lock().unwrap() = None;
    wallet.verify_wallet().await.unwrap();
    *f.service.failed.lock().unwrap() = Some(("wallet execute", EXPIRED));
    let lost = wallet.execute(&f.call(), "fifth").await.unwrap();
    assert_eq!(lost.state(), OperationState::Unknown);
    assert_eq!((lost.wallet_operation_id(), lost.tx_hash()), (None, None));
    assert_eq!(lost.cause(), Some("Authentication (AUTH_REQUIRED)"));
    assert_eq!(f.verbs(), ["--version", "wallet list", "wallet execute"]);
    // That failure cleared the record, so the next signature checks first.
    *f.service.failed.lock().unwrap() = None;
    wallet.sign_typed_data(&typed).await.unwrap();
    assert_eq!(f.verbs(), ["--version", "wallet list", "wallet sign"]);
}
#[tokio::test]
async fn a_refused_check_leaves_no_record_for_signing() {
    let typed = json!({"domain":{"chainId":5042}});
    for fault in ["version", "wallet", "type"] {
        let f = Fixture::new(5042);
        let wallet = f.wallet(false, false).unwrap();
        wallet.verify_wallet().await.unwrap();
        f.clock.advance(Duration::from_secs(60));
        // The execute's check is refused: another CLI version, or a listing
        // without this agent wallet. The earlier pass no longer counts.
        *f.service.fault.lock().unwrap() = Some(fault);
        let refused = wallet.execute(&f.call(), "refused").await.unwrap();
        assert_eq!(refused.state(), OperationState::Failed, "{fault}");
        *f.service.fault.lock().unwrap() = None;
        f.verbs();
        wallet.verify_wallet().await.unwrap();
        wallet.sign_typed_data(&typed).await.unwrap();
        assert_eq!(
            f.verbs(),
            ["--version", "wallet list", "wallet sign"],
            "{fault}"
        );
    }
}
#[tokio::test]
async fn hashless_service_failure_and_ambiguous_history_cannot_authorize_replacement() {
    let f = Fixture::new(5042);
    *f.service.fault.lock().unwrap() = Some("failed");
    let wallet = f.wallet(false, false).unwrap();
    let first = wallet.execute(&f.call(), "original").await.unwrap();
    assert_eq!(first.state(), OperationState::Unknown);
    assert_eq!(first.wallet_operation_id(), Some(ID));
    *f.service.fault.lock().unwrap() = Some("duplicate");
    let result = wallet.reconcile("original").await.unwrap();
    assert_eq!(result.state(), OperationState::Unknown);
    assert_eq!(result.wallet_operation_id(), Some(ID));
    assert_eq!(f.executions(), 1);
}
#[tokio::test]
async fn altered_history_keeps_original_handles_without_accepting_new_evidence() {
    for (field, value) in [
        ("sourceAddress", json!(encoded(Address::repeat_byte(9)))),
        ("contractAddress", json!(encoded(Address::repeat_byte(9)))),
        (
            "abiParameters",
            json!([encoded(Address::repeat_byte(7)), "1", "0xdeadbeef"]),
        ),
        ("txHash", json!(encoded(B256::repeat_byte(9)))),
        ("blockchain", json!("ARC-TESTNET")),
        ("state", json!("invented")),
    ] {
        let f = Fixture::new(5042);
        let wallet = f.wallet(false, false).unwrap();
        wallet.execute(&f.call(), "original").await.unwrap();
        f.service.transaction.lock().unwrap().as_mut().unwrap()[field] = value;
        let result = wallet.reconcile("original").await.unwrap();
        assert_eq!(result.state(), OperationState::Unknown);
        assert_eq!(result.tx_hash(), Some(B256::repeat_byte(8)));
        assert_eq!(result.wallet_operation_id(), Some(ID));
        assert_eq!(f.executions(), 1);
    }
}
#[tokio::test]
async fn version_session_and_identity_are_required_before_dispatch() {
    for (fault, cause) in [
        ("version", "Version"),
        ("auth", "Authentication (AUTH_EXPIRED)"),
        ("wallet", "InvalidEvidence"),
        ("type", "InvalidEvidence"),
    ] {
        let f = Fixture::new(5042);
        *f.service.fault.lock().unwrap() = Some(fault);
        let wallet = f.wallet(false, false).unwrap();
        let error = wallet.verify_wallet().await.unwrap_err();
        assert!(!error.to_string().contains("SECRET"));
        if fault == "auth" {
            assert_eq!(error, CircleError::Authentication);
            assert!(error.to_string().contains("circle wallet login"));
        }
        // Nothing was journaled, so Circle was never asked: a definite failure.
        let result = wallet.execute(&f.call(), "original").await.unwrap();
        assert_eq!(result.state(), OperationState::Failed);
        assert_eq!(
            (result.wallet_operation_id(), result.tx_hash()),
            (None, None)
        );
        assert_eq!(
            result.cause(),
            Some(&*format!("NotSubmittedError: {cause}"))
        );
        assert_eq!(f.row()["operations"], json!({}));
        assert_eq!(f.executions(), 0);
        // A reference with no row proves nothing about any earlier record.
        let later = wallet.reconcile("original").await.unwrap();
        assert_eq!(later.state(), OperationState::Unknown);
    }
}
#[tokio::test]
async fn only_a_readable_journal_without_the_row_proves_nothing_was_sent() {
    let f = Fixture::new(5042);
    let wallet = f.wallet(false, false).unwrap();
    // A save that cannot land before handoff is still a definite failure.
    crate::local_state::fail_next_write(&f.path);
    let result = wallet.execute(&f.call(), "original").await.unwrap();
    assert_eq!(result.state(), OperationState::Failed);
    assert_eq!(result.cause(), Some("NotSubmittedError: Journal"));
    // An unreadable journal could hold an earlier submission: no proof. One
    // another process holds past the execute's timeout is such a journal: the
    // ordinary journal error, and Circle is asked nothing.
    let wallet = f.wallet_within(Duration::from_millis(300), true).unwrap();
    f.verbs();
    let held = LocalState::open(&f.path, MAX_JOURNAL).unwrap();
    let started = Instant::now();
    assert!(wallet.execute(&f.call(), "original").await.is_err());
    assert!(started.elapsed() >= Duration::from_millis(300));
    drop(held);
    assert!(f.verbs().is_empty(), "Circle was asked something");
    assert_eq!(f.row()["operations"], json!({}));
}
/// Another process holding the journal at `path` for 1.5 seconds, from
/// when this returns.
fn held_for_a_while(path: &Path) -> std::thread::JoinHandle<()> {
    let (held, waiting) = mpsc::channel();
    let path = path.to_owned();
    let holder = std::thread::spawn(move || {
        let store = LocalState::open(&path, MAX_JOURNAL).unwrap();
        held.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(1500));
        drop(store);
    });
    waiting.recv().unwrap();
    holder
}
#[tokio::test]
async fn execute_and_reconcile_wait_for_the_journal_up_to_their_own_timeout() {
    let f = Fixture::new(5042);
    let wallet = f.wallet(false, false).unwrap();
    // Another process holds the journal for longer than the one second other
    // records wait for, but within the CLI timeout (240 s by default).
    let holder = held_for_a_while(&f.path);
    let executed = wallet.execute(&f.call(), "original").await.unwrap();
    holder.join().unwrap();
    assert_eq!(
        executed.state(),
        OperationState::Pending,
        "{:?}",
        executed.cause()
    );
    let holder = held_for_a_while(&f.path);
    assert_eq!(wallet.reconcile("original").await.unwrap(), executed);
    holder.join().unwrap();
    assert_eq!(f.executions(), 1);
}
/// A current-thread runtime of its own, for a caller on its own thread.
fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
/// An execute of `reference` whose record step another process keeps out
/// past the 300 ms timeout of the wallet returned: Unknown with Circle's
/// transaction id and hash, which that wallet keeps for its next journal
/// step.
fn kept_answer(f: &Fixture, reference: &'static str) -> (CircleAgentWallet, OperationResult) {
    let (entered, held) = mpsc::channel();
    let (release, waiting) = mpsc::channel();
    *f.service.held.lock().unwrap() = Some(("wallet execute", entered, waiting));
    let wallet = f.wallet_within(Duration::from_millis(300), false).unwrap();
    let (slow, call) = (wallet.clone(), f.call());
    let slow =
        std::thread::spawn(move || current_thread().block_on(slow.execute(&call, reference)));
    held.recv_timeout(Duration::from_secs(10)).unwrap();
    // Another process takes the journal while Circle executes, and keeps it
    // past the execute's timeout.
    let other = LocalState::open(&f.path, MAX_JOURNAL).unwrap();
    release.send(()).unwrap();
    let result = slow.join().unwrap().unwrap();
    drop(other);
    (wallet, result)
}
#[test]
fn a_result_the_journal_cannot_take_is_returned_and_written_at_the_next_step() {
    let f = Fixture::new(5042);
    let runtime = current_thread;
    let (wallet, result) = kept_answer(&f, "settle:x");
    // Unknown, with Circle's transaction id and hash for the runtime to check.
    assert_eq!(result.state(), OperationState::Unknown);
    assert_eq!(
        (result.wallet_operation_id(), result.tx_hash()),
        (Some(ID), Some(B256::repeat_byte(8)))
    );
    assert_eq!(result.cause(), Some("Journal"));
    let row = || f.row()["operations"]["settle:x"].clone();
    assert_eq!(
        (
            row()["wallet_operation_id"].clone(),
            row()["tx_hash"].clone()
        ),
        (Value::Null, Value::Null)
    );
    // The adapter's next journal step writes them, here a retire that names
    // no reference.
    runtime()
        .block_on(wallet.retire(&[], f.clock.wall()))
        .unwrap();
    assert_eq!(
        (
            row()["wallet_operation_id"].clone(),
            row()["tx_hash"].clone()
        ),
        (json!(ID), json!(encoded(B256::repeat_byte(8))))
    );
    // So a later reconcile, even from another adapter object, finds the
    // transaction in Circle's history.
    let reopened = f.wallet(false, true).unwrap();
    let found = runtime().block_on(reopened.reconcile("settle:x")).unwrap();
    assert_eq!(found.state(), OperationState::Pending);
    assert_eq!(
        (found.wallet_operation_id(), found.tx_hash()),
        (Some(ID), Some(B256::repeat_byte(8)))
    );
    assert_eq!(f.executions(), 1);
}
#[tokio::test]
async fn journal_scope_fields_unique_keys_and_symlinks_fail_without_replacement() {
    let f = Fixture::new(5042);
    assert!(f.wallet(false, true).is_err());
    assert!(!f.path.exists());
    let wallet = f.wallet(false, false).unwrap();
    wallet.execute(&f.call(), "original").await.unwrap();
    let original = f.row();
    for mut altered in [
        original.clone(),
        original.clone(),
        original.clone(),
        original.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        match altered.0 {
            0 => altered.1["wallet"] = json!(encoded(Address::repeat_byte(9))),
            1 => {
                altered.1["operations"]["original"]
                    .as_object_mut()
                    .unwrap()
                    .remove("tx_hash");
            }
            2 => altered.1["operations"]["duplicate"] = original["operations"]["original"].clone(),
            _ => altered.1["operations"]["original"]["route"] = json!("direct_register"),
        };
        let raw = serde_json::to_vec(&altered.1).unwrap();
        std::fs::write(&f.path, &raw).unwrap();
        assert!(f.wallet(false, true).is_err());
        assert_eq!(std::fs::read(&f.path).unwrap(), raw);
    }
    std::fs::write(&f.path, serde_json::to_vec(&original).unwrap()).unwrap();
    #[cfg(unix)]
    {
        let retained = f.directory.join("retained");
        std::fs::rename(&f.path, &retained).unwrap();
        std::os::unix::fs::symlink(&retained, &f.path).unwrap();
        assert!(f.wallet(false, true).is_err());
    }
}
#[tokio::test]
async fn version_1_shared_record_keeps_its_operations_and_drops_account_pins() {
    let f = Fixture::new(5042);
    let wallet = f.wallet(false, false).unwrap();
    let result = wallet.execute(&f.call(), "original").await.unwrap();
    let current = f.row();
    assert_eq!(current["version"], 2);
    assert_eq!(current["wallet"], json!(encoded(operator())));
    let mut legacy = current.clone();
    legacy["version"] = json!(1);
    legacy["wallet"] =
        json!({"address":encoded(operator()),"code_hash":encoded(B256::repeat_byte(2))});
    legacy["implementation"] = json!({"address":encoded(Address::repeat_byte(0x55)),"code_hash":encoded(B256::repeat_byte(1))});
    legacy["operations"]["original"] = whole(&f.call(), &current["operations"]["original"]);
    std::fs::write(&f.path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    let restored = f.wallet(false, true).unwrap();
    assert_eq!(restored.reconcile("original").await.unwrap(), result);
    assert_eq!(f.row(), current); // The reconciliation saved it as version 2, compact.
    legacy["wallet"]["address"] = json!(encoded(Address::repeat_byte(9)));
    std::fs::write(&f.path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    assert!(f.wallet(false, true).is_err());
}
#[tokio::test]
async fn an_earlier_whole_call_record_keeps_binding_its_call_by_digest() {
    let f = Fixture::new(5042);
    let wallet = f.wallet(false, false).unwrap();
    let result = wallet.execute(&f.call(), "original").await.unwrap();
    let current = f.row();
    let mut earlier = current.clone();
    earlier["operations"]["original"] = whole(&f.call(), &current["operations"]["original"]);
    let raw = serde_json::to_vec(&earlier).unwrap();
    std::fs::write(&f.path, &raw).unwrap();
    let restored = f.wallet(false, true).unwrap();
    assert_eq!(std::fs::read(&f.path).unwrap(), raw, "opening rewrote it");
    let changed = WalletCall::new(5042, f.call().to(), vec![1]).unwrap();
    assert!(restored.execute(&changed, "original").await.is_err());
    assert_eq!(restored.reconcile("original").await.unwrap(), result);
    assert_eq!(f.row(), current, "the next save keeps only the digest");
    assert!(restored.execute(&changed, "original").await.is_err());
    assert_eq!(
        restored.execute(&f.call(), "original").await.unwrap(),
        result
    );
    assert_eq!(f.executions(), 1);
    // A record with both its call and a digest is neither.
    earlier["operations"]["original"]["call_digest"] =
        current["operations"]["original"]["call_digest"].clone();
    std::fs::write(&f.path, serde_json::to_vec(&earlier).unwrap()).unwrap();
    assert!(f.wallet(false, true).is_err());
}
#[tokio::test]
async fn retired_records_leave_the_journal_so_operations_never_stop_at_a_count() {
    let f = Fixture::new(5042);
    f.service.distinct.store(true, Ordering::SeqCst);
    let wallet = f.wallet(false, false).unwrap();
    // 5,000 operations over 1,000 sessions of five, each session's retired once
    // it ends: past the 4,096 records the adapter once refused beyond.
    for session in 0..1000 {
        let references = (0..5)
            .map(|n| format!("settle:{session}:{n}"))
            .collect::<Vec<_>>();
        for reference in &references {
            let result = wallet.execute(&f.call(), reference).await.unwrap();
            assert_eq!(
                result.state(),
                OperationState::Pending,
                "{reference}: {:?}",
                result.cause()
            );
        }
        wallet.retire(&references, f.clock.wall()).await.unwrap();
    }
    assert_eq!(f.executions(), 5000);
    assert_eq!(f.row()["operations"], json!({}));
    assert!(!wallet.near_capacity());
}
#[tokio::test]
async fn retire_keeps_every_record_whose_call_may_still_reach_circle() {
    let f = Fixture::new(5042);
    f.service.distinct.store(true, Ordering::SeqCst);
    let wallet = f.wallet(false, false).unwrap();
    wallet.execute(&f.call(), "hashed").await.unwrap();
    // Circle's answer was lost, so no transaction id is known.
    *f.service.fault.lock().unwrap() = Some("lost");
    let lost = wallet.execute(&f.call(), "lost").await.unwrap();
    // Circle reported a failure, but no transaction hash.
    *f.service.fault.lock().unwrap() = Some("failed");
    let hashless = wallet.execute(&f.call(), "hashless").await.unwrap();
    *f.service.fault.lock().unwrap() = None;
    let references = ["hashed", "lost", "hashless", "unrecorded"].map(String::from);
    wallet.retire(&references, f.clock.wall()).await.unwrap();
    let kept = f.row()["operations"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(kept, ["hashless", "lost"]);
    // Each keeps its mark, so it is forgotten once it holds a hash.
    for reference in kept {
        assert_eq!(
            f.row()["operations"][reference]["retire_at"],
            mark(f.clock.wall())
        );
    }
    // Each still only reconciles its original intent.
    assert_eq!(wallet.execute(&f.call(), "lost").await.unwrap(), lost);
    assert_eq!(
        wallet.execute(&f.call(), "hashless").await.unwrap(),
        hashless
    );
    assert_eq!(f.executions(), 3);
    let changed = WalletCall::new(5042, f.call().to(), vec![1]).unwrap();
    assert!(wallet.execute(&changed, "lost").await.is_err());
    // The retired one answers as any reference with no record: unknown.
    let retired = wallet.reconcile("hashed").await.unwrap();
    assert_eq!(retired.state(), OperationState::Unknown);
    assert_eq!(
        (retired.wallet_operation_id(), retired.tx_hash()),
        (None, None)
    );
}
#[tokio::test]
async fn a_compaction_that_cannot_replace_the_journal_leaves_it_whole() {
    let f = Fixture::new(5042);
    f.service.distinct.store(true, Ordering::SeqCst);
    let wallet = f.wallet(false, false).unwrap();
    let references = ["first", "second"].map(String::from);
    for reference in &references {
        wallet.execute(&f.call(), reference).await.unwrap();
    }
    let before = std::fs::read(&f.path).unwrap();
    // The replacement cannot be renamed into place: as after a crash before
    // its rename, the journal is the earlier one, whole.
    crate::local_state::fail_next_write(&f.path);
    let failed = wallet.retire(&references, f.clock.wall()).await;
    assert!(failed.is_err());
    assert_eq!(std::fs::read(&f.path).unwrap(), before);
    // A crash after writing a replacement but before its rename leaves only a
    // temporary file beside the journal, which nothing reads.
    std::fs::write(f.directory.join(".mppi-crashed.tmp"), b"{\"operations\":").unwrap();
    let reopened = f.wallet(false, true).unwrap();
    let first = reopened.reconcile("first").await.unwrap();
    assert_eq!(
        first.wallet_operation_id(),
        Some("00000000-1111-4111-8111-111111111111")
    );
    reopened.retire(&references, f.clock.wall()).await.unwrap();
    assert_eq!(f.row()["operations"], json!({}));
    assert_eq!(f.executions(), 2);
}
/// Records added to the journal at `f.path`, as copies of its `original` with
/// their own keys and ids, until it is within one record of `size` bytes.
/// Every one, `original` included, is marked to retire in 2100, so none needs
/// room for a mark.
fn grow(f: &Fixture, size: usize) {
    let mut journal = f.row();
    journal["operations"]["original"]["retire_at"] = json!(4_102_444_800u64);
    let template = journal["operations"]["original"].clone();
    let row = |template: &Value| {
        let mut row = template.clone();
        row["idempotency_key"] = json!(fresh_key().unwrap());
        row["wallet_operation_id"] = json!(fresh_key().unwrap());
        row
    };
    // Compact JSON: each further member adds `,"<reference>":<row>`.
    let mut total = serde_json::to_vec(&journal).unwrap().len();
    for n in 0.. {
        let next = row(&template);
        let fixed = serde_json::to_vec(&next).unwrap().len() + 4;
        let Some(room) = size.checked_sub(total + fixed + 8) else {
            break;
        };
        let reference = format!("{n:08}{}", "r".repeat(room.min(4000)));
        total += fixed + reference.len();
        journal["operations"][reference] = next;
    }
    let raw = serde_json::to_vec(&journal).unwrap();
    assert_eq!(raw.len(), total);
    std::fs::write(&f.path, raw).unwrap();
}
#[tokio::test]
async fn near_capacity_from_three_quarters_and_nothing_reaches_circle_past_the_bound() {
    let f = Fixture::new(5042);
    let wallet = f.wallet(false, false).unwrap();
    wallet.execute(&f.call(), "original").await.unwrap();
    assert!(!wallet.near_capacity());
    grow(&f, MAX_JOURNAL / 4 * 3 + 1000);
    wallet.reconcile("original").await.unwrap();
    assert!(wallet.near_capacity(), "read at three quarters");
    assert!(wallet.clone().near_capacity(), "clones share it");
    let filler = f.row()["operations"]
        .as_object()
        .unwrap()
        .keys()
        .filter(|reference| *reference != "original")
        .cloned()
        .collect::<Vec<_>>();
    wallet.retire(&filler, f.clock.wall()).await.unwrap();
    assert!(!wallet.near_capacity(), "written below three quarters");
    // At the bound a new call's record does not fit: never sent to Circle.
    grow(&f, MAX_JOURNAL);
    let raw = std::fs::read(&f.path).unwrap();
    let result = wallet
        .execute(&f.call(), &"over".repeat(250))
        .await
        .unwrap();
    assert_eq!(result.state(), OperationState::Failed);
    assert_eq!(result.cause(), Some("NotSubmittedError: Capacity"));
    assert_eq!(f.executions(), 1);
    assert_eq!(std::fs::read(&f.path).unwrap(), raw);
    assert!(wallet.near_capacity());
}
#[tokio::test]
async fn the_last_record_keeps_room_for_the_id_and_hash_circle_returns() {
    let f = Fixture::new(5042);
    f.service.distinct.store(true, Ordering::SeqCst);
    let wallet = f.wallet(false, false).unwrap();
    wallet.execute(&f.call(), "original").await.unwrap();
    grow(&f, MAX_JOURNAL - 2000);
    let size = std::fs::read(&f.path).unwrap().len();
    // A new member `,"<reference>":<row>`, whose row has no id, hash or mark yet.
    let row = json!({"route":"self","call_digest":encoded(B256::ZERO),"idempotency_key":fresh_key().unwrap(),
        "wallet_operation_id":null,"tx_hash":null,"state":"unknown","retire_at":null});
    let member = serde_json::to_vec(&row).unwrap().len() + 4;
    let reference = |free: usize| "r".repeat(MAX_JOURNAL - free - size - member);
    // Room for the record, not for the id and hash Circle returns and a
    // retirement mark of up to 20 digits (34 + 64 + 16 bytes): unsent.
    let refused = wallet.execute(&f.call(), &reference(113)).await.unwrap();
    assert_eq!(refused.cause(), Some("NotSubmittedError: Capacity"));
    assert_eq!(f.executions(), 1);
    // Room for all three: sent, and its id and hash recorded, 16 bytes short
    // of the bound, which its mark may take.
    let last = reference(114);
    let result = wallet.execute(&f.call(), &last).await.unwrap();
    assert_eq!(
        (result.wallet_operation_id(), result.tx_hash()),
        (
            Some("00000001-1111-4111-8111-111111111111"),
            Some(B256::repeat_byte(8))
        )
    );
    assert_eq!(std::fs::read(&f.path).unwrap().len(), MAX_JOURNAL - 16);
    let later = f.clock.wall() + Duration::from_secs(86_400);
    wallet.retire(&[last], later).await.unwrap();
    assert!(std::fs::read(&f.path).unwrap().len() <= MAX_JOURNAL);
}
#[tokio::test]
async fn cancelled_caller_does_not_kill_or_forget_original_execution() {
    let f = Fixture::new(5042);
    let (entered, release) = (
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    );
    *f.service.gate.lock().unwrap() = Some((entered.clone(), release.clone()));
    let wallet = f.wallet(false, false).unwrap();
    let call = f.call();
    let retained = wallet.clone();
    let task = tokio::spawn(async move { retained.execute(&call, "original").await });
    // The caller is cancelled while Circle is still executing.
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("execute never reached Circle");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let id = || f.row()["operations"]["original"]["wallet_operation_id"].clone();
    assert_eq!(id(), Value::Null);
    // The original execution goes on once Circle answers, holding the
    // adapter's lock until it has recorded that answer.
    release.notify_one();
    drop(wallet.lock.lock().await);
    assert_eq!(id(), ID);
    assert_eq!(f.executions(), 1);
    assert_eq!(
        wallet.reconcile("original").await.unwrap().tx_hash(),
        Some(B256::repeat_byte(8))
    );
}
#[tokio::test]
async fn typed_data_preserves_complete_contract_signature_and_rejects_wrong_chain() {
    let f = Fixture::new(5042);
    let wallet = f.wallet(false, false).unwrap();
    assert_eq!(
        wallet
            .sign_typed_data(&json!({"domain":{"chainId":5042}}))
            .await
            .unwrap(),
        vec![0xab; 97]
    );
    assert!(
        wallet
            .sign_typed_data(&json!({"domain":{"chainId":5042002}}))
            .await
            .is_err()
    );
}
#[test]
fn mutating_process_finishes_after_read_deadline_and_bounds_both_pipes() {
    let options = CircleOptions {
        command: vec!["/bin/sh".into(), "-c".into()],
        read_timeout_ms: 5,
        ..CircleOptions::default()
    };
    let result = run_process(
        options.clone(),
        vec!["sleep 0.03; printf completed".into()],
        true,
    )
    .unwrap();
    assert!(result.success);
    assert_eq!(result.stdout, b"completed");
    assert!(run_process(options, vec!["sleep 1".into()], false).is_err());
    assert!(drain(&b"12345"[..], 4).is_err());
    assert_eq!(drain(&b"1234"[..], 4).unwrap(), b"1234");
}
#[test]
fn a_slow_circle_call_holds_no_journal_lock_and_every_row_survives() {
    // Two wallets on one journal, each on its own thread and runtime, share
    // nothing but the journal's file lock, as two processes would: the
    // lock belongs to each open file description (flock(2)).
    let f = Fixture::new(5042);
    f.service.distinct.store(true, Ordering::SeqCst);
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    };
    let (entered, held) = mpsc::channel();
    let (release, waiting) = mpsc::channel();
    *f.service.held.lock().unwrap() = Some(("wallet execute", entered, waiting));
    let first = f.wallet(false, false).unwrap();
    let call = f.call();
    let slow = std::thread::spawn(move || runtime().block_on(first.execute(&call, "slow")));
    held.recv_timeout(Duration::from_secs(10)).unwrap();
    // Meanwhile the other saves its intent, records its result and
    // reconciles it, all while the first waits inside Circle's call.
    let second = f.wallet(false, true).unwrap();
    let local = runtime();
    let quick = local.block_on(second.execute(&f.call(), "quick")).unwrap();
    assert_eq!(quick.state(), OperationState::Pending);
    assert_eq!(
        local.block_on(second.reconcile("quick")).unwrap(),
        quick,
        "reconciled from Circle's history"
    );
    release.send(()).unwrap();
    let slow = slow.join().unwrap().unwrap();
    assert_eq!(slow.state(), OperationState::Pending);
    // Neither lost the other's record, and both hold their handles.
    let rows = f.row()["operations"].clone();
    for (reference, result) in [("slow", &slow), ("quick", &quick)] {
        assert_eq!(
            rows[reference]["wallet_operation_id"],
            json!(result.wallet_operation_id().unwrap())
        );
        assert_eq!(
            rows[reference]["tx_hash"],
            json!(encoded(result.tx_hash().unwrap()))
        );
    }
    assert_ne!(slow.wallet_operation_id(), quick.wallet_operation_id());
    assert_eq!(f.executions(), 2);
}
#[tokio::test]
async fn a_call_refused_under_an_unbound_reference_is_a_definite_failure() {
    use crate::{contract::ContractCalls, signing::SessionDescriptor};
    let calls = ContractCalls::new(
        Domain::new(5042, Address::repeat_byte(1)).unwrap(),
        "USDC".into(),
        "2".into(),
    )
    .unwrap();
    let descriptor = SessionDescriptor {
        agent: Address::repeat_byte(0x21),
        sessionSigner: Address::repeat_byte(0x22),
        providerId: U256::from(7),
        recipient: Address::repeat_byte(0x23),
        operator: operator(),
        token: crate::contract::ARC_USDC,
        salt: B256::repeat_byte(0x24),
    };
    let signature = [0x1b; 65];
    // A settlement of 137 sessions in one batch is over the CLI's call bound.
    let batch = calls
        .settle_batch(&vec![(&descriptor, 100, 50, &signature[..]); 137])
        .unwrap();
    assert_eq!(batch.call().data().len(), 61_636);
    assert!(batch.call().data().len() > MAX_CALL);
    let other_network = WalletCall::new(5_042_002, Address::repeat_byte(7), vec![1]).unwrap();
    let f = Fixture::new(5042);
    let wallet = f.wallet(false, false).unwrap();
    let long = "r".repeat(4097);
    for (call, reference) in [
        (batch.call(), "settle:batch"),
        (&other_network, "settle:other"),
        (&f.call(), long.as_str()),
    ] {
        // Refused before any record of it was saved: never sent.
        let result = wallet.execute(call, reference).await.unwrap();
        assert_eq!(result.state(), OperationState::Failed);
        assert_eq!(
            (result.wallet_operation_id(), result.tx_hash()),
            (None, None)
        );
        assert_eq!(result.cause(), Some("NotSubmittedError: Configuration"));
    }
    assert_eq!(f.row()["operations"], json!({}));
    assert!(f.verbs().is_empty(), "Circle was asked nothing");
    // A reference that binds another call is still a refused reuse: that
    // call may have reached Circle.
    wallet.execute(&f.call(), "bound").await.unwrap();
    assert!(wallet.execute(batch.call(), "bound").await.is_err());
    assert_eq!(f.executions(), 1);
}
#[tokio::test]
async fn a_save_that_landed_but_reported_failure_leaves_no_binding() {
    let f = Fixture::new(5042);
    let wallet = f.wallet(false, false).unwrap();
    // The intent is renamed into place, then syncing the directory fails.
    crate::local_state::fail_next_write_after_rename(&f.path);
    let first = wallet.execute(&f.call(), "close:x").await.unwrap();
    assert_eq!(first.state(), OperationState::Failed);
    assert_eq!(first.cause(), Some("NotSubmittedError: Journal"));
    assert_eq!(
        f.row()["operations"],
        json!({}),
        "the landed intent is gone"
    );
    // So the reference may carry a newly prepared call, as a close does.
    let prepared = WalletCall::new(5042, Address::repeat_byte(7), vec![0xde, 0xad]).unwrap();
    let second = wallet.execute(&prepared, "close:x").await.unwrap();
    assert_eq!(second.state(), OperationState::Pending);
    assert_eq!(f.executions(), 1);
    // A journal that cannot be shown free of the intent proves nothing: the
    // removal is reported failed too, so the outcome is unknown.
    crate::local_state::fail_next_write_after_rename(&f.path);
    crate::local_state::fail_next_write_after_rename(&f.path);
    let third = wallet.execute(&f.call(), "close:y").await.unwrap();
    assert_eq!(third.state(), OperationState::Unknown);
    assert_eq!((third.wallet_operation_id(), third.tx_hash()), (None, None));
    assert_eq!(third.cause(), Some("Journal"));
    assert_eq!(f.executions(), 1);
}
#[tokio::test]
async fn a_retirement_mark_survives_a_restart_and_the_first_save_after_it_forgets() {
    let f = Fixture::new(5042);
    f.service.distinct.store(true, Ordering::SeqCst);
    let wallet = f.wallet(false, false).unwrap();
    wallet.execute(&f.call(), "retired").await.unwrap();
    *f.service.fault.lock().unwrap() = Some("lost");
    wallet.execute(&f.call(), "hashless").await.unwrap();
    *f.service.fault.lock().unwrap() = None;
    let due = f.clock.wall() + Duration::from_secs(3600);
    let references = ["retired", "hashless", "unrecorded"].map(String::from);
    wallet.retire(&references, due).await.unwrap();
    // A later time keeps the earlier mark.
    wallet
        .retire(&references, due + Duration::from_secs(60))
        .await
        .unwrap();
    for reference in ["retired", "hashless"] {
        assert_eq!(f.row()["operations"][reference]["retire_at"], mark(due));
    }
    drop(wallet);
    let restarted = f.wallet(false, true).unwrap();
    restarted.execute(&f.call(), "before").await.unwrap();
    assert!(
        f.row()["operations"].get("retired").is_some(),
        "not yet due"
    );
    f.clock.advance(Duration::from_secs(3600));
    restarted.execute(&f.call(), "after").await.unwrap();
    let kept = f.row()["operations"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    // Due and holding a hash: forgotten. Without one it keeps its mark.
    assert_eq!(kept, ["after", "before", "hashless"]);
    assert_eq!(f.row()["operations"]["hashless"]["retire_at"], mark(due));
    // A time before the Unix epoch is refused.
    assert!(
        restarted
            .retire(&references, UNIX_EPOCH - Duration::from_secs(1))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn retire_skips_a_reference_no_record_can_have() {
    let f = Fixture::new(5042);
    let wallet = f.wallet(false, false).unwrap();
    wallet.execute(&f.call(), "done").await.unwrap();
    let references = ["".into(), "r".repeat(4097), "done".into()];
    wallet.retire(&references, f.clock.wall()).await.unwrap();
    assert_eq!(f.row()["operations"], json!({}));
}
#[tokio::test]
async fn a_reconcile_withdraws_a_retirement_mark() {
    let f = Fixture::new(5042);
    f.service.distinct.store(true, Ordering::SeqCst);
    let wallet = f.wallet(false, false).unwrap();
    let executed = wallet.execute(&f.call(), "superseded").await.unwrap();
    let due = f.clock.wall() + Duration::from_secs(3600);
    let references = ["superseded".to_owned()];
    wallet.retire(&references, due).await.unwrap();
    assert_eq!(f.row()["operations"]["superseded"]["retire_at"], mark(due));
    // A recovery from an older checkpoint looks the reference up again, so
    // it may be presented again: the lookup withdraws its mark.
    let reopened = f.wallet(false, true).unwrap();
    assert_eq!(reopened.reconcile("superseded").await.unwrap(), executed);
    let row = f.row()["operations"]["superseded"].clone();
    assert!(row.is_object(), "forgotten");
    assert_eq!(row["retire_at"], Value::Null);
    // Past the time it was marked for, a save keeps it.
    f.clock.advance(Duration::from_secs(3600));
    reopened.execute(&f.call(), "later").await.unwrap();
    assert!(f.row()["operations"].get("superseded").is_some());
    // Marked anew, the first save from that time forgets it.
    reopened.retire(&references, f.clock.wall()).await.unwrap();
    assert!(f.row()["operations"].get("superseded").is_none());
    assert_eq!(f.executions(), 2);
}
/// The handles of `reference`'s record in the journal.
fn handles(f: &Fixture, reference: &str) -> (Value, Value) {
    let row = &f.row()["operations"][reference];
    (row["wallet_operation_id"].clone(), row["tx_hash"].clone())
}
#[test]
fn a_kept_answer_whose_save_fails_leaves_a_new_execute_definite() {
    let f = Fixture::new(5042);
    let (wallet, kept) = kept_answer(&f, "settle:x");
    // The disk refuses the next write, the save that would carry the kept
    // answer.
    crate::local_state::fail_next_write(&f.path);
    let executions = f.executions();
    let second = current_thread()
        .block_on(wallet.execute(&f.call(), "settle:y"))
        .unwrap();
    // The journal held no record of the new reference and none was saved:
    // never sent, so a definite failure, as without a kept answer.
    assert_eq!(second.state(), OperationState::Failed);
    assert_eq!(second.cause(), Some("NotSubmittedError: Journal"));
    assert_eq!(f.executions(), executions, "Circle was asked to execute");
    assert!(f.row()["operations"].get("settle:y").is_none());
    // The next save that landed, the read-back's, wrote the kept answer.
    assert_eq!(
        handles(&f, "settle:x"),
        (json!(ID), json!(encoded(kept.tx_hash().unwrap())))
    );
}
#[test]
fn a_failed_save_of_a_kept_answer_fails_no_step_and_the_next_save_writes_it() {
    let f = Fixture::new(5042);
    let (wallet, kept) = kept_answer(&f, "settle:x");
    // The first write fails: the one a reconcile's first step makes for the
    // kept answer alone, since that step saves nothing of its own. The
    // reconcile goes on, with the kept answer's id, to Circle's history.
    crate::local_state::fail_next_write(&f.path);
    let found = current_thread()
        .block_on(wallet.reconcile("settle:x"))
        .unwrap();
    assert_eq!(found.state(), OperationState::Pending);
    assert_eq!(
        (found.wallet_operation_id(), found.tx_hash()),
        (kept.wallet_operation_id(), kept.tx_hash())
    );
    // Its record step's save, the next that landed, wrote it.
    assert_eq!(
        handles(&f, "settle:x"),
        (json!(ID), json!(encoded(kept.tx_hash().unwrap())))
    );
    assert_eq!(f.executions(), 1);
}
#[test]
fn a_step_whose_own_save_fails_with_a_kept_answer_reports_its_own_failure() {
    let f = Fixture::new(5042);
    let (wallet, kept) = kept_answer(&f, "settle:x");
    let runtime = current_thread();
    // Every write fails. Before an intent is saved, an execute is definitely
    // not submitted: its save and the read-back's both fail.
    for _ in 0..2 {
        crate::local_state::fail_next_write(&f.path);
    }
    let second = runtime
        .block_on(wallet.execute(&f.call(), "settle:y"))
        .unwrap();
    assert_eq!(second.state(), OperationState::Failed);
    assert_eq!(second.cause(), Some("NotSubmittedError: Journal"));
    assert!(f.row()["operations"].get("settle:y").is_none());
    // After Circle answered, a record step that cannot save is unknown with
    // the transaction id and hash: here a reconcile's, whose first step's
    // save of the kept answer fails too.
    for _ in 0..2 {
        crate::local_state::fail_next_write(&f.path);
    }
    let found = runtime.block_on(wallet.reconcile("settle:x")).unwrap();
    assert_eq!(found.state(), OperationState::Unknown);
    assert_eq!(
        (found.wallet_operation_id(), found.tx_hash()),
        (kept.wallet_operation_id(), kept.tx_hash())
    );
    assert_eq!(found.cause(), Some("Journal"));
    // Still kept, and written once a save lands.
    assert_eq!(handles(&f, "settle:x"), (Value::Null, Value::Null));
    runtime
        .block_on(wallet.retire(&[], f.clock.wall()))
        .unwrap();
    assert_eq!(
        handles(&f, "settle:x"),
        (json!(ID), json!(encoded(kept.tx_hash().unwrap())))
    );
    assert_eq!(f.executions(), 1);
}
#[test]
fn a_record_step_that_cannot_read_the_journal_keeps_circles_answer() {
    let f = Fixture::new(5042);
    let (entered, held) = mpsc::channel();
    let (release, waiting) = mpsc::channel();
    *f.service.held.lock().unwrap() = Some(("wallet execute", entered, waiting));
    let wallet = f.wallet(false, false).unwrap();
    let (slow, call) = (wallet.clone(), f.call());
    let slow =
        std::thread::spawn(move || current_thread().block_on(slow.execute(&call, "settle:x")));
    held.recv_timeout(Duration::from_secs(10)).unwrap();
    // While Circle executes, the journal gains a record no adapter accepts:
    // its idempotency key is not a version 4 UUID.
    let readable = std::fs::read(&f.path).unwrap();
    let mut journal = f.row();
    let mut invalid = journal["operations"]["settle:x"].clone();
    invalid["idempotency_key"] = json!("11111111-1111-1111-8111-111111111111");
    journal["operations"]["settle:z"] = invalid;
    std::fs::write(&f.path, serde_json::to_vec(&journal).unwrap()).unwrap();
    release.send(()).unwrap();
    let result = slow.join().unwrap().unwrap();
    // Unknown, with Circle's transaction id and hash for the runtime to check.
    assert_eq!(result.state(), OperationState::Unknown);
    assert_eq!(
        (result.wallet_operation_id(), result.tx_hash()),
        (Some(ID), Some(B256::repeat_byte(8)))
    );
    assert_eq!(result.cause(), Some("InvalidEvidence"));
    // While the journal stays unreadable every step fails, and the answer
    // stays kept.
    let runtime = current_thread();
    assert!(
        runtime
            .block_on(wallet.retire(&[], f.clock.wall()))
            .is_err()
    );
    // Readable again, the next journal step writes it.
    std::fs::write(&f.path, &readable).unwrap();
    runtime
        .block_on(wallet.retire(&[], f.clock.wall()))
        .unwrap();
    assert_eq!(
        handles(&f, "settle:x"),
        (json!(ID), json!(encoded(B256::repeat_byte(8))))
    );
}
#[test]
fn a_merge_the_journal_refuses_still_returns_circles_answer() {
    let f = Fixture::new(5042);
    let (entered, held) = mpsc::channel();
    let (release, waiting) = mpsc::channel();
    *f.service.held.lock().unwrap() = Some(("wallet execute", entered, waiting));
    let wallet = f.wallet(false, false).unwrap();
    let (slow, call) = (wallet.clone(), f.call());
    let slow =
        std::thread::spawn(move || current_thread().block_on(slow.execute(&call, "settle:x")));
    held.recv_timeout(Duration::from_secs(10)).unwrap();
    // While Circle executes, another record in the journal takes the
    // transaction id Circle is about to report for this reference.
    let mut journal = f.row();
    let mut other = journal["operations"]["settle:x"].clone();
    other["idempotency_key"] = json!(fresh_key().unwrap());
    other["wallet_operation_id"] = json!(ID);
    journal["operations"]["settle:z"] = other;
    std::fs::write(&f.path, serde_json::to_vec(&journal).unwrap()).unwrap();
    release.send(()).unwrap();
    let result = slow.join().unwrap().unwrap();
    // Unknown, with Circle's transaction id and hash for the runtime to check:
    // the journal contradicting them never discards them.
    assert_eq!(result.state(), OperationState::Unknown);
    assert_eq!(
        (result.wallet_operation_id(), result.tx_hash()),
        (Some(ID), Some(B256::repeat_byte(8)))
    );
    assert_eq!(result.cause(), Some("InvalidEvidence"));
    // The next journal step still refuses them: they are never written over
    // the journal's own evidence.
    current_thread()
        .block_on(wallet.retire(&[], f.clock.wall()))
        .unwrap();
    assert_eq!(handles(&f, "settle:x"), (Value::Null, Value::Null));
    assert_eq!(handles(&f, "settle:z"), (json!(ID), Value::Null));
}
