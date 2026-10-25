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
#[derive(Default)]
struct Service {
    chain: u64,
    fault: StdMutex<Option<&'static str>>,
    commands: StdMutex<Vec<(Vec<String>, bool)>>,
    transaction: StdMutex<Option<Value>>,
    delay: AtomicBool,
}
#[tonic::async_trait]
impl CircleCommand for Service {
    async fn run(&self, args: Vec<String>, mutating: bool) -> Result<CommandOutput, CircleError> {
        self.commands.lock().unwrap().push((args.clone(), mutating));
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
            if self.delay.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(70)).await;
            }
            if fault == Some("lost") {
                return Err(CircleError::Unavailable);
            }
            let direct = args[2] == "register()";
            let row = json!({"id":ID,"idempotencyKey":get("--idempotency-key"),"state":"COMPLETE","blockchain":blockchain(self.chain).unwrap(),"operation":"CONTRACT_EXECUTION","transactionType":"OUTBOUND","sourceAddress":get("--address"),"contractAddress":get("--contract"),"abiFunctionSignature":args[2],"abiParameters":if direct {Value::Null} else {json!([args[3],args[4],args[5]])},"txHash":encoded(B256::repeat_byte(8))});
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
struct Fixture {
    directory: PathBuf,
    path: PathBuf,
    rpc: Arc<TestRpc>,
    service: Arc<Service>,
}
impl Fixture {
    fn new(chain: u64) -> Self {
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
        }
    }
    fn wallet(&self, activate: bool, existing: bool) -> Result<CircleAgentWallet, CircleError> {
        CircleAgentWallet::with_command(
            Arc::new(ChainReader::new(self.rpc.clone(), profile(self.rpc.chain))),
            operator(),
            &self.path,
            CircleOptions {
                allow_registration_activation: activate,
                ..CircleOptions::default()
            },
            existing,
            self.service.clone(),
        )
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
    fn row(&self) -> Value {
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
        assert_eq!(row["operations"]["original"]["call"]["data"], "0xdeadbeef");
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
        assert!(reopened.execute(&wrong, "other").await.is_err());
        assert_eq!(f.executions(), 1);
    }
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
    for fault in ["version", "auth", "wallet", "type"] {
        let f = Fixture::new(5042);
        *f.service.fault.lock().unwrap() = Some(fault);
        let wallet = f.wallet(false, false).unwrap();
        let error = wallet.verify_wallet().await.unwrap_err();
        assert!(!error.to_string().contains("SECRET"));
        if fault == "auth" {
            assert_eq!(error, CircleError::Authentication);
            assert!(error.to_string().contains("circle wallet login"));
        }
        assert!(wallet.execute(&f.call(), "original").await.is_err());
        assert_eq!(f.executions(), 0);
    }
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
    std::fs::write(&f.path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    let restored = f.wallet(false, true).unwrap();
    assert_eq!(restored.reconcile("original").await.unwrap(), result);
    assert_eq!(f.row(), current); // The reconciliation saved it as version 2.
    legacy["wallet"]["address"] = json!(encoded(Address::repeat_byte(9)));
    std::fs::write(&f.path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    assert!(f.wallet(false, true).is_err());
}
#[tokio::test]
async fn legacy_shared_self_route_record_is_accepted_without_rewriting() {
    let f = Fixture::new(5042);
    let wallet = f.wallet(false, false).unwrap();
    wallet.execute(&f.call(), "original").await.unwrap();
    let mut row = f.row();
    row["operations"]["original"]
        .as_object_mut()
        .unwrap()
        .remove("route");
    let raw = serde_json::to_vec(&row).unwrap();
    std::fs::write(&f.path, &raw).unwrap();
    f.wallet(false, true).unwrap();
    assert_eq!(std::fs::read(&f.path).unwrap(), raw);
}
#[tokio::test]
async fn cancelled_caller_does_not_kill_or_forget_original_execution() {
    let f = Fixture::new(5042);
    f.service.delay.store(true, Ordering::SeqCst);
    let wallet = f.wallet(false, false).unwrap();
    let call = f.call();
    let retained = wallet.clone();
    let task = tokio::spawn(async move { retained.execute(&call, "original").await });
    while f.executions() == 0 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    task.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(f.row()["operations"]["original"]["wallet_operation_id"], ID);
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
