//! Synthetic Circle service and isolated wallet journals; no credentials/network.

use super::*;
use aws_lc_rs::{
    encoding::{AsDer, PublicKeyX509Der},
    rsa::{KeySize, OaepPrivateDecryptingKey, PrivateDecryptingKey},
};
use std::{
    fs,
    path::PathBuf,
    sync::{Mutex as StdMutex, OnceLock},
};
use tokio::sync::Notify;

const WALLET: &str = "11111111-1111-4111-8111-111111111111";
const SET: &str = "33333333-3333-4333-8333-333333333333";
const TRANSACTION: &str = "22222222-2222-4222-8222-222222222222";
const KEY: &str = "44444444-4444-4444-8444-444444444444";
const CHAIN: u64 = 5_042_002;

fn wallet_value() -> Value {
    json!({"id":WALLET, "walletSetId":SET, "address":format!("0x{}", "11".repeat(20)),
        "blockchain":"ARC-TESTNET", "accountType":"SCA", "custodyType":"DEVELOPER", "state":"LIVE"})
}
fn identity() -> CircleWalletIdentity {
    CircleWalletIdentity::parse(&wallet_value(), Some(WALLET), CHAIN).unwrap()
}
fn call() -> WalletCall {
    WalletCall::new(
        CHAIN,
        Address::repeat_byte(0x22),
        vec![0x12, 0x34, 0x56, 0x78],
    )
    .unwrap()
}
fn key() -> &'static PrivateDecryptingKey {
    static KEY: OnceLock<PrivateDecryptingKey> = OnceLock::new();
    KEY.get_or_init(|| PrivateDecryptingKey::generate(KeySize::Rsa2048).unwrap())
}
fn pem() -> String {
    let der = AsDer::<PublicKeyX509Der>::as_der(&key().public_key()).unwrap();
    format!(
        "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----",
        STANDARD.encode(der.as_ref())
    )
}
#[derive(Default)]
struct ServiceState {
    calls: Vec<(Method, String, Option<Value>)>,
    records: Vec<Value>,
    wallet_records: Vec<Value>,
    wallet_detail: Option<Value>,
    fail_wallet: bool,
    fail_list: bool,
    fail_detail: bool,
    lose_post: bool,
    hang_post: bool,
    block_journal_after_key: bool,
    fail_sign: bool,
    fail_setup_key: bool,
    invalid_setup_key: bool,
    lose_setup_post: bool,
    setup_path: Option<PathBuf>,
    journal_path: Option<PathBuf>,
}
#[derive(Default)]
struct Service {
    state: StdMutex<ServiceState>,
    posted: Notify,
}
impl Service {
    fn posts(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, p, _)| {
                *m == Method::POST && p == "/developer/transactions/contractExecution"
            })
            .count()
    }
    fn transaction_body(&self) -> Value {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .find(|(m, p, _)| {
                *m == Method::POST && p == "/developer/transactions/contractExecution"
            })
            .unwrap()
            .2
            .clone()
            .unwrap()
    }
}
#[tonic::async_trait]
impl CircleTransport for Service {
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, CircleError> {
        let result = {
            let mut state = self.state.lock().unwrap();
            state
                .calls
                .push((method.clone(), path.into(), body.clone()));
            if path == format!("/wallets/{WALLET}") {
                return if state.fail_wallet {
                    Err(CircleError::Unavailable)
                } else {
                    Ok(json!({"wallet":state.wallet_detail.clone().unwrap_or_else(wallet_value)}))
                };
            }
            if path.starts_with("/wallets?") {
                assert_eq!(method, Method::GET);
                assert!(path.contains(&format!("walletSetId={SET}")));
                assert!(path.contains(&format!("refId={KEY}")));
                assert!(path.ends_with("pageSize=2"));
                return Ok(json!({"wallets":state.wallet_records}));
            }
            if path == format!("/walletSets/{SET}") {
                return Ok(json!({"walletSet":{"id":SET,"custodyType":"DEVELOPER"}}));
            }
            if path == "/config/entity/publicKey" {
                if state.fail_setup_key {
                    return Err(CircleError::Unavailable);
                }
                if state.invalid_setup_key {
                    return Ok(json!({"publicKey":"invalid RSA public key"}));
                }
                if state.block_journal_after_key {
                    let path = state.journal_path.as_ref().unwrap();
                    fs::rename(path, path.with_extension("retained.json")).unwrap();
                    fs::create_dir(path).unwrap();
                }
                return Ok(json!({"publicKey":pem()}));
            }
            if path.starts_with("/transactions?") {
                assert_eq!(method, Method::GET);
                assert!(!path.contains("blockchain="));
                assert!(path.contains(&format!("walletIds={WALLET}")));
                assert!(path.ends_with("pageSize=2"));
                return if state.fail_list {
                    Err(CircleError::Unavailable)
                } else {
                    Ok(json!({"transactions":state.records}))
                };
            }
            if path == format!("/transactions/{TRANSACTION}") {
                return if state.fail_detail {
                    Err(CircleError::Unavailable)
                } else {
                    Ok(json!({"transaction":state.records[0]}))
                };
            }
            if path == "/developer/walletSets" || path == "/developer/wallets" {
                if let Some(path) = &state.setup_path {
                    let saved: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
                    assert_eq!(saved["attempted"], true);
                    assert_eq!(saved["key"], body.as_ref().unwrap()["idempotencyKey"]);
                }
                if state.lose_setup_post {
                    return Err(CircleError::Unavailable);
                }
            }
            if path == "/developer/walletSets" {
                assert_eq!(method, Method::POST);
                return Ok(json!({"walletSet":{"id":SET,"custodyType":"DEVELOPER"}}));
            }
            if path == "/developer/wallets" {
                let mut value = wallet_value();
                value["refId"] = body.as_ref().unwrap()["metadata"][0]["refId"].clone();
                return Ok(json!({"wallets":[value]}));
            }
            if path == "/developer/sign/typedData" {
                if state.fail_sign {
                    return Err(CircleError::Unavailable);
                }
                return Ok(json!({"signature":format!("0x{}", "aa".repeat(140))}));
            }
            assert_eq!(path, "/developer/transactions/contractExecution");
            assert_eq!(method, Method::POST);
            let body = body.unwrap();
            if let Some(path) = &state.journal_path {
                let saved: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
                let entry = saved["operations"]
                    .as_object()
                    .unwrap()
                    .values()
                    .find(|entry| entry["service_ref"] == body["refId"])
                    .unwrap();
                assert_eq!(entry["attempted"], true);
                assert_eq!(entry["to"], body["contractAddress"]);
                assert_eq!(entry["calldata"], body["callData"]);
            }
            state.records.push(json!({
                "id":TRANSACTION,"refId":body["refId"],"walletId":WALLET,"blockchain":"ARC-TESTNET",
                "custodyType":"DEVELOPER","operation":"CONTRACT_EXECUTION","transactionType":"OUTBOUND",
                "sourceAddress":address_hex(identity().address()),"contractAddress":body["contractAddress"],"state":"INITIATED"
            }));
            (state.lose_post, state.hang_post)
        };
        self.posted.notify_one();
        if result.1 {
            std::future::pending::<()>().await;
        }
        if result.0 {
            Err(CircleError::Unavailable)
        } else {
            Ok(json!({"id":TRANSACTION,"state":"INITIATED"}))
        }
    }
}

struct Temp {
    directory: PathBuf,
    path: PathBuf,
}
impl Temp {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("mppi-circle-{}", fresh_key().unwrap()));
        fs::create_dir(&directory).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let path = directory.join("wallet.json");
        Self { directory, path }
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}
fn client(service: Arc<Service>) -> Arc<CircleClient> {
    Arc::new(CircleClient {
        transport: service,
        entity_secret: Zeroizing::new([0x34; 32]),
    })
}
fn adapter(service: Arc<Service>, path: &Path) -> CircleDeveloperWallet {
    service.state.lock().unwrap().journal_path = Some(path.to_owned());
    CircleDeveloperWallet::new(client(service), identity(), path).unwrap()
}

#[tokio::test]
async fn exact_call_is_durably_bound_before_post_and_ciphertext_decrypts() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    let wallet = adapter(service.clone(), &temp.path);
    let result = wallet.execute(&call(), "capture:original").await.unwrap();
    assert_eq!(result.state(), OperationState::Pending);
    assert_eq!(result.wallet_operation_id(), Some(TRANSACTION));
    assert!(result.observation().is_none() && result.sessions().is_empty());
    let body = service.transaction_body();
    assert_eq!(body["walletId"], WALLET);
    assert_eq!(body["contractAddress"], address_hex(call().to()));
    assert_eq!(body["callData"], "0x12345678");
    assert_eq!(body["amount"], "0");
    assert_eq!(body["idempotencyKey"], body["refId"]);
    idempotency_key(body["idempotencyKey"].as_str().unwrap()).unwrap();
    assert!(body.get("abiParameters").is_none() && body.get("blockchain").is_none());
    let encrypted = STANDARD
        .decode(body["entitySecretCiphertext"].as_str().unwrap())
        .unwrap();
    let decryptor = OaepPrivateDecryptingKey::new(key().clone()).unwrap();
    let mut plaintext = vec![0; decryptor.min_output_size()];
    assert_eq!(
        decryptor
            .decrypt(&OAEP_SHA256_MGF1SHA256, &encrypted, &mut plaintext, None)
            .unwrap(),
        &[0x34; 32]
    );
    let journal = fs::read_to_string(&temp.path).unwrap();
    assert!(!journal.contains("entitySecret") && !journal.contains("api_key"));
}

#[tokio::test]
async fn lost_response_and_fresh_adapter_only_reconcile_original_call() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    service.state.lock().unwrap().lose_post = true;
    let wallet = adapter(service.clone(), &temp.path);
    wallet.execute(&call(), "original").await.unwrap();
    let original_ref = service.transaction_body()["refId"].clone();
    drop(wallet);
    let wallet = adapter(service.clone(), &temp.path);
    let recovered = wallet.reconcile("original").await.unwrap();
    assert_eq!(recovered.wallet_operation_id(), Some(TRANSACTION));
    assert_eq!(
        wallet.execute(&call(), "original").await.unwrap(),
        recovered
    );
    let altered = WalletCall::new(CHAIN, call().to(), vec![1, 2, 3, 4]).unwrap();
    assert!(wallet.execute(&altered, "original").await.is_err());
    assert_eq!(service.posts(), 1);
    assert_eq!(service.transaction_body()["refId"], original_ref);
}

#[tokio::test]
async fn cancelled_submission_keeps_durable_query_only_intent() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    service.state.lock().unwrap().hang_post = true;
    let wallet = Arc::new(adapter(service.clone(), &temp.path));
    let runner = wallet.clone();
    let task = tokio::spawn(async move { runner.execute(&call(), "cancelled").await });
    service.posted.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(wallet);
    let wallet = adapter(service.clone(), &temp.path);
    assert_eq!(
        wallet
            .reconcile("cancelled")
            .await
            .unwrap()
            .wallet_operation_id(),
        Some(TRANSACTION)
    );
    wallet.execute(&call(), "cancelled").await.unwrap();
    assert_eq!(service.posts(), 1);
}

#[tokio::test]
async fn validated_summary_handles_survive_failed_detail_and_restart() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    {
        let mut state = service.state.lock().unwrap();
        state.lose_post = true;
        state.fail_detail = true;
    }
    let wallet = adapter(service.clone(), &temp.path);
    let result = wallet.execute(&call(), "original").await.unwrap();
    assert_eq!(result.state(), OperationState::Unknown);
    assert_eq!(result.wallet_operation_id(), Some(TRANSACTION));
    drop(wallet);
    service.state.lock().unwrap().fail_list = true;
    let wallet = adapter(service.clone(), &temp.path);
    assert_eq!(
        wallet
            .reconcile("original")
            .await
            .unwrap()
            .wallet_operation_id(),
        Some(TRANSACTION)
    );
    assert_eq!(service.posts(), 1);
}

#[tokio::test]
async fn mismatched_service_identity_never_adopts_transaction_hash() {
    for (field, bad) in [
        ("id", json!("44444444-4444-4444-8444-444444444444")),
        ("refId", json!(KEY)),
        ("walletId", json!(SET)),
        ("blockchain", json!("ARC")),
        ("custodyType", json!("ENDUSER")),
        ("operation", json!("TRANSFER")),
        ("transactionType", json!("INBOUND")),
        ("sourceAddress", json!(address_hex(Address::repeat_byte(9)))),
        (
            "contractAddress",
            json!(address_hex(Address::repeat_byte(9))),
        ),
        ("callData", json!("0xdeadbeef")),
    ] {
        let temp = Temp::new();
        let service = Arc::new(Service::default());
        let wallet = adapter(service.clone(), &temp.path);
        wallet.execute(&call(), "original").await.unwrap();
        {
            let mut state = service.state.lock().unwrap();
            state.records[0][field] = bad;
            state.records[0]["txHash"] = json!(format!("0x{}", "77".repeat(32)));
        }
        let result = wallet.reconcile("original").await.unwrap();
        assert_eq!(result.state(), OperationState::Unknown, "{field}");
        assert_eq!(result.wallet_operation_id(), Some(TRANSACTION), "{field}");
        assert_eq!(result.tx_hash(), None, "{field}");
        assert_eq!(service.posts(), 1);
    }
}

#[tokio::test]
async fn duplicate_and_missing_lookups_keep_uncertainty() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    let wallet = adapter(service.clone(), &temp.path);
    wallet.execute(&call(), "original").await.unwrap();
    {
        let mut state = service.state.lock().unwrap();
        let duplicate = state.records[0].clone();
        state.records.push(duplicate);
    }
    assert_eq!(
        wallet.reconcile("original").await.unwrap().state(),
        OperationState::Unknown
    );
    service.state.lock().unwrap().records.clear();
    assert_eq!(
        wallet.execute(&call(), "original").await.unwrap().state(),
        OperationState::Unknown
    );
    assert_eq!(service.posts(), 1);
}

#[tokio::test]
async fn service_finality_never_becomes_chain_confirmation() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    let wallet = adapter(service.clone(), &temp.path);
    wallet.execute(&call(), "original").await.unwrap();
    for (state, expected) in [
        ("INITIATED", OperationState::Pending),
        ("STUCK", OperationState::Pending),
        ("COMPLETE", OperationState::Pending),
        ("CONFIRMED", OperationState::Pending),
        ("FAILED", OperationState::Unknown),
        ("DENIED", OperationState::Unknown),
        ("CANCELLED", OperationState::Unknown),
        ("NEW_STATE", OperationState::Unknown),
    ] {
        {
            let mut service = service.state.lock().unwrap();
            service.records[0]["state"] = json!(state);
            service.records[0]["txHash"] = json!(format!("0x{}", "77".repeat(32)));
        }
        let result = wallet.reconcile("original").await.unwrap();
        assert_eq!(result.state(), expected);
        assert_eq!(result.tx_hash(), Some(B256::repeat_byte(0x77)));
        assert!(result.observation().is_none());
    }
}

#[tokio::test]
async fn attempted_service_failure_without_hash_cannot_attest_no_submission() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    let wallet = adapter(service.clone(), &temp.path);
    wallet.execute(&call(), "original").await.unwrap();
    drop(wallet);
    // A fresh process must consult the same attempted original operation.
    // Only the durable pre-POST marker can prove no submission was started.
    let wallet = adapter(service.clone(), &temp.path);
    for state in ["FAILED", "DENIED", "CANCELLED"] {
        {
            let mut service = service.state.lock().unwrap();
            service.records[0]["state"] = json!(state);
            service.records[0]["txHash"] = Value::Null;
        }
        let result = wallet.reconcile("original").await.unwrap();
        assert_eq!(result.state(), OperationState::Unknown, "{state}");
        assert_eq!(result.wallet_operation_id(), Some(TRANSACTION));
        assert!(result.tx_hash().is_none());
        assert!(result.observation().is_none());
        let repeated = wallet.execute(&call(), "original").await.unwrap();
        assert_eq!(repeated.state(), OperationState::Unknown, "{state}");
        assert_eq!(service.posts(), 1);
    }
}

#[tokio::test]
async fn changed_hash_is_rejected_and_known_hash_is_retained() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    let wallet = adapter(service.clone(), &temp.path);
    wallet.execute(&call(), "original").await.unwrap();
    service.state.lock().unwrap().records[0]["txHash"] = json!(format!("0x{}", "77".repeat(32)));
    wallet.reconcile("original").await.unwrap();
    service.state.lock().unwrap().records[0]["txHash"] = json!(format!("0x{}", "88".repeat(32)));
    let result = wallet.reconcile("original").await.unwrap();
    assert_eq!(result.state(), OperationState::Unknown);
    assert_eq!(result.tx_hash(), Some(B256::repeat_byte(0x77)));
    assert_eq!(service.posts(), 1);
}

#[tokio::test]
async fn unknown_reference_has_no_service_or_submission_side_effect() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    let wallet = adapter(service.clone(), &temp.path);
    assert!(wallet.reconcile("absent").await.is_err());
    assert!(service.state.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn pre_submission_failure_is_definitive_and_reconcile_is_read_only() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    service.state.lock().unwrap().fail_wallet = true;
    let wallet = adapter(service.clone(), &temp.path);
    let result = wallet.execute(&call(), "original").await.unwrap();
    assert_eq!(result.state(), OperationState::Failed);
    drop(wallet);
    service.state.lock().unwrap().fail_wallet = false;
    let wallet = adapter(service.clone(), &temp.path);
    assert_eq!(
        wallet.reconcile("original").await.unwrap().state(),
        OperationState::Failed
    );
    wallet.execute(&call(), "original").await.unwrap();
    assert_eq!(service.posts(), 0);
}

#[tokio::test]
async fn wallet_journal_allows_idle_instances_but_excludes_concurrent_operation() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    let wallet = Arc::new(adapter(service.clone(), &temp.path));
    let second =
        CircleDeveloperWallet::new(client(service.clone()), identity(), &temp.path).unwrap();
    service.state.lock().unwrap().hang_post = true;
    let runner = wallet.clone();
    let task = tokio::spawn(async move { runner.execute(&call(), "original").await });
    service.posted.notified().await;
    assert!(second.reconcile("original").await.is_err());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        second
            .reconcile("original")
            .await
            .unwrap()
            .wallet_operation_id(),
        Some(TRANSACTION)
    );
    assert_eq!(service.posts(), 1);
}

#[tokio::test]
async fn concurrent_explicit_calls_submit_once() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    let wallet = adapter(service.clone(), &temp.path);
    let exact = call();
    let (a, b) = tokio::join!(
        wallet.execute(&exact, "original"),
        wallet.execute(&exact, "original")
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(service.posts(), 1);
}

#[tokio::test]
async fn corrupt_or_foreign_journal_is_rejected_before_network() {
    for field in [
        "wallet_id",
        "wallet_address",
        "chain_id",
        "version",
        "unexpected",
    ] {
        let temp = Temp::new();
        let service = Arc::new(Service::default());
        let wallet = adapter(service.clone(), &temp.path);
        drop(wallet);
        let mut value: Value = serde_json::from_slice(&fs::read(&temp.path).unwrap()).unwrap();
        value[field] = json!("corrupt");
        fs::write(&temp.path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(
            CircleDeveloperWallet::new(client(service.clone()), identity(), &temp.path).is_err(),
            "{field}"
        );
        assert!(service.state.lock().unwrap().calls.is_empty());
    }
}

#[tokio::test]
async fn invalid_call_is_rejected_before_journal_or_network() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    let wallet = adapter(service.clone(), &temp.path);
    for bad in [
        WalletCall::new(5042, call().to(), vec![1]).unwrap(),
        WalletCall::new(CHAIN, call().to(), vec![1; MAX_CALL + 1]).unwrap(),
    ] {
        assert!(wallet.execute(&bad, "original").await.is_err());
    }
    assert!(wallet.execute(&call(), "").await.is_err());
    let journal: Value = serde_json::from_slice(&fs::read(&temp.path).unwrap()).unwrap();
    assert_eq!(journal["operations"], json!({}));
    assert!(service.state.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn failed_durable_submission_marker_prevents_wallet_post() {
    let temp = Temp::new();
    let service = Arc::new(Service::default());
    service.state.lock().unwrap().block_journal_after_key = true;
    let wallet = adapter(service.clone(), &temp.path);
    assert!(wallet.execute(&call(), "original").await.is_err());
    assert_eq!(service.posts(), 0);
    let retained: Value =
        serde_json::from_slice(&fs::read(temp.path.with_extension("retained.json")).unwrap())
            .unwrap();
    assert_eq!(retained["operations"]["original"]["attempted"], false);
    assert_eq!(retained["operations"]["original"]["calldata"], "0x12345678");
}

#[tokio::test]
async fn setup_uses_original_keys_fresh_ciphertext_and_one_arc_sca() {
    let service = Arc::new(Service::default());
    let client = client(service.clone());
    assert_eq!(
        client
            .create_wallet_set("MPPI provider", KEY)
            .await
            .unwrap(),
        SET
    );
    assert_eq!(
        client
            .create_wallet_set("MPPI provider", KEY)
            .await
            .unwrap(),
        SET
    );
    let wallet = client
        .create_wallet(SET, CHAIN, "MPPI operator", KEY)
        .await
        .unwrap();
    assert_eq!(wallet, identity());
    client.verify_wallet(&wallet).await.unwrap();
    let state = service.state.lock().unwrap();
    let sets: Vec<_> = state
        .calls
        .iter()
        .filter(|(_, p, _)| p == "/developer/walletSets")
        .map(|(_, _, b)| b.as_ref().unwrap())
        .collect();
    assert_eq!(sets[0]["idempotencyKey"], sets[1]["idempotencyKey"]);
    assert_ne!(
        sets[0]["entitySecretCiphertext"],
        sets[1]["entitySecretCiphertext"]
    );
    let body = state
        .calls
        .iter()
        .find(|(_, p, _)| p == "/developer/wallets")
        .unwrap()
        .2
        .as_ref()
        .unwrap();
    assert_eq!(body["blockchains"], json!(["ARC-TESTNET"]));
    assert_eq!(body["accountType"], "SCA");
    assert_eq!(body["count"], 1);
    assert_eq!(body["walletSetId"], SET);
}

#[tokio::test]
async fn credential_preflight_only_signs_distinct_fresh_nonpayment_context() {
    let service = Arc::new(Service::default());
    let client = client(service.clone());
    client.verify_credentials(&identity()).await.unwrap();
    client.verify_credentials(&identity()).await.unwrap();
    let state = service.state.lock().unwrap();
    let bodies: Vec<_> = state
        .calls
        .iter()
        .filter(|(m, _, _)| *m == Method::POST)
        .map(|(_, p, b)| {
            assert_eq!(p, "/developer/sign/typedData");
            b.as_ref().unwrap()
        })
        .collect();
    assert_eq!(bodies.len(), 2);
    let a: Value = serde_json::from_str(bodies[0]["data"].as_str().unwrap()).unwrap();
    let b: Value = serde_json::from_str(bodies[1]["data"].as_str().unwrap()).unwrap();
    assert_eq!(a["primaryType"], "CredentialCheck");
    assert_eq!(a["domain"]["name"], "MPPI Circle Credential Check");
    assert_eq!(a["domain"]["chainId"], CHAIN);
    assert_eq!(
        a["domain"]["verifyingContract"],
        address_hex(identity().address())
    );
    assert_ne!(a["message"]["nonce"], b["message"]["nonce"]);
    assert_ne!(
        bodies[0]["entitySecretCiphertext"],
        bodies[1]["entitySecretCiphertext"]
    );
}

#[tokio::test]
async fn rejected_registered_secret_stops_preflight_without_transaction() {
    let service = Arc::new(Service::default());
    service.state.lock().unwrap().fail_sign = true;
    let client = client(service.clone());
    assert_eq!(
        client.verify_credentials(&identity()).await.unwrap_err(),
        CircleError::Unavailable
    );
    assert_eq!(service.posts(), 0);
    assert!(
        service
            .state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, _, _)| *m == Method::POST)
            .all(|(_, p, _)| p == "/developer/sign/typedData")
    );
}

#[tokio::test]
async fn lost_wallet_creation_is_reconciled_by_set_and_original_metadata_reference() {
    let service = Arc::new(Service::default());
    let client = client(service.clone());
    assert!(
        client
            .find_created_wallet(SET, CHAIN, KEY)
            .await
            .unwrap()
            .is_none()
    );
    {
        let mut state = service.state.lock().unwrap();
        let mut wallet = wallet_value();
        wallet["refId"] = json!(KEY);
        state.wallet_records = vec![wallet.clone()];
        state.wallet_detail = Some(wallet);
    }
    assert_eq!(
        client.find_created_wallet(SET, CHAIN, KEY).await.unwrap(),
        Some(identity())
    );
    assert!(
        service
            .state
            .lock()
            .unwrap()
            .calls
            .iter()
            .all(|(method, _, _)| *method == Method::GET)
    );
}

#[tokio::test]
async fn created_wallet_lookup_rejects_ambiguous_foreign_and_changed_detail() {
    let service = Arc::new(Service::default());
    let client = client(service.clone());
    let mut wallet = wallet_value();
    wallet["refId"] = json!(KEY);
    {
        let mut state = service.state.lock().unwrap();
        state.wallet_records = vec![wallet.clone(), wallet.clone()];
        state.wallet_detail = Some(wallet.clone());
    }
    assert!(client.find_created_wallet(SET, CHAIN, KEY).await.is_err());
    for field in [
        "refId",
        "walletSetId",
        "blockchain",
        "accountType",
        "custodyType",
        "state",
        "address",
    ] {
        for detail in [false, true] {
            {
                let mut state = service.state.lock().unwrap();
                state.wallet_records = vec![wallet.clone()];
                state.wallet_detail = Some(wallet.clone());
                let target = if detail {
                    state.wallet_detail.as_mut().unwrap()
                } else {
                    &mut state.wallet_records[0]
                };
                target[field] = json!("mismatch");
            }
            assert!(
                client.find_created_wallet(SET, CHAIN, KEY).await.is_err(),
                "{field} detail={detail}"
            );
        }
    }
    assert!(
        service
            .state
            .lock()
            .unwrap()
            .calls
            .iter()
            .all(|(method, _, _)| *method == Method::GET)
    );
}

#[test]
fn credentials_and_error_formatting_are_redacted() {
    let credentials = CircleCredentials::new("TEST_API_KEY:hidden".into(), [0x34; 32]).unwrap();
    assert!(!format!("{credentials:?}").contains("hidden"));
    assert!(CircleCredentials::new("bad\ncredential".into(), [0x34; 32]).is_err());
    for error in [
        CircleError::Configuration,
        CircleError::Unavailable,
        CircleError::InvalidEvidence,
        CircleError::Journal,
        CircleError::Capacity,
    ] {
        assert!(!format!("{error:?}: {error}").contains("hidden"));
    }
}

#[test]
fn key_and_identifier_inputs_fail_closed() {
    for invalid in [
        "",
        "-----BEGIN PRIVATE KEY-----\nAA==\n-----END PRIVATE KEY-----",
        "-----BEGIN PUBLIC KEY-----\nAA==\n-----END PUBLIC KEY-----",
    ] {
        assert!(encrypt_secret(invalid, &[0x34; 32]).is_err());
    }
    for invalid in [
        "",
        "../wallet",
        "11111111-1111-4111-8111-11111111111A",
        "11111111-1111-4111-8111-111111111111/",
    ] {
        assert!(uuid(invalid).is_err());
    }
    assert!(idempotency_key("11111111-1111-5111-8111-111111111111").is_err());
    assert!(idempotency_key("11111111-1111-4111-7111-111111111111").is_err());
}

#[test]
fn wallet_service_identity_checks_every_required_binding() {
    for (field, value) in [
        ("id", json!(SET)),
        ("walletSetId", json!("bad")),
        ("address", json!(address_hex(Address::ZERO))),
        ("blockchain", json!("ARC")),
        ("custodyType", json!("ENDUSER")),
        ("accountType", json!("EOA")),
        ("state", json!("FROZEN")),
    ] {
        let mut record = wallet_value();
        record[field] = value;
        assert!(
            CircleWalletIdentity::parse(&record, Some(WALLET), CHAIN).is_err(),
            "{field}"
        );
    }
}

async fn local_http(response: String) -> (Result<Value, CircleError>, String) {
    use std::{
        io::{Read, Write},
        net::TcpListener,
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        loop {
            let mut buffer = [0u8; 1024];
            let n = socket.read(&mut buffer).unwrap();
            if n == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..n]);
            if request.windows(4).any(|part| part == b"\r\n\r\n") {
                break;
            }
        }
        let _ = socket.write_all(response.as_bytes());
        String::from_utf8(request).unwrap()
    });
    let transport = CircleHttp::new(Zeroizing::new("TEST_API_KEY:synthetic".into()), 2000).unwrap();
    let result = transport
        .send(
            transport.client.get(format!("http://{address}/fixture")),
            None,
        )
        .await;
    (result, server.join().unwrap())
}

#[tokio::test]
async fn authenticated_http_bounds_and_validates_service_responses() {
    for response in [
        "HTTP/1.1 500 Error\r\nContent-Length: 16\r\n\r\nREMOTE_SECRET!!!".to_owned(),
        "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/leak\r\nContent-Length: 0\r\n\r\n".to_owned(),
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\n{}".to_owned(),
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}".to_owned(),
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: gzip\r\nContent-Length: 2\r\n\r\n{}".to_owned(),
        format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", MAX_RESPONSE+1),
    ] {
        let (result, request) = local_http(response).await;
        let error = result.unwrap_err();
        assert!(!format!("{error:?}: {error}").contains("REMOTE_SECRET"));
        assert!(request.to_ascii_lowercase().contains("authorization: bearer test_api_key:synthetic"));
    }
    for body in [
        r#"{"data":{"key":1,"key":2}}"#,
        r#"{"data":[]}"#,
        r#"{"data":{}} garbage"#,
    ] {
        let (result, _) = local_http(format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ))
        .await;
        assert!(result.is_err());
    }
    let body = r#"{"data":{"accepted":true}}"#;
    let (result, _) = local_http(format!("HTTP/1.1 201 Created\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n\r\n{body}", body.len())).await;
    assert_eq!(result.unwrap(), json!({"accepted":true}));
}

// Mirrors only the CLI's durable boundary; the actual HTTP helper determines
// whether preparation or mutation has begun. No remote resources are created.
async fn setup_request(
    client: &CircleClient,
    wallet: bool,
    before_submit: impl FnOnce() -> Result<(), CircleError>,
) -> Result<(), CircleError> {
    if wallet {
        client
            .create_wallet_before_submit(SET, CHAIN, "MPPI provider", KEY, before_submit)
            .await
            .map(|_| ())
    } else {
        client
            .create_wallet_set_before_submit("MPPI provider", KEY, before_submit)
            .await
            .map(|_| ())
    }
}

fn setup_posts(service: &Service) -> usize {
    service
        .state
        .lock()
        .unwrap()
        .calls
        .iter()
        .filter(|(method, path, _)| {
            *method == Method::POST
                && matches!(
                    path.as_str(),
                    "/developer/walletSets" | "/developer/wallets"
                )
        })
        .count()
}

#[tokio::test]
async fn resource_preparation_failure_retains_unattempted_original_intent() {
    for wallet in [false, true] {
        for invalid_key in [false, true] {
            let temp = Temp::new();
            let service = Arc::new(Service::default());
            let client = client(service.clone());
            let mut store = LocalState::open(&temp.path, 4096).unwrap();
            let mut record = json!({"key":KEY,"attempted":false});
            store.write(&record).unwrap();
            {
                let mut state = service.state.lock().unwrap();
                state.setup_path = Some(temp.path.clone());
                state.fail_setup_key = !invalid_key;
                state.invalid_setup_key = invalid_key;
            }
            let error = setup_request(&client, wallet, || {
                record["attempted"] = json!(true);
                store.write(&record).map_err(|_| CircleError::Journal)
            })
            .await
            .unwrap_err();
            assert_eq!(
                error,
                if invalid_key {
                    CircleError::InvalidEvidence
                } else {
                    CircleError::Unavailable
                }
            );
            assert_eq!(
                store.read().unwrap().unwrap(),
                json!({"key":KEY,"attempted":false})
            );
            assert_eq!(setup_posts(&service), 0);
            {
                let mut state = service.state.lock().unwrap();
                state.fail_setup_key = false;
                state.invalid_setup_key = false;
            }
            setup_request(&client, wallet, || {
                record["attempted"] = json!(true);
                store.write(&record).map_err(|_| CircleError::Journal)
            })
            .await
            .unwrap();
            assert_eq!(setup_posts(&service), 1);
            assert_eq!(
                store.read().unwrap().unwrap(),
                json!({"key":KEY,"attempted":true})
            );
        }
    }
}

#[tokio::test]
async fn resource_submission_marker_blocks_failed_storage_and_survives_lost_response() {
    for wallet in [false, true] {
        let temp = Temp::new();
        let service = Arc::new(Service::default());
        let client = client(service.clone());
        assert_eq!(
            setup_request(&client, wallet, || Err(CircleError::Journal))
                .await
                .unwrap_err(),
            CircleError::Journal
        );
        assert_eq!(setup_posts(&service), 0);
        let mut store = LocalState::open(&temp.path, 4096).unwrap();
        let mut record = json!({"key":KEY,"attempted":false});
        store.write(&record).unwrap();
        {
            let mut state = service.state.lock().unwrap();
            state.setup_path = Some(temp.path.clone());
            state.lose_setup_post = true;
        }
        assert_eq!(
            setup_request(&client, wallet, || {
                record["attempted"] = json!(true);
                store.write(&record).map_err(|_| CircleError::Journal)
            })
            .await
            .unwrap_err(),
            CircleError::Unavailable
        );
        drop(store);
        let reopened = LocalState::open(&temp.path, 4096).unwrap();
        assert_eq!(
            reopened.read().unwrap().unwrap(),
            json!({"key":KEY,"attempted":true})
        );
        // Lookup absence never establishes non-submission, including a lost POST.
        assert!(
            client
                .find_created_wallet(SET, CHAIN, KEY)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(setup_posts(&service), 1);
    }
}
