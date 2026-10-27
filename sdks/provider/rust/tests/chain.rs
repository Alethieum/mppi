//! Independent RPC evidence fixtures. No wallet or live network is contacted.
use alloy_primitives::{Address, B256, U256, hex, keccak256};
use mppi_provider::{
    capabilities::{TransactionWallet, WalletCall, WalletError},
    chain::{ChainError, ChainReader, CodePin, DeploymentProfile, Rpc},
    contract::{ARC_USDC, ContractCalls},
    operations::{reconcile_operation, submit_operation},
    signing::{SessionDescriptor, Voucher},
    types::{Domain, OperationResult, OperationState, SessionStatus},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

const CHAIN: u64 = 5042002;
const TIME: u64 = 1790331000;
fn proxy() -> Address {
    Address::repeat_byte(0x99)
}
fn tx() -> B256 {
    B256::repeat_byte(0xaa)
}
fn block_hash() -> B256 {
    B256::repeat_byte(0xbb)
}
fn descriptor() -> SessionDescriptor {
    SessionDescriptor {
        agent: Address::repeat_byte(0x11),
        sessionSigner: Address::repeat_byte(0x12),
        providerId: (U256::from(1) << 200) + U256::from(7),
        recipient: Address::repeat_byte(0x22),
        operator: Address::repeat_byte(0x33),
        token: ARC_USDC,
        salt: B256::repeat_byte(0x44),
    }
}
fn domain() -> Domain {
    Domain::new(CHAIN, proxy()).unwrap()
}
fn calls() -> ContractCalls {
    ContractCalls::new(domain(), "USDC".into(), "2".into()).unwrap()
}
fn sid(d: &SessionDescriptor) -> B256 {
    domain().signing_context().session_id(d)
}
fn hx(raw: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(raw))
}
fn word(value: u64) -> Vec<u8> {
    U256::from(value).to_be_bytes::<32>().to_vec()
}
fn aword(value: Address) -> Vec<u8> {
    let mut out = vec![0; 12];
    out.extend(value.as_slice());
    out
}
fn state(deposit: u64, paid: u64, status: u64) -> Value {
    json!(hx([
        word(deposit),
        word(paid),
        word(status),
        word(0),
        word(TIME),
        vec![0x66; 32]
    ]
    .concat()))
}
fn event(signature: &str, topics: Vec<Vec<u8>>, words: Vec<Vec<u8>>, index: u64) -> Value {
    let mut all = vec![hx(keccak256(signature))];
    all.extend(topics.into_iter().map(hx));
    json!({"address":hx(proxy()),"topics":all,"data":hx(words.concat()),"logIndex":format!("0x{index:x}"),"transactionIndex":"0x2","transactionHash":hx(tx()),"blockNumber":"0x64","blockHash":hx(block_hash()),"removed":false})
}
fn settled(d: &SessionDescriptor, target: u64, delta: u64, index: u64) -> Value {
    event(
        "Settled(bytes32,address,address,uint64,uint64,uint64,uint64)",
        vec![sid(d).to_vec(), aword(d.agent), aword(d.recipient)],
        vec![word(5510000), word(target), word(delta), word(target)],
        index,
    )
}
fn opened(d: &SessionDescriptor) -> Value {
    event(
        "SessionOpened(bytes32,address,address,uint256,address,address,address,bytes32,uint64,bytes32)",
        vec![
            sid(d).to_vec(),
            aword(d.agent),
            d.providerId.to_be_bytes::<32>().to_vec(),
        ],
        vec![
            aword(d.sessionSigner),
            aword(d.recipient),
            aword(d.operator),
            aword(d.token),
            d.salt.to_vec(),
            word(5510000),
            vec![0x66; 32],
        ],
        0,
    )
}

struct Mock {
    receipt: Mutex<Value>,
    states: Mutex<HashMap<String, Value>>,
    block: Mutex<Value>,
    fail: Mutex<bool>,
    reorg_after: Mutex<Option<usize>>,
    block_reads: AtomicUsize,
    code: Mutex<HashMap<String, String>>,
    slots: Mutex<HashMap<String, String>>,
    digest: Mutex<B256>,
    calls: Mutex<Vec<(String, Value)>>,
}
impl Mock {
    fn new() -> Self {
        let mut code = HashMap::new();
        let mut slots = HashMap::new();
        for byte in [0x99, 0x98, 0x97, 0x96] {
            code.insert(hx(Address::repeat_byte(byte)), "0x6000".into());
        }
        slots.insert(hx(proxy()), hx(aword(Address::repeat_byte(0x98))));
        slots.insert(
            hx(Address::repeat_byte(0x97)),
            hx(aword(Address::repeat_byte(0x96))),
        );
        Self {
            receipt: Mutex::new(
                json!({"transactionHash":hx(tx()),"blockHash":hx(block_hash()),"blockNumber":"0x64","transactionIndex":"0x2","status":"0x1","logs":[]}),
            ),
            states: Mutex::new(HashMap::from([(
                hx(sid(&descriptor())),
                state(6000000, 0, 0),
            )])),
            block: Mutex::new(
                json!({"number":"0x64","hash":hx(block_hash()),"parentHash":hx(B256::repeat_byte(0xbc)),"timestamp":format!("0x{TIME:x}"),"gasLimit":"0x1c9c380"}),
            ),
            fail: Mutex::new(false),
            reorg_after: Mutex::new(None),
            block_reads: AtomicUsize::new(0),
            code: Mutex::new(code),
            slots: Mutex::new(slots),
            digest: Mutex::new(domain().signing_context().digest(&Voucher {
                sessionId: B256::ZERO,
                cumulativeAmount: 0,
            })),
            calls: Mutex::new(vec![]),
        }
    }
    fn logs(&self, logs: Vec<Value>) {
        self.receipt.lock().unwrap()["logs"] = json!(logs);
    }
    fn session(&self, d: &SessionDescriptor, paid: u64) {
        self.states
            .lock()
            .unwrap()
            .insert(hx(sid(d)), state(6000000, paid, 0));
    }
    fn profile(&self) -> DeploymentProfile {
        let pin = |byte| CodePin::new(Address::repeat_byte(byte), keccak256([0x60, 0])).unwrap();
        DeploymentProfile::new(
            domain(),
            Address::repeat_byte(0x97),
            pin(0x99),
            pin(0x98),
            pin(0x97),
            pin(0x96),
        )
        .unwrap()
    }
}
#[tonic::async_trait]
impl Rpc for Mock {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        self.calls
            .lock()
            .unwrap()
            .push((method.into(), params.clone()));
        if *self.fail.lock().unwrap() {
            return Err(ChainError::Unavailable);
        }
        match method {
            "eth_chainId" => Ok(json!(format!("0x{CHAIN:x}"))),
            "eth_getBlockByNumber" => {
                let count = self.block_reads.fetch_add(1, Ordering::SeqCst) + 1;
                let mut block = self.block.lock().unwrap().clone();
                if self.reorg_after.lock().unwrap().is_some_and(|n| count > n) {
                    block["hash"] = json!(hx(B256::repeat_byte(0xee)));
                }
                Ok(block)
            }
            "eth_getTransactionReceipt" => {
                assert_eq!(params, json!([hx(tx())]));
                Ok(self.receipt.lock().unwrap().clone())
            }
            "eth_getCode" => {
                assert_eq!(
                    params[1],
                    json!({"blockHash":hx(block_hash()),"requireCanonical":true})
                );
                Ok(json!(
                    self.code.lock().unwrap()[params[0].as_str().unwrap()]
                ))
            }
            "eth_getStorageAt" => {
                assert_eq!(
                    params[2],
                    json!({"blockHash":hx(block_hash()),"requireCanonical":true})
                );
                Ok(json!(
                    self.slots.lock().unwrap()[params[0].as_str().unwrap()]
                ))
            }
            "eth_call" => {
                assert_eq!(
                    params[1],
                    json!({"blockHash":hx(block_hash()),"requireCanonical":true})
                );
                assert_eq!(params[0]["to"], hx(proxy()));
                let raw = params[0]["data"].as_str().unwrap();
                if params[0].get("from").is_some() {
                    return Ok(json!("0x"));
                }
                if raw.starts_with(&hx(&keccak256("getSession(bytes32)")[..4])) {
                    return self
                        .states
                        .lock()
                        .unwrap()
                        .get(&format!("0x{}", &raw[10..]))
                        .cloned()
                        .ok_or(ChainError::Unavailable);
                }
                if raw.starts_with(&hx(&keccak256("voucherDigest(bytes32,uint64)")[..4])) {
                    return Ok(json!(hx(*self.digest.lock().unwrap())));
                }
                if raw.starts_with(&hx(&keccak256("pauseLevel()")[..4])) {
                    return Ok(json!(hx(word(0))));
                }
                Err(ChainError::InvalidEvidence("unplanned fixture call"))
            }
            "eth_estimateGas" => {
                assert_eq!(params[1], "0x64");
                Ok(json!("0x13880"))
            }
            _ => panic!("unexpected RPC method {method}"),
        }
    }
}
fn setup() -> (Arc<Mock>, ChainReader) {
    let rpc = Arc::new(Mock::new());
    let reader = ChainReader::new(rpc.clone(), rpc.profile());
    (rpc, reader)
}
async fn reconcile(
    reader: &ChainReader,
    operation: &mppi_provider::contract::PreparedOperation,
) -> OperationResult {
    reader
        .reconcile(operation, "original", Some(tx()), Some("wallet-id"))
        .await
        .unwrap()
}

#[tokio::test]
async fn missing_failed_and_unavailable_receipts_keep_original_handles() {
    let (rpc, reader) = setup();
    let operation = calls().request_close(&descriptor()).unwrap();
    let original = rpc.receipt.lock().unwrap().clone();
    *rpc.receipt.lock().unwrap() = Value::Null;
    assert_eq!(
        reconcile(&reader, &operation).await.state(),
        OperationState::Pending
    );
    *rpc.receipt.lock().unwrap() = original;
    rpc.receipt.lock().unwrap()["status"] = json!("0x0");
    assert_eq!(
        reconcile(&reader, &operation).await.state(),
        OperationState::Failed
    );
    *rpc.fail.lock().unwrap() = true;
    let result = reconcile(&reader, &operation).await;
    assert_eq!(result.state(), OperationState::Unknown);
    assert_eq!(result.tx_hash(), Some(tx()));
    assert_eq!(result.wallet_operation_id(), Some("wallet-id"));
    assert_eq!(
        reader
            .reconcile(&operation, "original", None, None)
            .await
            .unwrap()
            .state(),
        OperationState::Unknown
    );
}

#[tokio::test]
async fn receipt_delta_does_not_claim_later_same_block_collection() {
    let (rpc, reader) = setup();
    let d = descriptor();
    rpc.session(&d, 5500000);
    rpc.logs(vec![settled(&d, 5420000, 420000, 0)]);
    let result = reconcile(
        &reader,
        &calls().settle(&d, 5510000, 5420000, &[1; 65]).unwrap(),
    )
    .await;
    assert_eq!(result.state(), OperationState::Confirmed);
    let reference = result.sessions()[0].settlement_reference().unwrap();
    assert_eq!(reference.delta(), 420000);
    assert_eq!(reference.settled_cumulative(), 5420000);
    reader.verify_settlement(&reference, &d).await.unwrap();
}

#[tokio::test]
async fn silent_noop_outer_success_and_ambiguous_batch_never_claim_payment() {
    let (rpc, reader) = setup();
    let d = descriptor();
    rpc.session(&d, 5420000);
    let sig = [1; 65];
    let item = (&d, 5510000, 5420000, sig.as_slice());
    for operation in [
        calls().settle(&d, 5510000, 5420000, &sig).unwrap(),
        calls().settle_batch(&[item]).unwrap(),
    ] {
        let result = reconcile(&reader, &operation).await;
        assert_eq!(result.state(), OperationState::Unknown);
        assert_eq!(result.sessions()[0].target_covered(), Some(true));
        assert!(result.sessions()[0].settlement_reference().is_none());
    }
    rpc.logs(vec![settled(&d, 5420000, 5420000, 0)]);
    let result = reconcile(&reader, &calls().settle_batch(&[item, item]).unwrap()).await;
    assert_eq!(result.state(), OperationState::Unknown);
    assert_eq!(result.sessions().len(), 2);
    assert!(
        result
            .sessions()
            .iter()
            .all(|x| x.settlement_reference().is_none())
    );
    assert_eq!(
        reconcile(&reader, &calls().settle_batch(&[]).unwrap())
            .await
            .state(),
        OperationState::Unknown
    );
}

#[tokio::test]
async fn batch_distinct_targets_and_independent_items_keep_ordered_attribution() {
    let (rpc, reader) = setup();
    let d = descriptor();
    let mut other = d.clone();
    other.salt = B256::repeat_byte(0x55);
    rpc.session(&d, 5420000);
    rpc.session(&other, 2);
    rpc.logs(vec![
        settled(&d, 4000000, 4000000, 0),
        settled(&d, 5420000, 1420000, 1),
        settled(&other, 2, 2, 2),
    ]);
    let sig = [1; 65];
    let result = reconcile(
        &reader,
        &calls()
            .settle_batch(&[
                (&d, 5510000, 4000000, &sig),
                (&other, 10, 2, &sig),
                (&d, 5510000, 5420000, &sig),
            ])
            .unwrap(),
    )
    .await;
    assert_eq!(result.state(), OperationState::Confirmed);
    let deltas = result
        .sessions()
        .iter()
        .map(|s| s.settlement_reference().unwrap().delta())
        .collect::<Vec<_>>();
    assert_eq!(deltas, vec![4000000, 2, 1420000]);
    let result = reconcile(
        &reader,
        &calls()
            .settle_batch(&[
                (&d, 5510000, 5420000, &sig),
                (&other, 10, 2, &sig),
                (&d, 5510000, 5420000, &sig),
            ])
            .unwrap(),
    )
    .await;
    assert_eq!(result.state(), OperationState::Confirmed);
    assert!(result.sessions()[0].settlement_reference().is_none());
    assert_eq!(
        result.sessions()[1].settlement_reference().unwrap().delta(),
        2
    );
    assert!(result.sessions()[2].settlement_reference().is_none());
}

#[tokio::test]
async fn every_log_inclusion_field_and_canonical_abi_is_checked() {
    let d = descriptor();
    let operation = calls().settle(&d, 5510000, 5420000, &[1; 65]).unwrap();
    for (field, value) in [
        ("transactionHash", json!(hx(B256::ZERO))),
        ("transactionIndex", json!("0x3")),
        ("blockHash", json!(hx(B256::ZERO))),
        ("blockNumber", json!("0x63")),
        ("removed", json!(true)),
        ("logIndex", json!("0x00")),
        ("topics", json!([])),
        (
            "data",
            json!(hx([
                word(5510000),
                word(5420000),
                word(5420000),
                word(5420000),
                word(0)
            ]
            .concat())),
        ),
    ] {
        let (rpc, reader) = setup();
        rpc.session(&d, 5420000);
        let mut log = settled(&d, 5420000, 5420000, 0);
        log[field] = value;
        rpc.logs(vec![log]);
        assert_eq!(
            reconcile(&reader, &operation).await.state(),
            OperationState::Unknown,
            "{field}"
        );
    }
    let (rpc, reader) = setup();
    rpc.session(&d, 5420000);
    rpc.logs(vec![
        settled(&d, 5420000, 5420000, 0),
        settled(&d, 5420000, 5420000, 0),
    ]);
    assert_eq!(
        reconcile(&reader, &operation).await.state(),
        OperationState::Unknown
    );
    rpc.logs(vec![settled(&d, 5420000, 5420000, 0)]);
    // The block read with the receipt's state reports another hash.
    rpc.block_reads.store(0, Ordering::SeqCst);
    *rpc.reorg_after.lock().unwrap() = Some(0);
    assert_eq!(
        reconcile(&reader, &operation).await.state(),
        OperationState::Unknown
    );
}

#[tokio::test]
async fn malformed_receipts_and_settlement_contradictions_remain_unknown() {
    let d = descriptor();
    let operation = calls().settle(&d, 5510000, 5420000, &[1; 65]).unwrap();
    for (field, value) in [
        ("transactionHash", json!(hx(B256::ZERO))),
        ("blockHash", json!(hx(B256::ZERO))),
        ("status", json!("0x2")),
        ("blockNumber", json!("0x064")),
        ("logs", Value::Null),
        ("transactionIndex", json!(2)),
    ] {
        let (rpc, reader) = setup();
        rpc.session(&d, 5420000);
        rpc.logs(vec![settled(&d, 5420000, 5420000, 0)]);
        rpc.receipt.lock().unwrap()[field] = value;
        assert_eq!(
            reconcile(&reader, &operation).await.state(),
            OperationState::Unknown,
            "{field}"
        );
    }
    for delta in [0, 5420001] {
        let (rpc, reader) = setup();
        rpc.session(&d, 5420000);
        rpc.logs(vec![settled(&d, 5420000, delta, 0)]);
        assert_eq!(
            reconcile(&reader, &operation).await.state(),
            OperationState::Unknown
        );
    }
    let (rpc, reader) = setup();
    rpc.session(&d, 5419999);
    rpc.logs(vec![settled(&d, 5420000, 5420000, 0)]);
    assert_eq!(
        reconcile(&reader, &operation).await.state(),
        OperationState::Unknown
    );
}

#[tokio::test]
async fn opening_checks_full_descriptor_and_accepts_later_same_block_activity() {
    use mppi_provider::capabilities::FundingAuthorization;
    let (rpc, reader) = setup();
    let d = descriptor();
    let auth = FundingAuthorization::new(
        CHAIN,
        ARC_USDC,
        "USDC".into(),
        "2".into(),
        d.agent,
        proxy(),
        U256::from(5510000),
        U256::from(1),
        U256::from(2),
        B256::ZERO,
    )
    .unwrap();
    let operation = calls()
        .open(&d, 5510000, B256::repeat_byte(0x66), &auth, &[1])
        .unwrap();
    rpc.states
        .lock()
        .unwrap()
        .insert(hx(sid(&d)), state(7000000, 5500000, 2));
    rpc.logs(vec![opened(&d)]);
    assert_eq!(
        reconcile(&reader, &operation).await.state(),
        OperationState::Confirmed
    );
    let mut wrong = d.clone();
    wrong.sessionSigner = Address::repeat_byte(0xfe);
    let mut log = opened(&wrong);
    log["topics"][1] = json!(hx(sid(&d)));
    rpc.logs(vec![log]);
    assert_eq!(
        reconcile(&reader, &operation).await.state(),
        OperationState::Unknown
    );
}

#[tokio::test]
async fn deployment_pins_slots_signing_domain_and_canonicality_are_enforced() {
    // Code at a pinned address cannot change once deployed (EIP-6780), so each
    // reader checks it once; implementation slots and the domain every time.
    for byte in [0x99, 0x98, 0x97, 0x96] {
        let (rpc, reader) = setup();
        let block = reader.block(None).await.unwrap();
        rpc.code
            .lock()
            .unwrap()
            .insert(hx(Address::repeat_byte(byte)), "0x6001".into());
        assert!(reader.verify_deployment(&block).await.is_err());
    }
    let (rpc, reader) = setup();
    let block = reader.block(None).await.unwrap();
    reader.verify_deployment(&block).await.unwrap();
    reader.verify_deployment(&block).await.unwrap();
    let calls = rpc.calls.lock().unwrap().clone();
    let count = |method| calls.iter().filter(|(m, _)| m == method).count();
    assert_eq!(
        (
            count("eth_chainId"),
            count("eth_getCode"),
            count("eth_getStorageAt")
        ),
        (1, 4, 4)
    );
    for byte in [0x99, 0x97] {
        let key = hx(Address::repeat_byte(byte));
        let previous = rpc
            .slots
            .lock()
            .unwrap()
            .insert(key.clone(), hx(B256::ZERO))
            .unwrap();
        assert!(reader.verify_deployment(&block).await.is_err());
        rpc.slots.lock().unwrap().insert(key, previous);
    }
    *rpc.digest.lock().unwrap() = B256::ZERO;
    assert!(reader.verify_deployment(&block).await.is_err());
    assert!(CodePin::new(Address::ZERO, B256::repeat_byte(1)).is_err());
    let p = rpc.profile();
    assert!(
        DeploymentProfile::new(
            Domain::new(1, proxy()).unwrap(),
            p.registry_address(),
            *p.proxy(),
            *p.implementation(),
            *p.registry(),
            *p.registry_implementation()
        )
        .is_err()
    );
}

#[tokio::test]
async fn session_enum_missing_state_and_strict_decoding_are_checked() {
    let (rpc, reader) = setup();
    let d = descriptor();
    let block = reader.block(None).await.unwrap();
    for (value, expected) in [
        (0, SessionStatus::Open),
        (1, SessionStatus::Closing),
        (2, SessionStatus::Closed),
    ] {
        rpc.states
            .lock()
            .unwrap()
            .insert(hx(sid(&d)), state(100, 50, value));
        assert_eq!(
            reader.session(sid(&d), &block).await.unwrap().status(),
            expected
        );
    }
    for bad in [
        state(100, 101, 0),
        state(100, 50, 3),
        state(0, 0, 0),
        json!(hx([
            word(100),
            word(0),
            word(0),
            word(0),
            word(TIME),
            vec![0x66; 32],
            word(0)
        ]
        .concat())),
    ] {
        rpc.states.lock().unwrap().insert(hx(sid(&d)), bad);
        assert!(reader.session(sid(&d), &block).await.is_err());
    }
    rpc.states
        .lock()
        .unwrap()
        .insert(hx(sid(&d)), json!(hx(vec![0; 192])));
    assert!(!reader.session(sid(&d), &block).await.unwrap().exists());
}

#[tokio::test]
async fn refunds_topups_and_close_requests_need_matching_event_and_state() {
    use mppi_provider::capabilities::FundingAuthorization;
    let d = descriptor();
    let c = calls();
    let auth = FundingAuthorization::new(
        CHAIN,
        ARC_USDC,
        "USDC".into(),
        "2".into(),
        d.agent,
        proxy(),
        U256::from(490000),
        U256::from(1),
        U256::from(2),
        B256::ZERO,
    )
    .unwrap();
    let cases = [
        (
            c.top_up(&d, 490000, &auth, &[1]).unwrap(),
            event(
                "ToppedUp(bytes32,uint64,uint64)",
                vec![sid(&d).to_vec()],
                vec![word(490000), word(6000000)],
                0,
            ),
            state(6000000, 5420000, 0),
        ),
        (
            c.request_close(&d).unwrap(),
            event(
                "CloseRequested(bytes32,uint64)",
                vec![sid(&d).to_vec()],
                vec![word(TIME + 3600)],
                0,
            ),
            json!(hx([
                word(6000000),
                word(5420000),
                word(1),
                word(TIME),
                word(TIME),
                vec![0x66; 32]
            ]
            .concat())),
        ),
        (
            c.close(&d, 5510000, 5420000, &[1; 65]).unwrap(),
            event(
                "SessionClosed(bytes32,uint64,uint64)",
                vec![sid(&d).to_vec()],
                vec![word(5420000), word(580000)],
                0,
            ),
            state(6000000, 5420000, 2),
        ),
        (
            c.withdraw(&d).unwrap(),
            event(
                "SessionClosed(bytes32,uint64,uint64)",
                vec![sid(&d).to_vec()],
                vec![word(5420000), word(580000)],
                0,
            ),
            state(6000000, 5420000, 2),
        ),
        (
            c.release_idle(&d).unwrap(),
            event(
                "IdleReleased(bytes32,uint64)",
                vec![sid(&d).to_vec()],
                vec![word(580000)],
                0,
            ),
            state(6000000, 5420000, 2),
        ),
    ];
    for (operation, log, session) in cases {
        let (rpc, reader) = setup();
        rpc.states.lock().unwrap().insert(hx(sid(&d)), session);
        rpc.logs(vec![log]);
        assert_eq!(
            reconcile(&reader, &operation).await.state(),
            OperationState::Confirmed
        );
        rpc.logs(vec![]);
        assert_eq!(
            reconcile(&reader, &operation).await.state(),
            OperationState::Unknown
        );
    }
}

#[tokio::test]
async fn simulation_is_exact_and_advisory_without_transaction_submission() {
    let (rpc, reader) = setup();
    let d = descriptor();
    let operation = calls()
        .settle_batch(&[(&d, 5510000, 5420000, &[1; 65])])
        .unwrap();
    let evidence = reader.simulate(&operation, d.operator).await.unwrap();
    assert_eq!(evidence.gas_estimate(), U256::from(80000));
    assert_eq!(evidence.gas_limit(), U256::from(30000000));
    let calls = rpc.calls.lock().unwrap();
    let (_, params) = calls
        .iter()
        .find(|(method, _)| method == "eth_estimateGas")
        .unwrap();
    assert_eq!(
        params[0],
        json!({"from":hx(d.operator),"to":hx(proxy()),"data":hx(operation.call().data()),"value":"0x0","gas":"0x1c9c380"})
    );
    assert!(
        calls
            .iter()
            .all(|(method, _)| !method.starts_with("eth_send"))
    );
}

struct Wallet {
    address: Address,
    response: Mutex<Result<OperationResult, WalletError>>,
    executions: AtomicUsize,
    lookups: AtomicUsize,
}
#[tonic::async_trait]
impl TransactionWallet for Wallet {
    fn address(&self) -> Address {
        self.address
    }
    async fn execute(&self, _: &WalletCall, _: &str) -> Result<OperationResult, WalletError> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.response.lock().unwrap().clone()
    }
    async fn reconcile(&self, _: &str) -> Result<OperationResult, WalletError> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        self.response.lock().unwrap().clone()
    }
}
#[tokio::test]
async fn uncertain_wallet_submission_is_only_looked_up_never_resubmitted() {
    let (rpc, reader) = setup();
    let d = descriptor();
    let operation = calls().settle(&d, 5510000, 5420000, &[1; 65]).unwrap();
    let wallet = Wallet {
        address: d.operator,
        response: Mutex::new(Err(WalletError)),
        executions: AtomicUsize::new(0),
        lookups: AtomicUsize::new(0),
    };
    assert_eq!(
        submit_operation(&wallet, &operation, "original", &reader)
            .await
            .unwrap()
            .state(),
        OperationState::Unknown
    );
    *wallet.response.lock().unwrap() = Ok(OperationResult::new(
        "original",
        OperationState::Unknown,
        Some("wallet-id".into()),
        Some(tx()),
        None,
        vec![],
    )
    .unwrap());
    rpc.session(&d, 5420000);
    rpc.logs(vec![settled(&d, 5420000, 5420000, 0)]);
    assert_eq!(
        reconcile_operation(&wallet, &operation, "original", &reader)
            .await
            .unwrap()
            .state(),
        OperationState::Confirmed
    );
    assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
    assert_eq!(wallet.lookups.load(Ordering::SeqCst), 1);
    *wallet.response.lock().unwrap() = Ok(OperationResult::new(
        "foreign",
        OperationState::Unknown,
        Some("foreign-id".into()),
        Some(tx()),
        None,
        vec![],
    )
    .unwrap());
    let result = reconcile_operation(&wallet, &operation, "original", &reader)
        .await
        .unwrap();
    assert_eq!(result.state(), OperationState::Unknown);
    assert!(result.tx_hash().is_none());
    assert!(result.wallet_operation_id().is_none());
}

#[tokio::test]
async fn a_wallet_cause_is_kept_but_never_compared() {
    let (_, reader) = setup();
    let d = descriptor();
    let operation = calls().settle(&d, 5510000, 5420000, &[1; 65]).unwrap();
    let plain = OperationResult::new(
        "original",
        OperationState::Unknown,
        None,
        None,
        None,
        vec![],
    )
    .unwrap();
    assert!(plain.clone().with_cause("").is_err());
    let wallet = Wallet {
        address: d.operator,
        response: Mutex::new(Ok(plain
            .clone()
            .with_cause("Unavailable (TIMEOUT)")
            .unwrap())),
        executions: AtomicUsize::new(0),
        lookups: AtomicUsize::new(0),
    };
    for result in [
        submit_operation(&wallet, &operation, "original", &reader)
            .await
            .unwrap(),
        reconcile_operation(&wallet, &operation, "original", &reader)
            .await
            .unwrap(),
    ] {
        assert_eq!(result.cause(), Some("Unavailable (TIMEOUT)"));
        assert_eq!(result, plain);
    }
}

#[tokio::test]
async fn wallet_authority_is_checked_for_every_operation_and_every_batch_item() {
    let (_, reader) = setup();
    let d = descriptor();
    let sig = [1; 65];
    let c = calls();
    let ops = [
        c.request_close(&d).unwrap(),
        c.withdraw(&d).unwrap(),
        c.settle(&d, 100, 50, &sig).unwrap(),
        c.settle_batch(&[(&d, 100, 50, &sig)]).unwrap(),
        c.close(&d, 100, 50, &sig).unwrap(),
        c.release_idle(&d).unwrap(),
    ];
    for address in [
        d.agent,
        d.operator,
        d.recipient,
        Address::repeat_byte(0xee),
        Address::ZERO,
    ] {
        for (i, op) in ops.iter().enumerate() {
            let wallet = Wallet {
                address,
                response: Mutex::new(Ok(OperationResult::new(
                    "original",
                    OperationState::Pending,
                    Some("wallet-id".into()),
                    None,
                    None,
                    vec![],
                )
                .unwrap())),
                executions: AtomicUsize::new(0),
                lookups: AtomicUsize::new(0),
            };
            let allowed = !address.is_zero()
                && if i < 2 {
                    address == d.agent
                } else if i == 5 {
                    true
                } else {
                    address == d.operator || address == d.recipient
                };
            assert_eq!(
                submit_operation(&wallet, op, "original", &reader)
                    .await
                    .is_ok(),
                allowed
            );
            assert_eq!(
                reconcile_operation(&wallet, op, "original", &reader)
                    .await
                    .is_ok(),
                allowed
            );
            assert_eq!(
                wallet.executions.load(Ordering::SeqCst),
                usize::from(allowed)
            );
            assert_eq!(wallet.lookups.load(Ordering::SeqCst), usize::from(allowed));
        }
    }
    let mut other = d.clone();
    other.operator = Address::repeat_byte(0xab);
    other.recipient = Address::repeat_byte(0xcd);
    let wallet = Wallet {
        address: d.operator,
        response: Mutex::new(Err(WalletError)),
        executions: AtomicUsize::new(0),
        lookups: AtomicUsize::new(0),
    };
    assert!(
        submit_operation(
            &wallet,
            &c.settle_batch(&[(&d, 100, 50, &sig), (&other, 100, 50, &sig)])
                .unwrap(),
            "original",
            &reader
        )
        .await
        .is_err()
    );
    assert_eq!(wallet.executions.load(Ordering::SeqCst), 0);
}
