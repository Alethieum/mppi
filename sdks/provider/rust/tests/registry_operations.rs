//! Registry operations tested with independent ABI encoding and adversarial RPC
//! evidence. Source profile: ERC-8004 implementation b9e466c, not the payment ABI.
use alloy_primitives::{hex, keccak256};
use mppi_provider::{
    Address, B256, DEFAULT_CATALOG_BYTES, U256,
    chain::{ChainError, ChainReader, CodePin, DeploymentProfile, Rpc},
    parse_catalog,
    registry::{RegistryCalls, RegistryKind, RegistryReader, agent_wallet_proof},
    signing::Voucher,
    types::{Domain, OperationState},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const CHAIN: u64 = 5042002;
const URI: &str = "https://provider.example/discover";
const CATALOG: &[u8] = br#"[{"input":"123","model":"example/model","output":"456"}]"#;
const REGISTERED: &str = "Registered(uint256,string,address)";
const TRANSFER: &str = "Transfer(address,address,uint256)";
const METADATA: &str = "MetadataSet(uint256,string,string,bytes)";
const URI_UPDATED: &str = "URIUpdated(uint256,string,address)";
fn owner() -> Address {
    Address::repeat_byte(0x11)
}
fn recipient() -> Address {
    Address::repeat_byte(0x22)
}
fn registry() -> Address {
    Address::repeat_byte(0x97)
}
fn proxy() -> Address {
    Address::repeat_byte(0x99)
}
fn block_hash() -> B256 {
    B256::repeat_byte(0xbb)
}
fn tx_hash() -> B256 {
    B256::repeat_byte(0xaa)
}
fn hx(value: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(value))
}
fn word(value: u64) -> Vec<u8> {
    full_word(U256::from(value))
}
fn full_word(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}
fn address_word(value: Address) -> Vec<u8> {
    [vec![0; 12], value.to_vec()].concat()
}
fn tail(value: &[u8]) -> Vec<u8> {
    let mut raw = word(value.len() as u64);
    raw.extend_from_slice(value);
    raw.resize(32 + value.len().div_ceil(32) * 32, 0);
    raw
}
fn dynamic(value: &[u8]) -> Vec<u8> {
    [word(32u64), tail(value)].concat()
}
fn two_dynamic(first: &[u8], second: &[u8]) -> Vec<u8> {
    [
        word(64u64),
        word((64 + tail(first).len()) as u64),
        tail(first),
        tail(second),
    ]
    .concat()
}
fn call(name: &str, arguments: Vec<u8>) -> Vec<u8> {
    [keccak256(name)[..4].to_vec(), arguments].concat()
}
fn domain() -> Domain {
    Domain::new(CHAIN, proxy()).unwrap()
}
fn profile() -> DeploymentProfile {
    let pin = |byte| CodePin::new(Address::repeat_byte(byte), keccak256([0x60, 0x00])).unwrap();
    DeploymentProfile::new(
        domain(),
        registry(),
        pin(0x99),
        pin(0x98),
        pin(0x97),
        pin(0x96),
    )
    .unwrap()
}
fn domain_response() -> Vec<u8> {
    let name = tail(b"ERC8004IdentityRegistry");
    let version = tail(b"1");
    let mut fields = vec![0; 32];
    fields[0] = 0x0f;
    [
        fields,
        word(224u64),
        word((224 + name.len()) as u64),
        word(CHAIN),
        address_word(registry()),
        vec![0; 32],
        word((224 + name.len() + version.len()) as u64),
        name,
        version,
        word(0u64),
    ]
    .concat()
}
fn event(signature: &str, topics: Vec<Vec<u8>>, raw: Vec<u8>, index: usize) -> Value {
    let mut all = vec![hx(keccak256(signature))];
    all.extend(topics.into_iter().map(hx));
    json!({"address":hx(registry()),"topics":all,"data":hx(raw),"logIndex":format!("0x{index:x}"),"removed":false,"transactionHash":hx(tx_hash()),"transactionIndex":"0x1","blockNumber":"0x64","blockHash":hx(block_hash())})
}
fn metadata(key: &str, value: &[u8], index: usize) -> Value {
    event(
        METADATA,
        vec![word(0u64), keccak256(key).to_vec()],
        two_dynamic(key.as_bytes(), value),
        index,
    )
}
fn registration_logs() -> Vec<Value> {
    vec![
        event(
            TRANSFER,
            vec![word(0u64), address_word(owner()), word(0u64)],
            vec![],
            0,
        ),
        event(
            REGISTERED,
            vec![word(0u64), address_word(owner())],
            dynamic(URI.as_bytes()),
            1,
        ),
        metadata("agentWallet", owner().as_slice(), 2),
    ]
}
struct State {
    owner: Address,
    timestamp: u64,
    wallet: Address,
    uri: Vec<u8>,
    catalog: Vec<u8>,
    proof_domain: Vec<u8>,
    receipt: Value,
    simulation: Option<(Vec<u8>, Address, Vec<u8>)>,
    workflow_simulation: bool,
    fail: Option<&'static str>,
    cause: ChainError,
    bad_code: bool,
    calls: Vec<(String, Value)>,
    replies: HashMap<Vec<u8>, Vec<u8>>,
}
struct MockRpc {
    state: Mutex<State>,
}
impl MockRpc {
    fn new() -> Self {
        Self {
            state: Mutex::new(State {
                owner: owner(),
                timestamp: 1000,
                wallet: owner(),
                uri: URI.as_bytes().to_vec(),
                catalog: vec![],
                proof_domain: domain_response(),
                receipt: json!({"transactionHash":hx(tx_hash()),"blockHash":hx(block_hash()),"blockNumber":"0x64","transactionIndex":"0x1","status":"0x1","logs":registration_logs()}),
                simulation: None,
                workflow_simulation: false,
                fail: None,
                cause: ChainError::Unavailable,
                bad_code: false,
                calls: vec![],
                replies: HashMap::new(),
            }),
        }
    }
    fn edit(&self, edit: impl FnOnce(&mut State)) {
        edit(&mut self.state.lock().unwrap());
    }
}
#[tonic::async_trait]
impl Rpc for MockRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        let mut state = self.state.lock().unwrap();
        state.calls.push((method.into(), params.clone()));
        if state.fail == Some(method) {
            return Err(state.cause);
        }
        match method {
            "eth_chainId" => Ok(json!(format!("0x{CHAIN:x}"))),
            "eth_getBlockByNumber" => Ok(
                json!({"number":"0x64","hash":hx(block_hash()),"parentHash":hx(B256::repeat_byte(0xbc)),"timestamp":format!("0x{:x}",state.timestamp)}),
            ),
            "eth_getCode" => Ok(json!(if state.bad_code { "0x6001" } else { "0x6000" })),
            "eth_getStorageAt" => Ok(json!(hx(address_word(if params[0] == hx(proxy()) {
                Address::repeat_byte(0x98)
            } else {
                Address::repeat_byte(0x96)
            })))),
            "eth_getTransactionReceipt" => Ok(state.receipt.clone()),
            "eth_call" => {
                assert_eq!(
                    params[1],
                    json!({"blockHash":hx(block_hash()),"requireCanonical":true})
                );
                let raw = hex::decode(
                    params[0]["data"]
                        .as_str()
                        .unwrap()
                        .strip_prefix("0x")
                        .unwrap(),
                )
                .unwrap();
                if params[0]["to"] == hx(proxy()) {
                    return Ok(json!(hx(domain().signing_context().digest(&Voucher {
                        sessionId: B256::ZERO,
                        cumulativeAmount: 0
                    }))));
                }
                assert_eq!(params[0]["to"], hx(registry()));
                if params[0].get("from").is_some() {
                    if state.workflow_simulation {
                        assert_eq!(params[0]["from"], hx(owner()));
                        assert_eq!(params[0]["value"], "0x0");
                        return Ok(json!(hx(
                            if raw[..4] == keccak256("register(string)")[..4]
                                || raw[..4] == keccak256("register()")[..4]
                            {
                                word(0u64)
                            } else {
                                vec![]
                            }
                        )));
                    }
                    let (expected, sender, response) =
                        state.simulation.as_ref().expect("unexpected simulation");
                    assert_eq!(&raw, expected);
                    assert_eq!(params[0]["from"], hx(sender));
                    assert_eq!(params[0]["value"], "0x0");
                    return Ok(json!(hx(response)));
                }
                if let Some(reply) = state.replies.get(&raw) {
                    return Ok(json!(hx(reply)));
                }
                let response = if raw == call("ownerOf(uint256)", word(0u64)) {
                    address_word(state.owner)
                } else if raw == call("getAgentWallet(uint256)", word(0u64)) {
                    address_word(state.wallet)
                } else if raw == call("tokenURI(uint256)", word(0u64)) {
                    dynamic(&state.uri)
                } else if raw
                    == call(
                        "getMetadata(uint256,string)",
                        [word(0u64), word(64u64), tail(b"mppi.catalog")].concat(),
                    )
                {
                    dynamic(&state.catalog)
                } else if raw == call("eip712Domain()", vec![]) {
                    state.proof_domain.clone()
                } else {
                    panic!("unexpected registry read {}", hx(raw));
                };
                Ok(json!(hx(response)))
            }
            _ => panic!("unexpected RPC method {method}"),
        }
    }
}
fn setup() -> (Arc<MockRpc>, RegistryReader, RegistryCalls) {
    let rpc = Arc::new(MockRpc::new());
    let reader = RegistryReader::new(Arc::new(ChainReader::new(rpc.clone(), profile())));
    (rpc, reader, RegistryCalls::new(CHAIN, registry()).unwrap())
}

#[test]
fn operation_calldata_matches_independent_abi_and_retains_uint256_identity() {
    let (_, _, calls) = setup();
    let id = U256::MAX;
    let operations = [
        (
            calls.register(owner(), None).unwrap(),
            call("register()", vec![]),
        ),
        (
            calls.register(owner(), Some(URI)).unwrap(),
            call("register(string)", dynamic(URI.as_bytes())),
        ),
        (
            calls.set_uri(owner(), id, URI).unwrap(),
            call(
                "setAgentURI(uint256,string)",
                [full_word(id), word(64u64), tail(URI.as_bytes())].concat(),
            ),
        ),
        (
            calls
                .set_agent_wallet(owner(), id, recipient(), U256::MAX, &[0x12, 0x34])
                .unwrap(),
            call(
                "setAgentWallet(uint256,address,uint256,bytes)",
                [
                    full_word(id),
                    address_word(recipient()),
                    full_word(U256::MAX),
                    word(128u64),
                    tail(&[0x12, 0x34]),
                ]
                .concat(),
            ),
        ),
        (
            calls
                .set_catalog(
                    owner(),
                    id,
                    &parse_catalog(CATALOG, DEFAULT_CATALOG_BYTES).unwrap(),
                )
                .unwrap(),
            call(
                "setMetadata(uint256,string,bytes)",
                [
                    full_word(id),
                    word(96u64),
                    word(160u64),
                    tail(b"mppi.catalog"),
                    tail(CATALOG),
                ]
                .concat(),
            ),
        ),
    ];
    for (op, expected) in operations {
        assert_eq!(op.call().data(), expected);
        assert_eq!(op.call().chain_id(), CHAIN);
        assert_eq!(op.call().to(), registry());
        assert_eq!(op.owner(), owner());
    }
    let op = calls
        .set_agent_wallet(owner(), id, recipient(), U256::MAX, &[0x12, 0x34])
        .unwrap();
    assert_eq!(op.kind(), RegistryKind::SetAgentWallet);
    assert_eq!(op.agent_id(), Some(id));
    assert_eq!(op.deadline(), Some(U256::MAX));
    assert_eq!(op.new_wallet(), Some(recipient()));
    assert_eq!(op.signature(), Some(&[0x12, 0x34][..]));
    assert!(!format!("{op:?}").contains("signature"));
}

#[test]
fn operation_inputs_reject_wrong_network_zero_addresses_and_unbounded_proofs() {
    assert!(RegistryCalls::new(1, registry()).is_err());
    assert!(RegistryCalls::new(CHAIN, Address::ZERO).is_err());
    let (_, _, calls) = setup();
    assert!(calls.register(Address::ZERO, None).is_err());
    assert!(
        calls
            .set_agent_wallet(owner(), U256::ZERO, Address::ZERO, U256::ZERO, &[1])
            .is_err()
    );
    for signature in [vec![], vec![1; (64 << 10) + 1]] {
        assert!(
            calls
                .set_agent_wallet(owner(), U256::ZERO, recipient(), U256::ZERO, &signature)
                .is_err()
        );
    }
    assert!(
        calls
            .register(owner(), Some(&"x".repeat((1 << 20) + 1)))
            .is_err()
    );
    assert!(
        calls
            .set_uri(owner(), U256::ZERO, &"x".repeat((1 << 20) + 1))
            .is_err()
    );
}

#[test]
fn recipient_typed_data_preserves_full_unsigned_numbers_and_exact_domain() {
    let proof = agent_wallet_proof(
        CHAIN,
        registry(),
        U256::MAX,
        recipient(),
        owner(),
        U256::MAX,
    )
    .unwrap();
    assert_eq!(
        proof["domain"],
        json!({"name":"ERC8004IdentityRegistry","version":"1","chainId":CHAIN,"verifyingContract":hx(registry())})
    );
    assert_eq!(proof["primaryType"], "AgentWalletSet");
    assert_eq!(
        proof["message"]["agentId"].to_string(),
        U256::MAX.to_string()
    );
    assert_eq!(
        proof["message"]["deadline"].to_string(),
        U256::MAX.to_string()
    );
    assert_eq!(
        proof["types"]["AgentWalletSet"],
        json!([{"name":"agentId","type":"uint256"},{"name":"newWallet","type":"address"},{"name":"owner","type":"address"},{"name":"deadline","type":"uint256"}])
    );
}

#[tokio::test]
async fn raw_identity_allows_empty_initial_catalog_and_cleared_wallet() {
    let (rpc, reader, _) = setup();
    rpc.edit(|s| s.wallet = Address::ZERO);
    let identity = reader.read(U256::ZERO, None).await.unwrap();
    assert_eq!(identity.agent_id(), U256::ZERO);
    assert_eq!(identity.agent_wallet(), Address::ZERO);
    assert_eq!(identity.owner(), owner());
    assert_eq!(identity.agent_uri(), URI);
    assert!(identity.catalog_bytes().is_empty());
    assert_eq!(identity.observation().block_hash(), block_hash());
}

#[tokio::test]
async fn raw_identity_rejects_missing_owner_and_oversized_fields() {
    for case in 0..4 {
        let (rpc, reader, _) = setup();
        rpc.edit(|s| match case {
            0 => s.owner = Address::ZERO,
            1 => s.uri = vec![b'x'; (1 << 20) + 1],
            2 => s.catalog = vec![0; DEFAULT_CATALOG_BYTES + 1],
            _ => s.bad_code = true,
        });
        assert!(reader.read(U256::ZERO, None).await.is_err(), "case {case}");
    }
}

#[tokio::test]
async fn live_recipient_proof_checks_inclusive_deadline_domain_and_owner() {
    let (rpc, reader, _) = setup();
    for deadline in [1000, 1300] {
        assert!(
            reader
                .wallet_proof(U256::ZERO, recipient(), owner(), U256::from(deadline), None)
                .await
                .is_ok()
        );
    }
    for deadline in [999, 1301] {
        assert!(
            reader
                .wallet_proof(U256::ZERO, recipient(), owner(), U256::from(deadline), None)
                .await
                .is_err()
        );
    }
    assert!(
        reader
            .wallet_proof(U256::ZERO, recipient(), recipient(), U256::from(1200), None)
            .await
            .is_err()
    );
    rpc.edit(|s| s.proof_domain[0] = 0x1f);
    assert!(
        reader
            .wallet_proof(U256::ZERO, recipient(), owner(), U256::from(1200), None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn live_recipient_proof_rejects_each_changed_domain_binding() {
    for index in [95, 127, 159, 191] {
        let (rpc, reader, _) = setup();
        rpc.edit(|s| s.proof_domain[index] ^= 1);
        assert!(
            reader
                .wallet_proof(U256::ZERO, recipient(), owner(), U256::from(1200), None)
                .await
                .is_err(),
            "index {index}"
        );
    }
}

#[tokio::test]
async fn simulation_uses_exact_owner_and_calldata_without_adopting_predicted_id() {
    let (rpc, reader, calls) = setup();
    let op = calls.register(owner(), Some(URI)).unwrap();
    rpc.edit(|s| s.simulation = Some((op.call().data().to_vec(), owner(), full_word(U256::MAX))));
    reader.simulate(&op).await.unwrap();
    assert_eq!(op.agent_id(), None);
    let op = calls
        .set_agent_wallet(
            owner(),
            U256::ZERO,
            recipient(),
            U256::from(1200),
            &[0xab; 65],
        )
        .unwrap();
    rpc.edit(|s| s.simulation = Some((op.call().data().to_vec(), owner(), vec![])));
    reader.simulate(&op).await.unwrap();
}

#[tokio::test]
async fn simulation_rejects_operators_wrong_profile_and_unexpected_outputs() {
    let (rpc, reader, calls) = setup();
    let op = calls.set_uri(recipient(), U256::ZERO, URI).unwrap();
    assert!(reader.simulate(&op).await.is_err());
    let wrong = RegistryCalls::new(CHAIN, Address::repeat_byte(0xee))
        .unwrap()
        .register(owner(), Some(URI))
        .unwrap();
    assert!(reader.simulate(&wrong).await.is_err());
    let op = calls.set_uri(owner(), U256::ZERO, URI).unwrap();
    rpc.edit(|s| s.simulation = Some((op.call().data().to_vec(), owner(), word(0u64))));
    assert!(reader.simulate(&op).await.is_err());
}

#[tokio::test]
async fn confirmed_registration_uses_unique_events_and_canonical_zero_id_state() {
    let (_, reader, calls) = setup();
    let op = calls.register(owner(), Some(URI)).unwrap();
    let result = reader
        .reconcile(&op, "original", Some(tx_hash()), Some("wallet-operation"))
        .await
        .unwrap();
    assert_eq!(result.operation().state(), OperationState::Confirmed);
    assert_eq!(result.operation().operation_ref(), "original");
    assert_eq!(
        result.operation().wallet_operation_id(),
        Some("wallet-operation")
    );
    assert_eq!(result.identity().unwrap().agent_id(), U256::ZERO);
}

#[tokio::test]
async fn successful_outer_receipt_requires_each_matching_registry_effect_and_state() {
    for case in 0..9 {
        let (rpc, reader, calls) = setup();
        rpc.edit(|s| match case {
            0 => s.receipt["logs"] = json!([]),
            1 => {
                s.receipt["logs"].as_array_mut().unwrap().remove(0);
            }
            2 => {
                s.receipt["logs"].as_array_mut().unwrap().remove(2);
            }
            3 => s.wallet = recipient(),
            4 => s.owner = recipient(),
            5 => s.catalog = CATALOG.to_vec(),
            6 => s.uri = b"https://other.example/discover".to_vec(),
            7 => s.receipt["logs"][1]["address"] = hx(recipient()).into(),
            _ => s.receipt["logs"].as_array_mut().unwrap().push(event(
                REGISTERED,
                vec![word(0u64), address_word(owner())],
                dynamic(URI.as_bytes()),
                3,
            )),
        });
        let result = reader
            .reconcile(
                &calls.register(owner(), Some(URI)).unwrap(),
                "original",
                Some(tx_hash()),
                Some("handle"),
            )
            .await
            .unwrap();
        assert_eq!(
            result.operation().state(),
            OperationState::Unknown,
            "case {case}"
        );
        assert!(result.identity().is_none());
        assert_eq!(result.operation().wallet_operation_id(), Some("handle"));
    }
}

#[tokio::test]
async fn receipt_validation_rejects_noncanonical_conflicting_and_malformed_inclusion() {
    for case in 0..10 {
        let (rpc, reader, calls) = setup();
        rpc.edit(|s| match case {
            0 => s.receipt["blockHash"] = hx(B256::ZERO).into(),
            1 => s.receipt["transactionHash"] = hx(B256::ZERO).into(),
            2 => s.receipt["status"] = "0x2".into(),
            3 => s.receipt["logs"][1]["removed"] = true.into(),
            4 => s.receipt["logs"][1]["logIndex"] = "0x0".into(),
            5 => s.receipt["logs"][1]["transactionHash"] = hx(B256::ZERO).into(),
            6 => s.receipt["logs"][1]["transactionIndex"] = "0x2".into(),
            7 => s.receipt["logs"][1]["blockNumber"] = "0x65".into(),
            8 => s.receipt["logs"][1]["blockHash"] = hx(B256::ZERO).into(),
            _ => s.bad_code = true,
        });
        let result = reader
            .reconcile(
                &calls.register(owner(), Some(URI)).unwrap(),
                "original",
                Some(tx_hash()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            result.operation().state(),
            OperationState::Unknown,
            "case {case}"
        );
    }
}

#[tokio::test]
async fn pending_unknown_and_canonical_failure_preserve_original_identifiers() {
    let (rpc, reader, calls) = setup();
    let op = calls.register(owner(), Some(URI)).unwrap();
    let absent = reader
        .reconcile(&op, "original", None, Some("handle"))
        .await
        .unwrap();
    assert_eq!(absent.operation().state(), OperationState::Unknown);
    rpc.edit(|s| s.receipt = Value::Null);
    let pending = reader
        .reconcile(&op, "original", Some(tx_hash()), Some("handle"))
        .await
        .unwrap();
    assert_eq!(pending.operation().state(), OperationState::Pending);
    rpc.edit(|s| s.fail = Some("eth_getTransactionReceipt"));
    let unknown = reader
        .reconcile(&op, "original", Some(tx_hash()), Some("handle"))
        .await
        .unwrap();
    assert_eq!(unknown.operation().state(), OperationState::Unknown);
    assert_eq!(unknown.operation().tx_hash(), Some(tx_hash()));
    assert_eq!(unknown.operation().wallet_operation_id(), Some("handle"));
    rpc.edit(|s|{s.fail=None;s.receipt=json!({"transactionHash":hx(tx_hash()),"blockHash":hx(block_hash()),"blockNumber":"0x64","status":"0x0"});});
    let failed = reader
        .reconcile(&op, "original", Some(tx_hash()), Some("handle"))
        .await
        .unwrap();
    assert_eq!(failed.operation().state(), OperationState::Failed);
    assert!(failed.operation().observation().is_some());
    assert!(
        reader
            .reconcile(&op, "", Some(tx_hash()), None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn unrelated_bundled_registration_with_non_utf8_uri_does_not_block_matching_identity() {
    let (rpc, reader, calls) = setup();
    rpc.edit(|s| {
        s.receipt["logs"].as_array_mut().unwrap().push(event(
            REGISTERED,
            vec![word(1u64), address_word(recipient())],
            dynamic(&[0xff]),
            3,
        ))
    });
    let result = reader
        .reconcile(
            &calls.register(owner(), Some(URI)).unwrap(),
            "original",
            Some(tx_hash()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.operation().state(), OperationState::Confirmed);
}

#[tokio::test]
async fn each_field_update_requires_its_own_exact_event_and_resulting_state() {
    for kind in 0..3 {
        for corrupt in [false, true] {
            let (rpc, reader, calls) = setup();
            let op = match kind {
                0 => calls
                    .set_agent_wallet(owner(), U256::ZERO, recipient(), U256::from(1200), &[1; 65])
                    .unwrap(),
                1 => calls
                    .set_uri(owner(), U256::ZERO, "https://next.example/discover")
                    .unwrap(),
                _ => calls
                    .set_catalog(
                        owner(),
                        U256::ZERO,
                        &parse_catalog(CATALOG, DEFAULT_CATALOG_BYTES).unwrap(),
                    )
                    .unwrap(),
            };
            rpc.edit(|s| match kind {
                0 => {
                    let expected_wallet = if corrupt { owner() } else { recipient() };
                    s.wallet = recipient();
                    s.receipt["logs"] =
                        json!([metadata("agentWallet", expected_wallet.as_slice(), 0)]);
                }
                1 => {
                    s.uri = b"https://next.example/discover".to_vec();
                    s.receipt["logs"] = json!([event(
                        URI_UPDATED,
                        vec![
                            word(0u64),
                            address_word(if corrupt { recipient() } else { owner() })
                        ],
                        dynamic(&s.uri),
                        0
                    )]);
                }
                _ => {
                    s.catalog = CATALOG.to_vec();
                    s.receipt["logs"] = json!([metadata(
                        "mppi.catalog",
                        if corrupt { b"[]" } else { CATALOG },
                        0
                    )]);
                }
            });
            let result = reader
                .reconcile(&op, "original", Some(tx_hash()), None)
                .await
                .unwrap();
            assert_eq!(
                result.operation().state(),
                if corrupt {
                    OperationState::Unknown
                } else {
                    OperationState::Confirmed
                },
                "kind {kind} corrupt {corrupt}"
            );
        }
    }
}

#[tokio::test]
async fn metadata_indexed_key_must_match_exact_decoded_key_and_event_is_unique() {
    for duplicate in [false, true] {
        let (rpc, reader, calls) = setup();
        rpc.edit(|s| {
            s.catalog = CATALOG.to_vec();
            s.receipt["logs"] = if duplicate {
                json!([
                    metadata("mppi.catalog", CATALOG, 0),
                    metadata("mppi.catalog", CATALOG, 1)
                ])
            } else {
                json!([event(
                    METADATA,
                    vec![word(0u64), keccak256("mppi.catalog").to_vec()],
                    two_dynamic(b"other", CATALOG),
                    0
                )])
            };
        });
        let op = calls
            .set_catalog(
                owner(),
                U256::ZERO,
                &parse_catalog(CATALOG, DEFAULT_CATALOG_BYTES).unwrap(),
            )
            .unwrap();
        assert_eq!(
            reader
                .reconcile(&op, "original", Some(tx_hash()), None)
                .await
                .unwrap()
                .operation()
                .state(),
            OperationState::Unknown
        );
    }
}

#[path = "support/configuration.rs"]
mod configuration;
use mppi_provider::{
    capabilities::{TransactionWallet, WalletError},
    config::ProviderDeclarations,
    onboarding::{OnboardingOptions, OnboardingProgress, SignedRecipientProof, onboard},
    types::{OperationResult, WalletCall},
};
use std::path::PathBuf;

fn declarations() -> ProviderDeclarations {
    let mut value = configuration::configuration();
    value.as_object_mut().unwrap().remove("identity");
    value["models"][0]["rates"] = json!({"input":"123","output":"456"});
    ProviderDeclarations::from_mapping(&value).unwrap()
}
struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("mppi-onboard-{}", hex::encode(nonce)));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn options(&self) -> OnboardingOptions {
        OnboardingOptions {
            state_path: self.0.join("setup.json"),
            adopted_identity: None,
            recipient_proof: None,
        }
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum WalletMode {
    Complete,
    Lost,
    Hold,
    Conflict,
    Revert,
    OmitHash,
    OmitHandle,
    AlterHash,
    AlterHandle,
    PreSubmissionFailure,
    /// Fails before its own record, which then has nothing to reconcile.
    UnrecordedFailure,
    AttemptedUnknown,
}
fn call_bytes(raw: &[u8], index: usize) -> Vec<u8> {
    let offset = usize::try_from(U256::from_be_slice(
        &raw[4 + index * 32..4 + (index + 1) * 32],
    ))
    .unwrap()
        + 4;
    let length = usize::try_from(U256::from_be_slice(&raw[offset..offset + 32])).unwrap();
    raw[offset + 32..offset + 32 + length].to_vec()
}
struct SetupWallet {
    rpc: Arc<MockRpc>,
    path: PathBuf,
    mode: Mutex<WalletMode>,
    writes: Mutex<Vec<(WalletCall, String)>>,
    submissions: Mutex<Vec<String>>,
    results: Mutex<HashMap<String, OperationResult>>,
    block: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl SetupWallet {
    fn new(rpc: Arc<MockRpc>, path: PathBuf) -> Self {
        rpc.edit(|s| s.workflow_simulation = true);
        Self {
            rpc,
            path,
            mode: Mutex::new(WalletMode::Complete),
            writes: Mutex::new(vec![]),
            submissions: Mutex::new(vec![]),
            results: Mutex::new(HashMap::new()),
            block: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }
    fn mode(&self, mode: WalletMode) {
        *self.mode.lock().unwrap() = mode;
    }
    fn write_count(&self) -> usize {
        self.writes.lock().unwrap().len()
    }
    fn apply(&self, call: &WalletCall) {
        let raw = call.data();
        self.rpc.edit(|s|{
            let events=if raw[..4]==keccak256("register()")[..4] {
                s.wallet=owner();s.uri=vec![];s.catalog=vec![];
                let mut logs=registration_logs();logs[1]["data"]=json!(hx(dynamic(b"")));logs
            } else if raw[..4]==keccak256("setAgentWallet(uint256,address,uint256,bytes)")[..4] {
                let retained:Value=serde_json::from_slice(&std::fs::read(&self.path).unwrap()).unwrap();
                if retained["kind"]=="mppi-provider-onboarding" {assert!(s.catalog.is_empty(),"initial catalog must follow recipient");}
                s.wallet=Address::from_slice(&raw[48..68]);vec![metadata("agentWallet",s.wallet.as_slice(),0)]
            } else if raw[..4]==keccak256("setMetadata(uint256,string,bytes)")[..4] {
                let retained:Value=serde_json::from_slice(&std::fs::read(&self.path).unwrap()).unwrap();
                if retained["kind"]=="mppi-provider-onboarding" {assert_eq!(s.wallet,recipient(),"recipient must be confirmed before initial catalog");assert_eq!(s.uri,URI.as_bytes());}
                let retained:Value=serde_json::from_slice(&std::fs::read(&self.path).unwrap()).unwrap();
                assert_eq!(retained["registration"]["registrations"][0]["agentId"],0,"registration document must be prepared before catalog");
                s.catalog=call_bytes(raw,2);vec![metadata("mppi.catalog",&s.catalog,0)]
            } else if raw[..4]==keccak256("setAgentURI(uint256,string)")[..4] {
                s.uri=call_bytes(raw,1);vec![event(URI_UPDATED,vec![word(0u64),address_word(owner())],dynamic(&s.uri),0)]
            } else {panic!("unexpected workflow call");};
            s.receipt=json!({"transactionHash":hx(tx_hash()),"blockHash":hx(block_hash()),"blockNumber":"0x64","transactionIndex":"0x1","status":"0x1","logs":events});
        });
    }
}
#[tonic::async_trait]
impl TransactionWallet for SetupWallet {
    fn address(&self) -> Address {
        owner()
    }
    async fn execute(
        &self,
        call: &WalletCall,
        reference: &str,
    ) -> Result<OperationResult, WalletError> {
        let persisted: Value = serde_json::from_slice(&std::fs::read(&self.path).unwrap()).unwrap();
        assert_eq!(
            persisted["pending"]["prepared"]["calldata"],
            hx(call.data()),
            "intent must be durable before wallet submission"
        );
        assert_eq!(persisted["pending"]["result"]["operation_ref"], reference);
        if persisted["kind"] == "mppi-provider-update" {
            assert_eq!(
                persisted["registration"]["services"][0]["endpoint"],
                persisted["desired"]["provider"]["public_origin"],
                "proposed registration document must be retained before any update mutation"
            );
            assert_eq!(persisted["registration"]["registrations"][0]["agentId"], 0);
        }
        self.writes
            .lock()
            .unwrap()
            .push((call.clone(), reference.to_owned()));
        if self.block.load(std::sync::atomic::Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let mode = *self.mode.lock().unwrap();
        if mode == WalletMode::UnrecordedFailure {
            return Ok(OperationResult::new(
                reference,
                OperationState::Failed,
                None,
                None,
                None,
                vec![],
            )
            .unwrap());
        }
        if mode != WalletMode::PreSubmissionFailure {
            self.submissions.lock().unwrap().push(reference.to_owned());
        }
        if matches!(
            mode,
            WalletMode::PreSubmissionFailure | WalletMode::AttemptedUnknown
        ) {
            let state = if mode == WalletMode::PreSubmissionFailure {
                OperationState::Failed
            } else {
                OperationState::Unknown
            };
            let result = OperationResult::new(reference, state, None, None, None, vec![]).unwrap();
            self.results
                .lock()
                .unwrap()
                .insert(reference.to_owned(), result.clone());
            return Ok(result);
        }
        let result = OperationResult::new(
            reference,
            OperationState::Pending,
            Some(format!("wallet-{reference}")),
            if mode == WalletMode::Hold {
                None
            } else {
                Some(tx_hash())
            },
            None,
            vec![],
        )
        .unwrap();
        self.results
            .lock()
            .unwrap()
            .insert(reference.to_owned(), result.clone());
        if mode != WalletMode::Hold {
            self.apply(call);
        }
        if mode == WalletMode::Revert {
            self.rpc.edit(|s| s.receipt["status"] = "0x0".into());
        }
        if mode == WalletMode::Lost {
            Err(WalletError)
        } else {
            Ok(result)
        }
    }
    async fn reconcile(&self, reference: &str) -> Result<OperationResult, WalletError> {
        let mode = *self.mode.lock().unwrap();
        if mode == WalletMode::Lost {
            return Err(WalletError);
        }
        if mode == WalletMode::Conflict {
            return Ok(OperationResult::new(
                "foreign-reference",
                OperationState::Unknown,
                None,
                None,
                None,
                vec![],
            )
            .unwrap());
        }
        let known = self.results.lock().unwrap().get(reference).cloned();
        let known = match (known, mode) {
            (Some(known), _) => known,
            (None, WalletMode::UnrecordedFailure) => {
                return Ok(OperationResult::new(
                    reference,
                    OperationState::Unknown,
                    None,
                    None,
                    None,
                    vec![],
                )
                .unwrap());
            }
            (None, _) => return Err(WalletError),
        };
        if matches!(
            mode,
            WalletMode::OmitHash
                | WalletMode::OmitHandle
                | WalletMode::AlterHash
                | WalletMode::AlterHandle
        ) {
            return Ok(OperationResult::new(
                reference,
                OperationState::Unknown,
                match mode {
                    WalletMode::OmitHandle => None,
                    WalletMode::AlterHandle => Some("foreign-handle".into()),
                    _ => known.wallet_operation_id().map(str::to_owned),
                },
                match mode {
                    WalletMode::OmitHash => None,
                    WalletMode::AlterHash => Some(B256::repeat_byte(0xab)),
                    _ => known.tx_hash(),
                },
                None,
                vec![],
            )
            .unwrap());
        }
        Ok(known)
    }
}
fn workflow() -> (TestDir, Arc<MockRpc>, Arc<RegistryReader>, Arc<SetupWallet>) {
    let dir = TestDir::new();
    let (rpc, reader, _) = setup();
    let wallet = Arc::new(SetupWallet::new(rpc.clone(), dir.options().state_path));
    (dir, rpc, Arc::new(reader), wallet)
}

#[tokio::test]
async fn onboarding_registers_then_exports_proof_and_publishes_catalog_last() {
    let (dir, _, registry, wallet) = workflow();
    let first = onboard(
        &declarations(),
        wallet.clone(),
        registry.clone(),
        dir.options(),
    )
    .await
    .unwrap();
    assert_eq!(first.progress(), OnboardingProgress::RecipientProofRequired);
    assert_eq!(first.identity().unwrap().agent_id(), U256::ZERO);
    assert_eq!(wallet.write_count(), 1);
    let mut options = dir.options();
    options.recipient_proof = Some(
        SignedRecipientProof::new(first.recipient_proof().unwrap().clone(), vec![1; 65]).unwrap(),
    );
    let complete = onboard(&declarations(), wallet.clone(), registry.clone(), options)
        .await
        .unwrap();
    assert_eq!(complete.progress(), OnboardingProgress::Complete);
    assert_eq!(wallet.write_count(), 4);
    assert_eq!(
        complete.config().unwrap().identity().provider_id(),
        U256::ZERO
    );
    assert_eq!(complete.identity().unwrap().agent_wallet(), recipient());
    let before = std::fs::read(dir.options().state_path).unwrap();
    let rerun = onboard(&declarations(), wallet.clone(), registry, dir.options())
        .await
        .unwrap();
    assert_eq!(rerun.progress(), OnboardingProgress::Complete);
    assert!(!rerun.submitted());
    assert_eq!(wallet.write_count(), 4);
    assert_eq!(std::fs::read(dir.options().state_path).unwrap(), before);
}

#[tokio::test]
async fn onboarding_lost_registration_response_queries_original_without_second_mint() {
    let (dir, _, registry, wallet) = workflow();
    wallet.mode(WalletMode::Lost);
    assert!(
        onboard(
            &declarations(),
            wallet.clone(),
            registry.clone(),
            dir.options()
        )
        .await
        .is_err()
    );
    assert_eq!(wallet.write_count(), 1);
    let original = std::fs::read(dir.options().state_path).unwrap();
    assert!(
        onboard(
            &declarations(),
            wallet.clone(),
            registry.clone(),
            dir.options()
        )
        .await
        .is_err()
    );
    assert_eq!(std::fs::read(dir.options().state_path).unwrap(), original);
    assert_eq!(wallet.write_count(), 1);
    wallet.mode(WalletMode::Complete);
    let resumed = onboard(&declarations(), wallet.clone(), registry, dir.options())
        .await
        .unwrap();
    assert_eq!(
        resumed.progress(),
        OnboardingProgress::RecipientProofRequired
    );
    assert_eq!(wallet.write_count(), 1);
}

#[tokio::test]
async fn pending_operation_preserves_original_handles_and_rejects_substitution() {
    let (dir, _, registry, wallet) = workflow();
    wallet.mode(WalletMode::Hold);
    let pending = onboard(
        &declarations(),
        wallet.clone(),
        registry.clone(),
        dir.options(),
    )
    .await
    .unwrap();
    let handle = pending
        .operation()
        .unwrap()
        .wallet_operation_id()
        .unwrap()
        .to_owned();
    wallet.mode(WalletMode::Conflict);
    let before = std::fs::read(dir.options().state_path).unwrap();
    assert!(
        onboard(&declarations(), wallet.clone(), registry, dir.options())
            .await
            .is_err()
    );
    let after = std::fs::read(dir.options().state_path).unwrap();
    assert_eq!(after, before);
    let record: Value = serde_json::from_slice(&after).unwrap();
    assert_eq!(record["pending"]["result"]["wallet_operation_id"], handle);
    assert_eq!(wallet.write_count(), 1);
}

#[tokio::test]
async fn recipient_proof_must_match_export_and_completed_binding_needs_no_signature() {
    let (dir, rpc, registry, wallet) = workflow();
    let first = onboard(
        &declarations(),
        wallet.clone(),
        registry.clone(),
        dir.options(),
    )
    .await
    .unwrap();
    let mut wrong = first.recipient_proof().unwrap().clone();
    wrong["message"]["owner"] = hx(recipient()).into();
    let mut options = dir.options();
    options.recipient_proof = Some(SignedRecipientProof::new(wrong, vec![1; 65]).unwrap());
    assert!(
        onboard(&declarations(), wallet.clone(), registry.clone(), options)
            .await
            .is_err()
    );
    assert_eq!(wallet.write_count(), 1);
    rpc.edit(|s| s.wallet = recipient());
    let complete = onboard(&declarations(), wallet.clone(), registry, dir.options())
        .await
        .unwrap();
    assert_eq!(complete.progress(), OnboardingProgress::Complete);
    assert_eq!(wallet.write_count(), 3);
}

#[tokio::test]
async fn adopted_identity_and_incomplete_uri_are_completed_without_registration() {
    let (dir, rpc, registry, wallet) = workflow();
    rpc.edit(|s| {
        s.wallet = recipient();
        s.uri = vec![];
    });
    let mut options = dir.options();
    options.adopted_identity = Some(U256::ZERO);
    let complete = onboard(&declarations(), wallet.clone(), registry, options)
        .await
        .unwrap();
    assert_eq!(complete.progress(), OnboardingProgress::Complete);
    assert_eq!(wallet.write_count(), 2);
    assert!(
        wallet
            .writes
            .lock()
            .unwrap()
            .iter()
            .all(|(call, _)| call.data()[..4] != keccak256("register(string)")[..4])
    );
}

#[tokio::test]
async fn published_conflicts_cannot_be_silently_changed_by_onboard() {
    for case in 0..4 {
        let (dir, rpc, registry, wallet) = workflow();
        rpc.edit(|s| {
            s.catalog = CATALOG.to_vec();
            s.wallet = recipient();
            match case {
                0 => s.owner = recipient(),
                1 => s.wallet = owner(),
                2 => s.uri = b"https://other.example/discover".to_vec(),
                _ => s.catalog = b"[]".to_vec(),
            }
        });
        let mut options = dir.options();
        options.adopted_identity = Some(U256::ZERO);
        assert!(
            onboard(&declarations(), wallet.clone(), registry, options)
                .await
                .is_err()
        );
        assert_eq!(wallet.write_count(), 0);
    }
}

#[tokio::test]
async fn a_failed_chain_read_keeps_its_cause_without_remote_text() {
    let (dir, rpc, registry, wallet) = workflow();
    let cause = ChainError::Rpc(-32000);
    rpc.edit(|s| {
        s.fail = Some("eth_call");
        s.cause = cause;
    });
    let mut options = dir.options();
    options.adopted_identity = Some(U256::ZERO);
    let error = onboard(&declarations(), wallet.clone(), registry, options)
        .await
        .unwrap_err();
    assert_eq!(error.message(), "provider registry identity is unavailable");
    assert_eq!(
        error.to_string(),
        "provider registry identity is unavailable: chain RPC returned JSON-RPC error -32000"
    );
    assert_eq!(wallet.write_count(), 0);
}

#[tokio::test]
async fn original_revert_is_not_retried_until_a_later_explicit_invocation() {
    let (dir, _, registry, wallet) = workflow();
    wallet.mode(WalletMode::Revert);
    assert!(
        onboard(
            &declarations(),
            wallet.clone(),
            registry.clone(),
            dir.options()
        )
        .await
        .is_err()
    );
    assert_eq!(wallet.write_count(), 1);
    wallet.mode(WalletMode::Complete);
    let result = onboard(&declarations(), wallet.clone(), registry, dir.options())
        .await
        .unwrap();
    assert_eq!(
        result.progress(),
        OnboardingProgress::RecipientProofRequired
    );
    assert_eq!(wallet.write_count(), 2);
}

#[tokio::test]
async fn sec03_onboarding_presubmission_failure_allows_explicit_repair_without_stalling() {
    for mode in [
        WalletMode::PreSubmissionFailure,
        WalletMode::UnrecordedFailure,
    ] {
        onboarding_presubmission_failure_allows_repair(mode).await;
    }
}
async fn onboarding_presubmission_failure_allows_repair(mode: WalletMode) {
    let (dir, rpc, registry, wallet) = workflow();
    wallet.mode(mode);
    let failed = onboard(
        &declarations(),
        wallet.clone(),
        registry.clone(),
        dir.options(),
    )
    .await;
    assert!(
        failed.is_err(),
        "a proven unsubmitted failure must terminate pending setup"
    );
    assert!(failed.unwrap_err().message().contains("before submission"));
    assert_eq!(wallet.write_count(), 1);
    assert!(wallet.submissions.lock().unwrap().is_empty());
    let retained: Value =
        serde_json::from_slice(&std::fs::read(dir.options().state_path).unwrap()).unwrap();
    assert!(retained["pending"].is_null());
    assert!(
        rpc.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .all(|(method, _)| method != "eth_getTransactionReceipt")
    );
    let failed_ref = wallet.writes.lock().unwrap()[0].1.clone();
    wallet.mode(WalletMode::Complete);
    let repaired = onboard(&declarations(), wallet.clone(), registry, dir.options())
        .await
        .unwrap();
    assert_eq!(
        repaired.progress(),
        OnboardingProgress::RecipientProofRequired
    );
    assert_eq!(wallet.write_count(), 2);
    assert_eq!(wallet.submissions.lock().unwrap().len(), 1);
    assert_ne!(wallet.writes.lock().unwrap()[1].1, failed_ref);
}

#[tokio::test]
async fn sec03_onboarding_hashless_uncertainty_never_authorizes_replacement() {
    let (dir, rpc, registry, wallet) = workflow();
    wallet.mode(WalletMode::AttemptedUnknown);
    let first = onboard(
        &declarations(),
        wallet.clone(),
        registry.clone(),
        dir.options(),
    )
    .await
    .unwrap();
    assert_eq!(first.progress(), OnboardingProgress::Pending);
    let reference = first.operation().unwrap().operation_ref().to_owned();
    wallet.mode(WalletMode::Complete);
    for _ in 0..2 {
        let again = onboard(
            &declarations(),
            wallet.clone(),
            registry.clone(),
            dir.options(),
        )
        .await
        .unwrap();
        assert_eq!(again.progress(), OnboardingProgress::Pending);
        assert_eq!(again.operation().unwrap().operation_ref(), reference);
    }
    assert_eq!(wallet.write_count(), 1);
    assert_eq!(wallet.submissions.lock().unwrap().len(), 1);
    assert!(
        rpc.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .all(|(method, _)| method != "eth_getTransactionReceipt")
    );
}

#[tokio::test]
async fn concurrent_command_cannot_submit_duplicate_registration() {
    let (dir, _, registry, wallet) = workflow();
    wallet
        .block
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let first_wallet = wallet.clone();
    let first_registry = registry.clone();
    let options = dir.options();
    let first = tokio::spawn(async move {
        onboard(&declarations(), first_wallet, first_registry, options).await
    });
    wallet.entered.notified().await;
    assert!(
        onboard(&declarations(), wallet.clone(), registry, dir.options())
            .await
            .is_err()
    );
    assert_eq!(wallet.write_count(), 1);
    wallet.release.notify_one();
    assert_eq!(
        first.await.unwrap().unwrap().progress(),
        OnboardingProgress::RecipientProofRequired
    );
}

#[tokio::test]
async fn saved_operation_schema_intent_and_selected_configuration_are_strict() {
    for case in 0..4 {
        let (dir, _, registry, wallet) = workflow();
        wallet.mode(WalletMode::Hold);
        onboard(
            &declarations(),
            wallet.clone(),
            registry.clone(),
            dir.options(),
        )
        .await
        .unwrap();
        let mut saved: Value =
            serde_json::from_slice(&std::fs::read(dir.options().state_path).unwrap()).unwrap();
        match case {
            0 => saved["unexpected"] = true.into(),
            1 => saved["pending"]["prepared"]["calldata"] = "0x00000000".into(),
            2 => saved["pending"]["prepared"]["agent_id"] = "1".into(),
            _ => saved["declarations_hash"] = hx(B256::ZERO).into(),
        }
        std::fs::write(
            dir.options().state_path,
            serde_json::to_vec(&saved).unwrap(),
        )
        .unwrap();
        assert!(
            onboard(&declarations(), wallet.clone(), registry, dir.options())
                .await
                .is_err(),
            "case {case}"
        );
        assert_eq!(wallet.write_count(), 1);
    }
}

#[tokio::test]
async fn retained_hash_needs_current_original_wallet_attestation_before_confirmation() {
    for mode in [
        WalletMode::Lost,
        WalletMode::OmitHash,
        WalletMode::OmitHandle,
        WalletMode::AlterHash,
        WalletMode::AlterHandle,
    ] {
        let (dir, rpc, registry, wallet) = workflow();
        wallet.mode(WalletMode::Hold);
        onboard(
            &declarations(),
            wallet.clone(),
            registry.clone(),
            dir.options(),
        )
        .await
        .unwrap();
        let (original_call, reference) = wallet.writes.lock().unwrap()[0].clone();
        wallet.apply(&original_call);
        wallet.results.lock().unwrap().insert(
            reference.clone(),
            OperationResult::new(
                &reference,
                OperationState::Pending,
                Some(format!("wallet-{reference}")),
                Some(tx_hash()),
                None,
                vec![],
            )
            .unwrap(),
        );
        // A saved hash can name matching chain effects, but is not a current
        // attestation by the original wallet operation.
        let path = dir.options().state_path;
        let mut record: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        record["pending"]["result"]["tx_hash"] = hx(tx_hash()).into();
        std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        let before = std::fs::read(&path).unwrap();
        rpc.edit(|s| s.calls.clear());
        wallet.mode(mode);
        assert!(
            onboard(&declarations(), wallet.clone(), registry, dir.options())
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(path).unwrap(), before);
        assert_eq!(wallet.write_count(), 1);
        assert!(
            rpc.state
                .lock()
                .unwrap()
                .calls
                .iter()
                .all(|(method, _)| method != "eth_getTransactionReceipt"),
            "no chain receipt can clear unverified original wallet identity"
        );
    }
}

use mppi_provider::{
    config::{ProviderConfig, ProviderIdentityConfig},
    provider_update::{UpdateChanges, UpdateOptions, update},
};
fn configured() -> ProviderConfig {
    declarations()
        .with_identity(ProviderIdentityConfig::new(U256::ZERO, owner()).unwrap())
        .unwrap()
}
fn update_options(dir: &TestDir) -> UpdateOptions {
    UpdateOptions {
        state_path: dir.0.join("update.json"),
        recipient_proof: None,
    }
}
fn updating() -> (TestDir, Arc<MockRpc>, Arc<RegistryReader>, Arc<SetupWallet>) {
    let dir = TestDir::new();
    let (rpc, registry, _) = setup();
    rpc.edit(|s| {
        s.wallet = recipient();
        s.catalog = CATALOG.to_vec();
    });
    let wallet = Arc::new(SetupWallet::new(
        rpc.clone(),
        update_options(&dir).state_path,
    ));
    (dir, rpc, Arc::new(registry), wallet)
}

#[tokio::test]
async fn sec03_update_presubmission_failure_allows_explicit_repair_without_stalling() {
    for mode in [
        WalletMode::PreSubmissionFailure,
        WalletMode::UnrecordedFailure,
    ] {
        update_presubmission_failure_allows_repair(mode).await;
    }
}
async fn update_presubmission_failure_allows_repair(mode: WalletMode) {
    let (dir, rpc, registry, wallet) = updating();
    wallet.mode(mode);
    let changes = UpdateChanges::from_mapping(
        &json!({"models":{"upsert":[{"model":"example/model","rates":{"input":"999"}}]}}),
    )
    .unwrap();
    let failed = update(
        &configured(),
        &changes,
        wallet.clone(),
        registry.clone(),
        update_options(&dir),
    )
    .await;
    assert!(
        failed.is_err(),
        "a proven unsubmitted failure must terminate pending update"
    );
    assert!(failed.unwrap_err().message().contains("before submission"));
    assert_eq!(wallet.write_count(), 1);
    assert!(wallet.submissions.lock().unwrap().is_empty());
    let retained: Value =
        serde_json::from_slice(&std::fs::read(update_options(&dir).state_path).unwrap()).unwrap();
    assert!(retained["pending"].is_null());
    assert!(
        rpc.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .all(|(method, _)| method != "eth_getTransactionReceipt")
    );
    let failed_ref = wallet.writes.lock().unwrap()[0].1.clone();
    wallet.mode(WalletMode::Complete);
    let repaired = update(
        &configured(),
        &changes,
        wallet.clone(),
        registry,
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(repaired.progress(), OnboardingProgress::Complete);
    assert_eq!(
        repaired
            .config()
            .unwrap()
            .catalog()
            .rates_for("example/model")
            .unwrap()
            .input(),
        999
    );
    assert_eq!(wallet.write_count(), 2);
    assert_eq!(wallet.submissions.lock().unwrap().len(), 1);
    assert_ne!(wallet.writes.lock().unwrap()[1].1, failed_ref);
}

#[tokio::test]
async fn sec03_update_hashless_uncertainty_never_authorizes_replacement() {
    let (dir, rpc, registry, wallet) = updating();
    wallet.mode(WalletMode::AttemptedUnknown);
    let changes = UpdateChanges::from_mapping(
        &json!({"models":{"upsert":[{"model":"example/model","rates":{"input":"999"}}]}}),
    )
    .unwrap();
    let first = update(
        &configured(),
        &changes,
        wallet.clone(),
        registry.clone(),
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(first.progress(), OnboardingProgress::Pending);
    let reference = first.operation().unwrap().operation_ref().to_owned();
    wallet.mode(WalletMode::Complete);
    for _ in 0..2 {
        let again = update(
            &configured(),
            &changes,
            wallet.clone(),
            registry.clone(),
            update_options(&dir),
        )
        .await
        .unwrap();
        assert_eq!(again.progress(), OnboardingProgress::Pending);
        assert_eq!(again.operation().unwrap().operation_ref(), reference);
    }
    assert_eq!(wallet.write_count(), 1);
    assert_eq!(wallet.submissions.lock().unwrap().len(), 1);
    assert!(
        rpc.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .all(|(method, _)| method != "eth_getTransactionReceipt")
    );
}

#[test]
fn update_selection_rejects_identity_network_unknown_and_ambiguous_model_edits() {
    for value in [
        json!({"identity":{}}),
        json!({"chain":{}}),
        json!({"provider":{"wallet":"bad"}}),
        json!({"models":{},"replace_models":[]}),
        json!({"models":{"upsert":[{"model":"example/model"},{"model":"example/model"}]}}),
        json!({"models":{"upsert":[{"model":"example/model"}],"remove":["example/model"]}}),
        json!({"models":{"upsert":[{"model":"example/model","rates":{"unknown":"1"}}]}}),
    ] {
        assert!(UpdateChanges::from_mapping(&value).is_err(), "{value}");
    }
}

#[tokio::test]
async fn explicit_unchanged_update_and_completed_rerun_issue_no_registry_writes() {
    let (dir, _, registry, wallet) = updating();
    let changes =
        UpdateChanges::from_mapping(&json!({"provider":{"recipient":hx(recipient())}})).unwrap();
    let first = update(
        &configured(),
        &changes,
        wallet.clone(),
        registry.clone(),
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(first.progress(), OnboardingProgress::Complete);
    assert!(!first.submitted());
    assert_eq!(wallet.write_count(), 0);
    let saved = std::fs::read(update_options(&dir).state_path).unwrap();
    let again = update(
        first.config().unwrap(),
        &changes,
        wallet.clone(),
        registry,
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(again.progress(), OnboardingProgress::Complete);
    assert_eq!(wallet.write_count(), 0);
    assert_eq!(
        std::fs::read(update_options(&dir).state_path).unwrap(),
        saved
    );
}

#[tokio::test]
async fn partial_rate_edits_merge_latest_confirmed_unrelated_rows() {
    let (dir, rpc, registry, wallet) = updating();
    let mut value = configured().to_mapping();
    let mut second = value["models"][0].clone();
    second["model"] = "example/second".into();
    second["rates"] = json!({"input":"1","output":"2"});
    value["models"].as_array_mut().unwrap().push(second);
    let current = ProviderConfig::from_mapping(&value, None).unwrap();
    let live=br#"[{"input":"123","model":"example/model","output":"777"},{"cache":"0","input":"800","model":"example/second","output":"900"}]"#;
    rpc.edit(|s| s.catalog = live.to_vec());
    let changes = UpdateChanges::from_mapping(
        &json!({"models":{"upsert":[{"model":"example/model","rates":{"input":"999"}}]}}),
    )
    .unwrap();
    let completed = update(
        &current,
        &changes,
        wallet.clone(),
        registry,
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(completed.progress(), OnboardingProgress::Complete);
    assert_eq!(wallet.write_count(), 1);
    let catalog = completed.config().unwrap().catalog();
    let first = catalog.rates_for("example/model").unwrap();
    assert_eq!(first.input(), 999);
    assert_eq!(first.output(), 777);
    let second = catalog.rates_for("example/second").unwrap();
    assert_eq!(second.input(), 800);
    assert_eq!(second.output(), 900);
    assert_eq!(second.cache(), Some(0));
}

#[tokio::test]
async fn capabilities_and_local_settings_only_update_without_catalog_publication() {
    let (dir, rpc, registry, wallet) = updating();
    rpc.edit(|s| {
        s.catalog = br#"[{"input":"123","model":"example/model","output":"777"}]"#.to_vec()
    });
    let changes=UpdateChanges::from_mapping(&json!({"models":{"upsert":[{"model":"example/model","description":"Changed capability description"}]},"upstream":{"url":"http://localhost:9000/custom/v1"},"payment":{"output_buffer_tokens":2000}})).unwrap();
    let completed = update(
        &configured(),
        &changes,
        wallet.clone(),
        registry,
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(completed.progress(), OnboardingProgress::Complete);
    assert_eq!(wallet.write_count(), 0);
    let target = completed.config().unwrap();
    assert_eq!(
        target
            .catalog()
            .rates_for("example/model")
            .unwrap()
            .output(),
        456
    );
    assert_eq!(
        target.to_mapping()["models"][0]["description"],
        "Changed capability description"
    );
    assert_eq!(target.upstream().url(), "http://localhost:9000/custom/v1");
    assert_eq!(target.payment().output_buffer_tokens(), 2000);
}

#[tokio::test]
async fn missing_capabilities_and_unserializable_updates_fail_before_wallet_write() {
    for case in 0..2 {
        let (dir, rpc, registry, wallet) = updating();
        if case == 0 {
            rpc.edit(|s| {
                s.catalog = br#"[{"input":"123","model":"example/new","output":"456"}]"#.to_vec()
            });
        }
        let change = if case == 0 {
            json!({"models":{"upsert":[{"model":"example/model","rates":{"input":"3"}}]}})
        } else {
            json!({"provider":{"services":[{"name":"extension","endpoint":"https://example.com","cannot_encode":null}]}})
        };
        let changes = UpdateChanges::from_mapping(&change).unwrap();
        assert!(
            update(
                &configured(),
                &changes,
                wallet.clone(),
                registry,
                update_options(&dir)
            )
            .await
            .is_err()
        );
        assert_eq!(wallet.write_count(), 0);
    }
}

#[tokio::test]
async fn pending_update_keeps_original_reference_without_duplicate_submission() {
    let (dir, _, registry, wallet) = updating();
    wallet.mode(WalletMode::Hold);
    let changes = UpdateChanges::from_mapping(
        &json!({"models":{"upsert":[{"model":"example/model","rates":{"input":"999"}}]}}),
    )
    .unwrap();
    let pending = update(
        &configured(),
        &changes,
        wallet.clone(),
        registry.clone(),
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(pending.progress(), OnboardingProgress::Pending);
    assert_eq!(wallet.write_count(), 1);
    let original = pending.operation().unwrap().operation_ref().to_owned();
    let again = update(
        &configured(),
        &changes,
        wallet.clone(),
        registry.clone(),
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(again.operation().unwrap().operation_ref(), original);
    assert_eq!(wallet.write_count(), 1);
    let changed =
        UpdateChanges::from_mapping(&json!({"provider":{"description":"other"}})).unwrap();
    assert!(
        update(
            &configured(),
            &changed,
            wallet.clone(),
            registry,
            update_options(&dir)
        )
        .await
        .is_err()
    );
    assert_eq!(wallet.write_count(), 1);
}

#[tokio::test]
async fn publication_conflict_while_recipient_proof_pending_prevents_all_writes() {
    let (dir, rpc, registry, wallet) = updating();
    let changes=UpdateChanges::from_mapping(&json!({"provider":{"recipient":hx(Address::repeat_byte(0x33))},"models":{"upsert":[{"model":"example/model","rates":{"input":"999"}}]}})).unwrap();
    let first = update(
        &configured(),
        &changes,
        wallet.clone(),
        registry.clone(),
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(first.progress(), OnboardingProgress::RecipientProofRequired);
    rpc.edit(|s| {
        s.catalog = br#"[{"input":"555","model":"example/model","output":"456"}]"#.to_vec()
    });
    let mut options = update_options(&dir);
    options.recipient_proof = Some(
        SignedRecipientProof::new(first.recipient_proof().unwrap().clone(), vec![1; 65]).unwrap(),
    );
    assert!(
        update(&configured(), &changes, wallet.clone(), registry, options)
            .await
            .is_err()
    );
    assert_eq!(wallet.write_count(), 0);
}

#[tokio::test]
async fn recipient_then_uri_then_catalog_use_shared_confirmed_update_engine() {
    let (dir, _, registry, wallet) = updating();
    let new_recipient = Address::repeat_byte(0x33);
    let changes=UpdateChanges::from_mapping(&json!({"provider":{"recipient":hx(new_recipient),"public_origin":"https://next.example"},"models":{"upsert":[{"model":"example/model","rates":{"input":"999"}}]}})).unwrap();
    let first = update(
        &configured(),
        &changes,
        wallet.clone(),
        registry.clone(),
        update_options(&dir),
    )
    .await
    .unwrap();
    let mut options = update_options(&dir);
    options.recipient_proof = Some(
        SignedRecipientProof::new(first.recipient_proof().unwrap().clone(), vec![1; 65]).unwrap(),
    );
    let done = update(&configured(), &changes, wallet.clone(), registry, options)
        .await
        .unwrap();
    assert_eq!(done.progress(), OnboardingProgress::Complete);
    assert_eq!(wallet.write_count(), 3);
    let writes = wallet.writes.lock().unwrap();
    for ((call, _), signature) in writes.iter().zip([
        "setAgentWallet(uint256,address,uint256,bytes)",
        "setAgentURI(uint256,string)",
        "setMetadata(uint256,string,bytes)",
    ]) {
        assert_eq!(call.data()[..4], keccak256(signature)[..4]);
    }
    assert_eq!(done.config().unwrap().provider().recipient(), new_recipient);
    assert_eq!(
        done.config().unwrap().provider().public_origin(),
        "https://next.example"
    );
}

#[tokio::test]
async fn new_explicit_update_starts_after_completed_task_and_target_tampering_rejects() {
    let (dir, _, registry, wallet) = updating();
    let first = UpdateChanges::from_mapping(&json!({"provider":{"description":"First"}})).unwrap();
    let completed = update(
        &configured(),
        &first,
        wallet.clone(),
        registry.clone(),
        update_options(&dir),
    )
    .await
    .unwrap();
    let second =
        UpdateChanges::from_mapping(&json!({"provider":{"description":"Second"}})).unwrap();
    let completed = update(
        completed.config().unwrap(),
        &second,
        wallet.clone(),
        registry.clone(),
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(
        completed.config().unwrap().provider().description(),
        "Second"
    );
    assert_eq!(wallet.write_count(), 0);
    let path = update_options(&dir).state_path;
    let mut record: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    record["desired"]["provider"]["description"] = "Tampered".into();
    std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
    assert!(
        update(
            completed.config().unwrap(),
            &second,
            wallet.clone(),
            registry,
            update_options(&dir)
        )
        .await
        .is_err()
    );
    assert_eq!(wallet.write_count(), 0);
}

#[tokio::test]
async fn pure_local_update_preserves_unselected_local_prices_and_provider_declarations() {
    let (dir, rpc, registry, wallet) = updating();
    rpc.edit(|s| {
        s.catalog = br#"[{"input":"800","model":"example/model","output":"777"}]"#.to_vec()
    });
    let current = configured();
    let changes =
        UpdateChanges::from_mapping(&json!({"upstream":{"url":"http://localhost:7777/v1"}}))
            .unwrap();
    let complete = update(
        &current,
        &changes,
        wallet.clone(),
        registry,
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(complete.progress(), OnboardingProgress::Complete);
    assert_eq!(wallet.write_count(), 0);
    let target = complete.config().unwrap();
    assert_eq!(target.catalog(), current.catalog());
    assert_eq!(target.provider(), current.provider());
    assert_eq!(target.upstream().url(), "http://localhost:7777/v1");
}

#[tokio::test]
async fn expired_imported_recipient_proof_cannot_submit_but_bound_recipient_needs_no_refresh() {
    let (dir, rpc, registry, wallet) = workflow();
    let first = onboard(
        &declarations(),
        wallet.clone(),
        registry.clone(),
        dir.options(),
    )
    .await
    .unwrap();
    let mut options = dir.options();
    options.recipient_proof = Some(
        SignedRecipientProof::new(first.recipient_proof().unwrap().clone(), vec![1; 65]).unwrap(),
    );
    rpc.edit(|s| s.timestamp = 1301);
    assert!(
        onboard(&declarations(), wallet.clone(), registry.clone(), options)
            .await
            .is_err()
    );
    assert_eq!(wallet.write_count(), 1);
    rpc.edit(|s| s.wallet = recipient());
    let complete = onboard(&declarations(), wallet.clone(), registry, dir.options())
        .await
        .unwrap();
    assert_eq!(complete.progress(), OnboardingProgress::Complete);
    assert_eq!(wallet.write_count(), 3);
}

#[tokio::test]
async fn empty_rate_selection_does_not_refresh_unselected_rates() {
    let (dir, rpc, registry, wallet) = updating();
    rpc.edit(|s| {
        s.catalog = br#"[{"input":"800","model":"example/model","output":"777"}]"#.to_vec()
    });
    let current = configured();
    let changes = UpdateChanges::from_mapping(
        &json!({"models":{"upsert":[{"model":"example/model","rates":{}}]}}),
    )
    .unwrap();
    let complete = update(
        &current,
        &changes,
        wallet.clone(),
        registry,
        update_options(&dir),
    )
    .await
    .unwrap();
    assert_eq!(complete.config().unwrap().catalog(), current.catalog());
    assert_eq!(wallet.write_count(), 0);
}
