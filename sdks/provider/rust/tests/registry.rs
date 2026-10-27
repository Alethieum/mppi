//! Read-only registry evidence against independently encoded ABI words/tails.
//! Sources read 2026-09-28: ERC-8004, Identity Registry and On-chain metadata
//! (https://eips.ethereum.org/EIPS/eip-8004#on-chain-metadata), and Solidity ABI,
//! Function Selector / Formal Specification of the Encoding
//! (https://docs.soliditylang.org/en/latest/abi-spec.html).
//! No registry call bindings are reused to construct expected calldata or replies.

use alloy_primitives::{hex, keccak256};
use mppi_provider::{
    Address, B256, DEFAULT_CATALOG_BYTES, U256,
    chain::{Block, ChainError, ChainReader, CodePin, DeploymentProfile, Rpc},
    discovery::MAX_DOCUMENT_BYTES,
    types::{ChainObservation, Domain},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const CHAIN: u64 = 5042002;
const OWNER: &str = "ownerOf(uint256)";
const WALLET: &str = "getAgentWallet(uint256)";
const URI: &str = "tokenURI(uint256)";
const METADATA: &str = "getMetadata(uint256,string)";
const AGENT_URI: &str = "https://Provider.EXAMPLE:443/discover";
const CATALOG: &[u8] = br#"[{"cache":"0","input":"123","model":"example/model","output":"456"}]"#;

fn owner() -> Address {
    Address::repeat_byte(0x11)
}
fn wallet() -> Address {
    Address::repeat_byte(0x22)
}
fn proxy() -> Address {
    Address::repeat_byte(0x99)
}
fn registry() -> Address {
    Address::repeat_byte(0x97)
}
fn block_hash() -> B256 {
    B256::repeat_byte(0xbb)
}
fn hx(value: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(value))
}
fn word(value: usize) -> Vec<u8> {
    let mut result = vec![0; 32];
    result[24..].copy_from_slice(&(value as u64).to_be_bytes());
    result
}
fn address_word(address: Address) -> Vec<u8> {
    let mut result = vec![0; 12];
    result.extend_from_slice(address.as_slice());
    result
}
fn tail(value: &[u8]) -> Vec<u8> {
    let mut result = word(value.len());
    result.extend_from_slice(value);
    result.resize(32 + value.len().div_ceil(32) * 32, 0);
    result
}
fn dynamic(value: &[u8]) -> Vec<u8> {
    [word(32), tail(value)].concat()
}
fn calldata(signature: &str, id: U256) -> Vec<u8> {
    let mut result = keccak256(signature)[..4].to_vec();
    result.extend_from_slice(&id.to_be_bytes::<32>());
    if signature == METADATA {
        result.extend(word(64));
        result.extend(tail(b"mppi.catalog"));
    }
    result
}

struct RegistryRpc {
    expected_id: U256,
    replies: Mutex<HashMap<&'static str, Value>>,
    failure: Mutex<Option<&'static str>>,
    chain: Mutex<Value>,
    block: Mutex<Value>,
    calls: Mutex<Vec<(String, Value)>>,
}
impl RegistryRpc {
    fn new(expected_id: U256) -> Self {
        Self {
            expected_id,
            replies: Mutex::new(HashMap::from([
                (OWNER, json!(hx(address_word(owner())))),
                (WALLET, json!(hx(address_word(wallet())))),
                (URI, json!(hx(dynamic(AGENT_URI.as_bytes())))),
                (METADATA, json!(hx(dynamic(CATALOG)))),
            ])),
            failure: Mutex::new(None),
            chain: Mutex::new(json!(format!("0x{CHAIN:x}"))),
            block: Mutex::new(json!({"number":"0x64","hash":hx(block_hash()),
                "parentHash":hx(B256::repeat_byte(0xbc)),"timestamp":"0x6ab34d78"})),
            calls: Mutex::new(Vec::new()),
        }
    }
    fn reply(&self, signature: &'static str, raw: Vec<u8>) {
        self.replies
            .lock()
            .unwrap()
            .insert(signature, json!(hx(raw)));
    }
    fn profile() -> DeploymentProfile {
        let pin = |byte| CodePin::new(Address::repeat_byte(byte), keccak256([0x60, 0x00])).unwrap();
        DeploymentProfile::new(
            Domain::new(CHAIN, proxy()).unwrap(),
            registry(),
            pin(0x99),
            pin(0x98),
            pin(0x97),
            pin(0x96),
        )
        .unwrap()
    }
    fn call_data(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(method, _)| method == "eth_call")
            .map(|(_, params)| params[0]["data"].as_str().unwrap().to_owned())
            .collect()
    }
}
#[tonic::async_trait]
impl Rpc for RegistryRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        self.calls
            .lock()
            .unwrap()
            .push((method.to_owned(), params.clone()));
        if *self.failure.lock().unwrap() == Some(method) {
            return Err(ChainError::Unavailable);
        }
        match method {
            "eth_chainId" => {
                assert_eq!(params, json!([]));
                Ok(self.chain.lock().unwrap().clone())
            }
            "eth_getBlockByNumber" => {
                assert!(params == json!(["latest", false]) || params == json!(["0x64", false]));
                Ok(self.block.lock().unwrap().clone())
            }
            "eth_call" => {
                assert_eq!(params[0]["to"], hx(registry()));
                assert_eq!(params[0].as_object().unwrap().len(), 2);
                assert_eq!(
                    params[1],
                    json!({"blockHash":hx(block_hash()),"requireCanonical":true})
                );
                let raw = params[0]["data"].as_str().unwrap();
                let signature = [OWNER, WALLET, URI, METADATA]
                    .into_iter()
                    .find(|signature| raw == hx(calldata(signature, self.expected_id)))
                    .expect(
                        "registry call must use exact signature, full uint256 id, and metadata key",
                    );
                if *self.failure.lock().unwrap() == Some(signature) {
                    return Err(ChainError::Unavailable);
                }
                Ok(self.replies.lock().unwrap()[signature].clone())
            }
            _ => panic!("unexpected RPC method {method}"),
        }
    }
}
async fn setup(id: U256) -> (Arc<RegistryRpc>, ChainReader, Block) {
    let rpc = Arc::new(RegistryRpc::new(id));
    let reader = ChainReader::new(rpc.clone(), RegistryRpc::profile());
    let block = reader.block(None).await.unwrap();
    (rpc, reader, block)
}

#[tokio::test]
async fn identity_binds_distinct_owner_and_recipient_exact_uri_catalog_and_full_uint256() {
    for id in [U256::ZERO, U256::MAX] {
        let (rpc, reader, block) = setup(id).await;
        let identity = reader.identity(id, &block, false).await.unwrap();
        assert_eq!(identity.provider_id(), id);
        assert_eq!(identity.registry_address(), registry());
        assert_eq!(identity.observation(), block.observation());
        assert_eq!(identity.owner(), owner());
        assert_eq!(identity.recipient(), wallet());
        assert_ne!(identity.owner(), identity.recipient());
        assert_eq!(identity.agent_uri(), AGENT_URI);
        let catalog = identity.catalog().unwrap();
        assert_eq!(catalog.canonical_bytes(), CATALOG);
        assert_eq!(catalog.rate_card_hash(), keccak256(CATALOG));
        assert_eq!(catalog.rows()[0].rates().cache(), Some(0));
        assert_eq!(
            rpc.call_data(),
            [OWNER, WALLET, URI, METADATA].map(|name| hx(calldata(name, id)))
        );
        let calls = rpc.calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|(method, _)| method == "eth_getBlockByNumber")
                .count(),
            2
        );
        assert_eq!(
            calls.last().unwrap(),
            &("eth_getBlockByNumber".into(), json!(["0x64", false]))
        );
    }
}

#[tokio::test]
async fn catalog_tolerance_preserves_identity_but_never_repairs_invalid_offerings() {
    for raw in [
        Vec::new(),
        b"[]".to_vec(),
        [b" ".as_slice(), CATALOG].concat(),
        br#"[{"input":"0","model":"example/model","output":0}]"#.to_vec(),
        br#"[{"input":"0","input":"0","model":"example/model","output":"0"}]"#.to_vec(),
        vec![b' '; DEFAULT_CATALOG_BYTES + 1],
    ] {
        let (rpc, reader, block) = setup(U256::MAX).await;
        rpc.reply(METADATA, dynamic(&raw));
        assert!(reader.identity(U256::MAX, &block, false).await.is_err());
        let identity = reader.identity(U256::MAX, &block, true).await.unwrap();
        assert!(identity.catalog().is_none());
        assert_eq!(identity.provider_id(), U256::MAX);
        assert_eq!(identity.owner(), owner());
        assert_eq!(identity.recipient(), wallet());
        assert_eq!(identity.agent_uri(), AGENT_URI);
        assert_eq!(identity.observation(), block.observation());
    }
}

#[tokio::test]
async fn cleared_recipient_or_missing_owner_reject_even_when_catalog_is_unavailable() {
    for signature in [OWNER, WALLET] {
        for allow in [false, true] {
            let (rpc, reader, block) = setup(U256::MAX).await;
            rpc.reply(signature, address_word(Address::ZERO));
            assert!(reader.identity(U256::MAX, &block, allow).await.is_err());
            rpc.reply(METADATA, dynamic(b""));
            assert!(reader.identity(U256::MAX, &block, allow).await.is_err());
        }
    }
}

#[tokio::test]
async fn catalog_tolerance_does_not_mask_rpc_failures_or_invalid_rpc_scalar_types() {
    for signature in [OWNER, WALLET, URI, METADATA] {
        let (rpc, reader, block) = setup(U256::MAX).await;
        *rpc.failure.lock().unwrap() = Some(signature);
        assert_eq!(
            reader.identity(U256::MAX, &block, true).await.unwrap_err(),
            ChainError::Unavailable
        );
        *rpc.failure.lock().unwrap() = None;
        for invalid in [
            Value::Null,
            json!(0),
            json!("6000"),
            json!("0x0"),
            json!("0xgg"),
        ] {
            rpc.replies.lock().unwrap().insert(signature, invalid);
            assert!(
                reader.identity(U256::MAX, &block, true).await.is_err(),
                "accepted bad response for {signature}"
            );
        }
    }
}

#[tokio::test]
async fn malformed_and_noncanonical_abi_is_not_an_unavailable_catalog() {
    let mut address_padding = address_word(owner());
    address_padding[0] = 1;
    let mut string_padding = dynamic(AGENT_URI.as_bytes());
    *string_padding.last_mut().unwrap() = 1;
    let mut metadata_padding = dynamic(b"[]");
    *metadata_padding.last_mut().unwrap() = 1;
    let mut displaced_metadata = word(64);
    displaced_metadata.extend(word(0));
    displaced_metadata.extend(tail(CATALOG));
    let malformed = [
        (OWNER, address_padding),
        (OWNER, [address_word(owner()), word(0)].concat()),
        (WALLET, vec![0; 31]),
        (URI, string_padding),
        (URI, dynamic(&[0xff])),
        (URI, [word(32), vec![0xff; 32]].concat()),
        (METADATA, metadata_padding),
        (METADATA, displaced_metadata),
        (METADATA, [dynamic(CATALOG), word(0)].concat()),
        (METADATA, word(32)),
    ];
    for (signature, raw) in malformed {
        let (rpc, reader, block) = setup(U256::MAX).await;
        rpc.reply(signature, raw);
        assert!(
            reader.identity(U256::MAX, &block, true).await.is_err(),
            "accepted malformed ABI for {signature}"
        );
    }
}

#[tokio::test]
async fn invalid_uri_and_uri_size_are_never_masked_by_catalog_tolerance() {
    let mut uris = [
        "",
        "http://provider.example/discover",
        "ipfs://cid",
        "https://provider.example/other",
        "https://provider.example//discover",
        "https://provider.example/discover?x=1",
        "https://user@provider.example/discover",
    ]
    .map(str::to_owned)
    .to_vec();
    uris.push("x".repeat(MAX_DOCUMENT_BYTES + 1));
    for uri in uris {
        let (rpc, reader, block) = setup(U256::MAX).await;
        rpc.reply(URI, dynamic(uri.as_bytes()));
        rpc.reply(METADATA, dynamic(b""));
        assert!(
            reader.identity(U256::MAX, &block, true).await.is_err(),
            "accepted invalid URI"
        );
    }
}

#[tokio::test]
async fn identity_and_agent_wallet_recheck_chain_and_complete_canonical_block() {
    for (field, value) in [
        ("hash", json!(hx(B256::repeat_byte(0xee)))),
        ("parentHash", json!(hx(B256::repeat_byte(0xdd)))),
        ("timestamp", json!("0x6ab34d79")),
        ("number", json!("0x65")),
    ] {
        let (rpc, reader, block) = setup(U256::MAX).await;
        rpc.reply(METADATA, dynamic(b""));
        rpc.block.lock().unwrap()[field] = value;
        assert!(
            reader.identity(U256::MAX, &block, true).await.is_err(),
            "accepted changed {field}"
        );
        assert!(
            reader.agent_wallet(U256::MAX, &block).await.is_err(),
            "accepted changed {field}"
        );
    }
    // A reader checks the RPC's chain id with its first read, then never again.
    for method in ["eth_chainId", "eth_getBlockByNumber"] {
        let (rpc, _, block) = setup(U256::MAX).await;
        rpc.reply(METADATA, dynamic(b""));
        *rpc.failure.lock().unwrap() = Some(method);
        for _ in 0..2 {
            let reader = ChainReader::new(rpc.clone(), RegistryRpc::profile());
            assert_eq!(
                reader.identity(U256::MAX, &block, true).await.unwrap_err(),
                ChainError::Unavailable
            );
            assert_eq!(
                reader.agent_wallet(U256::MAX, &block).await.unwrap_err(),
                ChainError::Unavailable
            );
        }
    }
    let (rpc, reader, block) = setup(U256::MAX).await;
    rpc.reply(METADATA, dynamic(b""));
    *rpc.chain.lock().unwrap() = json!("0x1");
    reader.identity(U256::MAX, &block, true).await.unwrap();
    reader.agent_wallet(U256::MAX, &block).await.unwrap();
    let fresh = ChainReader::new(rpc.clone(), RegistryRpc::profile());
    assert!(fresh.identity(U256::MAX, &block, true).await.is_err());
    assert!(fresh.agent_wallet(U256::MAX, &block).await.is_err());
    let chain_reads = rpc
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(m, _)| m == "eth_chainId")
        .count();
    assert_eq!(chain_reads, 3);
}

#[tokio::test]
async fn foreign_block_domain_rejects_before_reading_registry() {
    for observation in [
        ChainObservation::new(5042, proxy(), 100, block_hash()).unwrap(),
        ChainObservation::new(CHAIN, Address::repeat_byte(0x55), 100, block_hash()).unwrap(),
    ] {
        let (rpc, reader, block) = setup(U256::MAX).await;
        let foreign = Block::new(observation, block.timestamp(), block.parent_hash());
        assert!(reader.identity(U256::MAX, &foreign, true).await.is_err());
        assert!(reader.agent_wallet(U256::MAX, &foreign).await.is_err());
        assert!(rpc.call_data().is_empty());
    }
}

#[tokio::test]
async fn agent_wallet_returns_funding_identity_independent_of_session_signer() {
    let (rpc, reader, block) = setup(U256::MAX).await;
    let funding_wallet = Address::repeat_byte(0x51);
    let session_signer = Address::repeat_byte(0x52);
    rpc.reply(WALLET, address_word(funding_wallet));
    let observed = reader.agent_wallet(U256::MAX, &block).await.unwrap();
    assert_eq!(observed, funding_wallet);
    assert_ne!(observed, session_signer);
    assert_eq!(rpc.call_data(), [hx(calldata(WALLET, U256::MAX))]);
    rpc.reply(WALLET, address_word(Address::ZERO));
    assert!(reader.agent_wallet(U256::MAX, &block).await.is_err());
    rpc.reply(WALLET, vec![0; 31]);
    assert!(reader.agent_wallet(U256::MAX, &block).await.is_err());
    *rpc.failure.lock().unwrap() = Some(WALLET);
    assert_eq!(
        reader.agent_wallet(U256::MAX, &block).await.unwrap_err(),
        ChainError::Unavailable
    );
}
