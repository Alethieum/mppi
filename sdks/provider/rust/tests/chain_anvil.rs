//! Called only by tests/test_rust_chain_anvil.py inside the owned Linux fixture.
//! The fixture provides public call inputs and mined hashes, never signing keys.
use alloy_primitives::{Address, B256, U256, hex};
use mppi_provider::{
    capabilities::FundingAuthorization,
    chain::{ChainError, ChainReader},
    contract::{ARC_USDC, ContractCalls, PreparedOperation},
    deployment::profile_from_mapping,
    follower::{ChainFollower, Clock},
    http::{HttpJsonRpc, HttpTransport, MAX_RPC_RESPONSE_BYTES},
    parse_catalog,
    signing::SessionDescriptor,
    types::{OperationState, SessionStatus},
};
use serde_json::{Value, json};
use std::{
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

struct AnvilClock {
    start: Instant,
    epoch: u64,
}
impl Clock for AnvilClock {
    fn monotonic(&self) -> Duration {
        self.start.elapsed()
    }
    fn wall(&self) -> Result<Duration, ChainError> {
        Ok(Duration::from_secs(self.epoch) + self.start.elapsed())
    }
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap()
}
fn address(value: &Value, key: &str) -> Address {
    text(value, key).parse().unwrap()
}
fn hash(value: &Value, key: &str) -> B256 {
    text(value, key).parse().unwrap()
}
fn bytes(value: &Value, key: &str) -> Vec<u8> {
    hex::decode(text(value, key).strip_prefix("0x").unwrap()).unwrap()
}
fn number(value: &Value, key: &str) -> u64 {
    value[key].as_u64().unwrap()
}
fn hx(value: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(value))
}
fn authorization(value: &Value, owner: Address, recipient: Address) -> FundingAuthorization {
    FundingAuthorization::new(
        5042002,
        ARC_USDC,
        "USDC".into(),
        "2".into(),
        owner,
        recipient,
        U256::from(number(value, "amount")),
        U256::from(number(value, "valid_after")),
        U256::from(number(value, "valid_before")),
        hash(value, "nonce"),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires the owned Anvil fixture from tests/test_rust_chain_anvil.py"]
async fn owned_anvil_interoperability() {
    const {
        assert!(
            cfg!(target_os = "linux"),
            "run inside the Apple Linux container"
        )
    };
    let input = std::env::var("MPPI_RUST_ANVIL_INPUT").unwrap();
    let output = std::env::var("MPPI_RUST_ANVIL_OUTPUT").unwrap();
    let input: Value = serde_json::from_slice(&fs::read(input).unwrap()).unwrap();
    let profile = profile_from_mapping(&input["profile"]).unwrap();
    assert_eq!(profile.domain().chain_id(), 5042002);
    let raw = &input["descriptor"];
    let descriptor = SessionDescriptor {
        agent: address(raw, "agent"),
        sessionSigner: address(raw, "session_signer"),
        providerId: U256::from(number(raw, "provider_id")),
        recipient: address(raw, "recipient"),
        operator: address(raw, "operator"),
        token: ARC_USDC,
        salt: hash(raw, "salt"),
    };
    let identifier = profile.domain().signing_context().session_id(&descriptor);
    assert_eq!(identifier, hash(&input, "session_id"));
    let catalog = parse_catalog(&bytes(&input, "catalog"), 1 << 20).unwrap();
    let calls = ContractCalls::new(*profile.domain(), "USDC".into(), "2".into()).unwrap();
    let open_auth = authorization(
        &input["open_auth"],
        descriptor.agent,
        profile.domain().verifying_contract(),
    );
    let top_up_auth = authorization(
        &input["top_up_auth"],
        descriptor.agent,
        profile.domain().verifying_contract(),
    );
    let voucher = bytes(&input, "voucher");
    let operations: [(&str, PreparedOperation); 4] = [
        (
            "open",
            calls
                .open(
                    &descriptor,
                    5510000,
                    catalog.rate_card_hash(),
                    &open_auth,
                    &bytes(&input["open_auth"], "signature"),
                )
                .unwrap(),
        ),
        (
            "top_up",
            calls
                .top_up(
                    &descriptor,
                    490000,
                    &top_up_auth,
                    &bytes(&input["top_up_auth"], "signature"),
                )
                .unwrap(),
        ),
        (
            "settle",
            calls
                .settle(&descriptor, 5510000, 5420000, &voucher)
                .unwrap(),
        ),
        (
            "close",
            calls
                .close(&descriptor, 5510000, 5420000, &voucher)
                .unwrap(),
        ),
    ];
    let endpoint = text(&input, "rpc");
    assert!(endpoint.starts_with("http://127.0.0.1:"));
    let rpc = HttpJsonRpc::new(
        endpoint,
        HttpTransport::new(5000, MAX_RPC_RESPONSE_BYTES, None).unwrap(),
    )
    .unwrap();
    let reader = ChainReader::new(Arc::new(rpc), profile);
    reader
        .verify_deployment(&reader.block(None).await.unwrap())
        .await
        .unwrap();
    let result = if text(&input, "phase") == "prepare" {
        let values=operations.iter().map(|(kind,operation)|(kind.to_string(),json!({"chain_id":operation.call().chain_id(),"to":hx(operation.call().to()),"data":hx(operation.call().data()),"session_id":hx(operation.session_ids()[0])}))).collect::<serde_json::Map<_,_>>();
        Value::Object(values)
    } else {
        let name = text(&input, "operation");
        let (_, operation) = operations.iter().find(|(kind, _)| *kind == name).unwrap();
        let transaction = hash(&input, "tx_hash");
        let repeated = input["repeated"].as_bool().unwrap_or(false);
        let result = reader
            .reconcile(
                operation,
                "rust-owned-anvil",
                Some(transaction),
                Some("original-wallet-operation"),
            )
            .await
            .unwrap();
        assert_eq!(
            result.state(),
            if repeated {
                OperationState::Unknown
            } else {
                OperationState::Confirmed
            }
        );
        let observation = result.observation().unwrap();
        assert_eq!(observation.block_number(), number(&input, "block_number"));
        assert_eq!(observation.block_hash(), hash(&input, "block_hash"));
        let state = reader
            .session(identifier, &reader.block(None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            state.deposit(),
            if name == "open" { 5510000 } else { 6000000 }
        );
        assert_eq!(
            state.settled_cumulative(),
            if matches!(name, "settle" | "close") {
                5420000
            } else {
                0
            }
        );
        assert_eq!(
            state.status(),
            if name == "close" {
                SessionStatus::Closed
            } else {
                SessionStatus::Open
            }
        );
        assert_eq!(state.initial_rate_card_hash(), catalog.rate_card_hash());
        let clock = Arc::new(AnvilClock {
            start: Instant::now(),
            epoch: reader.block(None).await.unwrap().timestamp(),
        });
        let follower =
            ChainFollower::with_clock(reader.clone(), 10_000, 1_000, None, clock).unwrap();
        follower.start().await.unwrap();
        assert_eq!(follower.watch_session(identifier).await.unwrap(), state);
        assert_eq!(
            follower.readiness(Some(identifier)).ready(),
            name != "close"
        );
        follower.stop().await.unwrap();
        assert!(!follower.readiness(Some(identifier)).ready());
        let settlement = result.sessions()[0].settlement_reference();
        if name == "settle" {
            assert_eq!(result.sessions()[0].target_covered(), Some(true));
            if repeated {
                assert!(settlement.is_none());
            } else {
                let settlement = settlement.unwrap();
                assert_eq!(settlement.delta(), 5420000);
                assert_eq!(settlement.settled_cumulative(), 5420000);
                reader
                    .verify_settlement(&settlement, &descriptor)
                    .await
                    .unwrap();
            }
        }
        if name == "open" {
            let opening = reader
                .opening(transaction, identifier, Some(&catalog))
                .await
                .unwrap();
            assert_eq!(opening.descriptor(), &descriptor);
            assert_eq!(opening.deposit(), 5510000);
        }
        if name == "close" {
            let closing = reader.closing(transaction, identifier).await.unwrap();
            assert_eq!(closing.capture_amount(), 5420000);
            assert_eq!(closing.refunded(), 580000);
        }
        json!({"state":result.state().as_str(),"deposit":state.deposit(),"settled_cumulative":state.settled_cumulative(),"last_activity":state.last_activity(),"settlement_delta":settlement.map(|s|s.delta()),"target_covered":result.sessions()[0].target_covered()})
    };
    fs::write(output, serde_json::to_vec(&result).unwrap()).unwrap();
}
