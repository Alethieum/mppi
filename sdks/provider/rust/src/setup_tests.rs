//! Synthetic account and RPC evidence: no external credentials or network.
use super::*;
use crate::{
    Address, TokenCounts,
    chain::{ChainError, CodePin, DeploymentProfile, Rpc, encoded},
    circle::{CircleCommand, CircleError, CommandOutput},
    config::ProviderConfig,
    interfaces::{
        ExecutionStream, IntegrationResult, PreparedRequest, ProviderLifecycle, ProviderRuns,
    },
    proto,
    signing::Voucher,
    types::{Domain, RunContext},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

const CHAIN: u64 = 5_042_002;
fn operator() -> Address {
    Address::repeat_byte(0x11)
}
fn profile(chain: u64) -> DeploymentProfile {
    let pin = |byte| CodePin::new(Address::repeat_byte(byte), keccak256([0x60, 0x00])).unwrap();
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
fn config(chain: u64) -> ProviderConfig {
    let raw = include_str!("provider-declarations.toml")
        .replace("REPLACE_HTTPS_PROVIDER_ORIGIN", "https://provider.example")
        .replace(
            "REPLACE_RECIPIENT_EVM_ADDRESS",
            "0x2222222222222222222222222222222222222222",
        )
        .replace("REPLACE_MODEL_ID", "provider/model")
        .replace("REPLACE_INPUT_RATE", "300000")
        .replace("REPLACE_OUTPUT_RATE", "1200000");
    let toml: toml::Value = toml::from_str(&raw).unwrap();
    let mut mapping = serde_json::to_value(toml).unwrap();
    mapping["chain"]["network"] = json!(if chain == CHAIN { "testnet" } else { "mainnet" });
    mapping["identity"] = json!({"provider_id":"42","wallet_address":encoded(operator())});
    ProviderConfig::from_mapping(&mapping, None).unwrap()
}

#[derive(Default)]
struct TestRpc {
    chain: u64,
    altered: Option<&'static str>,
    calls: Mutex<Vec<(String, Value)>>,
}
#[tonic::async_trait]
impl Rpc for TestRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        let mut calls = self.calls.lock().unwrap();
        calls.push((method.into(), params.clone()));
        match method {
            "eth_chainId" => Ok(json!(format!(
                "0x{:x}",
                if self.altered == Some("network") {
                    if self.chain == CHAIN { 5042 } else { CHAIN }
                } else {
                    self.chain
                }
            ))),
            "eth_getBlockByNumber" => {
                let changed = self.altered == Some("canonical")
                    && calls.iter().any(|(_, value)| {
                        value[0]["data"] == encoded(ArcUsdc::DOMAIN_SEPARATORCall {}.abi_encode())
                    });
                Ok(
                    json!({"number":"0x1", "hash":encoded(B256::repeat_byte(if changed {9} else {7})),
                    "parentHash":encoded(B256::repeat_byte(6)),"timestamp":"0x6b49d200"}),
                )
            }
            "eth_getCode" => Ok(json!(if self.altered == Some("code") {
                "0x6001"
            } else {
                "0x6000"
            })),
            "eth_getStorageAt" => {
                let address = if params[0] == encoded(Address::repeat_byte(1)) {
                    2
                } else if params[0] == encoded(operator()) {
                    0x55
                } else {
                    4
                };
                Ok(json!(encoded(Address::repeat_byte(address).into_word())))
            }
            "eth_call" => {
                assert_eq!(
                    params[1],
                    json!({"blockHash":encoded(B256::repeat_byte(7)),"requireCanonical":true})
                );
                let data = params[0]["data"].as_str().unwrap();
                if params[0]["to"] == encoded(profile(self.chain).domain().verifying_contract())
                    && data.starts_with(&encoded(
                        crate::contract::abi::IMPPI::voucherDigestCall::SELECTOR,
                    ))
                {
                    return Ok(json!(encoded(
                        profile(self.chain)
                            .domain()
                            .signing_context()
                            .digest(&Voucher {
                                sessionId: B256::ZERO,
                                cumulativeAmount: 0,
                            })
                    )));
                }
                if params[0]["to"] != encoded(ARC_USDC) {
                    // Setup never reads provider registry or starts chain following.
                    return Err(ChainError::Unavailable);
                }
                let raw = if data == encoded(ArcUsdc::nameCall {}.abi_encode()) {
                    let value = if self.altered == Some("name") {
                        "USD Coin"
                    } else {
                        "USDC"
                    };
                    let mut bytes = ArcUsdc::nameCall::abi_encode_returns(&value.to_owned());
                    if self.altered == Some("surplus") {
                        bytes.extend([0; 32]);
                    }
                    bytes
                } else if data == encoded(ArcUsdc::versionCall {}.abi_encode()) {
                    ArcUsdc::versionCall::abi_encode_returns(
                        &if self.altered == Some("version") {
                            "3"
                        } else {
                            "2"
                        }
                        .to_owned(),
                    )
                } else if data == encoded(ArcUsdc::decimalsCall {}.abi_encode()) {
                    ArcUsdc::decimalsCall::abi_encode_returns(
                        &if self.altered == Some("decimals") {
                            18
                        } else {
                            6
                        },
                    )
                } else if data == encoded(ArcUsdc::DOMAIN_SEPARATORCall {}.abi_encode()) {
                    // Independent fixed-width EIP-712 encoding, not the production tuple helper.
                    let separator = keccak256([
                        keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)").to_vec(),
                        keccak256("USDC").to_vec(), keccak256("2").to_vec(),
                        U256::from(self.chain).to_be_bytes::<32>().to_vec(), ARC_USDC.into_word().to_vec(),
                    ].concat());
                    ArcUsdc::DOMAIN_SEPARATORCall::abi_encode_returns(&if self.altered
                        == Some("separator")
                    {
                        B256::ZERO
                    } else {
                        separator
                    })
                } else {
                    return Err(ChainError::Unavailable);
                };
                Ok(json!(encoded(raw)))
            }
            _ => Err(ChainError::Unavailable),
        }
    }
}
struct Service {
    chain: u64,
    address: Address,
    calls: AtomicUsize,
}
#[tonic::async_trait]
impl CircleCommand for Service {
    async fn run(
        &self,
        arguments: Vec<String>,
        mutating: bool,
    ) -> Result<CommandOutput, CircleError> {
        assert!(!mutating);
        self.calls.fetch_add(1, Ordering::SeqCst);
        let stdout = if arguments == ["--version"] {
            b"1.1.4\n".to_vec()
        } else {
            assert_eq!(
                &arguments[..5],
                ["wallet", "list", "--type", "agent", "--chain"]
            );
            serde_json::to_vec(&json!({"data":{"wallets":[{"address":encoded(self.address),"type":"agent","blockchain":if self.chain==CHAIN {"ARC-TESTNET"} else {"ARC"}}]}})).unwrap()
        };
        Ok(CommandOutput {
            success: true,
            stdout,
        })
    }
}
struct Callbacks;
#[tonic::async_trait]
impl CoverageProvider for Callbacks {
    async fn coverage(
        &self,
        _: &PreparedRequest,
        _: &RunContext,
    ) -> IntegrationResult<TokenCounts> {
        panic!("setup cannot invoke provider coverage")
    }
}
impl ExecutionProvider for Callbacks {
    fn execute(
        &self,
        _: PreparedRequest,
        _: RunContext,
        _: Arc<dyn ProviderRuns>,
        _: CancellationToken,
    ) -> ExecutionStream {
        panic!("setup cannot invoke provider execution")
    }
}
struct Fixture {
    directory: PathBuf,
    path: PathBuf,
    rpc: Arc<TestRpc>,
    service: Arc<Service>,
}
impl Fixture {
    fn new(altered: Option<&'static str>, address: Address) -> Self {
        Self::on_chain(CHAIN, altered, address)
    }
    fn on_chain(chain: u64, altered: Option<&'static str>, address: Address) -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let directory = std::env::temp_dir().join(format!(
            "mppi-setup-{}",
            alloy_primitives::hex::encode(nonce)
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("wallet-operations.json");
        let fixture = Self {
            directory,
            path,
            rpc: Arc::new(TestRpc {
                chain,
                altered,
                ..Default::default()
            }),
            service: Arc::new(Service {
                chain,
                address,
                calls: AtomicUsize::new(0),
            }),
        };
        fixture.make_wallet(false).unwrap();
        fixture
    }
    fn host(&self) -> HostConfiguration {
        HostConfiguration {
            config: config(self.rpc.chain),
            profile: profile(self.rpc.chain),
            chain: Arc::new(ChainReader::new(self.rpc.clone(), profile(self.rpc.chain))),
        }
    }
    fn make_wallet(&self, existing: bool) -> Result<Arc<CircleAgentWallet>, CircleError> {
        Ok(Arc::new(CircleAgentWallet::with_command(
            self.host().chain,
            CodePin::new(operator(), keccak256([0x60, 0])).unwrap(),
            CodePin::new(Address::repeat_byte(0x55), keccak256([0x60, 0])).unwrap(),
            &self.path,
            CircleOptions::default(),
            existing,
            self.service.clone(),
        )?))
    }
    async fn compose(&self) -> Result<Mppi, SetupError> {
        compose(
            self.host(),
            self.make_wallet(true)
                .map_err(|_| SetupError("operation record invalid"))?,
            Arc::new(Callbacks),
            Arc::new(Callbacks),
            RuntimeOptions::default(),
        )
        .await
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

#[tokio::test]
async fn mainnet_composition_checks_its_wallet_and_usdc_domain_without_starting() {
    let fixture = Fixture::on_chain(5042, None, operator());
    let original = fs::read(&fixture.path).unwrap();
    let runtime = fixture.compose().await.unwrap();
    assert!(runtime.discovery("/discover").is_none());
    let _services = runtime.grpc_services();
    assert_eq!(fs::read(&fixture.path).unwrap(), original);
    assert_eq!(fixture.service.calls.load(Ordering::SeqCst), 2);
    for altered in ["network", "separator"] {
        let fixture = Fixture::on_chain(5042, Some(altered), operator());
        assert!(fixture.compose().await.is_err(), "accepted {altered}");
    }
    let mut fixture = Fixture::on_chain(5042, None, operator());
    Arc::get_mut(&mut fixture.service).unwrap().chain = CHAIN;
    assert!(fixture.compose().await.is_err());
    assert!(fixture.rpc.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn composition_preserves_existing_record_services_hooks_and_explicit_lifecycle() {
    use crate::proto::chat_server::Chat;
    let fixture = Fixture::new(None, operator());
    let original = fs::read(&fixture.path).unwrap();
    let errors = Arc::new(Mutex::new(Vec::new()));
    let captured = errors.clone();
    let options = RuntimeOptions {
        poll_interval_ms: Some(2000),
        hooks: Hooks {
            on_error: Some(Arc::new(move |error| captured.lock().unwrap().push(error))),
            ..Hooks::default()
        },
        ..RuntimeOptions::default()
    };
    let runtime = compose(
        fixture.host(),
        fixture.make_wallet(true).unwrap(),
        Arc::new(Callbacks),
        Arc::new(Callbacks),
        options,
    )
    .await
    .unwrap();
    assert_eq!(fs::read(&fixture.path).unwrap(), original);
    assert_eq!(fixture.service.calls.load(Ordering::SeqCst), 2);
    assert!(runtime.discovery("/discover").is_none());
    let _services = runtime.grpc_services();
    assert!(
        runtime
            .completions(tonic::Request::new(proto::CompleteRequest::default()))
            .await
            .is_err()
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while errors.lock().unwrap().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(errors.lock().unwrap()[0].operation, "complete");
    // Explicit start performs registry/follower work and fails closed on missing
    // provider evidence in this fixture. Composition did not attempt those reads.
    assert!(!runtime.start().await.unwrap().ready());
    runtime
        .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert!(!runtime.start().await.unwrap().ready());
    assert_eq!(fs::read(&fixture.path).unwrap(), original);
}

#[tokio::test]
async fn composition_rejects_wallet_and_profile_mismatch_without_new_history() {
    let fixture = Fixture::new(None, Address::repeat_byte(0x55));
    assert!(
        fixture
            .compose()
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("could not be verified")
    );
    assert!(fixture.rpc.calls.lock().unwrap().is_empty());
    let fixture = Fixture::new(None, operator());
    let mut host = fixture.host();
    host.profile = crate::deployment::official_testnet_profile().unwrap();
    assert!(
        compose(
            host,
            fixture.make_wallet(true).unwrap(),
            Arc::new(Callbacks),
            Arc::new(Callbacks),
            RuntimeOptions::default()
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.service.calls.load(Ordering::SeqCst), 0);
    assert!(fixture.rpc.calls.lock().unwrap().is_empty());
    fs::remove_file(&fixture.path).unwrap();
    assert!(fixture.compose().await.is_err());
    assert!(!fixture.path.exists());
    assert_eq!(fixture.service.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn usdc_domain_rejects_wrong_network_code_values_separator_abi_and_reorg() {
    for altered in [
        "network",
        "code",
        "name",
        "version",
        "decimals",
        "separator",
        "surplus",
        "canonical",
    ] {
        let fixture = Fixture::new(Some(altered), operator());
        let original = fs::read(&fixture.path).unwrap();
        assert!(fixture.compose().await.is_err(), "accepted {altered}");
        assert_eq!(fs::read(&fixture.path).unwrap(), original);
    }
}

#[tokio::test]
async fn operation_record_identity_is_checked_and_cannot_be_replaced() {
    let fixture = Fixture::new(None, operator());
    let mut value: Value = serde_json::from_slice(&fs::read(&fixture.path).unwrap()).unwrap();
    value["wallet"]["address"] = json!(encoded(Address::repeat_byte(0x22)));
    fs::write(&fixture.path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(fixture.compose().await.is_err());
    let retained: Value = serde_json::from_slice(&fs::read(&fixture.path).unwrap()).unwrap();
    assert_eq!(retained, value);
    #[cfg(unix)]
    {
        let saved = fixture.directory.join("retained.json");
        fs::rename(&fixture.path, &saved).unwrap();
        std::os::unix::fs::symlink(&saved, &fixture.path).unwrap();
        assert!(fixture.compose().await.is_err());
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(saved).unwrap()).unwrap(),
            value
        );
    }
}
