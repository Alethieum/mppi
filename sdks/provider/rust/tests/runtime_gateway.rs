//! Independent native provider process for tests/test_rust_runtime.py.
//!
//! Execution, counts, the OpenAI-compatible HTTP adapter and the disposable Anvil
//! wallet below are supplied by this fixture. They are not SDK implementations.

use alloy_primitives::{Address, B256, hex};
use alloy_sol_types::SolCall;
use futures_core::Stream;
use mppi_provider::{
    RunStatus, TokenCounts,
    capabilities::{TransactionWallet, WalletCall, WalletError},
    chain::ChainReader,
    config::ProviderConfig,
    contract::{ContractCalls, abi::IMPPI},
    deployment::profile_from_mapping,
    follower::ChainFollower,
    http::{HttpJsonRpc, HttpTransport, MAX_RPC_RESPONSE_BYTES},
    interfaces::{
        CoverageProvider, ExecutionProvider, ExecutionStream, IntegrationResult, PreparedRequest,
        ProviderLifecycle, ProviderRuns,
    },
    runtime::Mppi,
    types::{OperationResult, OperationState, RunContext},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;

const CHAIN_ID: u64 = 5042002;
const MODEL: &str = "fixture/model";
const MAX_UPSTREAM_BYTES: usize = 1 << 20;

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap()
}

fn hx(value: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(value))
}

fn write_json(path: &Path, value: &Value) {
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, serde_json::to_vec(value).unwrap()).unwrap();
    fs::rename(temporary, path).unwrap();
}

fn fixture_error(message: &'static str) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::other(message).into()
}

/// Anvil writes deliberately stay outside the production read-only RPC trait.
struct OwnedRpc {
    endpoint: String,
    transport: HttpTransport,
    next_id: AtomicU64,
}

impl OwnedRpc {
    async fn new(endpoint: &str) -> Self {
        let url = reqwest::Url::parse(endpoint).unwrap();
        assert_eq!(url.scheme(), "http");
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert!(url.port().is_some());
        assert!(url.username().is_empty() && url.password().is_none());
        let rpc = Self {
            endpoint: endpoint.to_owned(),
            transport: HttpTransport::new(5000, MAX_RPC_RESPONSE_BYTES, None).unwrap(),
            next_id: AtomicU64::new(1),
        };
        assert_eq!(
            rpc.call("eth_chainId", json!([])).await.unwrap(),
            json!(format!("0x{CHAIN_ID:x}"))
        );
        assert!(
            rpc.call("web3_clientVersion", json!([]))
                .await
                .unwrap()
                .as_str()
                .unwrap()
                .to_ascii_lowercase()
                .contains("anvil")
        );
        rpc
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, WalletError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
                .map_err(|_| WalletError)?;
        let bytes = self
            .transport
            .request(&self.endpoint, Some(&request))
            .await
            .map_err(|_| WalletError)?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| WalletError)?;
        if value["jsonrpc"] != "2.0" || value["id"] != id || value.get("error").is_some() {
            return Err(WalletError);
        }
        value.get("result").cloned().ok_or(WalletError)
    }
}

/// Process-local fixture evidence; production durable custody is caller-owned.
struct LocalWallet {
    rpc: OwnedRpc,
    address: Address,
    proxy: Address,
    operations: Mutex<BTreeMap<String, (WalletCall, OperationResult)>>,
    submissions: AtomicUsize,
}

impl LocalWallet {
    async fn new(endpoint: &str, address: Address, proxy: Address) -> Self {
        let rpc = OwnedRpc::new(endpoint).await;
        rpc.call("anvil_impersonateAccount", json!([hx(address)]))
            .await
            .unwrap();
        Self {
            rpc,
            address,
            proxy,
            operations: Mutex::new(BTreeMap::new()),
            submissions: AtomicUsize::new(0),
        }
    }

    fn evidence(&self) -> Vec<Value> {
        self.operations
            .lock()
            .unwrap()
            .iter()
            .map(|(reference, (call, result))| {
                json!({
                    "reference":reference,"calldata":hx(call.data()),"to":hx(call.to()),
                    "tx_hash":result.tx_hash().map(hx),"state":result.state().as_str()
                })
            })
            .collect()
    }
}

#[tonic::async_trait]
impl TransactionWallet for LocalWallet {
    fn address(&self) -> Address {
        self.address
    }

    async fn execute(
        &self,
        call: &WalletCall,
        operation_ref: &str,
    ) -> Result<OperationResult, WalletError> {
        if call.chain_id() != CHAIN_ID
            || call.to() != self.proxy
            || operation_ref.is_empty()
            || !(call.data().starts_with(&IMPPI::settleCall::SELECTOR)
                || call.data().starts_with(&IMPPI::settleBatchCall::SELECTOR)
                || call.data().starts_with(&IMPPI::closeCall::SELECTOR))
        {
            return Err(WalletError);
        }
        {
            let mut operations = self.operations.lock().map_err(|_| WalletError)?;
            if let Some((original, result)) = operations.get(operation_ref) {
                if original != call {
                    return Err(WalletError);
                }
                return Ok(result.clone());
            }
            let result = OperationResult::new(
                operation_ref,
                OperationState::Unknown,
                Some(operation_ref.into()),
                None,
                None,
                vec![],
            )
            .map_err(|_| WalletError)?;
            operations.insert(operation_ref.into(), (call.clone(), result));
        }
        self.submissions.fetch_add(1, Ordering::Relaxed);
        let transaction = self
            .rpc
            .call(
                "eth_sendTransaction",
                json!([{
                    "from":hx(self.address),"to":hx(self.proxy),"data":hx(call.data()),
                    "value":"0x0","gas":"0xb71b00","chainId":format!("0x{CHAIN_ID:x}")
                }]),
            )
            .await?;
        let tx_hash: B256 = transaction
            .as_str()
            .ok_or(WalletError)?
            .parse()
            .map_err(|_| WalletError)?;
        let result = OperationResult::new(
            operation_ref,
            OperationState::Pending,
            Some(operation_ref.into()),
            Some(tx_hash),
            None,
            vec![],
        )
        .map_err(|_| WalletError)?;
        // Retain the hash before any receipt wait; a lost reply cannot resubmit.
        self.operations
            .lock()
            .map_err(|_| WalletError)?
            .get_mut(operation_ref)
            .ok_or(WalletError)?
            .1 = result.clone();
        for _ in 0..250 {
            if self
                .rpc
                .call("eth_getTransactionReceipt", json!([hx(tx_hash)]))
                .await?
                .is_object()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(result)
    }

    async fn reconcile(&self, operation_ref: &str) -> Result<OperationResult, WalletError> {
        Ok(self
            .operations
            .lock()
            .map_err(|_| WalletError)?
            .get(operation_ref)
            .map(|(_, result)| result.clone())
            .unwrap_or(
                OperationResult::new(
                    operation_ref,
                    OperationState::Unknown,
                    None,
                    None,
                    None,
                    vec![],
                )
                .map_err(|_| WalletError)?,
            ))
    }
}

#[derive(Default)]
struct Metrics {
    preparations: AtomicUsize,
    executions: AtomicUsize,
    usage_reports: AtomicUsize,
    final_reports: AtomicUsize,
    coverage_grants: AtomicUsize,
    coverage_denials: AtomicUsize,
    stop_requests: AtomicUsize,
    execution_errors: AtomicUsize,
    actual_usage: AtomicU64,
    session_id: Mutex<Option<B256>>,
}

struct Gateway {
    endpoint: String,
    client: reqwest::Client,
    metrics: Arc<Metrics>,
}

#[tonic::async_trait]
impl CoverageProvider for Gateway {
    async fn coverage(
        &self,
        request: &PreparedRequest,
        context: &RunContext,
    ) -> IntegrationResult<TokenCounts> {
        self.metrics.preparations.fetch_add(1, Ordering::Relaxed);
        assert_eq!(context.model(), MODEL);
        assert!(request.stream());
        let payload: Value = serde_json::from_slice(request.payload())?;
        assert_eq!(payload["model"], MODEL);
        assert_eq!(payload["stream_options"]["include_usage"], true);
        // The simulated provider supplies this bound; the SDK does no counting.
        Ok(TokenCounts::new(4510, 0, 1000)?)
    }
}

struct Output(mpsc::Receiver<IntegrationResult<Vec<u8>>>);
impl Stream for Output {
    type Item = IntegrationResult<Vec<u8>>;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(context)
    }
}

impl ExecutionProvider for Gateway {
    fn execute(
        &self,
        request: PreparedRequest,
        context: RunContext,
        runs: Arc<dyn ProviderRuns>,
        stop_requested: CancellationToken,
    ) -> ExecutionStream {
        self.metrics.executions.fetch_add(1, Ordering::Relaxed);
        *self.metrics.session_id.lock().unwrap() = Some(context.session_id());
        let (sender, receiver) = mpsc::channel(4);
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let result = tokio::select! {
                _ = stop_requested.cancelled() => {
                    metrics.stop_requests.fetch_add(1, Ordering::Relaxed);
                    Err(fixture_error("upstream termination is unresolved"))
                },
                result = consume_upstream(client, endpoint, request, context, runs, &sender, &metrics) => result,
            };
            if let Err(error) = result {
                metrics.execution_errors.fetch_add(1, Ordering::Relaxed);
                // Missing final provider facts keep the SDK reservation intact.
                let _ = sender.send(Err(error)).await;
            }
        });
        Box::pin(Output(receiver))
    }
}

async fn consume_upstream(
    client: reqwest::Client,
    endpoint: String,
    request: PreparedRequest,
    context: RunContext,
    runs: Arc<dyn ProviderRuns>,
    sender: &mpsc::Sender<IntegrationResult<Vec<u8>>>,
    metrics: &Metrics,
) -> IntegrationResult<()> {
    // This is the test provider's actual HTTP forward of original request bytes.
    let mut response = client
        .post(endpoint)
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .body(request.payload().to_vec())
        .send()
        .await?;
    if response.status() != reqwest::StatusCode::OK
        || response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            != Some("text/event-stream")
    {
        return Err(fixture_error(
            "fixture upstream did not return an event stream",
        ));
    }
    let mut buffered = Vec::new();
    let mut received = 0usize;
    let mut payloads = 0usize;
    let mut final_counts = None;
    while let Some(chunk) = response.chunk().await? {
        received = received
            .checked_add(chunk.len())
            .ok_or_else(|| fixture_error("fixture stream overflow"))?;
        if received > MAX_UPSTREAM_BYTES {
            return Err(fixture_error("fixture stream limit"));
        }
        buffered.extend_from_slice(&chunk);
        while let Some(end) = buffered.windows(2).position(|pair| pair == b"\n\n") {
            let frame: Vec<u8> = buffered.drain(..end + 2).collect();
            let data = frame[..end]
                .strip_prefix(b"data: ")
                .ok_or_else(|| fixture_error("unexpected fixture event frame"))?;
            if data == b"[DONE]" {
                let counts =
                    final_counts.ok_or_else(|| fixture_error("upstream omitted final usage"))?;
                let report = runs
                    .report_terminal(&context, counts, RunStatus::Completed)
                    .await?;
                assert_eq!(report.accounting().session_usage_amount(), 5_420_000);
                metrics.final_reports.fetch_add(1, Ordering::Relaxed);
                metrics.actual_usage.store(
                    report.accounting().session_usage_amount(),
                    Ordering::Relaxed,
                );
                return Ok(());
            }
            let value: Value = serde_json::from_slice(data)?;
            if value["usage"].is_object() {
                let usage = &value["usage"];
                final_counts = Some(TokenCounts::new(
                    usage["prompt_tokens"]
                        .as_u64()
                        .ok_or_else(|| fixture_error("missing input count"))?,
                    usage["prompt_tokens_details"]["cached_tokens"]
                        .as_u64()
                        .unwrap_or(0),
                    usage["completion_tokens"]
                        .as_u64()
                        .ok_or_else(|| fixture_error("missing output count"))?,
                )?);
                if !value["choices"].as_array().is_some_and(Vec::is_empty) {
                    return Err(fixture_error("usage event must have empty choices"));
                }
            }
            sender
                .send(Ok(data.to_vec()))
                .await
                .map_err(|_| fixture_error("agent output closed"))?;
            payloads += 1;
            if payloads == 1 {
                // Deterministic provider facts for the simulated execution stage.
                runs.report_usage(&context, TokenCounts::new(4510, 0, 450)?)
                    .await?;
                metrics.usage_reports.fetch_add(1, Ordering::Relaxed);
                let grant = runs
                    .require_coverage(&context, TokenCounts::new(0, 0, 550)?)
                    .await?;
                if !grant.granted() {
                    metrics.coverage_denials.fetch_add(1, Ordering::Relaxed);
                    return Err(fixture_error("continuation coverage denied"));
                }
                metrics.coverage_grants.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    Err(fixture_error(
        "upstream ended without definitive usage and completion",
    ))
}

#[tokio::test]
#[ignore = "requires the owned Anvil, HTTP upstream and Python agent launcher"]
async fn owned_runtime_gateway() {
    let input: Value = serde_json::from_slice(
        &fs::read(std::env::var("MPPI_RUST_RUNTIME_INPUT").unwrap()).unwrap(),
    )
    .unwrap();
    let profile = profile_from_mapping(&input["profile"]).unwrap();
    assert_eq!(profile.domain().chain_id(), CHAIN_ID);
    let config = ProviderConfig::from_mapping(&input["config"], None).unwrap();
    let endpoint = text(&input, "rpc");
    let wallet = Arc::new(
        LocalWallet::new(
            endpoint,
            config.identity().wallet_address(),
            profile.domain().verifying_contract(),
        )
        .await,
    );
    let reader = ChainReader::new(
        Arc::new(
            HttpJsonRpc::new(
                endpoint,
                HttpTransport::new(5000, MAX_RPC_RESPONSE_BYTES, None).unwrap(),
            )
            .unwrap(),
        ),
        profile,
    );
    let chain = Arc::new(
        ChainFollower::new(
            reader.clone(),
            config.chain().max_age_ms(),
            config.payment().collection_margin_ms(),
            Some(100),
        )
        .unwrap(),
    );
    let metrics = Arc::new(Metrics::default());
    let upstream = config.upstream().completions_url();
    let url = reqwest::Url::parse(&upstream).unwrap();
    assert_eq!(url.scheme(), "http");
    assert_eq!(url.host_str(), Some("127.0.0.1"));
    let gateway = Arc::new(Gateway {
        endpoint: upstream,
        metrics: metrics.clone(),
        client: reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap(),
    });
    let mut provider = Mppi::new(
        config.clone(),
        chain,
        ContractCalls::new(*reader.profile().domain(), "USDC".into(), "2".into()).unwrap(),
        wallet.clone(),
        gateway.clone(),
        gateway.clone(),
    )
    .unwrap();
    let (chat, control) = provider.grpc_services();
    let grpc_addr: SocketAddr = text(&input, "grpc_addr").parse().unwrap();
    assert!(grpc_addr.ip().is_loopback() && grpc_addr.port() != 0);
    let mut stop_server = CancellationToken::new();
    let server_stop = stop_server.clone();
    let mut server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(chat)
            .add_service(control)
            .serve_with_shutdown(grpc_addr, server_stop.cancelled_owned())
            .await
            .unwrap();
    });
    let readiness = provider.start().await.unwrap();
    assert!(
        readiness.ready(),
        "native provider readiness failed: {readiness:?}"
    );
    let discovery = [
        "/discover",
        "/.well-known/agent-registration.json",
        "/models",
    ]
    .into_iter()
    .map(|path| {
        let response = provider.discovery(path).unwrap();
        (
            path.to_owned(),
            json!({"status":response.status,"headers":response.headers,
                "body":String::from_utf8(response.body).unwrap()}),
        )
    })
    .collect::<serde_json::Map<_, _>>();
    write_json(
        Path::new(text(&input, "ready_path")),
        &json!({"grpc_addr":grpc_addr.to_string(),"discovery":discovery}),
    );
    let stop = Path::new(text(&input, "stop_path"));
    let mut recovery_result = None;
    let deadline = Instant::now() + Duration::from_secs(90);
    while !stop.exists() {
        assert!(
            Instant::now() < deadline,
            "Python runtime fixture did not stop"
        );
        assert!(!server.is_finished(), "native tonic server exited early");
        if recovery_result.is_none()
            && let Some(path) = input.get("recover_path").and_then(Value::as_str)
            && Path::new(path).exists()
        {
            let sid = metrics.session_id.lock().unwrap().unwrap();
            let evidence = provider.sessions().checkpoint(sid).await.unwrap();
            let before_generation = evidence.snapshot.control_generation.unwrap();
            assert_eq!(evidence.snapshot.settled_cumulative, Some(0));
            let stopped = provider
                .shutdown(Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
            assert!(stopped.unresolved_runs.is_empty());
            assert!(stopped.pending_operations.is_empty());
            stop_server.cancel();
            tokio::time::timeout(Duration::from_secs(5), &mut server)
                .await
                .unwrap()
                .unwrap();
            // The fixture owns the detached history. Construct a completely new
            // SDK runtime/follower and supply it through the public boundary.
            let chain = Arc::new(
                ChainFollower::new(
                    reader.clone(),
                    config.chain().max_age_ms(),
                    config.payment().collection_margin_ms(),
                    Some(100),
                )
                .unwrap(),
            );
            provider = Mppi::new(
                config.clone(),
                chain,
                ContractCalls::new(*reader.profile().domain(), "USDC".into(), "2".into()).unwrap(),
                wallet.clone(),
                gateway.clone(),
                gateway.clone(),
            )
            .unwrap();
            assert!(provider.start().await.unwrap().ready());
            let restored = provider.sessions().recover(evidence).await.unwrap();
            assert_eq!(restored.usage_amount(), Some(5_420_000));
            assert_eq!(restored.settled_cumulative(), 0);
            assert!(!restored.readiness().ready());
            let operation = provider.sessions().settle_batch(&[sid, sid]).await.unwrap();
            // Duplicate targets have confirmed coverage but no attributable
            // per-item result: one paid and the repeated item was silent.
            assert_eq!(operation.state(), OperationState::Unknown);
            assert!(operation.observation().is_some());
            assert_eq!(operation.sessions().len(), 2);
            assert!(operation.sessions().iter().all(|item| {
                item.target_covered() == Some(true) && item.settlement_reference().is_none()
            }));
            let captured = provider.sessions().get(sid).await.unwrap();
            assert_eq!(captured.settled_cumulative(), 5_420_000);
            let (chat, control) = provider.grpc_services();
            stop_server = CancellationToken::new();
            let server_stop = stop_server.clone();
            server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(chat)
                    .add_service(control)
                    .serve_with_shutdown(grpc_addr, server_stop.cancelled_owned())
                    .await
                    .unwrap();
            });
            let result = json!({
                "generation_before":before_generation,
                "restored_usage":restored.usage_amount(),
                "restored_ready":restored.readiness().ready(),
                "batch_tx":operation.tx_hash().map(hx),
                "batch_items":operation.sessions().len(),
                "captured":captured.settled_cumulative(),
            });
            write_json(Path::new(text(&input, "recovered_path")), &result);
            recovery_result = Some(result);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let sessions = provider.sessions().list(None, 100).await.unwrap();
    let views = sessions.sessions.iter().map(|view| json!({
        "session_id":hx(view.session_id()),"deposit":view.deposit(),
        "settled_cumulative":view.settled_cumulative(),"authorization":view.accepted_authorization(),
        "usage":view.usage_amount(),"status":view.status().as_str()
    })).collect::<Vec<_>>();
    let shutdown = provider
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    stop_server.cancel();
    if tokio::time::timeout(Duration::from_secs(5), &mut server)
        .await
        .is_err()
    {
        server.abort();
        let _ = server.await;
    }
    write_json(
        Path::new(text(&input, "result_path")),
        &json!({
            "preparations":metrics.preparations.load(Ordering::Relaxed),
            "executions":metrics.executions.load(Ordering::Relaxed),
            "usage_reports":metrics.usage_reports.load(Ordering::Relaxed),
            "final_reports":metrics.final_reports.load(Ordering::Relaxed),
            "coverage_grants":metrics.coverage_grants.load(Ordering::Relaxed),
            "coverage_denials":metrics.coverage_denials.load(Ordering::Relaxed),
            "stop_requests":metrics.stop_requests.load(Ordering::Relaxed),
            "execution_errors":metrics.execution_errors.load(Ordering::Relaxed),
            "actual_usage":metrics.actual_usage.load(Ordering::Relaxed),
            "session_id":metrics.session_id.lock().unwrap().map(hx),
            "wallet_submissions":wallet.submissions.load(Ordering::Relaxed),
            "operations":wallet.evidence(),"sessions":views,
            "recovery":recovery_result,
            "shutdown_unresolved_runs":shutdown.unresolved_runs.len(),
            "shutdown_pending_operations":shutdown.pending_operations.len()
        }),
    );
}
