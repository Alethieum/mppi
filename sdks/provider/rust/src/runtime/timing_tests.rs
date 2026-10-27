//! `on_timing` splits a payment step into its measured parts on Tokio's paused
//! clock, so every duration below is exact.
use super::*;
use crate::{
    capabilities::{WalletCall, WalletError},
    chain::{ChainError, Rpc},
    interfaces::ProviderSessions,
    types::OperationState,
};
use serde_json::Value;
use std::sync::mpsc;

fn timings(fixture: &mut Fixture) -> mpsc::Receiver<OperationTiming> {
    let (sent, received) = mpsc::channel();
    Arc::get_mut(&mut fixture.provider.inner).unwrap().observers = hooks::Observers::new(Hooks {
        on_timing: Some(Arc::new(move |timing| sent.send(timing).unwrap())),
        ..Hooks::default()
    });
    received
}
fn next(timings: &mpsc::Receiver<OperationTiming>) -> OperationTiming {
    timings.recv_timeout(Duration::from_secs(5)).unwrap()
}

/// A wallet service that takes 400 ms to accept a transaction.
struct SlowWallet;
#[tonic::async_trait]
impl TransactionWallet for SlowWallet {
    fn address(&self) -> Address {
        addr(0x11)
    }
    async fn execute(
        &self,
        _: &WalletCall,
        reference: &str,
    ) -> Result<OperationResult, WalletError> {
        tokio::time::sleep(Duration::from_millis(400)).await;
        Ok(OperationResult::new(
            reference,
            OperationState::Pending,
            Some("wallet-1".into()),
            Some(hash(0xdd)),
            None,
            vec![],
        )
        .unwrap())
    }
    async fn reconcile(&self, _: &str) -> Result<OperationResult, WalletError> {
        panic!("a first collection submits")
    }
}
/// An RPC whose receipt for that transaction takes 100 ms and is not mined yet.
struct SlowReceipt(Arc<TestRpc>);
#[tonic::async_trait]
impl Rpc for SlowReceipt {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        if method == "eth_getTransactionReceipt" && params[0] == hx(hash(0xdd)) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(Value::Null);
        }
        self.0.call(method, params).await
    }
}

#[tokio::test(start_paused = true)]
async fn each_collection_reports_where_its_time_went() {
    let mut fixture =
        Fixture::with_wallet(1000, Arc::new(SlowWallet), |rpc| Arc::new(SlowReceipt(rpc))).await;
    let timings = timings(&mut fixture);
    fixture.voucher(100);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(1000), Some(RunStatus::Completed))
        .unwrap();
    // Another wallet operation holds the runtime's one-at-a-time turn for 300 ms.
    let turn = fixture.provider.inner.operations.lock().await;
    let provider = fixture.provider.clone();
    let sid = fixture.sid;
    let settle = tokio::spawn(async move { provider.settle(sid).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(turn);
    let result = settle.await.unwrap().unwrap();
    assert_eq!(result.state(), OperationState::Pending);
    assert_eq!(
        next(&timings),
        OperationTiming {
            operation: "settle",
            state: Some("pending"),
            total_ms: 800,
            coverage_ms: 0,
            chain_ms: 100,
            wallet_ms: 400,
            waiting_ms: 300,
        }
    );
    // A refused step reports no state.
    fixture.provider.inner.lock().unwrap().sessions.clear();
    assert!(fixture.provider.close(sid).await.is_err());
    let refused = next(&timings);
    assert_eq!((refused.operation, refused.state), ("close", None));
}

/// Host coverage that takes 300 ms, such as tokenizing the prompt.
struct SlowCoverage;
#[tonic::async_trait]
impl CoverageProvider for SlowCoverage {
    async fn coverage(
        &self,
        _: &PreparedRequest,
        _: &RunContext,
    ) -> IntegrationResult<TokenCounts> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(counts(1000))
    }
}
impl ExecutionProvider for SlowCoverage {
    fn execute(
        &self,
        _: PreparedRequest,
        _: RunContext,
        _: Arc<dyn ProviderRuns>,
        _: CancellationToken,
    ) -> ExecutionStream {
        Box::pin(Streaming)
    }
}
/// An admitted run that keeps streaming.
struct Streaming;
impl futures_core::Stream for Streaming {
    type Item = IntegrationResult<Vec<u8>>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Pending
    }
}

#[tokio::test(start_paused = true)]
async fn an_admission_reports_its_coverage_and_its_wait() {
    use proto::chat_server::Chat;
    let mut fixture = Fixture::new(1000).await; // 1,000 output tokens cost one unit.
    {
        let runtime = Arc::get_mut(&mut fixture.provider.inner).unwrap();
        runtime.coverage = Arc::new(SlowCoverage);
        runtime.execution = Arc::new(SlowCoverage);
    }
    let timings = timings(&mut fixture);
    let start = Instant::now();
    let provider = fixture.provider.clone();
    let request = fixture.request(1);
    let admitted =
        tokio::spawn(async move { provider.completions(tonic::Request::new(request)).await });
    // The agent's voucher arrives 500 ms after the coverage request.
    tokio::time::sleep_until(start + Duration::from_millis(800)).await;
    fixture.voucher(1);
    fixture.provider.inner.changed.notify_waiters();
    let _stream = admitted.await.unwrap().unwrap();
    assert_eq!(
        next(&timings),
        OperationTiming {
            operation: "admission",
            state: Some("admitted"),
            total_ms: 800,
            coverage_ms: 300,
            chain_ms: 0,
            wallet_ms: 0,
            waiting_ms: 500,
        }
    );
    // Nothing covers the next run: its 10 s authorization wait ends it.
    let error = fixture
        .provider
        .completions(tonic::Request::new(fixture.request(2)))
        .await
        .err()
        .unwrap();
    assert_eq!(reason(&error).as_deref(), Some("AUTHORIZATION_REQUIRED"));
    assert_eq!(
        next(&timings),
        OperationTiming {
            operation: "admission",
            state: None,
            total_ms: 10_300,
            coverage_ms: 300,
            chain_ms: 0,
            wallet_ms: 0,
            waiting_ms: 10_000,
        }
    );
}
