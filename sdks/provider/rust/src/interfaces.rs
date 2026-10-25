//! Native async contracts for provider-owned execution and SDK payment reports.
//!
//! These traits add no serving runtime, storage, token measurement or execution
//! engine. Implementations must keep one consistent session view and validate
//! supplied recovery evidence before admitting work.

use crate::types::{
    AccountingResult, ChainObservation, OperationResult, PaymentFailure, Readiness, RunContext,
    SessionPage, SessionView, ShutdownResult,
};
use crate::{B256, RunAccounting, RunStatus, TokenCounts, ValidationCode, ValidationError, proto};
use futures_core::Stream;
use std::{pin::Pin, sync::Arc};
use tokio_util::sync::CancellationToken;

pub type IntegrationError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type IntegrationResult<T> = Result<T, IntegrationError>;
pub type ExecutionStream = Pin<Box<dyn Stream<Item = IntegrationResult<Vec<u8>>> + Send + 'static>>;
/// Native services with MPPI's message-length checks retained around decoding.
pub type GrpcServices<C, P> = (
    crate::transport::LimitsService<proto::chat_server::ChatServer<C>>,
    crate::transport::LimitsService<proto::control_server::ControlServer<P>>,
);

/// Validated original inference bytes; preparation performs no paid work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedRequest {
    payload: Vec<u8>,
    stream: bool,
}
impl PreparedRequest {
    pub fn new(
        payload: Vec<u8>,
        stream: bool,
        context: &RunContext,
    ) -> Result<Self, ValidationError> {
        Self::with_limits(
            payload,
            stream,
            context,
            &crate::wire::WireLimits::default(),
        )
    }
    pub fn with_limits(
        payload: Vec<u8>,
        stream: bool,
        context: &RunContext,
        limits: &crate::wire::WireLimits,
    ) -> Result<Self, ValidationError> {
        crate::wire::validate_json_with_limits(
            &payload,
            Some(context.model()),
            Some(stream),
            limits,
        )?;
        Ok(Self { payload, stream })
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub const fn stream(&self) -> bool {
        self.stream
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoverageResult {
    granted: bool,
    required_amount: u64,
    failure: Option<PaymentFailure>,
}
impl CoverageResult {
    pub fn new(
        granted: bool,
        required_amount: u64,
        failure: Option<PaymentFailure>,
    ) -> Result<Self, ValidationError> {
        if granted && failure.is_some() {
            return Err(ValidationError::new(
                ValidationCode::InvalidValue,
                "coverage grant cannot contain a payment failure",
            ));
        }
        Ok(Self {
            granted,
            required_amount,
            failure,
        })
    }
    pub const fn granted(&self) -> bool {
        self.granted
    }
    pub const fn required_amount(&self) -> u64 {
        self.required_amount
    }
    pub fn failure(&self) -> Option<&PaymentFailure> {
        self.failure.as_ref()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalAccountingResult {
    accounting: AccountingResult,
    status: RunStatus,
}
impl FinalAccountingResult {
    pub fn new(accounting: AccountingResult, status: RunStatus) -> Result<Self, ValidationError> {
        if status == RunStatus::NotAdmitted {
            return Err(ValidationError::new(
                ValidationCode::InvalidValue,
                "final accounting requires an admitted outcome",
            ));
        }
        Ok(Self { accounting, status })
    }
    pub fn accounting(&self) -> &AccountingResult {
        &self.accounting
    }
    pub const fn status(&self) -> RunStatus {
        self.status
    }
}

/// Detached caller-supplied recovery evidence. These records remain untrusted
/// until the runtime checks original proofs, price history and chain evidence.
#[derive(Clone, Debug)]
pub struct RecoveryRun {
    pub context: RunContext,
    pub signature: [u8; 65],
    pub accounting: RunAccounting,
    pub covered_counts: TokenCounts,
    pub meter_seq: u64,
    pub admitted: bool,
    pub not_admitted: bool,
}
#[derive(Clone, Debug)]
pub struct RecoveryOperation {
    pub prepared: crate::contract::PreparedOperation,
    pub result: OperationResult,
    pub units: Vec<(B256, u64)>,
}
#[derive(Clone, Debug)]
pub struct RecoveryEvidence {
    /// Detached local evidence, never attachment authority. Generation zero
    /// requires authoritative unused history with no voucher, runs or usage.
    /// Chain records alone cannot establish it. Recovery installs no owner;
    /// the first fresh authenticated attachment still assigns generation one.
    pub snapshot: proto::SessionState,
    pub reservations: Vec<(RunContext, RunAccounting)>,
    pub observation: ChainObservation,
    pub opening_tx: Option<B256>,
    pub runs: Vec<RecoveryRun>,
    pub publications: Vec<(Vec<u8>, ChainObservation)>,
    pub operation: Option<RecoveryOperation>,
    pub baseline: Option<RecoveryOperation>,
    pub confirmed_units: u64,
    pub close_reference: Option<String>,
    pub inconsistent: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[tonic::async_trait]
pub trait CoverageProvider: Send + Sync {
    async fn coverage(
        &self,
        request: &PreparedRequest,
        context: &RunContext,
    ) -> IntegrationResult<TokenCounts>;
}
#[tonic::async_trait]
pub trait ProviderRuns: Send + Sync {
    async fn require_coverage(
        &self,
        context: &RunContext,
        remaining_tokens: TokenCounts,
    ) -> IntegrationResult<CoverageResult>;
    async fn report_usage(
        &self,
        context: &RunContext,
        counts: TokenCounts,
    ) -> IntegrationResult<AccountingResult>;
    async fn report_terminal(
        &self,
        context: &RunContext,
        counts: TokenCounts,
        status: RunStatus,
    ) -> IntegrationResult<FinalAccountingResult>;
}
/// Called only after payment admission. Output is original JSON after the
/// provider consumes upstream framing. Cancellation requests termination;
/// stream EOF and cancellation are never definitive accounting reports.
pub trait ExecutionProvider: Send + Sync {
    fn execute(
        &self,
        request: PreparedRequest,
        context: RunContext,
        runs: Arc<dyn ProviderRuns>,
        stop_requested: CancellationToken,
    ) -> ExecutionStream;
}
#[tonic::async_trait]
pub trait ProviderSessions: Send + Sync {
    async fn checkpoint(&self, session_id: B256) -> IntegrationResult<RecoveryEvidence>;
    async fn recover(&self, evidence: RecoveryEvidence) -> IntegrationResult<SessionView>;
    /// As `recover`, for evidence the host asserts is the session's current
    /// history: checkpointed after every admission and before dispatch, and
    /// never older than its newest checkpoint. A pending run absent from that
    /// history was then never admitted, and a reattachment answers it
    /// NOT_ADMITTED. After `recover` such a run refuses the attachment
    /// instead, since an older checkpoint may lack a run that was served.
    async fn recover_current(&self, evidence: RecoveryEvidence) -> IntegrationResult<SessionView> {
        let _ = evidence;
        Err(Box::new(tonic::Status::unimplemented(
            "recovery from asserted current history is unsupported",
        )))
    }
    async fn settle(&self, session_id: B256) -> IntegrationResult<OperationResult>;
    async fn settle_batch(&self, session_ids: &[B256]) -> IntegrationResult<OperationResult>;
    async fn close(&self, session_id: B256) -> IntegrationResult<OperationResult>;
    async fn get(&self, session_id: B256) -> IntegrationResult<SessionView>;
    /// Implementations validate limit through types::validate_page_limit.
    async fn list(&self, cursor: Option<&str>, limit: usize) -> IntegrationResult<SessionPage>;
}
/// Embedded resource lifetime. A tonic server accepts the associated generated
/// service implementations; the caller retains its listener and task runtime.
#[tonic::async_trait]
pub trait ProviderLifecycle: Send + Sync {
    type Chat: proto::chat_server::Chat;
    type Control: proto::control_server::Control;
    fn runs(&self) -> Arc<dyn ProviderRuns>;
    fn sessions(&self) -> Arc<dyn ProviderSessions>;
    fn grpc_services(&self) -> GrpcServices<Self::Chat, Self::Control>;
    fn discovery(&self, path: &str) -> Option<DiscoveryResponse>;
    async fn start(&self) -> IntegrationResult<Readiness>;
    /// Absolute monotonic deadline; unresolved runs retain reservations.
    async fn shutdown(&self, deadline: tokio::time::Instant) -> IntegrationResult<ShutdownResult>;
}
