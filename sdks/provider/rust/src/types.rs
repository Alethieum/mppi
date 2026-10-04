//! Validated integration values. Construction does not verify chain evidence,
//! signatures, ownership, freshness or eligibility for paid execution.

use crate::signing::{SessionDescriptor, SigningContext};
use crate::{Address, B256, TokenCounts, ValidationCode, ValidationError};

fn invalid(message: &'static str) -> ValidationError {
    ValidationError::new(ValidationCode::InvalidValue, message)
}

/// Validate the represented addresses of the existing ABI descriptor.
pub fn validate_descriptor(value: &SessionDescriptor) -> Result<(), ValidationError> {
    if [
        value.agent,
        value.sessionSigner,
        value.recipient,
        value.operator,
        value.token,
    ]
    .iter()
    .any(|address| address.is_zero())
    {
        return Err(invalid("descriptor addresses must be nonzero"));
    }
    Ok(())
}

macro_rules! copy_getters {
    ($($name:ident : $ty:ty),* $(,)?) => {$ (
        pub const fn $name(&self) -> $ty { self.$name }
    )*};
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Domain {
    chain_id: u64,
    verifying_contract: Address,
}
impl Domain {
    pub fn new(chain_id: u64, verifying_contract: Address) -> Result<Self, ValidationError> {
        if chain_id == 0 || verifying_contract.is_zero() {
            return Err(invalid(
                "domain requires positive chain id and nonzero contract",
            ));
        }
        Ok(Self {
            chain_id,
            verifying_contract,
        })
    }
    copy_getters!(chain_id: u64, verifying_contract: Address);
    pub const fn signing_context(&self) -> SigningContext {
        SigningContext {
            chain_id: self.chain_id,
            verifying_contract: self.verifying_contract,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunContext {
    session_id: B256,
    run_id: B256,
    control_generation: u64,
    model: String,
    rate_card_hash: B256,
}
impl RunContext {
    pub fn new(
        session_id: B256,
        run_id: B256,
        control_generation: u64,
        model: impl Into<String>,
        rate_card_hash: B256,
    ) -> Result<Self, ValidationError> {
        if control_generation == 0 {
            return Err(invalid("control generation must be positive"));
        }
        let model = model.into();
        crate::validate_model(&model)?;
        Ok(Self {
            session_id,
            run_id,
            control_generation,
            model,
            rate_card_hash,
        })
    }
    copy_getters!(session_id: B256, run_id: B256, control_generation: u64, rate_card_hash: B256);
    pub fn model(&self) -> &str {
        &self.model
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainObservation {
    chain_id: u64,
    contract_address: Address,
    block_number: u64,
    block_hash: B256,
}
impl ChainObservation {
    pub fn new(
        chain_id: u64,
        contract_address: Address,
        block_number: u64,
        block_hash: B256,
    ) -> Result<Self, ValidationError> {
        Domain::new(chain_id, contract_address)?;
        Ok(Self {
            chain_id,
            contract_address,
            block_number,
            block_hash,
        })
    }
    copy_getters!(chain_id: u64, contract_address: Address, block_number: u64, block_hash: B256);
}

/// A structurally valid settlement reference; callers must verify the event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfirmedSettlementRef {
    observation: ChainObservation,
    tx_hash: B256,
    session_id: B256,
    settled_cumulative: u64,
    delta: u64,
}
impl ConfirmedSettlementRef {
    pub fn new(
        observation: ChainObservation,
        tx_hash: B256,
        session_id: B256,
        settled_cumulative: u64,
        delta: u64,
    ) -> Result<Self, ValidationError> {
        if delta == 0 || delta > settled_cumulative {
            return Err(invalid(
                "settlement delta must be positive and within cumulative amount",
            ));
        }
        Ok(Self {
            observation,
            tx_hash,
            session_id,
            settled_cumulative,
            delta,
        })
    }
    copy_getters!(observation: ChainObservation, tx_hash: B256, session_id: B256,
                  settled_cumulative: u64, delta: u64);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationState {
    Pending,
    Confirmed,
    Failed,
    Unknown,
}
impl OperationState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Confirmed => "confirmed",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionStatus {
    Open,
    Closing,
    Closed,
}
impl SessionStatus {
    /// The contract and protobuf enums deliberately have different values.
    pub fn from_contract(value: u8) -> Result<Self, ValidationError> {
        match value {
            0 => Ok(Self::Open),
            1 => Ok(Self::Closing),
            2 => Ok(Self::Closed),
            _ => Err(invalid("invalid contract session status")),
        }
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Closing => "CLOSING",
            Self::Closed => "CLOSED",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PauseLevel {
    None = 0,
    Payments = 1,
    All = 2,
}
impl TryFrom<u8> for PauseLevel {
    type Error = ValidationError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Payments),
            2 => Ok(Self::All),
            _ => Err(invalid("invalid pause level")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaymentCode {
    InsufficientDeposit,
    SessionNotFound,
    VoucherStale,
    SessionNotReady,
}
impl PaymentCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InsufficientDeposit => "INSUFFICIENT_DEPOSIT",
            Self::SessionNotFound => "SESSION_NOT_FOUND",
            Self::VoucherStale => "VOUCHER_STALE",
            Self::SessionNotReady => "SESSION_NOT_READY",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaymentFailure {
    code: PaymentCode,
    reason: Option<String>,
    session_id: Option<B256>,
    run_id: Option<B256>,
    requested_rate_card_hash: Option<B256>,
    current_rate_card_hash: Option<B256>,
    pause_level: Option<PauseLevel>,
}
impl PaymentFailure {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        code: PaymentCode,
        reason: Option<String>,
        session_id: Option<B256>,
        run_id: Option<B256>,
        requested_rate_card_hash: Option<B256>,
        current_rate_card_hash: Option<B256>,
        pause_level: Option<PauseLevel>,
    ) -> Result<Self, ValidationError> {
        if run_id.is_some() && session_id.is_none() {
            return Err(invalid("run diagnostic requires session id"));
        }
        if reason.as_ref().is_some_and(String::is_empty)
            || (code == PaymentCode::SessionNotReady && reason.is_none())
        {
            return Err(invalid("not-ready failure requires a nonempty reason"));
        }
        if reason.as_deref() == Some("PRICE_CHANGED") {
            if code != PaymentCode::SessionNotReady
                || requested_rate_card_hash.is_none()
                || current_rate_card_hash.is_none()
            {
                return Err(invalid("PRICE_CHANGED requires both catalog hashes"));
            }
        } else if requested_rate_card_hash.is_some() || current_rate_card_hash.is_some() {
            return Err(invalid("catalog diagnostics require PRICE_CHANGED"));
        }
        if reason.as_deref() == Some("CONTRACT_PAUSED") {
            if code != PaymentCode::SessionNotReady
                || !matches!(pause_level, Some(PauseLevel::Payments | PauseLevel::All))
            {
                return Err(invalid(
                    "CONTRACT_PAUSED requires an active observed pause level",
                ));
            }
        } else if pause_level.is_some() {
            return Err(invalid("pause diagnostic requires CONTRACT_PAUSED"));
        }
        Ok(Self {
            code,
            reason,
            session_id,
            run_id,
            requested_rate_card_hash,
            current_rate_card_hash,
            pause_level,
        })
    }
    copy_getters!(code: PaymentCode, session_id: Option<B256>, run_id: Option<B256>,
        requested_rate_card_hash: Option<B256>, current_rate_card_hash: Option<B256>,
        pause_level: Option<PauseLevel>);
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}
impl core::fmt::Display for PaymentFailure {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.code.as_str())
    }
}
impl std::error::Error for PaymentFailure {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Readiness {
    ready: bool,
    failures: Vec<PaymentFailure>,
}
impl Readiness {
    pub fn new(ready: bool, failures: Vec<PaymentFailure>) -> Result<Self, ValidationError> {
        if ready && !failures.is_empty() {
            return Err(invalid("ready result cannot contain failures"));
        }
        Ok(Self { ready, failures })
    }
    copy_getters!(ready: bool);
    pub fn failures(&self) -> &[PaymentFailure] {
        &self.failures
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionOperationOutcome {
    session_id: B256,
    target: Option<u64>,
    target_covered: Option<bool>,
    settlement_reference: Option<ConfirmedSettlementRef>,
    failure: Option<PaymentFailure>,
}
impl SessionOperationOutcome {
    pub fn new(
        session_id: B256,
        target: Option<u64>,
        target_covered: Option<bool>,
        settlement_reference: Option<ConfirmedSettlementRef>,
        failure: Option<PaymentFailure>,
    ) -> Result<Self, ValidationError> {
        if settlement_reference.is_some_and(|value| value.session_id != session_id) {
            return Err(invalid("settlement reference names another session"));
        }
        Ok(Self {
            session_id,
            target,
            target_covered,
            settlement_reference,
            failure,
        })
    }
    copy_getters!(session_id: B256, target: Option<u64>, target_covered: Option<bool>,
                  settlement_reference: Option<ConfirmedSettlementRef>);
    pub fn failure(&self) -> Option<&PaymentFailure> {
        self.failure.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationResult {
    operation_ref: String,
    state: OperationState,
    wallet_operation_id: Option<String>,
    tx_hash: Option<B256>,
    observation: Option<ChainObservation>,
    sessions: Vec<SessionOperationOutcome>,
}
impl OperationResult {
    pub fn new(
        operation_ref: impl Into<String>,
        state: OperationState,
        wallet_operation_id: Option<String>,
        tx_hash: Option<B256>,
        observation: Option<ChainObservation>,
        sessions: Vec<SessionOperationOutcome>,
    ) -> Result<Self, ValidationError> {
        let operation_ref = operation_ref.into();
        if operation_ref.is_empty() {
            return Err(invalid("operation reference is required"));
        }
        if wallet_operation_id.as_ref().is_some_and(String::is_empty) {
            return Err(invalid("wallet operation id must be nonempty when present"));
        }
        if state == OperationState::Confirmed && (observation.is_none() || tx_hash.is_none()) {
            return Err(invalid(
                "confirmed operation requires transaction and block evidence",
            ));
        }
        Ok(Self {
            operation_ref,
            state,
            wallet_operation_id,
            tx_hash,
            observation,
            sessions,
        })
    }
    copy_getters!(state: OperationState, tx_hash: Option<B256>, observation: Option<ChainObservation>);
    pub fn operation_ref(&self) -> &str {
        &self.operation_ref
    }
    pub fn wallet_operation_id(&self) -> Option<&str> {
        self.wallet_operation_id.as_deref()
    }
    pub fn sessions(&self) -> &[SessionOperationOutcome] {
        &self.sessions
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionView {
    session_id: B256,
    descriptor: SessionDescriptor,
    deposit: u64,
    settled_cumulative: u64,
    accepted_authorization: Option<u64>,
    usage_amount: Option<u64>,
    status: SessionStatus,
    observation: ChainObservation,
    fresh: bool,
    readiness: Readiness,
}
impl SessionView {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_id: B256,
        descriptor: SessionDescriptor,
        deposit: u64,
        settled_cumulative: u64,
        accepted_authorization: Option<u64>,
        usage_amount: Option<u64>,
        status: SessionStatus,
        observation: ChainObservation,
        fresh: bool,
        readiness: Readiness,
    ) -> Result<Self, ValidationError> {
        validate_descriptor(&descriptor)?;
        if deposit == 0 || settled_cumulative > deposit {
            return Err(invalid(
                "session requires positive deposit covering settlement",
            ));
        }
        Ok(Self {
            session_id,
            descriptor,
            deposit,
            settled_cumulative,
            accepted_authorization,
            usage_amount,
            status,
            observation,
            fresh,
            readiness,
        })
    }
    copy_getters!(session_id: B256, deposit: u64, settled_cumulative: u64,
        accepted_authorization: Option<u64>, usage_amount: Option<u64>, status: SessionStatus,
        observation: ChainObservation, fresh: bool);
    pub fn descriptor(&self) -> &SessionDescriptor {
        &self.descriptor
    }
    pub fn readiness(&self) -> &Readiness {
        &self.readiness
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionPage {
    pub sessions: Vec<SessionView>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShutdownResult {
    pub unresolved_session_ids: Vec<B256>,
    pub unresolved_runs: Vec<RunContext>,
    pub pending_operations: Vec<OperationResult>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountingResult {
    context: RunContext,
    counts: TokenCounts,
    meter_seq: u64,
    session_usage_amount: u64,
    duplicate: bool,
}
impl AccountingResult {
    pub fn new(
        context: RunContext,
        counts: TokenCounts,
        meter_seq: u64,
        session_usage_amount: u64,
        duplicate: bool,
    ) -> Result<Self, ValidationError> {
        if meter_seq == 0 {
            return Err(invalid("meter sequence must be positive"));
        }
        Ok(Self {
            context,
            counts,
            meter_seq,
            session_usage_amount,
            duplicate,
        })
    }
    copy_getters!(counts: TokenCounts, meter_seq: u64, session_usage_amount: u64, duplicate: bool);
    pub fn context(&self) -> &RunContext {
        &self.context
    }
}

/// A nonpayable call; no transaction or wallet execution is implied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalletCall {
    chain_id: u64,
    to: Address,
    data: Vec<u8>,
}
impl WalletCall {
    pub fn new(chain_id: u64, to: Address, data: Vec<u8>) -> Result<Self, ValidationError> {
        Domain::new(chain_id, to)?;
        Ok(Self { chain_id, to, data })
    }
    copy_getters!(chain_id: u64, to: Address);
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

/// Pagination is local known-session enumeration, with no invented recovery pages.
pub fn validate_page_limit(limit: usize) -> Result<usize, ValidationError> {
    if !(1..=1000).contains(&limit) {
        return Err(invalid("session page limit must be in 1..1000"));
    }
    Ok(limit)
}
