//! Embedded native provider payment runtime. The caller owns its server,
//! execution, token measurement, wallet and durable payment history.

use prost::Message;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};
use tokio::{sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::{
    B256, Catalog, Rates, RunAccounting, RunStatus, TokenCounts, U256, ValidationCode,
    ValidationError,
    capabilities::TransactionWallet,
    chain::{ChainReader, OpeningEvidence},
    config::ProviderConfig,
    contract::{ContractCalls, PreparedOperation},
    cumulative_usage,
    discovery::DiscoveryDocuments,
    follower::{ChainFollower, ChainSnapshot},
    interfaces::{
        CoverageProvider, CoverageResult, ExecutionProvider, FinalAccountingResult,
        IntegrationResult, PreparedRequest, ProviderRuns,
    },
    priced_numerator, proto,
    queue::ByteQueue,
    remaining_funding_tokens, report_transition, required_coverage,
    types::{
        AccountingResult, ChainObservation, OperationResult, PaymentCode, PaymentFailure,
        Readiness, RunContext, SessionStatus,
    },
    wire::{self, WireLimits},
};

mod core;
mod hooks;
pub use hooks::{HookError, Hooks, ObserverStats, OperationTiming};
mod lifecycle;
mod recovery;
mod services;
mod sessions;
pub use services::{RpcStream, UpstreamFailure};
#[cfg(test)]
mod tests;

/// Calldata the runtime prepares, in bytes. Every argument has a fixed size, so
/// these are exact for every session, the worst case included. One `settle` or
/// `close`: the 4-byte selector, the descriptor's seven 32-byte words, the
/// cumulative and capture words, the signature's offset and length words, and
/// its 65 bytes padded to 96 (452; a refund-only close has no signature, 356).
/// A `settleBatch`: the selector, four head offsets and four array lengths
/// (260), then per session the descriptor's seven words, the two amount words,
/// and the signature's offset, length and 96 padded bytes (448).
pub(super) const SETTLE_CALL_BYTES: usize = 4 + 7 * 32 + 2 * 32 + 2 * 32 + 96;
const BATCH_CALL_BYTES: usize = 4 + 4 * 32 + 4 * 32;
const BATCH_ITEM_BYTES: usize = 7 * 32 + 2 * 32 + 2 * 32 + 96;
/// The most sessions one batch holds within `max_call_bytes`: 260 + 448 n
/// bytes. Circle's 61,440 hold (61,440 - 260) / 448 = 136 sessions in 61,188
/// bytes; 137 would take 61,636. At least one: a single session is collected by
/// `settle`, which fits every bound the runtime accepts.
pub(super) fn batch_sessions(max_call_bytes: usize) -> usize {
    (max_call_bytes.saturating_sub(BATCH_CALL_BYTES) / BATCH_ITEM_BYTES).max(1)
}

/// Local retention limits. They do not change MPPI wire messages. No count of
/// sessions is limited: a closed session retires after `closed_retention`.
#[derive(Clone, Debug)]
pub struct RuntimeLimits {
    pub(super) max_control_streams: usize,
    pub(super) wire: WireLimits,
    pub(super) control_auth_timeout: Duration,
    pub(super) closed_retention: Duration,
}
impl RuntimeLimits {
    pub fn new(max_control_streams: usize, wire: WireLimits) -> Result<Self, ValidationError> {
        if max_control_streams == 0 {
            return Err(ValidationError::new(
                ValidationCode::InvalidValue,
                "runtime retention limits must be positive",
            ));
        }
        Ok(Self {
            max_control_streams,
            wire,
            ..Self::default()
        })
    }
    /// Bound the unauthenticated Control setup phase independently of client
    /// grpc-timeout and Complete's authorization wait. Defaults to 60 seconds.
    /// Successful signed attachment removes this setup limit. A client that
    /// needs longer funding preparation can reopen Control and authenticate its
    /// existing session; expiration never creates replacement funding.
    pub fn with_control_auth_timeout(mut self, timeout: Duration) -> Result<Self, ValidationError> {
        if timeout.is_zero() || timeout > Duration::from_secs(3600) {
            return Err(ValidationError::new(
                ValidationCode::InvalidValue,
                "Control authentication timeout must be positive and at most one hour",
            ));
        }
        self.control_auth_timeout = timeout;
        Ok(self)
    }
    pub fn control_auth_timeout(&self) -> Duration {
        self.control_auth_timeout
    }
    /// How long a final session stays retained before it retires, from one
    /// hour to seven days; 24 hours by default. Within it an agent can
    /// reattach to read the final state and the host can take a final
    /// checkpoint. A final session has nothing left to serve, collect or
    /// reconcile, and a closed session is terminal on chain.
    pub fn with_closed_retention(mut self, retention: Duration) -> Result<Self, ValidationError> {
        if retention < Duration::from_secs(3600) || retention > Duration::from_secs(7 * 86400) {
            return Err(ValidationError::new(
                ValidationCode::InvalidValue,
                "closed session retention must be one hour to seven days",
            ));
        }
        self.closed_retention = retention;
        Ok(self)
    }
    pub fn closed_retention(&self) -> Duration {
        self.closed_retention
    }
    pub fn max_control_streams(&self) -> usize {
        self.max_control_streams
    }
    pub fn wire(&self) -> &WireLimits {
        &self.wire
    }
}
impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            max_control_streams: 1024,
            wire: WireLimits::default(),
            control_auth_timeout: Duration::from_secs(60),
            closed_retention: Duration::from_secs(86400),
        }
    }
}

/// Cloneable handle to one provider runtime; construction starts no I/O.
#[derive(Clone)]
pub struct Mppi {
    pub(super) inner: Arc<Runtime>,
}

pub(super) struct Runtime {
    pub(super) config: ProviderConfig,
    pub(super) chain: Arc<ChainFollower>,
    pub(super) reader: ChainReader,
    pub(super) contract: ContractCalls,
    pub(super) wallet: Arc<dyn TransactionWallet>,
    /// `TransactionWallet::max_call_bytes`, read once at construction.
    pub(super) max_call_bytes: usize,
    pub(super) coverage: Arc<dyn CoverageProvider>,
    pub(super) execution: Arc<dyn ExecutionProvider>,
    pub(super) limits: RuntimeLimits,
    observers: hooks::Observers,
    pub(super) state: Mutex<State>,
    /// Wakes shutdown, which waits for the final report of every active run
    /// (`Runtime::wake_locked`).
    pub(super) changed: Notify,
    pub(super) operations: tokio::sync::Mutex<()>,
    pub(super) openings: tokio::sync::Mutex<()>,
    pub(super) lifecycle: tokio::sync::Mutex<()>,
    pub(super) maintenance: Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub(super) tasks: Mutex<tokio::task::JoinSet<()>>,
    pub(super) halt: CancellationToken,
}

pub(super) struct State {
    pub(super) sessions: BTreeMap<B256, Session>,
    /// Openings and recoveries in progress that hold a session's chain watch,
    /// counted: recoveries run concurrently (`PendingWatch`).
    pub(super) pending_watch: BTreeMap<B256, usize>,
    pub(super) collecting: BTreeSet<B256>,
    pub(super) empty_batch: Option<PendingOperation>,
    pub(super) unsubmitted_collection: Option<String>,
    pub(super) attachments: BTreeMap<u64, Attachment>,
    pub(super) started: bool,
    pub(super) stopping: bool,
    pub(super) start_block: Option<u64>,
    pub(super) retained_request_bytes: usize,
    pub(super) next_attachment: u64,
    pub(super) documents: Option<DiscoveryDocuments>,
    /// The follower's readiness at the last maintenance pass.
    pub(super) readiness: Option<Readiness>,
    /// Each operation reference this runtime prepared or restored, and for
    /// each session it covers the time from which that session no longer
    /// needs it: `None` while the session's checkpoint may still name it; the
    /// time the session retired here; or `closed_retention` after a later
    /// confirmed collection of the session superseded it.
    pub(super) references: BTreeMap<String, BTreeMap<B256, Option<SystemTime>>>,
    /// References no covered session needs, each with the time from which the
    /// wallet may forget it (`TransactionWallet::retire`), the latest of its
    /// sessions' times; whether that call is under way; and, after the wallet
    /// refused one, when maintenance may try again.
    pub(super) retirable: Vec<(String, SystemTime)>,
    /// The record of each reference no wallet answer in this process has
    /// matched (`recovery::agrees`): restored without a transaction hash, so
    /// not looked up at recovery, or kept against a wallet answer that
    /// disagreed with it. Its hand-over to `retire` takes recovery's wallet
    /// check; a matching answer removes it.
    pub(super) unconfirmed: BTreeMap<String, OperationResult>,
    pub(super) retiring: bool,
    pub(super) retire_after: Option<Instant>,
    /// `TransactionWallet::near_capacity` at the last maintenance pass.
    pub(super) near_capacity: bool,
}
/// Runtime-local watch interest across asynchronous opening/recovery. The
/// public follower retains idempotent watch/unwatch semantics. The last
/// interest in a session that was not installed releases its watch.
struct PendingWatch {
    runtime: Arc<Runtime>,
    sid: B256,
}
impl PendingWatch {
    fn new(runtime: &Arc<Runtime>, sid: B256) -> Result<Self, Status> {
        *runtime.lock()?.pending_watch.entry(sid).or_default() += 1;
        Ok(Self {
            runtime: runtime.clone(),
            sid,
        })
    }
}
impl Drop for PendingWatch {
    fn drop(&mut self) {
        let Ok(mut state) = self.runtime.lock() else {
            return;
        };
        let Some(count) = state.pending_watch.get_mut(&self.sid) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            state.pending_watch.remove(&self.sid);
            // Current installed state owns the watch. A cancelled/failed
            // operation with no remaining session must release its interest.
            if !state.sessions.contains_key(&self.sid) {
                let _ = self.runtime.chain.unwatch_session(self.sid);
            }
        }
    }
}

pub(super) struct Attachment {
    pub(super) queue: Arc<ByteQueue>,
    pub(super) nonce: B256,
    pub(super) live: bool,
    pub(super) sid: Option<B256>,
    pub(super) generation: u64,
    pub(super) request: Option<proto::ReconnectRequest>,
    pub(super) challenges: BTreeMap<B256, proto::Challenge>,
    pub(super) catalogs: BTreeMap<B256, Catalog>,
    pub(super) challenge_bytes: usize,
}
pub(super) struct Session {
    pub(super) sid: B256,
    pub(super) opening: OpeningEvidence,
    pub(super) catalogs: BTreeMap<B256, Catalog>,
    pub(super) publications: BTreeMap<B256, ChainObservation>,
    pub(super) runs: BTreeMap<B256, Run>,
    pub(super) voucher: Option<proto::Voucher>,
    pub(super) generation: u64,
    pub(super) owner: Option<u64>,
    // Only the unauthenticated attachment that inserted this temporary opening
    // may discard it. Recovery never inherits provisional cleanup ownership.
    pub(super) provisional_owner: Option<u64>,
    pub(super) meter_seq: u64,
    pub(super) close_pending: bool,
    pub(super) close_reference: Option<String>,
    pub(super) inconsistent: bool,
    pub(super) history_available: bool,
    /// Every admission is in `runs`: the session was opened by this runtime,
    /// or restored from history its host asserts is current. Only then does a
    /// pending run absent from `runs` prove it was never admitted.
    pub(super) admissions_known: bool,
    pub(super) operation: Option<PendingOperation>,
    pub(super) baseline: Option<PendingOperation>,
    pub(super) close_result: Option<proto::CloseResult>,
    pub(super) confirmed_units: u64,
    pub(super) last_collection: Instant,
    /// Runs that have waited for admission so far: the next one's position.
    pub(super) waited: u64,
    /// When maintenance first found this session final (`Session::is_final`).
    pub(super) final_since: Option<Instant>,
    /// Wakes this session's runs waiting for admission (`Runtime::wake_locked`).
    /// A session is replaced, retired or discarded only while none of its runs
    /// is preparing (`check_replacement`, `Session::is_final`, a provisional
    /// opening has no runs), so no waiting run outlives the session it waits on.
    pub(super) wake: Arc<Notify>,
    /// Admission checks of this session's runs, counted for the wake-up tests.
    #[cfg(test)]
    pub(super) checks: u64,
}
#[derive(Clone)]
pub(super) struct PendingOperation {
    pub(super) prepared: Arc<PreparedOperation>,
    pub(super) result: Arc<OperationResult>,
    pub(super) units: Arc<BTreeMap<B256, u64>>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RunPhase {
    Preparing,
    Admitted,
    Dispatched,
    NotAdmitted,
}
pub(super) struct Run {
    pub(super) context: RunContext,
    pub(super) signature: [u8; 65],
    pub(super) rates: Rates,
    pub(super) accounting: RunAccounting,
    pub(super) covered_counts: TokenCounts,
    pub(super) phase: RunPhase,
    pub(super) stop: CancellationToken,
    pub(super) terminal: Option<proto::Terminal>,
    pub(super) meter_seq: u64,
    /// Advisory deduplication only; no payment authority or durable history.
    pub(super) last_low_credit: Option<(u64, U256)>,
    pub(super) unresolved: bool,
    pub(super) retained_bytes: usize,
    /// Its place among runs waiting for admission, from its first wait.
    pub(super) queued: Option<Queued>,
    /// This runtime's first refusal of the run's coverage. A host error that
    /// then ends its execution reaches the agent as this typed failure.
    pub(super) refusal: Option<PaymentFailure>,
}
pub(super) struct Queued {
    pub(super) position: u64,
    pub(super) since: Instant,
    /// The last coverage request sent for it, when, and the voucher then.
    pub(super) sent: Option<(Vec<u8>, Instant, Option<u64>)>,
}
/// How long a waiting run keeps its place ahead of later covered runs, and how
/// often a head the agent has not covered repeats its request: a few voucher
/// round trips. The turn is also at most half the authorization wait, so a
/// covered run never waits out its turn into that wait's expiry.
pub(super) const TURN: Duration = Duration::from_secs(2);
pub(super) struct RunHandle {
    pub(super) context: RunContext,
    pub(super) request: PreparedRequest,
    pub(super) stop: CancellationToken,
    pub(super) output: Arc<ByteQueue>,
    /// Its session's `Session::wake`.
    pub(super) wake: Arc<Notify>,
}
pub(super) enum Admission {
    Granted,
    /// Authorization is missing: the configured wait applies.
    Authorizing,
    /// Covered, but an earlier run is still within its turn.
    Deferred,
}

impl Mppi {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: ProviderConfig,
        chain: Arc<ChainFollower>,
        contract: ContractCalls,
        wallet: Arc<dyn TransactionWallet>,
        coverage: Arc<dyn CoverageProvider>,
        execution: Arc<dyn ExecutionProvider>,
    ) -> Result<Self, ValidationError> {
        Self::with_limits(
            config,
            chain,
            contract,
            wallet,
            coverage,
            execution,
            RuntimeLimits::default(),
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn with_limits(
        config: ProviderConfig,
        chain: Arc<ChainFollower>,
        contract: ContractCalls,
        wallet: Arc<dyn TransactionWallet>,
        coverage: Arc<dyn CoverageProvider>,
        execution: Arc<dyn ExecutionProvider>,
        limits: RuntimeLimits,
    ) -> Result<Self, ValidationError> {
        Self::with_limits_and_hooks(
            config,
            chain,
            contract,
            wallet,
            coverage,
            execution,
            limits,
            Hooks::default(),
        )
    }
    /// Configure typed best-effort observers without changing payment callbacks.
    #[allow(clippy::too_many_arguments)]
    pub fn with_limits_and_hooks(
        config: ProviderConfig,
        chain: Arc<ChainFollower>,
        contract: ContractCalls,
        wallet: Arc<dyn TransactionWallet>,
        coverage: Arc<dyn CoverageProvider>,
        execution: Arc<dyn ExecutionProvider>,
        limits: RuntimeLimits,
        hooks: Hooks,
    ) -> Result<Self, ValidationError> {
        if contract.domain() != chain.reader().profile().domain()
            || config.chain().chain_id() != contract.domain().chain_id()
            || wallet.address() != config.identity().wallet_address()
            || chain.max_age_ms() != config.chain().max_age_ms()
            || chain.collection_margin_ms() != config.payment().collection_margin_ms()
        {
            return Err(ValidationError::new(
                ValidationCode::InvalidValue,
                "provider deployment, wallet or follower configuration mismatch",
            ));
        }
        let max_call_bytes = wallet.max_call_bytes();
        if max_call_bytes < SETTLE_CALL_BYTES {
            return Err(ValidationError::new(
                ValidationCode::InvalidValue,
                "wallet call bound is below one settlement call (452 bytes)",
            ));
        }
        let reader = chain.reader().clone();
        Ok(Self {
            inner: Arc::new(Runtime {
                config,
                chain,
                reader,
                contract,
                wallet,
                max_call_bytes,
                coverage,
                execution,
                limits,
                observers: hooks::Observers::new(hooks),
                state: Mutex::new(State {
                    sessions: BTreeMap::new(),
                    pending_watch: BTreeMap::new(),
                    collecting: BTreeSet::new(),
                    empty_batch: None,
                    unsubmitted_collection: None,
                    attachments: BTreeMap::new(),
                    started: false,
                    stopping: false,
                    start_block: None,
                    retained_request_bytes: 0,
                    next_attachment: 0,
                    documents: None,
                    readiness: None,
                    references: BTreeMap::new(),
                    retirable: Vec::new(),
                    unconfirmed: BTreeMap::new(),
                    retiring: false,
                    retire_after: None,
                    near_capacity: false,
                }),
                changed: Notify::new(),
                operations: tokio::sync::Mutex::new(()),
                openings: tokio::sync::Mutex::new(()),
                lifecycle: tokio::sync::Mutex::new(()),
                maintenance: Mutex::new(None),
                tasks: Mutex::new(tokio::task::JoinSet::new()),
                halt: CancellationToken::new(),
            }),
        })
    }
}

impl Mppi {
    pub fn hook_stats(&self) -> ObserverStats {
        self.inner.observers.stats()
    }
}

impl Runtime {
    pub(super) fn lock(&self) -> Result<MutexGuard<'_, State>, Status> {
        self.state
            .lock()
            .map_err(|_| Status::internal("payment state unavailable"))
    }
}

impl Run {
    pub(super) fn admitted(&self) -> bool {
        matches!(self.phase, RunPhase::Admitted | RunPhase::Dispatched)
    }
    pub(super) fn active(&self) -> bool {
        self.admitted() && self.accounting.final_status().is_none()
    }
    /// When this run's turn began, while it waits for admission.
    pub(super) fn waiting(&self) -> Option<&Queued> {
        self.queued
            .as_ref()
            .filter(|_| self.phase == RunPhase::Preparing && !self.stop.is_cancelled())
    }
}
impl Session {
    pub(super) fn new(sid: B256, opening: OpeningEvidence, history_available: bool) -> Self {
        let catalogs = BTreeMap::from([(
            opening.initial_catalog().rate_card_hash(),
            opening.initial_catalog().clone(),
        )]);
        Self {
            sid,
            opening,
            catalogs,
            publications: BTreeMap::new(),
            runs: BTreeMap::new(),
            voucher: None,
            generation: 0,
            owner: None,
            provisional_owner: None,
            meter_seq: 0,
            close_pending: false,
            close_reference: None,
            inconsistent: false,
            history_available,
            admissions_known: true,
            operation: None,
            baseline: None,
            close_result: None,
            confirmed_units: 0,
            last_collection: Instant::now(),
            waited: 0,
            final_since: None,
            wake: Arc::new(Notify::new()),
            #[cfg(test)]
            checks: 0,
        }
    }
    pub(super) fn active(&self) -> impl Iterator<Item = &Run> {
        self.runs.values().filter(|run| run.active())
    }
    pub(super) fn usage_numerator(&self) -> Result<U256, ValidationError> {
        self.runs
            .values()
            .filter(|run| run.admitted())
            .try_fold(U256::ZERO, |total, run| {
                total
                    .checked_add(priced_numerator(run.accounting.counts(), run.rates)?)
                    .ok_or_else(|| {
                        ValidationError::new(
                            ValidationCode::InvalidValue,
                            "session numerator overflow",
                        )
                    })
            })
    }
    pub(super) fn usage(&self) -> Result<u64, ValidationError> {
        cumulative_usage([self.usage_numerator()?])
    }
    pub(super) fn units(&self) -> Result<u64, ValidationError> {
        self.runs
            .values()
            .filter(|run| run.admitted())
            .try_fold(0u64, |total, run| {
                total
                    .checked_add(run.accounting.counts().input_tokens())
                    .and_then(|n| n.checked_add(run.accounting.counts().output_tokens()))
                    .ok_or_else(|| {
                        ValidationError::new(ValidationCode::InvalidValue, "session units overflow")
                    })
            })
    }
}

pub(super) fn not_ready(reason: &str, sid: Option<B256>, rid: Option<B256>) -> Status {
    wire::payment_status(
        &PaymentFailure::new(
            PaymentCode::SessionNotReady,
            Some(reason.into()),
            sid,
            rid,
            None,
            None,
            None,
        )
        .expect("fixed payment failure structure"),
    )
}
pub(super) fn validation_status(error: ValidationError) -> Status {
    if error.code() == ValidationCode::LimitExceeded {
        Status::resource_exhausted(error.to_string())
    } else {
        Status::invalid_argument(error.to_string())
    }
}
pub(super) fn control(kind: proto::control_message::Kind) -> proto::ControlMessage {
    proto::ControlMessage { kind: Some(kind) }
}
pub(super) fn zero_counts() -> TokenCounts {
    TokenCounts::new(0, 0, 0).expect("zero counts")
}

pub(super) fn random_id() -> Result<B256, Status> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|_| Status::unavailable("secure randomness unavailable"))?;
    Ok(B256::from(bytes))
}
