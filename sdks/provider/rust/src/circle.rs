//! Circle Agent Wallet CLI 1.1.4, with retained exact original operations.
//!
//! Login and custody belong to the official CLI. No API key or entity secret is
//! accepted. CLI status is never canonical chain confirmation. Once dispatched,
//! an intent is only reconciled by its returned transaction ID, never resubmitted.
//! Any Circle account generation is accepted: no account code is pinned. An
//! unresolved result's cause names the error kind and any listed CLI error
//! code; CLI messages and other output are never kept. An execute checks the
//! CLI version and wallet selection first unless a check began within the last
//! 60 seconds; signing relies on the last passing check until a command fails.
//! The journal's lock is held only while it is read or replaced, never
//! while a CLI command runs; an execute or reconcile waits for it up to its CLI
//! timeout. A result Circle returned that the journal then cannot take is
//! returned unknown with Circle's transaction id and hash, and written at the
//! adapter's next journal step. A record keeps its call's digest, not the call;
//! `retire` marks it with a time from which it is forgotten, once it holds a
//! transaction hash, and a reconcile of it withdraws that mark.
//! Sources: @circle-fin/cli 1.1.4 wallet/execute.ts, wallet/sign.ts,
//! transaction/list.ts and wallet/helpers.ts (published dist/index.js.map);
//! https://developers.circle.com/agent-stack/agent-wallets/quickstart
//! https://developers.circle.com/agent-stack/agent-wallets/supported-blockchains

use std::{
    collections::{BTreeMap, HashSet},
    ffi::OsString,
    fmt,
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex as StdMutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{Address, B256, U256, keccak256};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::{
    capabilities::{TransactionWallet, WalletError},
    chain::{ChainReader, encoded},
    deployment::profile_to_mapping,
    discovery::parse_address,
    local_state::{LOCK_WAIT, LocalState},
    registry::RegistryCalls,
    types::{OperationResult, OperationState, WalletCall},
};

const MAX_OUTPUT: usize = 1 << 20;
const MAX_STDERR: usize = 64 << 10;
const MAX_CALL: usize = 60 << 10;
// The journal's only bound, the same in the Python adapter: a new call it
// cannot record is refused before Circle is asked. From three quarters of it,
// `near_capacity` reports true.
const MAX_JOURNAL: usize = 16 << 20;
// An execute checks the CLI and wallet first unless a check that passed began
// less than this long ago; signing and `verify_wallet` accept any passing check.
const CHECK_AGE: Duration = Duration::from_secs(60);
const EXECUTE: &str = "execute(address,uint256,bytes)";
// CLI 1.1.4 reports a failure as {"error":{"code","message",...}} on stdout and
// exits 1 (dist/index.js:732-750). These are all the codes its callers pass:
// literals, the HTTP status mappings (:97292-97300, :113126-113134), the remote
// version block (:974) and the one propagated exception code (:113304-113306).
// Only a listed code is retained, never the message or any other output.
const CLI_ERRORS: [&str; 10] = [
    "AUTH_REQUIRED",
    "AUTH_EXPIRED",
    "PERMISSION_DENIED",
    "INVALID_ARGUMENT",
    "NOT_FOUND",
    "CONFLICT",
    "TIMEOUT",
    "VERSION_BLOCKED",
    "LOCAL_WALLET_UNAVAILABLE",
    "INTERNAL",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CircleError {
    Configuration,
    Unavailable,
    Authentication,
    Version,
    InvalidEvidence,
    Journal,
    Capacity,
}
impl fmt::Display for CircleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Configuration => "invalid Circle Agent Wallet configuration",
            Self::Unavailable => "Circle Agent Wallet command unavailable; check CLI access and the selected network session",
            Self::Authentication => "Circle Agent Wallet session missing or expired; run circle wallet login EMAIL for mainnet, or circle wallet login EMAIL --testnet, then rerun with the same retained operation records",
            Self::Version => "Circle CLI must be the verified @circle-fin/cli version 1.1.4",
            Self::InvalidEvidence => "Circle Agent Wallet evidence differs from the selected account or original operation",
            Self::Journal => "Circle Agent Wallet operation record unavailable or inconsistent; restore the original record",
            Self::Capacity => "Circle Agent Wallet operation record capacity reached",
        })
    }
}
impl std::error::Error for CircleError {}

/// Where the adapter reads the time: the monotonic clock ages the last check,
/// the wall clock decides which retired records are due. Tests set both.
pub(crate) trait Clock: Send + Sync {
    fn monotonic(&self) -> Instant;
    fn wall(&self) -> SystemTime;
}
struct SystemClock;
impl Clock for SystemClock {
    fn monotonic(&self) -> Instant {
        Instant::now()
    }
    fn wall(&self) -> SystemTime {
        SystemTime::now()
    }
}
/// Whole seconds since the Unix epoch, rounded down; none before it.
fn unix_seconds(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|elapsed| elapsed.as_secs())
}

/// A failed step: the adapter's error and the listed code the CLI reported.
#[derive(Clone, Copy, Debug)]
struct Failure {
    error: CircleError,
    code: Option<&'static str>,
}
impl From<CircleError> for Failure {
    fn from(error: CircleError) -> Self {
        Self { error, code: None }
    }
}
impl Failure {
    /// An operation result's cause: the error kind, then any CLI code.
    fn cause(self) -> String {
        match self.code {
            Some(code) => format!("{:?} ({code})", self.error),
            None => format!("{:?}", self.error),
        }
    }
}
/// A hashless Failed: the journal proves Circle was never asked to execute.
fn not_submitted(reference: &str, failure: Failure) -> Result<OperationResult, CircleError> {
    OperationResult::new(reference, OperationState::Failed, None, None, None, vec![])
        .and_then(|result| result.with_cause(format!("NotSubmittedError: {}", failure.cause())))
        .map_err(|_| CircleError::Journal)
}
/// An Unknown with no handles: whether Circle was asked cannot be shown.
fn unresolved(reference: &str, failure: Failure) -> Result<OperationResult, CircleError> {
    OperationResult::new(reference, OperationState::Unknown, None, None, None, vec![])
        .and_then(|result| result.with_cause(failure.cause()))
        .map_err(|_| CircleError::Journal)
}

/// Trusted CLI executable prefix. Never shell text or credentials. Read commands
/// have a deadline; execute/sign are drained to completion without a timeout kill.
/// `read_timeout_ms` also bounds how long an execute or reconcile waits for the
/// journal's lock.
#[derive(Clone, Debug)]
pub struct CircleOptions {
    pub command: Vec<OsString>,
    pub read_timeout_ms: u64,
    pub max_pages: usize,
    pub allow_registration_activation: bool,
}
impl Default for CircleOptions {
    fn default() -> Self {
        Self {
            command: vec!["circle".into()],
            read_timeout_ms: 240_000,
            max_pages: 20,
            allow_registration_activation: false,
        }
    }
}
impl CircleOptions {
    fn validate(&self) -> Result<(), CircleError> {
        if self.command.is_empty()
            || self.command.len() > 16
            || self
                .command
                .iter()
                .any(|s| s.is_empty() || s.len() > 4096 || s.as_encoded_bytes().contains(&0))
            || !(1..=300_000).contains(&self.read_timeout_ms)
            || !(1..=100).contains(&self.max_pages)
        {
            return Err(CircleError::Configuration);
        }
        Ok(())
    }
}

fn blockchain(chain: u64) -> Result<&'static str, CircleError> {
    match chain {
        5042 => Ok("ARC"),
        5_042_002 => Ok("ARC-TESTNET"),
        _ => Err(CircleError::Configuration),
    }
}
fn uuid(value: &str, v4: bool) -> Result<(), CircleError> {
    if value.len() != 36
        || !value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
        || v4 && (value.as_bytes()[14] != b'4' || !b"89ab".contains(&value.as_bytes()[19]))
    {
        return Err(CircleError::InvalidEvidence);
    }
    Ok(())
}
fn fresh_key() -> Result<String, CircleError> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| CircleError::Unavailable)?;
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    let h = alloy_primitives::hex::encode(bytes);
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    ))
}
fn reference(value: &str) -> Result<(), CircleError> {
    if value.is_empty() || value.len() > 4096 {
        Err(CircleError::Configuration)
    } else {
        Ok(())
    }
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str, CircleError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(CircleError::InvalidEvidence)
}
fn address(value: &Value, key: &str) -> Result<Address, CircleError> {
    parse_address(text(value, key)?).map_err(|_| CircleError::InvalidEvidence)
}
fn bytes(value: &str, maximum: usize) -> Result<Vec<u8>, CircleError> {
    let h = value
        .strip_prefix("0x")
        .ok_or(CircleError::InvalidEvidence)?;
    if h.len() > maximum * 2 || h.len() % 2 != 0 {
        return Err(CircleError::InvalidEvidence);
    }
    alloy_primitives::hex::decode(h).map_err(|_| CircleError::InvalidEvidence)
}
fn hash(value: &str) -> Result<B256, CircleError> {
    B256::try_from(bytes(value, 32)?.as_slice()).map_err(|_| CircleError::InvalidEvidence)
}
/// What a record binds: keccak256(uint256 chainId ‖ to ‖ data). Only `data`
/// varies in length, so two calls never share these bytes.
fn digest(call: &WalletCall) -> B256 {
    let chain = U256::from(call.chain_id()).to_be_bytes::<32>();
    keccak256([&chain[..], call.to().as_slice(), call.data()].concat())
}

#[derive(Debug)]
pub(crate) struct CommandOutput {
    pub success: bool,
    pub stdout: Vec<u8>,
}
#[tonic::async_trait]
pub(crate) trait CircleCommand: Send + Sync {
    async fn run(
        &self,
        arguments: Vec<String>,
        mutating: bool,
    ) -> Result<CommandOutput, CircleError>;
}
struct CliCommand(CircleOptions);

// Drain both pipes concurrently, retaining only bounded bytes. An overflow from a
// mutating command never kills its custodian process or permits a replacement.
fn drain(mut pipe: impl Read, limit: usize) -> Result<Vec<u8>, CircleError> {
    let mut output = Vec::new();
    let mut overflow = false;
    let mut chunk = [0; 8192];
    loop {
        let count = pipe
            .read(&mut chunk)
            .map_err(|_| CircleError::Unavailable)?;
        if count == 0 {
            break;
        }
        if output.len().saturating_add(count) <= limit && !overflow {
            output.extend_from_slice(&chunk[..count]);
        } else {
            overflow = true;
        }
    }
    if overflow {
        Err(CircleError::Unavailable)
    } else {
        Ok(output)
    }
}
fn run_process(
    options: CircleOptions,
    args: Vec<String>,
    mutating: bool,
) -> Result<CommandOutput, CircleError> {
    let mut command = Command::new(&options.command[0]);
    command
        .args(&options.command[1..])
        .args(args)
        .env("DO_NOT_TRACK", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn().map_err(|_| CircleError::Unavailable)?;
    let stdout = child.stdout.take().ok_or(CircleError::Unavailable)?;
    let stderr = child.stderr.take().ok_or(CircleError::Unavailable)?;
    let out = std::thread::spawn(move || drain(stdout, MAX_OUTPUT));
    let err = std::thread::spawn(move || drain(stderr, MAX_STDERR));
    let deadline = Instant::now() + Duration::from_millis(options.read_timeout_ms);
    let mut timed_out = false;
    let mut status = None;
    while status.is_none() || !out.is_finished() || !err.is_finished() {
        if status.is_none() {
            status = child.try_wait().map_err(|_| CircleError::Unavailable)?;
        }
        if !mutating && Instant::now() >= deadline {
            timed_out = true;
            #[cfg(unix)]
            // SAFETY: this invocation created its own process group with its PID;
            // only read-only CLI invocations are terminated. No shared group.
            unsafe {
                libc::kill(-(child.id() as i32), libc::SIGKILL);
            }
            #[cfg(not(unix))]
            {
                let _ = child.kill();
            }
            let _ = child.wait();
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let stdout = out.join().map_err(|_| CircleError::Unavailable)??;
    let _ = err.join().map_err(|_| CircleError::Unavailable)??;
    if timed_out {
        return Err(CircleError::Unavailable);
    }
    Ok(CommandOutput {
        success: status.is_some_and(|s| s.success()),
        stdout,
    })
}
#[tonic::async_trait]
impl CircleCommand for CliCommand {
    async fn run(
        &self,
        arguments: Vec<String>,
        mutating: bool,
    ) -> Result<CommandOutput, CircleError> {
        let options = self.0.clone();
        // spawn_blocking work is not aborted when its awaiting future is dropped:
        // https://docs.rs/tokio/1.53.1/tokio/task/fn.spawn_blocking.html
        tokio::task::spawn_blocking(move || run_process(options, arguments, mutating))
            .await
            .map_err(|_| CircleError::Unavailable)?
    }
}

/// The whole call, as a record kept it before it kept only the digest.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedCall {
    chain_id: u64,
    to: String,
    data: String,
}
impl RetainedCall {
    fn call(&self) -> Result<WalletCall, CircleError> {
        WalletCall::new(
            self.chain_id,
            parse_address(&self.to).map_err(|_| CircleError::Journal)?,
            bytes(&self.data, MAX_CALL)?,
        )
        .map_err(|_| CircleError::Journal)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retained {
    route: String,
    call_digest: String,
    idempotency_key: String,
    wallet_operation_id: Option<String>,
    tx_hash: Option<String>,
    state: String,
    /// When `retire` marked it, in Unix seconds: from then on, once it holds a
    /// transaction hash, the next save forgets it. A reconcile clears it.
    retire_at: Option<u64>,
}
impl Retained {
    /// Whether a save made at Unix second `now` forgets it.
    fn due(&self, now: u64) -> bool {
        self.tx_hash.is_some() && self.retire_at.is_some_and(|at| at <= now)
    }
    fn result(&self, reference: &str, unknown: bool) -> Result<OperationResult, CircleError> {
        let state = if unknown
            || self.state == "unknown"
            || self.state == "failed" && self.tx_hash.is_none()
        {
            // A hashless failure after dispatch never proves no submission.
            OperationState::Unknown
        } else if self.state == "failed" {
            OperationState::Failed
        } else {
            OperationState::Pending
        };
        OperationResult::new(
            reference,
            state,
            self.wallet_operation_id.clone(),
            self.tx_hash.as_deref().map(hash).transpose()?,
            None,
            vec![],
        )
        .map_err(|_| CircleError::Journal)
    }
    /// Bytes this record may still grow by: the transaction id and hash Circle
    /// has yet to report replace `null` (a 36-character id, a 66-character hash),
    /// and so may a retirement mark (at most 20 digits). A later mark only
    /// lowers it, which never adds a digit; once a reconcile clears it, a new
    /// mark of as many digits (every second from 2001 to 2286 has ten) brings
    /// the record back to its size.
    fn growth(&self) -> usize {
        usize::from(self.wallet_operation_id.is_none()) * 34
            + usize::from(self.tx_hash.is_none()) * 64
            + usize::from(self.retire_at.is_none()) * 16
    }
    /// The retained handles after `failure` left this operation unresolved.
    fn unknown(&self, reference: &str, failure: Failure) -> Result<OperationResult, CircleError> {
        self.result(reference, true)?
            .with_cause(failure.cause())
            .map_err(|_| CircleError::Journal)
    }
}

/// What an execute found or did while it held the journal's lock.
enum Intent {
    /// The reference already binds this call: it is only reconciled.
    Bound,
    /// No check began recently enough: check, then read the journal again.
    Unchecked,
    /// Refused before the save: nothing was sent.
    Refused(Failure),
    /// The save failed, perhaps once it replaced the journal; the record's key.
    Unsaved(Failure, String),
    /// Saved, with whether it is the direct registration: Circle may be asked.
    Saved(Retained, bool),
}

/// Native TransactionWallet using the authenticated Circle Agent CLI 1.1.4.
/// Keep one original operation file per wallet/profile, including across native
/// onboarding and Python runtime use. Constructors never provision a wallet.
#[derive(Clone)]
pub struct CircleAgentWallet {
    chain: Arc<ChainReader>,
    address: Address,
    path: PathBuf,
    options: CircleOptions,
    command: Arc<dyn CircleCommand>,
    // One operation at a time for this wallet and its clones, CLI commands
    // included, so no command fails unseen between the start of a check and
    // the record that check sets. Another process sharing the journal is kept
    // apart only by the journal's file lock, which each operation holds just
    // to read and save it.
    lock: Arc<Mutex<()>>,
    // When the last passing check of the CLI's version and its selection of
    // this wallet began, on the monotonic clock, shared by clones; written
    // under `lock`. Signing, which submits nothing, relies on it whatever its
    // age; an execute checks again once it is CHECK_AGE old. A check clears it
    // first, and any failed command or refused output clears it.
    checked: Arc<StdMutex<Option<Instant>>>,
    clock: Arc<dyn Clock>,
    // Whether the journal was at least three quarters of its bound when last
    // read or written, shared by clones (`near_capacity`).
    full: Arc<AtomicBool>,
    // What Circle reported for each reference whose record step failed,
    // shared by clones; written under `lock`. The next journal step merges it
    // into the journal first.
    unrecorded: Arc<StdMutex<BTreeMap<String, Retained>>>,
    require_existing: bool,
}
impl fmt::Debug for CircleAgentWallet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CircleAgentWallet")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}
impl CircleAgentWallet {
    pub fn new(
        chain: Arc<ChainReader>,
        address: Address,
        path: impl AsRef<Path>,
        options: CircleOptions,
    ) -> Result<Self, CircleError> {
        Self::construct(chain, address, path.as_ref(), options, false)
    }
    pub fn open_existing(
        chain: Arc<ChainReader>,
        address: Address,
        path: impl AsRef<Path>,
        options: CircleOptions,
    ) -> Result<Self, CircleError> {
        Self::construct(chain, address, path.as_ref(), options, true)
    }
    fn construct(
        chain: Arc<ChainReader>,
        address: Address,
        path: &Path,
        options: CircleOptions,
        require_existing: bool,
    ) -> Result<Self, CircleError> {
        let command = Arc::new(CliCommand(options.clone()));
        Self::with_command(chain, address, path, options, require_existing, command)
    }
    pub(crate) fn with_command(
        chain: Arc<ChainReader>,
        address: Address,
        path: &Path,
        options: CircleOptions,
        require_existing: bool,
        command: Arc<dyn CircleCommand>,
    ) -> Result<Self, CircleError> {
        options.validate()?;
        blockchain(chain.profile().domain().chain_id())?;
        if address == Address::ZERO {
            return Err(CircleError::Configuration);
        }
        let mut result = Self {
            chain,
            address,
            path: path.to_owned(),
            options,
            command,
            lock: Arc::new(Mutex::new(())),
            checked: Arc::new(StdMutex::new(None)),
            clock: Arc::new(SystemClock),
            full: Arc::new(AtomicBool::new(false)),
            unrecorded: Arc::new(StdMutex::new(BTreeMap::new())),
            require_existing,
        };
        let mut store = result.store(LOCK_WAIT)?;
        result.path = store.path().to_owned();
        let mut operations = result.load(&store)?;
        if store.read().map_err(|_| CircleError::Journal)?.is_none() {
            result.save(&mut store, &mut operations, 0)?;
        }
        Ok(result)
    }
    /// This wallet reading the time from `clock`, before any clone is made.
    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
    pub fn address(&self) -> Address {
        self.address
    }
    fn scope(&self) -> Value {
        json!({"version":2,"kind":"mppi-circle-agent-wallet","profile":profile_to_mapping(self.chain.profile()),
            "wallet":encoded(self.address)})
    }
    /// This wallet and profile's record. Version 1 also retained the account's
    /// code pins, which no longer select anything; the next save drops them.
    fn owns(&self, value: &Value) -> bool {
        let scope = self.scope();
        if value["version"] != 1 {
            return *value == scope;
        }
        value
            .as_object()
            .is_some_and(|fields| fields.len() == 5 && fields.contains_key("implementation"))
            && value["kind"] == scope["kind"]
            && value["profile"] == scope["profile"]
            && value["wallet"].is_object()
            && value["wallet"]["address"] == scope["wallet"]
    }
    /// The journal, opened once its lock is free, waiting up to `wait`.
    fn store(&self, wait: Duration) -> Result<LocalState, CircleError> {
        if self.require_existing {
            let mut lock = self.path.as_os_str().to_os_string();
            lock.push(".lock");
            for path in [&self.path, &PathBuf::from(lock)] {
                if !std::fs::symlink_metadata(path).is_ok_and(|m| m.is_file()) {
                    return Err(CircleError::Journal);
                }
            }
        }
        LocalState::open_within(&self.path, MAX_JOURNAL, wait).map_err(|_| CircleError::Journal)
    }
    /// How long an execute or reconcile waits for the journal's lock: its CLI
    /// timeout. Other steps wait as other local records do, a second.
    fn timeout(&self) -> Duration {
        Duration::from_millis(self.options.read_timeout_ms)
    }
    fn registration_call(&self) -> Result<WalletCall, CircleError> {
        RegistryCalls::new(
            self.chain.profile().domain().chain_id(),
            self.chain.profile().registry_address(),
        )
        .and_then(|calls| calls.register(self.address(), None))
        .map(|p| p.call().clone())
        .map_err(|_| CircleError::Configuration)
    }
    fn validate_call(&self, call: &WalletCall) -> Result<(), CircleError> {
        if call.chain_id() != self.chain.profile().domain().chain_id()
            || call.data().len() > MAX_CALL
        {
            Err(CircleError::Configuration)
        } else {
            Ok(())
        }
    }
    fn load(&self, store: &LocalState) -> Result<BTreeMap<String, Retained>, CircleError> {
        let Some(mut value) = store.read().map_err(|_| CircleError::Journal)? else {
            return if self.require_existing {
                Err(CircleError::Journal)
            } else {
                Ok(BTreeMap::new())
            };
        };
        if let Ok(metadata) = std::fs::metadata(store.path()) {
            self.measure(metadata.len());
        }
        let mut operations = value
            .as_object_mut()
            .ok_or(CircleError::Journal)?
            .remove("operations")
            .ok_or(CircleError::Journal)?;
        if !self.owns(&value) {
            return Err(CircleError::Journal);
        }
        for row in operations
            .as_object_mut()
            .ok_or(CircleError::Journal)?
            .values_mut()
        {
            let fields = row.as_object_mut().ok_or(CircleError::Journal)?;
            // An earlier release kept the whole call, and before that no route:
            // only self execution. Its digest binds it now, unmarked; the next
            // save writes the compact record, which an earlier release refuses.
            if let Some(call) = fields.remove("call") {
                let call = serde_json::from_value::<RetainedCall>(call)
                    .map_err(|_| CircleError::Journal)?
                    .call()?;
                self.validate_call(&call)?;
                if fields
                    .insert("call_digest".into(), json!(encoded(digest(&call))))
                    .is_some()
                    || fields.insert("retire_at".into(), Value::Null).is_some()
                {
                    return Err(CircleError::Journal);
                }
                fields.entry("route").or_insert_with(|| json!("self"));
            }
            // serde Option accepts omitted fields; reject them before typed decoding.
            for name in [
                "route",
                "call_digest",
                "idempotency_key",
                "wallet_operation_id",
                "tx_hash",
                "state",
                "retire_at",
            ] {
                if !fields.contains_key(name) {
                    return Err(CircleError::Journal);
                }
            }
        }
        let operations: BTreeMap<String, Retained> =
            serde_json::from_value(operations).map_err(|_| CircleError::Journal)?;
        let mut keys = HashSet::new();
        let mut ids = HashSet::new();
        for (reference_, row) in &operations {
            reference(reference_)?;
            let bound = hash(&row.call_digest)?;
            if !(row.route == "self"
                || row.route == "direct_register" && bound == digest(&self.registration_call()?))
                || !["unknown", "pending", "failed"].contains(&row.state.as_str())
            {
                return Err(CircleError::Journal);
            }
            uuid(&row.idempotency_key, true)?;
            if !keys.insert(&row.idempotency_key) {
                return Err(CircleError::Journal);
            }
            if let Some(id) = &row.wallet_operation_id {
                uuid(id, false)?;
                if !ids.insert(id) {
                    return Err(CircleError::Journal);
                }
            }
            if let Some(tx) = &row.tx_hash {
                hash(tx)?;
                if row.wallet_operation_id.is_none() {
                    return Err(CircleError::Journal);
                }
            }
        }
        Ok(operations)
    }
    /// Replace the journal, first forgetting every record due to retire;
    /// refused if it would leave less than `reserve` bytes of its bound.
    fn save(
        &self,
        store: &mut LocalState,
        operations: &mut BTreeMap<String, Retained>,
        reserve: usize,
    ) -> Result<(), CircleError> {
        self.forget_due(operations);
        let mut value = self.scope();
        value["operations"] = serde_json::to_value(operations).map_err(|_| CircleError::Journal)?;
        let size = serde_json::to_vec(&value)
            .map_err(|_| CircleError::Journal)?
            .len();
        if size + reserve > MAX_JOURNAL {
            return Err(CircleError::Capacity);
        }
        store.write(&value).map_err(|_| CircleError::Journal)?;
        self.measure(size as u64);
        // Every journal step merges each kept answer into the journal it
        // saves, so this save wrote them all.
        self.unrecorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        Ok(())
    }
    fn measure(&self, size: u64) {
        self.full
            .store(size >= (MAX_JOURNAL / 4 * 3) as u64, Ordering::Relaxed);
    }
    /// Forget each record due to retire by the wall clock's current second
    /// that holds a transaction hash; whether any was. A record without one
    /// keeps its mark, since its call may still reach Circle.
    fn forget_due(&self, operations: &mut BTreeMap<String, Retained>) -> bool {
        let Some(now) = unix_seconds(self.clock.wall()) else {
            return false;
        };
        let before = operations.len();
        operations.retain(|_, row| !row.due(now));
        operations.len() < before
    }
    /// Mark each record of `references` to be forgotten from `at`, keeping an
    /// earlier mark, in one atomic replacement of the journal. That save, like
    /// every save, forgets each record that is due and holds a transaction
    /// hash; any other keeps its mark, since its call may have reached Circle.
    /// A reference with no record is skipped, as is one no record can have
    /// (empty or over 4,096 bytes). A time before the Unix epoch is refused.
    /// A reconcile of a reference withdraws its mark.
    async fn retire_inner(&self, references: &[String], at: SystemTime) -> Result<(), CircleError> {
        let at = unix_seconds(at).ok_or(CircleError::Configuration)?;
        let references = references.to_vec();
        let _lock = self.lock.lock().await;
        self.journal(LOCK_WAIT, move |wallet, store, operations| {
            let mut marked = false;
            for reference in &references {
                if let Some(row) = operations.get_mut(reference)
                    && row.retire_at.is_none_or(|due| at < due)
                {
                    row.retire_at = Some(at);
                    marked = true;
                }
            }
            if marked || wallet.forget_due(operations) {
                wallet.save(store, operations, 0)?;
            }
            Ok(())
        })
        .await
    }
    /// `step` with the journal open and loaded, on a blocking thread: opening
    /// waits up to `wait` for another holder, and reading and replacing the
    /// journal are file I/O, none of which may stall the async executor. What
    /// an earlier record step could not save is merged into the journal
    /// first, so the step's own save writes it; a step that saves nothing has
    /// it saved alone after. A failed save of it keeps it in memory and is
    /// never the step's failure. That save writes the journal the step
    /// leaves, which holds only what the step meant to save: an intent that
    /// did not save is taken back out (`save_intent`). The journal's lock is
    /// released when `step` returns.
    async fn journal<T: Send + 'static>(
        &self,
        wait: Duration,
        step: impl FnOnce(
            &Self,
            &mut LocalState,
            &mut BTreeMap<String, Retained>,
        ) -> Result<T, CircleError>
        + Send
        + 'static,
    ) -> Result<T, CircleError> {
        let wallet = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut store = wallet.store(wait)?;
            let mut operations = wallet.load(&store)?;
            wallet.merge_unrecorded(&mut operations);
            let result = step(&wallet, &mut store, &mut operations);
            // A save that lands clears them (`save`).
            let unsaved = !wallet
                .unrecorded
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty();
            if unsaved {
                let _ = wallet.save(&mut store, &mut operations, 0);
            }
            result
        })
        .await
        .map_err(|_| CircleError::Journal)?
    }
    /// Merge into the journal just read what Circle reported for each
    /// reference whose record step failed, before any other change. One the
    /// journal no longer holds the record for, or refuses, is dropped: the
    /// journal then holds its own evidence of it, and the record step's
    /// unknown result carried Circle's.
    fn merge_unrecorded(&self, operations: &mut BTreeMap<String, Retained>) {
        self.unrecorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|reference, evidence| self.merge(reference, evidence, operations).is_ok());
    }
    /// Whether a check passed since the last failed command and, `within`
    /// a given age, began less than that long ago.
    fn passed(&self, within: Option<Duration>) -> bool {
        let checked = *self.checked.lock().unwrap_or_else(PoisonError::into_inner);
        checked.is_some_and(|began| {
            within.is_none_or(|age| self.clock.monotonic().saturating_duration_since(began) < age)
        })
    }
    fn record_check(&self, began: Option<Instant>) {
        *self.checked.lock().unwrap_or_else(PoisonError::into_inner) = began;
    }
    /// A command's JSON `data` object. A command that fails, or prints
    /// anything else, clears the record of the last check.
    async fn json(&self, mut arguments: Vec<String>, mutating: bool) -> Result<Value, Failure> {
        arguments.extend(["--output".into(), "json".into()]);
        let result = async {
            let output = self.command.run(arguments, mutating).await?;
            let value = crate::json::parse_document(&output.stdout, MAX_OUTPUT, 32).ok();
            let code = value
                .as_ref()
                .and_then(|v| v["error"]["code"].as_str())
                .and_then(|code| CLI_ERRORS.into_iter().find(|listed| *listed == code));
            if matches!(code, Some("AUTH_REQUIRED" | "AUTH_EXPIRED")) {
                return Err(Failure {
                    error: CircleError::Authentication,
                    code,
                });
            }
            if !output.success {
                return Err(Failure {
                    error: CircleError::Unavailable,
                    code,
                });
            }
            value
                .as_ref()
                .and_then(|v| v.get("data"))
                .filter(|v| v.is_object())
                .cloned()
                .ok_or(CircleError::InvalidEvidence.into())
        }
        .await;
        if result.is_err() {
            self.record_check(None);
        }
        result
    }
    /// The CLI version and its selection of this agent wallet on this network,
    /// checked unless a check passed since the last failed command, whatever
    /// its age, for this wallet or its clones: after a command fails, or prints
    /// output the adapter refuses, the next call checks again. An execute
    /// checks again unless the last passing check began within 60 seconds.
    pub async fn verify_wallet(&self) -> Result<(), CircleError> {
        let _lock = self.lock.lock().await;
        self.verify().await.map_err(|failure| failure.error)
    }
    /// [`Self::check`] unless one passed since the last failed command; under
    /// `lock`.
    async fn verify(&self) -> Result<(), Failure> {
        if self.passed(None) {
            return Ok(());
        }
        self.check().await
    }
    /// The CLI version and its selection of this agent wallet, checked now;
    /// under `lock`. The record is cleared first and set, to when the check
    /// began, only once all of it passes.
    async fn check(&self) -> Result<(), Failure> {
        self.record_check(None);
        let began = self.clock.monotonic();
        let version = self.command.run(vec!["--version".into()], false).await?;
        if !version.success || version.stdout.trim_ascii() != b"1.1.4" {
            return Err(CircleError::Version.into());
        }
        let chain = blockchain(self.chain.profile().domain().chain_id())?;
        let result = self
            .json(
                vec![
                    "wallet".into(),
                    "list".into(),
                    "--type".into(),
                    "agent".into(),
                    "--chain".into(),
                    chain.into(),
                ],
                false,
            )
            .await?;
        let wallets = result["wallets"]
            .as_array()
            .filter(|v| v.len() <= 50)
            .ok_or(CircleError::InvalidEvidence)?;
        let matches: Vec<_> = wallets
            .iter()
            .filter(|row| address(row, "address").ok() == Some(self.address()))
            .collect();
        if matches.len() != 1 || matches[0]["type"] != "agent" || matches[0]["blockchain"] != chain
        {
            return Err(CircleError::InvalidEvidence.into());
        }
        self.record_check(Some(began));
        Ok(())
    }
    /// verify_wallet for a wallet explicitly opted in to registration activation.
    pub async fn verify_registration_wallet(&self) -> Result<(), CircleError> {
        if !self.options.allow_registration_activation {
            return Err(CircleError::Configuration);
        }
        self.verify_wallet().await
    }
    /// Sign exact caller-supplied EIP-712 data for this selected chain, preserving
    /// Circle's complete ERC-1271 proof bytes. Never used for MPPI session vouchers.
    pub async fn sign_typed_data(&self, typed_data: &Value) -> Result<Vec<u8>, CircleError> {
        if typed_data["domain"]["chainId"].as_u64()
            != Some(self.chain.profile().domain().chain_id())
        {
            return Err(CircleError::Configuration);
        }
        let raw = serde_json::to_string(typed_data).map_err(|_| CircleError::Configuration)?;
        if raw.len() > MAX_CALL {
            return Err(CircleError::Configuration);
        }
        let _lock = self.lock.lock().await;
        // The journal must still be this wallet's; its lock is not held while
        // Circle signs.
        self.journal(LOCK_WAIT, |_, _, _| Ok(())).await?;
        self.verify().await.map_err(|failure| failure.error)?;
        let value = self
            .json(
                vec![
                    "wallet".into(),
                    "sign".into(),
                    "typed-data".into(),
                    raw,
                    "--address".into(),
                    encoded(self.address()),
                    "--chain".into(),
                    blockchain(self.chain.profile().domain().chain_id())?.into(),
                ],
                true,
            )
            .await
            .map_err(|failure| failure.error)?;
        let signature = bytes(text(&value, "signature")?, MAX_STDERR)?;
        if signature.is_empty() {
            return Err(CircleError::InvalidEvidence);
        }
        Ok(signature)
    }
    /// What Circle reported for `original`'s call, checked against that
    /// record without the journal: the record with Circle's transaction id,
    /// hash and state.
    fn evidence(
        &self,
        original: &Retained,
        row: &Value,
        from_execute: bool,
    ) -> Result<Retained, CircleError> {
        let id = text(row, "id")?;
        uuid(id, false)?;
        if original
            .wallet_operation_id
            .as_ref()
            .is_some_and(|old| old != id)
            || from_execute && text(row, "idempotencyKey")? != original.idempotency_key
            || !from_execute && original.wallet_operation_id.is_none()
        {
            return Err(CircleError::InvalidEvidence);
        }
        let bound = hash(&original.call_digest)?;
        let chain = self.chain.profile().domain().chain_id();
        let direct = original.route == "direct_register";
        let (exact_call, destination) = if direct {
            let registration = self.registration_call()?;
            (
                bound == digest(&registration)
                    && row["abiFunctionSignature"] == "register()"
                    && (row["abiParameters"].is_null()
                        || row["abiParameters"].as_array().is_some_and(Vec::is_empty)),
                registration.to(),
            )
        } else {
            (
                row["abiFunctionSignature"] == EXECUTE
                    && row["abiParameters"].as_array().is_some_and(|p| {
                        p.len() == 3
                            && p[1] == "0"
                            && p[0]
                                .as_str()
                                .and_then(|s| parse_address(s).ok())
                                .zip(p[2].as_str().and_then(|s| bytes(s, MAX_CALL).ok()))
                                .and_then(|(to, data)| WalletCall::new(chain, to, data).ok())
                                .is_some_and(|call| digest(&call) == bound)
                    }),
                self.address(),
            )
        };
        if !exact_call
            || row["blockchain"] != blockchain(chain)?
            || row["operation"] != "CONTRACT_EXECUTION"
            || row["transactionType"] != "OUTBOUND"
            || address(row, "sourceAddress")? != self.address()
            || address(row, "contractAddress")? != destination
            || !row["destinationAddress"].is_null()
                && address(row, "destinationAddress")? != destination
        {
            return Err(CircleError::InvalidEvidence);
        }
        let tx = match row.get("txHash") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) if s.is_empty() => None,
            Some(Value::String(s)) => Some(hash(s)?),
            _ => return Err(CircleError::InvalidEvidence),
        };
        let previous = original.tx_hash.as_deref().map(hash).transpose()?;
        if previous.is_some() && tx.is_some() && previous != tx {
            return Err(CircleError::InvalidEvidence);
        }
        let state = match text(row, "state")? {
            "INITIATED" | "CLEARED" | "QUEUED" | "SENT" | "STUCK" | "CONFIRMED" | "COMPLETE" => {
                "pending"
            }
            "FAILED" | "CANCELLED" | "DENIED" => "failed",
            _ => return Err(CircleError::InvalidEvidence),
        };
        Ok(Retained {
            wallet_operation_id: Some(id.into()),
            tx_hash: tx.or(previous).map(encoded),
            state: state.into(),
            ..original.clone()
        })
    }
    /// Merge `evidence` into the reference's record in the journal just read
    /// under its lock: only that record changes. Refused if the journal no
    /// longer holds the record (`Journal`), or holds other evidence for it or
    /// gives its transaction id to another record (`InvalidEvidence`).
    fn merge(
        &self,
        reference: &str,
        evidence: &Retained,
        operations: &mut BTreeMap<String, Retained>,
    ) -> Result<(), CircleError> {
        let id = evidence
            .wallet_operation_id
            .as_deref()
            .ok_or(CircleError::Journal)?;
        let tx = evidence.tx_hash.as_deref().map(hash).transpose()?;
        if operations
            .iter()
            .any(|(key, row)| key != reference && row.wallet_operation_id.as_deref() == Some(id))
        {
            return Err(CircleError::InvalidEvidence);
        }
        let retained = operations.get_mut(reference).ok_or(CircleError::Journal)?;
        let previous = retained.tx_hash.as_deref().map(hash).transpose()?;
        if retained.idempotency_key != evidence.idempotency_key
            || retained
                .wallet_operation_id
                .as_deref()
                .is_some_and(|old| old != id)
            || previous.is_some() && tx.is_some() && previous != tx
        {
            return Err(CircleError::InvalidEvidence);
        }
        retained.wallet_operation_id = Some(id.into());
        retained.tx_hash = tx.or(previous).map(encoded);
        retained.state = evidence.state.clone();
        Ok(())
    }
    /// Record what Circle reported for `original`, the reference's record as
    /// this operation read it: checked against it, then merged into the
    /// journal read again under its lock. If that step fails (the lock not
    /// taken within the timeout, a journal that cannot be read or validated,
    /// a record no longer there, a merge the journal refuses, a failed save),
    /// the result is unknown with Circle's transaction id and hash, kept to be
    /// written at the next journal step.
    async fn record(
        &self,
        reference: &str,
        original: &Retained,
        row: &Value,
        from_execute: bool,
    ) -> Result<OperationResult, CircleError> {
        let evidence = self.evidence(original, row, from_execute)?;
        let (owned, merged) = (reference.to_owned(), evidence.clone());
        let recorded = self
            .journal(self.timeout(), move |wallet, store, operations| {
                wallet.merge(&owned, &merged, operations)?;
                // Before the save, which forgets this record if it is due to retire.
                let result = operations[&owned].result(&owned, false)?;
                wallet.save(store, operations, 0)?;
                Ok(result)
            })
            .await;
        recorded.or_else(|error| {
            self.unrecorded
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(reference.to_owned(), evidence.clone());
            evidence.unknown(reference, error.into())
        })
    }
    /// The reference's record and what Circle's history shows of it; under
    /// `lock`. The journal's lock is released while the history is listed.
    /// A reference looked up again may be presented again, so its retirement
    /// mark is withdrawn first: that only keeps the record longer.
    async fn reconcile_inner(&self, reference: &str) -> Result<OperationResult, CircleError> {
        let key = reference.to_owned();
        let retained = self
            .journal(self.timeout(), move |wallet, store, operations| {
                let Some(row) = operations.get_mut(&key) else {
                    return Ok(None);
                };
                let marked = row.retire_at.take().is_some();
                let row = row.clone();
                if marked {
                    wallet.save(store, operations, 0)?;
                }
                Ok(Some(row))
            })
            .await?;
        let Some(retained) = retained else {
            return OperationResult::new(
                reference,
                OperationState::Unknown,
                None,
                None,
                None,
                vec![],
            )
            .map_err(|_| CircleError::Journal);
        };
        let Some(id) = retained.wallet_operation_id.clone() else {
            return retained.result(reference, true);
        };
        let result: Result<OperationResult, Failure> = async {
            let mut cursor: Option<String> = None;
            let mut seen = HashSet::new();
            for _ in 0..self.options.max_pages {
                let mut args = vec![
                    "transaction".into(),
                    "list".into(),
                    "--address".into(),
                    encoded(self.address()),
                    "--chain".into(),
                    blockchain(self.chain.profile().domain().chain_id())?.into(),
                    "--operation".into(),
                    "execute".into(),
                    "--tx-type".into(),
                    "outbound".into(),
                    "--limit".into(),
                    "50".into(),
                ];
                if let Some(cursor) = &cursor {
                    args.extend(["--cursor".into(), cursor.clone()]);
                }
                let result = self.json(args, false).await?;
                let page = result["transactions"]
                    .as_array()
                    .filter(|v| v.len() <= 50)
                    .ok_or(CircleError::InvalidEvidence)?;
                let mut found = None;
                for row in page {
                    let candidate = text(row, "id")?;
                    uuid(candidate, false)?;
                    if !seen.insert(candidate.to_owned()) {
                        return Err(CircleError::InvalidEvidence.into());
                    }
                    cursor = Some(candidate.to_owned());
                    if candidate == id {
                        found = Some(row);
                    }
                }
                if let Some(row) = found {
                    return Ok(self.record(reference, &retained, row, false).await?);
                }
                if page.len() < 50 {
                    break;
                }
            }
            Err(CircleError::Unavailable.into())
        }
        .await;
        result.or_else(|failure| retained.unknown(reference, failure))
    }
    /// Under the journal's lock: whether the reference already binds `call`,
    /// or else this call's intent saved, or why not. A reference bound to
    /// another call is refused as reused.
    fn save_intent(
        &self,
        call: &WalletCall,
        reference: &str,
        checked: bool,
        store: &mut LocalState,
        operations: &mut BTreeMap<String, Retained>,
    ) -> Result<Intent, CircleError> {
        if let Some(row) = operations.get(reference) {
            return if hash(&row.call_digest)? == digest(call) {
                Ok(Intent::Bound)
            } else {
                Err(CircleError::InvalidEvidence)
            };
        }
        // The journal holds no record of this reference, and Circle is asked
        // only after one is saved: every refusal until then is definite.
        let direct = match self.validate_call(call).and_then(|()| {
            Ok(self.options.allow_registration_activation && *call == self.registration_call()?)
        }) {
            Ok(direct) => direct,
            Err(error) => return Ok(Intent::Refused(error.into())),
        };
        // Execute moves money: unless a check began within CHECK_AGE, check
        // first, so a login lost since fails before this save, never sent.
        if !checked && !self.passed(Some(CHECK_AGE)) {
            return Ok(Intent::Unchecked);
        }
        let row = Retained {
            route: if direct { "direct_register" } else { "self" }.into(),
            call_digest: encoded(digest(call)),
            idempotency_key: match fresh_key() {
                Ok(key) => key,
                Err(error) => return Ok(Intent::Refused(error.into())),
            },
            wallet_operation_id: None,
            tx_hash: None,
            state: "unknown".into(),
            retire_at: None,
        };
        operations.insert(reference.into(), row.clone());
        // It leaves room for what every record may still gain, so recording
        // what Circle reports, or a retirement mark, never fails for space.
        let reserve = operations.values().map(Retained::growth).sum();
        Ok(match self.save(store, operations, reserve) {
            Ok(()) => Intent::Saved(row, direct),
            Err(error) => {
                // Out of the journal this step leaves, so no later save of
                // the step writes it.
                operations.remove(reference);
                Intent::Unsaved(error.into(), row.idempotency_key)
            }
        })
    }
    /// After an intent save failed, perhaps once it replaced the journal (the
    /// rename landed, then syncing the directory failed): the journal read
    /// again, without that intent, which was never sent. Only a journal shown
    /// free of the reference proves a definite failure; otherwise unknown.
    async fn unsaved(
        &self,
        reference: &str,
        key: String,
        failure: Failure,
    ) -> Result<OperationResult, CircleError> {
        let owned = reference.to_owned();
        let cleared = self
            .journal(self.timeout(), move |wallet, store, operations| {
                if operations
                    .get(&owned)
                    .is_some_and(|row| row.idempotency_key == key)
                {
                    operations.remove(&owned);
                    wallet.save(store, operations, 0)?;
                }
                Ok(!operations.contains_key(&owned))
            })
            .await;
        if cleared == Ok(true) {
            not_submitted(reference, failure)
        } else {
            unresolved(reference, failure)
        }
    }
    async fn execute_inner(
        &self,
        call: &WalletCall,
        reference_: &str,
    ) -> Result<OperationResult, CircleError> {
        // No journal can hold a reference that is empty or too long: never sent.
        if let Err(error) = reference(reference_) {
            return not_submitted(reference_, error.into());
        }
        let _lock = self.lock.lock().await;
        let mut checked = false;
        // The journal's lock is held only to read it and save the intent: the
        // check runs without it, and the journal is read again after.
        let (row, direct) = loop {
            let (call, reference) = (call.clone(), reference_.to_owned());
            let intent = self
                .journal(self.timeout(), move |wallet, store, operations| {
                    wallet.save_intent(&call, &reference, checked, store, operations)
                })
                .await?;
            match intent {
                Intent::Bound => return self.reconcile_inner(reference_).await,
                Intent::Unchecked => {
                    if let Err(failure) = self.check().await {
                        return not_submitted(reference_, failure);
                    }
                    checked = true;
                }
                Intent::Refused(failure) => return not_submitted(reference_, failure),
                Intent::Unsaved(failure, key) => {
                    return self.unsaved(reference_, key, failure).await;
                }
                Intent::Saved(row, direct) => break (row, direct),
            }
        };
        let key = row.idempotency_key.clone();
        let mut args = vec!["wallet".into(), "execute".into()];
        if direct {
            args.extend(["register()".into(), "--amount".into(), "0".into()]);
        } else {
            args.extend([
                EXECUTE.into(),
                encoded(call.to()),
                "0".into(),
                encoded(call.data()),
            ]);
        }
        args.extend([
            "--contract".into(),
            encoded(if direct { call.to() } else { self.address() }),
            "--address".into(),
            encoded(self.address()),
            "--chain".into(),
            blockchain(call.chain_id())?.into(),
            "--idempotency-key".into(),
            key,
        ]);
        // Without the journal's lock: another process may save and record its
        // own operations meanwhile.
        let result = match self.json(args, true).await {
            Ok(transaction) => self
                .record(reference_, &row, &transaction, true)
                .await
                .map_err(Failure::from),
            Err(failure) => Err(failure),
        };
        result.or_else(|failure| row.unknown(reference_, failure))
    }
}
#[tonic::async_trait]
impl TransactionWallet for CircleAgentWallet {
    fn address(&self) -> Address {
        self.address()
    }
    async fn execute(
        &self,
        call: &WalletCall,
        reference: &str,
    ) -> Result<OperationResult, WalletError> {
        // Keep the lock and original returned handles alive if a caller drops its
        // future. The detached task finishes the original invocation; no retry.
        let wallet = self.clone();
        let call = call.clone();
        let reference = reference.to_owned();
        tokio::spawn(async move { wallet.execute_inner(&call, &reference).await })
            .await
            .map_err(|_| WalletError)?
            .map_err(|_| WalletError)
    }
    async fn reconcile(&self, reference_: &str) -> Result<OperationResult, WalletError> {
        reference(reference_).map_err(|_| WalletError)?;
        let _lock = self.lock.lock().await;
        self.reconcile_inner(reference_)
            .await
            .map_err(|_| WalletError)
    }
    async fn retire(&self, operation_refs: &[String], at: SystemTime) -> Result<(), WalletError> {
        self.retire_inner(operation_refs, at)
            .await
            .map_err(|_| WalletError)
    }
    fn near_capacity(&self) -> bool {
        self.full.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
#[path = "circle_tests.rs"]
pub(crate) mod tests;
