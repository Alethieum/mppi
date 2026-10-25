//! Circle Agent Wallet CLI 1.1.4, with retained exact original operations.
//!
//! Login and custody belong to the official CLI. No API key or entity secret is
//! accepted. CLI status is never canonical chain confirmation. Once dispatched,
//! an intent is only reconciled by its returned transaction ID, never resubmitted.
//! Any Circle account generation is accepted: no account code is pinned.
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
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::{
    capabilities::{TransactionWallet, WalletError},
    chain::{ChainReader, encoded},
    deployment::profile_to_mapping,
    discovery::parse_address,
    local_state::LocalState,
    registry::RegistryCalls,
    types::{OperationResult, OperationState, WalletCall},
};

const MAX_OUTPUT: usize = 1 << 20;
const MAX_STDERR: usize = 64 << 10;
const MAX_CALL: usize = 60 << 10;
const MAX_OPERATIONS: usize = 4096;
const MAX_JOURNAL: usize = 16 << 20;
const EXECUTE: &str = "execute(address,uint256,bytes)";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// Trusted CLI executable prefix. Never shell text or credentials. Read commands
/// have a deadline; execute/sign are drained to completion without a timeout kill.
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedCall {
    chain_id: u64,
    to: String,
    data: String,
}
impl RetainedCall {
    fn from_call(call: &WalletCall) -> Self {
        Self {
            chain_id: call.chain_id(),
            to: encoded(call.to()),
            data: encoded(call.data()),
        }
    }
    fn call(&self) -> Result<WalletCall, CircleError> {
        WalletCall::new(
            self.chain_id,
            parse_address(&self.to).map_err(|_| CircleError::Journal)?,
            bytes(&self.data, MAX_CALL)?,
        )
        .map_err(|_| CircleError::Journal)
    }
}
fn self_route() -> String {
    "self".into()
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retained {
    #[serde(default = "self_route")]
    route: String,
    call: RetainedCall,
    idempotency_key: String,
    wallet_operation_id: Option<String>,
    tx_hash: Option<String>,
    state: String,
}
impl Retained {
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
    lock: Arc<Mutex<()>>,
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
            require_existing,
        };
        let mut store = result.store()?;
        result.path = store.path().to_owned();
        let operations = result.load(&store)?;
        if store.read().map_err(|_| CircleError::Journal)?.is_none() {
            result.save(&mut store, &operations)?;
        }
        Ok(result)
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
    fn store(&self) -> Result<LocalState, CircleError> {
        if self.require_existing {
            let mut lock = self.path.as_os_str().to_os_string();
            lock.push(".lock");
            for path in [&self.path, &PathBuf::from(lock)] {
                if !std::fs::symlink_metadata(path).is_ok_and(|m| m.is_file()) {
                    return Err(CircleError::Journal);
                }
            }
        }
        LocalState::open(&self.path, MAX_JOURNAL).map_err(|_| CircleError::Journal)
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
        let operations = value
            .as_object_mut()
            .ok_or(CircleError::Journal)?
            .remove("operations")
            .ok_or(CircleError::Journal)?;
        if !self.owns(&value) {
            return Err(CircleError::Journal);
        }
        // serde Option accepts omitted fields; reject them before typed decoding.
        let rows = operations.as_object().ok_or(CircleError::Journal)?;
        if rows.len() > MAX_OPERATIONS {
            return Err(CircleError::Capacity);
        }
        for row in rows.values() {
            let fields = row.as_object().ok_or(CircleError::Journal)?;
            for name in [
                "call",
                "idempotency_key",
                "wallet_operation_id",
                "tx_hash",
                "state",
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
            let call = row.call.call()?;
            self.validate_call(&call)?;
            if !(row.route == "self"
                || row.route == "direct_register" && call == self.registration_call()?)
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
    fn save(
        &self,
        store: &mut LocalState,
        operations: &BTreeMap<String, Retained>,
    ) -> Result<(), CircleError> {
        let mut value = self.scope();
        value["operations"] = serde_json::to_value(operations).map_err(|_| CircleError::Journal)?;
        if serde_json::to_vec(&value)
            .map_err(|_| CircleError::Journal)?
            .len()
            > MAX_JOURNAL
        {
            return Err(CircleError::Capacity);
        }
        store.write(&value).map_err(|_| CircleError::Journal)
    }
    async fn json(&self, mut arguments: Vec<String>, mutating: bool) -> Result<Value, CircleError> {
        arguments.extend(["--output".into(), "json".into()]);
        let output = self.command.run(arguments, mutating).await?;
        let value = crate::json::parse_document(&output.stdout, MAX_OUTPUT, 32)
            .map_err(|_| CircleError::InvalidEvidence)?;
        if matches!(
            value["error"]["code"].as_str(),
            Some("AUTH_REQUIRED" | "AUTH_EXPIRED")
        ) {
            return Err(CircleError::Authentication);
        }
        if !output.success {
            return Err(CircleError::Unavailable);
        }
        value
            .get("data")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or(CircleError::InvalidEvidence)
    }
    /// The CLI version and its selection of this agent wallet on this network.
    pub async fn verify_wallet(&self) -> Result<(), CircleError> {
        let version = self.command.run(vec!["--version".into()], false).await?;
        if !version.success || version.stdout.trim_ascii() != b"1.1.4" {
            return Err(CircleError::Version);
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
            return Err(CircleError::InvalidEvidence);
        }
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
        let store = self.store()?;
        self.load(&store)?;
        self.verify_wallet().await?;
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
            .await?;
        let signature = bytes(text(&value, "signature")?, MAX_STDERR)?;
        if signature.is_empty() {
            return Err(CircleError::InvalidEvidence);
        }
        Ok(signature)
    }
    async fn observe(
        &self,
        reference: &str,
        row: &Value,
        from_execute: bool,
        operations: &mut BTreeMap<String, Retained>,
        store: &mut LocalState,
    ) -> Result<OperationResult, CircleError> {
        let original = operations
            .get(reference)
            .ok_or(CircleError::Journal)?
            .clone();
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
        let call = original.call.call()?;
        let direct = original.route == "direct_register";
        let destination = if direct { call.to() } else { self.address() };
        let exact_call = if direct {
            call == self.registration_call()?
                && row["abiFunctionSignature"] == "register()"
                && (row["abiParameters"].is_null()
                    || row["abiParameters"].as_array().is_some_and(Vec::is_empty))
        } else {
            row["abiFunctionSignature"] == EXECUTE
                && row["abiParameters"].as_array().is_some_and(|p| {
                    p.len() == 3
                        && p[0].as_str().and_then(|s| parse_address(s).ok()) == Some(call.to())
                        && p[1] == "0"
                        && p[2]
                            .as_str()
                            .and_then(|s| bytes(s, MAX_CALL).ok())
                            .as_deref()
                            == Some(call.data())
                })
        };
        if !exact_call
            || row["blockchain"] != blockchain(call.chain_id())?
            || row["operation"] != "CONTRACT_EXECUTION"
            || row["transactionType"] != "OUTBOUND"
            || address(row, "sourceAddress")? != self.address()
            || address(row, "contractAddress")? != destination
            || !row["destinationAddress"].is_null()
                && address(row, "destinationAddress")? != destination
            || operations.iter().any(|(key, value)| {
                key != reference && value.wallet_operation_id.as_deref() == Some(id)
            })
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
        let retained = operations.get_mut(reference).ok_or(CircleError::Journal)?;
        retained.wallet_operation_id = Some(id.into());
        retained.tx_hash = tx.or(previous).map(encoded);
        retained.state = state.into();
        self.save(store, operations)?;
        operations[reference].result(reference, false)
    }
    async fn reconcile_locked(
        &self,
        reference: &str,
        operations: &mut BTreeMap<String, Retained>,
        store: &mut LocalState,
    ) -> Result<OperationResult, CircleError> {
        let Some(retained) = operations.get(reference) else {
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
        let result = async {
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
                        return Err(CircleError::InvalidEvidence);
                    }
                    cursor = Some(candidate.to_owned());
                    if candidate == id {
                        found = Some(row);
                    }
                }
                if let Some(row) = found {
                    return self.observe(reference, row, false, operations, store).await;
                }
                if page.len() < 50 {
                    break;
                }
            }
            Err(CircleError::Unavailable)
        }
        .await;
        match result {
            Ok(result) => Ok(result),
            Err(CircleError::Journal | CircleError::Capacity) => Err(CircleError::Journal),
            Err(_) => operations[reference].result(reference, true),
        }
    }
    async fn execute_inner(
        &self,
        call: &WalletCall,
        reference_: &str,
    ) -> Result<OperationResult, CircleError> {
        reference(reference_)?;
        self.validate_call(call)?;
        let _lock = self.lock.lock().await;
        let mut store = self.store()?;
        let mut operations = self.load(&store)?;
        if let Some(row) = operations.get(reference_) {
            if row.call.call()? != *call {
                return Err(CircleError::InvalidEvidence);
            }
            return self
                .reconcile_locked(reference_, &mut operations, &mut store)
                .await;
        }
        if operations.len() >= MAX_OPERATIONS {
            return Err(CircleError::Capacity);
        }
        let direct =
            self.options.allow_registration_activation && *call == self.registration_call()?;
        self.verify_wallet().await?;
        let key = fresh_key()?;
        operations.insert(
            reference_.into(),
            Retained {
                route: if direct { "direct_register" } else { "self" }.into(),
                call: RetainedCall::from_call(call),
                idempotency_key: key.clone(),
                wallet_operation_id: None,
                tx_hash: None,
                state: "unknown".into(),
            },
        );
        self.save(&mut store, &operations)?;
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
        let result = match self.json(args, true).await {
            Ok(row) => {
                self.observe(reference_, &row, true, &mut operations, &mut store)
                    .await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(result) => Ok(result),
            Err(CircleError::Journal | CircleError::Capacity) => Err(CircleError::Journal),
            Err(_) => operations[reference_].result(reference_, true),
        }
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
        let mut store = self.store().map_err(|_| WalletError)?;
        let mut operations = self.load(&store).map_err(|_| WalletError)?;
        self.reconcile_locked(reference_, &mut operations, &mut store)
            .await
            .map_err(|_| WalletError)
    }
}

#[cfg(test)]
#[path = "circle_tests.rs"]
mod tests;
