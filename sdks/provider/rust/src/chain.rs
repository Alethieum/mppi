//! Canonical, block-pinned read evidence from a caller-selected trusted RPC.
//! Consistency checks do not authenticate an RPC or establish manifest provenance.
//!
//! Each step sends its reads at one block through one [`Rpc::call_many`], which
//! an RPC with JSON-RPC batches answers in one round trip per batch. State
//! reads are pinned with EIP-1898 `{blockHash, requireCanonical: true}`, and
//! the same `call_many` reads that block by number, last: requireCanonical is
//! only a SHOULD, so the matching hash is what shows the block was canonical
//! while the reads were served. Facts that cannot change for one reader are
//! read once: the RPC's chain id, and the code at each pinned address (EIP-6780
//! removes code only in the transaction that created it). Proxy implementation
//! slots are mutable and read every time.

use crate::{
    catalog::Catalog,
    contract::{OperationKind, PreparedOperation, abi::IMPPI},
    discovery::ProviderIdentity,
    errors::ValidationError,
    http::TransportError,
    signing::{SessionDescriptor, Voucher},
    types::{
        ChainObservation, ConfirmedSettlementRef, Domain, OperationResult, OperationState,
        PauseLevel, SessionOperationOutcome, SessionStatus, validate_descriptor,
    },
};
use alloy_primitives::{Address, B256, U256, address, b256, hex, keccak256};
use alloy_sol_types::{SolCall, SolEvent, abi::AbiDecoderConfig, sol};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

const IMPLEMENTATION_SLOT: &str =
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";
const MAX_DATA_BYTES: usize = 4 << 20;
const MAX_RECEIPT_LOGS: usize = 16_384;

sol! {
    interface Multicall3 {
        struct Call3 { address target; bool allowFailure; bytes callData; }
        struct Answer { bool success; bytes returnData; }
        function aggregate3(Call3[] calldata calls) external payable returns (Answer[] memory returnData);
        function getBlockNumber() external view returns (uint256 blockNumber);
        function getCurrentBlockTimestamp() external view returns (uint256 timestamp);
        function getLastBlockHash() external view returns (bytes32 blockHash);
    }
}
// Multicall3 (github.com/mds1/multicall3, src/Multicall3.sol) at its canonical
// address. It has no proxy, delegatecall or selfdestruct, so its code never
// changes; this runtime Keccak-256 is the code at that address on Arc testnet,
// Arc mainnet and Ethereum mainnet (read October 2026). Different or absent
// code falls back to pinned batches.
const MULTICALL3: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");
const MULTICALL3_CODE_HASH: B256 =
    b256!("d5c15df687b16f2ff992fc8d767b4216323184a2bbc6ee2f9c398c318e770891");
// The most views one snapshot `aggregate3` holds besides its three head views.
// Arc's node runs an `eth_call` without a gas field at its `--rpc.gascap`,
// 30,000,000 by default (circlefin/arc-node v0.8.1 `crates/node/src/args.rs`;
// Reth v2.2.0 `prepare_call_env`). Measured on a local revm node (anvil 1.5.1,
// Osaka rules), 512 `getSession` views with the other snapshot views took
// 6,167,605 gas, about a fifth of that cap, where one call of 2,249 exceeded
// it; their calldata is 231,562 characters of hex, under a quarter of the
// 1 MiB request bound.
const SNAPSHOT_VIEWS: usize = 512;
const UNKNOWN: u8 = 0;
const PRESENT: u8 = 1;
const ABSENT: u8 = 2;

/// A chain read that produced no evidence, by cause. No variant carries an
/// endpoint, credential or text a remote party wrote. Later releases may add
/// causes, so a `match` outside this crate needs a wildcard arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChainError {
    /// No evidence, for a reason the reader does not type: a stale or stopped
    /// follower, say, or an [`Rpc`] that reports no cause.
    Unavailable,
    /// The JSON HTTP exchange failed.
    Transport(TransportError),
    /// The RPC answered with this JSON-RPC error code.
    Rpc(i64),
    /// The node executed the `eth_call` or `eth_estimateGas` and it reverted
    /// (JSON-RPC error code 3), with the revert data's 4-byte selector when it
    /// returned one. The same call against the same state reverts again.
    Reverted(Option<[u8; 4]>),
    /// The RPC served a head behind evidence already accepted, or has not yet
    /// served a block it was asked for: by number, or by hash with Arc's
    /// -32014, "hasn't yet imported that block", which
    /// [`HttpJsonRpc`](crate::http::HttpJsonRpc) reports so once its repeats
    /// are spent. A load-balanced RPC routes reads to backends at slightly
    /// different heights (https://docs.arc.io/arc/references/rpc-endpoints):
    /// this is lag, not a contradiction, and accepted evidence stays valid.
    Lagging,
    /// Malformed evidence, or evidence contradicting the deployment or what was
    /// accepted.
    InvalidEvidence(&'static str),
}

enum Event {
    Opened(IMPPI::SessionOpened),
    TopUp(IMPPI::ToppedUp),
    Settled(IMPPI::Settled),
    CloseRequested(IMPPI::CloseRequested),
    Closed(IMPPI::SessionClosed),
    Idle(IMPPI::IdleReleased),
}
impl Event {
    fn session_id(&self) -> B256 {
        match self {
            Self::Opened(e) => e.sessionId,
            Self::TopUp(e) => e.sessionId,
            Self::Settled(e) => e.sessionId,
            Self::CloseRequested(e) => e.sessionId,
            Self::Closed(e) => e.sessionId,
            Self::Idle(e) => e.sessionId,
        }
    }
    fn capture(&self) -> Option<u64> {
        match self {
            Self::Settled(e) => Some(e.captureAmount),
            _ => None,
        }
    }
}

impl ChainReader {
    /// The operation's own events in a receipt, checked for inclusion in it.
    fn operation_events(
        &self,
        kind: OperationKind,
        receipt: &Receipt,
        tx_hash: B256,
    ) -> Result<Vec<Event>, ChainError> {
        Ok(match kind {
            OperationKind::Open => self
                .events::<IMPPI::SessionOpened>(receipt, tx_hash)?
                .into_iter()
                .map(Event::Opened)
                .collect(),
            OperationKind::TopUp => self
                .events::<IMPPI::ToppedUp>(receipt, tx_hash)?
                .into_iter()
                .map(Event::TopUp)
                .collect(),
            OperationKind::Settle | OperationKind::SettleBatch => self
                .events::<IMPPI::Settled>(receipt, tx_hash)?
                .into_iter()
                .map(Event::Settled)
                .collect(),
            OperationKind::RequestClose => self
                .events::<IMPPI::CloseRequested>(receipt, tx_hash)?
                .into_iter()
                .map(Event::CloseRequested)
                .collect(),
            OperationKind::Close | OperationKind::Withdraw => self
                .events::<IMPPI::SessionClosed>(receipt, tx_hash)?
                .into_iter()
                .map(Event::Closed)
                .collect(),
            OperationKind::ReleaseIdle => self
                .events::<IMPPI::IdleReleased>(receipt, tx_hash)?
                .into_iter()
                .map(Event::Idle)
                .collect(),
        })
    }
    /// Each item's outcome from its event, checked against `states`, the
    /// session states read at the receipt's block.
    fn outcomes(
        &self,
        operation: &PreparedOperation,
        events: &[Event],
        block: &Block,
        tx_hash: B256,
        states: &HashMap<B256, ChainSession>,
    ) -> Result<(Vec<SessionOperationOutcome>, bool), ChainError> {
        let kind = operation.kind();
        let ids = operation.session_ids();
        let mut multiplicities: HashMap<(B256, u64), usize> = HashMap::new();
        if kind == OperationKind::SettleBatch {
            for (id, target) in ids.iter().zip(operation.amounts()) {
                *multiplicities.entry((*id, *target)).or_default() += 1;
            }
        }
        let mut outcomes = Vec::new();
        let mut proven = true;
        for (index, (id, descriptor)) in ids.iter().zip(operation.descriptors()).enumerate() {
            let state = states
                .get(id)
                .ok_or_else(|| invalid("operation session state missing"))?;
            let target = operation.amounts().get(index).copied();
            let mut candidates = events.iter().filter(|e| {
                e.session_id() == *id
                    && (kind != OperationKind::SettleBatch || e.capture() == target)
            });
            let event = candidates.next();
            if candidates.next().is_some() {
                return Err(invalid("ambiguous repeated operation event"));
            }
            let mut reference = None;
            let mut covered = None;
            if matches!(kind, OperationKind::Settle | OperationKind::SettleBatch) {
                let target = target.ok_or_else(|| invalid("missing capture target"))?;
                let is_covered = state.exists() && state.settled_cumulative >= target;
                covered = Some(is_covered);
                if let Some(Event::Settled(e)) = event {
                    if e.agent != descriptor.agent
                        || e.recipient != descriptor.recipient
                        || e.captureAmount != target
                        || e.newSettled != target
                        || e.cumulativeAmount < target
                        || e.deltaPaid == 0
                        || e.deltaPaid > target
                        || !is_covered
                    {
                        return Err(invalid(
                            "settlement event contradicts intended target or state",
                        ));
                    }
                    if multiplicities.get(&(*id, target)).copied().unwrap_or(1) == 1 {
                        reference = Some(ConfirmedSettlementRef::new(
                            block.observation,
                            tx_hash,
                            *id,
                            target,
                            e.deltaPaid,
                        )?);
                    }
                } else {
                    proven = false;
                }
            } else if !state.exists() || event.is_none() {
                proven = false;
            } else {
                match event {
                    Some(Event::Opened(e)) => {
                        if e.agent != descriptor.agent
                            || e.sessionSigner != descriptor.sessionSigner
                            || e.providerId != descriptor.providerId
                            || e.recipient != descriptor.recipient
                            || e.operator != descriptor.operator
                            || e.token != descriptor.token
                            || e.salt != descriptor.salt
                            || Some(e.deposit) != target
                            || state.deposit < e.deposit
                            || Some(e.initialRateCardHash) != operation.expected_rate_card_hash()
                            || Some(state.initial_rate_card_hash)
                                != operation.expected_rate_card_hash()
                        {
                            return Err(invalid(
                                "opening event contradicts descriptor or funding terms",
                            ));
                        }
                    }
                    Some(Event::TopUp(e)) => {
                        if Some(e.amount) != target
                            || e.newDeposit < e.amount
                            || state.deposit < e.newDeposit
                        {
                            return Err(invalid("top-up event contradicts intended funding"));
                        }
                    }
                    Some(Event::CloseRequested(e)) => {
                        if !matches!(state.status, SessionStatus::Closing | SessionStatus::Closed)
                            || state.close_requested_at.checked_add(3600) != Some(e.graceEnd)
                            || state.close_requested_at != block.timestamp
                        {
                            return Err(invalid("close request contradicts canonical state"));
                        }
                    }
                    Some(Event::Closed(e)) => {
                        if state.status != SessionStatus::Closed
                            || e.captureAmount != state.settled_cumulative
                            || e.captureAmount.checked_add(e.refunded) != Some(state.deposit)
                            || (kind == OperationKind::Close && Some(e.captureAmount) != target)
                        {
                            return Err(invalid("closing event contradicts canonical state"));
                        }
                    }
                    Some(Event::Idle(e)) => {
                        if state.status != SessionStatus::Closed
                            || e.amount != state.deposit - state.settled_cumulative
                        {
                            return Err(invalid("idle refund contradicts canonical state"));
                        }
                    }
                    _ => return Err(invalid("unexpected operation event")),
                }
            }
            outcomes.push(SessionOperationOutcome::new(
                *id, target, covered, reference, None,
            )?);
        }
        if kind == OperationKind::SettleBatch {
            proven = outcomes.iter().any(|o| o.settlement_reference().is_some());
        }
        Ok((outcomes, proven))
    }
}
impl core::fmt::Display for ChainError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Unavailable => f.write_str("chain evidence unavailable"),
            Self::Transport(error) => write!(f, "chain RPC unavailable: {error}"),
            Self::Rpc(code) => write!(f, "chain RPC returned JSON-RPC error {code}"),
            Self::Reverted(Some(selector)) => {
                write!(f, "chain call reverted (0x{})", hex::encode(selector))
            }
            Self::Reverted(None) => f.write_str("chain call reverted"),
            Self::Lagging => f.write_str("chain RPC is behind accepted evidence"),
            Self::InvalidEvidence(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for ChainError {}
impl From<ValidationError> for ChainError {
    fn from(_: ValidationError) -> Self {
        Self::InvalidEvidence("invalid chain integration value")
    }
}
fn invalid(message: &'static str) -> ChainError {
    ChainError::InvalidEvidence(message)
}

/// Read-only RPC capability. Implementations must bound response size and time,
/// preserve JSON scalar types, and never turn a read failure into a transaction.
#[tonic::async_trait]
pub trait Rpc: Send + Sync {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError>;
    /// Results of `calls`, in order, or the first failure. [`ChainReader`] sends
    /// each step's reads through this, so an RPC with JSON-RPC batches answers
    /// them in one exchange per batch. The default sends one call at a time.
    async fn call_many(&self, calls: &[(&str, Value)]) -> Result<Vec<Value>, ChainError> {
        let mut results = Vec::with_capacity(calls.len());
        for (method, params) in calls {
            results.push(self.call(method, params.clone()).await?);
        }
        Ok(results)
    }
}

pub(crate) fn quantity(value: &Value) -> Result<U256, ChainError> {
    let raw = value
        .as_str()
        .ok_or_else(|| invalid("invalid RPC quantity"))?;
    let digits = raw
        .strip_prefix("0x")
        .ok_or_else(|| invalid("invalid RPC quantity"))?;
    if digits.is_empty()
        || digits.len() > 64
        || (digits.len() > 1 && digits.starts_with('0'))
        || !digits
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid("invalid RPC quantity"));
    }
    U256::from_str_radix(digits, 16).map_err(|_| invalid("invalid RPC quantity"))
}
pub(crate) fn quantity_u64(value: &Value) -> Result<u64, ChainError> {
    u64::try_from(quantity(value)?).map_err(|_| invalid("RPC quantity exceeds uint64"))
}
pub(crate) fn data(value: &Value) -> Result<Vec<u8>, ChainError> {
    let raw = value
        .as_str()
        .and_then(|v| v.strip_prefix("0x"))
        .ok_or_else(|| invalid("invalid RPC bytes"))?;
    if raw.len() > MAX_DATA_BYTES * 2
        || !raw.len().is_multiple_of(2)
        || !raw.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid("invalid or oversized RPC bytes"));
    }
    hex::decode(raw).map_err(|_| invalid("invalid RPC bytes"))
}
pub(crate) fn hash(value: &Value) -> Result<B256, ChainError> {
    B256::try_from(data(value)?.as_slice()).map_err(|_| invalid("invalid RPC hash width"))
}
pub(crate) fn address(value: &Value) -> Result<Address, ChainError> {
    Address::try_from(data(value)?.as_slice()).map_err(|_| invalid("invalid RPC address width"))
}
pub(crate) fn encoded(value: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(value))
}
/// One JSON-RPC read: a method and its parameters.
pub(crate) type Call = (&'static str, Value);
/// A contract view: its target and calldata.
type View = (Address, Vec<u8>);
pub(crate) fn eth_call(to: Address, calldata: &[u8]) -> Call {
    (
        "eth_call",
        json!([{"to":encoded(to),"data":encoded(calldata)}]),
    )
}
/// Each call with `position` appended to its parameters.
fn at(calls: Vec<Call>, position: &Value) -> Vec<Call> {
    calls
        .into_iter()
        .map(|(method, mut params)| {
            if let Value::Array(items) = &mut params {
                items.push(position.clone());
            }
            (method, params)
        })
        .collect()
}
fn pinned(block_hash: B256) -> Value {
    json!({"blockHash":encoded(block_hash),"requireCanonical":true})
}
/// A snapshot's views, in order, split into `aggregate3` calls of at most
/// [`SNAPSHOT_VIEWS`] views and one provider's identity each: an identity's
/// gas grows with the URI and catalog it stores.
fn snapshot_chunks(
    fixed: Vec<View>,
    identities: impl IntoIterator<Item = [View; 4]>,
    sessions: impl IntoIterator<Item = View>,
) -> Vec<Vec<View>> {
    let (mut chunks, mut chunk, mut identity) = (Vec::new(), fixed, false);
    for group in identities {
        if identity || chunk.len() + group.len() > SNAPSHOT_VIEWS {
            chunks.push(std::mem::take(&mut chunk));
        }
        chunk.extend(group);
        identity = true;
    }
    for view in sessions {
        if chunk.len() == SNAPSHOT_VIEWS {
            chunks.push(std::mem::take(&mut chunk));
        }
        chunk.push(view);
    }
    chunks.push(chunk);
    chunks
}
/// The `aggregate3` reading `views` after its block number, time and parent hash.
fn aggregate(views: &[View]) -> Call {
    let head = [
        Multicall3::getBlockNumberCall {}.abi_encode(),
        Multicall3::getCurrentBlockTimestampCall {}.abi_encode(),
        Multicall3::getLastBlockHashCall {}.abi_encode(),
    ];
    let calls = head
        .into_iter()
        .map(|data| (MULTICALL3, data))
        .chain(views.iter().cloned())
        .map(|(target, data)| Multicall3::Call3 {
            target,
            allowFailure: false,
            callData: data.into(),
        })
        .collect();
    eth_call(
        MULTICALL3,
        &Multicall3::aggregate3Call { calls }.abi_encode(),
    )
}
/// The answers to an [`aggregate`] of `views` views after its head views, or
/// `None` when they ran at another height than `head`.
fn answers(raw: &Value, views: usize, head: &Block) -> Result<Option<Vec<Vec<u8>>>, ChainError> {
    let answers = decode_return::<Multicall3::aggregate3Call>(&data(raw)?)?;
    if answers.len() != 3 + views || !answers.iter().all(|answer| answer.success) {
        return Err(invalid("RPC snapshot is incomplete"));
    }
    let number = decode_return::<Multicall3::getBlockNumberCall>(&answers[0].returnData)?;
    let timestamp =
        decode_return::<Multicall3::getCurrentBlockTimestampCall>(&answers[1].returnData)?;
    let parent = decode_return::<Multicall3::getLastBlockHashCall>(&answers[2].returnData)?;
    if number != U256::from(head.observation.block_number()) {
        return Ok(None);
    }
    if timestamp != U256::from(head.timestamp) || parent != head.parent_hash {
        return Err(invalid("RPC snapshot and head are from different blocks"));
    }
    Ok(Some(
        answers
            .into_iter()
            .skip(3)
            .map(|answer| answer.returnData.to_vec())
            .collect(),
    ))
}
fn parse_session(raw: &[u8]) -> Result<ChainSession, ChainError> {
    let result = decode_return::<IMPPI::getSessionCall>(raw)?;
    let status = match result.status {
        IMPPI::SessionStatus::Open => SessionStatus::Open,
        IMPPI::SessionStatus::Closing => SessionStatus::Closing,
        IMPPI::SessionStatus::Closed => SessionStatus::Closed,
        _ => return Err(invalid("invalid session status")),
    };
    ChainSession::new(
        result.deposit,
        result.settledCumulative,
        status,
        result.closeRequestedAt,
        result.lastActivity,
        result.initialRateCardHash,
    )
}
fn parse_pause(raw: &[u8]) -> Result<PauseLevel, ChainError> {
    match decode_return::<IMPPI::pauseLevelCall>(raw)? {
        IMPPI::PauseLevel::None => Ok(PauseLevel::None),
        IMPPI::PauseLevel::Payments => Ok(PauseLevel::Payments),
        IMPPI::PauseLevel::All => Ok(PauseLevel::All),
        _ => Err(invalid("invalid MPPI pause level")),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodePin {
    address: Address,
    code_hash: B256,
}
impl CodePin {
    pub fn new(address: Address, code_hash: B256) -> Result<Self, ChainError> {
        if address.is_zero() || code_hash.is_zero() {
            return Err(invalid("code pins must be nonzero"));
        }
        Ok(Self { address, code_hash })
    }
    pub fn address(&self) -> Address {
        self.address
    }
    pub fn code_hash(&self) -> B256 {
        self.code_hash
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeploymentProfile {
    domain: Domain,
    registry_address: Address,
    proxy: CodePin,
    implementation: CodePin,
    registry: CodePin,
    registry_implementation: CodePin,
}
impl DeploymentProfile {
    pub fn new(
        domain: Domain,
        registry_address: Address,
        proxy: CodePin,
        implementation: CodePin,
        registry: CodePin,
        registry_implementation: CodePin,
    ) -> Result<Self, ChainError> {
        if !matches!(domain.chain_id(), 5042 | 5042002)
            || proxy.address() != domain.verifying_contract()
            || registry_address.is_zero()
            || registry.address() != registry_address
        {
            return Err(invalid(
                "deployment profile requires matching Arc addresses and pins",
            ));
        }
        Ok(Self {
            domain,
            registry_address,
            proxy,
            implementation,
            registry,
            registry_implementation,
        })
    }
    pub fn domain(&self) -> &Domain {
        &self.domain
    }
    pub fn registry_address(&self) -> Address {
        self.registry_address
    }
    pub fn proxy(&self) -> &CodePin {
        &self.proxy
    }
    pub fn implementation(&self) -> &CodePin {
        &self.implementation
    }
    pub fn registry(&self) -> &CodePin {
        &self.registry
    }
    pub fn registry_implementation(&self) -> &CodePin {
        &self.registry_implementation
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    observation: ChainObservation,
    timestamp: u64,
    parent_hash: B256,
}
impl Block {
    pub fn new(observation: ChainObservation, timestamp: u64, parent_hash: B256) -> Self {
        Self {
            observation,
            timestamp,
            parent_hash,
        }
    }
    pub fn observation(&self) -> &ChainObservation {
        &self.observation
    }
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
    pub fn parent_hash(&self) -> B256 {
        self.parent_hash
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainSession {
    deposit: u64,
    settled_cumulative: u64,
    status: SessionStatus,
    close_requested_at: u64,
    last_activity: u64,
    initial_rate_card_hash: B256,
}
impl ChainSession {
    pub fn new(
        deposit: u64,
        settled_cumulative: u64,
        status: SessionStatus,
        close_requested_at: u64,
        last_activity: u64,
        initial_rate_card_hash: B256,
    ) -> Result<Self, ChainError> {
        if settled_cumulative > deposit
            || (deposit == 0
                && (settled_cumulative != 0
                    || status != SessionStatus::Open
                    || close_requested_at != 0
                    || last_activity != 0
                    || !initial_rate_card_hash.is_zero()))
        {
            return Err(invalid("inconsistent MPPI session state"));
        }
        Ok(Self {
            deposit,
            settled_cumulative,
            status,
            close_requested_at,
            last_activity,
            initial_rate_card_hash,
        })
    }
    pub fn deposit(&self) -> u64 {
        self.deposit
    }
    pub fn settled_cumulative(&self) -> u64 {
        self.settled_cumulative
    }
    pub fn status(&self) -> SessionStatus {
        self.status
    }
    pub fn close_requested_at(&self) -> u64 {
        self.close_requested_at
    }
    pub fn last_activity(&self) -> u64 {
        self.last_activity
    }
    pub fn initial_rate_card_hash(&self) -> B256 {
        self.initial_rate_card_hash
    }
    pub fn exists(&self) -> bool {
        self.deposit > 0
    }
}
#[derive(Clone, Debug)]
pub struct OpeningEvidence {
    descriptor: SessionDescriptor,
    block: Block,
    deposit: u64,
    initial_catalog: Catalog,
    tx_hash: B256,
}
impl OpeningEvidence {
    pub fn descriptor(&self) -> &SessionDescriptor {
        &self.descriptor
    }
    pub fn block(&self) -> &Block {
        &self.block
    }
    pub fn deposit(&self) -> u64 {
        self.deposit
    }
    pub fn initial_catalog(&self) -> &Catalog {
        &self.initial_catalog
    }
    pub fn tx_hash(&self) -> B256 {
        self.tx_hash
    }
}
#[derive(Clone, Debug)]
pub struct ClosingEvidence {
    block: Block,
    capture_amount: u64,
    refunded: u64,
    tx_hash: B256,
}
impl ClosingEvidence {
    pub fn block(&self) -> &Block {
        &self.block
    }
    pub fn capture_amount(&self) -> u64 {
        self.capture_amount
    }
    pub fn refunded(&self) -> u64 {
        self.refunded
    }
    pub fn tx_hash(&self) -> B256 {
        self.tx_hash
    }
}
#[derive(Clone, Debug)]
pub struct SimulationEvidence {
    block: Block,
    gas_estimate: U256,
    gas_limit: U256,
}
impl SimulationEvidence {
    pub fn block(&self) -> &Block {
        &self.block
    }
    pub fn gas_estimate(&self) -> U256 {
        self.gas_estimate
    }
    pub fn gas_limit(&self) -> U256 {
        self.gas_limit
    }
}

#[derive(Clone)]
pub struct ChainReader {
    rpc: Arc<dyn Rpc>,
    profile: DeploymentProfile,
    // What this reader, and every clone of it, has checked once.
    checked: Arc<Checked>,
}
#[derive(Default)]
struct Checked {
    chain: AtomicBool,
    code: AtomicBool,
    domain: AtomicBool,
    multicall: AtomicU8,
}
impl core::fmt::Debug for ChainReader {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ChainReader")
            .field("profile", &self.profile)
            .finish_non_exhaustive()
    }
}
/// The head and what is watched at it: a follower snapshot's contents.
pub(crate) struct Latest {
    pub(crate) block: Block,
    pub(crate) pause_level: PauseLevel,
    pub(crate) identities: BTreeMap<U256, ProviderIdentity>,
    pub(crate) sessions: BTreeMap<B256, ChainSession>,
}
/// A served receipt: its block by number and hash, and whether it succeeded.
struct Receipt {
    raw: Value,
    number: u64,
    block_hash: B256,
    success: bool,
}

/// Decode with type checks and exact re-encoding, including surplus word rejection.
pub(crate) fn decode_return<C: SolCall>(raw: &[u8]) -> Result<C::Return, ChainError> {
    let result = C::abi_decode_returns_with_config(
        raw,
        AbiDecoderConfig::new()
            .strict(true)
            .validate(true)
            .memory_limit(MAX_DATA_BYTES),
    )
    .map_err(|_| invalid("invalid canonical ABI return"))?;
    if C::abi_encode_returns(&result) != raw {
        return Err(invalid("noncanonical ABI return"));
    }
    Ok(result)
}
fn decode_event<E: SolEvent>(topics: &[B256], raw: &[u8]) -> Result<E, ChainError> {
    let result = E::decode_raw_log_with_config(
        topics.iter().copied(),
        raw,
        AbiDecoderConfig::new()
            .strict(true)
            .validate(true)
            .memory_limit(MAX_DATA_BYTES),
    )
    .map_err(|_| invalid("invalid canonical ABI event"))?;
    let encoded = result.encode_log_data();
    if encoded.topics() != topics || encoded.data.as_ref() != raw {
        return Err(invalid("noncanonical ABI event"));
    }
    Ok(result)
}

impl ChainReader {
    pub fn new(rpc: Arc<dyn Rpc>, profile: DeploymentProfile) -> Self {
        Self {
            rpc,
            profile,
            checked: Arc::default(),
        }
    }
    pub fn rpc(&self) -> &dyn Rpc {
        self.rpc.as_ref()
    }
    pub fn profile(&self) -> &DeploymentProfile {
        &self.profile
    }
    /// `calls` in one [`Rpc::call_many`], after the RPC's chain id the first time.
    pub(crate) async fn many(&self, mut calls: Vec<Call>) -> Result<Vec<Value>, ChainError> {
        let network = !self.checked.chain.load(Ordering::Acquire);
        if network {
            calls.insert(0, ("eth_chainId", json!([])));
        }
        let mut results = self.rpc.call_many(&calls).await?;
        if results.len() != calls.len() {
            return Err(invalid("RPC batch response count mismatch"));
        }
        if network {
            if quantity(&results.remove(0))? != U256::from(self.profile.domain.chain_id()) {
                return Err(invalid("RPC chain differs from selected deployment"));
            }
            self.checked.chain.store(true, Ordering::Release);
        }
        Ok(results)
    }
    fn parse_block(&self, raw: &Value, number: Option<u64>) -> Result<Block, ChainError> {
        if raw.is_null() && number.is_some() {
            return Err(ChainError::Lagging);
        }
        let actual = quantity_u64(&raw["number"])?;
        if number.is_some_and(|n| n != actual) {
            return Err(invalid("canonical block number mismatch"));
        }
        Ok(Block::new(
            ChainObservation::new(
                self.profile.domain.chain_id(),
                self.profile.domain.verifying_contract(),
                actual,
                hash(&raw["hash"])?,
            )?,
            quantity_u64(&raw["timestamp"])?,
            hash(&raw["parentHash"])?,
        ))
    }
    fn block_call(number: Option<u64>) -> Call {
        (
            "eth_getBlockByNumber",
            json!([
                number.map_or_else(|| "latest".to_owned(), |n| format!("0x{n:x}")),
                false
            ]),
        )
    }
    /// The head (`None`) or a block by number; a block the RPC has not served
    /// yet is [`ChainError::Lagging`].
    pub async fn block(&self, number: Option<u64>) -> Result<Block, ChainError> {
        let results = self.many(vec![Self::block_call(number)]).await?;
        self.parse_block(&results[0], number)
    }
    pub(crate) fn position(&self, block: &Block) -> Result<Value, ChainError> {
        if block.observation.chain_id() != self.profile.domain.chain_id()
            || block.observation.contract_address() != self.profile.domain.verifying_contract()
        {
            return Err(invalid(
                "block observation differs from selected deployment",
            ));
        }
        Ok(pinned(block.observation.block_hash()))
    }
    pub(crate) async fn call_at(
        &self,
        address: Address,
        calldata: &[u8],
        block: &Block,
    ) -> Result<Vec<u8>, ChainError> {
        data(
            &self
                .rpc
                .call(
                    "eth_call",
                    json!([{"to":encoded(address),"data":encoded(calldata)},self.position(block)?]),
                )
                .await?,
        )
    }
    /// State reads pinned to block `number` by `block_hash`, with that block
    /// read by number last in the same [`Rpc::call_many`].
    pub(crate) async fn read_at(
        &self,
        number: u64,
        block_hash: B256,
        calls: Vec<Call>,
    ) -> Result<(Block, Vec<Value>), ChainError> {
        let mut calls = at(calls, &pinned(block_hash));
        calls.push(Self::block_call(Some(number)));
        let mut results = self.many(calls).await?;
        let raw = results.pop().ok_or_else(|| invalid("RPC block missing"))?;
        let block = self.parse_block(&raw, Some(number))?;
        if block.observation.block_hash() != block_hash {
            return Err(invalid("canonical block changed during observation"));
        }
        Ok((block, results))
    }
    /// [`Self::read_at`] an observed block, which must read back unchanged.
    pub(crate) async fn read(
        &self,
        block: &Block,
        calls: Vec<Call>,
    ) -> Result<Vec<Value>, ChainError> {
        self.position(block)?;
        let (found, results) = self
            .read_at(
                block.observation.block_number(),
                block.observation.block_hash(),
                calls,
            )
            .await?;
        if found != *block {
            return Err(invalid("canonical block changed during observation"));
        }
        Ok(results)
    }
    pub(crate) async fn check_canonical(&self, block: &Block) -> Result<(), ChainError> {
        self.position(block)?;
        if self.block(Some(block.observation.block_number())).await? != *block {
            return Err(invalid("canonical block changed during observation"));
        }
        Ok(())
    }
    fn pins(&self) -> [&CodePin; 4] {
        [
            &self.profile.proxy,
            &self.profile.implementation,
            &self.profile.registry,
            &self.profile.registry_implementation,
        ]
    }
    /// Whether the next deployment check must still read the pinned code.
    pub(crate) fn code_unchecked(&self) -> bool {
        !self.checked.code.load(Ordering::Acquire)
    }
    fn code_calls(&self) -> Vec<Call> {
        self.pins()
            .iter()
            .map(|pin| ("eth_getCode", json!([encoded(pin.address)])))
            .collect()
    }
    fn check_code<'a>(&self, results: &'a [Value]) -> Result<&'a [Value], ChainError> {
        for (pin, raw) in self.pins().iter().zip(results) {
            let code = data(raw)?;
            if code.is_empty() || keccak256(code) != pin.code_hash {
                return Err(invalid("deployed code differs from release profile"));
            }
        }
        self.checked.code.store(true, Ordering::Release);
        Ok(&results[4..])
    }
    fn slot_calls(&self) -> Vec<Call> {
        [&self.profile.proxy, &self.profile.registry]
            .iter()
            .map(|proxy| {
                (
                    "eth_getStorageAt",
                    json!([encoded(proxy.address), IMPLEMENTATION_SLOT]),
                )
            })
            .collect()
    }
    fn check_slots(&self, raw: &[Value]) -> Result<(), ChainError> {
        for (implementation, raw) in [
            &self.profile.implementation,
            &self.profile.registry_implementation,
        ]
        .iter()
        .zip(raw)
        {
            let mut expected = [0u8; 32];
            expected[12..].copy_from_slice(implementation.address.as_slice());
            if hash(raw)?.as_slice() != expected {
                return Err(invalid("proxy implementation differs from release profile"));
            }
        }
        Ok(())
    }
    fn domain_data() -> Vec<u8> {
        IMPPI::voucherDigestCall {
            sessionId: B256::ZERO,
            cumulativeAmount: 0,
        }
        .abi_encode()
    }
    fn check_domain(&self, raw: &[u8]) -> Result<(), ChainError> {
        if decode_return::<IMPPI::voucherDigestCall>(raw)?
            != self.profile.domain.signing_context().digest(&Voucher {
                sessionId: B256::ZERO,
                cumulativeAmount: 0,
            })
        {
            return Err(invalid("MPPI signing domain differs from release profile"));
        }
        Ok(())
    }
    /// Reads that verify the deployment: the pinned code `with_code`, the
    /// implementation slots and the signing domain.
    pub(crate) fn deployment_calls(&self, with_code: bool) -> Vec<Call> {
        let mut calls = if with_code { self.code_calls() } else { vec![] };
        calls.extend(self.slot_calls());
        calls.push(eth_call(
            self.profile.domain.verifying_contract(),
            &Self::domain_data(),
        ));
        calls
    }
    /// Check the results of [`Self::deployment_calls`]; return what follows them.
    pub(crate) fn check_deployment<'a>(
        &self,
        results: &'a [Value],
        with_code: bool,
    ) -> Result<&'a [Value], ChainError> {
        if results.len() < 3 + 4 * usize::from(with_code) {
            return Err(invalid("RPC batch response count mismatch"));
        }
        let results = if with_code {
            self.check_code(results)?
        } else {
            results
        };
        self.check_slots(&results[..2])?;
        self.check_domain(&data(&results[2])?)?;
        Ok(&results[3..])
    }
    /// Compare the pinned code, proxy implementations and MPPI signing domain
    /// with the release profile at `block`, in one exchange.
    pub async fn verify_deployment(&self, block: &Block) -> Result<(), ChainError> {
        let with_code = self.code_unchecked();
        let results = self.read(block, self.deployment_calls(with_code)).await?;
        self.check_deployment(&results, with_code)?;
        Ok(())
    }
    pub(crate) fn session_call(&self, id: B256) -> Call {
        eth_call(
            self.profile.domain.verifying_contract(),
            &IMPPI::getSessionCall { sessionId: id }.abi_encode(),
        )
    }
    pub async fn session(&self, id: B256, block: &Block) -> Result<ChainSession, ChainError> {
        let call = IMPPI::getSessionCall { sessionId: id };
        parse_session(
            &self
                .call_at(
                    self.profile.domain.verifying_contract(),
                    &call.abi_encode(),
                    block,
                )
                .await?,
        )
    }
    pub async fn pause_level(&self, block: &Block) -> Result<PauseLevel, ChainError> {
        parse_pause(
            &self
                .call_at(
                    self.profile.domain.verifying_contract(),
                    &IMPPI::pauseLevelCall {}.abi_encode(),
                    block,
                )
                .await?,
        )
    }
    /// The deployment check, pause level, provider identities (an unusable
    /// catalog read as `None`) and sessions at the latest block.
    ///
    /// Multicall3 `aggregate3` `eth_call`s at `latest`, of at most
    /// [`SNAPSHOT_VIEWS`] views each, read every contract view, each with the
    /// block number, time and parent hash it ran at. They go in the
    /// [`Rpc::call_many`] that reads the header at `latest`, the anchor: Arc
    /// commits each block with BFT finality, "Every transaction in a committed
    /// block is immediately and irreversibly settled" with "no reorganization
    /// risk" (https://docs.arc.io/arc/concepts/deterministic-finality). A call
    /// whose views ran at another height is read again pinned to the header's
    /// hash, with the header read by number last ([`Self::read`]); one at the
    /// header's height but another block is a contradiction. Views from two
    /// blocks never form a snapshot. The implementation slots, which no
    /// contract exposes, are read with the header. One round trip while the
    /// calls fit one JSON-RPC batch ([`HttpJsonRpc`](crate::http::HttpJsonRpc):
    /// 32 calls and 1 MiB) and ran at the header's block; each further batch
    /// adds one, and calls that ran elsewhere are sent once more. The first
    /// snapshot first reads the pinned code and Multicall3's code with the
    /// head; without verified Multicall3 code each snapshot is the head and
    /// then the reads pinned to it.
    pub(crate) async fn latest(
        &self,
        providers: &BTreeSet<U256>,
        sessions: &BTreeSet<B256>,
    ) -> Result<Latest, ChainError> {
        match self.checked.multicall.load(Ordering::Acquire) {
            UNKNOWN => {
                let with_code = self.code_unchecked();
                let mut calls = if with_code { self.code_calls() } else { vec![] };
                calls.push(("eth_getCode", json!([encoded(MULTICALL3)])));
                let mut calls = at(calls, &json!("latest"));
                calls.push(Self::block_call(None));
                let results = self.many(calls).await?;
                let results = if with_code {
                    self.check_code(&results)?
                } else {
                    &results[..]
                };
                let present = keccak256(data(&results[0])?) == MULTICALL3_CODE_HASH;
                self.checked
                    .multicall
                    .store(if present { PRESENT } else { ABSENT }, Ordering::Release);
                if !present {
                    let head = self.parse_block(&results[1], None)?;
                    return self.observe_at(&head, providers, sessions).await;
                }
            }
            PRESENT => {}
            _ => {
                let head = self.block(None).await?;
                return self.observe_at(&head, providers, sessions).await;
            }
        }
        let proxy = self.profile.domain.verifying_contract();
        let registry = self.profile.registry_address;
        let domain = !self.checked.domain.load(Ordering::Acquire);
        let mut fixed = Vec::new();
        if domain {
            fixed.push((proxy, Self::domain_data()));
        }
        fixed.push((proxy, IMPPI::pauseLevelCall {}.abi_encode()));
        let chunks = snapshot_chunks(
            fixed,
            providers
                .iter()
                .map(|id| Self::identity_data(*id).map(|data| (registry, data))),
            sessions
                .iter()
                .map(|id| (proxy, IMPPI::getSessionCall { sessionId: *id }.abi_encode())),
        );
        let aggregates: Vec<Call> = chunks.iter().map(|chunk| aggregate(chunk)).collect();
        let mut calls = aggregates.clone();
        calls.extend(self.slot_calls());
        let mut calls = at(calls, &json!("latest"));
        calls.insert(0, Self::block_call(None));
        let results = self.many(calls).await?;
        let head = self.parse_block(&results[0], None)?;
        let mut answered = chunks
            .iter()
            .zip(&results[1..])
            .map(|(chunk, raw)| answers(raw, chunk.len(), &head))
            .collect::<Result<Vec<_>, _>>()?;
        self.check_slots(&results[1 + chunks.len()..])?;
        // Views that ran at another height than the header (sent in another
        // batch, or to another backend) are read again at the header's block.
        let elsewhere: Vec<usize> = (0..chunks.len())
            .filter(|&i| answered[i].is_none())
            .collect();
        if !elsewhere.is_empty() {
            let again = self
                .read(
                    &head,
                    elsewhere.iter().map(|&i| aggregates[i].clone()).collect(),
                )
                .await?;
            for (&i, raw) in elsewhere.iter().zip(&again) {
                answered[i] =
                    Some(answers(raw, chunks[i].len(), &head)?.ok_or_else(|| {
                        invalid("RPC snapshot and head are from different blocks")
                    })?);
            }
        }
        let answered = answered
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| invalid("RPC snapshot is incomplete"))?;
        let mut values = answered.iter().flatten();
        let mut next = || {
            values
                .next()
                .ok_or_else(|| invalid("RPC snapshot is incomplete"))
        };
        if domain {
            self.check_domain(next()?)?;
            self.checked.domain.store(true, Ordering::Release);
        }
        let pause_level = parse_pause(next()?)?;
        let mut identities = BTreeMap::new();
        for id in providers {
            let raw = (0..4)
                .map(|_| next().cloned())
                .collect::<Result<Vec<_>, _>>()?;
            identities.insert(*id, self.parse_identity(*id, &head, &raw, true)?);
        }
        let mut states = BTreeMap::new();
        for id in sessions {
            states.insert(*id, parse_session(next()?)?);
        }
        Ok(Latest {
            block: head,
            pause_level,
            identities,
            sessions: states,
        })
    }
    /// The deployment check and everything watched, pinned to `head` in one
    /// [`Rpc::call_many`] that reads `head` back by number, last.
    async fn observe_at(
        &self,
        head: &Block,
        providers: &BTreeSet<U256>,
        sessions: &BTreeSet<B256>,
    ) -> Result<Latest, ChainError> {
        let with_code = self.code_unchecked();
        let mut calls = self.deployment_calls(with_code);
        calls.push(eth_call(
            self.profile.domain.verifying_contract(),
            &IMPPI::pauseLevelCall {}.abi_encode(),
        ));
        for id in providers {
            calls.extend(self.identity_calls(*id));
        }
        calls.extend(sessions.iter().map(|id| self.session_call(*id)));
        let results = self.read(head, calls).await?;
        let mut results = self.check_deployment(&results, with_code)?;
        let pause_level = parse_pause(&data(&results[0])?)?;
        results = &results[1..];
        let mut identities = BTreeMap::new();
        for id in providers {
            let raw = results[..4]
                .iter()
                .map(data)
                .collect::<Result<Vec<_>, _>>()?;
            identities.insert(*id, self.parse_identity(*id, head, &raw, true)?);
            results = &results[4..];
        }
        let mut states = BTreeMap::new();
        for (id, raw) in sessions.iter().zip(results) {
            states.insert(*id, parse_session(&data(raw)?)?);
        }
        Ok(Latest {
            block: head.clone(),
            pause_level,
            identities,
            sessions: states,
        })
    }
    /// Simulate exactly from the intended wallet. `eth_estimateGas` takes a
    /// block number, unlike EIP-1898 `eth_call`; the same exchange reads that
    /// number's block back. Both results remain advisory.
    pub async fn simulate(
        &self,
        operation: &PreparedOperation,
        sender: Address,
    ) -> Result<SimulationEvidence, ChainError> {
        self.check_operation(operation)?;
        if sender.is_zero() {
            return Err(invalid("simulation sender must be nonzero"));
        }
        let results = self.many(vec![Self::block_call(None)]).await?;
        let block = self.parse_block(&results[0], None)?;
        let gas_limit = quantity(&results[0]["gasLimit"])?;
        if gas_limit.is_zero() {
            return Err(invalid("simulation block has no gas capacity"));
        }
        let number = block.observation.block_number();
        let call = json!({"from":encoded(sender),"to":encoded(operation.call().to()),"data":encoded(operation.call().data()),"value":"0x0","gas":format!("0x{gas_limit:x}")});
        let position = self.position(&block)?;
        let with_code = self.code_unchecked();
        let mut calls = at(self.deployment_calls(with_code), &position);
        calls.push(("eth_call", json!([call, position])));
        calls.push(("eth_estimateGas", json!([call, format!("0x{number:x}")])));
        calls.push(Self::block_call(Some(number)));
        let results = self.many(calls).await?;
        let results = self.check_deployment(&results, with_code)?;
        if results.len() != 3 {
            return Err(invalid("RPC batch response count mismatch"));
        }
        if !data(&results[0])?.is_empty() {
            return Err(invalid("MPPI simulation returned unexpected data"));
        }
        let gas_estimate = quantity(&results[1])?;
        if gas_estimate.is_zero() || gas_estimate > gas_limit {
            return Err(invalid("simulation estimate exceeds block capacity"));
        }
        if self.parse_block(&results[2], Some(number))? != block {
            return Err(invalid("simulation block changed"));
        }
        Ok(SimulationEvidence {
            block,
            gas_estimate,
            gas_limit,
        })
    }
    fn check_operation(&self, operation: &PreparedOperation) -> Result<(), ChainError> {
        if operation.call().chain_id() != self.profile.domain.chain_id()
            || operation.call().to() != self.profile.domain.verifying_contract()
        {
            return Err(invalid("operation differs from selected deployment"));
        }
        Ok(())
    }
    async fn receipt(&self, tx_hash: B256) -> Result<Option<Receipt>, ChainError> {
        let raw = self
            .many(vec![(
                "eth_getTransactionReceipt",
                json!([encoded(tx_hash)]),
            )])
            .await?
            .remove(0);
        if raw.is_null() {
            return Ok(None);
        }
        if hash(&raw["transactionHash"])? != tx_hash {
            return Err(invalid("receipt transaction mismatch"));
        }
        let number = quantity_u64(&raw["blockNumber"])?;
        let block_hash = hash(&raw["blockHash"])?;
        let status = quantity_u64(&raw["status"])?;
        if status > 1 {
            return Err(invalid("invalid receipt status"));
        }
        Ok(Some(Receipt {
            raw,
            number,
            block_hash,
            success: status == 1,
        }))
    }
    fn events<E: SolEvent>(&self, receipt: &Receipt, tx_hash: B256) -> Result<Vec<E>, ChainError> {
        let logs = receipt.raw["logs"]
            .as_array()
            .ok_or_else(|| invalid("receipt logs missing"))?;
        if logs.len() > MAX_RECEIPT_LOGS {
            return Err(invalid("receipt log limit exceeded"));
        }
        let tx_index = quantity(&receipt.raw["transactionIndex"])?;
        let mut previous = None;
        let mut events = Vec::new();
        for log in logs {
            let index = quantity(&log["logIndex"])?;
            if previous.is_some_and(|p| index <= p)
                || log["removed"] != Value::Bool(false)
                || hash(&log["transactionHash"])? != tx_hash
                || quantity(&log["transactionIndex"])? != tx_index
                || quantity_u64(&log["blockNumber"])? != receipt.number
                || hash(&log["blockHash"])? != receipt.block_hash
            {
                return Err(invalid("log inclusion or order mismatch"));
            }
            previous = Some(index);
            if address(&log["address"])? != self.profile.domain.verifying_contract() {
                continue;
            }
            let topics = log["topics"]
                .as_array()
                .ok_or_else(|| invalid("log topics missing"))?;
            if topics.len() > 4 {
                return Err(invalid("invalid event topic count"));
            }
            let topics = topics.iter().map(hash).collect::<Result<Vec<_>, _>>()?;
            if topics.first() != Some(&E::SIGNATURE_HASH) {
                continue;
            }
            events.push(decode_event::<E>(&topics, &data(&log["data"])?)?);
        }
        Ok(events)
    }
    /// One unambiguous MPPI event of a successful receipt; the caller reads its
    /// block, and the state there, in one more exchange.
    async fn event<E: SolEvent>(
        &self,
        tx_hash: B256,
        select: impl Fn(&E) -> bool,
    ) -> Result<(Receipt, E), ChainError> {
        let receipt = self
            .receipt(tx_hash)
            .await?
            .ok_or_else(|| invalid("receipt unavailable"))?;
        if !receipt.success {
            return Err(invalid("successful canonical receipt required"));
        }
        let mut events = self
            .events::<E>(&receipt, tx_hash)?
            .into_iter()
            .filter(select);
        let event = events
            .next()
            .ok_or_else(|| invalid("required MPPI event missing"))?;
        if events.next().is_some() {
            return Err(invalid("required MPPI event ambiguous"));
        }
        Ok((receipt, event))
    }
    pub async fn opening(
        &self,
        tx_hash: B256,
        session_id: B256,
        accepted_catalog: Option<&Catalog>,
    ) -> Result<OpeningEvidence, ChainError> {
        let (receipt, event) = self
            .event::<IMPPI::SessionOpened>(tx_hash, |e| e.sessionId == session_id)
            .await?;
        let descriptor = SessionDescriptor {
            agent: event.agent,
            sessionSigner: event.sessionSigner,
            providerId: event.providerId,
            recipient: event.recipient,
            operator: event.operator,
            token: event.token,
            salt: event.salt,
        };
        validate_descriptor(&descriptor)?;
        let mut calls = vec![self.session_call(session_id)];
        if accepted_catalog.is_none() {
            calls.extend(self.identity_calls(descriptor.providerId));
        }
        let (block, results) = self
            .read_at(receipt.number, receipt.block_hash, calls)
            .await?;
        let state = parse_session(&data(&results[0])?)?;
        let catalog = match accepted_catalog {
            Some(catalog) => catalog.clone(),
            None => {
                let raw = results[1..]
                    .iter()
                    .map(data)
                    .collect::<Result<Vec<_>, _>>()?;
                self.parse_identity(descriptor.providerId, &block, &raw, false)?
                    .catalog()
                    .cloned()
                    .ok_or_else(|| invalid("opening catalog unavailable"))?
            }
        };
        if self
            .profile
            .domain
            .signing_context()
            .session_id(&descriptor)
            != session_id
            || descriptor.token != crate::contract::ARC_USDC
            || !state.exists()
            || event.deposit == 0
            || state.deposit < event.deposit
            || state.initial_rate_card_hash != event.initialRateCardHash
            || catalog.rate_card_hash() != event.initialRateCardHash
        {
            return Err(invalid("opening descriptor, deposit or catalog mismatch"));
        }
        Ok(OpeningEvidence {
            descriptor,
            block,
            deposit: event.deposit,
            initial_catalog: catalog,
            tx_hash,
        })
    }
    pub async fn closing(
        &self,
        tx_hash: B256,
        session_id: B256,
    ) -> Result<ClosingEvidence, ChainError> {
        let (receipt, event) = self
            .event::<IMPPI::SessionClosed>(tx_hash, |e| e.sessionId == session_id)
            .await?;
        let (block, results) = self
            .read_at(
                receipt.number,
                receipt.block_hash,
                vec![self.session_call(session_id)],
            )
            .await?;
        let state = parse_session(&data(&results[0])?)?;
        if !state.exists()
            || state.status != SessionStatus::Closed
            || event.captureAmount != state.settled_cumulative
            || event.captureAmount.checked_add(event.refunded) != Some(state.deposit)
        {
            return Err(invalid("closing event contradicts canonical state"));
        }
        Ok(ClosingEvidence {
            block,
            capture_amount: event.captureAmount,
            refunded: event.refunded,
            tx_hash,
        })
    }
    pub async fn verify_settlement(
        &self,
        reference: &ConfirmedSettlementRef,
        descriptor: &SessionDescriptor,
    ) -> Result<(), ChainError> {
        validate_descriptor(descriptor)?;
        if self.profile.domain.signing_context().session_id(descriptor) != reference.session_id() {
            return Err(invalid("settlement descriptor mismatch"));
        }
        let (receipt, event) = self
            .event::<IMPPI::Settled>(reference.tx_hash(), |e| {
                e.sessionId == reference.session_id()
                    && e.captureAmount == reference.settled_cumulative()
                    && e.deltaPaid == reference.delta()
            })
            .await?;
        let (block, results) = self
            .read_at(
                receipt.number,
                receipt.block_hash,
                vec![self.session_call(reference.session_id())],
            )
            .await?;
        let state = parse_session(&data(&results[0])?)?;
        if *block.observation() != reference.observation()
            || event.agent != descriptor.agent
            || event.recipient != descriptor.recipient
            || event.newSettled != reference.settled_cumulative()
            || event.cumulativeAmount < reference.settled_cumulative()
            || !state.exists()
            || state.settled_cumulative < reference.settled_cumulative()
        {
            return Err(invalid("settlement reference contradicts canonical event"));
        }
        Ok(())
    }
    pub async fn reconcile(
        &self,
        operation: &PreparedOperation,
        operation_ref: &str,
        tx_hash: Option<B256>,
        wallet_operation_id: Option<&str>,
    ) -> Result<OperationResult, ChainError> {
        self.check_operation(operation)?;
        let unknown = || {
            OperationResult::new(
                operation_ref.to_owned(),
                OperationState::Unknown,
                wallet_operation_id.map(str::to_owned),
                tx_hash,
                None,
                vec![],
            )
            .map_err(ChainError::from)
        };
        let Some(tx_hash) = tx_hash else {
            return unknown();
        };
        // Unavailable or contradictory evidence is never a terminal economic failure.
        match self
            .reconcile_receipt(operation, operation_ref, tx_hash, wallet_operation_id)
            .await
        {
            Ok(result) => Ok(result),
            Err(_) => unknown(),
        }
    }
    async fn reconcile_receipt(
        &self,
        operation: &PreparedOperation,
        operation_ref: &str,
        tx_hash: B256,
        wallet_operation_id: Option<&str>,
    ) -> Result<OperationResult, ChainError> {
        let result = |state, observation, sessions| {
            OperationResult::new(
                operation_ref.to_owned(),
                state,
                wallet_operation_id.map(str::to_owned),
                Some(tx_hash),
                observation,
                sessions,
            )
            .map_err(ChainError::from)
        };
        let Some(receipt) = self.receipt(tx_hash).await? else {
            return result(OperationState::Pending, None, vec![]);
        };
        // Check the receipt's own events before reading state at its block.
        let (events, ids) = if receipt.success {
            (
                self.operation_events(operation.kind(), &receipt, tx_hash)?,
                operation
                    .session_ids()
                    .into_iter()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            )
        } else {
            (vec![], vec![])
        };
        let (block, results) = self
            .read_at(
                receipt.number,
                receipt.block_hash,
                ids.iter().map(|id| self.session_call(*id)).collect(),
            )
            .await?;
        if !receipt.success {
            return result(OperationState::Failed, Some(block.observation), vec![]);
        }
        let mut states = HashMap::new();
        for (id, raw) in ids.iter().zip(&results) {
            states.insert(*id, parse_session(&data(raw)?)?);
        }
        let (outcomes, proven) = self.outcomes(operation, &events, &block, tx_hash, &states)?;
        result(
            if proven {
                OperationState::Confirmed
            } else {
                OperationState::Unknown
            },
            Some(block.observation),
            outcomes,
        )
    }
}
