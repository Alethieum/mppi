//! Canonical, block-pinned read evidence from a caller-selected trusted RPC.
//! Consistency checks do not authenticate an RPC or establish manifest provenance.

use crate::{
    catalog::Catalog,
    contract::{OperationKind, PreparedOperation, abi::IMPPI},
    errors::ValidationError,
    signing::{SessionDescriptor, Voucher},
    types::{
        ChainObservation, ConfirmedSettlementRef, Domain, OperationResult, OperationState,
        PauseLevel, SessionOperationOutcome, SessionStatus, validate_descriptor,
    },
};
use alloy_primitives::{Address, B256, U256, hex, keccak256};
use alloy_sol_types::{SolCall, SolEvent, abi::AbiDecoderConfig};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};

const IMPLEMENTATION_SLOT: &str =
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";
const MAX_DATA_BYTES: usize = 4 << 20;
const MAX_RECEIPT_LOGS: usize = 16_384;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChainError {
    Unavailable,
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
    async fn outcomes(
        &self,
        operation: &PreparedOperation,
        receipt: &Value,
        block: &Block,
        tx_hash: B256,
    ) -> Result<(Vec<SessionOperationOutcome>, bool), ChainError> {
        let kind = operation.kind();
        let ids = operation.session_ids();
        let events: Vec<Event> = match kind {
            OperationKind::Open => self
                .events::<IMPPI::SessionOpened>(receipt, block, tx_hash)?
                .into_iter()
                .map(Event::Opened)
                .collect(),
            OperationKind::TopUp => self
                .events::<IMPPI::ToppedUp>(receipt, block, tx_hash)?
                .into_iter()
                .map(Event::TopUp)
                .collect(),
            OperationKind::Settle | OperationKind::SettleBatch => self
                .events::<IMPPI::Settled>(receipt, block, tx_hash)?
                .into_iter()
                .map(Event::Settled)
                .collect(),
            OperationKind::RequestClose => self
                .events::<IMPPI::CloseRequested>(receipt, block, tx_hash)?
                .into_iter()
                .map(Event::CloseRequested)
                .collect(),
            OperationKind::Close | OperationKind::Withdraw => self
                .events::<IMPPI::SessionClosed>(receipt, block, tx_hash)?
                .into_iter()
                .map(Event::Closed)
                .collect(),
            OperationKind::ReleaseIdle => self
                .events::<IMPPI::IdleReleased>(receipt, block, tx_hash)?
                .into_iter()
                .map(Event::Idle)
                .collect(),
        };
        let mut multiplicities: HashMap<(B256, u64), usize> = HashMap::new();
        if kind == OperationKind::SettleBatch {
            for (id, target) in ids.iter().zip(operation.amounts()) {
                *multiplicities.entry((*id, *target)).or_default() += 1;
            }
        }
        let mut states = HashMap::new();
        let mut outcomes = Vec::new();
        let mut proven = true;
        for (index, (id, descriptor)) in ids.iter().zip(operation.descriptors()).enumerate() {
            if let std::collections::hash_map::Entry::Vacant(entry) = states.entry(*id) {
                entry.insert(self.session(*id, block).await?);
            }
            let state = &states[id];
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
        f.write_str(match self {
            Self::Unavailable => "chain evidence unavailable",
            Self::InvalidEvidence(message) => message,
        })
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
}
impl core::fmt::Debug for ChainReader {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ChainReader")
            .field("profile", &self.profile)
            .finish_non_exhaustive()
    }
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
        Self { rpc, profile }
    }
    pub fn rpc(&self) -> &dyn Rpc {
        self.rpc.as_ref()
    }
    pub fn profile(&self) -> &DeploymentProfile {
        &self.profile
    }
    async fn network(&self) -> Result<(), ChainError> {
        if quantity(&self.rpc.call("eth_chainId", json!([])).await?)?
            != U256::from(self.profile.domain.chain_id())
        {
            return Err(invalid("RPC chain differs from selected deployment"));
        }
        Ok(())
    }
    pub async fn block(&self, number: Option<u64>) -> Result<Block, ChainError> {
        self.network().await?;
        let raw = self
            .rpc
            .call(
                "eth_getBlockByNumber",
                json!([
                    number.map_or_else(|| "latest".to_owned(), |n| format!("0x{n:x}")),
                    false
                ]),
            )
            .await?;
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
    pub(crate) fn position(&self, block: &Block) -> Result<Value, ChainError> {
        if block.observation.chain_id() != self.profile.domain.chain_id()
            || block.observation.contract_address() != self.profile.domain.verifying_contract()
        {
            return Err(invalid(
                "block observation differs from selected deployment",
            ));
        }
        Ok(json!({"blockHash":encoded(block.observation.block_hash()),"requireCanonical":true}))
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
    pub(crate) async fn check_canonical(&self, block: &Block) -> Result<(), ChainError> {
        self.position(block)?;
        if self.block(Some(block.observation.block_number())).await? != *block {
            return Err(invalid("canonical block changed during observation"));
        }
        Ok(())
    }
    pub async fn verify_deployment(&self, block: &Block) -> Result<(), ChainError> {
        self.network().await?;
        for pin in [
            &self.profile.proxy,
            &self.profile.implementation,
            &self.profile.registry,
            &self.profile.registry_implementation,
        ] {
            let code = data(
                &self
                    .rpc
                    .call(
                        "eth_getCode",
                        json!([encoded(pin.address), self.position(block)?]),
                    )
                    .await?,
            )?;
            if code.is_empty() || keccak256(code) != pin.code_hash {
                return Err(invalid("deployed code differs from release profile"));
            }
        }
        for (proxy, implementation) in [
            (&self.profile.proxy, &self.profile.implementation),
            (
                &self.profile.registry,
                &self.profile.registry_implementation,
            ),
        ] {
            let slot = hash(
                &self
                    .rpc
                    .call(
                        "eth_getStorageAt",
                        json!([
                            encoded(proxy.address),
                            IMPLEMENTATION_SLOT,
                            self.position(block)?
                        ]),
                    )
                    .await?,
            )?;
            let mut expected = [0u8; 32];
            expected[12..].copy_from_slice(implementation.address.as_slice());
            if slot.as_slice() != expected {
                return Err(invalid("proxy implementation differs from release profile"));
            }
        }
        let call = IMPPI::voucherDigestCall {
            sessionId: B256::ZERO,
            cumulativeAmount: 0,
        };
        let digest = decode_return::<IMPPI::voucherDigestCall>(
            &self
                .call_at(
                    self.profile.domain.verifying_contract(),
                    &call.abi_encode(),
                    block,
                )
                .await?,
        )?;
        if digest
            != self.profile.domain.signing_context().digest(&Voucher {
                sessionId: B256::ZERO,
                cumulativeAmount: 0,
            })
        {
            return Err(invalid("MPPI signing domain differs from release profile"));
        }
        self.check_canonical(block).await
    }
    pub async fn session(&self, id: B256, block: &Block) -> Result<ChainSession, ChainError> {
        let call = IMPPI::getSessionCall { sessionId: id };
        let result = decode_return::<IMPPI::getSessionCall>(
            &self
                .call_at(
                    self.profile.domain.verifying_contract(),
                    &call.abi_encode(),
                    block,
                )
                .await?,
        )?;
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
    pub async fn pause_level(&self, block: &Block) -> Result<PauseLevel, ChainError> {
        let result = decode_return::<IMPPI::pauseLevelCall>(
            &self
                .call_at(
                    self.profile.domain.verifying_contract(),
                    &IMPPI::pauseLevelCall {}.abi_encode(),
                    block,
                )
                .await?,
        )?;
        match result {
            IMPPI::PauseLevel::None => Ok(PauseLevel::None),
            IMPPI::PauseLevel::Payments => Ok(PauseLevel::Payments),
            IMPPI::PauseLevel::All => Ok(PauseLevel::All),
            _ => Err(invalid("invalid MPPI pause level")),
        }
    }
    pub async fn simulate(
        &self,
        operation: &PreparedOperation,
        sender: Address,
    ) -> Result<SimulationEvidence, ChainError> {
        self.check_operation(operation)?;
        if sender.is_zero() {
            return Err(invalid("simulation sender must be nonzero"));
        }
        let block = self.block(None).await?;
        self.verify_deployment(&block).await?;
        let number = format!("0x{:x}", block.observation.block_number());
        let raw = self
            .rpc
            .call("eth_getBlockByNumber", json!([number, false]))
            .await?;
        if hash(&raw["hash"])? != block.observation.block_hash() {
            return Err(invalid("simulation block changed"));
        }
        let gas_limit = quantity(&raw["gasLimit"])?;
        if gas_limit.is_zero() {
            return Err(invalid("simulation block has no gas capacity"));
        }
        let call = json!({"from":encoded(sender),"to":encoded(operation.call().to()),"data":encoded(operation.call().data()),"value":"0x0","gas":format!("0x{gas_limit:x}")});
        if !data(
            &self
                .rpc
                .call("eth_call", json!([call, self.position(&block)?]))
                .await?,
        )?
        .is_empty()
        {
            return Err(invalid("MPPI simulation returned unexpected data"));
        }
        let gas_estimate = quantity(
            &self
                .rpc
                .call("eth_estimateGas", json!([call, number]))
                .await?,
        )?;
        if gas_estimate.is_zero() || gas_estimate > gas_limit {
            return Err(invalid("simulation estimate exceeds block capacity"));
        }
        self.check_canonical(&block).await?;
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
    async fn receipt(&self, tx_hash: B256) -> Result<Option<(Value, Block, bool)>, ChainError> {
        self.network().await?;
        let raw = self
            .rpc
            .call("eth_getTransactionReceipt", json!([encoded(tx_hash)]))
            .await?;
        if raw.is_null() {
            return Ok(None);
        }
        if hash(&raw["transactionHash"])? != tx_hash {
            return Err(invalid("receipt transaction mismatch"));
        }
        let block = self.block(Some(quantity_u64(&raw["blockNumber"])?)).await?;
        if hash(&raw["blockHash"])? != block.observation.block_hash() {
            return Err(invalid("receipt is not canonical"));
        }
        let status = quantity_u64(&raw["status"])?;
        if status > 1 {
            return Err(invalid("invalid receipt status"));
        }
        Ok(Some((raw, block, status == 1)))
    }
    fn events<E: SolEvent>(
        &self,
        receipt: &Value,
        block: &Block,
        tx_hash: B256,
    ) -> Result<Vec<E>, ChainError> {
        let logs = receipt["logs"]
            .as_array()
            .ok_or_else(|| invalid("receipt logs missing"))?;
        if logs.len() > MAX_RECEIPT_LOGS {
            return Err(invalid("receipt log limit exceeded"));
        }
        let tx_index = quantity(&receipt["transactionIndex"])?;
        let mut previous = None;
        let mut events = Vec::new();
        for log in logs {
            let index = quantity(&log["logIndex"])?;
            if previous.is_some_and(|p| index <= p)
                || log["removed"] != Value::Bool(false)
                || hash(&log["transactionHash"])? != tx_hash
                || quantity(&log["transactionIndex"])? != tx_index
                || quantity_u64(&log["blockNumber"])? != block.observation.block_number()
                || hash(&log["blockHash"])? != block.observation.block_hash()
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
    async fn event<E: SolEvent>(
        &self,
        tx_hash: B256,
        select: impl Fn(&E) -> bool,
    ) -> Result<(Block, E), ChainError> {
        let (receipt, block, success) = self
            .receipt(tx_hash)
            .await?
            .ok_or_else(|| invalid("receipt unavailable"))?;
        if !success {
            return Err(invalid("successful canonical receipt required"));
        }
        let mut events = self
            .events::<E>(&receipt, &block, tx_hash)?
            .into_iter()
            .filter(select);
        let event = events
            .next()
            .ok_or_else(|| invalid("required MPPI event missing"))?;
        if events.next().is_some() {
            return Err(invalid("required MPPI event ambiguous"));
        }
        self.check_canonical(&block).await?;
        Ok((block, event))
    }
    pub async fn opening(
        &self,
        tx_hash: B256,
        session_id: B256,
        accepted_catalog: Option<&Catalog>,
    ) -> Result<OpeningEvidence, ChainError> {
        let (block, event) = self
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
        let state = self.session(session_id, &block).await?;
        let catalog = match accepted_catalog {
            Some(catalog) => catalog.clone(),
            None => self
                .identity(descriptor.providerId, &block, false)
                .await?
                .catalog()
                .cloned()
                .ok_or_else(|| invalid("opening catalog unavailable"))?,
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
        self.check_canonical(&block).await?;
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
        let (block, event) = self
            .event::<IMPPI::SessionClosed>(tx_hash, |e| e.sessionId == session_id)
            .await?;
        let state = self.session(session_id, &block).await?;
        if !state.exists()
            || state.status != SessionStatus::Closed
            || event.captureAmount != state.settled_cumulative
            || event.captureAmount.checked_add(event.refunded) != Some(state.deposit)
        {
            return Err(invalid("closing event contradicts canonical state"));
        }
        self.check_canonical(&block).await?;
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
        let (block, event) = self
            .event::<IMPPI::Settled>(reference.tx_hash(), |e| {
                e.sessionId == reference.session_id()
                    && e.captureAmount == reference.settled_cumulative()
                    && e.deltaPaid == reference.delta()
            })
            .await?;
        let state = self.session(reference.session_id(), &block).await?;
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
        self.check_canonical(&block).await
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
        let Some((receipt, block, success)) = self.receipt(tx_hash).await? else {
            return result(OperationState::Pending, None, vec![]);
        };
        if !success {
            self.check_canonical(&block).await?;
            return result(OperationState::Failed, Some(block.observation), vec![]);
        }
        let (outcomes, proven) = self.outcomes(operation, &receipt, &block, tx_hash).await?;
        self.check_canonical(&block).await?;
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
