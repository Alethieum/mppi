//! Exact ERC-8004 calls and canonical identity evidence. No wallet is created,
//! transaction sent, or operation persisted here. The caller retains each intent
//! before passing its call to a transaction wallet.
//!
//! ABI/proof profile: erc-8004/erc-8004-contracts at
//! b9e466c250744a7e06b13dff9d3c2844ed64f825, IdentityRegistryUpgradeable.sol.
use crate::{
    Address, B256, Catalog, U256, ValidationCode, ValidationError,
    catalog::{DEFAULT_CATALOG_BYTES, parse_catalog},
    chain::{Block, Call, ChainError, ChainReader, decode_return, eth_call},
    discovery::{MAX_DOCUMENT_BYTES, ProviderIdentity},
};
use alloy_primitives::keccak256;
use alloy_sol_types::{SolCall, SolEvent, abi::AbiDecoderConfig, sol};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::{
    chain::{address, data, encoded, hash, quantity, quantity_u64},
    types::{ChainObservation, OperationResult, OperationState, WalletCall},
};

sol! {
    interface IdentityReads {
        function ownerOf(uint256 agentId) external view returns (address);
        function getAgentWallet(uint256 agentId) external view returns (address);
        function tokenURI(uint256 agentId) external view returns (string);
        function getMetadata(uint256 agentId,string key) external view returns (bytes);
    }
    interface IdentityWrites {
        function register() external returns (uint256);
        function register(string agentURI) external returns (uint256);
        function setAgentWallet(uint256 agentId, address newWallet, uint256 deadline, bytes signature) external;
        function setAgentURI(uint256 agentId, string newURI) external;
        function setMetadata(uint256 agentId, string key, bytes value) external;
        function eip712Domain() external view returns (bytes1 fields, string name, string version, uint256 chainId, address verifyingContract, bytes32 salt, uint256[] extensions);
        event Registered(uint256 indexed agentId, string agentURI, address indexed owner);
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event MetadataSet(uint256 indexed agentId, string indexed indexedMetadataKey, string metadataKey, bytes metadataValue);
        event URIUpdated(uint256 indexed agentId, string newURI, address indexed updatedBy);
    }
}

const MAX_SIGNATURE_BYTES: usize = 64 << 10;
const MAX_RECEIPT_LOGS: usize = 16_384;
const MAX_EVENT_BYTES: usize = DEFAULT_CATALOG_BYTES + MAX_DOCUMENT_BYTES + 256;
const PROOF_NAME: &str = "ERC8004IdentityRegistry";
const PROOF_VERSION: &str = "1";

fn invalid(message: &'static str) -> ChainError {
    ChainError::InvalidEvidence(message)
}
fn validation(message: &'static str) -> ValidationError {
    ValidationError::new(ValidationCode::InvalidValue, message)
}
fn nonzero(value: Address) -> Result<(), ValidationError> {
    if value.is_zero() {
        return Err(validation("registry address must be nonzero"));
    }
    Ok(())
}
fn uri(value: &str) -> Result<(), ValidationError> {
    if value.len() > MAX_DOCUMENT_BYTES {
        return Err(validation("registry URI exceeds document limit"));
    }
    Ok(())
}

/// A registry operation's field-specific effect, separate from MPPI payments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegistryKind {
    Register,
    SetAgentWallet,
    SetUri,
    SetCatalog,
}

#[derive(Clone, PartialEq, Eq)]
enum RegistryIntent {
    Register(Option<String>),
    SetAgentWallet {
        agent_id: U256,
        new_wallet: Address,
        deadline: U256,
        signature: Vec<u8>,
    },
    SetUri {
        agent_id: U256,
        uri: String,
    },
    SetCatalog {
        agent_id: U256,
        bytes: Vec<u8>,
    },
}

/// Immutable intent with calldata derived from its validated fields. Reconstruct
/// it with `RegistryCalls` when checking a retained command record.
#[derive(Clone, PartialEq, Eq)]
pub struct RegistryOperation {
    call: WalletCall,
    owner: Address,
    intent: RegistryIntent,
}
impl core::fmt::Debug for RegistryOperation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RegistryOperation")
            .field("kind", &self.kind())
            .field("owner", &self.owner)
            .field("agent_id", &self.agent_id())
            .finish_non_exhaustive()
    }
}
impl RegistryOperation {
    pub fn call(&self) -> &WalletCall {
        &self.call
    }
    pub fn owner(&self) -> Address {
        self.owner
    }
    pub fn kind(&self) -> RegistryKind {
        match self.intent {
            RegistryIntent::Register(_) => RegistryKind::Register,
            RegistryIntent::SetAgentWallet { .. } => RegistryKind::SetAgentWallet,
            RegistryIntent::SetUri { .. } => RegistryKind::SetUri,
            RegistryIntent::SetCatalog { .. } => RegistryKind::SetCatalog,
        }
    }
    pub fn agent_id(&self) -> Option<U256> {
        match self.intent {
            RegistryIntent::Register(_) => None,
            RegistryIntent::SetAgentWallet { agent_id, .. }
            | RegistryIntent::SetUri { agent_id, .. }
            | RegistryIntent::SetCatalog { agent_id, .. } => Some(agent_id),
        }
    }
    pub fn agent_uri(&self) -> Option<&str> {
        match &self.intent {
            RegistryIntent::Register(uri) => uri.as_deref(),
            RegistryIntent::SetUri { uri, .. } => Some(uri),
            _ => None,
        }
    }
    pub fn new_wallet(&self) -> Option<Address> {
        match self.intent {
            RegistryIntent::SetAgentWallet { new_wallet, .. } => Some(new_wallet),
            _ => None,
        }
    }
    pub fn deadline(&self) -> Option<U256> {
        match self.intent {
            RegistryIntent::SetAgentWallet { deadline, .. } => Some(deadline),
            _ => None,
        }
    }
    pub fn signature(&self) -> Option<&[u8]> {
        match &self.intent {
            RegistryIntent::SetAgentWallet { signature, .. } => Some(signature),
            _ => None,
        }
    }
    pub fn catalog_bytes(&self) -> Option<&[u8]> {
        match &self.intent {
            RegistryIntent::SetCatalog { bytes, .. } => Some(bytes),
            _ => None,
        }
    }
}

/// Prepare registry calls without reading state, signing, submitting or retrying.
#[derive(Clone, Copy, Debug)]
pub struct RegistryCalls {
    chain_id: u64,
    registry: Address,
}
impl RegistryCalls {
    pub fn new(chain_id: u64, registry: Address) -> Result<Self, ValidationError> {
        if !matches!(chain_id, 5042 | 5042002) {
            return Err(validation("registry requires Arc"));
        }
        nonzero(registry)?;
        Ok(Self { chain_id, registry })
    }
    fn prepare(
        &self,
        owner: Address,
        intent: RegistryIntent,
        data: Vec<u8>,
    ) -> Result<RegistryOperation, ValidationError> {
        nonzero(owner)?;
        Ok(RegistryOperation {
            call: WalletCall::new(self.chain_id, self.registry, data)?,
            owner,
            intent,
        })
    }
    pub fn register(
        &self,
        owner: Address,
        agent_uri: Option<&str>,
    ) -> Result<RegistryOperation, ValidationError> {
        let data = match agent_uri {
            Some(value) => {
                uri(value)?;
                IdentityWrites::register_1Call {
                    agentURI: value.into(),
                }
                .abi_encode()
            }
            None => IdentityWrites::register_0Call {}.abi_encode(),
        };
        self.prepare(
            owner,
            RegistryIntent::Register(agent_uri.map(str::to_owned)),
            data,
        )
    }
    pub fn set_agent_wallet(
        &self,
        owner: Address,
        agent_id: U256,
        new_wallet: Address,
        deadline: U256,
        signature: &[u8],
    ) -> Result<RegistryOperation, ValidationError> {
        nonzero(new_wallet)?;
        if signature.is_empty() || signature.len() > MAX_SIGNATURE_BYTES {
            return Err(validation("invalid registry wallet proof size"));
        }
        let data = IdentityWrites::setAgentWalletCall {
            agentId: agent_id,
            newWallet: new_wallet,
            deadline,
            signature: signature.to_vec().into(),
        }
        .abi_encode();
        self.prepare(
            owner,
            RegistryIntent::SetAgentWallet {
                agent_id,
                new_wallet,
                deadline,
                signature: signature.to_vec(),
            },
            data,
        )
    }
    pub fn set_uri(
        &self,
        owner: Address,
        agent_id: U256,
        agent_uri: &str,
    ) -> Result<RegistryOperation, ValidationError> {
        uri(agent_uri)?;
        let data = IdentityWrites::setAgentURICall {
            agentId: agent_id,
            newURI: agent_uri.into(),
        }
        .abi_encode();
        self.prepare(
            owner,
            RegistryIntent::SetUri {
                agent_id,
                uri: agent_uri.into(),
            },
            data,
        )
    }
    pub fn set_catalog(
        &self,
        owner: Address,
        agent_id: U256,
        catalog: &Catalog,
    ) -> Result<RegistryOperation, ValidationError> {
        let bytes = catalog.canonical_bytes().to_vec();
        if parse_catalog(&bytes, DEFAULT_CATALOG_BYTES)? != *catalog {
            return Err(validation("catalog differs from canonical bytes"));
        }
        let data = IdentityWrites::setMetadataCall {
            agentId: agent_id,
            key: "mppi.catalog".into(),
            value: bytes.clone().into(),
        }
        .abi_encode();
        self.prepare(owner, RegistryIntent::SetCatalog { agent_id, bytes }, data)
    }
}

fn json_uint(value: U256) -> Value {
    // arbitrary_precision preserves all uint256 digits; decimal output is valid JSON.
    Value::Number(
        value
            .to_string()
            .parse()
            .expect("uint256 decimal is a JSON integer"),
    )
}

/// Construct exact recipient-signed EIP-712 data. This pure function does not
/// establish the registry domain, owner, deadline validity or signature validity.
pub fn agent_wallet_proof(
    chain_id: u64,
    registry: Address,
    agent_id: U256,
    new_wallet: Address,
    owner: Address,
    deadline: U256,
) -> Result<Value, ValidationError> {
    RegistryCalls::new(chain_id, registry)?;
    nonzero(new_wallet)?;
    nonzero(owner)?;
    Ok(json!({
        "types": {
            "EIP712Domain":[{"name":"name","type":"string"},{"name":"version","type":"string"},{"name":"chainId","type":"uint256"},{"name":"verifyingContract","type":"address"}],
            "AgentWalletSet":[{"name":"agentId","type":"uint256"},{"name":"newWallet","type":"address"},{"name":"owner","type":"address"},{"name":"deadline","type":"uint256"}]
        },
        "primaryType":"AgentWalletSet",
        "domain":{"name":PROOF_NAME,"version":PROOF_VERSION,"chainId":chain_id,"verifyingContract":encoded(registry)},
        "message":{"agentId":json_uint(agent_id),"newWallet":encoded(new_wallet),"owner":encoded(owner),"deadline":json_uint(deadline)}
    }))
}

/// Raw canonical identity, including intentionally empty onboarding fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryIdentity {
    observation: ChainObservation,
    agent_id: U256,
    owner: Address,
    agent_wallet: Address,
    agent_uri: String,
    catalog_bytes: Vec<u8>,
}
impl RegistryIdentity {
    pub fn observation(&self) -> &ChainObservation {
        &self.observation
    }
    pub fn agent_id(&self) -> U256 {
        self.agent_id
    }
    pub fn owner(&self) -> Address {
        self.owner
    }
    pub fn agent_wallet(&self) -> Address {
        self.agent_wallet
    }
    pub fn agent_uri(&self) -> &str {
        &self.agent_uri
    }
    pub fn catalog_bytes(&self) -> &[u8] {
        &self.catalog_bytes
    }
}

#[derive(Clone, Debug)]
pub struct RegistryResult {
    operation: OperationResult,
    identity: Option<RegistryIdentity>,
}
impl RegistryResult {
    pub fn operation(&self) -> &OperationResult {
        &self.operation
    }
    pub fn identity(&self) -> Option<&RegistryIdentity> {
        self.identity.as_ref()
    }
}

/// Read-only setup adapter. All proofs and receipts are checked against the
/// supplied trusted deployment profile and canonical RPC observations.
#[derive(Clone, Debug)]
pub struct RegistryReader {
    chain: Arc<ChainReader>,
}
impl RegistryReader {
    pub fn new(chain: Arc<ChainReader>) -> Self {
        Self { chain }
    }
    pub fn chain(&self) -> &Arc<ChainReader> {
        &self.chain
    }
    pub async fn verified_block(&self) -> Result<Block, ChainError> {
        let block = self.chain.block(None).await?;
        self.chain.verify_deployment(&block).await?;
        Ok(block)
    }
    /// The deployment check and `calls` at `block`, or at the head, in one
    /// exchange; the results that follow the deployment check.
    async fn read_verified(
        &self,
        block: Option<&Block>,
        calls: Vec<Call>,
    ) -> Result<(Block, Vec<Value>), ChainError> {
        let block = match block {
            Some(block) => block.clone(),
            None => self.chain.block(None).await?,
        };
        let with_code = self.chain.code_unchecked();
        let mut all = self.chain.deployment_calls(with_code);
        all.extend(calls);
        let results = self.chain.read(&block, all).await?;
        let results = self.chain.check_deployment(&results, with_code)?.to_vec();
        Ok((block, results))
    }
    /// The raw identity from the results of `identity_calls`.
    fn identity_at(
        &self,
        agent_id: U256,
        block: &Block,
        results: &[Value],
    ) -> Result<RegistryIdentity, ChainError> {
        let [owner, agent_wallet, agent_uri, bytes] = results else {
            return Err(invalid("RPC batch response count mismatch"));
        };
        let owner = decode_return::<IdentityReads::ownerOfCall>(&data(owner)?)?;
        if owner.is_zero() {
            return Err(invalid("registry identity owner is zero"));
        }
        let agent_wallet =
            decode_return::<IdentityReads::getAgentWalletCall>(&data(agent_wallet)?)?;
        let agent_uri = decode_return::<IdentityReads::tokenURICall>(&data(agent_uri)?)?;
        uri(&agent_uri)?;
        let bytes = decode_return::<IdentityReads::getMetadataCall>(&data(bytes)?)?;
        if bytes.len() > DEFAULT_CATALOG_BYTES {
            return Err(invalid("registry catalog exceeds resource limit"));
        }
        Ok(RegistryIdentity {
            observation: *block.observation(),
            agent_id,
            owner,
            agent_wallet,
            agent_uri,
            catalog_bytes: bytes.to_vec(),
        })
    }
    pub async fn read(
        &self,
        agent_id: U256,
        block: Option<&Block>,
    ) -> Result<RegistryIdentity, ChainError> {
        let (block, results) = self
            .read_verified(block, self.chain.identity_calls(agent_id))
            .await?;
        self.identity_at(agent_id, &block, &results)
    }
    fn proof_domain_call(&self) -> Call {
        eth_call(
            self.chain.profile().registry_address(),
            &IdentityWrites::eip712DomainCall {}.abi_encode(),
        )
    }
    /// The registry's signing domain, and a deadline inside its window at `block`.
    fn check_proof_context(
        &self,
        domain: &Value,
        block: &Block,
        deadline: U256,
    ) -> Result<(), ChainError> {
        let profile = self.chain.profile();
        let domain = decode_return::<IdentityWrites::eip712DomainCall>(&data(domain)?)?;
        if domain.fields.as_slice() != [0x0f]
            || domain.name != PROOF_NAME
            || domain.version != PROOF_VERSION
            || domain.chainId != U256::from(profile.domain().chain_id())
            || domain.verifyingContract != profile.registry_address()
            || !domain.salt.is_zero()
            || !domain.extensions.is_empty()
        {
            return Err(invalid(
                "registry signing domain differs from selected profile",
            ));
        }
        let timestamp = U256::from(block.timestamp());
        if deadline < timestamp || deadline > timestamp + U256::from(300) {
            return Err(invalid("recipient proof deadline outside registry window"));
        }
        Ok(())
    }
    pub async fn wallet_proof(
        &self,
        agent_id: U256,
        new_wallet: Address,
        owner: Address,
        deadline: U256,
        block: Option<&Block>,
    ) -> Result<Value, ChainError> {
        let profile = self.chain.profile();
        let proof = agent_wallet_proof(
            profile.domain().chain_id(),
            profile.registry_address(),
            agent_id,
            new_wallet,
            owner,
            deadline,
        )?;
        let mut calls = vec![self.proof_domain_call()];
        calls.extend(self.chain.identity_calls(agent_id));
        let (block, results) = self.read_verified(block, calls).await?;
        self.check_proof_context(&results[0], &block, deadline)?;
        if self.identity_at(agent_id, &block, &results[1..])?.owner != owner {
            return Err(invalid("registry owner differs from intended caller"));
        }
        Ok(proof)
    }
    fn check_operation(&self, operation: &RegistryOperation) -> Result<(), ChainError> {
        if operation.call.chain_id() != self.chain.profile().domain().chain_id()
            || operation.call.to() != self.chain.profile().registry_address()
        {
            return Err(invalid("registry operation differs from selected profile"));
        }
        Ok(())
    }
    /// Identity owner and recipient proof context at the head, then the exact
    /// call from the owner at that block: nothing is simulated for a caller
    /// the identity does not name.
    pub async fn simulate(&self, operation: &RegistryOperation) -> Result<Block, ChainError> {
        self.check_operation(operation)?;
        let mut calls = Vec::new();
        if let Some(id) = operation.agent_id() {
            calls.extend(self.chain.identity_calls(id));
        }
        if let RegistryIntent::SetAgentWallet {
            agent_id,
            new_wallet,
            deadline,
            ..
        } = operation.intent
        {
            let profile = self.chain.profile();
            agent_wallet_proof(
                profile.domain().chain_id(),
                profile.registry_address(),
                agent_id,
                new_wallet,
                operation.owner,
                deadline,
            )?;
            calls.push(self.proof_domain_call());
        }
        let (block, results) = self.read_verified(None, calls).await?;
        let mut results = &results[..];
        if let Some(id) = operation.agent_id() {
            if self.identity_at(id, &block, &results[..4])?.owner != operation.owner {
                return Err(invalid("registry caller must be identity owner"));
            }
            results = &results[4..];
        }
        if let RegistryIntent::SetAgentWallet { deadline, .. } = operation.intent {
            self.check_proof_context(&results[0], &block, deadline)?;
        }
        let simulated = self
            .chain
            .read(
                &block,
                vec![("eth_call", json!([{"from":encoded(operation.owner),"to":encoded(operation.call.to()),"data":encoded(operation.call.data()),"value":"0x0"}]))],
            )
            .await?;
        let raw = data(&simulated[0])?;
        if operation.kind() == RegistryKind::Register {
            decode_return::<IdentityWrites::register_0Call>(&raw)?;
        } else if !raw.is_empty() {
            return Err(invalid("unexpected registry simulation output"));
        }
        Ok(block)
    }
    /// Only exact canonical registry effects confirm a transaction. Unknown
    /// evidence preserves the original handles and never submits anything.
    pub async fn reconcile(
        &self,
        operation: &RegistryOperation,
        operation_ref: &str,
        tx_hash: Option<B256>,
        wallet_operation_id: Option<&str>,
    ) -> Result<RegistryResult, ChainError> {
        self.check_operation(operation)?;
        let result = |state, observation, identity| -> Result<RegistryResult, ChainError> {
            Ok(RegistryResult {
                operation: OperationResult::new(
                    operation_ref,
                    state,
                    wallet_operation_id.map(str::to_owned),
                    tx_hash,
                    observation,
                    vec![],
                )?,
                identity,
            })
        };
        // Check caller identifiers even when no chain observation is available.
        let unknown = result(OperationState::Unknown, None, None)?;
        let Some(tx_hash) = tx_hash else {
            return Ok(unknown);
        };
        match self.reconcile_inner(operation, tx_hash).await {
            Ok(None) => result(OperationState::Pending, None, None),
            Ok(Some((block, identity))) => result(
                if identity.is_some() {
                    OperationState::Confirmed
                } else {
                    OperationState::Failed
                },
                Some(*block.observation()),
                identity,
            ),
            Err(_) => Ok(unknown),
        }
    }
    async fn reconcile_inner(
        &self,
        operation: &RegistryOperation,
        tx_hash: B256,
    ) -> Result<Option<(Block, Option<RegistryIdentity>)>, ChainError> {
        let raw = self
            .chain
            .many(vec![(
                "eth_getTransactionReceipt",
                json!([encoded(tx_hash)]),
            )])
            .await?
            .remove(0);
        if raw.is_null() {
            // A missing receipt still needs the selected network established.
            self.verified_block().await?;
            return Ok(None);
        }
        if hash(&raw["transactionHash"])? != tx_hash {
            return Err(invalid("registry receipt is not canonical"));
        }
        let number = quantity_u64(&raw["blockNumber"])?;
        let block_hash = hash(&raw["blockHash"])?;
        let status = quantity_u64(&raw["status"])?;
        if status > 1 {
            return Err(invalid("invalid registry receipt status"));
        }
        let with_code = self.chain.code_unchecked();
        let mut calls = self.chain.deployment_calls(with_code);
        let confirmed = if status == 1 {
            let events = self.events(&raw, number, block_hash, tx_hash)?;
            let agent_id = self.confirmed_agent_id(operation, &events)?;
            calls.extend(self.chain.identity_calls(agent_id));
            Some((events, agent_id))
        } else {
            None
        };
        let (block, results) = self.chain.read_at(number, block_hash, calls).await?;
        let results = self.chain.check_deployment(&results, with_code)?;
        let Some((events, agent_id)) = confirmed else {
            return Ok(Some((block, None)));
        };
        let identity = self.identity_at(agent_id, &block, results)?;
        Ok(Some((
            block,
            Some(self.confirmed_identity(operation, &events, identity)?),
        )))
    }
    fn events(
        &self,
        receipt: &Value,
        number: u64,
        block_hash: B256,
        tx_hash: B256,
    ) -> Result<Vec<RegistryLog>, ChainError> {
        let logs = receipt["logs"]
            .as_array()
            .ok_or_else(|| invalid("missing registry receipt logs"))?;
        if logs.len() > MAX_RECEIPT_LOGS {
            return Err(invalid("registry receipt log limit exceeded"));
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
                || quantity_u64(&log["blockNumber"])? != number
                || hash(&log["blockHash"])? != block_hash
            {
                return Err(invalid("registry event inclusion or order mismatch"));
            }
            previous = Some(index);
            if address(&log["address"])? != self.chain.profile().registry_address() {
                continue;
            }
            let topics = log["topics"]
                .as_array()
                .ok_or_else(|| invalid("invalid registry event topics"))?;
            if topics.is_empty() || topics.len() > 4 {
                return Err(invalid("invalid registry event topic count"));
            }
            let topics = topics.iter().map(hash).collect::<Result<Vec<_>, _>>()?;
            if ![
                IdentityWrites::Registered::SIGNATURE_HASH,
                IdentityWrites::Transfer::SIGNATURE_HASH,
                IdentityWrites::MetadataSet::SIGNATURE_HASH,
                IdentityWrites::URIUpdated::SIGNATURE_HASH,
            ]
            .contains(&topics[0])
            {
                continue;
            }
            if log["data"]
                .as_str()
                .is_none_or(|raw| raw.len() > 2 * MAX_EVENT_BYTES + 2)
            {
                return Err(invalid("registry event exceeds resource limit"));
            }
            events.push(RegistryLog {
                topics,
                data: data(&log["data"])?,
            });
        }
        Ok(events)
    }
    /// The identity the receipt's events name: the one registration for this
    /// owner and URI, or the operation's own identity.
    fn confirmed_agent_id(
        &self,
        operation: &RegistryOperation,
        events: &[RegistryLog],
    ) -> Result<U256, ChainError> {
        if operation.kind() == RegistryKind::Register {
            let mut ids = Vec::new();
            for log in events
                .iter()
                .filter(|log| log.topics[0] == IdentityWrites::Registered::SIGNATURE_HASH)
            {
                if log.topics.len() != 3 {
                    return Err(invalid("invalid Registered topic count"));
                }
                // Ignore other users' arbitrary Solidity strings before decoding.
                if topic_address(log.topics[2])? != operation.owner {
                    continue;
                }
                let event: IdentityWrites::Registered = log.decode()?;
                if event.agentURI == operation.agent_uri().unwrap_or("") {
                    ids.push(event.agentId);
                }
            }
            if ids.len() != 1 {
                return Err(invalid("registration event missing or ambiguous"));
            }
            Ok(ids[0])
        } else {
            operation
                .agent_id()
                .ok_or_else(|| invalid("registry identity missing"))
        }
    }
    /// Only the intended effects, as events and as state read at their block.
    fn confirmed_identity(
        &self,
        operation: &RegistryOperation,
        events: &[RegistryLog],
        identity: RegistryIdentity,
    ) -> Result<RegistryIdentity, ChainError> {
        let agent_id = identity.agent_id;
        let mut mints = 0usize;
        let mut metadata_matches = 0usize;
        let mut uri_matches = 0usize;
        let key = if operation.kind() == RegistryKind::SetCatalog {
            "mppi.catalog"
        } else {
            "agentWallet"
        };
        let wanted_metadata = match &operation.intent {
            RegistryIntent::Register(_) => operation.owner.as_slice(),
            RegistryIntent::SetAgentWallet { new_wallet, .. } => new_wallet.as_slice(),
            RegistryIntent::SetCatalog { bytes, .. } => bytes.as_slice(),
            RegistryIntent::SetUri { .. } => &[],
        };
        for log in events {
            let kind = log.topics[0];
            if kind == IdentityWrites::Registered::SIGNATURE_HASH {
                continue;
            }
            if kind == IdentityWrites::Transfer::SIGNATURE_HASH {
                if log.topics.len() != 4 || !log.data.is_empty() {
                    return Err(invalid("invalid registry transfer encoding"));
                }
                if U256::from_be_slice(log.topics[3].as_slice()) != agent_id {
                    continue;
                }
                let event: IdentityWrites::Transfer = log.decode()?;
                if event.from.is_zero() && event.to == operation.owner {
                    mints += 1;
                }
                continue;
            }
            if log.topics.len() != 3 {
                return Err(invalid("invalid registry event topic count"));
            }
            if U256::from_be_slice(log.topics[1].as_slice()) != agent_id {
                continue;
            }
            if kind == IdentityWrites::URIUpdated::SIGNATURE_HASH
                && operation.kind() == RegistryKind::SetUri
            {
                let event: IdentityWrites::URIUpdated = log.decode()?;
                if Some(event.newURI.as_str()) == operation.agent_uri()
                    && event.updatedBy == operation.owner
                {
                    uri_matches += 1;
                }
            } else if kind == IdentityWrites::MetadataSet::SIGNATURE_HASH
                && log.topics[2] == keccak256(key)
            {
                let event: IdentityWrites::MetadataSet = log.decode()?;
                if keccak256(event.metadataKey.as_bytes()) != log.topics[2] {
                    return Err(invalid("registry metadata key hash mismatch"));
                }
                if event.metadataKey == key && event.metadataValue.as_ref() == wanted_metadata {
                    metadata_matches += 1;
                }
            }
        }
        if identity.owner != operation.owner {
            return Err(invalid("confirmed registry owner differs from intent"));
        }
        let matches = match &operation.intent {
            RegistryIntent::Register(_) => {
                mints == 1
                    && metadata_matches == 1
                    && identity.agent_wallet == operation.owner
                    && identity.agent_uri == operation.agent_uri().unwrap_or("")
                    && identity.catalog_bytes.is_empty()
            }
            RegistryIntent::SetAgentWallet { new_wallet, .. } => {
                metadata_matches == 1 && identity.agent_wallet == *new_wallet
            }
            RegistryIntent::SetUri { uri, .. } => uri_matches == 1 && identity.agent_uri == *uri,
            RegistryIntent::SetCatalog { bytes, .. } => {
                metadata_matches == 1 && identity.catalog_bytes == *bytes
            }
        };
        if !matches {
            return Err(invalid("registry events and intended state differ"));
        }
        Ok(identity)
    }
}

struct RegistryLog {
    topics: Vec<B256>,
    data: Vec<u8>,
}
impl RegistryLog {
    fn decode<E: SolEvent>(&self) -> Result<E, ChainError> {
        let result = E::decode_raw_log_with_config(
            self.topics.iter().copied(),
            &self.data,
            AbiDecoderConfig::new()
                .strict(true)
                .validate(true)
                .memory_limit(MAX_EVENT_BYTES),
        )
        .map_err(|_| invalid("invalid registry event ABI"))?;
        let encoded = result.encode_log_data();
        if encoded.topics() != self.topics || encoded.data.as_ref() != self.data {
            return Err(invalid("noncanonical registry event ABI"));
        }
        Ok(result)
    }
}
fn topic_address(topic: B256) -> Result<Address, ChainError> {
    if topic[..12] != [0; 12] {
        return Err(invalid("noncanonical registry address topic"));
    }
    Ok(Address::from_slice(&topic[12..]))
}
impl ChainReader {
    /// Registry calldata for a provider's owner, recipient, URI and catalog.
    pub(crate) fn identity_data(provider_id: U256) -> [Vec<u8>; 4] {
        [
            IdentityReads::ownerOfCall {
                agentId: provider_id,
            }
            .abi_encode(),
            IdentityReads::getAgentWalletCall {
                agentId: provider_id,
            }
            .abi_encode(),
            IdentityReads::tokenURICall {
                agentId: provider_id,
            }
            .abi_encode(),
            IdentityReads::getMetadataCall {
                agentId: provider_id,
                key: "mppi.catalog".into(),
            }
            .abi_encode(),
        ]
    }
    pub(crate) fn identity_calls(&self, provider_id: U256) -> Vec<Call> {
        let registry = self.profile().registry_address();
        Self::identity_data(provider_id)
            .iter()
            .map(|calldata| eth_call(registry, calldata))
            .collect()
    }
    /// The identity from the return data of [`Self::identity_data`] at `block`.
    /// Only malformed or unavailable catalog content may be tolerated.
    pub(crate) fn parse_identity(
        &self,
        provider_id: U256,
        block: &Block,
        raw: &[Vec<u8>],
        allow_unavailable_catalog: bool,
    ) -> Result<ProviderIdentity, ChainError> {
        let [owner, recipient, uri, catalog] = raw else {
            return Err(ChainError::InvalidEvidence(
                "RPC batch response count mismatch",
            ));
        };
        let owner = decode_return::<IdentityReads::ownerOfCall>(owner)?;
        let recipient = decode_return::<IdentityReads::getAgentWalletCall>(recipient)?;
        let uri = decode_return::<IdentityReads::tokenURICall>(uri)?;
        if uri.len() > MAX_DOCUMENT_BYTES {
            return Err(ChainError::InvalidEvidence(
                "registry URI exceeds document limit",
            ));
        }
        let raw = decode_return::<IdentityReads::getMetadataCall>(catalog)?;
        let catalog = match parse_catalog(&raw, DEFAULT_CATALOG_BYTES) {
            Ok(catalog) => Some(catalog),
            Err(_) if allow_unavailable_catalog => None,
            Err(_) => return Err(ChainError::InvalidEvidence("registry catalog is invalid")),
        };
        Ok(ProviderIdentity::new(
            *block.observation(),
            self.profile().registry_address(),
            provider_id,
            owner,
            recipient,
            uri,
            catalog,
        )?)
    }
    /// Read recipient, owner, discovery URI and catalog at one canonical block.
    /// Only malformed/unavailable catalog content may be tolerated. RPC or
    /// identity failures still reject, including a cleared recipient wallet.
    pub async fn identity(
        &self,
        provider_id: U256,
        block: &Block,
        allow_unavailable_catalog: bool,
    ) -> Result<ProviderIdentity, ChainError> {
        let results = self.read(block, self.identity_calls(provider_id)).await?;
        let raw = results.iter().map(data).collect::<Result<Vec<_>, _>>()?;
        self.parse_identity(provider_id, block, &raw, allow_unavailable_catalog)
    }
    /// The ERC-8004 identity binds the funded wallet, independently of the
    /// delegated session signer. A cleared wallet cannot authorize attachment.
    pub async fn agent_wallet(&self, agent_id: U256, block: &Block) -> Result<Address, ChainError> {
        let results = self
            .read(
                block,
                vec![eth_call(
                    self.profile().registry_address(),
                    &IdentityReads::getAgentWalletCall { agentId: agent_id }.abi_encode(),
                )],
            )
            .await?;
        let wallet = decode_return::<IdentityReads::getAgentWalletCall>(&data(&results[0])?)?;
        if wallet.is_zero() {
            return Err(ChainError::InvalidEvidence("registry wallet is unset"));
        }
        Ok(wallet)
    }
}
