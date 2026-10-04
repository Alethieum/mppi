//! Resumable provider payment setup. The local record contains only this command's
//! identity, declarations hash, recipient proof request and exact wallet intent.
//! It stores no credentials, session usage or provider recovery infrastructure.
use crate::{
    Address, B256, U256,
    capabilities::TransactionWallet,
    config::{ProviderConfig, ProviderDeclarations, ProviderIdentityConfig},
    deployment::profile_to_mapping,
    discovery::{
        DiscoveryDocuments, ProviderIdentity, decimal_uint256, parse_address, parse_document,
    },
    local_state::LocalState,
    registry::{RegistryCalls, RegistryIdentity, RegistryKind, RegistryOperation, RegistryReader},
    types::{OperationResult, OperationState},
};
use alloy_primitives::{hex, keccak256};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

const RECORD_LIMIT: usize = 4 << 20;
const PROOF_LIMIT: usize = 64 << 10;

/// A bounded, redacted setup failure; rejected credentials/remote bodies are omitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OnboardingError(&'static str);
impl std::fmt::Display for OnboardingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for OnboardingError {}
impl OnboardingError {
    pub fn message(&self) -> &'static str {
        self.0
    }
}
pub(crate) fn fail(message: &'static str) -> OnboardingError {
    OnboardingError(message)
}

/// The recipient signs the previously exported exact request in its own wallet.
#[derive(Clone)]
pub struct SignedRecipientProof {
    request: Value,
    signature: Vec<u8>,
}
impl SignedRecipientProof {
    pub fn new(request: Value, signature: Vec<u8>) -> Result<Self, OnboardingError> {
        let raw = serde_json::to_vec(&request).map_err(|_| fail("recipient request is invalid"))?;
        parse_document(&raw, PROOF_LIMIT).map_err(|_| fail("recipient request exceeds limits"))?;
        if signature.is_empty() || signature.len() > PROOF_LIMIT {
            return Err(fail("recipient signature exceeds limits"));
        }
        Ok(Self { request, signature })
    }
    pub fn request(&self) -> &Value {
        &self.request
    }
    pub fn signature(&self) -> &[u8] {
        &self.signature
    }
}
impl std::fmt::Debug for SignedRecipientProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SignedRecipientProof { .. }")
    }
}

#[derive(Clone, Debug)]
pub struct OnboardingOptions {
    pub state_path: PathBuf,
    pub wallet_id: Option<String>,
    pub adopted_identity: Option<U256>,
    pub recipient_proof: Option<SignedRecipientProof>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnboardingProgress {
    Pending,
    RecipientProofRequired,
    Complete,
}
#[derive(Clone, Debug)]
pub struct OnboardingResult {
    pub(crate) progress: OnboardingProgress,
    pub(crate) config: Option<ProviderConfig>,
    pub(crate) identity: Option<RegistryIdentity>,
    pub(crate) operation: Option<OperationResult>,
    pub(crate) recipient_proof: Option<Value>,
    pub(crate) submitted: bool,
}
impl OnboardingResult {
    pub fn progress(&self) -> OnboardingProgress {
        self.progress
    }
    pub fn config(&self) -> Option<&ProviderConfig> {
        self.config.as_ref()
    }
    pub fn identity(&self) -> Option<&RegistryIdentity> {
        self.identity.as_ref()
    }
    pub fn operation(&self) -> Option<&OperationResult> {
        self.operation.as_ref()
    }
    pub fn recipient_proof(&self) -> Option<&Value> {
        self.recipient_proof.as_ref()
    }
    pub fn submitted(&self) -> bool {
        self.submitted
    }
}

pub(crate) fn exact(value: &Value, fields: &[&str]) -> Result<(), OnboardingError> {
    let map = value
        .as_object()
        .ok_or_else(|| fail("setup record is not an object"))?;
    if map.len() != fields.len() || fields.iter().any(|key| !map.contains_key(*key)) {
        return Err(fail("setup record has unexpected fields"));
    }
    Ok(())
}
pub(crate) fn decimal(value: &Value) -> Result<U256, OnboardingError> {
    decimal_uint256(
        value
            .as_str()
            .ok_or_else(|| fail("setup identity is not a decimal string"))?,
    )
    .map_err(|_| fail("setup identity is invalid"))
}
fn address(value: &Value) -> Result<Address, OnboardingError> {
    parse_address(
        value
            .as_str()
            .ok_or_else(|| fail("setup address is invalid"))?,
    )
    .map_err(|_| fail("setup address is invalid"))
}
fn text(value: &Value, limit: usize) -> Result<&str, OnboardingError> {
    value
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= limit)
        .ok_or_else(|| fail("setup identifier is invalid"))
}
fn bytes(value: &Value, limit: usize) -> Result<Vec<u8>, OnboardingError> {
    let raw = value
        .as_str()
        .and_then(|s| s.strip_prefix("0x"))
        .ok_or_else(|| fail("setup byte field is invalid"))?;
    if raw.len() > limit * 2 || !raw.len().is_multiple_of(2) {
        return Err(fail("setup byte field exceeds limits"));
    }
    hex::decode(raw).map_err(|_| fail("setup byte field is invalid"))
}
fn hx(value: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(value))
}

pub(crate) fn encode_registry_operation(op: &RegistryOperation) -> Value {
    json!({"kind":match op.kind() {RegistryKind::Register=>"REGISTER",RegistryKind::SetAgentWallet=>"SET_AGENT_WALLET",RegistryKind::SetUri=>"SET_URI",RegistryKind::SetCatalog=>"SET_CATALOG"},
        "owner":hx(op.owner()),"agent_id":op.agent_id().map(|id|id.to_string()),"agent_uri":op.agent_uri(),
        "new_wallet":op.new_wallet().map(hx),"deadline":op.deadline().map(|d|d.to_string()),
        "signature":op.signature().map(hx),"catalog_bytes":op.catalog_bytes().map(hx),"calldata":hx(op.call().data())})
}
pub(crate) fn decode_registry_operation(
    value: &Value,
    calls: &RegistryCalls,
) -> Result<RegistryOperation, OnboardingError> {
    exact(
        value,
        &[
            "kind",
            "owner",
            "agent_id",
            "agent_uri",
            "new_wallet",
            "deadline",
            "signature",
            "catalog_bytes",
            "calldata",
        ],
    )?;
    let owner = address(&value["owner"])?;
    let op = match value["kind"].as_str() {
        Some("REGISTER") => calls.register(
            owner,
            if value["agent_uri"].is_null() {
                None
            } else {
                Some(
                    value["agent_uri"]
                        .as_str()
                        .ok_or_else(|| fail("setup URI is invalid"))?,
                )
            },
        ),
        Some("SET_AGENT_WALLET") => calls.set_agent_wallet(
            owner,
            decimal(&value["agent_id"])?,
            address(&value["new_wallet"])?,
            decimal(&value["deadline"])?,
            &bytes(&value["signature"], PROOF_LIMIT)?,
        ),
        Some("SET_URI") => calls.set_uri(
            owner,
            decimal(&value["agent_id"])?,
            value["agent_uri"]
                .as_str()
                .ok_or_else(|| fail("setup URI is invalid"))?,
        ),
        Some("SET_CATALOG") => calls.set_catalog(
            owner,
            decimal(&value["agent_id"])?,
            &crate::parse_catalog(
                &bytes(&value["catalog_bytes"], crate::DEFAULT_CATALOG_BYTES)?,
                crate::DEFAULT_CATALOG_BYTES,
            )
            .map_err(|_| fail("setup catalog is invalid"))?,
        ),
        _ => return Err(fail("setup operation kind is invalid")),
    }
    .map_err(|_| fail("setup operation is invalid"))?;
    if encode_registry_operation(&op) != *value {
        return Err(fail("setup calldata or fields differ from retained intent"));
    }
    Ok(op)
}
#[derive(Clone, Debug)]
pub(crate) struct RetainedRegistryOperation {
    pub(crate) prepared: RegistryOperation,
    pub(crate) result: OperationResult,
    wallet_verified: bool,
}
pub(crate) fn encode_retained(retained: &RetainedRegistryOperation) -> Value {
    json!({"prepared":encode_registry_operation(&retained.prepared),"result":{
        "operation_ref":retained.result.operation_ref(),"wallet_operation_id":retained.result.wallet_operation_id(),
        "tx_hash":retained.result.tx_hash().map(hx)}})
}
pub(crate) fn decode_retained(
    value: &Value,
    calls: &RegistryCalls,
) -> Result<RetainedRegistryOperation, OnboardingError> {
    exact(value, &["prepared", "result"])?;
    let result = &value["result"];
    exact(result, &["operation_ref", "wallet_operation_id", "tx_hash"])?;
    let tx = if result["tx_hash"].is_null() {
        None
    } else {
        Some(
            B256::try_from(bytes(&result["tx_hash"], 32)?.as_slice())
                .map_err(|_| fail("setup transaction hash is invalid"))?,
        )
    };
    let handle = if result["wallet_operation_id"].is_null() {
        None
    } else {
        Some(text(&result["wallet_operation_id"], 256)?.to_owned())
    };
    Ok(RetainedRegistryOperation {
        wallet_verified: false,
        prepared: decode_registry_operation(&value["prepared"], calls)?,
        result: OperationResult::new(
            text(&result["operation_ref"], 256)?,
            OperationState::Unknown,
            handle,
            tx,
            None,
            vec![],
        )
        .map_err(|_| fail("setup operation reference is invalid"))?,
    })
}
pub(crate) fn new_retained(
    prepared: RegistryOperation,
) -> Result<RetainedRegistryOperation, OnboardingError> {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).map_err(|_| fail("operation randomness unavailable"))?;
    Ok(RetainedRegistryOperation {
        wallet_verified: false,
        prepared,
        result: OperationResult::new(
            format!("mppi-registry-{}", hex::encode(nonce)),
            OperationState::Unknown,
            None,
            None,
            None,
            vec![],
        )
        .map_err(|_| fail("operation reference unavailable"))?,
    })
}
/// Query the original wallet operation, preserving known handles. The caller
/// saves the returned record before requesting chain confirmation.
pub(crate) async fn wallet_registry_result(
    wallet: &dyn TransactionWallet,
    retained: &RetainedRegistryOperation,
    submit: bool,
) -> Result<RetainedRegistryOperation, OnboardingError> {
    if wallet.address() != retained.prepared.owner() {
        return Err(fail("selected wallet differs from retained registry owner"));
    }
    let known = &retained.result;
    let response = if submit {
        wallet
            .execute(retained.prepared.call(), known.operation_ref())
            .await
    } else {
        wallet.reconcile(known.operation_ref()).await
    };
    let response = response.map_err(|_| {
        fail("original wallet operation is unavailable; retained intent remains unresolved")
    })?;
    if response.operation_ref() != known.operation_ref()
        || response
            .wallet_operation_id()
            .is_some_and(|id| id.len() > 256)
        || known
            .wallet_operation_id()
            .is_some_and(|known| Some(known) != response.wallet_operation_id())
        || known
            .tx_hash()
            .is_some_and(|known| Some(known) != response.tx_hash())
    {
        return Err(fail(
            "wallet response conflicts with the retained registry operation",
        ));
    }
    // A fresh wallet response may prove that submission never began. Service
    // terminal states after an attempted submission cannot supply this evidence;
    // the TransactionWallet contract requires those to remain unresolved.
    let state = if response.tx_hash().is_none() && response.state() == OperationState::Failed {
        OperationState::Failed
    } else {
        OperationState::Unknown
    };
    Ok(RetainedRegistryOperation {
        wallet_verified: true,
        prepared: retained.prepared.clone(),
        result: OperationResult::new(
            known.operation_ref(),
            state,
            response
                .wallet_operation_id()
                .or(known.wallet_operation_id())
                .map(str::to_owned),
            response.tx_hash().or(known.tx_hash()),
            None,
            vec![],
        )
        .map_err(|_| fail("wallet result is invalid"))?,
    })
}
pub(crate) async fn confirm_registry_result(
    registry: &RegistryReader,
    retained: &RetainedRegistryOperation,
) -> Result<(RetainedRegistryOperation, Option<RegistryIdentity>), OnboardingError> {
    if !retained.wallet_verified {
        return Err(fail(
            "original wallet attestation is required before registry confirmation",
        ));
    }
    if retained.result.tx_hash().is_none() && retained.result.state() == OperationState::Failed {
        return Ok((retained.clone(), None));
    }
    let result = registry
        .reconcile(
            &retained.prepared,
            retained.result.operation_ref(),
            retained.result.tx_hash(),
            retained.result.wallet_operation_id(),
        )
        .await
        .map_err(|_| fail("registry operation evidence is invalid"))?;
    Ok((
        RetainedRegistryOperation {
            wallet_verified: true,
            prepared: retained.prepared.clone(),
            result: result.operation().clone(),
        },
        result.identity().cloned(),
    ))
}

pub(crate) fn save(store: &mut LocalState, record: &Value) -> Result<(), OnboardingError> {
    store
        .write(record)
        .map_err(|_| fail("setup record could not be saved; no further write was started"))
}
pub(crate) fn result(
    progress: OnboardingProgress,
    identity: Option<RegistryIdentity>,
    operation: Option<OperationResult>,
    proof: Option<Value>,
    submitted: bool,
) -> OnboardingResult {
    OnboardingResult {
        progress,
        config: None,
        identity,
        operation,
        recipient_proof: proof,
        submitted,
    }
}
fn check_intent(
    op: &RegistryOperation,
    id: Option<U256>,
    owner: Address,
    declarations: &ProviderDeclarations,
) -> Result<(), OnboardingError> {
    let intended_uri = format!("{}/discover", declarations.provider().public_origin());
    if op.owner() != owner || op.agent_id() != id && op.kind() != RegistryKind::Register {
        return Err(fail("retained operation belongs to another provider"));
    }
    let matches = match op.kind() {
        RegistryKind::Register => id.is_none() && op.agent_uri() == Some(&intended_uri),
        RegistryKind::SetAgentWallet => {
            op.new_wallet() == Some(declarations.provider().recipient())
        }
        RegistryKind::SetUri => op.agent_uri() == Some(&intended_uri),
        RegistryKind::SetCatalog => {
            op.catalog_bytes() == Some(declarations.catalog().canonical_bytes())
        }
    };
    if !matches {
        return Err(fail("retained operation differs from planned onboarding"));
    }
    Ok(())
}

pub(crate) fn registration_document(
    config: &ProviderConfig,
    identity: &RegistryIdentity,
    registry: Address,
) -> Result<Value, OnboardingError> {
    let observed = ProviderIdentity::new(
        *identity.observation(),
        registry,
        identity.agent_id(),
        identity.owner(),
        identity.agent_wallet(),
        identity.agent_uri().to_owned(),
        None,
    )
    .map_err(|_| fail("registration identity is invalid"))?;
    let documents = DiscoveryDocuments::new(config, &observed)
        .map_err(|_| fail("registration document could not be prepared"))?;
    serde_json::from_slice(documents.registration())
        .map_err(|_| fail("generated registration document is invalid"))
}

/// Check completed steps and advance only missing payment setup. Rerun the same
/// record after interruption. Pending writes are queried, never replaced.
/// Circle wallet provisioning and endpoint readiness are separate CLI steps.
pub async fn onboard(
    declarations: &ProviderDeclarations,
    wallet: Arc<dyn TransactionWallet>,
    registry: Arc<RegistryReader>,
    options: OnboardingOptions,
) -> Result<OnboardingResult, OnboardingError> {
    let owner = wallet.address();

    // Validate wallet identifiers without assigning an absent provider identity.
    ProviderIdentityConfig::new(U256::ZERO, owner, options.wallet_id.clone())
        .map_err(|_| fail("provider wallet configuration is invalid"))?;
    if declarations.provider().recipient() == owner {
        return Err(fail(
            "payout recipient must differ from the operator wallet",
        ));
    }
    declarations
        .with_identity(
            ProviderIdentityConfig::new(U256::ZERO, owner, options.wallet_id.clone())
                .map_err(|_| fail("provider wallet configuration is invalid"))?,
        )
        .map_err(|_| fail("provider declarations are invalid"))?
        .to_toml()
        .map_err(|_| fail("provider declarations cannot be serialized to configuration"))?;
    if declarations.chain().chain_id() != registry.chain().profile().domain().chain_id() {
        return Err(fail(
            "declarations and selected deployment use different networks",
        ));
    }
    let profile = profile_to_mapping(registry.chain().profile());
    let declaration_bytes = serde_json::to_vec(&declarations.to_mapping())
        .map_err(|_| fail("provider declarations are invalid"))?;
    let declaration_hash = hx(keccak256(declaration_bytes));
    let mut store = LocalState::open(&options.state_path, RECORD_LIMIT)
        .map_err(|_| fail("setup record is invalid or another command owns it"))?;
    let mut record=store.read().map_err(|_|fail("setup record is unavailable or invalid"))?.unwrap_or_else(||json!({
        "version":1,"kind":"mppi-provider-onboarding","profile":profile,"wallet":hx(owner),"wallet_id":options.wallet_id,
        "declarations_hash":declaration_hash,"agent_id":options.adopted_identity.map(|id|id.to_string()),"complete":false,"pending":null,"recipient_proof":null,"registration":null
    }));
    exact(
        &record,
        &[
            "version",
            "kind",
            "profile",
            "wallet",
            "wallet_id",
            "declarations_hash",
            "agent_id",
            "complete",
            "pending",
            "recipient_proof",
            "registration",
        ],
    )?;
    if record["version"] != 1
        || record["kind"] != "mppi-provider-onboarding"
        || record["profile"] != profile
        || record["wallet"] != hx(owner)
        || record["wallet_id"] != json!(options.wallet_id)
        || record["declarations_hash"] != declaration_hash
        || !record["complete"].is_boolean()
    {
        return Err(fail(
            "setup record differs from selected wallet, deployment or declarations; use explicit update for completed setup",
        ));
    }
    let mut id = if record["agent_id"].is_null() {
        None
    } else {
        Some(decimal(&record["agent_id"])?)
    };
    if options.adopted_identity.is_some() && options.adopted_identity != id {
        return Err(fail(
            "requested identity differs from retained provider identity",
        ));
    }
    if record["complete"] == true
        && (id.is_none() || !record["pending"].is_null() || !record["recipient_proof"].is_null())
    {
        return Err(fail("completed setup record is inconsistent"));
    }
    if !record["recipient_proof"].is_null() {
        let raw = serde_json::to_vec(&record["recipient_proof"])
            .map_err(|_| fail("saved recipient request is invalid"))?;
        parse_document(&raw, PROOF_LIMIT)
            .map_err(|_| fail("saved recipient request exceeds limits"))?;
    }
    let calls = RegistryCalls::new(
        registry.chain().profile().domain().chain_id(),
        registry.chain().profile().registry_address(),
    )
    .map_err(|_| fail("registry profile is invalid"))?;
    let mut submitted = false;
    let mut last_operation = None;
    // At most register + recipient + URI + catalog, plus one initial reconcile.
    for _ in 0..6 {
        if !record["pending"].is_null() {
            let retained = decode_retained(&record["pending"], &calls)?;
            check_intent(&retained.prepared, id, owner, declarations)?;
            let retained = wallet_registry_result(wallet.as_ref(), &retained, false).await?;
            record["pending"] = encode_retained(&retained);
            save(&mut store, &record)?;
            let (retained, confirmed) = confirm_registry_result(&registry, &retained).await?;
            last_operation = Some(retained.result.clone());
            match retained.result.state() {
                OperationState::Confirmed => {
                    let identity = confirmed
                        .ok_or_else(|| fail("confirmed registry operation has no identity"))?;
                    if retained.prepared.kind() == RegistryKind::Register {
                        id = Some(identity.agent_id());
                        record["agent_id"] = identity.agent_id().to_string().into();
                    }
                    record["pending"] = Value::Null;
                    save(&mut store, &record)?;
                }
                OperationState::Failed => {
                    record["pending"] = Value::Null;
                    save(&mut store, &record)?;
                    return Err(fail(if retained.result.tx_hash().is_none() {
                        "original registry operation failed before submission; repair wallet setup and rerun"
                    } else {
                        "original registry transaction reverted; rerun to check the remaining setup"
                    }));
                }
                _ => {
                    return Ok(result(
                        OnboardingProgress::Pending,
                        None,
                        Some(retained.result),
                        None,
                        submitted,
                    ));
                }
            }
        }
        let planned = if let Some(id) = id {
            let identity = registry
                .read(id, None)
                .await
                .map_err(|_| fail("provider registry identity is unavailable"))?;
            let intended_uri = format!("{}/discover", declarations.provider().public_origin());
            if identity.owner() != owner {
                return Err(fail(
                    "provider identity is not owned by the selected wallet",
                ));
            }
            let recipient_matches = identity.agent_wallet() == declarations.provider().recipient();
            let uri_matches = identity.agent_uri() == intended_uri;
            let catalog_matches =
                identity.catalog_bytes() == declarations.catalog().canonical_bytes();
            if recipient_matches && uri_matches && catalog_matches {
                let config = declarations
                    .with_identity(
                        ProviderIdentityConfig::new(id, owner, options.wallet_id.clone())
                            .map_err(|_| fail("provider identity configuration is invalid"))?,
                    )
                    .map_err(|_| fail("completed provider configuration is invalid"))?;
                record["registration"] = registration_document(
                    &config,
                    &identity,
                    registry.chain().profile().registry_address(),
                )?;
                record["complete"] = true.into();
                record["recipient_proof"] = Value::Null;
                // A completed rerun performs no local rewrite either.
                if store
                    .read()
                    .map_err(|_| fail("setup record cannot be checked"))?
                    .as_ref()
                    != Some(&record)
                {
                    save(&mut store, &record)?;
                }
                let mut complete = result(
                    OnboardingProgress::Complete,
                    Some(identity),
                    last_operation,
                    None,
                    submitted,
                );
                complete.config = Some(config);
                return Ok(complete);
            }
            if record["complete"] == true || !identity.catalog_bytes().is_empty() {
                return Err(fail(
                    "published provider setup conflicts with declarations; use explicit update",
                ));
            }
            if !recipient_matches {
                if !identity.agent_wallet().is_zero() && identity.agent_wallet() != owner {
                    return Err(fail(
                        "provider recipient conflicts with incomplete setup; use explicit update",
                    ));
                }
                if let Some(proof) = &options.recipient_proof {
                    if record["recipient_proof"] != proof.request {
                        return Err(fail(
                            "imported recipient proof does not match the exported request",
                        ));
                    }
                    let deadline =
                        crate::discovery::exact_uint256(&proof.request["message"]["deadline"])
                            .map_err(|_| fail("recipient proof deadline is invalid"))?;
                    let expected=registry.wallet_proof(id,declarations.provider().recipient(),owner,deadline,None).await.map_err(|_|fail("recipient proof context changed or expired; export a fresh request"))?;
                    if expected != proof.request {
                        return Err(fail(
                            "recipient proof context differs from selected provider",
                        ));
                    }
                    calls
                        .set_agent_wallet(
                            owner,
                            id,
                            declarations.provider().recipient(),
                            deadline,
                            &proof.signature,
                        )
                        .map_err(|_| fail("recipient proof operation is invalid"))?
                } else {
                    let block = registry
                        .verified_block()
                        .await
                        .map_err(|_| fail("recipient proof chain context is unavailable"))?;
                    let deadline = U256::from(block.timestamp()) + U256::from(300u64);
                    let proof = registry
                        .wallet_proof(
                            id,
                            declarations.provider().recipient(),
                            owner,
                            deadline,
                            Some(&block),
                        )
                        .await
                        .map_err(|_| fail("recipient proof context is unavailable"))?;
                    record["recipient_proof"] = proof.clone();
                    save(&mut store, &record)?;
                    return Ok(result(
                        OnboardingProgress::RecipientProofRequired,
                        Some(identity),
                        last_operation,
                        Some(proof),
                        submitted,
                    ));
                }
            } else if !uri_matches {
                if !identity.agent_uri().is_empty() {
                    return Err(fail(
                        "provider URI conflicts with incomplete setup; use explicit update",
                    ));
                }
                calls
                    .set_uri(owner, id, &intended_uri)
                    .map_err(|_| fail("provider URI operation is invalid"))?
            } else {
                let config = declarations
                    .with_identity(
                        ProviderIdentityConfig::new(id, owner, options.wallet_id.clone())
                            .map_err(|_| fail("provider identity configuration is invalid"))?,
                    )
                    .map_err(|_| fail("provider configuration is invalid"))?;
                record["registration"] = registration_document(
                    &config,
                    &identity,
                    registry.chain().profile().registry_address(),
                )?;
                calls
                    .set_catalog(owner, id, declarations.catalog())
                    .map_err(|_| fail("catalog publication is invalid"))?
            }
        } else {
            calls
                .register(
                    owner,
                    Some(&format!(
                        "{}/discover",
                        declarations.provider().public_origin()
                    )),
                )
                .map_err(|_| fail("provider registration is invalid"))?
        };
        registry
            .simulate(&planned)
            .await
            .map_err(|_| fail("planned registry write did not pass simulation"))?;
        let retained = new_retained(planned)?;
        record["pending"] = encode_retained(&retained);
        save(&mut store, &record)?;
        let retained = wallet_registry_result(wallet.as_ref(), &retained, true).await?;
        submitted = true;
        record["pending"] = encode_retained(&retained);
        save(&mut store, &record)?;
        // Reconciliation at the top of the next iteration queries the original
        // wallet reference and independently confirms its registry effects.
    }
    Err(fail(
        "setup exceeded its bounded step count; rerun to reconcile retained intent",
    ))
}

/// Refresh the completed onboarding declarations only after the caller has
/// durably saved this replacement configuration. This verifies current canonical
/// owner, recipient, URI and catalog and refuses unresolved onboarding records.
/// Runtime application of the configuration remains provider-owned.
pub async fn refresh_completed_onboarding(
    config: &ProviderConfig,
    path: impl AsRef<Path>,
    registry: &RegistryReader,
) -> Result<(), OnboardingError> {
    config
        .to_toml()
        .map_err(|_| fail("replacement configuration cannot be serialized"))?;
    let mut store = LocalState::open(path, RECORD_LIMIT)
        .map_err(|_| fail("completed onboarding record is unavailable or in use"))?;
    let mut record = store
        .read()
        .map_err(|_| fail("completed onboarding record is invalid"))?
        .ok_or_else(|| fail("completed onboarding record is missing"))?;
    exact(
        &record,
        &[
            "version",
            "kind",
            "profile",
            "wallet",
            "wallet_id",
            "declarations_hash",
            "agent_id",
            "complete",
            "pending",
            "recipient_proof",
            "registration",
        ],
    )?;
    if record["version"] != 1
        || record["kind"] != "mppi-provider-onboarding"
        || record["profile"] != profile_to_mapping(registry.chain().profile())
        || record["wallet"] != hx(config.identity().wallet_address())
        || record["wallet_id"] != json!(config.identity().wallet_id())
        || decimal(&record["agent_id"])? != config.identity().provider_id()
        || record["complete"] != true
        || !record["pending"].is_null()
        || !record["recipient_proof"].is_null()
    {
        return Err(fail(
            "onboarding record is incomplete or belongs to another provider",
        ));
    }
    let identity = registry
        .read(config.identity().provider_id(), None)
        .await
        .map_err(|_| fail("updated provider identity is unavailable"))?;
    if config.chain().chain_id() != identity.observation().chain_id()
        || identity.owner() != config.identity().wallet_address()
        || identity.agent_wallet() != config.provider().recipient()
        || identity.agent_uri() != format!("{}/discover", config.provider().public_origin())
        || identity.catalog_bytes() != config.catalog().canonical_bytes()
    {
        return Err(fail(
            "replacement configuration differs from confirmed provider setup",
        ));
    }
    let mut declarations = config.to_mapping();
    declarations
        .as_object_mut()
        .ok_or_else(|| fail("replacement configuration is invalid"))?
        .remove("identity");
    record["declarations_hash"] = hx(keccak256(
        serde_json::to_vec(&declarations)
            .map_err(|_| fail("replacement configuration is invalid"))?,
    ))
    .into();
    record["registration"] = registration_document(
        config,
        &identity,
        registry.chain().profile().registry_address(),
    )?;
    if store
        .read()
        .map_err(|_| fail("onboarding record cannot be checked"))?
        .as_ref()
        != Some(&record)
    {
        save(&mut store, &record)?;
    }
    Ok(())
}
