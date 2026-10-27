//! Explicit provider updates using the same retained wallet intent and canonical
//! registry confirmation as onboarding. Worker rollout remains provider-owned.
use crate::{
    capabilities::TransactionWallet,
    config::{MAX_CONFIG_BYTES, ProviderConfig},
    deployment::profile_to_mapping,
    discovery::parse_document,
    local_state::LocalState,
    onboarding::{
        OnboardingError, OnboardingProgress, OnboardingResult, SignedRecipientProof,
        confirm_registry_result, decode_retained, encode_retained, exact, fail, new_retained,
        read_failed, registration_document, result, save, wallet_registry_result,
    },
    registry::{RegistryCalls, RegistryIdentity, RegistryKind, RegistryOperation, RegistryReader},
    types::OperationState,
};
use alloy_primitives::{hex, keccak256};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

const RECORD_LIMIT: usize = 8 << 20;
const UNSUBMITTED: &str = "original update operation failed before submission; repair wallet setup and rerun the same requested changes";
fn hx(value: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(value))
}
fn hash(value: &Value) -> Result<String, OnboardingError> {
    Ok(hx(keccak256(
        serde_json::to_vec(value).map_err(|_| fail("update document is invalid"))?,
    )))
}
fn keys(value: &Value, allowed: &[&str]) -> Result<(), OnboardingError> {
    let map = value
        .as_object()
        .ok_or_else(|| fail("update section must be an object"))?;
    if map.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(fail("update contains an unsupported field"));
    }
    Ok(())
}

/// Validated explicit field selections; loading a configuration alone never
/// selects its fields for registry publication.
#[derive(Clone, Debug)]
pub struct UpdateChanges {
    value: Value,
}
impl UpdateChanges {
    pub fn from_mapping(value: &Value) -> Result<Self, OnboardingError> {
        let raw = serde_json::to_vec(value).map_err(|_| fail("update is invalid"))?;
        parse_document(&raw, MAX_CONFIG_BYTES).map_err(|_| fail("update exceeds limits"))?;
        keys(
            value,
            &[
                "provider",
                "upstream",
                "payment",
                "settlement",
                "listen",
                "models",
                "replace_models",
            ],
        )?;
        for (section, allowed) in [
            (
                "provider",
                &[
                    "name",
                    "description",
                    "public_origin",
                    "recipient",
                    "image",
                    "services",
                ][..],
            ),
            ("upstream", &["url", "api_key_env"][..]),
            (
                "payment",
                &[
                    "output_buffer_tokens",
                    "suggest_session_tokens",
                    "collection_margin_ms",
                    "authorization_wait_ms",
                ][..],
            ),
            ("settlement", &["interval_ms", "amount", "units"][..]),
            ("listen", &["bind", "tls_mode", "tls_cert", "tls_key"][..]),
        ] {
            if let Some(fields) = value.get(section)
                && (section != "listen" || !fields.is_null())
            {
                keys(fields, allowed)?;
            }
        }
        if value.get("models").is_some() && value.get("replace_models").is_some() {
            return Err(fail(
                "model edits and full replacement are mutually exclusive",
            ));
        }
        if let Some(models) = value.get("models") {
            keys(models, &["upsert", "remove"])?;
            let mut ids = BTreeSet::new();
            if let Some(upserts) = models.get("upsert") {
                for row in upserts
                    .as_array()
                    .ok_or_else(|| fail("model upsert must be an array"))?
                {
                    keys(
                        row,
                        &[
                            "model",
                            "description",
                            "context_window",
                            "max_output",
                            "modalities",
                            "embeddings",
                            "streaming",
                            "tools",
                            "structured_output",
                            "rates",
                        ],
                    )?;
                    let id = row
                        .get("model")
                        .and_then(Value::as_str)
                        .ok_or_else(|| fail("model edit requires model id"))?;
                    crate::validate_model(id).map_err(|_| fail("model edit id is invalid"))?;
                    if !ids.insert(id) {
                        return Err(fail("duplicate model edit"));
                    }
                    if let Some(rates) = row.get("rates") {
                        keys(rates, &["input", "output", "cache"])?;
                    }
                }
            }
            if let Some(removals) = models.get("remove") {
                for id in removals
                    .as_array()
                    .ok_or_else(|| fail("model removal must be an array"))?
                {
                    let id = id
                        .as_str()
                        .ok_or_else(|| fail("model removal requires model id"))?;
                    crate::validate_model(id).map_err(|_| fail("model removal id is invalid"))?;
                    if !ids.insert(id) {
                        return Err(fail("model is repeated across update edits"));
                    }
                }
            }
        }
        if value
            .get("replace_models")
            .is_some_and(|rows| !rows.is_array())
        {
            return Err(fail("full model replacement must be an array"));
        }
        Ok(Self {
            value: value.clone(),
        })
    }
    pub fn to_mapping(&self) -> Value {
        self.value.clone()
    }
    fn recipient(&self) -> bool {
        self.value["provider"].get("recipient").is_some()
    }
    fn uri(&self) -> bool {
        self.value["provider"].get("public_origin").is_some()
    }
    fn catalog(&self) -> bool {
        self.value.get("replace_models").is_some()
            || self.value["models"]["remove"]
                .as_array()
                .is_some_and(|a| !a.is_empty())
            || self.value["models"]["upsert"]
                .as_array()
                .is_some_and(|rows| {
                    rows.iter().any(|r| {
                        r.get("rates")
                            .and_then(Value::as_object)
                            .is_some_and(|rates| !rates.is_empty())
                    })
                })
    }
}
#[derive(Clone, Debug)]
pub struct UpdateOptions {
    pub state_path: PathBuf,
    pub recipient_proof: Option<SignedRecipientProof>,
}

fn merge_fields(target: &mut Value, changes: &Value) -> Result<(), OnboardingError> {
    let target = target
        .as_object_mut()
        .ok_or_else(|| fail("configuration section is invalid"))?;
    for (key, value) in changes
        .as_object()
        .ok_or_else(|| fail("update section is invalid"))?
    {
        if value.is_null() {
            target.remove(key);
        } else {
            target.insert(key.clone(), value.clone());
        }
    }
    Ok(())
}
fn target_config(
    current: &ProviderConfig,
    changes: &UpdateChanges,
    base: &Value,
) -> Result<ProviderConfig, OnboardingError> {
    exact(base, &["recipient", "uri", "catalog"])?;
    let mut value = current.to_mapping();
    for section in ["provider", "upstream", "payment", "settlement", "listen"] {
        if let Some(fields) = changes.value.get(section) {
            if section == "listen" && fields.is_null() {
                value.as_object_mut().unwrap().remove(section);
            } else {
                if value.get(section).is_none() {
                    value[section] = json!({});
                }
                merge_fields(&mut value[section], fields)?;
            }
        }
    }
    if let Some(replacement) = changes.value.get("replace_models") {
        value["models"] = replacement.clone();
    } else if changes.value.get("models").is_some() {
        let local = value["models"]
            .as_array()
            .ok_or_else(|| fail("local model declarations are invalid"))?;
        let mut rows = BTreeMap::new();
        if changes.catalog() {
            let raw = base["catalog"]
                .as_str()
                .and_then(|s| s.strip_prefix("0x"))
                .filter(|s| s.len() <= 2 * crate::DEFAULT_CATALOG_BYTES)
                .ok_or_else(|| fail("baseline catalog is invalid"))?;
            let bytes = hex::decode(raw).map_err(|_| fail("baseline catalog is invalid"))?;
            let catalog = crate::parse_catalog(&bytes, crate::DEFAULT_CATALOG_BYTES)
                .map_err(|_| fail("current confirmed catalog is unavailable"))?;
            for published in catalog.rows() {
                let mut declaration=local.iter().find(|row|row["model"].as_str()==Some(published.model())).cloned()
                    .ok_or_else(||fail("confirmed model has no local capability declaration; provide an explicit full model replacement"))?;
                let rates = published.rates();
                declaration["rates"] =
                    json!({"input":rates.input().to_string(),"output":rates.output().to_string()});
                if let Some(cache) = rates.cache() {
                    declaration["rates"]["cache"] = cache.to_string().into();
                }
                rows.insert(published.model().to_owned(), declaration);
            }
        } else {
            for declaration in local {
                rows.insert(
                    declaration["model"]
                        .as_str()
                        .ok_or_else(|| fail("local model id is invalid"))?
                        .to_owned(),
                    declaration.clone(),
                );
            }
        }
        if let Some(edits) = changes.value.get("models") {
            if let Some(removals) = edits.get("remove") {
                for id in removals
                    .as_array()
                    .ok_or_else(|| fail("model removals are invalid"))?
                {
                    rows.remove(
                        id.as_str()
                            .ok_or_else(|| fail("model removal id is invalid"))?,
                    );
                }
            }
            if let Some(upserts) = edits.get("upsert") {
                for row in upserts
                    .as_array()
                    .ok_or_else(|| fail("model upserts are invalid"))?
                {
                    let id = row["model"]
                        .as_str()
                        .ok_or_else(|| fail("model edit id is invalid"))?;
                    let entry = rows.entry(id.to_owned()).or_insert_with(|| json!({}));
                    for (key, field) in row
                        .as_object()
                        .ok_or_else(|| fail("model edit is invalid"))?
                    {
                        if key == "rates" {
                            if entry.get("rates").is_none() {
                                entry["rates"] = json!({});
                            }
                            merge_fields(&mut entry["rates"], field)?;
                        } else {
                            entry[key] = field.clone();
                        }
                    }
                }
            }
        }
        let ordered = if changes.catalog() {
            rows.into_values().collect()
        } else {
            let mut ordered = Vec::new();
            for original in local {
                if let Some(row) = rows.remove(
                    original["model"]
                        .as_str()
                        .ok_or_else(|| fail("local model id is invalid"))?,
                ) {
                    ordered.push(row);
                }
            }
            ordered.extend(rows.into_values());
            ordered
        };
        value["models"] = Value::Array(ordered);
    }
    let config = ProviderConfig::from_mapping(&value, None)
        .map_err(|_| fail("resulting provider configuration is invalid"))?;
    config
        .to_toml()
        .map_err(|_| fail("resulting provider configuration cannot be serialized"))?;
    Ok(config)
}
fn baseline(identity: &RegistryIdentity) -> Value {
    json!({"recipient":hx(identity.agent_wallet()),"uri":identity.agent_uri(),"catalog":hx(identity.catalog_bytes())})
}
fn matches_selected(
    identity: &RegistryIdentity,
    base: &Value,
    target: &ProviderConfig,
    changes: &UpdateChanges,
) -> Result<(), OnboardingError> {
    exact(base, &["recipient", "uri", "catalog"])?;
    let target_uri = format!("{}/discover", target.provider().public_origin());
    if changes.recipient()
        && base["recipient"] != hx(identity.agent_wallet())
        && target.provider().recipient() != identity.agent_wallet()
    {
        return Err(fail(
            "recipient changed concurrently; retained update remains unresolved",
        ));
    }
    if changes.uri() && base["uri"] != identity.agent_uri() && target_uri != identity.agent_uri() {
        return Err(fail(
            "provider URI changed concurrently; retained update remains unresolved",
        ));
    }
    if changes.catalog()
        && base["catalog"] != hx(identity.catalog_bytes())
        && target.catalog().canonical_bytes() != identity.catalog_bytes()
    {
        return Err(fail(
            "catalog changed concurrently; retained update remains unresolved",
        ));
    }
    Ok(())
}
fn check_operation(
    operation: &RegistryOperation,
    target: &ProviderConfig,
    changes: &UpdateChanges,
) -> Result<(), OnboardingError> {
    if operation.owner() != target.identity().wallet_address()
        || operation.agent_id() != Some(target.identity().provider_id())
    {
        return Err(fail("retained update belongs to another provider"));
    }
    let matched = match operation.kind() {
        RegistryKind::Register => false,
        RegistryKind::SetAgentWallet => {
            changes.recipient() && operation.new_wallet() == Some(target.provider().recipient())
        }
        RegistryKind::SetUri => {
            changes.uri()
                && operation.agent_uri()
                    == Some(format!("{}/discover", target.provider().public_origin()).as_str())
        }
        RegistryKind::SetCatalog => {
            changes.catalog()
                && operation.catalog_bytes() == Some(target.catalog().canonical_bytes())
        }
    };
    if !matched {
        return Err(fail(
            "retained registry operation differs from requested update",
        ));
    }
    Ok(())
}
fn validate_record(
    record: &Value,
    profile: &Value,
    current: &ProviderConfig,
) -> Result<(), OnboardingError> {
    exact(
        record,
        &[
            "version",
            "kind",
            "profile",
            "wallet",
            "agent_id",
            "request",
            "request_hash",
            "base",
            "source",
            "desired",
            "pending",
            "recipient_proof",
            "registration",
            "complete",
        ],
    )?;
    if record["version"] != 1
        || record["kind"] != "mppi-provider-update"
        || record["profile"] != *profile
        || record["wallet"] != hx(current.identity().wallet_address())
        || record["agent_id"] != current.identity().provider_id().to_string()
        || !record["complete"].is_boolean()
        || record["request_hash"] != hash(&record["request"])?
    {
        return Err(fail(
            "update record differs from provider context or is invalid",
        ));
    }
    UpdateChanges::from_mapping(&record["request"])?;
    exact(&record["base"], &["recipient", "uri", "catalog"])?;
    if record["complete"] == true
        && (!record["pending"].is_null() || !record["recipient_proof"].is_null())
    {
        return Err(fail("completed update record is inconsistent"));
    }
    Ok(())
}

/// Apply only explicitly requested changes. Wallet setup, filesystem publication
/// of the returned config, endpoint hosting and worker reload are caller-owned.
pub async fn update(
    current: &ProviderConfig,
    changes: &UpdateChanges,
    wallet: Arc<dyn TransactionWallet>,
    registry: Arc<RegistryReader>,
    options: UpdateOptions,
) -> Result<OnboardingResult, OnboardingError> {
    if wallet.address() != current.identity().wallet_address()
        || current.chain().chain_id() != registry.chain().profile().domain().chain_id()
    {
        return Err(fail(
            "update wallet or deployment differs from configured provider",
        ));
    }
    let mut store = LocalState::open(&options.state_path, RECORD_LIMIT)
        .map_err(|_| fail("update record is invalid or another command owns it"))?;
    let profile = profile_to_mapping(registry.chain().profile());
    let request_hash = hash(&changes.value)?;
    let saved = store.read().map_err(|_| fail("update record is invalid"))?;
    if let Some(record) = &saved {
        validate_record(record, &profile, current)?;
        if record["request_hash"] != request_hash && record["complete"] != true {
            return Err(fail(
                "previous explicit update is unresolved; rerun its original changes",
            ));
        }
    }
    let mut record = match saved {
        Some(record) if record["request_hash"] == request_hash => record,
        _ => {
            let identity = registry
                .read(current.identity().provider_id(), None)
                .await
                .map_err(read_failed("current provider identity is unavailable"))?;
            if identity.owner() != wallet.address() {
                return Err(fail(
                    "provider identity is not owned by the selected wallet",
                ));
            }
            let base = baseline(&identity);
            let target = target_config(current, changes, &base)?;
            json!({"version":1,"kind":"mppi-provider-update","profile":profile,"wallet":hx(wallet.address()),"agent_id":current.identity().provider_id().to_string(),"request":changes.value,"request_hash":request_hash,"base":base,"source":current.to_mapping(),"desired":target.to_mapping(),"pending":null,"recipient_proof":null,"registration":null,"complete":false})
        }
    };
    let source = ProviderConfig::from_mapping(&record["source"], None)
        .map_err(|_| fail("retained source configuration is invalid"))?;
    if current != &source && current.to_mapping() != record["desired"] {
        return Err(fail(
            "local configuration changed while update was retained; resume with its original configuration",
        ));
    }
    let expected = target_config(&source, changes, &record["base"])?;
    if expected.to_mapping() != record["desired"] {
        return Err(fail(
            "retained desired configuration differs from explicit update intent",
        ));
    }
    let target = ProviderConfig::from_mapping(&record["desired"], None)
        .map_err(|_| fail("retained target configuration is invalid"))?;
    target
        .to_toml()
        .map_err(|_| fail("retained target configuration cannot be serialized"))?;
    if target.identity() != current.identity() || target.chain() != current.chain() {
        return Err(fail(
            "update cannot change provider identity or chain configuration",
        ));
    }
    let proposed = crate::discovery::proposed_registration(
        &target,
        registry.chain().profile().registry_address(),
    )
    .map_err(|_| fail("proposed registration document is invalid"))?;
    let proposed: Value = serde_json::from_slice(&proposed)
        .map_err(|_| fail("proposed registration document is invalid"))?;
    if !record["registration"].is_null() && record["registration"] != proposed {
        return Err(fail(
            "saved registration document differs from requested update",
        ));
    }
    record["registration"] = proposed;
    let calls = RegistryCalls::new(
        current.chain().chain_id(),
        registry.chain().profile().registry_address(),
    )
    .map_err(|_| fail("selected registry profile is invalid"))?;
    let mut submitted = false;
    let mut last_operation = None;
    for _ in 0..5 {
        if !record["pending"].is_null() {
            let retained = decode_retained(&record["pending"], &calls)?;
            check_operation(&retained.prepared, &target, changes)?;
            let retained = wallet_registry_result(wallet.as_ref(), &retained, false).await?;
            record["pending"] = encode_retained(&retained);
            save(&mut store, &record)?;
            let (retained, _) = confirm_registry_result(&registry, &retained).await?;
            last_operation = Some(retained.result.clone());
            match retained.result.state() {
                OperationState::Confirmed => {
                    record["pending"] = Value::Null;
                    save(&mut store, &record)?;
                }
                OperationState::Failed => {
                    record["pending"] = Value::Null;
                    save(&mut store, &record)?;
                    return Err(fail(if retained.result.tx_hash().is_none() {
                        UNSUBMITTED
                    } else {
                        "original update transaction reverted; rerun the same requested changes"
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
        let identity = registry
            .read(current.identity().provider_id(), None)
            .await
            .map_err(read_failed("current provider identity is unavailable"))?;
        if identity.owner() != wallet.address() {
            return Err(fail("provider identity owner changed"));
        }
        matches_selected(&identity, &record["base"], &target, changes)?;
        let recipient_needed =
            changes.recipient() && identity.agent_wallet() != target.provider().recipient();
        let intended_uri = format!("{}/discover", target.provider().public_origin());
        let uri_needed = changes.uri() && identity.agent_uri() != intended_uri;
        let catalog_needed =
            changes.catalog() && identity.catalog_bytes() != target.catalog().canonical_bytes();
        if !recipient_needed && !uri_needed && !catalog_needed {
            // This also surfaces unrelated changes that make the saved complete
            // local declaration inconsistent, without publishing those fields.
            let registration = registration_document(
                &target,
                &identity,
                registry.chain().profile().registry_address(),
            )?;
            if changes.catalog() && identity.catalog_bytes() != target.catalog().canonical_bytes() {
                return Err(fail("confirmed catalog changed while update was prepared"));
            }
            record["registration"] = registration;
            record["recipient_proof"] = Value::Null;
            record["complete"] = true.into();
            if store
                .read()
                .map_err(|_| fail("update record cannot be checked"))?
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
            complete.config = Some(target);
            return Ok(complete);
        }
        if record["complete"] == true {
            return Err(fail(
                "completed update no longer matches current publication; submit explicit new changes",
            ));
        }
        let planned = if recipient_needed {
            if let Some(proof) = &options.recipient_proof {
                if record["recipient_proof"] != *proof.request() {
                    return Err(fail(
                        "imported recipient proof differs from the exported update request",
                    ));
                }
                let deadline =
                    crate::discovery::exact_uint256(&proof.request()["message"]["deadline"])
                        .map_err(|_| fail("recipient proof deadline is invalid"))?;
                let expected = registry
                    .wallet_proof(
                        current.identity().provider_id(),
                        target.provider().recipient(),
                        wallet.address(),
                        deadline,
                        None,
                    )
                    .await
                    .map_err(read_failed(
                        "recipient proof expired or context changed; export a fresh request",
                    ))?;
                if expected != *proof.request() {
                    return Err(fail(
                        "recipient proof context differs from requested update",
                    ));
                }
                calls
                    .set_agent_wallet(
                        wallet.address(),
                        current.identity().provider_id(),
                        target.provider().recipient(),
                        deadline,
                        proof.signature(),
                    )
                    .map_err(|_| fail("recipient update is invalid"))?
            } else {
                let block = registry
                    .verified_block()
                    .await
                    .map_err(read_failed("recipient proof context unavailable"))?;
                let deadline = crate::U256::from(block.timestamp()) + crate::U256::from(300u64);
                let proof = registry
                    .wallet_proof(
                        current.identity().provider_id(),
                        target.provider().recipient(),
                        wallet.address(),
                        deadline,
                        Some(&block),
                    )
                    .await
                    .map_err(read_failed("recipient proof request unavailable"))?;
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
        } else if uri_needed {
            calls
                .set_uri(
                    wallet.address(),
                    current.identity().provider_id(),
                    &intended_uri,
                )
                .map_err(|_| fail("URI update is invalid"))?
        } else {
            record["registration"] = registration_document(
                &target,
                &identity,
                registry.chain().profile().registry_address(),
            )?;
            calls
                .set_catalog(
                    wallet.address(),
                    current.identity().provider_id(),
                    target.catalog(),
                )
                .map_err(|_| fail("catalog update is invalid"))?
        };
        // Re-read the relevant confirmed fields immediately before submission.
        let latest = registry
            .read(current.identity().provider_id(), None)
            .await
            .map_err(read_failed("update preflight state unavailable"))?;
        if latest.owner() != wallet.address() {
            return Err(fail("provider identity owner changed before update"));
        }
        matches_selected(&latest, &record["base"], &target, changes)?;
        let already_applied = match planned.kind() {
            RegistryKind::SetAgentWallet => latest.agent_wallet() == target.provider().recipient(),
            RegistryKind::SetUri => latest.agent_uri() == intended_uri,
            RegistryKind::SetCatalog => {
                latest.catalog_bytes() == target.catalog().canonical_bytes()
            }
            RegistryKind::Register => false,
        };
        if already_applied
            || planned.kind() != RegistryKind::SetAgentWallet
                && changes.recipient()
                && latest.agent_wallet() != target.provider().recipient()
            || planned.kind() == RegistryKind::SetCatalog
                && changes.uri()
                && latest.agent_uri() != intended_uri
        {
            continue;
        }
        registry
            .simulate(&planned)
            .await
            .map_err(read_failed("planned update failed registry simulation"))?;
        let retained = new_retained(planned)?;
        record["pending"] = encode_retained(&retained);
        save(&mut store, &record)?;
        let retained = wallet_registry_result(wallet.as_ref(), &retained, true).await?;
        if retained.result.state() == OperationState::Failed {
            // Proven never submitted. The wallet may keep no record of it to
            // reconcile, so end it now; a rerun prepares a fresh intent.
            record["pending"] = Value::Null;
            save(&mut store, &record)?;
            return Err(fail(UNSUBMITTED));
        }
        submitted = true;
        record["pending"] = encode_retained(&retained);
        save(&mut store, &record)?;
    }
    Err(fail(
        "update exceeded bounded steps; rerun to reconcile its retained intent",
    ))
}
