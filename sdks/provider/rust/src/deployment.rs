//! Explicit, non-secret deployment pins. Parsing does not establish provenance.
use crate::{
    B256, ValidationCode, ValidationError,
    chain::{CodePin, DeploymentProfile},
    discovery::{MAX_DOCUMENT_BYTES, parse_address, parse_document},
    types::Domain,
};
use serde_json::{Value, json};
use std::{fs::File, io::Read, path::Path};

/// Exact public release pins from `mppi/deployments/arc-testnet.json`.
/// These pins are packaged with the SDK; RPC responses never supply trust anchors.
pub fn official_testnet_profile() -> Result<DeploymentProfile, ValidationError> {
    profile_from_mapping(&parse_document(
        include_bytes!("arc-testnet.json"),
        MAX_DOCUMENT_BYTES,
    )?)
}

/// Exact public release pins from `mppi/deployments/arc-mainnet.json`.
/// These pins are packaged with the SDK; RPC responses never supply trust anchors.
pub fn official_mainnet_profile() -> Result<DeploymentProfile, ValidationError> {
    profile_from_mapping(&parse_document(
        include_bytes!("arc-mainnet.json"),
        MAX_DOCUMENT_BYTES,
    )?)
}

fn invalid() -> ValidationError {
    ValidationError::new(ValidationCode::InvalidValue, "invalid deployment profile")
}
fn keys(value: &Value, expected: &[&str]) -> Result<(), ValidationError> {
    let map = value.as_object().ok_or_else(invalid)?;
    if map.len() != expected.len() || expected.iter().any(|k| !map.contains_key(*k)) {
        return Err(invalid());
    }
    Ok(())
}
fn pin(value: &Value) -> Result<CodePin, ValidationError> {
    keys(value, &["address", "code_hash"])?;
    let address = parse_address(value["address"].as_str().ok_or_else(invalid)?)?;
    let raw = value["code_hash"].as_str().ok_or_else(invalid)?;
    if raw.len() != 66
        || !raw.starts_with("0x")
        || !raw.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
    {
        return Err(invalid());
    }
    let hash: B256 = raw.parse().map_err(|_| invalid())?;
    CodePin::new(address, hash).map_err(|_| invalid())
}
pub fn profile_from_mapping(value: &Value) -> Result<DeploymentProfile, ValidationError> {
    keys(
        value,
        &[
            "version",
            "chain_id",
            "proxy",
            "implementation",
            "registry",
            "registry_implementation",
        ],
    )?;
    if value["version"].as_u64() != Some(1) {
        return Err(invalid());
    }
    let proxy = pin(&value["proxy"])?;
    let implementation = pin(&value["implementation"])?;
    let registry = pin(&value["registry"])?;
    let registry_implementation = pin(&value["registry_implementation"])?;
    let domain = Domain::new(
        value["chain_id"].as_u64().ok_or_else(invalid)?,
        proxy.address(),
    )?;
    DeploymentProfile::new(
        domain,
        registry.address(),
        proxy,
        implementation,
        registry,
        registry_implementation,
    )
    .map_err(|_| invalid())
}
pub fn profile_to_mapping(profile: &DeploymentProfile) -> Value {
    fn pin(value: &CodePin) -> Value {
        json!({"address":format!("{:#x}",value.address()),"code_hash":format!("{:#x}",value.code_hash())})
    }
    json!({"version":1,"chain_id":profile.domain().chain_id(),"proxy":pin(profile.proxy()),"implementation":pin(profile.implementation()),"registry":pin(profile.registry()),"registry_implementation":pin(profile.registry_implementation())})
}
pub fn load_profile(path: impl AsRef<Path>) -> Result<DeploymentProfile, ValidationError> {
    let mut raw = Vec::new();
    File::open(path)
        .map_err(|_| invalid())?
        .take((MAX_DOCUMENT_BYTES + 1) as u64)
        .read_to_end(&mut raw)
        .map_err(|_| invalid())?;
    profile_from_mapping(&parse_document(&raw, MAX_DOCUMENT_BYTES)?)
}
