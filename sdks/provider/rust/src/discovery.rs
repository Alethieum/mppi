//! Bounded HTTP discovery comparisons against separately observed registry data.
//!
//! These records do not establish RPC honesty, freshness, deployed code identity,
//! wallet access or paid readiness. The provider publishes the returned responses.

use std::{collections::BTreeSet, net::Ipv6Addr};

use alloy_primitives::{Address, U256};
use serde_json::{Value, json};

use crate::{
    Catalog, ValidationCode, ValidationError,
    config::{ConfigError, ProviderConfig, bounded_json_bytes},
    interfaces::DiscoveryResponse,
    types::ChainObservation,
};

pub const MAX_DOCUMENT_BYTES: usize = 1 << 20;
pub const REGISTRATION_TYPE: &str = "https://eips.ethereum.org/EIPS/eip-8004#registration-v1";

fn invalid(message: &'static str) -> ValidationError {
    ValidationError::new(ValidationCode::InvalidValue, message)
}

/// Parse a nonzero address without accepting shortened or unprefixed forms.
pub fn parse_address(value: &str) -> Result<Address, ValidationError> {
    if value.len() != 42
        || !value.starts_with("0x")
        || !value.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
    {
        return Err(invalid("address requires 0x and 40 hexadecimal characters"));
    }
    let address: Address = value.parse().map_err(|_| invalid("invalid address"))?;
    if address == Address::ZERO {
        return Err(invalid("address must be nonzero"));
    }
    Ok(address)
}

pub fn address_hex(address: Address) -> Result<String, ValidationError> {
    if address == Address::ZERO {
        return Err(invalid("address must be nonzero"));
    }
    Ok(format!("{address:#x}"))
}

// Python's IPv6Address.compressed uses hexadecimal segments for mapped IPv4 too.
// Rust's Display uses dotted decimal for that special case, so format segments.
fn compressed_ipv6(address: Ipv6Addr) -> String {
    let segments = address.segments();
    let mut best = (0, 0);
    let mut index = 0;
    while index < segments.len() {
        if segments[index] != 0 {
            index += 1;
            continue;
        }
        let start = index;
        while index < segments.len() && segments[index] == 0 {
            index += 1;
        }
        if index - start > best.1 {
            best = (start, index - start);
        }
    }
    if best.1 < 2 {
        return segments
            .iter()
            .map(|s| format!("{s:x}"))
            .collect::<Vec<_>>()
            .join(":");
    }
    let before = segments[..best.0]
        .iter()
        .map(|s| format!("{s:x}"))
        .collect::<Vec<_>>()
        .join(":");
    let after = segments[best.0 + best.1..]
        .iter()
        .map(|s| format!("{s:x}"))
        .collect::<Vec<_>>()
        .join(":");
    format!("{before}::{after}")
}

pub(crate) fn authority(value: &str) -> Result<(String, Option<u16>), ValidationError> {
    if value.is_empty()
        || value
            .bytes()
            .any(|b| !(33..127).contains(&b) || b"\\%@?#/".contains(&b))
    {
        return Err(invalid("invalid URL authority"));
    }
    let (host, port) = if let Some(bracketed) = value.strip_prefix('[') {
        let (host, tail) = bracketed
            .split_once(']')
            .ok_or_else(|| invalid("invalid IPv6 authority"))?;
        let host: Ipv6Addr = host.parse().map_err(|_| invalid("invalid IPv6 host"))?;
        let port = if tail.is_empty() {
            None
        } else {
            Some(
                tail.strip_prefix(':')
                    .ok_or_else(|| invalid("invalid IPv6 authority"))?,
            )
        };
        (format!("[{}]", compressed_ipv6(host)), port)
    } else {
        let (host, port) = value
            .split_once(':')
            .map_or((value, None), |(h, p)| (h, Some(p)));
        if host.len() > 253
            || host.split('.').any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || !label.as_bytes()[0].is_ascii_alphanumeric()
                    || !label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                    || !label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
        {
            return Err(invalid("invalid DNS host"));
        }
        (host.to_ascii_lowercase(), port)
    };
    let port = port
        .map(|p| {
            if p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid("invalid URL port"));
            }
            // Leading zeroes are permitted by Python urlsplit, without an integer-size limit.
            let significant = p.trim_start_matches('0');
            let port: u16 = significant
                .parse()
                .map_err(|_| invalid("invalid URL port"))?;
            if port == 0 {
                return Err(invalid("invalid URL port"));
            }
            Ok(port)
        })
        .transpose()?;
    Ok((host, port))
}

/// Normalize only the protocol's HTTPS ASCII origin grammar; never repair a path.
pub fn normalize_origin(value: &str) -> Result<String, ValidationError> {
    if value.is_empty()
        || value.len() > 2048
        || value
            .bytes()
            .any(|b| !(33..127).contains(&b) || b"\\%@?#".contains(&b))
    {
        return Err(invalid("public origin is not a valid HTTPS origin"));
    }
    let (scheme, remainder) = value
        .split_once("://")
        .ok_or_else(|| invalid("public origin requires HTTPS"))?;
    if !scheme.eq_ignore_ascii_case("https") {
        return Err(invalid("public origin requires HTTPS"));
    }
    let host = remainder.strip_suffix('/').unwrap_or(remainder);
    let (host, port) = authority(host)?;
    Ok(format!(
        "https://{host}{}",
        port.filter(|p| *p != 443)
            .map_or(String::new(), |p| format!(":{p}"))
    ))
}

pub fn origin_from_discover(uri: &str) -> Result<String, ValidationError> {
    let origin = uri
        .strip_suffix("/discover")
        .ok_or_else(|| invalid("registry URI must identify /discover"))?;
    if origin.ends_with('/') {
        return Err(invalid("registry URI must identify exactly /discover"));
    }
    normalize_origin(origin)
}

/// Parse a bounded object and reject every decoded duplicate key, including extensions.
pub fn parse_document(raw: &[u8], max_bytes: usize) -> Result<Value, ValidationError> {
    if !(1..=MAX_DOCUMENT_BYTES).contains(&max_bytes) {
        return Err(invalid("invalid document byte limit"));
    }
    let value = crate::json::parse_document(raw, max_bytes, 128)?;
    if !value.is_object() {
        return Err(invalid("document must be a JSON object"));
    }
    Ok(value)
}

/// Identity values correlated to one chain observation by the supplied chain reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderIdentity {
    observation: ChainObservation,
    registry_address: Address,
    provider_id: U256,
    owner: Address,
    recipient: Address,
    agent_uri: String,
    catalog: Option<Catalog>,
}

impl ProviderIdentity {
    pub fn new(
        observation: ChainObservation,
        registry_address: Address,
        provider_id: U256,
        owner: Address,
        recipient: Address,
        agent_uri: String,
        catalog: Option<Catalog>,
    ) -> Result<Self, ValidationError> {
        if [registry_address, owner, recipient].contains(&Address::ZERO) {
            return Err(invalid("identity addresses must be nonzero"));
        }
        origin_from_discover(&agent_uri)?;
        Ok(Self {
            observation,
            registry_address,
            provider_id,
            owner,
            recipient,
            agent_uri,
            catalog,
        })
    }
    pub fn observation(&self) -> &ChainObservation {
        &self.observation
    }
    pub fn registry_address(&self) -> Address {
        self.registry_address
    }
    pub fn provider_id(&self) -> U256 {
        self.provider_id
    }
    pub fn owner(&self) -> Address {
        self.owner
    }
    pub fn recipient(&self) -> Address {
        self.recipient
    }
    pub fn agent_uri(&self) -> &str {
        &self.agent_uri
    }
    pub fn catalog(&self) -> Option<&Catalog> {
        self.catalog.as_ref()
    }
}

pub(crate) fn exact_uint256(value: &Value) -> Result<U256, ValidationError> {
    let spelling = match value {
        Value::Number(n) => n.to_string(),
        _ => return Err(invalid("expected an exact integer")),
    };
    decimal_uint256(&spelling)
}

pub(crate) fn decimal_uint256(value: &str) -> Result<U256, ValidationError> {
    if value.is_empty()
        || value.len() > 78
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid("expected canonical decimal uint256"));
    }
    U256::from_str_radix(value, 10).map_err(|_| invalid("uint256 overflow"))
}

/// Return the bound origin. HTTP declarations cannot replace registry identity.
pub fn verify_registration(
    raw: &[u8],
    identity: &ProviderIdentity,
    expected_origin: Option<&str>,
) -> Result<String, ValidationError> {
    let origin = origin_from_discover(identity.agent_uri())?;
    if expected_origin
        .map(normalize_origin)
        .transpose()?
        .is_some_and(|v| v != origin)
    {
        return Err(invalid("selected origin differs from registry URI"));
    }
    let doc = parse_document(raw, MAX_DOCUMENT_BYTES)?;
    if doc.get("type").and_then(Value::as_str) != Some(REGISTRATION_TYPE)
        || doc
            .get("name")
            .and_then(Value::as_str)
            .is_none_or(|s| s.is_empty())
        || !doc.get("description").is_some_and(Value::is_string)
        || doc.get("active").and_then(Value::as_bool) != Some(true)
        || doc.get("x402Support").and_then(Value::as_bool) != Some(false)
    {
        return Err(invalid("registration fields do not match MPPI profile"));
    }
    let registrations = doc
        .get("registrations")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("registration requires identity array"))?;
    let mut matched = 0;
    for entry in registrations {
        let id = exact_uint256(
            entry
                .get("agentId")
                .ok_or_else(|| invalid("registration requires agentId"))?,
        )?;
        let registry = entry
            .get("agentRegistry")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("registration requires agentRegistry"))?;
        let parts: Vec<_> = registry.split(':').collect();
        if parts.len() != 3
            || parts[0] != "eip155"
            || parts[1].is_empty()
            || parts[1].starts_with('0')
            || !parts[1].bytes().all(|b| b.is_ascii_digit())
        {
            return Err(invalid("registration must identify EVM registry"));
        }
        let address = parse_address(parts[2])?;
        if parts[1] == identity.observation.chain_id().to_string()
            && address == identity.registry_address
        {
            if id != identity.provider_id {
                return Err(invalid("registration conflicts with registry identity"));
            }
            matched += 1;
        }
    }
    if matched != 1 {
        return Err(invalid(
            "registration requires exactly one selected identity",
        ));
    }
    let services = doc
        .get("services")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("registration requires services array"))?;
    let mut selected = None;
    for service in services {
        let name = service
            .get("name")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid("service requires nonempty name"))?;
        service
            .get("endpoint")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| invalid("service requires endpoint"))?;
        if name == "mppi" && selected.replace(service).is_some() {
            return Err(invalid("registration requires exactly one MPPI service"));
        }
    }
    let service = selected.ok_or_else(|| invalid("registration requires MPPI service"))?;
    if service.get("version").and_then(Value::as_str) != Some("1")
        || normalize_origin(
            service["endpoint"]
                .as_str()
                .ok_or_else(|| invalid("invalid MPPI endpoint"))?,
        )? != origin
        || service.get("models").and_then(Value::as_str)
            != Some(format!("{origin}/models").as_str())
    {
        return Err(invalid(
            "MPPI endpoints or version differ from selected origin",
        ));
    }
    Ok(origin)
}

/// Validate advisory cards and return ids. HTTP rates never become payment authority.
pub fn verify_models(raw: &[u8], catalog: &Catalog) -> Result<Vec<String>, ValidationError> {
    let doc = parse_document(raw, MAX_DOCUMENT_BYTES)?;
    let cards = doc
        .get("models")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("models document requires array"))?;
    let mut found = Vec::new();
    let mut seen = BTreeSet::new();
    for card in cards {
        let model = card
            .get("model")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("model card requires model"))?;
        if catalog.rates_for(model).is_none() || !seen.insert(model) {
            return Err(invalid("cards must uniquely identify catalog models"));
        }
        if !card.get("description").is_some_and(Value::is_string) {
            return Err(invalid("model description requires string"));
        }
        let context = card
            .get("context_window")
            .and_then(Value::as_u64)
            .filter(|v| *v > 0)
            .ok_or_else(|| invalid("invalid context window"))?;
        let output = card
            .get("max_output")
            .and_then(Value::as_u64)
            .filter(|v| *v > 0)
            .ok_or_else(|| invalid("invalid output limit"))?;
        let modalities = card
            .get("modalities")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("invalid modalities"))?;
        let mut unique = BTreeSet::new();
        if output > context
            || modalities.is_empty()
            || modalities.iter().any(|v| {
                !v.as_str().is_some_and(|v| {
                    matches!(v, "text" | "image" | "audio" | "video") && unique.insert(v)
                })
            })
            || ["embeddings", "streaming", "tools", "structured_output"]
                .iter()
                .any(|flag| !card.get(flag).is_some_and(Value::is_boolean))
        {
            return Err(invalid("invalid model capabilities"));
        }
        found.push(model.to_owned());
    }
    Ok(found)
}

/// Prepare a proposed registration document from validated declarations and the
/// selected registry address. This performs no chain read and proves neither
/// publication nor endpoint readiness. `DiscoveryDocuments::new` separately
/// checks applied configuration against the observed identity before serving it.
pub fn proposed_registration(
    config: &ProviderConfig,
    registry_address: Address,
) -> Result<Vec<u8>, ConfigError> {
    if registry_address.is_zero() {
        return Err(ConfigError::new(
            "discovery",
            "registry address must be nonzero",
        ));
    }
    let origin = config.provider().public_origin();
    let mut services = vec![
        json!({"name":"mppi", "endpoint":origin,"version":"1", "models":format!("{origin}/models")}),
    ];
    services.extend(
        config
            .provider()
            .services()
            .iter()
            .map(|s| s.as_object().clone()),
    );
    // serde_json's arbitrary_precision representation keeps the entire uint256.
    let id: serde_json::Number = config
        .identity()
        .provider_id()
        .to_string()
        .parse()
        .map_err(|_| ConfigError::new("discovery", "invalid provider id"))?;
    let mut registration = json!({"type":REGISTRATION_TYPE,"name":config.provider().name(),"description":config.provider().description(),"services":services,"active":true,"x402Support":false,"registrations":[{"agentId":id,"agentRegistry":format!("eip155:{}:{}", config.chain().chain_id(), address_hex(registry_address).map_err(|_| ConfigError::new("discovery", "invalid registry address"))?)}]});
    if let Some(image) = config.provider().image() {
        registration["image"] = Value::String(image.to_owned());
    }
    bounded_json_bytes(&registration, "discovery.registration")
}

/// Snapshot of public documents derived from applied configuration and evidence.
#[derive(Clone)]
pub struct DiscoveryDocuments {
    registration: Vec<u8>,
    models: Vec<u8>,
}

impl DiscoveryDocuments {
    pub fn new(config: &ProviderConfig, identity: &ProviderIdentity) -> Result<Self, ConfigError> {
        if config.chain().chain_id() != identity.observation().chain_id()
            || config.identity().provider_id() != identity.provider_id()
            || config.identity().wallet_address() != identity.owner()
            || config.provider().recipient() != identity.recipient()
            || origin_from_discover(identity.agent_uri())
                .map_err(|_| ConfigError::new("discovery", "invalid registry URI"))?
                != config.provider().public_origin()
        {
            return Err(ConfigError::new(
                "discovery",
                "configuration differs from registry identity",
            ));
        }
        if identity.catalog().is_some_and(|c| c != config.catalog()) {
            return Err(ConfigError::new(
                "models.rates",
                "applied catalog differs from confirmed publication",
            ));
        }
        let origin = config.provider().public_origin();
        let registration = proposed_registration(config, identity.registry_address())?;
        let cards: Vec<_> = if identity.catalog().is_some() {
            config.models().iter().map(|m| m.card()).collect()
        } else {
            vec![]
        };
        let models = bounded_json_bytes(&json!({"models":cards}), "discovery.models")?;
        verify_registration(&registration, identity, Some(origin)).map_err(|_| {
            ConfigError::new("discovery.registration", "invalid generated registration")
        })?;
        if let Some(catalog) = identity.catalog() {
            verify_models(&models, catalog)
                .map_err(|_| ConfigError::new("discovery.models", "invalid generated cards"))?;
        }
        Ok(Self {
            registration,
            models,
        })
    }
    pub fn registration(&self) -> &[u8] {
        &self.registration
    }
    pub fn models(&self) -> &[u8] {
        &self.models
    }
    /// Handle an already parsed HTTP GET path; routing and TLS remain provider-owned.
    pub fn get(&self, path: &str) -> Option<DiscoveryResponse> {
        let body = match path {
            "/discover" | "/.well-known/agent-registration.json" => &self.registration,
            "/models" => &self.models,
            _ => return None,
        };
        Some(DiscoveryResponse {
            status: 200,
            headers: vec![
                (
                    "content-type".into(),
                    "application/json; charset=utf-8".into(),
                ),
                ("content-length".into(), body.len().to_string()),
                ("cache-control".into(), "no-store".into()),
            ],
            body: body.clone(),
        })
    }
}
