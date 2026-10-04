//! Immutable provider declarations. Loading them never publishes or resolves secrets.

use std::{
    collections::BTreeSet,
    fmt,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use alloy_primitives::{Address, U256};
use serde_json::{Map, Value, json};

use crate::discovery::{
    MAX_DOCUMENT_BYTES, authority, decimal_uint256, exact_uint256, normalize_origin, parse_address,
    parse_document,
};
use crate::{
    Catalog, CatalogRow, DEFAULT_CATALOG_BYTES, Rates, decimal_uint64, publish_catalog,
    validate_model,
};

pub const MAX_CONFIG_BYTES: usize = 1 << 20;
pub const MAX_DURATION_MS: u64 = i64::MAX as u64 / 1_000_000;
const SECTIONS: &[&str] = &[
    "provider",
    "identity",
    "chain",
    "upstream",
    "models",
    "payment",
    "settlement",
    "listen",
];

/// A bounded field-local error. Rejected values and parser diagnostics are omitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError {
    field: String,
    reason: &'static str,
    source: Option<PathBuf>,
}
impl ConfigError {
    pub(crate) fn new(field: impl Into<String>, reason: &'static str) -> Self {
        Self {
            field: field.into(),
            reason,
            source: None,
        }
    }
    pub fn field(&self) -> &str {
        &self.field
    }
    pub fn reason(&self) -> &'static str {
        self.reason
    }
    pub fn source_path(&self) -> Option<&Path> {
        self.source.as_deref()
    }
}
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(path) => write!(f, "{}", path.display())?,
            None => f.write_str("configuration")?,
        }
        write!(f, ":{}: {}", self.field, self.reason)
    }
}
impl std::error::Error for ConfigError {}

fn text(value: &str, name: &str, nonempty: bool) -> Result<(), ConfigError> {
    if value.len() > MAX_DOCUMENT_BYTES || (nonempty && value.is_empty()) {
        return Err(ConfigError::new(
            name,
            "expected a bounded string with required content",
        ));
    }
    Ok(())
}
fn string<'a>(value: &'a Value, name: &str, nonempty: bool) -> Result<&'a str, ConfigError> {
    let value = value
        .as_str()
        .ok_or_else(|| ConfigError::new(name, "expected a string"))?;
    text(value, name, nonempty)?;
    Ok(value)
}
fn positive(value: u64, name: &str, duration: bool) -> Result<u64, ConfigError> {
    if value == 0 {
        return Err(ConfigError::new(name, "expected a positive integer"));
    }
    if duration && value > MAX_DURATION_MS {
        return Err(ConfigError::new(
            name,
            "duration exceeds signed-64-bit nanosecond range",
        ));
    }
    Ok(value)
}
fn integer(value: &Value, name: &str, duration: bool) -> Result<u64, ConfigError> {
    positive(
        value
            .as_u64()
            .ok_or_else(|| ConfigError::new(name, "expected an exact positive integer"))?,
        name,
        duration,
    )
}
fn amount(value: &Value, name: &str) -> Result<u64, ConfigError> {
    match value {
        Value::String(s) => {
            decimal_uint64(s).map_err(|_| ConfigError::new(name, "expected an exact uint64 amount"))
        }
        _ => value
            .as_u64()
            .ok_or_else(|| ConfigError::new(name, "expected an exact uint64 amount")),
    }
}
fn nonzero_address(address: Address, name: &str) -> Result<Address, ConfigError> {
    if address == Address::ZERO {
        return Err(ConfigError::new(name, "expected a nonzero EVM address"));
    }
    Ok(address)
}
fn address(value: &Value, name: &str) -> Result<Address, ConfigError> {
    parse_address(string(value, name, true)?)
        .map_err(|_| ConfigError::new(name, "expected a nonzero EVM address"))
}
fn object<'a>(
    value: &'a Value,
    name: &str,
    allowed: &[&str],
    required: &[&str],
) -> Result<&'a Map<String, Value>, ConfigError> {
    let map = value
        .as_object()
        .ok_or_else(|| ConfigError::new(name, "expected a table"))?;
    if map.keys().any(|k| !allowed.contains(&k.as_str())) {
        return Err(ConfigError::new(name, "unexpected field"));
    }
    for key in required {
        if !map.contains_key(*key) {
            return Err(ConfigError::new(format!("{name}.{key}"), "missing field"));
        }
    }
    Ok(map)
}
fn http_url(value: &str, name: &str) -> Result<String, ConfigError> {
    text(value, name, true)?;
    if value
        .chars()
        .any(|c| c <= ' ' || c == '\x7f' || "\\?#".contains(c))
    {
        return Err(ConfigError::new(name, "invalid HTTP URL"));
    }
    let (scheme, rest) = value
        .split_once("://")
        .ok_or_else(|| ConfigError::new(name, "expected absolute HTTP/HTTPS URL"))?;
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return Err(ConfigError::new(name, "expected absolute HTTP/HTTPS URL"));
    }
    let host = rest.split('/').next().unwrap_or("");
    normalize_origin(&format!("https://{host}"))
        .map_err(|_| ConfigError::new(name, "invalid HTTP URL authority"))?;
    Ok(value.trim_end_matches('/').to_owned())
}

fn check_json(
    value: &Value,
    depth: usize,
    remaining: &mut usize,
    name: &str,
) -> Result<(), ConfigError> {
    if depth > 128 {
        return Err(ConfigError::new(name, "JSON document depth limit exceeded"));
    }
    *remaining = remaining
        .checked_sub(1)
        .ok_or_else(|| ConfigError::new(name, "JSON document limit exceeded"))?;
    match value {
        Value::String(s) => {
            *remaining = remaining
                .checked_sub(s.len())
                .ok_or_else(|| ConfigError::new(name, "JSON document limit exceeded"))?;
        }
        Value::Object(values) => {
            for (key, value) in values {
                *remaining = remaining
                    .checked_sub(key.len() + 1)
                    .ok_or_else(|| ConfigError::new(name, "JSON document limit exceeded"))?;
                check_json(
                    value,
                    depth + usize::from(value.is_array() || value.is_object()),
                    remaining,
                    name,
                )?;
            }
        }
        Value::Array(values) => {
            for value in values {
                check_json(
                    value,
                    depth + usize::from(value.is_array() || value.is_object()),
                    remaining,
                    name,
                )?;
            }
        }
        Value::Number(n) => {
            let s = n.as_str();
            *remaining = remaining
                .checked_sub(s.len())
                .ok_or_else(|| ConfigError::new(name, "JSON document limit exceeded"))?;
            if s.len() > MAX_DOCUMENT_BYTES
                || (s.contains(['.', 'e', 'E']) && !s.parse::<f64>().is_ok_and(f64::is_finite))
            {
                return Err(ConfigError::new(name, "invalid or non-finite JSON number"));
            }
        }
        Value::Null | Value::Bool(_) => {}
    }
    Ok(())
}

struct BoundedWriter<W>(W, usize);
impl<W: Write> Write for BoundedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.1 {
            return Err(std::io::Error::other("JSON byte limit exceeded"));
        }
        let written = self.0.write(bytes)?;
        self.1 -= written;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}
fn bound_mapping(value: &Value, name: &str) -> Result<(), ConfigError> {
    let mut remaining = MAX_CONFIG_BYTES;
    check_json(value, 1, &mut remaining, name)?;
    // Count exact escaped JSON bytes into a sink before cloning. Node/string
    // limits alone undercount dense arrays, punctuation and escaped strings.
    let mut writer = BoundedWriter(std::io::sink(), MAX_CONFIG_BYTES);
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| ConfigError::new(name, "invalid or oversized configuration"))
}
pub(crate) fn bounded_json_bytes(value: &Value, name: &str) -> Result<Vec<u8>, ConfigError> {
    let mut remaining = MAX_DOCUMENT_BYTES;
    check_json(value, 1, &mut remaining, name)?;
    let mut writer = BoundedWriter(Vec::new(), MAX_DOCUMENT_BYTES);
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| ConfigError::new(name, "invalid or oversized JSON document"))?;
    parse_document(&writer.0, MAX_DOCUMENT_BYTES)
        .map_err(|_| ConfigError::new(name, "invalid JSON document"))?;
    Ok(writer.0)
}

/// Additional discovery service, retaining bounded arbitrary JSON extensions.
#[derive(Clone, PartialEq)]
pub struct AdditionalService {
    document: Value,
}
impl AdditionalService {
    pub fn from_mapping(value: &Value) -> Result<Self, ConfigError> {
        bounded_json_bytes(value, "provider.services")?;
        let name = string(
            value.get("name").unwrap_or(&Value::Null),
            "provider.services.name",
            true,
        )?;
        string(
            value.get("endpoint").unwrap_or(&Value::Null),
            "provider.services.endpoint",
            true,
        )?;
        if name == "mppi" {
            return Err(ConfigError::new(
                "provider.services",
                "MPPI service is derived by SDK",
            ));
        }
        Ok(Self {
            document: value.clone(),
        })
    }
    pub fn from_bytes(raw: &[u8]) -> Result<Self, ConfigError> {
        Self::from_mapping(
            &parse_document(raw, MAX_DOCUMENT_BYTES)
                .map_err(|_| ConfigError::new("provider.services", "invalid additional service"))?,
        )
    }
    pub fn as_object(&self) -> &Value {
        &self.document
    }
}
impl fmt::Debug for AdditionalService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AdditionalService { .. }")
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProviderDetails {
    name: String,
    description: String,
    public_origin: String,
    recipient: Address,
    image: Option<String>,
    services: Vec<AdditionalService>,
}
impl ProviderDetails {
    pub fn new(
        name: String,
        description: String,
        public_origin: String,
        recipient: Address,
        image: Option<String>,
        services: Vec<AdditionalService>,
    ) -> Result<Self, ConfigError> {
        text(&name, "provider.name", true)?;
        text(&description, "provider.description", false)?;
        let public_origin = normalize_origin(&public_origin)
            .map_err(|_| ConfigError::new("provider.public_origin", "expected an HTTPS origin"))?;
        nonzero_address(recipient, "provider.recipient")?;
        if let Some(image) = &image {
            text(image, "provider.image", true)?;
            let scheme = image
                .split_once(':')
                .map(|(scheme, _)| scheme)
                .unwrap_or("");
            if !scheme
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphabetic)
                || !scheme
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"+-.".contains(&b))
                || image.chars().any(|c| c <= ' ')
            {
                return Err(ConfigError::new("provider.image", "expected a URI"));
            }
        }
        Ok(Self {
            name,
            description,
            public_origin,
            recipient,
            image,
            services,
        })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn description(&self) -> &str {
        &self.description
    }
    pub fn public_origin(&self) -> &str {
        &self.public_origin
    }
    pub fn recipient(&self) -> Address {
        self.recipient
    }
    pub fn image(&self) -> Option<&str> {
        self.image.as_deref()
    }
    pub fn services(&self) -> &[AdditionalService] {
        &self.services
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderIdentityConfig {
    provider_id: U256,
    wallet_address: Address,
    wallet_id: Option<String>,
}
impl ProviderIdentityConfig {
    pub fn new(
        provider_id: U256,
        wallet_address: Address,
        wallet_id: Option<String>,
    ) -> Result<Self, ConfigError> {
        nonzero_address(wallet_address, "identity.wallet_address")?;
        if let Some(id) = &wallet_id {
            text(id, "identity.wallet_id", true)?;
            let digits = id
                .replace("urn:", "")
                .replace("uuid:", "")
                .trim_matches(['{', '}'])
                .replace('-', "");
            if digits.len() != 32 || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(ConfigError::new(
                    "identity.wallet_id",
                    "expected a wallet UUID",
                ));
            }
        }
        Ok(Self {
            provider_id,
            wallet_address,
            wallet_id,
        })
    }
    pub fn provider_id(&self) -> U256 {
        self.provider_id
    }
    pub fn wallet_address(&self) -> Address {
        self.wallet_address
    }
    pub fn wallet_id(&self) -> Option<&str> {
        self.wallet_id.as_deref()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ChainConfig {
    max_age_ms: u64,
    network: String,
    rpc: Option<String>,
}
impl ChainConfig {
    pub fn new(max_age_ms: u64, network: String, rpc: Option<String>) -> Result<Self, ConfigError> {
        positive(max_age_ms, "chain.max_age_ms", true)?;
        if !matches!(network.as_str(), "mainnet" | "testnet") {
            return Err(ConfigError::new(
                "chain.network",
                "expected mainnet or testnet",
            ));
        }
        if let Some(rpc) = &rpc {
            http_url(rpc, "chain.rpc")?;
        }
        Ok(Self {
            max_age_ms,
            network,
            rpc,
        })
    }
    pub fn max_age_ms(&self) -> u64 {
        self.max_age_ms
    }
    pub fn network(&self) -> &str {
        &self.network
    }
    pub fn chain_id(&self) -> u64 {
        if self.network == "mainnet" {
            5042
        } else {
            5042002
        }
    }
    pub fn rpc_url(&self) -> &str {
        self.rpc.as_deref().unwrap_or(if self.network == "mainnet" {
            "https://rpc.mainnet.arc.io"
        } else {
            "https://rpc.testnet.arc.io"
        })
    }
}
impl fmt::Debug for ChainConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChainConfig")
            .field("max_age_ms", &self.max_age_ms)
            .field("network", &self.network)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct UpstreamConfig {
    url: String,
    api_key_env: Option<String>,
}
impl UpstreamConfig {
    pub fn new(url: String, api_key_env: Option<String>) -> Result<Self, ConfigError> {
        let url = http_url(&url, "upstream.url")?;
        if let Some(name) = &api_key_env
            && (!name
                .as_bytes()
                .first()
                .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
                || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'))
        {
            return Err(ConfigError::new(
                "upstream.api_key_env",
                "expected an environment variable name",
            ));
        }
        Ok(Self { url, api_key_env })
    }
    pub fn url(&self) -> &str {
        &self.url
    }
    pub fn api_key_env(&self) -> Option<&str> {
        self.api_key_env.as_deref()
    }
    pub fn completions_url(&self) -> String {
        format!("{}/chat/completions", self.url)
    }
}
impl fmt::Debug for UpstreamConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpstreamConfig")
            .field("api_key_env", &self.api_key_env)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelConfig {
    model: String,
    description: String,
    context_window: u64,
    max_output: u64,
    modalities: Vec<String>,
    embeddings: bool,
    streaming: bool,
    tools: bool,
    structured_output: bool,
    rates: Rates,
}
impl ModelConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        model: String,
        description: String,
        context_window: u64,
        max_output: u64,
        modalities: Vec<String>,
        embeddings: bool,
        streaming: bool,
        tools: bool,
        structured_output: bool,
        rates: Rates,
    ) -> Result<Self, ConfigError> {
        validate_model(&model)
            .map_err(|_| ConfigError::new("models.model", "invalid model identifier"))?;
        text(&description, "models.description", false)?;
        positive(context_window, "models.context_window", false)?;
        positive(max_output, "models.max_output", false)?;
        if max_output > context_window {
            return Err(ConfigError::new(
                "models.max_output",
                "exceeds context window",
            ));
        }
        let mut seen = BTreeSet::new();
        if modalities.is_empty()
            || modalities.iter().any(|m| {
                !matches!(m.as_str(), "text" | "image" | "audio" | "video") || !seen.insert(m)
            })
        {
            return Err(ConfigError::new(
                "models.modalities",
                "expected unique supported modalities",
            ));
        }
        Ok(Self {
            model,
            description,
            context_window,
            max_output,
            modalities,
            embeddings,
            streaming,
            tools,
            structured_output,
            rates,
        })
    }
    pub fn model(&self) -> &str {
        &self.model
    }
    pub fn description(&self) -> &str {
        &self.description
    }
    pub fn context_window(&self) -> u64 {
        self.context_window
    }
    pub fn max_output(&self) -> u64 {
        self.max_output
    }
    pub fn modalities(&self) -> &[String] {
        &self.modalities
    }
    pub fn embeddings(&self) -> bool {
        self.embeddings
    }
    pub fn streaming(&self) -> bool {
        self.streaming
    }
    pub fn tools(&self) -> bool {
        self.tools
    }
    pub fn structured_output(&self) -> bool {
        self.structured_output
    }
    pub fn rates(&self) -> &Rates {
        &self.rates
    }
    pub fn card(&self) -> Value {
        json!({"model":self.model,"description":self.description,"context_window":self.context_window,"max_output":self.max_output,"modalities":self.modalities,"embeddings":self.embeddings,"streaming":self.streaming,"tools":self.tools,"structured_output":self.structured_output})
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaymentConfig {
    collection_margin_ms: u64,
    authorization_wait_ms: u64,
    output_buffer_tokens: u64,
    suggest_session_tokens: u64,
}
impl PaymentConfig {
    pub fn new(
        collection_margin_ms: u64,
        authorization_wait_ms: u64,
        output_buffer_tokens: u64,
        suggest_session_tokens: u64,
    ) -> Result<Self, ConfigError> {
        positive(collection_margin_ms, "payment.collection_margin_ms", true)?;
        positive(authorization_wait_ms, "payment.authorization_wait_ms", true)?;
        positive(output_buffer_tokens, "payment.output_buffer_tokens", false)?;
        positive(
            suggest_session_tokens,
            "payment.suggest_session_tokens",
            false,
        )?;
        Ok(Self {
            collection_margin_ms,
            authorization_wait_ms,
            output_buffer_tokens,
            suggest_session_tokens,
        })
    }
    pub fn collection_margin_ms(&self) -> u64 {
        self.collection_margin_ms
    }
    pub fn authorization_wait_ms(&self) -> u64 {
        self.authorization_wait_ms
    }
    pub fn output_buffer_tokens(&self) -> u64 {
        self.output_buffer_tokens
    }
    pub fn suggest_session_tokens(&self) -> u64 {
        self.suggest_session_tokens
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettlementConfig {
    interval_ms: u64,
    amount: Option<u64>,
    units: Option<u64>,
}
impl SettlementConfig {
    pub fn new(
        interval_ms: u64,
        amount: Option<u64>,
        units: Option<u64>,
    ) -> Result<Self, ConfigError> {
        positive(interval_ms, "settlement.interval_ms", true)?;
        if let Some(v) = amount {
            positive(v, "settlement.amount", false)?;
        }
        if let Some(v) = units {
            positive(v, "settlement.units", false)?;
        }
        Ok(Self {
            interval_ms,
            amount,
            units,
        })
    }
    pub fn interval_ms(&self) -> u64 {
        self.interval_ms
    }
    pub fn amount(&self) -> Option<u64> {
        self.amount
    }
    pub fn units(&self) -> Option<u64> {
        self.units
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ListenerConfig {
    bind: String,
    tls_mode: String,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
}
impl ListenerConfig {
    pub fn new(
        bind: String,
        tls_mode: String,
        tls_cert: Option<PathBuf>,
        tls_key: Option<PathBuf>,
    ) -> Result<Self, ConfigError> {
        text(&bind, "listen.bind", true)?;
        normalize_origin(&format!("https://{bind}"))
            .map_err(|_| ConfigError::new("listen.bind", "invalid listener address"))?;
        if authority(&bind)
            .map_err(|_| ConfigError::new("listen.bind", "invalid listener address"))?
            .1
            .is_none()
        {
            return Err(ConfigError::new("listen.bind", "expected host and port"));
        }
        for path in [&tls_cert, &tls_key].into_iter().flatten() {
            if path.as_os_str().is_empty() || path.to_string_lossy().contains('\0') {
                return Err(ConfigError::new("listen", "invalid credential path"));
            }
        }
        match tls_mode.as_str() {
            "direct" if tls_cert.is_some() && tls_key.is_some() => {}
            "ingress" if tls_cert.is_none() && tls_key.is_none() => {}
            _ => {
                return Err(ConfigError::new(
                    "listen",
                    "direct TLS requires paths; ingress TLS rejects paths",
                ));
            }
        }
        Ok(Self {
            bind,
            tls_mode,
            tls_cert,
            tls_key,
        })
    }
    pub fn bind(&self) -> &str {
        &self.bind
    }
    pub fn tls_mode(&self) -> &str {
        &self.tls_mode
    }
    pub fn tls_cert(&self) -> Option<&Path> {
        self.tls_cert.as_deref()
    }
    pub fn tls_key(&self) -> Option<&Path> {
        self.tls_key.as_deref()
    }
}
impl fmt::Debug for ListenerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListenerConfig")
            .field("bind", &self.bind)
            .field("tls_mode", &self.tls_mode)
            .field("tls_cert", &self.tls_cert)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProviderDeclarations {
    provider: ProviderDetails,
    chain: ChainConfig,
    upstream: UpstreamConfig,
    models: Vec<ModelConfig>,
    payment: PaymentConfig,
    settlement: SettlementConfig,
    listen: Option<ListenerConfig>,
    catalog: Catalog,
}
impl ProviderDeclarations {
    /// Validate setup before an identity exists. Absence is not identity zero.
    pub fn from_mapping(values: &Value) -> Result<Self, ConfigError> {
        bound_mapping(values, "configuration")?;
        if values.get("identity").is_some() {
            return Err(ConfigError::new(
                "identity",
                "declarations must not contain identity",
            ));
        }
        Ok(parse_parts(values, false)?.0)
    }
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        Self::from_mapping(&load_mapping(path.as_ref())?)
    }
    pub fn provider(&self) -> &ProviderDetails {
        &self.provider
    }
    pub fn chain(&self) -> &ChainConfig {
        &self.chain
    }
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
    pub fn with_identity(
        &self,
        identity: ProviderIdentityConfig,
    ) -> Result<ProviderConfig, ConfigError> {
        ProviderConfig::new(
            self.provider.clone(),
            identity,
            self.chain.clone(),
            self.upstream.clone(),
            self.models.clone(),
            self.payment.clone(),
            self.settlement.clone(),
            self.listen.clone(),
        )
    }
    pub fn to_mapping(&self) -> Value {
        declarations_mapping(
            &self.provider,
            &self.chain,
            &self.upstream,
            &self.models,
            &self.payment,
            &self.settlement,
            self.listen.as_ref(),
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProviderConfig {
    provider: ProviderDetails,
    identity: ProviderIdentityConfig,
    chain: ChainConfig,
    upstream: UpstreamConfig,
    models: Vec<ModelConfig>,
    payment: PaymentConfig,
    settlement: SettlementConfig,
    listen: Option<ListenerConfig>,
    catalog: Catalog,
}
impl ProviderConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        provider: ProviderDetails,
        identity: ProviderIdentityConfig,
        chain: ChainConfig,
        upstream: UpstreamConfig,
        models: Vec<ModelConfig>,
        payment: PaymentConfig,
        settlement: SettlementConfig,
        listen: Option<ListenerConfig>,
    ) -> Result<Self, ConfigError> {
        if provider.recipient() == identity.wallet_address() {
            return Err(ConfigError::new(
                "provider.recipient",
                "must differ from operator wallet",
            ));
        }
        let rows = models.iter().map(|m| {
            CatalogRow::new(m.model().to_owned(), *m.rates()).expect("validated model record")
        });
        let catalog = publish_catalog(rows, DEFAULT_CATALOG_BYTES)
            .map_err(|_| ConfigError::new("models", "invalid, duplicate or oversized catalog"))?;
        Ok(Self {
            provider,
            identity,
            chain,
            upstream,
            models,
            payment,
            settlement,
            listen,
            catalog,
        })
    }
    pub fn provider(&self) -> &ProviderDetails {
        &self.provider
    }
    pub fn identity(&self) -> &ProviderIdentityConfig {
        &self.identity
    }
    pub fn chain(&self) -> &ChainConfig {
        &self.chain
    }
    pub fn upstream(&self) -> &UpstreamConfig {
        &self.upstream
    }
    pub fn models(&self) -> &[ModelConfig] {
        &self.models
    }
    pub fn payment(&self) -> &PaymentConfig {
        &self.payment
    }
    pub fn settlement(&self) -> &SettlementConfig {
        &self.settlement
    }
    pub fn listen(&self) -> Option<&ListenerConfig> {
        self.listen.as_ref()
    }
    /// Desired publication only; this getter performs no registry write.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Complete non-secret declarations, preserving exact decimal money values.
    pub fn to_mapping(&self) -> Value {
        let mut value = declarations_mapping(
            &self.provider,
            &self.chain,
            &self.upstream,
            &self.models,
            &self.payment,
            &self.settlement,
            self.listen.as_ref(),
        );
        let mut identity = json!({"provider_id": self.identity.provider_id().to_string(), "wallet_address": format!("{:#x}", self.identity.wallet_address())});
        if let Some(id) = self.identity.wallet_id() {
            identity["wallet_id"] = json!(id);
        }
        value["identity"] = identity;
        value
    }

    pub fn to_toml(&self) -> Result<String, ConfigError> {
        let text = toml::to_string(&json_toml(&self.to_mapping())?).map_err(|_| {
            ConfigError::new(
                "configuration",
                "declarations cannot be represented in TOML",
            )
        })?;
        if text.len() > MAX_CONFIG_BYTES {
            return Err(ConfigError::new(
                "configuration",
                "TOML byte limit exceeded",
            ));
        }
        Ok(text)
    }

    pub fn from_mapping(values: &Value, overrides: Option<&Value>) -> Result<Self, ConfigError> {
        object(values, "configuration", SECTIONS, &[])?;
        // Bound every caller-owned input before any cloning, including data
        // which an override will replace. Number spellings are borrowed.
        bound_mapping(values, "configuration")?;
        if let Some(overrides) = overrides {
            object(overrides, "overrides", SECTIONS, &[])?;
            bound_mapping(overrides, "overrides")?;
        }
        let mut values = values.clone();
        if let Some(overrides) = overrides {
            let changes = object(overrides, "overrides", SECTIONS, &[])?;
            for (name, value) in changes {
                if name != "models"
                    && value.is_object()
                    && values.get(name).is_some_and(Value::is_object)
                {
                    values[name]
                        .as_object_mut()
                        .expect("checked table")
                        .extend(value.as_object().expect("checked table").clone());
                } else {
                    values[name] = value.clone();
                }
            }
        }
        parse_config(&values)
    }

    pub fn from_file(
        path: impl AsRef<Path>,
        overrides: Option<&Value>,
    ) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        Self::from_mapping(&load_mapping(path)?, overrides).map_err(|mut error| {
            error.source = Some(if path.is_absolute() {
                path.to_owned()
            } else {
                std::env::current_dir().unwrap_or_default().join(path)
            });
            error
        })
    }
}

// serde_json with arbitrary_precision serializes Number using a private struct
// representation. Convert numeric leaves explicitly instead of exposing that
// JSON-only representation to another serializer. TOML integers are signed i64;
// money/identity values are already exact quoted decimal strings above.
fn json_toml(value: &Value) -> Result<toml::Value, ConfigError> {
    let invalid = || {
        ConfigError::new(
            "configuration",
            "declarations cannot be represented in TOML",
        )
    };
    Ok(match value {
        Value::String(value) => toml::Value::String(value.clone()),
        Value::Bool(value) => toml::Value::Boolean(*value),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                toml::Value::Integer(value)
            } else if value.is_f64() {
                toml::Value::Float(
                    value
                        .as_f64()
                        .filter(|v| v.is_finite())
                        .ok_or_else(invalid)?,
                )
            } else {
                return Err(invalid());
            }
        }
        Value::Array(values) => {
            toml::Value::Array(values.iter().map(json_toml).collect::<Result<_, _>>()?)
        }
        Value::Object(values) => toml::Value::Table(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_toml(value)?)))
                .collect::<Result<_, ConfigError>>()?,
        ),
        Value::Null => return Err(invalid()),
    })
}

pub(crate) fn load_mapping(path: &Path) -> Result<Value, ConfigError> {
    let filename = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|_| ConfigError::new("configuration", "cannot resolve working directory"))?
            .join(path)
    };
    let load = || -> Result<Value, ConfigError> {
        let file = File::open(&filename)
            .map_err(|_| ConfigError::new("configuration", "cannot read configuration file"))?;
        let mut raw = Vec::new();
        file.take(MAX_CONFIG_BYTES as u64 + 1)
            .read_to_end(&mut raw)
            .map_err(|_| ConfigError::new("configuration", "cannot read configuration file"))?;
        if raw.len() > MAX_CONFIG_BYTES {
            return Err(ConfigError::new(
                "configuration",
                "TOML byte limit exceeded",
            ));
        }
        let source = std::str::from_utf8(&raw)
            .map_err(|_| ConfigError::new("configuration", "invalid TOML"))?;
        let parsed: toml::Value = toml::from_str(source)
            .map_err(|_| ConfigError::new("configuration", "invalid TOML"))?;
        let mut values = toml_json(parsed, 1)?;
        require_quoted_money(&values)?;
        if let Some(listener) = values.get_mut("listen").and_then(Value::as_object_mut) {
            for name in ["tls_cert", "tls_key"] {
                if let Some(value) = listener.get_mut(name) {
                    let resolved = path_value(value, &format!("listen.{name}"), filename.parent())?;
                    *value = Value::String(
                        resolved
                            .to_str()
                            .ok_or_else(|| {
                                ConfigError::new("listen", "credential path is not UTF-8")
                            })?
                            .to_owned(),
                    );
                }
            }
        }
        Ok(values)
    };
    load().map_err(|mut error| {
        error.source = Some(filename);
        error
    })
}

#[allow(clippy::too_many_arguments)]
fn declarations_mapping(
    provider: &ProviderDetails,
    chain: &ChainConfig,
    upstream: &UpstreamConfig,
    models: &[ModelConfig],
    payment: &PaymentConfig,
    settlement: &SettlementConfig,
    listen: Option<&ListenerConfig>,
) -> Value {
    let mut value = json!({
        "provider":{"name":provider.name(), "description":provider.description(), "public_origin":provider.public_origin(), "recipient":format!("{:#x}",provider.recipient()), "services":provider.services().iter().map(AdditionalService::as_object).collect::<Vec<_>>()},
        "chain":{"network":chain.network(), "max_age_ms":chain.max_age_ms()},
        "upstream":{"url":upstream.url()},
        "models": models.iter().map(|model| {
            let mut row = model.card();
            row["rates"] = json!({"input":model.rates().input().to_string(), "output":model.rates().output().to_string()});
            if let Some(cache)=model.rates().cache() { row["rates"]["cache"]=json!(cache.to_string()); }
            row
        }).collect::<Vec<_>>(),
        "payment":{"collection_margin_ms":payment.collection_margin_ms(), "authorization_wait_ms":payment.authorization_wait_ms(), "output_buffer_tokens":payment.output_buffer_tokens(), "suggest_session_tokens":payment.suggest_session_tokens()},
        "settlement":{"interval_ms":settlement.interval_ms()}
    });
    if let Some(image) = provider.image() {
        value["provider"]["image"] = json!(image);
    }
    if let Some(rpc) = &chain.rpc {
        value["chain"]["rpc"] = json!(rpc);
    }
    if let Some(name) = upstream.api_key_env() {
        value["upstream"]["api_key_env"] = json!(name);
    }
    if let Some(amount) = settlement.amount() {
        value["settlement"]["amount"] = json!(amount.to_string());
    }
    if let Some(units) = settlement.units() {
        value["settlement"]["units"] = json!(units);
    }
    if let Some(listen) = listen {
        value["listen"] = json!({"bind":listen.bind(), "tls_mode":listen.tls_mode()});
        if let Some(cert) = listen.tls_cert() {
            value["listen"]["tls_cert"] = json!(cert);
        }
        if let Some(key) = listen.tls_key() {
            value["listen"]["tls_key"] = json!(key);
        }
    }
    value
}

fn path_value(value: &Value, name: &str, base: Option<&Path>) -> Result<PathBuf, ConfigError> {
    let value = string(value, name, true)?;
    if value.contains('\0') {
        return Err(ConfigError::new(name, "invalid path"));
    }
    let path = PathBuf::from(value);
    if path.is_absolute() {
        return Ok(path);
    }
    let base = match base {
        Some(base) => base.to_owned(),
        None => std::env::current_dir()
            .map_err(|_| ConfigError::new(name, "cannot resolve working directory"))?,
    };
    Ok(base.join(path))
}

fn toml_json(value: toml::Value, depth: usize) -> Result<Value, ConfigError> {
    if depth > 128 {
        return Err(ConfigError::new(
            "configuration",
            "TOML nesting limit exceeded",
        ));
    }
    Ok(match value {
        toml::Value::String(v) => Value::String(v),
        toml::Value::Integer(v) => Value::Number(v.into()),
        toml::Value::Boolean(v) => Value::Bool(v),
        toml::Value::Float(v) => Value::Number(
            serde_json::Number::from_f64(v)
                .ok_or_else(|| ConfigError::new("configuration", "non-finite TOML number"))?,
        ),
        toml::Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|v| toml_json(v, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
        toml::Value::Table(values) => Value::Object(
            values
                .into_iter()
                .map(|(k, v)| Ok((k, toml_json(v, depth + 1)?)))
                .collect::<Result<_, ConfigError>>()?,
        ),
        toml::Value::Datetime(_) => {
            return Err(ConfigError::new(
                "configuration",
                "TOML dates are not configuration values",
            ));
        }
    })
}
fn require_quoted_money(values: &Value) -> Result<(), ConfigError> {
    for (section, key) in [("identity", "provider_id"), ("settlement", "amount")] {
        if values
            .get(section)
            .and_then(|v| v.get(key))
            .is_some_and(|v| !v.is_string())
        {
            return Err(ConfigError::new(
                format!("{section}.{key}"),
                "TOML amount and provider id must be decimal strings",
            ));
        }
    }
    if let Some(models) = values.get("models").and_then(Value::as_array) {
        for model in models {
            if model
                .get("rates")
                .and_then(Value::as_object)
                .is_some_and(|m| m.values().any(|v| !v.is_string()))
            {
                return Err(ConfigError::new(
                    "models.rates",
                    "TOML rates must be decimal strings",
                ));
            }
        }
    }
    Ok(())
}

fn parse_config(values: &Value) -> Result<ProviderConfig, ConfigError> {
    let (declarations, identity) = parse_parts(values, true)?;
    declarations.with_identity(identity.expect("required identity was validated"))
}
fn parse_parts(
    values: &Value,
    require_identity: bool,
) -> Result<(ProviderDeclarations, Option<ProviderIdentityConfig>), ConfigError> {
    object(
        values,
        "configuration",
        SECTIONS,
        &[
            "provider",
            "chain",
            "upstream",
            "models",
            "payment",
            "settlement",
        ],
    )?;
    let identity = match values.get("identity") {
        Some(value) => {
            let i = object(
                value,
                "identity",
                &["provider_id", "wallet_address", "wallet_id"],
                &["provider_id", "wallet_address"],
            )?;
            let provider_id = match &i["provider_id"] {
                Value::String(s) => decimal_uint256(s),
                v => exact_uint256(v),
            }
            .map_err(|_| {
                ConfigError::new("identity.provider_id", "expected canonical decimal uint256")
            })?;
            Some(ProviderIdentityConfig::new(
                provider_id,
                address(&i["wallet_address"], "identity.wallet_address")?,
                optional_string(i, "wallet_id", "identity.wallet_id")?,
            )?)
        }
        None if require_identity => return Err(ConfigError::new("identity", "missing field")),
        None => None,
    };
    let p = object(
        &values["provider"],
        "provider",
        &[
            "name",
            "description",
            "public_origin",
            "recipient",
            "image",
            "services",
        ],
        &["name", "description", "public_origin", "recipient"],
    )?;
    let c = object(
        &values["chain"],
        "chain",
        &["network", "rpc", "max_age_ms"],
        &["max_age_ms"],
    )?;
    let u = object(
        &values["upstream"],
        "upstream",
        &["url", "api_key_env"],
        &["url"],
    )?;
    let pay = object(
        &values["payment"],
        "payment",
        &[
            "collection_margin_ms",
            "authorization_wait_ms",
            "output_buffer_tokens",
            "suggest_session_tokens",
        ],
        &["collection_margin_ms", "authorization_wait_ms"],
    )?;
    let s = object(
        &values["settlement"],
        "settlement",
        &["interval_ms", "amount", "units"],
        &["interval_ms"],
    )?;
    let services = match p.get("services") {
        Some(v) => v
            .as_array()
            .ok_or_else(|| ConfigError::new("provider.services", "expected array"))?
            .iter()
            .map(AdditionalService::from_mapping)
            .collect::<Result<_, _>>()?,
        None => vec![],
    };
    let models: Vec<ModelConfig> = values["models"]
        .as_array()
        .ok_or_else(|| ConfigError::new("models", "expected array"))?
        .iter()
        .map(parse_model)
        .collect::<Result<_, _>>()?;
    let listen = values
        .get("listen")
        .map(|value| {
            let fields = object(
                value,
                "listen",
                &["bind", "tls_mode", "tls_cert", "tls_key"],
                &["bind", "tls_mode"],
            )?;
            ListenerConfig::new(
                string(&fields["bind"], "listen.bind", true)?.into(),
                string(&fields["tls_mode"], "listen.tls_mode", true)?.into(),
                fields
                    .get("tls_cert")
                    .map(|v| path_value(v, "listen.tls_cert", None))
                    .transpose()?,
                fields
                    .get("tls_key")
                    .map(|v| path_value(v, "listen.tls_key", None))
                    .transpose()?,
            )
        })
        .transpose()?;
    let rows = models
        .iter()
        .map(|m| CatalogRow::new(m.model().to_owned(), *m.rates()).expect("validated model"));
    let catalog = publish_catalog(rows, DEFAULT_CATALOG_BYTES)
        .map_err(|_| ConfigError::new("models", "invalid, duplicate or oversized catalog"))?;
    Ok((
        ProviderDeclarations {
            provider: ProviderDetails::new(
                string(&p["name"], "provider.name", true)?.into(),
                string(&p["description"], "provider.description", false)?.into(),
                string(&p["public_origin"], "provider.public_origin", true)?.into(),
                address(&p["recipient"], "provider.recipient")?,
                optional_string(p, "image", "provider.image")?,
                services,
            )?,
            chain: ChainConfig::new(
                integer(&c["max_age_ms"], "chain.max_age_ms", true)?,
                c.get("network")
                    .map(|v| string(v, "chain.network", true).map(String::from))
                    .transpose()?
                    .unwrap_or_else(|| "mainnet".into()),
                optional_string(c, "rpc", "chain.rpc")?,
            )?,
            upstream: UpstreamConfig::new(
                string(&u["url"], "upstream.url", true)?.into(),
                optional_string(u, "api_key_env", "upstream.api_key_env")?,
            )?,
            models,
            payment: PaymentConfig::new(
                integer(
                    &pay["collection_margin_ms"],
                    "payment.collection_margin_ms",
                    true,
                )?,
                integer(
                    &pay["authorization_wait_ms"],
                    "payment.authorization_wait_ms",
                    true,
                )?,
                pay.get("output_buffer_tokens")
                    .map(|v| integer(v, "payment.output_buffer_tokens", false))
                    .transpose()?
                    .unwrap_or(1000),
                pay.get("suggest_session_tokens")
                    .map(|v| integer(v, "payment.suggest_session_tokens", false))
                    .transpose()?
                    .unwrap_or(1_000_000),
            )?,
            settlement: SettlementConfig::new(
                integer(&s["interval_ms"], "settlement.interval_ms", true)?,
                s.get("amount")
                    .map(|v| amount(v, "settlement.amount"))
                    .transpose()?,
                s.get("units")
                    .map(|v| integer(v, "settlement.units", false))
                    .transpose()?,
            )?,
            listen,
            catalog,
        },
        identity,
    ))
}
fn optional_string(
    values: &Map<String, Value>,
    key: &str,
    name: &str,
) -> Result<Option<String>, ConfigError> {
    values
        .get(key)
        .filter(|v| !v.is_null())
        .map(|v| string(v, name, true).map(String::from))
        .transpose()
}
fn parse_model(value: &Value) -> Result<ModelConfig, ConfigError> {
    let fields = &[
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
    ];
    let m = object(value, "models", fields, fields)?;
    let r = object(
        &m["rates"],
        "models.rates",
        &["input", "output", "cache"],
        &["input", "output"],
    )?;
    let modalities = m["modalities"]
        .as_array()
        .ok_or_else(|| ConfigError::new("models.modalities", "expected array"))?
        .iter()
        .map(|v| string(v, "models.modalities", true).map(String::from))
        .collect::<Result<_, _>>()?;
    let flag = |name: &str| {
        m[name]
            .as_bool()
            .ok_or_else(|| ConfigError::new("models", "capability flags must be booleans"))
    };
    ModelConfig::new(
        string(&m["model"], "models.model", true)?.into(),
        string(&m["description"], "models.description", false)?.into(),
        integer(&m["context_window"], "models.context_window", false)?,
        integer(&m["max_output"], "models.max_output", false)?,
        modalities,
        flag("embeddings")?,
        flag("streaming")?,
        flag("tools")?,
        flag("structured_output")?,
        Rates::new(
            amount(&r["input"], "models.rates.input")?,
            amount(&r["output"], "models.rates.output")?,
            r.get("cache")
                .map(|v| amount(v, "models.rates.cache"))
                .transpose()?,
        ),
    )
}
