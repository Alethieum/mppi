//! Circle developer-controlled SCA operations on Arc.
//!
//! Credentials are supplied by the caller. Transaction references bind to exact
//! calls in an explicit local wallet journal before submission. Reconciliation
//! only reads the original service operation; Circle status is never chain proof.
//! This journal holds wallet idempotency evidence, not provider payment history.
//! The adapter trusts authenticated Circle reference/transaction-ID lookup to
//! identify its transaction hash. Canonical effects are verified separately;
//! Circle lookup does not provide independent proof of inner calldata.

use crate::{
    Address, B256,
    capabilities::{TransactionWallet, WalletError},
    discovery::parse_address,
    local_state::LocalState,
    types::{OperationResult, OperationState, WalletCall},
};
use aws_lc_rs::rsa::{OAEP_SHA256_MGF1SHA256, OaepPublicEncryptingKey, PublicEncryptingKey};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::{Client, Method, header};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use zeroize::Zeroizing;

const ORIGIN: &str = "https://api.circle.com/v1/w3s";
const MAX_RESPONSE: usize = 1 << 20;
const MAX_REQUEST: usize = 4 << 20;
const MAX_CALL: usize = 1 << 20;
const MAX_JOURNAL: usize = 16 << 20;
const MAX_OPERATIONS: usize = 16_384;

/// Sanitized errors never contain credentials, request bodies or service text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircleError {
    Configuration,
    Unavailable,
    InvalidEvidence,
    Journal,
    Capacity,
}
impl fmt::Display for CircleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Configuration => "invalid Circle wallet configuration",
            Self::Unavailable => "Circle service evidence unavailable",
            Self::InvalidEvidence => "Circle evidence differs from the original operation",
            Self::Journal => "Circle wallet journal unavailable or inconsistent",
            Self::Capacity => "Circle wallet journal capacity reached; retain its original operation evidence before changing configuration",
        })
    }
}
impl std::error::Error for CircleError {}

/// Registered Circle API credentials. Neither Debug nor errors reveal secrets.
pub struct CircleCredentials {
    api_key: Zeroizing<String>,
    entity_secret: Zeroizing<[u8; 32]>,
}
impl CircleCredentials {
    pub fn new(api_key: String, entity_secret: [u8; 32]) -> Result<Self, CircleError> {
        let api_key = Zeroizing::new(api_key);
        let entity_secret = Zeroizing::new(entity_secret);
        if api_key.is_empty()
            || api_key.len() > 8192
            || api_key.bytes().any(|byte| !(33..=126).contains(&byte))
        {
            return Err(CircleError::Configuration);
        }
        Ok(Self {
            api_key,
            entity_secret,
        })
    }
}
impl fmt::Debug for CircleCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CircleCredentials").finish_non_exhaustive()
    }
}

fn blockchain(chain_id: u64) -> Result<&'static str, CircleError> {
    match chain_id {
        5042 => Ok("ARC"),
        5_042_002 => Ok("ARC-TESTNET"),
        _ => Err(CircleError::Configuration),
    }
}
fn uuid(value: &str) -> Result<(), CircleError> {
    if value.len() != 36
        || !value.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
    {
        return Err(CircleError::InvalidEvidence);
    }
    Ok(())
}
fn idempotency_key(value: &str) -> Result<(), CircleError> {
    uuid(value)?;
    if value.as_bytes()[14] != b'4' || !matches!(value.as_bytes()[19], b'8' | b'9' | b'a' | b'b') {
        return Err(CircleError::Configuration);
    }
    Ok(())
}
fn fresh_key() -> Result<String, CircleError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|_| CircleError::Unavailable)?;
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    let hex = alloy_primitives::hex::encode(bytes);
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    ))
}
fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str, CircleError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or(CircleError::InvalidEvidence)
}
fn address(value: &Value, field: &str) -> Result<Address, CircleError> {
    parse_address(string(value, field)?).map_err(|_| CircleError::InvalidEvidence)
}
fn address_hex(value: Address) -> String {
    format!("{value:#x}")
}
fn bytes_hex(value: &str, max: usize) -> Result<Vec<u8>, CircleError> {
    let hex = value
        .strip_prefix("0x")
        .ok_or(CircleError::InvalidEvidence)?;
    if hex.len() > max * 2 || hex.len() % 2 != 0 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(CircleError::InvalidEvidence);
    }
    alloy_primitives::hex::decode(hex).map_err(|_| CircleError::InvalidEvidence)
}
fn hash(value: &str) -> Result<B256, CircleError> {
    B256::try_from(bytes_hex(value, 32)?.as_slice()).map_err(|_| CircleError::InvalidEvidence)
}
fn reference(value: &str) -> Result<(), CircleError> {
    if value.is_empty() || value.len() > 4096 {
        return Err(CircleError::Configuration);
    }
    Ok(())
}
fn name(value: &str) -> Result<(), CircleError> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(CircleError::Configuration);
    }
    Ok(())
}

#[tonic::async_trait]
pub(crate) trait CircleTransport: Send + Sync {
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, CircleError>;
}
struct CircleHttp {
    client: Client,
    api_key: Zeroizing<String>,
}
impl CircleHttp {
    fn new(api_key: Zeroizing<String>, timeout_ms: u64) -> Result<Self, CircleError> {
        if !(1..=86_400_000).contains(&timeout_ms) {
            return Err(CircleError::Configuration);
        }
        let client = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .referer(false)
            .http1_only()
            .tls_sslkeylogfile(false)
            .build()
            .map_err(|_| CircleError::Configuration)?;
        Ok(Self { client, api_key })
    }
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        body: Option<Value>,
    ) -> Result<Value, CircleError> {
        let mut authorization =
            header::HeaderValue::from_str(&format!("Bearer {}", &*self.api_key))
                .map_err(|_| CircleError::Configuration)?;
        authorization.set_sensitive(true);
        let mut request = request
            .header(header::AUTHORIZATION, authorization)
            .header(header::ACCEPT, "application/json")
            .header(header::ACCEPT_ENCODING, "identity");
        if let Some(body) = body {
            let body = serde_json::to_vec(&body).map_err(|_| CircleError::Configuration)?;
            if body.len() > MAX_REQUEST {
                return Err(CircleError::Configuration);
            }
            request = request
                .header(header::CONTENT_TYPE, "application/json")
                .body(body);
        }
        let mut response = request.send().await.map_err(|_| CircleError::Unavailable)?;
        if !matches!(response.status().as_u16(), 200 | 201) {
            return Err(CircleError::Unavailable);
        }
        let headers = response.headers();
        if headers.get_all(header::CONTENT_TYPE).iter().count() != 1
            || headers.get_all(header::CONTENT_ENCODING).iter().count() > 1
            || !headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| {
                    v.split(';')
                        .next()
                        .unwrap_or("")
                        .trim()
                        .eq_ignore_ascii_case("application/json")
                })
            || headers
                .get(header::CONTENT_ENCODING)
                .is_some_and(|v| !v.to_str().is_ok_and(|v| v.eq_ignore_ascii_case("identity")))
            || response
                .content_length()
                .is_some_and(|n| n > MAX_RESPONSE as u64)
        {
            return Err(CircleError::InvalidEvidence);
        }
        let mut raw = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| CircleError::Unavailable)?
        {
            if chunk.len() > MAX_RESPONSE - raw.len() {
                return Err(CircleError::InvalidEvidence);
            }
            raw.extend_from_slice(&chunk);
        }
        let value = crate::json::parse_document(&raw, MAX_RESPONSE, crate::DEFAULT_JSON_DEPTH)
            .map_err(|_| CircleError::InvalidEvidence)?;
        value
            .get("data")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or(CircleError::InvalidEvidence)
    }
}
#[tonic::async_trait]
impl CircleTransport for CircleHttp {
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, CircleError> {
        if !path.starts_with('/') || path.starts_with("//") || path.contains(['#', '\\']) {
            return Err(CircleError::Configuration);
        }
        self.send(self.client.request(method, format!("{ORIGIN}{path}")), body)
            .await
    }
}

/// Validated service wallet identity; obtaining it does not establish chain state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CircleWalletIdentity {
    wallet_id: String,
    address: Address,
    chain_id: u64,
    wallet_set_id: String,
}
impl CircleWalletIdentity {
    fn parse(value: &Value, wallet_id: Option<&str>, chain_id: u64) -> Result<Self, CircleError> {
        let id = string(value, "id")?;
        uuid(id)?;
        let wallet_set_id = string(value, "walletSetId")?;
        uuid(wallet_set_id)?;
        let address = address(value, "address")?;
        if wallet_id.is_some_and(|expected| expected != id)
            || string(value, "blockchain")? != blockchain(chain_id)?
            || string(value, "custodyType")? != "DEVELOPER"
            || string(value, "accountType")? != "SCA"
            || string(value, "state")? != "LIVE"
            || address.is_zero()
        {
            return Err(CircleError::InvalidEvidence);
        }
        Ok(Self {
            wallet_id: id.into(),
            address,
            chain_id,
            wallet_set_id: wallet_set_id.into(),
        })
    }
    pub fn wallet_id(&self) -> &str {
        &self.wallet_id
    }
    pub fn address(&self) -> Address {
        self.address
    }
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }
    pub fn wallet_set_id(&self) -> &str {
        &self.wallet_set_id
    }
}

/// Fixed-origin Circle API client. Setup callers retain their request and key
/// before invoking a mutating method, then preserve the returned resource IDs.
pub struct CircleClient {
    transport: Arc<dyn CircleTransport>,
    entity_secret: Zeroizing<[u8; 32]>,
}
impl fmt::Debug for CircleClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CircleClient").finish_non_exhaustive()
    }
}
impl CircleClient {
    #[cfg(test)]
    pub(crate) fn with_test_transport(transport: Arc<dyn CircleTransport>) -> Self {
        Self {
            transport,
            entity_secret: Zeroizing::new([0; 32]),
        }
    }
    pub fn new(credentials: CircleCredentials, timeout_ms: u64) -> Result<Self, CircleError> {
        Ok(Self {
            transport: Arc::new(CircleHttp::new(credentials.api_key, timeout_ms)?),
            entity_secret: credentials.entity_secret,
        })
    }
    async fn ciphertext(&self) -> Result<String, CircleError> {
        let value = self
            .transport
            .request(Method::GET, "/config/entity/publicKey", None)
            .await?;
        encrypt_secret(string(&value, "publicKey")?, &self.entity_secret)
    }
    pub async fn wallet(
        &self,
        wallet_id: &str,
        chain_id: u64,
    ) -> Result<CircleWalletIdentity, CircleError> {
        uuid(wallet_id)?;
        blockchain(chain_id)?;
        let value = self
            .transport
            .request(Method::GET, &format!("/wallets/{wallet_id}"), None)
            .await?;
        CircleWalletIdentity::parse(
            value.get("wallet").ok_or(CircleError::InvalidEvidence)?,
            Some(wallet_id),
            chain_id,
        )
    }
    pub async fn verify_wallet(&self, identity: &CircleWalletIdentity) -> Result<(), CircleError> {
        if self
            .wallet(identity.wallet_id(), identity.chain_id())
            .await?
            != *identity
        {
            return Err(CircleError::InvalidEvidence);
        }
        Ok(())
    }
    /// Validates registered entity-secret access through a fresh, nonpayment
    /// EIP-712 signature. It submits no chain transaction and discards the proof.
    /// A wallet must already exist; initial creation validates the secret in its
    /// separately retained, idempotent setup request.
    pub async fn verify_credentials(
        &self,
        identity: &CircleWalletIdentity,
    ) -> Result<(), CircleError> {
        let mut nonce = [0u8; 32];
        getrandom::fill(&mut nonce).map_err(|_| CircleError::Unavailable)?;
        self.sign_typed_data(identity, &json!({
            "types": {
                "EIP712Domain": [
                    {"name":"name","type":"string"},
                    {"name":"version","type":"string"},
                    {"name":"chainId","type":"uint256"},
                    {"name":"verifyingContract","type":"address"}
                ],
                "CredentialCheck": [{"name":"nonce","type":"bytes32"}]
            },
            "domain": {"name":"MPPI Circle Credential Check", "version":"1",
                "chainId":identity.chain_id(), "verifyingContract":address_hex(identity.address())},
            "primaryType":"CredentialCheck",
            "message":{"nonce":format!("0x{}", alloy_primitives::hex::encode(nonce))}
        })).await?;
        Ok(())
    }
    pub async fn get_wallet_set(&self, wallet_set_id: &str) -> Result<String, CircleError> {
        uuid(wallet_set_id)?;
        let value = self
            .transport
            .request(Method::GET, &format!("/walletSets/{wallet_set_id}"), None)
            .await?;
        let set = value.get("walletSet").ok_or(CircleError::InvalidEvidence)?;
        if string(set, "id")? != wallet_set_id || string(set, "custodyType")? != "DEVELOPER" {
            return Err(CircleError::InvalidEvidence);
        }
        Ok(wallet_set_id.into())
    }
    /// Explicit setup mutation. Retry only this same retained key and intent.
    pub async fn create_wallet_set(
        &self,
        set_name: &str,
        key: &str,
    ) -> Result<String, CircleError> {
        self.create_wallet_set_before_submit(set_name, key, || Ok(()))
            .await
    }
    /// Prepare before the caller durably marks submission. No await or fallible
    /// preparation separates that marker from the original mutation request.
    pub(crate) async fn create_wallet_set_before_submit(
        &self,
        set_name: &str,
        key: &str,
        before_submit: impl FnOnce() -> Result<(), CircleError>,
    ) -> Result<String, CircleError> {
        name(set_name)?;
        idempotency_key(key)?;
        let body = json!({
            "name": set_name, "idempotencyKey": key, "entitySecretCiphertext": self.ciphertext().await?
        });
        before_submit()?;
        let value = self
            .transport
            .request(Method::POST, "/developer/walletSets", Some(body))
            .await?;
        let set = value.get("walletSet").ok_or(CircleError::InvalidEvidence)?;
        let id = string(set, "id")?;
        uuid(id)?;
        if string(set, "custodyType")? != "DEVELOPER" {
            return Err(CircleError::InvalidEvidence);
        }
        Ok(id.into())
    }
    /// Explicit setup mutation creating exactly one Arc SCA in the selected set.
    pub async fn create_wallet(
        &self,
        wallet_set_id: &str,
        chain_id: u64,
        wallet_name: &str,
        key: &str,
    ) -> Result<CircleWalletIdentity, CircleError> {
        self.create_wallet_before_submit(wallet_set_id, chain_id, wallet_name, key, || Ok(()))
            .await
    }
    pub(crate) async fn create_wallet_before_submit(
        &self,
        wallet_set_id: &str,
        chain_id: u64,
        wallet_name: &str,
        key: &str,
        before_submit: impl FnOnce() -> Result<(), CircleError>,
    ) -> Result<CircleWalletIdentity, CircleError> {
        uuid(wallet_set_id)?;
        name(wallet_name)?;
        idempotency_key(key)?;
        let chain = blockchain(chain_id)?;
        self.get_wallet_set(wallet_set_id).await?;
        let body = json!({
            "walletSetId": wallet_set_id, "blockchains": [chain], "accountType": "SCA", "count": 1,
            "metadata": [{"name": wallet_name, "refId": key}], "idempotencyKey": key,
            "entitySecretCiphertext": self.ciphertext().await?
        });
        before_submit()?;
        let value = self
            .transport
            .request(Method::POST, "/developer/wallets", Some(body))
            .await?;
        let rows = value
            .get("wallets")
            .and_then(Value::as_array)
            .ok_or(CircleError::InvalidEvidence)?;
        if rows.len() != 1 {
            return Err(CircleError::InvalidEvidence);
        }
        let identity = CircleWalletIdentity::parse(&rows[0], None, chain_id)?;
        if identity.wallet_set_id() != wallet_set_id || string(&rows[0], "refId")? != key {
            return Err(CircleError::InvalidEvidence);
        }
        Ok(identity)
    }
    /// Query a possibly created wallet by the original setup key stored in its
    /// metadata. Missing evidence returns None and never authorizes resubmission.
    /// Both list and detail records must retain the original reference and set.
    pub async fn find_created_wallet(
        &self,
        wallet_set_id: &str,
        chain_id: u64,
        ref_id: &str,
    ) -> Result<Option<CircleWalletIdentity>, CircleError> {
        uuid(wallet_set_id)?;
        blockchain(chain_id)?;
        idempotency_key(ref_id)?;
        let path = format!("/wallets?walletSetId={wallet_set_id}&refId={ref_id}&pageSize=2");
        let value = self.transport.request(Method::GET, &path, None).await?;
        let rows = value
            .get("wallets")
            .and_then(Value::as_array)
            .ok_or(CircleError::InvalidEvidence)?;
        if rows.is_empty() {
            return Ok(None);
        }
        if rows.len() != 1 {
            return Err(CircleError::InvalidEvidence);
        }
        let identity = CircleWalletIdentity::parse(&rows[0], None, chain_id)?;
        if identity.wallet_set_id() != wallet_set_id || string(&rows[0], "refId")? != ref_id {
            return Err(CircleError::InvalidEvidence);
        }
        let value = self
            .transport
            .request(
                Method::GET,
                &format!("/wallets/{}", identity.wallet_id()),
                None,
            )
            .await?;
        let record = value.get("wallet").ok_or(CircleError::InvalidEvidence)?;
        if CircleWalletIdentity::parse(record, Some(identity.wallet_id()), chain_id)? != identity
            || string(record, "refId")? != ref_id
        {
            return Err(CircleError::InvalidEvidence);
        }
        Ok(Some(identity))
    }
    /// Sign EIP-712 data using this verified operator wallet only. The returned
    /// SCA bytes are not an MPPI session signature. Recipient proof custody stays
    /// with the separately selected recipient wallet.
    pub async fn sign_typed_data(
        &self,
        identity: &CircleWalletIdentity,
        typed_data: &Value,
    ) -> Result<Vec<u8>, CircleError> {
        if !typed_data.is_object() {
            return Err(CircleError::Configuration);
        }
        let data = serde_json::to_string(typed_data).map_err(|_| CircleError::Configuration)?;
        if data.len() > MAX_RESPONSE {
            return Err(CircleError::Configuration);
        }
        self.verify_wallet(identity).await?;
        let value = self.transport.request(Method::POST, "/developer/sign/typedData", Some(json!({
            "walletId": identity.wallet_id(), "data": data, "entitySecretCiphertext": self.ciphertext().await?
        }))).await?;
        let signature = bytes_hex(string(&value, "signature")?, MAX_RESPONSE / 2)?;
        if signature.is_empty() {
            return Err(CircleError::InvalidEvidence);
        }
        Ok(signature)
    }
}

fn encrypt_secret(pem: &str, secret: &[u8; 32]) -> Result<String, CircleError> {
    if pem.len() > 16_384 {
        return Err(CircleError::InvalidEvidence);
    }
    let body = pem
        .trim()
        .strip_prefix("-----BEGIN PUBLIC KEY-----")
        .and_then(|v| v.strip_suffix("-----END PUBLIC KEY-----"))
        .ok_or(CircleError::InvalidEvidence)?;
    let body: String = body.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    let der = STANDARD
        .decode(body)
        .map_err(|_| CircleError::InvalidEvidence)?;
    let public = PublicEncryptingKey::from_der(&der).map_err(|_| CircleError::InvalidEvidence)?;
    if !(2048..=8192).contains(&public.key_size_bits()) {
        return Err(CircleError::InvalidEvidence);
    }
    let public = OaepPublicEncryptingKey::new(public).map_err(|_| CircleError::InvalidEvidence)?;
    let mut ciphertext = vec![0u8; public.ciphertext_size()];
    let ciphertext = public
        .encrypt(&OAEP_SHA256_MGF1SHA256, secret, &mut ciphertext, None)
        .map_err(|_| CircleError::Unavailable)?;
    Ok(STANDARD.encode(ciphertext))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalOperation {
    service_ref: String,
    to: String,
    calldata: String,
    attempted: bool,
    wallet_operation_id: Option<String>,
    tx_hash: Option<String>,
}
impl JournalOperation {
    fn validate(&self) -> Result<(), CircleError> {
        idempotency_key(&self.service_ref).map_err(|_| CircleError::Journal)?;
        if parse_address(&self.to)
            .map_err(|_| CircleError::Journal)?
            .is_zero()
        {
            return Err(CircleError::Journal);
        }
        bytes_hex(&self.calldata, MAX_CALL).map_err(|_| CircleError::Journal)?;
        if let Some(id) = &self.wallet_operation_id {
            uuid(id).map_err(|_| CircleError::Journal)?;
        }
        if let Some(tx) = &self.tx_hash {
            hash(tx).map_err(|_| CircleError::Journal)?;
        }
        if (!self.attempted && (self.wallet_operation_id.is_some() || self.tx_hash.is_some()))
            || (self.tx_hash.is_some() && self.wallet_operation_id.is_none())
        {
            return Err(CircleError::Journal);
        }
        Ok(())
    }
    fn result(
        &self,
        reference: &str,
        state: OperationState,
    ) -> Result<OperationResult, CircleError> {
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
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u32,
    wallet_id: String,
    wallet_address: String,
    wallet_set_id: String,
    chain_id: u64,
    operations: BTreeMap<String, JournalOperation>,
}
impl Journal {
    fn new(identity: &CircleWalletIdentity) -> Self {
        Self {
            version: 1,
            wallet_id: identity.wallet_id.clone(),
            wallet_address: address_hex(identity.address),
            wallet_set_id: identity.wallet_set_id.clone(),
            chain_id: identity.chain_id,
            operations: BTreeMap::new(),
        }
    }
    fn read(store: &LocalState, identity: &CircleWalletIdentity) -> Result<Self, CircleError> {
        let value = store
            .read()
            .map_err(|_| CircleError::Journal)?
            .ok_or(CircleError::Journal)?;
        let record: Self = serde_json::from_value(value).map_err(|_| CircleError::Journal)?;
        if record.version != 1
            || record.wallet_id != identity.wallet_id
            || record.wallet_address != address_hex(identity.address)
            || record.wallet_set_id != identity.wallet_set_id
            || record.chain_id != identity.chain_id
            || record.operations.len() > MAX_OPERATIONS
        {
            return Err(CircleError::Journal);
        }
        let mut service_refs = std::collections::BTreeSet::new();
        for (key, operation) in &record.operations {
            reference(key).map_err(|_| CircleError::Journal)?;
            operation.validate()?;
            if !service_refs.insert(&operation.service_ref) {
                return Err(CircleError::Journal);
            }
        }
        Ok(record)
    }
    fn write(&self, store: &mut LocalState) -> Result<(), CircleError> {
        let value = serde_json::to_value(self).map_err(|_| CircleError::Journal)?;
        if serde_json::to_vec(&value)
            .map_err(|_| CircleError::Journal)?
            .len()
            > MAX_JOURNAL
        {
            return Err(CircleError::Capacity);
        }
        store.write(&value).map_err(|_| CircleError::Journal)
    }
}

/// Native configured Circle transaction wallet, backed by an explicit private
/// wallet-operation journal. Keep this journal for the lifetime of its operations.
/// Each operation locks out concurrent processes using the same path. Records contain
/// exact calls and public handles; credentials and inference usage are absent.
pub struct CircleDeveloperWallet {
    client: Arc<CircleClient>,
    identity: CircleWalletIdentity,
    journal_path: PathBuf,
    operation_lock: Mutex<()>,
}
impl fmt::Debug for CircleDeveloperWallet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CircleDeveloperWallet")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}
impl CircleDeveloperWallet {
    pub fn new(
        client: Arc<CircleClient>,
        identity: CircleWalletIdentity,
        journal_path: impl AsRef<Path>,
    ) -> Result<Self, CircleError> {
        Self::open(client, identity, journal_path.as_ref(), true)
    }
    /// Resume an existing operation record. Missing records fail without creating
    /// replacement wallet history; retain the path selected during onboarding.
    pub fn open_existing(
        client: Arc<CircleClient>,
        identity: CircleWalletIdentity,
        journal_path: impl AsRef<Path>,
    ) -> Result<Self, CircleError> {
        let path = journal_path.as_ref();
        if !std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file()) {
            return Err(CircleError::Journal);
        }
        Self::open(client, identity, path, false)
    }
    fn open(
        client: Arc<CircleClient>,
        identity: CircleWalletIdentity,
        journal_path: &Path,
        create: bool,
    ) -> Result<Self, CircleError> {
        let journal_path = journal_path.to_owned();
        let mut journal =
            LocalState::open(&journal_path, MAX_JOURNAL).map_err(|_| CircleError::Journal)?;
        if journal.read().map_err(|_| CircleError::Journal)?.is_none() {
            if !create {
                return Err(CircleError::Journal);
            }
            Journal::new(&identity).write(&mut journal)?;
        }
        Journal::read(&journal, &identity)?;
        let journal_path = journal.path().to_owned();
        Ok(Self {
            client,
            identity,
            journal_path,
            operation_lock: Mutex::new(()),
        })
    }
    pub fn identity(&self) -> &CircleWalletIdentity {
        &self.identity
    }
    /// Confirms current Circle account access to the configured SCA.
    pub async fn verify_wallet(&self) -> Result<(), CircleError> {
        self.client.verify_wallet(&self.identity).await
    }

    fn record(
        &self,
        original: &JournalOperation,
        value: &Value,
    ) -> Result<(JournalOperation, OperationState), CircleError> {
        let id = string(value, "id")?;
        uuid(id)?;
        if !original.attempted
            || string(value, "refId")? != original.service_ref
            || string(value, "walletId")? != self.identity.wallet_id
            || string(value, "blockchain")? != blockchain(self.identity.chain_id)?
            || string(value, "custodyType")? != "DEVELOPER"
            || string(value, "operation")? != "CONTRACT_EXECUTION"
            || string(value, "transactionType")? != "OUTBOUND"
            || address(value, "sourceAddress")? != self.identity.address
            || address(value, "contractAddress")?
                != parse_address(&original.to).map_err(|_| CircleError::Journal)?
            || original
                .wallet_operation_id
                .as_deref()
                .is_some_and(|previous| previous != id)
        {
            return Err(CircleError::InvalidEvidence);
        }
        if let Some(data) = value.get("callData")
            && bytes_hex(data.as_str().ok_or(CircleError::InvalidEvidence)?, MAX_CALL)?
                != bytes_hex(&original.calldata, MAX_CALL)?
        {
            return Err(CircleError::InvalidEvidence);
        }
        let mut next = original.clone();
        next.wallet_operation_id = Some(id.into());
        if let Some(tx) = value
            .get("txHash")
            .filter(|v| !v.is_null() && v.as_str() != Some(""))
        {
            let tx = hash(tx.as_str().ok_or(CircleError::InvalidEvidence)?)?;
            if original
                .tx_hash
                .as_deref()
                .map(hash)
                .transpose()?
                .is_some_and(|old| old != tx)
            {
                return Err(CircleError::InvalidEvidence);
            }
            next.tx_hash = Some(format!("{tx:#x}"));
        }
        let state = match string(value, "state")? {
            "INITIATED" | "CLEARED" | "QUEUED" | "SENT" | "STUCK" => OperationState::Pending,
            "CONFIRMED" | "COMPLETE" if next.tx_hash.is_some() => OperationState::Pending,
            // Once a POST may have started, a service failure label does not
            // prove that the exact call was never submitted. Preserve its
            // identity/hash for canonical reconciliation; only the durable
            // pre-submission marker below can attest hashless Failed.
            "FAILED" | "DENIED" | "CANCELLED" => OperationState::Unknown,
            _ => OperationState::Unknown,
        };
        Ok((next, state))
    }
    async fn lookup(
        &self,
        reference: &str,
        journal: &mut Journal,
        store: &mut LocalState,
    ) -> Result<(JournalOperation, OperationState), CircleError> {
        let operation = journal
            .operations
            .get(reference)
            .ok_or(CircleError::Journal)?
            .clone();
        if !operation.attempted {
            return Ok((operation, OperationState::Failed));
        }
        let path = format!(
            "/transactions?refId={}&walletIds={}&operation=CONTRACT_EXECUTION&custodyType=DEVELOPER&pageSize=2",
            operation.service_ref, self.identity.wallet_id
        );
        let value = self
            .client
            .transport
            .request(Method::GET, &path, None)
            .await?;
        let rows = value
            .get("transactions")
            .and_then(Value::as_array)
            .ok_or(CircleError::InvalidEvidence)?;
        if rows.is_empty() {
            return Ok((operation.clone(), OperationState::Unknown));
        }
        if rows.len() != 1 {
            return Err(CircleError::InvalidEvidence);
        }
        let (summary, _) = self.record(&operation, &rows[0])?;
        journal.operations.insert(reference.into(), summary.clone());
        journal.write(store)?; // Save validated handles before another await.
        let id = summary
            .wallet_operation_id
            .as_deref()
            .ok_or(CircleError::InvalidEvidence)?;
        let value = self
            .client
            .transport
            .request(Method::GET, &format!("/transactions/{id}"), None)
            .await?;
        self.record(
            &summary,
            value
                .get("transaction")
                .ok_or(CircleError::InvalidEvidence)?,
        )
    }
    async fn reconcile_locked(
        &self,
        reference: &str,
        journal: &mut Journal,
        store: &mut LocalState,
    ) -> Result<OperationResult, CircleError> {
        match self.lookup(reference, journal, store).await {
            Ok((next, state)) => {
                let result = next.result(reference, state)?;
                journal.operations.insert(reference.into(), next);
                journal.write(store)?;
                Ok(result)
            }
            Err(CircleError::Journal | CircleError::Capacity) => Err(CircleError::Journal),
            Err(_) => journal
                .operations
                .get(reference)
                .ok_or(CircleError::Journal)?
                .result(reference, OperationState::Unknown),
        }
    }
    async fn execute_inner(
        &self,
        call: &WalletCall,
        operation_ref: &str,
    ) -> Result<OperationResult, CircleError> {
        reference(operation_ref)?;
        if call.chain_id() != self.identity.chain_id || call.data().len() > MAX_CALL {
            return Err(CircleError::Configuration);
        }
        let _guard = self.operation_lock.lock().await;
        let mut store =
            LocalState::open(&self.journal_path, MAX_JOURNAL).map_err(|_| CircleError::Journal)?;
        let mut journal = Journal::read(&store, &self.identity)?;
        let to = address_hex(call.to());
        let calldata = format!("0x{}", alloy_primitives::hex::encode(call.data()));
        if let Some(original) = journal.operations.get(operation_ref) {
            if original.to != to || original.calldata != calldata {
                return Err(CircleError::InvalidEvidence);
            }
            return self
                .reconcile_locked(operation_ref, &mut journal, &mut store)
                .await;
        }
        if journal.operations.len() >= MAX_OPERATIONS {
            return Err(CircleError::Capacity);
        }
        let mut operation = JournalOperation {
            service_ref: fresh_key()?,
            to,
            calldata,
            attempted: false,
            wallet_operation_id: None,
            tx_hash: None,
        };
        journal
            .operations
            .insert(operation_ref.into(), operation.clone());
        journal.write(&mut store)?;
        let ciphertext = match async {
            self.verify_wallet().await?;
            self.client.ciphertext().await
        }
        .await
        {
            Ok(value) => value,
            Err(_) => return operation.result(operation_ref, OperationState::Failed),
        };
        operation.attempted = true;
        journal
            .operations
            .insert(operation_ref.into(), operation.clone());
        journal.write(&mut store)?; // Required before submission can start, including cancellation.
        let response = self.client.transport.request(Method::POST, "/developer/transactions/contractExecution", Some(json!({
            "idempotencyKey": operation.service_ref, "refId": operation.service_ref,
            "walletId": self.identity.wallet_id, "contractAddress": operation.to,
            "callData": operation.calldata, "amount": "0", "feeLevel": "MEDIUM", "entitySecretCiphertext": ciphertext
        }))).await;
        if let Ok(response) = response
            && let Ok(id) = string(&response, "id")
            && uuid(id).is_ok()
        {
            operation.wallet_operation_id = Some(id.into());
            journal
                .operations
                .insert(operation_ref.into(), operation.clone());
            journal.write(&mut store)?;
        }
        // The original submission remains the only write. A lost response is
        // recovered through refId lookup, with handles saved before further I/O.
        self.reconcile_locked(operation_ref, &mut journal, &mut store)
            .await
    }
}
#[tonic::async_trait]
impl TransactionWallet for CircleDeveloperWallet {
    fn address(&self) -> Address {
        self.identity.address
    }
    async fn execute(
        &self,
        call: &WalletCall,
        operation_ref: &str,
    ) -> Result<OperationResult, WalletError> {
        self.execute_inner(call, operation_ref)
            .await
            .map_err(|_| WalletError)
    }
    async fn reconcile(&self, operation_ref: &str) -> Result<OperationResult, WalletError> {
        reference(operation_ref).map_err(|_| WalletError)?;
        let _guard = self.operation_lock.lock().await;
        let mut store =
            LocalState::open(&self.journal_path, MAX_JOURNAL).map_err(|_| WalletError)?;
        let mut journal = Journal::read(&store, &self.identity).map_err(|_| WalletError)?;
        self.reconcile_locked(operation_ref, &mut journal, &mut store)
            .await
            .map_err(|_| WalletError)
    }
}

#[cfg(test)]
#[path = "circle_tests.rs"]
mod tests;
