//! Bounded asynchronous JSON HTTP reads with verified TLS and sanitized errors.
//! Endpoints and trust roots are chosen by the caller. No redirects, environment
//! proxies, automatic retries, compression, cookies or transaction submission.

use crate::{
    chain::{ChainError, Rpc},
    discovery::MAX_DOCUMENT_BYTES,
};
use reqwest::{Client, Url, header};
use serde_json::{Value, json};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

/// ABI framing plus hexadecimal JSON-RPC encoding exceeds twice the exact
/// catalog size. Keep the RPC envelope bound separate from discovery documents.
pub const MAX_RPC_RESPONSE_BYTES: usize = 4 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransportError;
impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JSON HTTP transport unavailable or invalid")
    }
}
impl std::error::Error for TransportError {}

fn endpoint(raw: &str) -> Result<Url, TransportError> {
    if raw.is_empty()
        || raw
            .bytes()
            .any(|b| !(33..127).contains(&b) || matches!(b, b'\\' | b'#'))
    {
        return Err(TransportError);
    }
    let (scheme, tail) = raw.split_once("://").ok_or(TransportError)?;
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return Err(TransportError);
    }
    let authority = tail.split(['/', '?']).next().ok_or(TransportError)?;
    if authority.is_empty() || authority.contains('@') {
        return Err(TransportError);
    }
    let url = Url::parse(raw).map_err(|_| TransportError)?;
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port() == Some(0)
    {
        return Err(TransportError);
    }
    // WHATWG URL parsing normalizes dot segments. Reject changed targets rather
    // than silently send an authenticated RPC path to a different resource.
    let target = &tail[authority.len()..];
    let target = if target.is_empty() {
        "/".to_owned()
    } else if target.starts_with('?') {
        format!("/{target}")
    } else {
        target.to_owned()
    };
    let parsed = format!(
        "{}{}",
        url.path(),
        url.query().map_or(String::new(), |q| format!("?{q}"))
    );
    if target != parsed {
        return Err(TransportError);
    }
    Ok(url)
}

#[derive(Clone)]
pub struct HttpTransport {
    client: Client,
    max_bytes: usize,
}
impl fmt::Debug for HttpTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpTransport")
            .field("max_bytes", &self.max_bytes)
            .finish_non_exhaustive()
    }
}
impl HttpTransport {
    /// A custom PEM bundle replaces system roots. The caller owns loading it.
    pub fn new(
        timeout_ms: u64,
        max_bytes: usize,
        ca_pem: Option<&[u8]>,
    ) -> Result<Self, TransportError> {
        if !(1..=86_400_000).contains(&timeout_ms)
            || !(1..=MAX_RPC_RESPONSE_BYTES).contains(&max_bytes)
        {
            return Err(TransportError);
        }
        let mut builder = Client::builder()
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
            .tls_sslkeylogfile(false);
        if let Some(pem) = ca_pem {
            if pem.len() > MAX_DOCUMENT_BYTES {
                return Err(TransportError);
            }
            let certs = reqwest::Certificate::from_pem_bundle(pem).map_err(|_| TransportError)?;
            if certs.is_empty() {
                return Err(TransportError);
            }
            builder = builder.tls_certs_only(certs);
        }
        Ok(Self {
            client: builder.build().map_err(|_| TransportError)?,
            max_bytes,
        })
    }
    /// GET without a body; POST with JSON bytes. Response media type must be JSON.
    pub async fn request(&self, url: &str, body: Option<&[u8]>) -> Result<Vec<u8>, TransportError> {
        let url = endpoint(url)?;
        let request = match body {
            Some(body) if body.len() <= MAX_DOCUMENT_BYTES => self
                .client
                .post(url)
                .header(header::CONTENT_TYPE, "application/json")
                .body(body.to_vec()),
            Some(_) => return Err(TransportError),
            None => self.client.get(url),
        };
        let mut response = request
            .header(header::ACCEPT, "application/json")
            .header(header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(|_| TransportError)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(TransportError);
        }
        let headers = response.headers();
        // Reject ambiguous duplicate headers as well as unexpected encodings.
        if headers.get_all(header::CONTENT_TYPE).iter().count() != 1
            || headers.get_all(header::CONTENT_ENCODING).iter().count() > 1
        {
            return Err(TransportError);
        }
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .ok_or(TransportError)?;
        if !content_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("application/json")
        {
            return Err(TransportError);
        }
        if let Some(encoding) = headers.get(header::CONTENT_ENCODING)
            && !encoding
                .to_str()
                .map_err(|_| TransportError)?
                .eq_ignore_ascii_case("identity")
        {
            return Err(TransportError);
        }
        if response
            .content_length()
            .is_some_and(|n| n > self.max_bytes as u64)
        {
            return Err(TransportError);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| TransportError)? {
            if chunk.len() > self.max_bytes - bytes.len() {
                return Err(TransportError);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

#[derive(Clone)]
pub struct HttpJsonRpc {
    endpoint: String,
    transport: HttpTransport,
    next_id: Arc<AtomicU64>,
}
impl fmt::Debug for HttpJsonRpc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpJsonRpc").finish_non_exhaustive()
    }
}
impl HttpJsonRpc {
    pub fn new(url: impl Into<String>, transport: HttpTransport) -> Result<Self, TransportError> {
        let url = url.into();
        endpoint(&url)?;
        Ok(Self {
            endpoint: url,
            transport,
            next_id: Arc::new(AtomicU64::new(1)),
        })
    }
}
#[tonic::async_trait]
impl Rpc for HttpJsonRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        if !matches!(
            method,
            "eth_chainId"
                | "eth_getBlockByNumber"
                | "eth_getTransactionReceipt"
                | "eth_getCode"
                | "eth_getStorageAt"
                | "eth_call"
                | "eth_estimateGas"
        ) || !params.is_array()
        {
            return Err(ChainError::Unavailable);
        }
        let id = self
            .next_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| ChainError::Unavailable)?
            .to_string();
        let request =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
                .map_err(|_| ChainError::Unavailable)?;
        let raw = self
            .transport
            .request(&self.endpoint, Some(&request))
            .await
            .map_err(|_| ChainError::Unavailable)?;
        let response =
            crate::json::parse_document(&raw, MAX_RPC_RESPONSE_BYTES, crate::DEFAULT_JSON_DEPTH)
                .map_err(|_| ChainError::Unavailable)?;
        if !response.is_object() {
            return Err(ChainError::Unavailable);
        }
        if response.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || response.get("id").and_then(Value::as_str) != Some(&id)
            || response.get("error").is_some()
        {
            return Err(ChainError::Unavailable);
        }
        response
            .get("result")
            .cloned()
            .ok_or(ChainError::Unavailable)
    }
}
