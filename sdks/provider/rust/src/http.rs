//! Bounded asynchronous JSON HTTP exchanges with verified TLS and sanitized
//! errors. Endpoints and trust roots are chosen by the caller. No redirects,
//! environment proxies, compression or cookies.
//!
//! [`HttpTransport`] sends each request once. Connections persist: reqwest
//! keeps idle connections per host for 90 s (`ClientBuilder::pool_idle_timeout`),
//! and a request that finds a reused idle connection unusable before anything
//! was written goes once more on a new connection (hyper-util
//! `retry_canceled_requests`, on by default). A request body holds at most
//! 1 MiB ([`MAX_DOCUMENT_BYTES`]); a longer one is refused before anything is
//! sent. [`HttpJsonRpc`] sends only reads, so it repeats a read that failed for
//! a transient reason, and of a batch only the calls not yet answered. It
//! never submits a transaction: a wallet that sends its own posts them through
//! [`HttpTransport::request`], with the same TLS and size policy, and treats
//! any failure other than [`TransportError::Invalid`] as a transaction that may
//! have reached the node.

use crate::{
    chain::{ChainError, Rpc},
    discovery::MAX_DOCUMENT_BYTES,
};
use alloy_primitives::hex;
use reqwest::{Client, Url, header};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{Instant, sleep};

/// ABI framing plus hexadecimal JSON-RPC encoding exceeds twice the exact
/// catalog size. Keep the RPC envelope bound separate from discovery documents.
pub const MAX_RPC_RESPONSE_BYTES: usize = 4 << 20;

/// Statuses a later identical request may not repeat: timeout, too early,
/// rate limit and server or gateway unavailability (RFC 9110 sections 15.5.9
/// and 15.6, RFC 8470 section 5.2, RFC 6585 section 4).
const TRANSIENT_STATUSES: [u16; 7] = [408, 425, 429, 500, 502, 503, 504];

/// Why a JSON HTTP exchange produced no response body. No variant carries an
/// endpoint, header or text the remote party wrote. Later releases may add
/// causes, so a `match` outside this crate needs a wildcard arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportError {
    /// Refused before anything was sent: the endpoint, body size or policy.
    Invalid,
    /// The connection or TLS failed, or closed before a complete response.
    Connection,
    /// The exchange did not finish within the transport's timeout.
    Timeout,
    /// The endpoint answered with this status instead of 200.
    Status(u16),
    /// The response was refused: its media type, encoding, duplicate headers
    /// or size.
    Response,
}
impl TransportError {
    /// A later identical request may succeed: a failed or dropped connection,
    /// a timeout, or one of the transient statuses.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Connection | Self::Timeout => true,
            Self::Status(status) => TRANSIENT_STATUSES.contains(status),
            Self::Invalid | Self::Response => false,
        }
    }
}
impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid => f.write_str("JSON HTTP request or endpoint is invalid"),
            Self::Connection => f.write_str("JSON HTTP connection failed"),
            Self::Timeout => f.write_str("JSON HTTP request timed out"),
            Self::Status(status) => write!(f, "JSON HTTP endpoint returned status {status}"),
            Self::Response => f.write_str("JSON HTTP response was refused"),
        }
    }
}
impl std::error::Error for TransportError {}
fn failed(error: reqwest::Error) -> TransportError {
    if error.is_timeout() {
        TransportError::Timeout
    } else if error.is_builder() {
        TransportError::Invalid
    } else {
        TransportError::Connection
    }
}

// Retry-After is delay-seconds or an HTTP-date (RFC 9110 section 10.2.3), and a
// recipient must accept each of the three HTTP-date formats (section 5.6.7).
// HTTP-date is case sensitive.
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
const MONTH_DAYS: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const DAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const LONG_DAYS: [&str; 7] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
];

/// Days from 1970-01-01 to a proleptic Gregorian date (`days_from_civil`,
/// https://howardhinnant.github.io/date_algorithms.html).
fn days(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let of_era = year - era * 400;
    let of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    era * 146_097 + of_era * 365 + of_era / 4 - of_era / 100 + of_year - 719_468
}
/// The year of a day counted from 1970-01-01 (`civil_from_days`, same source).
fn year_of(days: i64) -> i64 {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let of_era = days - era * 146_097;
    let years = (of_era - of_era / 1460 + of_era / 36_524 - of_era / 146_096) / 365;
    let of_year = of_era - (365 * years + years / 4 - years / 100);
    // Counted from 1 March: January and February belong to the next year.
    era * 400 + years + i64::from((5 * of_year + 2) / 153 >= 10)
}
/// An HTTP-date as Unix time, or `None`; `now` is Unix time.
fn http_date(value: &str, now: i64) -> Option<i64> {
    let field = |text: &str, width: usize| -> Option<i64> {
        (text.len() == width && text.bytes().all(|b| b.is_ascii_digit()))
            .then(|| text.parse().ok())?
    };
    let named =
        |word: &str, names: &[&str]| word.strip_suffix(',').is_some_and(|n| names.contains(&n));
    let words: Vec<&str> = value.split(' ').collect();
    let (day, month, year, two_digit, clock) = match words[..] {
        // IMF-fixdate: Sun, 06 Nov 1994 08:49:37 GMT
        [name, day, month, year, clock, "GMT"] if named(name, &DAYS) => {
            (field(day, 2)?, month, field(year, 4)?, false, clock)
        }
        // rfc850-date: Sunday, 06-Nov-94 08:49:37 GMT
        [name, date, clock, "GMT"] if named(name, &LONG_DAYS) => {
            let [day, month, year] = date.split('-').collect::<Vec<_>>()[..] else {
                return None;
            };
            (field(day, 2)?, month, field(year, 2)?, true, clock)
        }
        // asctime-date: Sun Nov  6 08:49:37 1994, or Sun Nov 16 08:49:37 1994
        [name, month, "", day, clock, year] if DAYS.contains(&name) => {
            (field(day, 1)?, month, field(year, 4)?, false, clock)
        }
        [name, month, day, clock, year] if DAYS.contains(&name) => {
            (field(day, 2)?, month, field(year, 4)?, false, clock)
        }
        _ => return None,
    };
    let month = MONTHS.iter().position(|name| *name == month)? as i64 + 1;
    let [hour, minute, second] = clock.split(':').collect::<Vec<_>>()[..] else {
        return None;
    };
    let (hour, minute, second) = (field(hour, 2)?, field(minute, 2)?, field(second, 2)?);
    let seconds = hour * 3600 + minute * 60 + second;
    let mut year = year;
    if two_digit {
        // The latest year with these two digits whose date is not more than
        // 50 years after now.
        let current = year_of(now.div_euclid(86_400));
        year = current + 50 - (current + 50 - year).rem_euclid(100);
        if days(year - 50, month, day) * 86_400 + seconds > now {
            year -= 100;
        }
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    if !(1..=MONTH_DAYS[month as usize - 1] + i64::from(leap && month == 2)).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    Some(days(year, month, day) * 86_400 + seconds)
}
/// The wait a `Retry-After` field value asks for from `now` (since the Unix
/// epoch): its delay-seconds, or the time until its HTTP-date, none once that
/// has passed. `None` for any other value.
fn retry_after(value: &str, now: Duration) -> Option<Duration> {
    let value = value.trim_matches([' ', '\t']);
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        return Some(value.parse().map_or(Duration::MAX, Duration::from_secs));
    }
    let at = http_date(value, i64::try_from(now.as_secs()).ok()?)?;
    Some(Duration::from_secs(u64::try_from(at).unwrap_or(0)).saturating_sub(now))
}

fn endpoint(raw: &str) -> Result<Url, TransportError> {
    if raw.is_empty()
        || raw
            .bytes()
            .any(|b| !(33..127).contains(&b) || matches!(b, b'\\' | b'#'))
    {
        return Err(TransportError::Invalid);
    }
    let (scheme, tail) = raw.split_once("://").ok_or(TransportError::Invalid)?;
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return Err(TransportError::Invalid);
    }
    let authority = tail
        .split(['/', '?'])
        .next()
        .ok_or(TransportError::Invalid)?;
    if authority.is_empty() || authority.contains('@') {
        return Err(TransportError::Invalid);
    }
    let url = Url::parse(raw).map_err(|_| TransportError::Invalid)?;
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port() == Some(0)
    {
        return Err(TransportError::Invalid);
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
        return Err(TransportError::Invalid);
    }
    Ok(url)
}

#[derive(Clone)]
pub struct HttpTransport {
    client: Client,
    timeout: Duration,
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
            return Err(TransportError::Invalid);
        }
        let timeout = Duration::from_millis(timeout_ms);
        let mut builder = Client::builder()
            .timeout(timeout)
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
                return Err(TransportError::Invalid);
            }
            let certs =
                reqwest::Certificate::from_pem_bundle(pem).map_err(|_| TransportError::Invalid)?;
            if certs.is_empty() {
                return Err(TransportError::Invalid);
            }
            builder = builder.tls_certs_only(certs);
        }
        Ok(Self {
            client: builder.build().map_err(|_| TransportError::Invalid)?,
            timeout,
            max_bytes,
        })
    }
    /// GET without a body; POST with JSON bytes. Response media type must be JSON.
    pub async fn request(&self, url: &str, body: Option<&[u8]>) -> Result<Vec<u8>, TransportError> {
        self.exchange(url, body, &mut None).await
    }
    /// [`Self::request`], keeping in `asked` the wait a refusing response's
    /// one `Retry-After` field asked for.
    pub(crate) async fn exchange(
        &self,
        url: &str,
        body: Option<&[u8]>,
        asked: &mut Option<Duration>,
    ) -> Result<Vec<u8>, TransportError> {
        let url = endpoint(url)?;
        let request = match body {
            Some(body) if body.len() <= MAX_DOCUMENT_BYTES => self
                .client
                .post(url)
                .header(header::CONTENT_TYPE, "application/json")
                .body(body.to_vec()),
            Some(_) => return Err(TransportError::Invalid),
            None => self.client.get(url),
        };
        let mut response = request
            .header(header::ACCEPT, "application/json")
            .header(header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(failed)?;
        if response.status() != reqwest::StatusCode::OK {
            let mut fields = response.headers().get_all(header::RETRY_AFTER).iter();
            if let (Some(field), None) = (fields.next(), fields.next()) {
                *asked = field.to_str().ok().and_then(|value| {
                    retry_after(value, SystemTime::now().duration_since(UNIX_EPOCH).ok()?)
                });
            }
            return Err(TransportError::Status(response.status().as_u16()));
        }
        let headers = response.headers();
        // Reject ambiguous duplicate headers as well as unexpected encodings.
        if headers.get_all(header::CONTENT_TYPE).iter().count() != 1
            || headers.get_all(header::CONTENT_ENCODING).iter().count() > 1
        {
            return Err(TransportError::Response);
        }
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .ok_or(TransportError::Response)?;
        if !content_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("application/json")
        {
            return Err(TransportError::Response);
        }
        if let Some(encoding) = headers.get(header::CONTENT_ENCODING)
            && !encoding
                .to_str()
                .map_err(|_| TransportError::Response)?
                .eq_ignore_ascii_case("identity")
        {
            return Err(TransportError::Response);
        }
        if response
            .content_length()
            .is_some_and(|n| n > self.max_bytes as u64)
        {
            return Err(TransportError::Response);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(failed)? {
            if chunk.len() > self.max_bytes - bytes.len() {
                return Err(TransportError::Response);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

// A transient read failure is repeated up to twice: rate limits, dropped
// connections and slow backends are routine on shared RPC plans. Each repeat
// waits a uniformly random share of its ceiling, 250 ms and then 1 s ("full
// jitter", the AWS SDKs' retry backoff:
// https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html),
// so clients refused together do not repeat together; a refusal's Retry-After
// (RFC 9110 section 10.2.3) is waited first. A repeat starts only within one
// transport timeout plus the two ceilings of the first attempt: a wait that
// would end later is not begun, and the read fails at once.
const READ_RETRY_DELAYS: [Duration; 2] = [Duration::from_millis(250), Duration::from_secs(1)];
// EIP-1474 -32005 "Limit exceeded" (https://eips.ethereum.org/EIPS/eip-1474);
// QuickNode's -32007 per-second request limit on EVM endpoints
// (https://www.quicknode.com/docs/berachain/error-references); Arc's -32014 from
// a load-balanced backend that "hasn't yet imported that block", which Arc says
// can be retried after a brief backoff (https://docs.arc.io/arc/references/rpc-endpoints).
// A -32014 that outlasts the repeats is lag (`ChainError::Lagging`).
const NOT_IMPORTED: i64 = -32014;
const RATE_LIMIT_CODES: [i64; 3] = [-32005, -32007, NOT_IMPORTED];
// At most this many calls go in one JSON-RPC batch: some plans count each call
// in a batch against a per-second request limit and refuse entries beyond it.
const BATCH_CALLS: usize = 32;
// The widest id a request carries: `next_id` is a `u64`.
const WIDEST_ID: &str = "18446744073709551615";
const READ_ONLY: [&str; 7] = [
    "eth_chainId",
    "eth_getBlockByNumber",
    "eth_getTransactionReceipt",
    "eth_getCode",
    "eth_getStorageAt",
    "eth_call",
    "eth_estimateGas",
];

fn invalid(message: &'static str) -> ChainError {
    ChainError::InvalidEvidence(message)
}
/// A uniform share in [0, 1) from the system's random source: the top 53 bits
/// of a `u64` are exact in an `f64`. Without randomness each ceiling is waited
/// whole.
fn jitter() -> f64 {
    getrandom::u64().map_or(1.0, |bits| (bits >> 11) as f64 / (1u64 << 53) as f64)
}
/// The wait before a repeat: what the refusal's Retry-After asked for, then
/// `share` of `ceiling`; `None` when the repeat could not start within `left`.
fn pause(
    ceiling: Duration,
    share: f64,
    asked: Option<Duration>,
    left: Duration,
) -> Option<Duration> {
    let delay = asked
        .unwrap_or_default()
        .saturating_add(ceiling.mul_f64(share));
    (delay < left).then_some(delay)
}
fn transient(error: &ChainError) -> bool {
    match error {
        ChainError::Transport(error) => error.is_transient(),
        ChainError::Rpc(code) => RATE_LIMIT_CODES.contains(code),
        _ => false,
    }
}
/// The answer to one request: its result, or its error as a code. Execution
/// clients answer a reverted `eth_call` or `eth_estimateGas` with code 3 and
/// the revert data (go-ethereum `internal/ethapi`: `revertError.ErrorCode`,
/// used by `BlockChainAPI.Call` and `DoEstimateGas`). Only the data's 4-byte
/// selector is kept; the message is remote text and is never kept.
fn answer(response: &Value, id: &str, method: &str) -> Result<Value, ChainError> {
    let object = response
        .as_object()
        .filter(|object| {
            object.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
                && object.get("id").and_then(Value::as_str) == Some(id)
        })
        .ok_or_else(|| invalid("RPC response identity mismatch"))?;
    match (object.get("result"), object.get("error")) {
        (Some(result), None) => Ok(result.clone()),
        (None, Some(error)) => match error.get("code").and_then(Value::as_i64) {
            Some(3) if matches!(method, "eth_call" | "eth_estimateGas") => {
                Err(ChainError::Reverted(selector(error.get("data"))))
            }
            Some(code) => Err(ChainError::Rpc(code)),
            None => Err(invalid("RPC request returned no usable result")),
        },
        _ => Err(invalid("RPC request returned no usable result")),
    }
}
fn selector(data: Option<&Value>) -> Option<[u8; 4]> {
    let raw = data?.as_str()?.strip_prefix("0x")?.get(..8)?;
    let mut selector = [0; 4];
    hex::decode_to_slice(raw, &mut selector).ok()?;
    Some(selector)
}
fn read_only(method: &str, params: &Value) -> Result<(), ChainError> {
    if !READ_ONLY.contains(&method) || !params.is_array() {
        return Err(invalid("unsupported read-only RPC method or parameters"));
    }
    Ok(())
}
/// One JSON-RPC 2.0 request object.
fn request(id: &str, method: &str, params: &Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
}
/// `calls` as consecutive JSON-RPC batches, each of at most [`BATCH_CALLS`]
/// calls and a body of at most [`MAX_DOCUMENT_BYTES`], the bound
/// [`HttpTransport`] enforces, measured with the widest id. A call that cannot
/// fit one body alone is refused as [`TransportError::Invalid`] before
/// anything is sent.
fn batches<'a>(calls: &'a [(&'a str, Value)]) -> Result<Vec<&'a [(&'a str, Value)]>, ChainError> {
    let mut batches = Vec::new();
    let (mut start, mut size) = (0, 0);
    for (index, (method, params)) in calls.iter().enumerate() {
        let entry = serde_json::to_vec(&request(WIDEST_ID, method, params))
            .map_err(|_| ChainError::Unavailable)?
            .len();
        // A batch is `[`, its entries separated by commas, then `]`.
        if entry + 2 > MAX_DOCUMENT_BYTES {
            return Err(ChainError::Transport(TransportError::Invalid));
        }
        if index > start && (index - start == BATCH_CALLS || size + 1 + entry > MAX_DOCUMENT_BYTES)
        {
            batches.push(&calls[start..index]);
            start = index;
        }
        size = if index == start {
            entry + 2
        } else {
            size + 1 + entry
        };
    }
    if start < calls.len() {
        batches.push(&calls[start..]);
    }
    Ok(batches)
}

/// A failed exchange, with the wait a refusal's Retry-After asked for.
struct Failed {
    error: ChainError,
    asked: Option<Duration>,
}
impl From<ChainError> for Failed {
    fn from(error: ChainError) -> Self {
        Self { error, asked: None }
    }
}

#[derive(Clone)]
pub struct HttpJsonRpc {
    endpoint: String,
    transport: HttpTransport,
    next_id: Arc<AtomicU64>,
    // Cleared once the endpoint answers a batch with one object, as a server
    // without batches does (JSON-RPC 2.0 section 6), or refuses a batch request
    // (an HTTP 400, 405, 413, 415 or 501, say) and then answers a single call:
    // then one call at a time.
    batches: Arc<AtomicBool>,
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
            batches: Arc::new(AtomicBool::new(true)),
        })
    }
    fn id(&self) -> Result<String, ChainError> {
        Ok(self
            .next_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| ChainError::Unavailable)?
            .to_string())
    }
    async fn send(&self, request: &Value) -> Result<Value, Failed> {
        let body = serde_json::to_vec(request).map_err(|_| ChainError::Unavailable)?;
        let mut asked = None;
        let raw = self
            .transport
            .exchange(&self.endpoint, Some(&body), &mut asked)
            .await
            .map_err(|error| Failed {
                error: ChainError::Transport(error),
                asked,
            })?;
        Ok(
            crate::json::parse_document(&raw, MAX_RPC_RESPONSE_BYTES, crate::DEFAULT_JSON_DEPTH)
                .map_err(|_| invalid("RPC response is not a bounded JSON document"))?,
        )
    }
    /// Results of `calls`, in order. Answers are kept across repeats, so a
    /// repeat sends only the calls a refusal left unanswered.
    async fn read(&self, calls: &[(&str, Value)]) -> Result<Vec<Value>, ChainError> {
        let mut results = vec![None; calls.len()];
        let budget = Instant::now() + self.transport.timeout + READ_RETRY_DELAYS.iter().sum();
        let mut ceilings = READ_RETRY_DELAYS.into_iter();
        loop {
            let Err(Failed { error, asked }) = self.fill(calls, &mut results).await else {
                return results
                    .into_iter()
                    .collect::<Option<_>>()
                    .ok_or(ChainError::Unavailable);
            };
            let left = budget.saturating_duration_since(Instant::now());
            match ceilings
                .next()
                .filter(|_| transient(&error))
                .and_then(|ceiling| pause(ceiling, jitter(), asked, left))
            {
                Some(delay) => sleep(delay).await,
                // A backend that still has not imported the block is behind:
                // lag, which keeps accepted evidence, not a failure.
                None if error == ChainError::Rpc(NOT_IMPORTED) => {
                    return Err(ChainError::Lagging);
                }
                None => return Err(error),
            }
        }
    }
    async fn once(&self, method: &str, params: &Value) -> Result<Value, Failed> {
        let id = self.id()?;
        let response = self.send(&request(&id, method, params)).await?;
        Ok(answer(&response, &id, method)?)
    }
    /// Fills each unanswered entry of `results` with its call's answer: in one
    /// batch, or one call at a time. A batch's responses may arrive in any
    /// order and are matched by id (JSON-RPC 2.0 section 6); each call is
    /// answered once. Answers are kept as they arrive; a refusal is returned
    /// after them, a definite failure at once.
    async fn fill(
        &self,
        calls: &[(&str, Value)],
        results: &mut [Option<Value>],
    ) -> Result<(), Failed> {
        let pending: Vec<usize> = (0..calls.len()).filter(|&i| results[i].is_none()).collect();
        if pending.len() == 1 || !self.batches.load(Ordering::Relaxed) {
            for i in pending {
                results[i] = Some(self.once(calls[i].0, &calls[i].1).await?);
            }
            return Ok(());
        }
        let ids = pending
            .iter()
            .map(|_| self.id())
            .collect::<Result<Vec<_>, _>>()?;
        let batch = ids
            .iter()
            .zip(&pending)
            .map(|(id, &i)| request(id, calls[i].0, &calls[i].1))
            .collect::<Vec<_>>();
        let response = match self.send(&Value::Array(batch)).await {
            Err(Failed {
                error: ChainError::Transport(error),
                ..
            }) if !error.is_transient() => {
                // A refused single call returns its own error and keeps batches on.
                let first = pending[0];
                results[first] = Some(self.once(calls[first].0, &calls[first].1).await?);
                self.batches.store(false, Ordering::Relaxed);
                for &i in &pending[1..] {
                    results[i] = Some(self.once(calls[i].0, &calls[i].1).await?);
                }
                return Ok(());
            }
            response => response?,
        };
        let rows = match response {
            Value::Array(rows) => rows,
            Value::Object(object) => {
                if let Some(code) = object
                    .get("error")
                    .and_then(|error| error.get("code"))
                    .and_then(Value::as_i64)
                    .filter(|code| RATE_LIMIT_CODES.contains(code))
                {
                    return Err(ChainError::Rpc(code).into());
                }
                self.batches.store(false, Ordering::Relaxed);
                for i in pending {
                    results[i] = Some(self.once(calls[i].0, &calls[i].1).await?);
                }
                return Ok(());
            }
            _ => return Err(invalid("RPC response identity mismatch").into()),
        };
        if rows.len() != ids.len() {
            return Err(invalid("RPC batch response count mismatch").into());
        }
        let mut answers = HashMap::with_capacity(rows.len());
        for row in &rows {
            let id = row
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("RPC response identity mismatch"))?;
            if answers.insert(id, row).is_some() {
                return Err(invalid("RPC response identity mismatch").into());
            }
        }
        let mut refused = None;
        for (id, &i) in ids.iter().zip(&pending) {
            let row = answers
                .get(id.as_str())
                .ok_or_else(|| invalid("RPC response identity mismatch"))?;
            match answer(row, id, calls[i].0) {
                // Left unanswered: only such entries are sent again.
                Err(ChainError::Rpc(code)) if RATE_LIMIT_CODES.contains(&code) => {
                    refused.get_or_insert(code);
                }
                answered => results[i] = Some(answered?),
            }
        }
        refused.map_or(Ok(()), |code| Err(ChainError::Rpc(code).into()))
    }
}
#[tonic::async_trait]
impl Rpc for HttpJsonRpc {
    /// This client only reads. It cannot turn reconciliation into a submit, so
    /// repeating a read after a transient failure never submits anything.
    async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
        read_only(method, &params)?;
        let mut results = self.read(&[(method, params)]).await?;
        results.pop().ok_or(ChainError::Unavailable)
    }
    /// Results of read-only calls, in order, sent as JSON-RPC batches of at
    /// most 32 calls and 1 MiB, one exchange after another; a repeat after a
    /// transient failure sends only the calls of that batch not yet answered.
    async fn call_many(&self, calls: &[(&str, Value)]) -> Result<Vec<Value>, ChainError> {
        for (method, params) in calls {
            read_only(method, params)?;
        }
        let mut results = Vec::with_capacity(calls.len());
        for batch in batches(calls)? {
            results.extend(self.read(batch).await?);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unix(year: i64, month: i64, day: i64, seconds: i64) -> Duration {
        Duration::from_secs(u64::try_from(days(year, month, day) * 86_400 + seconds).unwrap())
    }

    #[test]
    fn retry_after_accepts_delay_seconds_and_each_http_date_format() {
        // RFC 9110 section 10.2.3; a recipient must accept all three HTTP-date
        // formats (section 5.6.7). Now is seven seconds before its example date.
        let now = unix(1994, 11, 6, 8 * 3600 + 49 * 60 + 30);
        let secs = |n| Some(Duration::from_secs(n));
        let leap_day = unix(1996, 2, 29, 0) - now;
        for (value, wait) in [
            ("120", secs(120)),
            ("0", secs(0)),
            ("0012", secs(12)),
            (&"9".repeat(400), Some(Duration::MAX)),
            ("Sun, 06 Nov 1994 08:49:37 GMT", secs(7)), // IMF-fixdate
            ("Sunday, 06-Nov-94 08:49:37 GMT", secs(7)), // obsolete RFC 850 format
            ("Sun Nov  6 08:49:37 1994", secs(7)),      // ANSI C asctime()
            ("Sun Nov 06 08:49:37 1994", secs(7)),
            ("Sun, 06 Nov 1994 08:49:00 GMT", secs(0)), // Already past: no wait.
            ("Sun, 06 Nov 1994 08:50:60 GMT", secs(90)), // A leap second.
            ("Thu, 29 Feb 1996 00:00:00 GMT", Some(leap_day)),
            (" 7 \t", secs(7)),
            ("1.5", None),
            ("-1", None),
            ("+1", None),
            ("", None),
            ("\u{661}\u{662}", None),                // Not ASCII digits.
            ("sun, 06 nov 1994 08:49:37 gmt", None), // HTTP-date is case sensitive.
            ("Sun, 6 Nov 1994 08:49:37 GMT", None),
            ("Sun,  06 Nov 1994 08:49:37 GMT", None),
            ("Sun, 06 Nov 1994 08:49:37 UTC", None),
            ("Sun, 31 Nov 1994 08:49:37 GMT", None),
            ("Fri, 29 Feb 1995 00:00:00 GMT", None),
            ("Sun, 06 Nov 1994 24:00:00 GMT", None),
            ("Sun, 06 Nov 1994 08:60:00 GMT", None),
            ("Sun Nov 6 08:49:37 1994", None),
            ("Sun, 06-Nov-94 08:49:37 GMT", None),
            ("Sunday, 06 Nov 1994 08:49:37 GMT", None),
        ] {
            assert_eq!(retry_after(value, now), wait, "{value:?}");
        }
    }

    #[test]
    fn a_two_digit_year_more_than_fifty_years_ahead_is_the_century_before() {
        // RFC 9110 section 5.6.7, for rfc850-date.
        let late_2099 = unix(2099, 12, 31, 86_390);
        assert_eq!(
            retry_after("Friday, 01-Jan-00 00:00:00 GMT", late_2099),
            Some(Duration::from_secs(10))
        );
        let now = unix(2026, 10, 27, 0);
        assert_eq!(
            retry_after("Tuesday, 27-Oct-76 00:00:00 GMT", now),
            Some(unix(2076, 10, 27, 0) - now)
        );
        for past in [
            "Wednesday, 28-Oct-76 00:00:00 GMT", // 1976
            "Monday, 26-Oct-26 00:00:00 GMT",
        ] {
            assert_eq!(retry_after(past, now), Some(Duration::ZERO), "{past}");
        }
        assert_eq!(year_of(days(2000, 2, 29)), 2000);
        assert_eq!(year_of(days(1969, 12, 31)), 1969);
        assert_eq!(year_of(days(2100, 1, 1)), 2100);
    }

    #[test]
    fn a_repeat_waits_retry_after_then_a_share_of_its_ceiling_within_the_budget() {
        let ms = Duration::from_millis;
        // Full jitter: a uniform share of the ceiling.
        assert_eq!(pause(ms(1000), 0.25, None, ms(5000)), Some(ms(250)));
        assert_eq!(pause(ms(250), 0.0, None, ms(5000)), Some(ms(0)));
        // Retry-After first, then the share.
        assert_eq!(
            pause(ms(250), 0.5, Some(ms(2000)), ms(5000)),
            Some(ms(2125))
        );
        // A wait that cannot end before repeats must start is not begun.
        assert_eq!(pause(ms(250), 0.5, Some(ms(4875)), ms(5000)), None);
        assert_eq!(pause(ms(250), 1.0, Some(Duration::MAX), ms(5000)), None);
        let share = jitter();
        assert!((0.0..1.0).contains(&share), "{share}");
    }
}
