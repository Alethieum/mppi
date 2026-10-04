//! Bounded validation of decoded protobuf messages and native gRPC payment errors.
//! Structural validation is not signature verification or a payment grant.

use crate::types::{
    ChainObservation, ConfirmedSettlementRef, PauseLevel, PaymentCode, PaymentFailure,
    validate_descriptor,
};
use crate::{Address, B256, U256, ValidationCode, ValidationError, proto as pb};
use prost::Message;
use std::collections::BTreeSet;

pub const MAX_MESSAGE_BYTES: usize = 16 << 20;
pub const MAX_CONTROL_BYTES: usize = 64 << 10;
pub const MAX_ERROR_BYTES: usize = 2048;
pub const MAX_PENDING_RUNS: usize = 1024;
pub const PAYMENT_ERROR_TYPE_URL: &str = "type.googleapis.com/mppi.v1.PaymentError";

fn invalid(message: &'static str) -> ValidationError {
    ValidationError::new(ValidationCode::InvalidValue, message)
}
fn limit(message: &'static str) -> ValidationError {
    ValidationError::new(ValidationCode::LimitExceeded, message)
}

/// Deployments may tighten these bounds; values cannot enlarge the v1 profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WireLimits {
    message_bytes: usize,
    control_bytes: usize,
    catalog_bytes: usize,
    json_depth: usize,
    pending_runs: usize,
    error_bytes: usize,
}
impl Default for WireLimits {
    fn default() -> Self {
        Self {
            message_bytes: MAX_MESSAGE_BYTES,
            control_bytes: MAX_CONTROL_BYTES,
            catalog_bytes: crate::DEFAULT_CATALOG_BYTES,
            json_depth: crate::DEFAULT_JSON_DEPTH,
            pending_runs: MAX_PENDING_RUNS,
            error_bytes: MAX_ERROR_BYTES,
        }
    }
}
impl WireLimits {
    pub fn new(
        message_bytes: usize,
        control_bytes: usize,
        catalog_bytes: usize,
        json_depth: usize,
        pending_runs: usize,
        error_bytes: usize,
    ) -> Result<Self, ValidationError> {
        if [
            (message_bytes, MAX_MESSAGE_BYTES),
            (control_bytes, MAX_CONTROL_BYTES),
            (catalog_bytes, crate::DEFAULT_CATALOG_BYTES),
            (json_depth, crate::DEFAULT_JSON_DEPTH),
            (pending_runs, MAX_PENDING_RUNS),
            (error_bytes, MAX_ERROR_BYTES),
        ]
        .iter()
        .any(|&(value, maximum)| value == 0 || value > maximum)
            || control_bytes > message_bytes
            || catalog_bytes > message_bytes
        {
            return Err(invalid("invalid wire limits"));
        }
        Ok(Self {
            message_bytes,
            control_bytes,
            catalog_bytes,
            json_depth,
            pending_runs,
            error_bytes,
        })
    }
    pub const fn message_bytes(&self) -> usize {
        self.message_bytes
    }
    pub const fn control_bytes(&self) -> usize {
        self.control_bytes
    }
    pub const fn catalog_bytes(&self) -> usize {
        self.catalog_bytes
    }
    pub const fn json_depth(&self) -> usize {
        self.json_depth
    }
    pub const fn pending_runs(&self) -> usize {
        self.pending_runs
    }
    pub const fn error_bytes(&self) -> usize {
        self.error_bytes
    }
}

/// Preserve the caller's payload; the returned borrow is exactly the input.
pub fn validate_json<'a>(
    payload: &'a [u8],
    model: Option<&str>,
    stream: Option<bool>,
) -> Result<&'a [u8], ValidationError> {
    validate_json_with_limits(payload, model, stream, &WireLimits::default())
}
pub fn validate_json_with_limits<'a>(
    payload: &'a [u8],
    model: Option<&str>,
    stream: Option<bool>,
    limits: &WireLimits,
) -> Result<&'a [u8], ValidationError> {
    let value = crate::json::parse_document(payload, limits.message_bytes, limits.json_depth)?;
    let object = value
        .as_object()
        .ok_or_else(|| invalid("inference JSON must be an object"))?;
    if model
        .is_some_and(|model| object.get("model").and_then(serde_json::Value::as_str) != Some(model))
    {
        return Err(invalid("inference model disagrees with signed envelope"));
    }
    if let Some(stream) = stream {
        let flag = match object.get("stream") {
            None => Some(false),
            Some(value) => value.as_bool(),
        };
        if flag != Some(stream) {
            return Err(invalid("inference stream flag disagrees with envelope"));
        }
    }
    Ok(payload)
}

mod sealed {
    pub trait Sealed {}
}
pub trait WireMessage: Message + Default + sealed::Sealed {
    #[doc(hidden)]
    fn validate_fields(&self, limits: &WireLimits) -> Result<(), ValidationError>;
    #[doc(hidden)]
    fn maximum_bytes(&self, limits: &WireLimits) -> usize {
        limits.message_bytes
    }
}
pub fn validate_message<T: WireMessage>(value: &T) -> Result<(), ValidationError> {
    validate_message_with_limits(value, &WireLimits::default())
}
pub fn validate_message_with_limits<T: WireMessage>(
    value: &T,
    limits: &WireLimits,
) -> Result<(), ValidationError> {
    if value.encoded_len() > value.maximum_bytes(limits) {
        return Err(limit("wire message limit exceeded"));
    }
    value.validate_fields(limits)
}
/// Check received bytes before decoding, including unknown fields discarded by prost.
/// Transport must also enforce the decompressed message limit before allocation.
pub fn decode_message<T: WireMessage>(
    raw: &[u8],
    limits: &WireLimits,
) -> Result<T, ValidationError> {
    if raw.len() > limits.message_bytes {
        return Err(limit("wire message limit exceeded"));
    }
    let value = T::decode(raw).map_err(|_| invalid("invalid protobuf encoding"))?;
    if raw.len() > value.maximum_bytes(limits) {
        return Err(limit("wire message limit exceeded"));
    }
    validate_message_with_limits(&value, limits)?;
    Ok(value)
}

fn required<T>(value: &Option<T>) -> Result<&T, ValidationError> {
    value
        .as_ref()
        .ok_or_else(|| invalid("required wire field is absent"))
}
fn bytes(value: &Option<Vec<u8>>, length: usize) -> Result<(), ValidationError> {
    if required(value)?.len() != length {
        return Err(invalid("invalid wire field width"));
    }
    Ok(())
}
fn optional_bytes(value: &Option<Vec<u8>>, length: usize) -> Result<(), ValidationError> {
    if value.is_some() {
        bytes(value, length)?;
    }
    Ok(())
}
fn address(value: &Option<Vec<u8>>) -> Result<(), ValidationError> {
    bytes(value, 20)?;
    if required(value)?.iter().all(|&byte| byte == 0) {
        return Err(invalid("wire address must be nonzero"));
    }
    Ok(())
}
fn model(value: &Option<String>) -> Result<(), ValidationError> {
    crate::validate_model(required(value)?)?;
    Ok(())
}
fn positive(value: &Option<u64>) -> Result<(), ValidationError> {
    if *required(value)? == 0 {
        return Err(invalid("wire counter must be positive"));
    }
    Ok(())
}
fn counts(
    input: &Option<u64>,
    cache: &Option<u64>,
    output: &Option<u64>,
) -> Result<(), ValidationError> {
    crate::TokenCounts::new(*required(input)?, *required(cache)?, *required(output)?)?;
    Ok(())
}
fn run_status(value: &Option<i32>, admitted: bool) -> Result<(), ValidationError> {
    let value = *required(value)?;
    if !(1..=4).contains(&value) || (admitted && value == 4) {
        return Err(invalid("invalid run status"));
    }
    Ok(())
}
fn unique<T: Ord>(values: impl IntoIterator<Item = T>) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(invalid("duplicate logical wire key"));
        }
    }
    Ok(())
}
macro_rules! check {
    ($v:ident, $fn:ident, $($field:ident),+ $(,)?) => { $( $fn(&$v.$field)?; )+ };
    ($v:ident, bytes $len:literal, $($field:ident),+ $(,)?) => { $( bytes(&$v.$field, $len)?; )+ };
}
macro_rules! wire_message {
    ($ty:ident, $value:ident, $limits:ident, $body:block) => {
        impl sealed::Sealed for pb::$ty {}
        impl WireMessage for pb::$ty {
            fn validate_fields(&self, $limits: &WireLimits) -> Result<(), ValidationError> {
                let $value = self;
                let _ = $limits;
                $body
                Ok(())
            }
        }
    };
}
wire_message!(CompleteRequest, v, l, {
    check!(v, model, model);
    check!(v, required, stream, payload);
    check!(v, bytes 32, session_id, run_id, rate_card_hash);
    check!(v, bytes 65, auth_signature);
    check!(v, positive, control_generation);
    validate_json_with_limits(
        required(&v.payload)?,
        Some(required(&v.model)?),
        Some(*required(&v.stream)?),
        l,
    )?;
});
wire_message!(ChatChunk, v, l, {
    match required(&v.kind)? {
        pb::chat_chunk::Kind::Payload(raw) => {
            validate_json_with_limits(raw, None, None, l)?;
        }
        pb::chat_chunk::Kind::Terminal(value) => validate_message_with_limits(value.as_ref(), l)?,
        pb::chat_chunk::Kind::UpstreamError(value) => validate_message_with_limits(value, l)?,
    }
});
wire_message!(Terminal, v, l, {
    counts(&v.input_tokens, &v.cache_tokens, &v.output_tokens)?;
    check!(v, required, amount);
    check!(v, bytes 32, session_id, run_id, rate_card_hash);
    check!(v, model, model);
    check!(v, positive, final_meter_seq);
    run_status(&v.status, true)?;
    if let Some(value) = &v.settlement_reference {
        validate_message_with_limits(value, l)?;
    }
});
impl sealed::Sealed for pb::ControlMessage {}
impl WireMessage for pb::ControlMessage {
    fn maximum_bytes(&self, limits: &WireLimits) -> usize {
        if matches!(self.kind, Some(pb::control_message::Kind::SessionState(_))) {
            limits.message_bytes
        } else {
            limits.control_bytes
        }
    }
    fn validate_fields(&self, l: &WireLimits) -> Result<(), ValidationError> {
        use pb::control_message::Kind;
        match required(&self.kind)? {
            Kind::ChallengeRequest(v) => validate_message_with_limits(v, l),
            Kind::Challenge(v) => validate_message_with_limits(v, l),
            Kind::SessionOpen(v) => validate_message_with_limits(v, l),
            Kind::ReconnectChallenge(v) => validate_message_with_limits(v, l),
            Kind::ReconnectRequest(v) => validate_message_with_limits(v, l),
            Kind::SessionState(v) => validate_message_with_limits(v, l),
            Kind::Voucher(v) => validate_message_with_limits(v, l),
            Kind::Meter(v) => validate_message_with_limits(v, l),
            Kind::TopUpNotice(v) => validate_message_with_limits(v, l),
            Kind::LowCredit(v) => validate_message_with_limits(v, l),
            Kind::Drain(v) => validate_message_with_limits(v, l),
            Kind::Receipt(v) => validate_message_with_limits(v, l),
            Kind::CloseRequest(v) => validate_message_with_limits(v, l),
            Kind::CloseResult(v) => validate_message_with_limits(v, l),
        }
    }
}
/// Validate sender direction separately from attachment authentication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlSender {
    Agent,
    Provider,
}
pub fn validate_control_direction(
    value: &pb::ControlMessage,
    sender: ControlSender,
) -> Result<(), ValidationError> {
    validate_message(value)?;
    use pb::control_message::Kind;
    let agent = matches!(
        value.kind,
        Some(
            Kind::ChallengeRequest(_)
                | Kind::SessionOpen(_)
                | Kind::ReconnectRequest(_)
                | Kind::Voucher(_)
                | Kind::TopUpNotice(_)
                | Kind::CloseRequest(_)
        )
    );
    if agent != (sender == ControlSender::Agent) {
        return Err(invalid("wrong Control message direction"));
    }
    Ok(())
}
wire_message!(ChallengeRequest, v, l, {
    check!(v, bytes 32, request_id);
    check!(v, model, model);
});
wire_message!(Challenge, v, l, {
    check!(v, bytes 32, request_id, rate_card_hash, provider_id);
    check!(v, model, model);
    check!(v, address, contract, operator);
    check!(v, positive, chain_id);
    check!(v, required, cover_output_tokens, suggest_session_tokens);
});
wire_message!(SessionOpen, v, l, {
    check!(v, bytes 32, session_id, open_tx);
    optional_bytes(&v.agent_id, 32)?;
});
wire_message!(ReconnectChallenge, v, l, {
    check!(v, bytes 32, nonce);
});
wire_message!(ReconnectRequest, v, l, {
    check!(v, bytes 32, session_id, nonce);
    check!(v, bytes 65, signature);
    check!(v, required, last_meter_seq);
    if v.pending_run_ids.len() > l.pending_runs {
        return Err(limit("pending run limit exceeded"));
    }
    if v.pending_run_ids.iter().any(|id| id.len() != 32) {
        return Err(invalid("pending run id must be 32 bytes"));
    }
    unique(v.pending_run_ids.iter())?;
    if let Some(value) = &v.latest_voucher {
        validate_message_with_limits(value, l)?;
    }
});
wire_message!(SessionState, v, l, {
    session_state_fields(v, l, false)?;
});

/// Provider-local checkpoint validation; never used for received/sent Control.
/// Canonical chain facts and authoritative private history are verified by recovery.
pub(crate) fn validate_recovery_snapshot(
    value: &pb::SessionState,
    limits: &WireLimits,
) -> Result<(), ValidationError> {
    if value.encoded_len() > limits.message_bytes {
        return Err(limit("wire message limit exceeded"));
    }
    session_state_fields(value, limits, true)
}

fn session_state_fields(
    v: &pb::SessionState,
    l: &WireLimits,
    unattached_snapshot: bool,
) -> Result<(), ValidationError> {
    check!(v, bytes 32, session_id, nonce, initial_rate_card_hash, chain_block_hash);
    optional_bytes(&v.current_rate_card_hash, 32)?;
    if !unattached_snapshot || *required(&v.control_generation)? != 0 {
        check!(v, positive, control_generation);
    }
    check!(
        v,
        required,
        ready,
        close_pending,
        meter_seq,
        deposit,
        settled_cumulative,
        chain_block_number
    );
    validate_message_with_limits(required(&v.descriptor)?, l)?;
    if let Some(value) = &v.accepted_voucher {
        validate_message_with_limits(value, l)?;
    }
    if !(1..=3).contains(required(&v.status)?) {
        return Err(invalid("invalid session status"));
    }
    for row in &v.usage_by_price {
        validate_message_with_limits(row, l)?;
    }
    for row in &v.run_states {
        validate_message_with_limits(row, l)?;
    }
    for row in &v.catalogs {
        validate_message_with_limits(row, l)?;
    }
    unique(
        v.usage_by_price
            .iter()
            .map(|r| (&r.model, &r.rate_card_hash)),
    )?;
    unique(v.run_states.iter().map(|r| &r.run_id))?;
    unique(v.catalogs.iter().map(|r| &r.rate_card_hash))?;
    if v.control_generation == Some(0)
        && (v.ready != Some(false)
            || v.nonce.as_deref() != Some(&[0; 32])
            || v.meter_seq != Some(0)
            || v.settled_cumulative != Some(0)
            || v.accepted_voucher.is_some()
            || !v.usage_by_price.is_empty()
            || !v.run_states.is_empty())
    {
        return Err(invalid(
            "unattached checkpoint contains attachment or accounting history",
        ));
    }
    Ok(())
}
wire_message!(SessionDescriptor, v, l, {
    check!(
        v,
        address,
        agent,
        session_signer,
        recipient,
        operator,
        token
    );
    check!(v, bytes 32, provider_id, salt);
});
wire_message!(Voucher, v, l, {
    check!(v, bytes 32, session_id);
    check!(v, bytes 65, signature);
    check!(v, required, cumulative);
});
wire_message!(Meter, v, l, {
    check!(v, bytes 32, session_id, run_id);
    check!(v, positive, seq);
    counts(&v.input_tokens, &v.cache_tokens, &v.output_tokens)?;
});
wire_message!(TopUpNotice, v, l, {
    check!(v, bytes 32, session_id, tx);
    match (
        v.model.is_some(),
        v.rate_card_hash.is_some(),
        v.tokens.is_some(),
    ) {
        (false, false, false) => (),
        (true, true, true) => {
            check!(v, model, model);
            check!(v, bytes 32, rate_card_hash);
            check!(v, positive, tokens);
        }
        _ => return Err(invalid("incomplete top-up provenance")),
    }
});
wire_message!(LowCredit, v, l, {
    check!(v, bytes 32, session_id, rate_card_hash);
    check!(v, model, model);
    if v.remaining_tokens.is_some() != v.suggest_top_up_tokens.is_some() {
        return Err(invalid("incomplete funding guidance"));
    }
    if let Some(value) = &v.coverage_request {
        validate_message_with_limits(value, l)?;
    }
});
wire_message!(CoverageRequest, v, l, {
    check!(v, bytes 32, run_id);
    check!(v, positive, control_generation);
    if v.required_by_price.is_empty() {
        return Err(invalid("coverage rows must be nonempty"));
    }
    for row in &v.required_by_price {
        validate_message_with_limits(row, l)?;
    }
    unique(
        v.required_by_price
            .iter()
            .map(|r| (&r.model, &r.rate_card_hash)),
    )?;
});
wire_message!(RequiredTokens, v, l, {
    check!(v, model, model);
    check!(v, bytes 32, rate_card_hash);
    counts(&v.input_tokens, &v.cache_tokens, &v.output_tokens)?;
});
wire_message!(Drain, v, l, {
    check!(v, bytes 32, session_id);
    optional_bytes(&v.run_id, 32)?;
    if !(1..=3).contains(required(&v.reason)?) {
        return Err(invalid("invalid drain reason"));
    }
});
wire_message!(Receipt, v, l, {
    check!(v, bytes 32, session_id, run_id, rate_card_hash);
    check!(v, model, model);
    counts(&v.input_tokens, &v.cache_tokens, &v.output_tokens)?;
    check!(v, required, amount, settled_cumulative);
    check!(v, positive, final_meter_seq);
    run_status(&v.status, true)?;
    validate_message_with_limits(required(&v.chain_observation)?, l)?;
    if let Some(value) = &v.settlement_reference {
        validate_message_with_limits(value, l)?;
    }
});
wire_message!(CloseRequest, v, l, {
    check!(v, bytes 32, session_id);
    check!(v, bytes 65, signature);
});
wire_message!(CloseResult, v, l, {
    check!(v, bytes 32, session_id, tx_hash);
    check!(v, required, capture_amount, refunded);
});
wire_message!(CatalogVersion, v, l, {
    check!(v, bytes 32, rate_card_hash);
    if required(&v.catalog)?.len() > l.catalog_bytes {
        return Err(limit("catalog byte limit exceeded"));
    }
});
wire_message!(PricedUsage, v, l, {
    check!(v, model, model);
    check!(v, bytes 32, rate_card_hash, acceptance_run_id);
    counts(&v.input_tokens, &v.cache_tokens, &v.output_tokens)?;
    check!(v, positive, acceptance_control_generation);
    check!(v, bytes 65, acceptance_signature);
});
wire_message!(RunState, v, l, {
    check!(v, bytes 32, run_id);
    run_status(&v.status, false)?;
    counts(&v.input_tokens, &v.cache_tokens, &v.output_tokens)?;
    if v.status == Some(4) {
        if !required(&v.model)?.is_empty()
            || v.rate_card_hash.is_some()
            || v.control_generation.is_some()
            || v.auth_signature.is_some()
            || v.input_tokens != Some(0)
            || v.cache_tokens != Some(0)
            || v.output_tokens != Some(0)
        {
            return Err(invalid("invalid not-admitted state"));
        }
    } else {
        check!(v, model, model);
        check!(v, bytes 32, rate_card_hash);
        check!(v, positive, control_generation);
        check!(v, bytes 65, auth_signature);
    }
});
wire_message!(ChainObservation, v, l, {
    check!(v, positive, chain_id);
    check!(v, address, contract_address);
    check!(v, required, block_number);
    check!(v, bytes 32, block_hash);
});
wire_message!(ConfirmedSettlementRef, v, l, {
    validate_message_with_limits(required(&v.observation)?, l)?;
    check!(v, bytes 32, tx_hash, session_id);
    check!(v, required, settled_cumulative);
    check!(v, positive, delta);
    if v.delta > v.settled_cumulative {
        return Err(invalid("settlement delta exceeds cumulative amount"));
    }
});
wire_message!(UpstreamError, v, l, {
    validate_json_with_limits(required(&v.body)?, None, None, l)?;
    if v.http_status
        .is_some_and(|status| !(400..=599).contains(&status))
    {
        return Err(invalid("invalid upstream HTTP status"));
    }
});
wire_message!(PaymentError, v, l, {
    payment_failure_from_wire(v)?;
});

pub fn descriptor_to_wire(
    value: &crate::signing::SessionDescriptor,
) -> Result<pb::SessionDescriptor, ValidationError> {
    validate_descriptor(value)?;
    Ok(pb::SessionDescriptor {
        agent: Some(value.agent.to_vec()),
        session_signer: Some(value.sessionSigner.to_vec()),
        provider_id: Some(value.providerId.to_be_bytes::<32>().to_vec()),
        recipient: Some(value.recipient.to_vec()),
        operator: Some(value.operator.to_vec()),
        token: Some(value.token.to_vec()),
        salt: Some(value.salt.to_vec()),
    })
}
pub fn descriptor_from_wire(
    value: &pb::SessionDescriptor,
) -> Result<crate::signing::SessionDescriptor, ValidationError> {
    validate_message(value)?;
    Ok(crate::signing::SessionDescriptor {
        agent: Address::from_slice(required(&value.agent)?),
        sessionSigner: Address::from_slice(required(&value.session_signer)?),
        providerId: U256::from_be_slice(required(&value.provider_id)?),
        recipient: Address::from_slice(required(&value.recipient)?),
        operator: Address::from_slice(required(&value.operator)?),
        token: Address::from_slice(required(&value.token)?),
        salt: B256::from_slice(required(&value.salt)?),
    })
}
pub fn observation_to_wire(value: &ChainObservation) -> pb::ChainObservation {
    pb::ChainObservation {
        chain_id: Some(value.chain_id()),
        contract_address: Some(value.contract_address().to_vec()),
        block_number: Some(value.block_number()),
        block_hash: Some(value.block_hash().to_vec()),
    }
}
pub fn observation_from_wire(
    value: &pb::ChainObservation,
) -> Result<ChainObservation, ValidationError> {
    validate_message(value)?;
    ChainObservation::new(
        *required(&value.chain_id)?,
        Address::from_slice(required(&value.contract_address)?),
        *required(&value.block_number)?,
        B256::from_slice(required(&value.block_hash)?),
    )
}
pub fn settlement_to_wire(value: &ConfirmedSettlementRef) -> pb::ConfirmedSettlementRef {
    pb::ConfirmedSettlementRef {
        observation: Some(observation_to_wire(&value.observation())),
        tx_hash: Some(value.tx_hash().to_vec()),
        session_id: Some(value.session_id().to_vec()),
        settled_cumulative: Some(value.settled_cumulative()),
        delta: Some(value.delta()),
    }
}
pub fn settlement_from_wire(
    value: &pb::ConfirmedSettlementRef,
) -> Result<ConfirmedSettlementRef, ValidationError> {
    validate_message(value)?;
    ConfirmedSettlementRef::new(
        observation_from_wire(required(&value.observation)?)?,
        B256::from_slice(required(&value.tx_hash)?),
        B256::from_slice(required(&value.session_id)?),
        *required(&value.settled_cumulative)?,
        *required(&value.delta)?,
    )
}

fn payment_code(value: PaymentCode) -> (i32, tonic::Code) {
    match value {
        PaymentCode::InsufficientDeposit => (1, tonic::Code::ResourceExhausted),
        PaymentCode::SessionNotFound => (2, tonic::Code::NotFound),
        PaymentCode::VoucherStale => (3, tonic::Code::FailedPrecondition),
        PaymentCode::SessionNotReady => (4, tonic::Code::FailedPrecondition),
    }
}
pub fn payment_failure_to_wire(value: &PaymentFailure) -> pb::PaymentError {
    pb::PaymentError {
        code: Some(payment_code(value.code()).0),
        reason: value.reason().map(str::to_owned),
        session_id: value.session_id().map(|v| v.to_vec()),
        run_id: value.run_id().map(|v| v.to_vec()),
        requested_rate_card_hash: value.requested_rate_card_hash().map(|v| v.to_vec()),
        current_rate_card_hash: value.current_rate_card_hash().map(|v| v.to_vec()),
        pause_level: value.pause_level().map(|v| v as i32),
    }
}
pub fn payment_failure_from_wire(
    value: &pb::PaymentError,
) -> Result<PaymentFailure, ValidationError> {
    let code = match *required(&value.code)? {
        1 => PaymentCode::InsufficientDeposit,
        2 => PaymentCode::SessionNotFound,
        3 => PaymentCode::VoucherStale,
        4 => PaymentCode::SessionNotReady,
        _ => return Err(invalid("invalid payment code")),
    };
    fn id(value: &Option<Vec<u8>>) -> Result<Option<B256>, ValidationError> {
        optional_bytes(value, 32)?;
        Ok(value.as_ref().map(|v| B256::from_slice(v)))
    }
    let pause = value
        .pause_level
        .map(|v| match v {
            0 => Ok(PauseLevel::None),
            1 => Ok(PauseLevel::Payments),
            2 => Ok(PauseLevel::All),
            _ => Err(invalid("invalid pause level")),
        })
        .transpose()?;
    PaymentFailure::new(
        code,
        value.reason.clone(),
        id(&value.session_id)?,
        id(&value.run_id)?,
        id(&value.requested_rate_card_hash)?,
        id(&value.current_rate_card_hash)?,
        pause,
    )
}

// Canonical google.rpc.Status / google.protobuf.Any fields, not a new public wire schema.
#[derive(Clone, PartialEq, Message)]
struct RichStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<RichAny>,
}
#[derive(Clone, PartialEq, Message)]
struct RichAny {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}
pub fn payment_status(failure: &PaymentFailure) -> tonic::Status {
    payment_status_with_limits(failure, &WireLimits::default())
}
pub fn payment_status_with_limits(failure: &PaymentFailure, limits: &WireLimits) -> tonic::Status {
    let code = payment_code(failure.code()).1;
    let message = failure.code().as_str();
    // Check the length before serializing a caller-supplied diagnostic string.
    if failure
        .reason()
        .is_some_and(|reason| reason.len() > limits.error_bytes)
    {
        return tonic::Status::new(code, message);
    }
    let payload = payment_failure_to_wire(failure);
    if payload.encoded_len() > limits.error_bytes {
        return tonic::Status::new(code, message);
    }
    let carrier = RichStatus {
        code: code as i32,
        message: message.to_owned(),
        details: vec![RichAny {
            type_url: PAYMENT_ERROR_TYPE_URL.to_owned(),
            value: payload.encode_to_vec(),
        }],
    };
    if carrier.encoded_len() > limits.error_bytes {
        return tonic::Status::new(code, message);
    }
    tonic::Status::with_details(code, message, carrier.encode_to_vec().into())
}
/// Parse one Status carrier. Empty details mean a native status without payment evidence.
pub fn parse_payment_error(
    status: &tonic::Status,
) -> Result<Option<PaymentFailure>, ValidationError> {
    let entries = [status.details()];
    parse_payment_error_details(
        status.code(),
        if status.details().is_empty() {
            &[]
        } else {
            &entries
        },
        &WireLimits::default(),
    )
}
/// A transport interceptor can use this before tonic normalizes metadata; duplicate
/// grpc-status-details-bin entries must be rejected rather than choosing one.
pub fn parse_payment_error_details(
    status: tonic::Code,
    entries: &[&[u8]],
    limits: &WireLimits,
) -> Result<Option<PaymentFailure>, ValidationError> {
    if entries.is_empty() {
        return Ok(None);
    }
    if entries.len() != 1 || entries[0].len() > limits.error_bytes {
        return Err(invalid("invalid payment error metadata"));
    }
    let carrier =
        RichStatus::decode(entries[0]).map_err(|_| invalid("invalid payment status encoding"))?;
    if carrier.code != status as i32 || carrier.details.len() != 1 {
        return Err(invalid("payment status carrier mismatch"));
    }
    let detail = &carrier.details[0];
    if detail.type_url != PAYMENT_ERROR_TYPE_URL {
        return Err(invalid("invalid payment error type"));
    }
    let value = pb::PaymentError::decode(detail.value.as_slice())
        .map_err(|_| invalid("invalid payment error encoding"))?;
    let failure = payment_failure_from_wire(&value)?;
    if payment_code(failure.code()).1 != status {
        return Err(invalid("payment error status mismatch"));
    }
    Ok(Some(failure))
}
