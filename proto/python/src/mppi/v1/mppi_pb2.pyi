from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RunStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_STATUS_UNSPECIFIED: _ClassVar[RunStatus]
    RUN_STATUS_COMPLETED: _ClassVar[RunStatus]
    RUN_STATUS_FAILED: _ClassVar[RunStatus]
    RUN_STATUS_CANCELLED: _ClassVar[RunStatus]
    RUN_STATUS_NOT_ADMITTED: _ClassVar[RunStatus]

class SessionStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SESSION_STATUS_UNSPECIFIED: _ClassVar[SessionStatus]
    SESSION_STATUS_OPEN: _ClassVar[SessionStatus]
    SESSION_STATUS_CLOSING: _ClassVar[SessionStatus]
    SESSION_STATUS_CLOSED: _ClassVar[SessionStatus]

class DrainReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DRAIN_REASON_UNSPECIFIED: _ClassVar[DrainReason]
    DRAIN_REASON_DEPOSIT_EXHAUSTED: _ClassVar[DrainReason]
    DRAIN_REASON_AUTHORIZATION_EXHAUSTED: _ClassVar[DrainReason]
    DRAIN_REASON_PROVIDER_INITIATED: _ClassVar[DrainReason]

class PaymentErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PAYMENT_ERROR_CODE_UNSPECIFIED: _ClassVar[PaymentErrorCode]
    PAYMENT_ERROR_CODE_INSUFFICIENT_DEPOSIT: _ClassVar[PaymentErrorCode]
    PAYMENT_ERROR_CODE_SESSION_NOT_FOUND: _ClassVar[PaymentErrorCode]
    PAYMENT_ERROR_CODE_VOUCHER_STALE: _ClassVar[PaymentErrorCode]
    PAYMENT_ERROR_CODE_SESSION_NOT_READY: _ClassVar[PaymentErrorCode]

class PauseLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PAUSE_LEVEL_NONE: _ClassVar[PauseLevel]
    PAUSE_LEVEL_PAYMENTS: _ClassVar[PauseLevel]
    PAUSE_LEVEL_ALL: _ClassVar[PauseLevel]
RUN_STATUS_UNSPECIFIED: RunStatus
RUN_STATUS_COMPLETED: RunStatus
RUN_STATUS_FAILED: RunStatus
RUN_STATUS_CANCELLED: RunStatus
RUN_STATUS_NOT_ADMITTED: RunStatus
SESSION_STATUS_UNSPECIFIED: SessionStatus
SESSION_STATUS_OPEN: SessionStatus
SESSION_STATUS_CLOSING: SessionStatus
SESSION_STATUS_CLOSED: SessionStatus
DRAIN_REASON_UNSPECIFIED: DrainReason
DRAIN_REASON_DEPOSIT_EXHAUSTED: DrainReason
DRAIN_REASON_AUTHORIZATION_EXHAUSTED: DrainReason
DRAIN_REASON_PROVIDER_INITIATED: DrainReason
PAYMENT_ERROR_CODE_UNSPECIFIED: PaymentErrorCode
PAYMENT_ERROR_CODE_INSUFFICIENT_DEPOSIT: PaymentErrorCode
PAYMENT_ERROR_CODE_SESSION_NOT_FOUND: PaymentErrorCode
PAYMENT_ERROR_CODE_VOUCHER_STALE: PaymentErrorCode
PAYMENT_ERROR_CODE_SESSION_NOT_READY: PaymentErrorCode
PAUSE_LEVEL_NONE: PauseLevel
PAUSE_LEVEL_PAYMENTS: PauseLevel
PAUSE_LEVEL_ALL: PauseLevel

class CompleteRequest(_message.Message):
    __slots__ = ("model", "stream", "session_id", "run_id", "control_generation", "rate_card_hash", "auth_signature", "payload")
    MODEL_FIELD_NUMBER: _ClassVar[int]
    STREAM_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CONTROL_GENERATION_FIELD_NUMBER: _ClassVar[int]
    RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    AUTH_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    model: str
    stream: bool
    session_id: bytes
    run_id: bytes
    control_generation: int
    rate_card_hash: bytes
    auth_signature: bytes
    payload: bytes
    def __init__(self, model: _Optional[str] = ..., stream: _Optional[bool] = ..., session_id: _Optional[bytes] = ..., run_id: _Optional[bytes] = ..., control_generation: _Optional[int] = ..., rate_card_hash: _Optional[bytes] = ..., auth_signature: _Optional[bytes] = ..., payload: _Optional[bytes] = ...) -> None: ...

class ChatChunk(_message.Message):
    __slots__ = ("payload", "terminal", "upstream_error")
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_FIELD_NUMBER: _ClassVar[int]
    UPSTREAM_ERROR_FIELD_NUMBER: _ClassVar[int]
    payload: bytes
    terminal: Terminal
    upstream_error: UpstreamError
    def __init__(self, payload: _Optional[bytes] = ..., terminal: _Optional[_Union[Terminal, _Mapping]] = ..., upstream_error: _Optional[_Union[UpstreamError, _Mapping]] = ...) -> None: ...

class Terminal(_message.Message):
    __slots__ = ("input_tokens", "cache_tokens", "output_tokens", "amount", "session_id", "run_id", "model", "rate_card_hash", "status", "final_meter_seq", "settlement_reference")
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHE_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FINAL_METER_SEQ_FIELD_NUMBER: _ClassVar[int]
    SETTLEMENT_REFERENCE_FIELD_NUMBER: _ClassVar[int]
    input_tokens: int
    cache_tokens: int
    output_tokens: int
    amount: int
    session_id: bytes
    run_id: bytes
    model: str
    rate_card_hash: bytes
    status: RunStatus
    final_meter_seq: int
    settlement_reference: ConfirmedSettlementRef
    def __init__(self, input_tokens: _Optional[int] = ..., cache_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ..., amount: _Optional[int] = ..., session_id: _Optional[bytes] = ..., run_id: _Optional[bytes] = ..., model: _Optional[str] = ..., rate_card_hash: _Optional[bytes] = ..., status: _Optional[_Union[RunStatus, str]] = ..., final_meter_seq: _Optional[int] = ..., settlement_reference: _Optional[_Union[ConfirmedSettlementRef, _Mapping]] = ...) -> None: ...

class ControlMessage(_message.Message):
    __slots__ = ("challenge_request", "challenge", "session_open", "reconnect_challenge", "reconnect_request", "session_state", "voucher", "meter", "top_up_notice", "low_credit", "drain", "receipt", "close_request", "close_result")
    CHALLENGE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    CHALLENGE_FIELD_NUMBER: _ClassVar[int]
    SESSION_OPEN_FIELD_NUMBER: _ClassVar[int]
    RECONNECT_CHALLENGE_FIELD_NUMBER: _ClassVar[int]
    RECONNECT_REQUEST_FIELD_NUMBER: _ClassVar[int]
    SESSION_STATE_FIELD_NUMBER: _ClassVar[int]
    VOUCHER_FIELD_NUMBER: _ClassVar[int]
    METER_FIELD_NUMBER: _ClassVar[int]
    TOP_UP_NOTICE_FIELD_NUMBER: _ClassVar[int]
    LOW_CREDIT_FIELD_NUMBER: _ClassVar[int]
    DRAIN_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    CLOSE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    CLOSE_RESULT_FIELD_NUMBER: _ClassVar[int]
    challenge_request: ChallengeRequest
    challenge: Challenge
    session_open: SessionOpen
    reconnect_challenge: ReconnectChallenge
    reconnect_request: ReconnectRequest
    session_state: SessionState
    voucher: Voucher
    meter: Meter
    top_up_notice: TopUpNotice
    low_credit: LowCredit
    drain: Drain
    receipt: Receipt
    close_request: CloseRequest
    close_result: CloseResult
    def __init__(self, challenge_request: _Optional[_Union[ChallengeRequest, _Mapping]] = ..., challenge: _Optional[_Union[Challenge, _Mapping]] = ..., session_open: _Optional[_Union[SessionOpen, _Mapping]] = ..., reconnect_challenge: _Optional[_Union[ReconnectChallenge, _Mapping]] = ..., reconnect_request: _Optional[_Union[ReconnectRequest, _Mapping]] = ..., session_state: _Optional[_Union[SessionState, _Mapping]] = ..., voucher: _Optional[_Union[Voucher, _Mapping]] = ..., meter: _Optional[_Union[Meter, _Mapping]] = ..., top_up_notice: _Optional[_Union[TopUpNotice, _Mapping]] = ..., low_credit: _Optional[_Union[LowCredit, _Mapping]] = ..., drain: _Optional[_Union[Drain, _Mapping]] = ..., receipt: _Optional[_Union[Receipt, _Mapping]] = ..., close_request: _Optional[_Union[CloseRequest, _Mapping]] = ..., close_result: _Optional[_Union[CloseResult, _Mapping]] = ...) -> None: ...

class ChallengeRequest(_message.Message):
    __slots__ = ("request_id", "model")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    request_id: bytes
    model: str
    def __init__(self, request_id: _Optional[bytes] = ..., model: _Optional[str] = ...) -> None: ...

class Challenge(_message.Message):
    __slots__ = ("request_id", "model", "rate_card_hash", "provider_id", "contract", "chain_id", "operator", "cover_output_tokens", "suggest_session_tokens")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    CONTRACT_FIELD_NUMBER: _ClassVar[int]
    CHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    COVER_OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    SUGGEST_SESSION_TOKENS_FIELD_NUMBER: _ClassVar[int]
    request_id: bytes
    model: str
    rate_card_hash: bytes
    provider_id: bytes
    contract: bytes
    chain_id: int
    operator: bytes
    cover_output_tokens: int
    suggest_session_tokens: int
    def __init__(self, request_id: _Optional[bytes] = ..., model: _Optional[str] = ..., rate_card_hash: _Optional[bytes] = ..., provider_id: _Optional[bytes] = ..., contract: _Optional[bytes] = ..., chain_id: _Optional[int] = ..., operator: _Optional[bytes] = ..., cover_output_tokens: _Optional[int] = ..., suggest_session_tokens: _Optional[int] = ...) -> None: ...

class SessionOpen(_message.Message):
    __slots__ = ("session_id", "open_tx", "agent_id")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    OPEN_TX_FIELD_NUMBER: _ClassVar[int]
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    open_tx: bytes
    agent_id: bytes
    def __init__(self, session_id: _Optional[bytes] = ..., open_tx: _Optional[bytes] = ..., agent_id: _Optional[bytes] = ...) -> None: ...

class ReconnectChallenge(_message.Message):
    __slots__ = ("nonce",)
    NONCE_FIELD_NUMBER: _ClassVar[int]
    nonce: bytes
    def __init__(self, nonce: _Optional[bytes] = ...) -> None: ...

class ReconnectRequest(_message.Message):
    __slots__ = ("session_id", "nonce", "signature", "latest_voucher", "last_meter_seq", "pending_run_ids")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    LATEST_VOUCHER_FIELD_NUMBER: _ClassVar[int]
    LAST_METER_SEQ_FIELD_NUMBER: _ClassVar[int]
    PENDING_RUN_IDS_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    nonce: bytes
    signature: bytes
    latest_voucher: Voucher
    last_meter_seq: int
    pending_run_ids: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, session_id: _Optional[bytes] = ..., nonce: _Optional[bytes] = ..., signature: _Optional[bytes] = ..., latest_voucher: _Optional[_Union[Voucher, _Mapping]] = ..., last_meter_seq: _Optional[int] = ..., pending_run_ids: _Optional[_Iterable[bytes]] = ...) -> None: ...

class SessionState(_message.Message):
    __slots__ = ("session_id", "nonce", "control_generation", "ready", "close_pending", "descriptor", "accepted_voucher", "meter_seq", "usage_by_price", "run_states", "catalogs", "initial_rate_card_hash", "current_rate_card_hash", "deposit", "settled_cumulative", "status", "chain_block_number", "chain_block_hash")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    CONTROL_GENERATION_FIELD_NUMBER: _ClassVar[int]
    READY_FIELD_NUMBER: _ClassVar[int]
    CLOSE_PENDING_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_VOUCHER_FIELD_NUMBER: _ClassVar[int]
    METER_SEQ_FIELD_NUMBER: _ClassVar[int]
    USAGE_BY_PRICE_FIELD_NUMBER: _ClassVar[int]
    RUN_STATES_FIELD_NUMBER: _ClassVar[int]
    CATALOGS_FIELD_NUMBER: _ClassVar[int]
    INITIAL_RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    CURRENT_RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    DEPOSIT_FIELD_NUMBER: _ClassVar[int]
    SETTLED_CUMULATIVE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CHAIN_BLOCK_NUMBER_FIELD_NUMBER: _ClassVar[int]
    CHAIN_BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    nonce: bytes
    control_generation: int
    ready: bool
    close_pending: bool
    descriptor: SessionDescriptor
    accepted_voucher: Voucher
    meter_seq: int
    usage_by_price: _containers.RepeatedCompositeFieldContainer[PricedUsage]
    run_states: _containers.RepeatedCompositeFieldContainer[RunState]
    catalogs: _containers.RepeatedCompositeFieldContainer[CatalogVersion]
    initial_rate_card_hash: bytes
    current_rate_card_hash: bytes
    deposit: int
    settled_cumulative: int
    status: SessionStatus
    chain_block_number: int
    chain_block_hash: bytes
    def __init__(self, session_id: _Optional[bytes] = ..., nonce: _Optional[bytes] = ..., control_generation: _Optional[int] = ..., ready: _Optional[bool] = ..., close_pending: _Optional[bool] = ..., descriptor: _Optional[_Union[SessionDescriptor, _Mapping]] = ..., accepted_voucher: _Optional[_Union[Voucher, _Mapping]] = ..., meter_seq: _Optional[int] = ..., usage_by_price: _Optional[_Iterable[_Union[PricedUsage, _Mapping]]] = ..., run_states: _Optional[_Iterable[_Union[RunState, _Mapping]]] = ..., catalogs: _Optional[_Iterable[_Union[CatalogVersion, _Mapping]]] = ..., initial_rate_card_hash: _Optional[bytes] = ..., current_rate_card_hash: _Optional[bytes] = ..., deposit: _Optional[int] = ..., settled_cumulative: _Optional[int] = ..., status: _Optional[_Union[SessionStatus, str]] = ..., chain_block_number: _Optional[int] = ..., chain_block_hash: _Optional[bytes] = ...) -> None: ...

class SessionDescriptor(_message.Message):
    __slots__ = ("agent", "session_signer", "provider_id", "recipient", "operator", "token", "salt")
    AGENT_FIELD_NUMBER: _ClassVar[int]
    SESSION_SIGNER_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    SALT_FIELD_NUMBER: _ClassVar[int]
    agent: bytes
    session_signer: bytes
    provider_id: bytes
    recipient: bytes
    operator: bytes
    token: bytes
    salt: bytes
    def __init__(self, agent: _Optional[bytes] = ..., session_signer: _Optional[bytes] = ..., provider_id: _Optional[bytes] = ..., recipient: _Optional[bytes] = ..., operator: _Optional[bytes] = ..., token: _Optional[bytes] = ..., salt: _Optional[bytes] = ...) -> None: ...

class Voucher(_message.Message):
    __slots__ = ("session_id", "cumulative", "signature")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CUMULATIVE_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    cumulative: int
    signature: bytes
    def __init__(self, session_id: _Optional[bytes] = ..., cumulative: _Optional[int] = ..., signature: _Optional[bytes] = ...) -> None: ...

class Meter(_message.Message):
    __slots__ = ("session_id", "run_id", "seq", "input_tokens", "cache_tokens", "output_tokens")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHE_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    run_id: bytes
    seq: int
    input_tokens: int
    cache_tokens: int
    output_tokens: int
    def __init__(self, session_id: _Optional[bytes] = ..., run_id: _Optional[bytes] = ..., seq: _Optional[int] = ..., input_tokens: _Optional[int] = ..., cache_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ...) -> None: ...

class TopUpNotice(_message.Message):
    __slots__ = ("session_id", "tx", "model", "rate_card_hash", "tokens")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TX_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    TOKENS_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    tx: bytes
    model: str
    rate_card_hash: bytes
    tokens: int
    def __init__(self, session_id: _Optional[bytes] = ..., tx: _Optional[bytes] = ..., model: _Optional[str] = ..., rate_card_hash: _Optional[bytes] = ..., tokens: _Optional[int] = ...) -> None: ...

class LowCredit(_message.Message):
    __slots__ = ("session_id", "model", "rate_card_hash", "remaining_tokens", "suggest_top_up_tokens", "coverage_request")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    REMAINING_TOKENS_FIELD_NUMBER: _ClassVar[int]
    SUGGEST_TOP_UP_TOKENS_FIELD_NUMBER: _ClassVar[int]
    COVERAGE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    model: str
    rate_card_hash: bytes
    remaining_tokens: int
    suggest_top_up_tokens: int
    coverage_request: CoverageRequest
    def __init__(self, session_id: _Optional[bytes] = ..., model: _Optional[str] = ..., rate_card_hash: _Optional[bytes] = ..., remaining_tokens: _Optional[int] = ..., suggest_top_up_tokens: _Optional[int] = ..., coverage_request: _Optional[_Union[CoverageRequest, _Mapping]] = ...) -> None: ...

class CoverageRequest(_message.Message):
    __slots__ = ("run_id", "control_generation", "required_by_price")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CONTROL_GENERATION_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_BY_PRICE_FIELD_NUMBER: _ClassVar[int]
    run_id: bytes
    control_generation: int
    required_by_price: _containers.RepeatedCompositeFieldContainer[RequiredTokens]
    def __init__(self, run_id: _Optional[bytes] = ..., control_generation: _Optional[int] = ..., required_by_price: _Optional[_Iterable[_Union[RequiredTokens, _Mapping]]] = ...) -> None: ...

class RequiredTokens(_message.Message):
    __slots__ = ("model", "rate_card_hash", "input_tokens", "cache_tokens", "output_tokens")
    MODEL_FIELD_NUMBER: _ClassVar[int]
    RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHE_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    model: str
    rate_card_hash: bytes
    input_tokens: int
    cache_tokens: int
    output_tokens: int
    def __init__(self, model: _Optional[str] = ..., rate_card_hash: _Optional[bytes] = ..., input_tokens: _Optional[int] = ..., cache_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ...) -> None: ...

class Drain(_message.Message):
    __slots__ = ("session_id", "run_id", "reason")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    run_id: bytes
    reason: DrainReason
    def __init__(self, session_id: _Optional[bytes] = ..., run_id: _Optional[bytes] = ..., reason: _Optional[_Union[DrainReason, str]] = ...) -> None: ...

class Receipt(_message.Message):
    __slots__ = ("session_id", "run_id", "model", "rate_card_hash", "input_tokens", "output_tokens", "amount", "settled_cumulative", "cache_tokens", "status", "final_meter_seq", "chain_observation", "settlement_reference")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    SETTLED_CUMULATIVE_FIELD_NUMBER: _ClassVar[int]
    CACHE_TOKENS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FINAL_METER_SEQ_FIELD_NUMBER: _ClassVar[int]
    CHAIN_OBSERVATION_FIELD_NUMBER: _ClassVar[int]
    SETTLEMENT_REFERENCE_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    run_id: bytes
    model: str
    rate_card_hash: bytes
    input_tokens: int
    output_tokens: int
    amount: int
    settled_cumulative: int
    cache_tokens: int
    status: RunStatus
    final_meter_seq: int
    chain_observation: ChainObservation
    settlement_reference: ConfirmedSettlementRef
    def __init__(self, session_id: _Optional[bytes] = ..., run_id: _Optional[bytes] = ..., model: _Optional[str] = ..., rate_card_hash: _Optional[bytes] = ..., input_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ..., amount: _Optional[int] = ..., settled_cumulative: _Optional[int] = ..., cache_tokens: _Optional[int] = ..., status: _Optional[_Union[RunStatus, str]] = ..., final_meter_seq: _Optional[int] = ..., chain_observation: _Optional[_Union[ChainObservation, _Mapping]] = ..., settlement_reference: _Optional[_Union[ConfirmedSettlementRef, _Mapping]] = ...) -> None: ...

class CloseRequest(_message.Message):
    __slots__ = ("session_id", "signature")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    signature: bytes
    def __init__(self, session_id: _Optional[bytes] = ..., signature: _Optional[bytes] = ...) -> None: ...

class CloseResult(_message.Message):
    __slots__ = ("session_id", "tx_hash", "capture_amount", "refunded")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TX_HASH_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    REFUNDED_FIELD_NUMBER: _ClassVar[int]
    session_id: bytes
    tx_hash: bytes
    capture_amount: int
    refunded: int
    def __init__(self, session_id: _Optional[bytes] = ..., tx_hash: _Optional[bytes] = ..., capture_amount: _Optional[int] = ..., refunded: _Optional[int] = ...) -> None: ...

class CatalogVersion(_message.Message):
    __slots__ = ("rate_card_hash", "catalog")
    RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    CATALOG_FIELD_NUMBER: _ClassVar[int]
    rate_card_hash: bytes
    catalog: bytes
    def __init__(self, rate_card_hash: _Optional[bytes] = ..., catalog: _Optional[bytes] = ...) -> None: ...

class PricedUsage(_message.Message):
    __slots__ = ("model", "rate_card_hash", "input_tokens", "cache_tokens", "output_tokens", "acceptance_run_id", "acceptance_control_generation", "acceptance_signature")
    MODEL_FIELD_NUMBER: _ClassVar[int]
    RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHE_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    ACCEPTANCE_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTANCE_CONTROL_GENERATION_FIELD_NUMBER: _ClassVar[int]
    ACCEPTANCE_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    model: str
    rate_card_hash: bytes
    input_tokens: int
    cache_tokens: int
    output_tokens: int
    acceptance_run_id: bytes
    acceptance_control_generation: int
    acceptance_signature: bytes
    def __init__(self, model: _Optional[str] = ..., rate_card_hash: _Optional[bytes] = ..., input_tokens: _Optional[int] = ..., cache_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ..., acceptance_run_id: _Optional[bytes] = ..., acceptance_control_generation: _Optional[int] = ..., acceptance_signature: _Optional[bytes] = ...) -> None: ...

class RunState(_message.Message):
    __slots__ = ("run_id", "model", "rate_card_hash", "control_generation", "auth_signature", "status", "input_tokens", "cache_tokens", "output_tokens")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    CONTROL_GENERATION_FIELD_NUMBER: _ClassVar[int]
    AUTH_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHE_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    run_id: bytes
    model: str
    rate_card_hash: bytes
    control_generation: int
    auth_signature: bytes
    status: RunStatus
    input_tokens: int
    cache_tokens: int
    output_tokens: int
    def __init__(self, run_id: _Optional[bytes] = ..., model: _Optional[str] = ..., rate_card_hash: _Optional[bytes] = ..., control_generation: _Optional[int] = ..., auth_signature: _Optional[bytes] = ..., status: _Optional[_Union[RunStatus, str]] = ..., input_tokens: _Optional[int] = ..., cache_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ...) -> None: ...

class ChainObservation(_message.Message):
    __slots__ = ("chain_id", "contract_address", "block_number", "block_hash")
    CHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    CONTRACT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    BLOCK_NUMBER_FIELD_NUMBER: _ClassVar[int]
    BLOCK_HASH_FIELD_NUMBER: _ClassVar[int]
    chain_id: int
    contract_address: bytes
    block_number: int
    block_hash: bytes
    def __init__(self, chain_id: _Optional[int] = ..., contract_address: _Optional[bytes] = ..., block_number: _Optional[int] = ..., block_hash: _Optional[bytes] = ...) -> None: ...

class ConfirmedSettlementRef(_message.Message):
    __slots__ = ("observation", "tx_hash", "session_id", "settled_cumulative", "delta")
    OBSERVATION_FIELD_NUMBER: _ClassVar[int]
    TX_HASH_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    SETTLED_CUMULATIVE_FIELD_NUMBER: _ClassVar[int]
    DELTA_FIELD_NUMBER: _ClassVar[int]
    observation: ChainObservation
    tx_hash: bytes
    session_id: bytes
    settled_cumulative: int
    delta: int
    def __init__(self, observation: _Optional[_Union[ChainObservation, _Mapping]] = ..., tx_hash: _Optional[bytes] = ..., session_id: _Optional[bytes] = ..., settled_cumulative: _Optional[int] = ..., delta: _Optional[int] = ...) -> None: ...

class UpstreamError(_message.Message):
    __slots__ = ("http_status", "body")
    HTTP_STATUS_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    http_status: int
    body: bytes
    def __init__(self, http_status: _Optional[int] = ..., body: _Optional[bytes] = ...) -> None: ...

class PaymentError(_message.Message):
    __slots__ = ("code", "session_id", "run_id", "reason", "requested_rate_card_hash", "current_rate_card_hash", "pause_level")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    CURRENT_RATE_CARD_HASH_FIELD_NUMBER: _ClassVar[int]
    PAUSE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    code: PaymentErrorCode
    session_id: bytes
    run_id: bytes
    reason: str
    requested_rate_card_hash: bytes
    current_rate_card_hash: bytes
    pause_level: PauseLevel
    def __init__(self, code: _Optional[_Union[PaymentErrorCode, str]] = ..., session_id: _Optional[bytes] = ..., run_id: _Optional[bytes] = ..., reason: _Optional[str] = ..., requested_rate_card_hash: _Optional[bytes] = ..., current_rate_card_hash: _Optional[bytes] = ..., pause_level: _Optional[_Union[PauseLevel, str]] = ...) -> None: ...
