"""Shared SDK value records. Structural validity is not verified chain evidence."""

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum

from .amounts import TokenCounts, uint64
from .catalog import validate_model
from .errors import ValidationCode, ValidationError


def fixed_bytes(value: bytes, length: int, *, field: str, nonzero: bool = False) -> bytes:
    if type(value) is not bytes or len(value) != length or (nonzero and not any(value)):
        raise ValidationError(
            ValidationCode.INVALID_VALUE,
            f"{field} must be {length} bytes" + (" and nonzero" if nonzero else ""),
        )
    return value


def uint256(value: int, *, field: str) -> int:
    if type(value) is not int or not 0 <= value < 1 << 256:
        raise ValidationError(ValidationCode.INVALID_VALUE, f"{field} must be uint256")
    return value


@dataclass(frozen=True, slots=True)
class SessionDescriptor:
    agent: bytes
    session_signer: bytes
    provider_id: int
    recipient: bytes
    operator: bytes
    token: bytes
    salt: bytes

    def __post_init__(self) -> None:
        for name in ("agent", "session_signer", "recipient", "operator", "token"):
            fixed_bytes(getattr(self, name), 20, field=name, nonzero=True)
        uint256(self.provider_id, field="provider_id")
        fixed_bytes(self.salt, 32, field="salt")


@dataclass(frozen=True, slots=True)
class RunContext:
    session_id: bytes
    run_id: bytes
    control_generation: int
    model: str
    rate_card_hash: bytes

    def __post_init__(self) -> None:
        fixed_bytes(self.session_id, 32, field="session_id")
        fixed_bytes(self.run_id, 32, field="run_id")
        uint64(self.control_generation, field="control_generation", positive=True)
        validate_model(self.model)
        fixed_bytes(self.rate_card_hash, 32, field="rate_card_hash")


@dataclass(frozen=True, slots=True)
class ChainObservation:
    chain_id: int
    contract_address: bytes
    block_number: int
    block_hash: bytes

    def __post_init__(self) -> None:
        uint64(self.chain_id, field="chain_id", positive=True)
        fixed_bytes(self.contract_address, 20, field="contract_address", nonzero=True)
        uint64(self.block_number, field="block_number")
        fixed_bytes(self.block_hash, 32, field="block_hash")


@dataclass(frozen=True, slots=True)
class ConfirmedSettlementRef:
    """Runtime creates this only after canonical event verification."""

    observation: ChainObservation
    tx_hash: bytes
    session_id: bytes
    settled_cumulative: int
    delta: int

    def __post_init__(self) -> None:
        fixed_bytes(self.tx_hash, 32, field="tx_hash")
        fixed_bytes(self.session_id, 32, field="session_id")
        uint64(self.settled_cumulative, field="settled_cumulative")
        uint64(self.delta, field="delta", positive=True)
        if self.delta > self.settled_cumulative:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "delta exceeds cumulative settlement"
            )


class OperationState(StrEnum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    UNKNOWN = "unknown"


class SessionStatus(StrEnum):
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


class PaymentCode(StrEnum):
    INSUFFICIENT_DEPOSIT = "INSUFFICIENT_DEPOSIT"
    SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
    VOUCHER_STALE = "VOUCHER_STALE"
    SESSION_NOT_READY = "SESSION_NOT_READY"


class PauseLevel(IntEnum):
    NONE = 0
    PAYMENTS = 1
    ALL = 2


@dataclass(frozen=True, slots=True)
class PaymentFailure:
    code: PaymentCode
    reason: str | None = None
    session_id: bytes | None = None
    run_id: bytes | None = None
    requested_rate_card_hash: bytes | None = None
    current_rate_card_hash: bytes | None = None
    pause_level: PauseLevel | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.code, PaymentCode):
            raise ValidationError(ValidationCode.INVALID_VALUE, "unknown payment code")
        for name in ("session_id", "run_id", "requested_rate_card_hash", "current_rate_card_hash"):
            value = getattr(self, name)
            if value is not None:
                fixed_bytes(value, 32, field=name)
        if self.run_id is not None and self.session_id is None:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "run diagnostic requires session_id"
            )
        if self.reason is not None and (type(self.reason) is not str or not self.reason):
            raise ValidationError(ValidationCode.INVALID_VALUE, "reason must be a nonempty string")
        if self.code is PaymentCode.SESSION_NOT_READY and self.reason is None:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "SESSION_NOT_READY requires a reason"
            )
        if self.reason == "PRICE_CHANGED":
            if (
                self.code is not PaymentCode.SESSION_NOT_READY
                or self.requested_rate_card_hash is None
                or self.current_rate_card_hash is None
            ):
                raise ValidationError(
                    ValidationCode.INVALID_VALUE, "PRICE_CHANGED requires both catalog hashes"
                )
        elif self.requested_rate_card_hash is not None or self.current_rate_card_hash is not None:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "catalog diagnostics require PRICE_CHANGED"
            )
        if self.reason == "CONTRACT_PAUSED":
            if (
                self.code is not PaymentCode.SESSION_NOT_READY
                or not isinstance(self.pause_level, PauseLevel)
                or self.pause_level is PauseLevel.NONE
            ):
                raise ValidationError(
                    ValidationCode.INVALID_VALUE,
                    "CONTRACT_PAUSED requires active observed pause level",
                )
        elif self.pause_level is not None:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "pause diagnostic requires CONTRACT_PAUSED"
            )


@dataclass(frozen=True, slots=True)
class Readiness:
    ready: bool
    failures: tuple[PaymentFailure, ...] = ()

    def __post_init__(self) -> None:
        if type(self.ready) is not bool or (self.ready and self.failures):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "ready result cannot contain failures"
            )


@dataclass(frozen=True, slots=True)
class SessionOperationOutcome:
    session_id: bytes
    target: int | None = None
    target_covered: bool | None = None
    settlement_reference: ConfirmedSettlementRef | None = None
    failure: PaymentFailure | None = None
    # The session's deposit after a proven opening or top-up, from its event.
    deposit: int | None = None

    def __post_init__(self) -> None:
        fixed_bytes(self.session_id, 32, field="session_id")
        if self.target is not None:
            uint64(self.target, field="target")
        if self.deposit is not None:
            uint64(self.deposit, field="deposit", positive=True)
        if self.target_covered is not None and type(self.target_covered) is not bool:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "target_covered must be a boolean when present"
            )
        if (
            self.settlement_reference is not None
            and self.settlement_reference.session_id != self.session_id
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "settlement reference names a different session"
            )


@dataclass(frozen=True, slots=True)
class OperationResult:
    operation_ref: str
    state: OperationState
    wallet_operation_id: str | None = None
    tx_hash: bytes | None = None
    observation: ChainObservation | None = None
    sessions: tuple[SessionOperationOutcome, ...] = ()
    # Why a wallet call ended this way: the kind of error an adapter raised,
    # SDK-written, never remote text. Diagnostic only; not part of equality.
    cause: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        if type(self.operation_ref) is not str or not self.operation_ref:
            raise ValidationError(ValidationCode.INVALID_VALUE, "operation_ref is required")
        if self.cause is not None and (type(self.cause) is not str or not self.cause):
            raise ValidationError(ValidationCode.INVALID_VALUE, "invalid operation cause")
        if not isinstance(self.state, OperationState):
            raise ValidationError(ValidationCode.INVALID_VALUE, "unknown operation state")
        if self.tx_hash is not None:
            fixed_bytes(self.tx_hash, 32, field="tx_hash")
        if self.state is OperationState.CONFIRMED and (
            self.observation is None or self.tx_hash is None
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "confirmed operation requires chain evidence"
            )


@dataclass(frozen=True, slots=True)
class SessionView:
    session_id: bytes
    descriptor: SessionDescriptor
    deposit: int
    settled_cumulative: int
    accepted_authorization: int | None
    usage_amount: int | None
    status: SessionStatus
    observation: ChainObservation
    fresh: bool
    readiness: Readiness

    def __post_init__(self) -> None:
        fixed_bytes(self.session_id, 32, field="session_id")
        uint64(self.deposit, field="deposit", positive=True)
        uint64(self.settled_cumulative, field="settled_cumulative")
        if self.settled_cumulative > self.deposit:
            raise ValidationError(ValidationCode.INVALID_VALUE, "settlement exceeds deposit")
        for name in ("accepted_authorization", "usage_amount"):
            value = getattr(self, name)
            if value is not None:
                uint64(value, field=name)
        if not isinstance(self.status, SessionStatus) or type(self.fresh) is not bool:
            raise ValidationError(ValidationCode.INVALID_VALUE, "invalid session status/freshness")


@dataclass(frozen=True, slots=True)
class SessionPage:
    sessions: tuple[SessionView, ...]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class ShutdownResult:
    unresolved_session_ids: tuple[bytes, ...]
    unresolved_runs: tuple[RunContext, ...]
    pending_operations: tuple[OperationResult, ...]


@dataclass(frozen=True, slots=True)
class AccountingResult:
    context: RunContext
    counts: TokenCounts
    meter_seq: int
    session_usage_amount: int
    duplicate: bool

    def __post_init__(self) -> None:
        uint64(self.meter_seq, field="meter_seq", positive=True)
        uint64(self.session_usage_amount, field="session_usage_amount")


@dataclass(frozen=True, slots=True)
class Domain:
    chain_id: int
    verifying_contract: bytes

    def __post_init__(self) -> None:
        uint64(self.chain_id, field="chain_id", positive=True)
        fixed_bytes(self.verifying_contract, 20, field="verifying_contract", nonzero=True)
