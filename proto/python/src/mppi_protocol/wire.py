"""Bounded wire validation and framing, independent of either SDK role.

Structural validity never proves authority, price acceptance or payment state.
The role runtimes must validate those against their retained evidence.
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import Iterable
from typing import Any, cast

import grpc
from google.protobuf import any_pb2  # type: ignore[import-untyped]
from google.protobuf.message import DecodeError, Message  # type: ignore[import-untyped]
from google.rpc import status_pb2  # type: ignore[import-untyped]
from mppi.v1 import mppi_pb2 as pb

from .catalog import _check_depth, _reject_constant, _unique_object, validate_model
from .errors import ValidationCode, ValidationError
from .types import ChainObservation, PauseLevel, PaymentCode, PaymentFailure, SessionDescriptor

MAX_MESSAGE_BYTES = 16 << 20
MAX_CONTROL_BYTES = 64 << 10
GRPC_OPTIONS = (
    ("grpc.max_receive_message_length", MAX_MESSAGE_BYTES),
    ("grpc.max_send_message_length", MAX_MESSAGE_BYTES),
)


def _invalid(message: str) -> None:
    raise ValidationError(ValidationCode.INVALID_VALUE, message)


def require_fields(message: Any, *names: str) -> None:
    if any(not message.HasField(name) for name in names):
        _invalid("required wire field is absent")


class _JsonNumber(str):
    """Opaque numeric text: validation must not round or coerce JSON numbers."""


def validate_json(
    payload: bytes, *, model: str | None = None, stream: bool | None = None
) -> dict[str, object]:
    if type(payload) is not bytes or len(payload) > MAX_MESSAGE_BYTES:
        raise ValidationError(ValidationCode.LIMIT_EXCEEDED, "JSON payload limit exceeded")
    _check_depth(payload)
    try:
        text = payload.decode("utf-8")
        value = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_int=_JsonNumber,
            parse_float=_JsonNumber,
            parse_constant=_reject_constant,
        )
        if type(value) is not dict:
            _invalid("inference JSON must be an object")
        # Escaped unpaired surrogates survive json.loads; reject all string slots.
        pending = [value]
        while pending:
            item = pending.pop()
            if isinstance(item, str):
                item.encode("utf-8")
            elif isinstance(item, dict):
                pending.extend(item.keys())
                pending.extend(item.values())
            elif isinstance(item, list):
                pending.extend(item)
        if model is not None and (type(value.get("model")) is not str or value["model"] != model):
            _invalid("inference model disagrees with signed envelope")
        if stream is not None:
            flag = value.get("stream", False)
            if type(flag) is not bool or flag != stream:
                _invalid("inference stream flag disagrees with envelope")
        return cast(dict[str, object], value)
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise ValidationError(ValidationCode.INVALID_VALUE, "invalid inference JSON") from None


_OPTIONAL = {
    "SessionOpen": {"agent_id"},
    "ReconnectRequest": {"latest_voucher"},
    "SessionState": {"accepted_voucher", "current_rate_card_hash"},
    "TopUpNotice": {"model", "rate_card_hash", "tokens"},
    "LowCredit": {"remaining_tokens", "suggest_top_up_tokens", "coverage_request"},
    "Drain": {"run_id"},
    "Terminal": {"settlement_reference"},
    "Receipt": {"settlement_reference"},
    "RunState": {"rate_card_hash", "control_generation", "auth_signature"},
    "UpstreamError": {"http_status"},
    "PaymentError": {
        "session_id",
        "run_id",
        "reason",
        "requested_rate_card_hash",
        "current_rate_card_hash",
        "pause_level",
    },
}
_BYTES32 = {
    "session_id",
    "run_id",
    "request_id",
    "nonce",
    "salt",
    "open_tx",
    "tx",
    "tx_hash",
    "rate_card_hash",
    "initial_rate_card_hash",
    "current_rate_card_hash",
    "requested_rate_card_hash",
    "chain_block_hash",
    "block_hash",
    "provider_id",
    "agent_id",
    "acceptance_run_id",
}
_ADDRESSES = {
    "agent",
    "session_signer",
    "recipient",
    "operator",
    "token",
    "contract",
    "contract_address",
}
_SIGNATURES = {"signature", "auth_signature", "acceptance_signature"}


def validate_message(message: Any) -> None:
    """Validate decoded protobuf structure before role-specific interpretation."""
    _validate_message(message)


def validate_recovery_snapshot(message: Any) -> None:
    """Validate provider-local checkpoint structure, never a wire message.

    Generation zero represents authoritative history before first attachment.
    Ordinary wire validation remains positive, including nested SessionState.
    The provider separately verifies private history and canonical chain facts.
    """
    if not isinstance(message, pb.SessionState):
        _invalid("expected a private SessionState checkpoint")
    _validate_message(message, unattached_snapshot=True)
    if message.control_generation == 0 and (
        message.ready
        or message.nonce != bytes(32)
        or message.meter_seq != 0
        or message.settled_cumulative != 0
        or message.HasField("accepted_voucher")
        or message.usage_by_price
        or message.run_states
    ):
        _invalid("unattached checkpoint contains attachment or accounting history")


def _validate_message(message: Any, *, unattached_snapshot: bool = False) -> None:
    if not isinstance(message, Message):
        _invalid("expected a protobuf message")
    name = message.DESCRIPTOR.name
    maximum = MAX_MESSAGE_BYTES
    if name == "ControlMessage" and message.WhichOneof("kind") != "session_state":
        maximum = MAX_CONTROL_BYTES
    if message.ByteSize() > maximum:
        raise ValidationError(ValidationCode.LIMIT_EXCEEDED, "wire message limit exceeded")
    if name in ("ControlMessage", "ChatChunk") and message.WhichOneof("kind") is None:
        _invalid("known wire message variant required")
    optional = _OPTIONAL.get(name, set())
    for field in message.DESCRIPTOR.fields:
        if field.is_repeated:
            values = getattr(message, field.name)
            if field.name == "pending_run_ids":
                if (
                    len(values) > 1024
                    or len(set(values)) != len(values)
                    or any(len(v) != 32 for v in values)
                ):
                    _invalid("invalid pending run ids")
            if field.message_type is not None:
                for value in values:
                    validate_message(value)
            continue
        present = message.HasField(field.name)
        real_oneof = name in ("ControlMessage", "ChatChunk")
        if not present:
            if not real_oneof and field.name not in optional:
                _invalid("required wire field is absent")
            continue
        value = getattr(message, field.name)
        if field.message_type is not None:
            validate_message(value)
        elif field.enum_type is not None:
            if value not in field.enum_type.values_by_number or (
                value == 0 and field.enum_type.name != "PauseLevel"
            ):
                _invalid("invalid wire enum")
        elif field.name in _BYTES32 and len(value) != 32:
            _invalid("wire identifier must be 32 bytes")
        elif field.name in _ADDRESSES and (len(value) != 20 or not any(value)):
            _invalid("wire address must be 20 nonzero bytes")
        elif field.name in _SIGNATURES:
            if len(value) != 65:
                _invalid("wire signature must be 65 bytes")
        elif field.name == "model":
            if not (
                name == "RunState" and message.status == pb.RUN_STATUS_NOT_ADMITTED and value == ""
            ):
                validate_model(value)
        elif (
            field.name
            in (
                "control_generation",
                "acceptance_control_generation",
                "seq",
                "final_meter_seq",
                "chain_id",
            )
            and value == 0
            and not (
                unattached_snapshot
                and name == "SessionState"
                and field.name == "control_generation"
            )
        ):
            _invalid("wire counter must be positive")
    if name == "CompleteRequest":
        validate_json(message.payload, model=message.model, stream=message.stream)
    elif name == "ChatChunk" and message.WhichOneof("kind") == "payload":
        validate_json(message.payload)
    elif name == "UpstreamError":
        validate_json(message.body)
        if message.HasField("http_status") and not 400 <= message.http_status <= 599:
            _invalid("upstream HTTP status out of range")
    elif name == "TopUpNotice":
        present = sum(message.HasField(field) for field in ("model", "rate_card_hash", "tokens"))
        if present not in (0, 3) or (present and message.tokens == 0):
            _invalid("invalid top-up provenance")
    elif name == "LowCredit":
        if message.HasField("remaining_tokens") != message.HasField("suggest_top_up_tokens"):
            _invalid("incomplete funding guidance")
    elif name == "RunState":
        fields = ("rate_card_hash", "control_generation", "auth_signature")
        if message.status == pb.RUN_STATUS_NOT_ADMITTED:
            if (
                any(message.HasField(f) for f in fields)
                or message.model
                or any((message.input_tokens, message.cache_tokens, message.output_tokens))
            ):
                _invalid("invalid not-admitted state")
        else:
            require_fields(message, *fields)
    elif name in ("Terminal", "Receipt") and message.status == pb.RUN_STATUS_NOT_ADMITTED:
        _invalid("definitive record requires admitted outcome")
    elif name == "CoverageRequest":
        keys = [(row.model, row.rate_card_hash) for row in message.required_by_price]
        if not keys or len(keys) != len(set(keys)):
            _invalid("coverage rows must be nonempty and unique")
    elif name == "SessionState":
        for values, key in (
            (message.catalogs, lambda v: v.rate_card_hash),
            (message.usage_by_price, lambda v: (v.model, v.rate_card_hash)),
            (message.run_states, lambda v: v.run_id),
        ):
            keys = [key(value) for value in values]
            if len(keys) != len(set(keys)):
                _invalid("duplicate snapshot evidence")
    if hasattr(message, "cache_tokens") and message.cache_tokens > message.input_tokens:
        _invalid("cached count exceeds input count")


def descriptor_to_wire(value: SessionDescriptor) -> Any:
    return pb.SessionDescriptor(
        agent=value.agent,
        session_signer=value.session_signer,
        provider_id=value.provider_id.to_bytes(32, "big"),
        recipient=value.recipient,
        operator=value.operator,
        token=value.token,
        salt=value.salt,
    )


def descriptor_from_wire(value: Any) -> SessionDescriptor:
    validate_message(value)
    return SessionDescriptor(
        value.agent,
        value.session_signer,
        int.from_bytes(value.provider_id, "big"),
        value.recipient,
        value.operator,
        value.token,
        value.salt,
    )


def observation_to_wire(value: ChainObservation) -> Any:
    return pb.ChainObservation(
        chain_id=value.chain_id,
        contract_address=value.contract_address,
        block_number=value.block_number,
        block_hash=value.block_hash,
    )


def observation_from_wire(value: Any) -> ChainObservation:
    validate_message(value)
    return ChainObservation(
        value.chain_id, value.contract_address, value.block_number, value.block_hash
    )


_ERRORS = {
    PaymentCode.INSUFFICIENT_DEPOSIT: (
        pb.PAYMENT_ERROR_CODE_INSUFFICIENT_DEPOSIT,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    ),
    PaymentCode.SESSION_NOT_FOUND: (
        pb.PAYMENT_ERROR_CODE_SESSION_NOT_FOUND,
        grpc.StatusCode.NOT_FOUND,
    ),
    PaymentCode.VOUCHER_STALE: (
        pb.PAYMENT_ERROR_CODE_VOUCHER_STALE,
        grpc.StatusCode.FAILED_PRECONDITION,
    ),
    PaymentCode.SESSION_NOT_READY: (
        pb.PAYMENT_ERROR_CODE_SESSION_NOT_READY,
        grpc.StatusCode.FAILED_PRECONDITION,
    ),
}


def payment_status(
    failure: PaymentFailure,
) -> tuple[grpc.StatusCode, str, tuple[tuple[str, bytes], ...]]:
    number, status = _ERRORS[failure.code]
    payload = pb.PaymentError(code=number)
    for field in (
        "reason",
        "session_id",
        "run_id",
        "requested_rate_card_hash",
        "current_rate_card_hash",
        "pause_level",
    ):
        value = getattr(failure, field)
        if value is not None:
            setattr(payload, field, value)
    detail = any_pb2.Any()
    detail.Pack(payload)
    message = failure.code.value
    raw = status_pb2.Status(
        code=status.value[0], message=message, details=[detail]
    ).SerializeToString()
    return status, message, (("grpc-status-details-bin", raw),) if len(raw) <= 2048 else ()


def parse_payment_error(
    status: grpc.StatusCode, metadata: Iterable[tuple[str, Any]] | None
) -> PaymentFailure | None:
    entries = [value for key, value in (metadata or ()) if key == "grpc-status-details-bin"]
    if not entries:
        return None
    if len(entries) != 1 or type(entries[0]) is not bytes or len(entries[0]) > 2048:
        _invalid("invalid payment error metadata")
    try:
        carrier = status_pb2.Status.FromString(entries[0])
        if carrier.code != status.value[0] or len(carrier.details) != 1:
            _invalid("payment status carrier mismatch")
        detail = carrier.details[0]
        if detail.type_url != "type.googleapis.com/mppi.v1.PaymentError":
            _invalid("invalid payment error type")
        value = pb.PaymentError.FromString(detail.value)
        validate_message(value)
        code = next((key for key, pair in _ERRORS.items() if pair == (value.code, status)), None)
        if code is None:
            _invalid("payment error status mismatch")
        fields = {
            field: getattr(value, field) if value.HasField(field) else None
            for field in (
                "reason",
                "session_id",
                "run_id",
                "requested_rate_card_hash",
                "current_rate_card_hash",
                "pause_level",
            )
        }
        if fields["pause_level"] is not None:
            fields["pause_level"] = PauseLevel(fields["pause_level"])
        return PaymentFailure(cast(PaymentCode, code), **fields)
    except DecodeError:
        raise ValidationError(
            ValidationCode.INVALID_VALUE, "invalid payment status encoding"
        ) from None


class ByteQueue:
    """Loop-owned queue bounded by both message count and serialized byte size."""

    def __init__(
        self, *, max_messages: int = 64, max_bytes: int = MAX_MESSAGE_BYTES, wait_ms: int = 1000
    ) -> None:
        if (
            type(max_messages) is not int
            or not 0 < max_messages <= 64
            or type(max_bytes) is not int
            or not 0 < max_bytes <= MAX_MESSAGE_BYTES
            or type(wait_ms) is not int
            or not 0 < wait_ms <= ((1 << 63) - 1) // 1_000_000
        ):
            raise ValueError("invalid queue limits")
        self.max_messages, self.max_bytes, self.wait_ms = max_messages, max_bytes, wait_ms
        self._items: deque[bytes] = deque()
        self._size = 0
        self._closed = False
        self._error: Exception | None = None
        self._changed = asyncio.Event()

    def put_nowait(self, data: bytes) -> None:
        if self._closed:
            raise EOFError("queue is closed")
        if type(data) is not bytes or len(data) > self.max_bytes:
            raise ValidationError(ValidationCode.LIMIT_EXCEEDED, "queue message exceeds byte limit")
        if len(self._items) >= self.max_messages or self._size + len(data) > self.max_bytes:
            raise asyncio.QueueFull
        self._items.append(data)
        self._size += len(data)
        self._changed.set()

    async def put(self, data: bytes) -> None:
        deadline = asyncio.get_running_loop().time() + self.wait_ms / 1000
        while True:
            try:
                self.put_nowait(data)
                return
            except asyncio.QueueFull:
                self._changed.clear()
                await asyncio.wait_for(
                    self._changed.wait(), max(0.0, deadline - asyncio.get_running_loop().time())
                )

    async def get(self) -> bytes:
        value = await self.get_or_end()
        if value is None:
            raise EOFError("queue is closed")
        return value

    async def get_or_end(self) -> bytes | None:
        """Return None only for normal closure; retained failures still raise."""
        while not self._items:
            if self._closed:
                if self._error is not None:
                    raise self._error
                return None
            self._changed.clear()
            await self._changed.wait()
        value = self._items.popleft()
        self._size -= len(value)
        self._changed.set()
        return value

    def close(self, error: Exception | None = None) -> None:
        if not self._closed:
            self._closed = True
            self._error = error
            self._changed.set()
