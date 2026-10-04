"""Typed MPPI call preparation using the compiler-generated application ABI.

Preparing a call grants no execution permission. Wallet submission, canonical
receipt verification and live session eligibility are separate steps.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from importlib.resources import files
from typing import Any, cast

from Crypto.Hash import keccak
from eth_abi.abi import decode, encode
from eth_abi.exceptions import DecodingError, EncodingError

from .amounts import uint64
from .capabilities import FundingAuthorization, WalletCall
from .errors import ValidationCode, ValidationError
from .signing import session_id
from .types import Domain, SessionDescriptor, fixed_bytes

ABI: list[dict[str, Any]] = json.loads(
    files("mppi_protocol").joinpath("contract_abi.json").read_text()
)
ARC_USDC = bytes.fromhex("3600000000000000000000000000000000000000")


def _invalid(message: str) -> ValidationError:
    return ValidationError(ValidationCode.INVALID_VALUE, message)


def _hash(data: bytes) -> bytes:
    return keccak.new(digest_bits=256, data=data).digest()


def _abi_type(item: dict[str, Any]) -> str:
    kind = cast(str, item["type"])
    if kind.startswith("tuple"):
        return "(" + ",".join(_abi_type(child) for child in item["components"]) + ")" + kind[5:]
    return kind


def _entry(name: str, kind: str) -> dict[str, Any]:
    matches = [item for item in ABI if item["type"] == kind and item.get("name") == name]
    if len(matches) != 1:
        raise _invalid("unknown or ambiguous MPPI ABI entry")
    return matches[0]


def encode_call(name: str, values: Sequence[object]) -> bytes:
    """Encode an application call; consumers normally use ContractCalls below."""
    entry = _entry(name, "function")
    types = [_abi_type(item) for item in entry["inputs"]]
    selector = _hash((name + "(" + ",".join(types) + ")").encode("ascii"))[:4]
    return selector + encode(types, values)


def decode_result(name: str, data: bytes) -> tuple[Any, ...]:
    types = [_abi_type(item) for item in _entry(name, "function")["outputs"]]
    return _decode_exact(types, data)


def _decode_exact(types: list[str], data: bytes) -> tuple[Any, ...]:
    try:
        values = decode(types, data, strict=True)
        # eth-abi's strict padding validation does not forbid surplus words.
        if encode(types, values) != data:
            raise ValueError("noncanonical ABI")
    except (ValueError, OverflowError, DecodingError, EncodingError) as error:
        raise _invalid("invalid canonical ABI encoding") from error
    return values


def event_topic(name: str) -> bytes:
    entry = _entry(name, "event")
    return _hash(
        (name + "(" + ",".join(_abi_type(item) for item in entry["inputs"]) + ")").encode("ascii")
    )


def decode_event(name: str, topics: tuple[bytes, ...], data: bytes) -> dict[str, Any]:
    entry = _entry(name, "event")
    inputs = entry["inputs"]
    indexed = [item for item in inputs if item["indexed"]]
    ordinary = [item for item in inputs if not item["indexed"]]
    if len(topics) != len(indexed) + 1 or topics[0] != event_topic(name):
        raise _invalid("event signature or indexed field count mismatch")
    values = dict(
        zip(
            (item["name"] for item in ordinary),
            _decode_exact([_abi_type(item) for item in ordinary], data),
            strict=True,
        )
    )
    for item, word in zip(indexed, topics[1:], strict=True):
        values[item["name"]] = _decode_exact(
            [_abi_type(item)], fixed_bytes(word, 32, field="topic")
        )[0]
    return values


def descriptor_values(descriptor: SessionDescriptor) -> tuple[object, ...]:
    return (
        descriptor.agent,
        descriptor.session_signer,
        descriptor.provider_id,
        descriptor.recipient,
        descriptor.operator,
        descriptor.token,
        descriptor.salt,
    )


class OperationKind(StrEnum):
    OPEN = "open"
    TOP_UP = "topUp"
    SETTLE = "settle"
    SETTLE_BATCH = "settleBatch"
    CLOSE = "close"
    REQUEST_CLOSE = "requestClose"
    WITHDRAW = "withdraw"
    RELEASE_IDLE = "releaseIdle"


@dataclass(frozen=True, slots=True)
class PreparedOperation:
    kind: OperationKind
    call: WalletCall
    descriptors: tuple[SessionDescriptor, ...]
    amounts: tuple[int, ...]
    expected_rate_card_hash: bytes | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, OperationKind) or not isinstance(self.call, WalletCall):
            raise _invalid("operation requires a typed kind and wallet call")
        if type(self.descriptors) is not tuple or type(self.amounts) is not tuple:
            raise _invalid("operation descriptors and amounts must be immutable tuples")
        for descriptor in self.descriptors:
            if not isinstance(descriptor, SessionDescriptor) or descriptor.token != ARC_USDC:
                raise _invalid("operation descriptor must use Arc USDC")
        inputs = [_abi_type(item) for item in _entry(self.kind, "function")["inputs"]]
        if len(self.call.data) < 4:
            raise _invalid("operation call data is incomplete")
        values = _decode_exact(inputs, self.call.data[4:])
        if encode_call(self.kind, values) != self.call.data:
            raise _invalid("operation kind differs from encoded call")
        is_batch = self.kind is OperationKind.SETTLE_BATCH
        encoded_descriptors = values[0] if is_batch else (values[0],)
        if not is_batch and len(self.descriptors) != 1:
            raise _invalid("operation requires exactly one descriptor")
        descriptor_type = inputs[0][:-2] if is_batch else inputs[0]
        expected_descriptors = tuple(descriptor_values(item) for item in self.descriptors)
        if encode([descriptor_type + "[]"], [encoded_descriptors]) != encode(
            [descriptor_type + "[]"], [expected_descriptors]
        ):
            raise _invalid("operation descriptors differ from encoded call")
        expected_amounts: tuple[object, ...]
        if self.kind in (OperationKind.OPEN, OperationKind.TOP_UP):
            expected_amounts = (values[1],)
            uint64(values[1], field="funding amount", positive=True)
        elif is_batch:
            expected_amounts = tuple(values[2])
            if any(len(values[index]) != len(self.descriptors) for index in (1, 2, 3)):
                raise _invalid("operation batch lengths differ")
        elif self.kind in (OperationKind.SETTLE, OperationKind.CLOSE):
            expected_amounts = (values[2],)
        else:
            expected_amounts = ()
        for amount in self.amounts:
            uint64(amount, field="operation amount")
        if self.amounts != expected_amounts:
            raise _invalid("operation amounts differ from encoded call")
        expected_hash = values[2] if self.kind is OperationKind.OPEN else None
        if self.expected_rate_card_hash != expected_hash:
            raise _invalid("operation catalog hash differs from encoded call")

    @property
    def session_ids(self) -> tuple[bytes, ...]:
        domain = Domain(self.call.chain_id, self.call.to)
        return tuple(session_id(domain, descriptor) for descriptor in self.descriptors)


@dataclass(frozen=True, slots=True)
class ContractCalls:
    domain: Domain
    token_domain_name: str
    token_domain_version: str

    def __post_init__(self) -> None:
        if not isinstance(self.domain, Domain):
            raise _invalid("contract calls require a checked MPPI domain")
        for value in (self.token_domain_name, self.token_domain_version):
            if type(value) is not str or not value:
                raise _invalid("verified USDC domain name and version are required")

    def _call(
        self,
        kind: OperationKind,
        descriptors: tuple[SessionDescriptor, ...],
        amounts: tuple[int, ...],
        values: Sequence[object],
        expected_hash: bytes | None = None,
    ) -> PreparedOperation:
        for descriptor in descriptors:
            if not isinstance(descriptor, SessionDescriptor) or descriptor.token != ARC_USDC:
                raise _invalid("descriptor must use Arc USDC")
        return PreparedOperation(
            kind,
            WalletCall(
                self.domain.chain_id, self.domain.verifying_contract, encode_call(kind, values)
            ),
            descriptors,
            amounts,
            expected_hash,
        )

    def _authorization(
        self,
        descriptor: SessionDescriptor,
        amount: int,
        authorization: FundingAuthorization,
        signature: bytes,
    ) -> tuple[object, ...]:
        uint64(amount, field="funding amount", positive=True)
        if (
            authorization.chain_id != self.domain.chain_id
            or authorization.token != ARC_USDC
            or authorization.domain_name != self.token_domain_name
            or authorization.domain_version != self.token_domain_version
            or authorization.owner != descriptor.agent
            or authorization.recipient != self.domain.verifying_contract
            or authorization.value != amount
        ):
            raise _invalid("USDC authorization does not match the intended MPPI funding call")
        # The funding wallet can be a smart account. Do not impose MPPI's ECDSA
        # proof format on the token's separately verified authorization bytes.
        if type(signature) is not bytes or not 0 < len(signature) <= 65536:
            raise _invalid("funding signature must be nonempty bounded bytes")
        return (
            authorization.valid_after,
            authorization.valid_before,
            authorization.nonce,
            signature,
        )

    def open(
        self,
        descriptor: SessionDescriptor,
        deposit: int,
        expected_rate_card_hash: bytes,
        authorization: FundingAuthorization,
        signature: bytes,
    ) -> PreparedOperation:
        fixed_bytes(expected_rate_card_hash, 32, field="expected_rate_card_hash")
        auth = self._authorization(descriptor, deposit, authorization, signature)
        return self._call(
            OperationKind.OPEN,
            (descriptor,),
            (deposit,),
            (descriptor_values(descriptor), deposit, expected_rate_card_hash, auth),
            expected_rate_card_hash,
        )

    def top_up(
        self,
        descriptor: SessionDescriptor,
        amount: int,
        authorization: FundingAuthorization,
        signature: bytes,
    ) -> PreparedOperation:
        auth = self._authorization(descriptor, amount, authorization, signature)
        return self._call(
            OperationKind.TOP_UP,
            (descriptor,),
            (amount,),
            (descriptor_values(descriptor), amount, auth),
        )

    def _capture(
        self,
        kind: OperationKind,
        descriptor: SessionDescriptor,
        cumulative_amount: int,
        capture_amount: int,
        signature: bytes,
    ) -> PreparedOperation:
        uint64(cumulative_amount, field="cumulative_amount")
        uint64(capture_amount, field="capture_amount")
        allowed_lengths = (0, 65) if kind is OperationKind.CLOSE else (65,)
        if (
            capture_amount > cumulative_amount
            or type(signature) is not bytes
            or len(signature) not in allowed_lengths
        ):
            raise _invalid(
                "capture must be covered and the voucher proof must be empty or 65 bytes"
            )
        # Empty signatures are valid for eligible refund-only close. Settlement
        # always verifies the voucher, including an equal capture/no-op.
        return self._call(
            kind,
            (descriptor,),
            (capture_amount,),
            (descriptor_values(descriptor), cumulative_amount, capture_amount, signature),
        )

    def settle(
        self,
        descriptor: SessionDescriptor,
        cumulative_amount: int,
        capture_amount: int,
        signature: bytes,
    ) -> PreparedOperation:
        return self._capture(
            OperationKind.SETTLE, descriptor, cumulative_amount, capture_amount, signature
        )

    def close(
        self,
        descriptor: SessionDescriptor,
        cumulative_amount: int,
        capture_amount: int,
        signature: bytes,
    ) -> PreparedOperation:
        return self._capture(
            OperationKind.CLOSE, descriptor, cumulative_amount, capture_amount, signature
        )

    def settle_batch(
        self, items: Sequence[tuple[SessionDescriptor, int, int, bytes]]
    ) -> PreparedOperation:
        calls = tuple(self.settle(*item) for item in items)
        descriptors = tuple(item[0] for item in items)
        return self._call(
            OperationKind.SETTLE_BATCH,
            descriptors,
            tuple(call.amounts[0] for call in calls),
            (
                tuple(descriptor_values(d) for d in descriptors),
                tuple(item[1] for item in items),
                tuple(item[2] for item in items),
                tuple(item[3] for item in items),
            ),
        )

    def request_close(self, descriptor: SessionDescriptor) -> PreparedOperation:
        return self._call(
            OperationKind.REQUEST_CLOSE, (descriptor,), (), (descriptor_values(descriptor),)
        )

    def withdraw(self, descriptor: SessionDescriptor) -> PreparedOperation:
        return self._call(
            OperationKind.WITHDRAW, (descriptor,), (), (descriptor_values(descriptor),)
        )

    def release_idle(self, descriptor: SessionDescriptor) -> PreparedOperation:
        return self._call(
            OperationKind.RELEASE_IDLE, (descriptor,), (), (descriptor_values(descriptor),)
        )
