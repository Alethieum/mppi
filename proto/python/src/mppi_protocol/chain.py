"""Read-only Arc evidence and reconciliation of previously submitted MPPI calls.

RPC consistency is checked, not cryptographically attested. Select a trusted RPC
and verified deployment manifest. This module does not implement a chain follower,
provider storage, transaction resubmission or session readiness by itself.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Protocol, TypeVar, cast
from uuid import uuid4

from eth_abi.abi import decode, encode

from .amounts import uint64
from .capabilities import FundingAuthorization
from .catalog import Catalog, parse_catalog
from .contract import (
    OperationKind,
    PreparedOperation,
    _decode_exact,
    _hash,
    decode_event,
    decode_result,
    encode_call,
    error_name,
    event_topic,
)
from .discovery import ProviderIdentity, _parse_document, _parse_json, address_hex, parse_address
from .errors import ValidationCode, ValidationError
from .http import MAX_RPC_RESPONSE_BYTES, HttpTransport, TransportError
from .signing import funding_digest, funding_signer, voucher_digest
from .signing import session_id as descriptor_id
from .types import (
    ChainObservation,
    ConfirmedSettlementRef,
    Domain,
    OperationResult,
    OperationState,
    PauseLevel,
    SessionDescriptor,
    SessionOperationOutcome,
    SessionStatus,
    fixed_bytes,
    uint256,
)

_IMPLEMENTATION_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
# ERC-1271 isValidSignature(bytes32,bytes) and the word USDC's SignatureChecker
# requires back: bytes32(IERC1271.isValidSignature.selector).
_IS_VALID_SIGNATURE = bytes.fromhex("1626ba7e")
_ERC1271_MAGIC = _IS_VALID_SIGNATURE + bytes(28)
# Multicall3 (github.com/mds1/multicall3, src/Multicall3.sol) at its canonical
# address. It has no proxy, so its code never changes; this runtime Keccak-256
# was read at that address on Arc testnet, Arc mainnet and Ethereum mainnet in
# October 2026. Different or absent code falls back to batched reads.
_MULTICALL3 = bytes.fromhex("ca11bde05977b3631167028862be2a173976ca11")
_MULTICALL3_CODE_HASH = bytes.fromhex(
    "d5c15df687b16f2ff992fc8d767b4216323184a2bbc6ee2f9c398c318e770891"
)
_AGGREGATE3 = _hash(b"aggregate3((address,bool,bytes)[])")[:4]
_HEAD_CALLS = tuple(
    _hash(name)[:4]
    for name in (b"getBlockNumber()", b"getCurrentBlockTimestamp()", b"getLastBlockHash()")
)
# Arc commits each block with BFT finality: "Every transaction in a committed
# block is immediately and irreversibly settled", with "no reorganization
# risk" (docs.arc.io/arc/concepts/deterministic-finality, read October 2026).
_INSTANT_FINALITY = frozenset({5042, 5042002})
# ERC-4337 EntryPoint v0.7 at its canonical address, and its event for a
# UserOperation whose execution reverted (eth-infinitism account-abstraction
# v0.7.0 contracts/interfaces/IEntryPoint.sol:60-65).
_ENTRYPOINT_V07 = bytes.fromhex("0000000071727de22e5e9d8baf0edac6f37da032")
_USER_OPERATION_REVERT = _hash(b"UserOperationRevertReason(bytes32,address,uint256,bytes)")
_ERROR_STRING = _hash(b"Error(string)")[:4]
# USDC's own refusals of a funding transfer (verified FiatTokenV2_2 sources:
# EIP3009.sol, FiatTokenV1.sol, Pausable.sol, Blacklistable.sol). A cause
# names one of these or an MPPI error, never other text an RPC returned.
_TOKEN_REASONS = frozenset(
    {
        "FiatTokenV2: caller must be the payee",
        "FiatTokenV2: invalid signature",
        "FiatTokenV2: authorization is used or canceled",
        "FiatTokenV2: authorization is not yet valid",
        "FiatTokenV2: authorization is expired",
        "ERC20: transfer amount exceeds balance",
        "Pausable: paused",
        "Blacklistable: account is blacklisted",
    }
)
# These operations are proved by their own event alone on a final network;
# a settlement's no-op or skipped item emits nothing, so it reads state.
_STATEFUL = frozenset({OperationKind.SETTLE, OperationKind.SETTLE_BATCH})


class ChainEvidenceError(RuntimeError):
    """Missing or inconsistent chain evidence; payment authorization must stop."""


class ChainContradictionError(ChainEvidenceError):
    """Evidence contradicting the pinned deployment or evidence already
    accepted: another chain, deployed code, implementation or signing domain,
    or a changed or regressed block or session. Unlike an unavailable or
    lagging read, nothing about the RPC's availability explains it.
    """


class ChainLagError(ChainEvidenceError):
    """The RPC served a head or block behind evidence already accepted.

    A load-balanced RPC may route reads to backends at slightly different
    heights (docs.arc.io/arc/references/rpc-endpoints). This is lag, not a
    regression: the accepted evidence stays valid and simply ages.
    """


class _RateLimited(ChainEvidenceError):
    """A JSON-RPC rate-limit or not-yet-imported error; a later read may succeed."""


# A transient read failure is repeated up to twice, 250 ms and then 1 s later:
# rate limits, dropped connections and slow backends are routine on shared RPC
# plans. Repeats start within one transport timeout plus those delays of the
# first attempt, so a timed-out attempt is repeated once.
_READ_RETRY_DELAYS = (0.25, 1.0)
# Each attempt is bounded at 5 s by default. A client allows each step 10 s by
# default, so after a timed-out attempt and the 250 ms delay its repeat has
# 4.75 s left.
_ATTEMPT_MS = 5_000
# EIP-1474 -32005 "Limit exceeded" (https://eips.ethereum.org/EIPS/eip-1474);
# QuickNode's -32007 per-second request limit on EVM endpoints
# (https://www.quicknode.com/docs/berachain/error-references); Arc's -32014 from
# a load-balanced backend that "hasn't yet imported that block", which Arc says
# can be retried after a brief backoff (docs.arc.io/arc/references/rpc-endpoints).
_RATE_LIMIT_CODES = frozenset({-32005, -32007, -32014})
# At most this many calls go in one JSON-RPC batch: some plans count each call
# in a batch against a per-second request limit and refuse entries beyond it.
_BATCH_CALLS = 32
_READ_ONLY = frozenset(
    {
        "eth_chainId",
        "eth_getBlockByNumber",
        "eth_getTransactionReceipt",
        "eth_getCode",
        "eth_getStorageAt",
        "eth_call",
        "eth_estimateGas",
    }
)
_T = TypeVar("_T")


class Rpc(Protocol):
    """A JSON-RPC reader. ``call_many(calls)``, returning results in order, is
    optional: ChainReader sends a step's reads through it when present.
    An adapter that raises for a reverted call stays correct; funding checks
    through it then report missing evidence rather than a refused signature.
    Without ``call_many`` a step's reads go one call at a time; a snapshot
    then anchors to the block its views ran at."""

    def call(self, method: str, params: list[object]) -> object: ...


@dataclass(frozen=True, slots=True)
class HttpJsonRpc:
    endpoint: str = field(repr=False)
    transport: HttpTransport = field(
        default_factory=lambda: HttpTransport(
            timeout_ms=_ATTEMPT_MS, max_bytes=MAX_RPC_RESPONSE_BYTES
        )
    )

    # Cleared once the endpoint answers a batch with one object, as a server
    # without batches does (JSON-RPC 2.0 section 6), or refuses a batch request
    # (an HTTP 400, 405, 413, 415 or 501, say) and then answers a single call:
    # then one call at a time.
    _batches: bool = field(default=True, init=False, repr=False, compare=False)

    def call(self, method: str, params: list[object]) -> object:
        # This adapter only reads. It cannot turn reconciliation into a submit,
        # so repeating a read after a transient failure never submits anything.
        _read_only(method)
        return self._retried(lambda: self._once(method, params))

    def call_many(self, calls: Sequence[tuple[str, list[object]]]) -> list[object]:
        """Results of read-only calls, in order, sent as JSON-RPC batches.

        A batch's responses may arrive in any order and are matched by id
        (JSON-RPC 2.0 section 6); each call must be answered exactly once.
        """
        for method, _ in calls:
            _read_only(method)
        results: list[object] = []
        for start in range(0, len(calls), _BATCH_CALLS):
            results.extend(
                self._retried(partial(self._batch, list(calls[start : start + _BATCH_CALLS])))
            )
        return results

    def close(self) -> None:
        """Close idle kept-alive connections; later reads open new ones."""
        self.transport.close()

    def _retried(self, read: Callable[[], _T]) -> _T:
        budget = (
            time.monotonic()
            + getattr(self.transport, "timeout_ms", _ATTEMPT_MS) / 1000
            + sum(_READ_RETRY_DELAYS)
        )
        for delay in _READ_RETRY_DELAYS:
            try:
                return read()
            except TransportError as error:
                if not error.transient or time.monotonic() + delay >= budget:
                    raise
            except _RateLimited:
                if time.monotonic() + delay >= budget:
                    raise
            time.sleep(delay)
        return read()

    def _once(self, method: str, params: list[object]) -> object:
        request_id = str(uuid4())
        payload = json.dumps(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
            separators=(",", ":"),
        ).encode()
        response = _parse_document(
            self.transport.request(self.endpoint, body=payload), max_bytes=MAX_RPC_RESPONSE_BYTES
        )
        return _answer(response, request_id, method)

    def _batch(self, calls: list[tuple[str, list[object]]]) -> list[object]:
        if not self._batches or len(calls) == 1:
            return [self._once(method, params) for method, params in calls]
        ids = [str(uuid4()) for _ in calls]
        payload = json.dumps(
            [
                {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
                for request_id, (method, params) in zip(ids, calls, strict=True)
            ],
            separators=(",", ":"),
        ).encode()
        try:
            raw = self.transport.request(self.endpoint, body=payload)
        except TransportError as refused:
            if refused.transient:
                raise
            # A refused single call raises its own error and keeps batches on.
            first = self._once(*calls[0])
            object.__setattr__(self, "_batches", False)
            return [first] + [self._once(method, params) for method, params in calls[1:]]
        response = _parse_json(raw, max_bytes=MAX_RPC_RESPONSE_BYTES)
        if isinstance(response, dict):
            error = response.get("error")
            if isinstance(error, dict) and error.get("code") in _RATE_LIMIT_CODES:
                raise _RateLimited("RPC request was rate limited")
            object.__setattr__(self, "_batches", False)
            return [self._once(method, params) for method, params in calls]
        if not isinstance(response, list) or len(response) != len(calls):
            raise ChainEvidenceError("RPC batch response count mismatch")
        rows: dict[str, dict[str, Any]] = {}
        for row in response:
            if not isinstance(row, dict) or type(row.get("id")) is not str or row["id"] in rows:
                raise ChainEvidenceError("RPC response identity mismatch")
            rows[row["id"]] = row
        if rows.keys() != set(ids):
            raise ChainEvidenceError("RPC response identity mismatch")
        return [
            _answer(rows[request_id], request_id, method)
            for request_id, (method, _) in zip(ids, calls, strict=True)
        ]


def _session(raw: object) -> ChainSession:
    deposit, settled, status, closing, activity, initial_hash = decode_result(
        "getSession", raw if isinstance(raw, bytes) else _data(raw)
    )[0]
    if (
        status not in (0, 1, 2)
        or settled > deposit
        or (deposit == 0 and any((settled, status, closing, activity, initial_hash != bytes(32))))
    ):
        raise ChainEvidenceError("inconsistent MPPI session state")
    return ChainSession(
        deposit,
        settled,
        (SessionStatus.OPEN, SessionStatus.CLOSING, SessionStatus.CLOSED)[status],
        closing,
        activity,
        initial_hash,
    )


def _pause(raw: object) -> PauseLevel:
    result = decode_result("pauseLevel", raw if isinstance(raw, bytes) else _data(raw))[0]
    if result not in (0, 1, 2):
        raise ChainEvidenceError("invalid MPPI pause level")
    return PauseLevel(result)


def _read_only(method: str) -> None:
    if method not in _READ_ONLY:
        raise ChainEvidenceError("unsupported read-only RPC method")


class _Reverted:
    """An eth_call the node executed and found reverting, in that call's place.

    Execution clients answer a reverted eth_call with JSON-RPC error code 3;
    Arc's endpoints do. It carries no data: every reader but the funding check
    refuses it as invalid RPC bytes.
    """

    __slots__ = ()


_REVERTED = _Reverted()


def _answer(response: dict[str, Any], request_id: str, method: str) -> object:
    if response.get("jsonrpc") != "2.0" or response.get("id") != request_id:
        raise ChainEvidenceError("RPC response identity mismatch")
    error = response.get("error")
    if isinstance(error, dict) and error.get("code") in _RATE_LIMIT_CODES:
        raise _RateLimited("RPC request was rate limited")
    if (
        method == "eth_call"
        and isinstance(error, dict)
        and error.get("code") == 3
        and "result" not in response
    ):
        return _REVERTED
    if "error" in response or "result" not in response:
        # No remote error data: it may contain private endpoint parameters.
        raise ChainEvidenceError("RPC request returned no usable result")
    return response["result"]


def _object(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ChainEvidenceError("RPC object missing")
    return cast(dict[str, Any], value)


def _quantity(value: object) -> int:
    if (
        type(value) is not str
        or re.fullmatch(r"0x(?:0|[1-9a-f][0-9a-f]*)", value) is None
        or len(value) > 66
    ):
        raise ChainEvidenceError("invalid RPC quantity")
    return int(value, 16)


def _data(value: object, length: int | None = None) -> bytes:
    if type(value) is not str or re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", value) is None:
        raise ChainEvidenceError("invalid RPC bytes")
    data = bytes.fromhex(value[2:])
    if length is not None and len(data) != length:
        raise ChainEvidenceError("invalid RPC byte width")
    return data


@dataclass(frozen=True, slots=True)
class CodePin:
    address: bytes
    code_hash: bytes

    def __post_init__(self) -> None:
        fixed_bytes(self.address, 20, field="code address", nonzero=True)
        fixed_bytes(self.code_hash, 32, field="code hash", nonzero=True)


@dataclass(frozen=True, slots=True)
class DeploymentProfile:
    domain: Domain
    registry_address: bytes
    proxy: CodePin
    implementation: CodePin
    registry: CodePin
    registry_implementation: CodePin

    def __post_init__(self) -> None:
        if not isinstance(self.domain, Domain) or self.domain.chain_id not in (5042, 5042002):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "MPPI requires an Arc network profile"
            )
        fixed_bytes(self.registry_address, 20, field="registry_address", nonzero=True)
        if (
            self.proxy.address != self.domain.verifying_contract
            or self.registry.address != self.registry_address
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "profile addresses and code pins differ"
            )

    @property
    def instant_finality(self) -> bool:
        """A committed block is final: no earlier block needs re-reading.

        True for both Arc networks; a network with probabilistic finality
        would keep the canonical re-reads and confirmation checks.
        """
        return self.domain.chain_id in _INSTANT_FINALITY


@dataclass(frozen=True, slots=True)
class Block:
    observation: ChainObservation
    timestamp: int
    parent_hash: bytes


@dataclass(frozen=True, slots=True)
class ChainSession:
    deposit: int
    settled_cumulative: int
    status: SessionStatus
    close_requested_at: int
    last_activity: int
    initial_rate_card_hash: bytes

    @property
    def exists(self) -> bool:
        return self.deposit > 0


@dataclass(frozen=True, slots=True)
class OpeningEvidence:
    descriptor: SessionDescriptor
    observation: ChainObservation
    deposit: int
    initial_catalog: Catalog
    tx_hash: bytes


@dataclass(frozen=True, slots=True)
class ClosingEvidence:
    observation: ChainObservation
    capture_amount: int
    refunded: int
    tx_hash: bytes


@dataclass(frozen=True, slots=True)
class FundingEvidence:
    """At ``block``: whether USDC accepts the funding signature, and the
    funding wallet the registry binds to the agent id (zero when none)."""

    block: Block
    signature_valid: bool
    agent_wallet: bytes


@dataclass(frozen=True, slots=True)
class SimulationEvidence:
    """Advisory execution/cost evidence at one canonical block, never a receipt."""

    block: Block
    gas_estimate: int
    gas_limit: int


class ChainReader:
    """Synchronous, block-pinned reads; use outside the payment event loop.

    Each step reads everything it needs at one block in one batch when the RPC
    offers ``call_many``. Every state read is pinned with EIP-1898
    ``{blockHash, requireCanonical: true}``, and the same batch reads that
    block by number: requireCanonical is only a SHOULD, so the matching hash
    is what shows the block was canonical while the batch was served. Facts
    that cannot change for one reader are read once: the RPC's chain id, and
    the code at each pinned address (EIP-6780 removes code only in the
    transaction that created it). Proxy implementation slots are mutable and
    read every time.
    """

    def __init__(self, rpc: Rpc, profile: DeploymentProfile) -> None:
        self.rpc = rpc
        self.profile = profile
        self._chain_checked = False
        self._code_checked = False
        self._domain_checked = False
        # None until Multicall3's code is checked once; False falls back to batches.
        self._multicall: bool | None = None

    def _many(self, calls: list[tuple[str, list[object]]]) -> list[object]:
        # Look on the type: an object answering any attribute, such as a proxy,
        # is read one call at a time rather than trusted to batch.
        if callable(getattr(type(self.rpc), "call_many", None)):
            results = list(self.rpc.call_many(calls))  # type: ignore[attr-defined]
            if len(results) != len(calls):
                raise ChainEvidenceError("RPC batch response count mismatch")
            return results
        return [self.rpc.call(method, params) for method, params in calls]

    def _network_calls(self) -> list[tuple[str, list[object]]]:
        return [] if self._chain_checked else [("eth_chainId", [])]

    def _check_network(self, results: list[object]) -> list[object]:
        """Check the chain id a batch began with, if it did; return the rest."""
        if self._chain_checked:
            return results
        if _quantity(results[0]) != self.profile.domain.chain_id:
            raise ChainContradictionError("RPC chain differs from the selected deployment")
        self._chain_checked = True
        return results[1:]

    def _network(self) -> None:
        self._check_network(self._many(self._network_calls()))

    def block(self, number: int | None = None) -> Block:
        return self._blocks((number,))[0]

    def _blocks(self, numbers: tuple[int | None, ...]) -> list[Block]:
        """The head (``None``) or blocks by number, read in one batch."""
        for number in numbers:
            if number is not None:
                uint64(number, field="block number")
        results = self._check_network(
            self._many(
                self._network_calls()
                + [
                    ("eth_getBlockByNumber", ["latest" if number is None else hex(number), False])
                    for number in numbers
                ]
            )
        )
        return [
            self._parse_block(raw, number) for raw, number in zip(results, numbers, strict=True)
        ]

    def _parse_block(self, result: object, number: int | None) -> Block:
        if result is None and number is not None:
            raise ChainLagError("RPC has not served that block yet")
        raw = _object(result)
        actual = _quantity(raw.get("number"))
        if number is not None and actual != number:
            raise ChainEvidenceError("canonical block number mismatch")
        timestamp = uint64(_quantity(raw.get("timestamp")), field="block timestamp")
        return Block(
            ChainObservation(
                self.profile.domain.chain_id,
                self.profile.domain.verifying_contract,
                actual,
                _data(raw.get("hash"), 32),
            ),
            timestamp,
            _data(raw.get("parentHash"), 32),
        )

    def _position(self, block: Block) -> dict[str, object]:
        observation = block.observation
        if (
            observation.chain_id != self.profile.domain.chain_id
            or observation.contract_address != self.profile.domain.verifying_contract
        ):
            raise ChainEvidenceError("block observation differs from selected deployment")
        return {"blockHash": "0x" + observation.block_hash.hex(), "requireCanonical": True}

    def _call(self, address: bytes, data: bytes, block: Block) -> bytes:
        result = self.rpc.call(
            "eth_call",
            [{"to": address_hex(address), "data": "0x" + data.hex()}, self._position(block)],
        )
        return _data(result)

    def _read(self, block: Block, calls: list[tuple[str, list[object]]]) -> list[object]:
        """State reads pinned to ``block``, with that block re-read in the same batch."""
        self._position(block)
        found, results = self._read_at(
            block.observation.block_number, block.observation.block_hash, calls
        )
        if found != block:
            raise ChainEvidenceError("canonical block evidence changed during observation")
        return results

    def _read_at(
        self,
        number: int,
        block_hash: bytes,
        calls: list[tuple[str, list[object]]],
        *,
        recheck: int | None = None,
    ) -> tuple[Block, list[object]]:
        """Block ``number`` and state reads pinned to ``block_hash``, in one batch.

        With ``recheck``, that earlier block is read by number in the same batch
        and returned first among the results, as a ``Block``.
        """
        position = {"blockHash": "0x" + block_hash.hex(), "requireCanonical": True}
        head: list[tuple[str, list[object]]] = [("eth_getBlockByNumber", [hex(number), False])]
        if recheck is not None:
            uint64(recheck, field="block number")
            head.append(("eth_getBlockByNumber", [hex(recheck), False]))
        results = self._check_network(
            self._many(
                self._network_calls()
                + head
                + [(method, [*params, position]) for method, params in calls]
            )
        )
        block = self._parse_block(results[0], number)
        if block.observation.block_hash != block_hash:
            raise ChainEvidenceError("canonical block evidence changed during observation")
        if recheck is None:
            return block, results[1:]
        return block, [self._parse_block(results[1], recheck), *results[2:]]

    def _call_params(self, address: bytes, data: bytes) -> tuple[str, list[object]]:
        return ("eth_call", [{"to": address_hex(address), "data": "0x" + data.hex()}])

    def _pins(self) -> tuple[CodePin, ...]:
        return (
            self.profile.proxy,
            self.profile.implementation,
            self.profile.registry,
            self.profile.registry_implementation,
        )

    def _deployment_calls(self, with_code: bool) -> list[tuple[str, list[object]]]:
        calls: list[tuple[str, list[object]]] = []
        if with_code:
            calls += [("eth_getCode", [address_hex(pin.address)]) for pin in self._pins()]
        calls += [
            ("eth_getStorageAt", [address_hex(proxy.address), _IMPLEMENTATION_SLOT])
            for proxy in (self.profile.proxy, self.profile.registry)
        ]
        calls.append(
            self._call_params(
                self.profile.domain.verifying_contract, encode_call("voucherDigest", (bytes(32), 0))
            )
        )
        return calls

    def _check_deployment(self, results: list[object], with_code: bool) -> list[object]:
        """Check the results of ``_deployment_calls``; return what follows them."""
        if with_code:
            for pin, raw in zip(self._pins(), results[:4], strict=True):
                code = _data(raw)
                if not code or _hash(code) != pin.code_hash:
                    raise ChainContradictionError("deployed code differs from release profile")
            results = results[4:]
            self._code_checked = True
        for implementation, raw in zip(
            (self.profile.implementation, self.profile.registry_implementation),
            results[:2],
            strict=True,
        ):
            if _data(raw, 32) != b"\0" * 12 + implementation.address:
                raise ChainContradictionError("proxy implementation differs from release profile")
        digest = decode_result("voucherDigest", _data(results[2]))[0]
        if digest != voucher_digest(self.profile.domain, bytes(32), 0):
            raise ChainContradictionError("MPPI signing domain differs from release profile")
        return results[3:]

    def latest(
        self, *, providers: tuple[int, ...] = (), sessions: tuple[bytes, ...] = ()
    ) -> tuple[Block, PauseLevel, dict[int, ProviderIdentity], dict[bytes, ChainSession]]:
        """The deployment, pause level, provider identities (an unusable
        catalog read as ``None``) and sessions at the latest block, in one
        round trip on a network whose committed blocks are final.

        One Multicall3 ``aggregate3`` eth_call reads every contract view at the
        head it runs on. Its block number, time and parent hash must equal the
        header read in the same batch, which anchors the snapshot to that
        block; views that ran at another head are anchored to that block's
        header, read by number in one more round trip. The implementation
        slots, which no contract exposes, are read in the same batch. Without verified Multicall3 code, or on a network
        without instant finality, this is the head and then one batch pinned
        to it: two round trips.
        """
        if not self.profile.instant_finality or self._multicall is False:
            (head,) = self._blocks((None,))
            found, _, pause, identities, states = self._observe_at(
                head.observation.block_number,
                head.observation.block_hash,
                providers=providers,
                sessions=sessions,
            )
            return found, pause, identities, states
        views: list[tuple[bytes, bytes]] = [(_MULTICALL3, call) for call in _HEAD_CALLS]
        domain = not self._domain_checked
        proxy = self.profile.domain.verifying_contract
        if domain:
            views.append((proxy, encode_call("voucherDigest", (bytes(32), 0))))
        views.append((proxy, encode_call("pauseLevel", ())))
        for provider_id in providers:
            views += [
                (self.profile.registry_address, data) for data in self._identity_data(provider_id)
            ]
        views += [
            (proxy, encode_call("getSession", (fixed_bytes(sid, 32, field="session_id"),)))
            for sid in sessions
        ]
        aggregate = _AGGREGATE3 + encode(
            ["(address,bool,bytes)[]"], [[(to, False, data) for to, data in views]]
        )
        code = not self._code_checked
        unchecked = self._multicall is None
        calls: list[tuple[str, list[object]]] = self._network_calls()
        if code:
            calls += [("eth_getCode", [address_hex(pin.address), "latest"]) for pin in self._pins()]
        if unchecked:
            calls.append(("eth_getCode", [address_hex(_MULTICALL3), "latest"]))
        calls += [
            ("eth_getBlockByNumber", ["latest", False]),
            (
                "eth_call",
                [{"to": address_hex(_MULTICALL3), "data": "0x" + aggregate.hex()}, "latest"],
            ),
        ]
        calls += [
            ("eth_getStorageAt", [address_hex(pin.address), _IMPLEMENTATION_SLOT, "latest"])
            for pin in (self.profile.proxy, self.profile.registry)
        ]
        results = self._check_network(self._many(calls))
        if code:
            for pin, raw in zip(self._pins(), results[:4], strict=True):
                found_code = _data(raw)
                if not found_code or _hash(found_code) != pin.code_hash:
                    raise ChainContradictionError("deployed code differs from release profile")
            results = results[4:]
            self._code_checked = True
        if unchecked:
            self._multicall = _hash(_data(results[0])) == _MULTICALL3_CODE_HASH
            results = results[1:]
            if not self._multicall:
                return self.latest(providers=providers, sessions=sessions)
        head = self._parse_block(results[0], None)
        answers = decode(["(bool,bytes)[]"], _data(results[1]), strict=True)[0]
        if len(answers) != len(views) or not all(ok for ok, _ in answers):
            raise ChainEvidenceError("RPC snapshot is incomplete")
        values: list[object] = ["0x" + bytes(data).hex() for _, data in answers]
        number, timestamp, parent = (
            _decode_exact([kind], _data(raw))[0]
            for kind, raw in zip(("uint256", "uint256", "bytes32"), values[:3], strict=True)
        )
        if number != head.observation.block_number:
            # Sent one call at a time, the views can run at a later head than the
            # header read before them: anchor to the header of their own block.
            (head,) = self._blocks((number,))
        if (number, timestamp, parent) != (
            head.observation.block_number,
            head.timestamp,
            head.parent_hash,
        ):
            raise ChainEvidenceError("RPC snapshot and head are from different blocks")
        for implementation, raw in zip(
            (self.profile.implementation, self.profile.registry_implementation),
            results[2:4],
            strict=True,
        ):
            if _data(raw, 32) != b"\0" * 12 + implementation.address:
                raise ChainContradictionError("proxy implementation differs from release profile")
        values = values[3:]
        if domain:
            digest = decode_result("voucherDigest", _data(values[0]))[0]
            if digest != voucher_digest(self.profile.domain, bytes(32), 0):
                raise ChainContradictionError("MPPI signing domain differs from release profile")
            self._domain_checked = True
            values = values[1:]
        pause = _pause(values[0])
        values = values[1:]
        identities = {}
        for provider_id in providers:
            identities[provider_id] = self._parse_identity(provider_id, head, values[:4], True)
            values = values[4:]
        states = {sid: _session(raw) for sid, raw in zip(sessions, values, strict=True)}
        return head, pause, identities, states

    def verify_deployment(self, block: Block) -> None:
        """Compare selected manifest code/implementations and MPPI EIP-712 domain.

        Governance/initializer manifest assertions are deployment release checks;
        these reads cannot manufacture a trusted manifest from an unknown RPC.
        """
        with_code = not self._code_checked
        self._check_deployment(self._read(block, self._deployment_calls(with_code)), with_code)

    def _check_canonical(self, block: Block) -> None:
        if self.block(block.observation.block_number) != block:
            raise ChainEvidenceError("canonical block evidence changed during observation")

    def session(self, session_id: bytes, block: Block) -> ChainSession:
        method, params = self._session_call(session_id)
        return _session(self.rpc.call(method, [*params, self._position(block)]))

    def pause_level(self, block: Block) -> PauseLevel:
        return _pause(
            self._call(self.profile.domain.verifying_contract, encode_call("pauseLevel", ()), block)
        )

    def _identity_calls(self, provider_id: int) -> list[tuple[str, list[object]]]:
        return [
            self._call_params(self.profile.registry_address, data)
            for data in self._identity_data(provider_id)
        ]

    def _identity_data(self, provider_id: int) -> list[bytes]:
        """Registry calldata for a provider's operator, recipient, URI and catalog."""
        uint256(provider_id, field="provider_id")
        return [
            _hash((name + "(" + ",".join(inputs) + ")").encode())[:4] + encode(inputs, values)
            for name, inputs, values in (
                ("ownerOf", ["uint256"], [provider_id]),
                ("getAgentWallet", ["uint256"], [provider_id]),
                ("tokenURI", ["uint256"], [provider_id]),
                ("getMetadata", ["uint256", "string"], [provider_id, "mppi.catalog"]),
            )
        ]

    def _parse_identity(
        self, provider_id: int, block: Block, results: list[object], allow_unavailable: bool
    ) -> ProviderIdentity:
        owner, recipient, uri, raw_catalog = (
            _decode_exact([output], _data(raw))[0]
            for output, raw in zip(("address", "address", "string", "bytes"), results, strict=True)
        )
        try:
            catalog: Catalog | None = parse_catalog(raw_catalog)
        except ValidationError:
            if not allow_unavailable:
                raise
            # Unusable offering is distinct from unavailable RPC/identity state.
            # Historical accounting and eligible exits must remain observable.
            catalog = None
        return ProviderIdentity(
            block.observation,
            self.profile.registry_address,
            provider_id,
            parse_address(owner),
            parse_address(recipient),
            uri,
            catalog,
        )

    def identity(
        self, provider_id: int, block: Block, *, allow_unavailable_catalog: bool = False
    ) -> ProviderIdentity:
        results = self._read(block, self._identity_calls(provider_id))
        return self._parse_identity(provider_id, block, results, allow_unavailable_catalog)

    def agent_wallet(self, agent_id: int, observation: ChainObservation) -> bytes:
        """Registry identity binds the funding wallet, never its session signer;
        read at the block another proof named, in one round trip."""
        uint256(agent_id, field="agent_id")
        data = _hash(b"getAgentWallet(uint256)")[:4] + encode(["uint256"], [agent_id])
        _, (raw,) = self._read_at(
            observation.block_number,
            observation.block_hash,
            [self._call_params(self.profile.registry_address, data)],
        )
        return parse_address(_decode_exact(["address"], _data(raw))[0])

    def funding(
        self,
        authorization: FundingAuthorization,
        signature: bytes,
        agent_id: int,
        block: Block,
    ) -> FundingEvidence:
        """Whether USDC accepts ``signature`` for ``authorization`` at ``block``,
        and the funding wallet the registry binds to ``agent_id``, in one batch.

        USDC decides as SignatureChecker.isValidSignatureNow: an owner without
        code must recover from the digest through ECRecover; one with code must
        answer isValidSignature with the ERC-1271 magic word, and a reverted
        call is a refusal, as its failed staticcall is. USDC repeats the check
        when funding executes; this one only refuses before submission.
        """
        uint256(agent_id, field="agent_id")
        if (
            not isinstance(authorization, FundingAuthorization)
            or authorization.chain_id != self.profile.domain.chain_id
            or type(signature) is not bytes
            or not 0 < len(signature) <= 65536
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "funding check differs from selected deployment"
            )
        owner = authorization.owner
        bound = _hash(b"getAgentWallet(uint256)")[:4] + encode(["uint256"], [agent_id])
        check = _IS_VALID_SIGNATURE + encode(
            ["bytes32", "bytes"], [funding_digest(authorization), signature]
        )
        code, wallet, answer = self._read(
            block,
            [
                ("eth_getCode", [address_hex(owner)]),
                self._call_params(self.profile.registry_address, bound),
                self._call_params(owner, check),
            ],
        )
        if _data(code):
            valid = answer is not _REVERTED and _data(answer)[:32] == _ERC1271_MAGIC
        else:
            valid = funding_signer(authorization, signature) == owner
        if wallet is _REVERTED:
            agent_wallet = bytes(20)  # A registry reverting for an unknown id binds none.
        else:
            # Zero once ERC-8004 clears it on transfer, or after unsetAgentWallet.
            agent_wallet = bytes.fromhex(_decode_exact(["address"], _data(wallet))[0][2:])
        return FundingEvidence(block, valid, agent_wallet)

    def _session_call(self, session_id: bytes) -> tuple[str, list[object]]:
        fixed_bytes(session_id, 32, field="session_id")
        return self._call_params(
            self.profile.domain.verifying_contract, encode_call("getSession", (session_id,))
        )

    def observe(
        self, block: Block, *, providers: tuple[int, ...] = (), sessions: tuple[bytes, ...] = ()
    ) -> tuple[PauseLevel, dict[int, ProviderIdentity], dict[bytes, ChainSession]]:
        """Verify the deployment and read the pause level, provider identities
        (an unusable catalog read as ``None``) and sessions at ``block``, in one
        batch."""
        found, _, pause, identities, states = self._observe_at(
            block.observation.block_number,
            block.observation.block_hash,
            providers=providers,
            sessions=sessions,
        )
        if found != block:
            raise ChainEvidenceError("canonical block evidence changed during observation")
        return pause, identities, states

    def _observe_at(
        self,
        number: int,
        block_hash: bytes,
        *,
        providers: tuple[int, ...] = (),
        sessions: tuple[bytes, ...] = (),
        recheck: int | None = None,
    ) -> tuple[
        Block, Block | None, PauseLevel, dict[int, ProviderIdentity], dict[bytes, ChainSession]
    ]:
        """``observe`` at a block known by number and hash, optionally re-reading
        an earlier block by number, all in one batch."""
        with_code = not self._code_checked
        calls = self._deployment_calls(with_code)
        calls.append(
            self._call_params(self.profile.domain.verifying_contract, encode_call("pauseLevel", ()))
        )
        for provider_id in providers:
            calls += self._identity_calls(provider_id)
        calls += [self._session_call(session_id) for session_id in sessions]
        block, results = self._read_at(number, block_hash, calls, recheck=recheck)
        again = None
        if recheck is not None:
            again, results = cast(Block, results[0]), results[1:]
        results = self._check_deployment(results, with_code)
        pause = _pause(results[0])
        results = results[1:]
        identities = {}
        for provider_id in providers:
            identities[provider_id] = self._parse_identity(provider_id, block, results[:4], True)
            results = results[4:]
        states = {
            session_id: _session(raw) for session_id, raw in zip(sessions, results, strict=True)
        }
        return block, again, pause, identities, states

    def session_at(
        self, number: int, block_hash: bytes, session_id: bytes
    ) -> tuple[Block, ChainSession]:
        """A session's state at a block another party named by number and hash,
        in one batch that also confirms that block is canonical at that number."""
        block, (raw,) = self._read_at(number, block_hash, [self._session_call(session_id)])
        return block, _session(raw)

    def _receipt(self, tx_hash: bytes) -> dict[str, Any] | None:
        """One transaction receipt, or ``None`` while it is not yet served."""
        fixed_bytes(tx_hash, 32, field="tx_hash")
        (raw,) = self._check_network(
            self._many(
                self._network_calls() + [("eth_getTransactionReceipt", ["0x" + tx_hash.hex()])]
            )
        )
        if raw is None:
            return None
        receipt = _object(raw)
        if _data(receipt.get("transactionHash"), 32) != tx_hash:
            raise ChainEvidenceError("receipt transaction mismatch")
        return receipt

    def _event(
        self,
        tx_hash: bytes,
        session_id: bytes,
        name: str,
        *,
        capture_amount: int | None = None,
        delta: int | None = None,
    ) -> tuple[int, bytes, dict[str, Any]]:
        """Read one unambiguous MPPI event; outer transaction success is insufficient.

        Returns its block number and hash, which ``_read_at`` then confirms
        together with the state the caller reads at that block.
        """
        fixed_bytes(session_id, 32, field="session_id")
        receipt = self._receipt(tx_hash)
        if receipt is None:
            raise ChainEvidenceError("successful canonical receipt required")
        number, block_hash = (
            _quantity(receipt.get("blockNumber")),
            _data(receipt.get("blockHash"), 32),
        )
        if _quantity(receipt.get("status")) != 1:
            raise ChainEvidenceError("successful canonical receipt required")
        logs = receipt.get("logs")
        if not isinstance(logs, list):
            raise ChainEvidenceError("receipt logs missing")
        matches = []
        previous = -1
        tx_index = _quantity(receipt.get("transactionIndex"))
        for raw in logs:
            log = _object(raw)
            index = _quantity(log.get("logIndex"))
            if (
                index <= previous
                or log.get("removed") is not False
                or _data(log.get("transactionHash"), 32) != tx_hash
                or _quantity(log.get("transactionIndex")) != tx_index
                or _quantity(log.get("blockNumber")) != number
                or _data(log.get("blockHash"), 32) != block_hash
            ):
                raise ChainEvidenceError("event inclusion mismatch")
            previous = index
            if _data(log.get("address"), 20) != self.profile.domain.verifying_contract:
                continue
            if not isinstance(log.get("topics"), list):
                raise ChainEvidenceError("event topics missing")
            topics = tuple(_data(value, 32) for value in log["topics"])
            if not topics or topics[0] != event_topic(name):
                continue
            event = decode_event(name, topics, _data(log.get("data")))
            if (
                event["sessionId"] == session_id
                and (capture_amount is None or event.get("captureAmount") == capture_amount)
                and (delta is None or event.get("deltaPaid") == delta)
            ):
                matches.append(event)
        if len(matches) != 1:
            raise ChainEvidenceError("required MPPI event missing or ambiguous")
        return number, block_hash, matches[0]

    def opening(
        self, tx_hash: bytes, session_id: bytes, *, accepted_catalog: Catalog | None = None
    ) -> OpeningEvidence:
        number, block_hash, event = self._event(tx_hash, session_id, "SessionOpened")
        observation = self._observation(number, block_hash)
        # On a final network the event alone proves the opening. The event
        # commits to the catalog by hash only, so its bytes are read at that
        # block when the caller holds none.
        final = self.profile.instant_finality
        calls = [] if final else [self._session_call(session_id)]
        if accepted_catalog is None:
            calls += self._identity_calls(event["providerId"])
        block, results = self._read_at(number, block_hash, calls) if calls else (None, [])
        state = None if final else _session(results.pop(0))
        descriptor = SessionDescriptor(
            parse_address(event["agent"]),
            parse_address(event["sessionSigner"]),
            event["providerId"],
            parse_address(event["recipient"]),
            parse_address(event["operator"]),
            parse_address(event["token"]),
            event["salt"],
        )
        # A later transaction in this block may have replaced registry metadata.
        # The opening event commits to the exact accepted bytes by hash. Reparse
        # supplied bytes; constructing a Catalog object is not verification.
        if accepted_catalog is not None:
            catalog: Catalog | None = parse_catalog(accepted_catalog.canonical_bytes)
        else:
            assert block is not None
            catalog = self._parse_identity(descriptor.provider_id, block, results, False).catalog
        if (
            descriptor_id(self.profile.domain, descriptor) != session_id
            or event["deposit"] <= 0
            or (
                state is not None
                and (
                    not state.exists
                    or state.deposit < event["deposit"]
                    or state.initial_rate_card_hash != event["initialRateCardHash"]
                )
            )
            or catalog is None
            or catalog.rate_card_hash != event["initialRateCardHash"]
        ):
            raise ChainEvidenceError("opening descriptor, deposit or catalog mismatch")
        return OpeningEvidence(descriptor, observation, event["deposit"], catalog, tx_hash)

    def _observation(self, number: int, block_hash: bytes) -> ChainObservation:
        return ChainObservation(
            self.profile.domain.chain_id, self.profile.domain.verifying_contract, number, block_hash
        )

    def simulate(self, operation: PreparedOperation, *, sender: bytes) -> SimulationEvidence:
        """Simulate exactly from the intended wallet before batch submission.

        Execution API ``eth_estimateGas`` takes a block number/tag, unlike
        EIP-1898 ``eth_call``. Recheck that height's hash after estimation; never
        silently fall back to latest. Both results remain advisory.
        """
        fixed_bytes(sender, 20, field="sender", nonzero=True)
        if (
            not isinstance(operation, PreparedOperation)
            or operation.call.chain_id != self.profile.domain.chain_id
            or operation.call.to != self.profile.domain.verifying_contract
        ):
            raise ChainEvidenceError("simulation differs from selected deployment")
        block = self.block()
        self.verify_deployment(block)
        raw = _object(
            self.rpc.call("eth_getBlockByNumber", [hex(block.observation.block_number), False])
        )
        if _data(raw.get("hash"), 32) != block.observation.block_hash:
            raise ChainEvidenceError("simulation block changed")
        gas_limit = _quantity(raw.get("gasLimit"))
        if gas_limit <= 0:
            raise ChainEvidenceError("simulation block has no gas capacity")
        call = {
            "from": address_hex(sender),
            "to": address_hex(operation.call.to),
            "data": "0x" + operation.call.data.hex(),
            "value": "0x0",
            "gas": hex(gas_limit),
        }
        if _data(self.rpc.call("eth_call", [call, self._position(block)])) != b"":
            raise ChainEvidenceError("MPPI simulation returned unexpected data")
        estimate = _quantity(
            self.rpc.call("eth_estimateGas", [call, hex(block.observation.block_number)])
        )
        if not 0 < estimate <= gas_limit:
            raise ChainEvidenceError("simulation estimate exceeds block capacity")
        self._check_canonical(block)
        return SimulationEvidence(block, estimate, gas_limit)

    def closing(self, tx_hash: bytes, session_id: bytes) -> ClosingEvidence:
        number, block_hash, event = self._event(tx_hash, session_id, "SessionClosed")
        if not self.profile.instant_finality:
            _, (raw,) = self._read_at(number, block_hash, [self._session_call(session_id)])
            state = _session(raw)
            if (
                not state.exists
                or state.status is not SessionStatus.CLOSED
                or event["captureAmount"] != state.settled_cumulative
                or event["captureAmount"] + event["refunded"] != state.deposit
            ):
                raise ChainEvidenceError("closing event contradicts canonical state")
        return ClosingEvidence(
            self._observation(number, block_hash),
            event["captureAmount"],
            event["refunded"],
            tx_hash,
        )

    def verify_settlement(
        self, reference: ConfirmedSettlementRef, descriptor: SessionDescriptor
    ) -> None:
        if descriptor_id(self.profile.domain, descriptor) != reference.session_id:
            raise ChainEvidenceError("settlement descriptor mismatch")
        number, block_hash, event = self._event(
            reference.tx_hash,
            reference.session_id,
            "Settled",
            capture_amount=reference.settled_cumulative,
            delta=reference.delta,
        )
        block, (raw,) = self._read_at(
            number, block_hash, [self._session_call(reference.session_id)]
        )
        state = _session(raw)
        if (
            block.observation != reference.observation
            or parse_address(event["agent"]) != descriptor.agent
            or parse_address(event["recipient"]) != descriptor.recipient
            or event["newSettled"] != reference.settled_cumulative
            or event["captureAmount"] != reference.settled_cumulative
            or event["deltaPaid"] != reference.delta
            or event["cumulativeAmount"] < reference.settled_cumulative
            or not state.exists
            or state.settled_cumulative < reference.settled_cumulative
        ):
            raise ChainEvidenceError("settlement reference contradicts canonical event")

    def reconcile(
        self,
        operation: PreparedOperation,
        *,
        operation_ref: str,
        tx_hash: bytes | None,
        wallet_operation_id: str | None = None,
    ) -> OperationResult:
        """Re-read one original transaction; never submit or replace a call.

        A missing hash/receipt remains unknown/pending. On a final network the
        receipt alone proves an operation by its own MPPI event; a mined
        receipt without that event is a definite failure, with any ERC-4337
        revert reason as its cause. Elsewhere, outer success without MPPI
        evidence remains unknown. Settlements always read session state: a
        no-op or skipped item emits no event. Batch confirmation describes the
        receipt; each item's coverage and attributable payment remain separate.
        """
        if (
            operation.call.chain_id != self.profile.domain.chain_id
            or operation.call.to != self.profile.domain.verifying_contract
        ):
            raise ChainEvidenceError("operation differs from selected deployment")
        if tx_hash is None:
            return OperationResult(operation_ref, OperationState.UNKNOWN, wallet_operation_id)
        fixed_bytes(tx_hash, 32, field="tx_hash")
        try:
            receipt = self._receipt(tx_hash)
            if receipt is None:
                return OperationResult(
                    operation_ref, OperationState.PENDING, wallet_operation_id, tx_hash
                )
            status = _quantity(receipt.get("status"))
            if status not in (0, 1):
                raise ChainEvidenceError("invalid receipt status")
            observation = self._observation(
                _quantity(receipt.get("blockNumber")), _data(receipt.get("blockHash"), 32)
            )
            alone = self.profile.instant_finality and operation.kind not in _STATEFUL
            states: dict[bytes, ChainSession] | None = None
            timestamp = None
            if not alone:
                sessions = tuple(dict.fromkeys(operation.session_ids)) if status == 1 else ()
                block, results = self._read_at(
                    observation.block_number,
                    observation.block_hash,
                    [self._session_call(session_id) for session_id in sessions],
                )
                observation, timestamp = block.observation, block.timestamp
                states = {sid: _session(raw) for sid, raw in zip(sessions, results, strict=True)}
            if status == 0:
                return OperationResult(
                    operation_ref, OperationState.FAILED, wallet_operation_id, tx_hash, observation
                )
            outcomes, proven = self._outcomes(
                operation, receipt, observation, timestamp, tx_hash, states
            )
            if not proven and alone:
                return OperationResult(
                    operation_ref,
                    OperationState.FAILED,
                    wallet_operation_id,
                    tx_hash,
                    observation,
                    cause=self._no_effect(operation, receipt),
                )
            return OperationResult(
                operation_ref,
                OperationState.CONFIRMED if proven else OperationState.UNKNOWN,
                wallet_operation_id,
                tx_hash,
                observation,
                outcomes,
            )
        except (
            TransportError,
            ChainEvidenceError,
            ValidationError,
            ValueError,
            KeyError,
            TypeError,
        ):
            # Evidence unavailable/contradictory never becomes an economic fail.
            return OperationResult(
                operation_ref, OperationState.UNKNOWN, wallet_operation_id, tx_hash
            )

    def _no_effect(self, operation: PreparedOperation, receipt: dict[str, Any]) -> str:
        """Why a mined transaction holds no MPPI event: the cause, with the
        EntryPoint v0.7 revert reason of the wallet's UserOperation when the
        receipt carries one, as a known USDC reason, an MPPI error name or
        the 4-byte selector."""
        cause = "the MPPI call did not take effect in its transaction"
        wallets = {
            address
            for descriptor in operation.descriptors
            for address in (descriptor.agent, descriptor.operator)
        }
        for raw in receipt.get("logs") or ():
            log = _object(raw)
            topics = log.get("topics")
            if (
                _data(log.get("address"), 20) != _ENTRYPOINT_V07
                or not isinstance(topics, list)
                or len(topics) != 3
                or _data(topics[0], 32) != _USER_OPERATION_REVERT
                or _data(topics[2], 32)[12:] not in wallets
            ):
                continue
            _, reason = _decode_exact(["uint256", "bytes"], _data(log.get("data")))
            known = error_name(reason)
            if reason[:4] == _ERROR_STRING:
                try:
                    (text,) = _decode_exact(["string"], reason[4:])
                except ValidationError:
                    text = ""
                known = text if text in _TOKEN_REASONS else None
            if known is not None:
                return f"{cause}: {known}"
            return f"{cause}: the wallet operation reverted (0x{reason[:4].hex()})"
        return cause

    def _outcomes(
        self,
        operation: PreparedOperation,
        receipt: dict[str, Any],
        observation: ChainObservation,
        timestamp: int | None,
        tx_hash: bytes,
        states: dict[bytes, ChainSession] | None,
    ) -> tuple[tuple[SessionOperationOutcome, ...], bool]:
        """Each item's outcome from its event, checked against ``states``, the
        session states at the receipt's block, when they were read."""
        name = {
            OperationKind.OPEN: "SessionOpened",
            OperationKind.TOP_UP: "ToppedUp",
            OperationKind.SETTLE: "Settled",
            OperationKind.SETTLE_BATCH: "Settled",
            OperationKind.CLOSE: "SessionClosed",
            OperationKind.WITHDRAW: "SessionClosed",
            OperationKind.REQUEST_CLOSE: "CloseRequested",
            OperationKind.RELEASE_IDLE: "IdleReleased",
        }[operation.kind]
        logs = receipt.get("logs")
        if not isinstance(logs, list):
            raise ChainEvidenceError("receipt logs missing")
        events: dict[bytes, list[dict[str, Any]]] = {}
        captures: dict[tuple[bytes, int], list[dict[str, Any]]] = {}
        wanted = set(operation.session_ids)
        previous_index = -1
        tx_index = _quantity(receipt.get("transactionIndex"))
        for raw in logs:
            log = _object(raw)
            index = _quantity(log.get("logIndex"))
            if (
                index <= previous_index
                or log.get("removed") is not False
                or _data(log.get("transactionHash"), 32) != tx_hash
                or _quantity(log.get("transactionIndex")) != tx_index
                or _quantity(log.get("blockNumber")) != observation.block_number
                or _data(log.get("blockHash"), 32) != observation.block_hash
            ):
                raise ChainEvidenceError("log inclusion or order mismatch")
            previous_index = index
            if _data(log.get("address"), 20) != self.profile.domain.verifying_contract:
                continue
            if not isinstance(log.get("topics"), list):
                raise ChainEvidenceError("log topics missing")
            topics = tuple(_data(value, 32) for value in log["topics"])
            if not topics or topics[0] != event_topic(name):
                continue
            decoded_event = decode_event(name, topics, _data(log.get("data")))
            session_id = decoded_event["sessionId"]
            if session_id not in wanted:
                # An ERC-4337 receipt may include other users' MPPI calls.
                continue
            if session_id in events and operation.kind is not OperationKind.SETTLE_BATCH:
                raise ChainEvidenceError("ambiguous repeated operation event")
            events.setdefault(session_id, []).append(decoded_event)
            if operation.kind is OperationKind.SETTLE_BATCH:
                captures.setdefault((session_id, decoded_event["captureAmount"]), []).append(
                    decoded_event
                )
        multiplicities: dict[tuple[bytes, int], int] = {}
        if operation.kind is OperationKind.SETTLE_BATCH:
            for sid, capture_target in zip(operation.session_ids, operation.amounts, strict=True):
                key = (sid, capture_target)
                multiplicities[key] = multiplicities.get(key, 0) + 1
        results = []
        proven = True
        for index, (session_id, descriptor) in enumerate(
            zip(operation.session_ids, operation.descriptors, strict=True)
        ):
            state = states[session_id] if states is not None else None
            candidates = events.get(session_id, [])
            event = candidates[0] if candidates else None
            target = operation.amounts[index] if operation.amounts else None
            settlement = None
            covered = None
            if operation.kind in (OperationKind.SETTLE, OperationKind.SETTLE_BATCH):
                assert target is not None and state is not None
                covered = state.exists and state.settled_cumulative >= target
                if operation.kind is OperationKind.SETTLE_BATCH:
                    # The ABI deliberately emits no item index. Distinct targets
                    # can be matched, but identical repeated inputs cannot tell
                    # which item paid. Preserve ordered coverage without assigning
                    # the same payment to multiple ambiguous items.
                    matches = captures.get((session_id, target), [])
                    if len(matches) > 1:
                        raise ChainEvidenceError("ambiguous repeated capture event")
                    event = matches[0] if matches else None
                if event is not None:
                    if (
                        parse_address(event["agent"]) != descriptor.agent
                        or parse_address(event["recipient"]) != descriptor.recipient
                        or event["captureAmount"] != target
                        or event["newSettled"] != target
                        or event["cumulativeAmount"] < target
                        or not 0 < event["deltaPaid"] <= target
                        or not covered
                    ):
                        raise ChainEvidenceError(
                            "settlement event contradicts the intended target/state"
                        )
                    if multiplicities.get((session_id, target), 1) == 1:
                        settlement = ConfirmedSettlementRef(
                            observation, tx_hash, session_id, target, event["deltaPaid"]
                        )
                # No event can mean a no-op, skipped item or inner wallet failure.
                # State coverage is useful but never attributable to this call.
                elif operation.kind is OperationKind.SETTLE:
                    proven = False
            elif event is None or (state is not None and not state.exists):
                proven = False
            elif operation.kind is OperationKind.OPEN:
                expected = {
                    "agent": descriptor.agent,
                    "sessionSigner": descriptor.session_signer,
                    "recipient": descriptor.recipient,
                    "operator": descriptor.operator,
                    "token": descriptor.token,
                }
                if (
                    any(parse_address(event[key]) != value for key, value in expected.items())
                    or event["providerId"] != descriptor.provider_id
                    or event["salt"] != descriptor.salt
                    or event["deposit"] != target
                    or event["initialRateCardHash"] != operation.expected_rate_card_hash
                    or (
                        state is not None
                        and (
                            state.deposit < event["deposit"]
                            or state.initial_rate_card_hash != operation.expected_rate_card_hash
                        )
                    )
                ):
                    raise ChainEvidenceError("opening event contradicts descriptor/funding terms")
            elif operation.kind is OperationKind.TOP_UP:
                if (
                    event["amount"] != target
                    or event["newDeposit"] < event["amount"]
                    or (state is not None and state.deposit < event["newDeposit"])
                ):
                    raise ChainEvidenceError("top-up event contradicts the intended funding")
            elif operation.kind is OperationKind.REQUEST_CLOSE:
                if state is not None and (
                    state.status not in (SessionStatus.CLOSING, SessionStatus.CLOSED)
                    or event["graceEnd"] != state.close_requested_at + 3600
                    or state.close_requested_at != timestamp
                ):
                    raise ChainEvidenceError("close request contradicts canonical state")
            elif operation.kind in (OperationKind.CLOSE, OperationKind.WITHDRAW):
                if (
                    state is not None
                    and (
                        state.status is not SessionStatus.CLOSED
                        or event["captureAmount"] != state.settled_cumulative
                        or event["captureAmount"] + event["refunded"] != state.deposit
                    )
                ) or (operation.kind is OperationKind.CLOSE and event["captureAmount"] != target):
                    raise ChainEvidenceError("close/refund event contradicts canonical state")
            elif operation.kind is OperationKind.RELEASE_IDLE:
                if state is not None and (
                    state.status is not SessionStatus.CLOSED
                    or event["amount"] != state.deposit - state.settled_cumulative
                ):
                    raise ChainEvidenceError("idle refund contradicts canonical state")
            deposit = None
            if event is not None and operation.kind is OperationKind.OPEN:
                deposit = event["deposit"]
            elif event is not None and operation.kind is OperationKind.TOP_UP:
                deposit = event["newDeposit"]
            results.append(
                SessionOperationOutcome(session_id, target, covered, settlement, deposit=deposit)
            )
        if operation.kind is OperationKind.SETTLE_BATCH:
            proven = any(result.settlement_reference is not None for result in results)
        return tuple(results), proven
