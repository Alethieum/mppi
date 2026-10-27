"""Read-only Arc evidence and reconciliation of previously submitted MPPI calls.

RPC consistency is checked, not cryptographically attested. Select a trusted RPC
and verified deployment manifest. This module does not implement a chain follower,
provider storage, transaction resubmission or session readiness by itself.
"""

from __future__ import annotations

import json
import random
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
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
from .discovery import (
    MAX_DOCUMENT_BYTES,
    ProviderIdentity,
    _parse_document,
    _parse_json,
    address_hex,
    parse_address,
)
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
# The most views one snapshot aggregate3 holds besides its three head views.
# Arc's node runs an eth_call without a gas field at its --rpc.gascap,
# 30,000,000 by default (circlefin/arc-node v0.8.1 crates/node/src/args.rs;
# Reth v2.2.0 prepare_call_env). Measured on a local revm node (anvil 1.5.1,
# Osaka rules), 512 getSession views with the other snapshot views took
# 6,167,605 gas, about a fifth of that cap, where one call of 2,249 exceeded
# it; their calldata is 231,562 characters of hex, under a quarter of the
# 1 MiB request bound.
_SNAPSHOT_VIEWS = 512
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
    """A JSON-RPC rate-limit or not-yet-imported error, by its ``code``; a
    later read may succeed."""

    def __init__(self, code: int) -> None:
        super().__init__("RPC request was rate limited")
        self.code = code


# A transient read failure is repeated up to twice: rate limits, dropped
# connections and slow backends are routine on shared RPC plans. Each repeat
# waits a uniformly random share of its ceiling, 250 ms and then 1 s ("full
# jitter", the AWS SDKs' retry backoff:
# https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html),
# so clients refused together do not repeat together; a refusal's Retry-After
# (RFC 9110 section 10.2.3) is waited first. A repeat starts only within one
# transport timeout plus the two ceilings of the first attempt: a wait that
# would end later is not begun, and the read fails at once.
_READ_RETRY_DELAYS = (0.25, 1.0)
_jitter = random.random
# Each attempt is bounded at 5 s by default. A client allows each step 10 s by
# default, so after a timed-out attempt and a delay of at most 250 ms its repeat
# has at least 4.75 s left; a second timed-out attempt could not start by 6.25 s.
_ATTEMPT_MS = 5_000
# EIP-1474 -32005 "Limit exceeded" (https://eips.ethereum.org/EIPS/eip-1474);
# QuickNode's -32007 per-second request limit on EVM endpoints
# (https://www.quicknode.com/docs/berachain/error-references); Arc's -32014 from
# a load-balanced backend that "hasn't yet imported that block", which Arc says
# can be retried after a brief backoff (docs.arc.io/arc/references/rpc-endpoints).
# A -32014 that outlasts the repeats is lag (ChainLagError).
_NOT_IMPORTED = -32014
_RATE_LIMIT_CODES = frozenset({-32005, -32007, _NOT_IMPORTED})
# At most this many calls go in one JSON-RPC batch: some plans count each call
# in a batch against a per-second request limit and refuse entries beyond it.
_BATCH_CALLS = 32
# Each request's id is a UUID in its 36-character text form.
_ID = "0" * 36
_UNANSWERED = object()
# The most blocks one eth_getLogs spans. Arc's endpoints refuse a range over
# 10,000 blocks (-32012): "Log-driven clients must page through history in
# <=9,999-block chunks" (docs.arc.io/arc/references/rpc-endpoints, Supported
# methods); QuickNode's paid plans allow 10,000 (support.quicknode.com,
# "Understanding the 10,000 Block Range Limit").
_LOG_SPAN = 9_999
_READ_ONLY = frozenset(
    {
        "eth_chainId",
        "eth_getBlockByNumber",
        "eth_getTransactionReceipt",
        "eth_getLogs",
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
    then reads views that ran at another block than its header again, pinned
    to the header's block."""

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
        return self._read([(method, params)])[0]

    def call_many(self, calls: Sequence[tuple[str, list[object]]]) -> list[object]:
        """Results of read-only calls, in order, sent as JSON-RPC batches of
        at most 32 calls and 1 MiB each, one exchange after another.

        A batch's responses may arrive in any order and are matched by id
        (JSON-RPC 2.0 section 6); each call must be answered exactly once.
        A repeat sends only the calls not yet answered.
        """
        for method, _ in calls:
            _read_only(method)
        results: list[object] = []
        for batch in _batches(calls):
            results.extend(self._read(batch))
        return results

    def close(self) -> None:
        """Close idle kept-alive connections; later reads open new ones."""
        self.transport.close()

    def _read(self, calls: list[tuple[str, list[object]]]) -> list[object]:
        """Results of ``calls``, in order. Answers are kept across repeats, so
        a repeat sends only the calls a refusal left unanswered."""
        results: list[object] = [_UNANSWERED] * len(calls)

        def read() -> list[object]:
            self._fill(calls, results)
            return results

        return self._retried(read)

    def _retried(self, read: Callable[[], _T]) -> _T:
        try:
            return self._repeated(read)
        except _RateLimited as error:
            if error.code != _NOT_IMPORTED:
                raise
            # Still behind once the repeats are spent: lag, which keeps
            # accepted evidence, not a failure.
            raise ChainLagError("RPC backend has not imported that block yet") from None

    def _repeated(self, read: Callable[[], _T]) -> _T:
        budget = (
            time.monotonic()
            + getattr(self.transport, "timeout_ms", _ATTEMPT_MS) / 1000
            + sum(_READ_RETRY_DELAYS)
        )
        for ceiling in _READ_RETRY_DELAYS:
            try:
                return read()
            except (TransportError, _RateLimited) as error:
                asked = None
                if isinstance(error, TransportError):
                    if not error.transient:
                        raise
                    asked = error.retry_after
                delay = (asked or 0.0) + ceiling * _jitter()
                if time.monotonic() + delay >= budget:
                    raise
            time.sleep(delay)
        return read()

    def _once(self, method: str, params: list[object]) -> object:
        request_id = str(uuid4())
        payload = _encoded(_request(request_id, method, params))
        response = _parse_document(
            self.transport.request(self.endpoint, body=payload), max_bytes=MAX_RPC_RESPONSE_BYTES
        )
        return _answer(response, request_id, method)

    def _fill(self, calls: list[tuple[str, list[object]]], results: list[object]) -> None:
        """Fill each unanswered entry of ``results`` with its call's answer: in
        one batch, or one call at a time. Answers are kept as they arrive; a
        refusal is raised after them, a definite failure at once."""
        pending = [index for index, result in enumerate(results) if result is _UNANSWERED]
        if not self._batches or len(pending) == 1:
            for index in pending:
                results[index] = self._once(*calls[index])
            return
        ids = {str(uuid4()): index for index in pending}
        payload = _encoded([_request(key, *calls[i]) for key, i in ids.items()])
        try:
            raw = self.transport.request(self.endpoint, body=payload)
        except TransportError as refused:
            if refused.transient:
                raise
            # A refused single call raises its own error and keeps batches on.
            results[pending[0]] = self._once(*calls[pending[0]])
            object.__setattr__(self, "_batches", False)
            for index in pending[1:]:
                results[index] = self._once(*calls[index])
            return
        response = _parse_json(raw, max_bytes=MAX_RPC_RESPONSE_BYTES)
        if isinstance(response, dict):
            error = response.get("error")
            if isinstance(error, dict) and error.get("code") in _RATE_LIMIT_CODES:
                raise _RateLimited(error["code"])
            object.__setattr__(self, "_batches", False)
            for index in pending:
                results[index] = self._once(*calls[index])
            return
        if not isinstance(response, list) or len(response) != len(ids):
            raise ChainEvidenceError("RPC batch response count mismatch")
        rows: dict[str, dict[str, Any]] = {}
        for row in response:
            if not isinstance(row, dict) or type(row.get("id")) is not str or row["id"] in rows:
                raise ChainEvidenceError("RPC response identity mismatch")
            rows[row["id"]] = row
        if rows.keys() != ids.keys():
            raise ChainEvidenceError("RPC response identity mismatch")
        limited: int | None = None  # The first refusal's code.
        for request_id, index in ids.items():
            try:
                results[index] = _answer(rows[request_id], request_id, calls[index][0])
            except _RateLimited as error:
                # Left unanswered: only such entries are sent again.
                limited = error.code if limited is None else limited
        if limited is not None:
            raise _RateLimited(limited)


def _request(request_id: str, method: str, params: list[object]) -> dict[str, object]:
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}


def _encoded(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def _batches(
    calls: Sequence[tuple[str, list[object]]],
) -> list[list[tuple[str, list[object]]]]:
    """``calls`` as consecutive JSON-RPC batches, each of at most _BATCH_CALLS
    calls and a body of at most MAX_DOCUMENT_BYTES, the bound HttpTransport
    enforces. A call that cannot fit one body alone is refused before anything
    is sent."""
    batches: list[list[tuple[str, list[object]]]] = []
    batch: list[tuple[str, list[object]]] = []
    size = 0
    for method, params in calls:
        entry = len(_encoded(_request(_ID, method, params)))
        # A batch is "[", its entries separated by commas, then "]".
        if entry + 2 > MAX_DOCUMENT_BYTES:
            raise TransportError("JSON-RPC request exceeds 1 MiB")
        if batch and (len(batch) == _BATCH_CALLS or size + 1 + entry > MAX_DOCUMENT_BYTES):
            batches.append(batch)
            batch = []
        size = size + 1 + entry if batch else entry + 2
        batch.append((method, params))
    if batch:
        batches.append(batch)
    return batches


# A contract view in a snapshot: its target, whether it may fail, its calldata.
_View = tuple[bytes, bool, bytes]


def _snapshot_chunks(
    fixed: list[_View], identities: list[list[_View]], sessions: list[_View]
) -> list[list[_View]]:
    """A snapshot's views, in order, split into aggregate3 calls of at most
    _SNAPSHOT_VIEWS views and one provider's identity each: an identity's gas
    grows with the URI and catalog it stores."""
    chunks: list[list[_View]] = []
    chunk, identity = list(fixed), False
    for group in identities:
        if identity or len(chunk) + len(group) > _SNAPSHOT_VIEWS:
            chunks.append(chunk)
            chunk = []
        chunk += group
        identity = True
    for view in sessions:
        if len(chunk) == _SNAPSHOT_VIEWS:
            chunks.append(chunk)
            chunk = []
        chunk.append(view)
    chunks.append(chunk)
    return chunks


def _aggregate(chunk: list[_View]) -> tuple[str, list[object]]:
    """The aggregate3 eth_call reading ``chunk`` after its block number, time
    and parent hash; its block position is added by the caller."""
    calls3 = [(_MULTICALL3, False, call) for call in _HEAD_CALLS] + chunk
    data = _AGGREGATE3 + encode(["(address,bool,bytes)[]"], [calls3])
    return ("eth_call", [{"to": address_hex(_MULTICALL3), "data": "0x" + data.hex()}])


def _answers(raw: object, chunk: list[_View], head: Block) -> list[object] | None:
    """The answers to an _aggregate of ``chunk`` after its head views, a view
    allowed to fail that failed as _REVERTED; None when they ran at another
    height than ``head``."""
    answers = decode(["(bool,bytes)[]"], _data(raw), strict=True)[0]
    if len(answers) != 3 + len(chunk) or not all(
        ok or allowed
        for (ok, _), allowed in zip(answers, [False] * 3 + [view[1] for view in chunk], strict=True)
    ):
        raise ChainEvidenceError("RPC snapshot is incomplete")
    number, timestamp, parent = (
        _decode_exact([kind], bytes(data))[0]
        for kind, (_, data) in zip(("uint256", "uint256", "bytes32"), answers[:3], strict=True)
    )
    if number != head.observation.block_number:
        return None
    if (timestamp, parent) != (head.timestamp, head.parent_hash):
        raise ChainEvidenceError("RPC snapshot and head are from different blocks")
    return ["0x" + bytes(data).hex() if ok else _REVERTED for ok, data in answers[3:]]


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


def _binding_data(agent_id: int) -> bytes:
    """Registry calldata for the funding wallet ``agent_id`` binds."""
    uint256(agent_id, field="agent_id")
    return _hash(b"getAgentWallet(uint256)")[:4] + encode(["uint256"], [agent_id])


def _bound(answer: object) -> bytes:
    """The funding wallet a getAgentWallet answer binds, zero when none."""
    if answer is _REVERTED:
        return bytes(20)  # A registry reverting for an unknown id binds none.
    # Zero once ERC-8004 clears it on transfer, or after unsetAgentWallet.
    return bytes.fromhex(_decode_exact(["address"], _data(answer))[0][2:])


def _funding_watch(funding: tuple[int, bytes] | None) -> tuple[int, bytes] | None:
    if funding is None:
        return None
    agent_id, wallet = funding
    uint256(agent_id, field="agent_id")
    return agent_id, fixed_bytes(wallet, 20, field="funding wallet", nonzero=True)


def _signature_check(authorization: FundingAuthorization, signature: bytes) -> bytes:
    """ERC-1271 isValidSignature calldata for a funding authorization."""
    return _IS_VALID_SIGNATURE + encode(
        ["bytes32", "bytes"], [funding_digest(authorization), signature]
    )


def _accepts(
    authorization: FundingAuthorization, signature: bytes, code: object, answer: object
) -> bool:
    """USDC's decision (SignatureChecker.isValidSignatureNow) from the owner's
    code and its isValidSignature answer: an owner without code must recover
    through ECRecover; one with code must answer the ERC-1271 magic word, and
    a reverted call is a refusal, as its failed staticcall is."""
    if _data(code):
        return answer is not _REVERTED and _data(answer)[:32] == _ERC1271_MAGIC
    return funding_signer(authorization, signature) == authorization.owner


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
        raise _RateLimited(error["code"])
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
class FundingState:
    """At a snapshot's block, what an opening's funding check reads: the
    funding wallet the registry binds to ``agent_id`` (zero when none), and
    whether ``wallet`` has code. USDC checks a funding signature from an
    account with code through its ERC-1271 isValidSignature, and from one
    without through ECDSA recovery (SignatureChecker.isValidSignatureNow)."""

    agent_id: int
    wallet: bytes
    agent_wallet: bytes
    has_code: bool


@dataclass(frozen=True, slots=True)
class SimulationEvidence:
    """Advisory execution/cost evidence at one canonical block, never a receipt."""

    block: Block
    gas_estimate: int
    gas_limit: int


class ChainReader:
    """Synchronous, block-pinned reads; use outside the payment event loop.

    Each step reads everything it needs at one block through one ``call_many``
    when the RPC offers it: HttpJsonRpc sends one round trip per JSON-RPC
    batch of 32 calls and 1 MiB. Every state read is pinned with EIP-1898
    ``{blockHash, requireCanonical: true}``, and the same ``call_many`` reads
    that block by number: requireCanonical is only a SHOULD, so the matching
    hash is what shows the block was canonical while the reads were served. Facts
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
        catalog read as ``None``) and sessions at the latest block, on a
        network whose committed blocks are final.

        Multicall3 ``aggregate3`` eth_calls at ``latest``, of at most
        _SNAPSHOT_VIEWS views each, read every contract view, each with the
        block number, time and parent hash it ran at. They go in the batch that
        reads the header at ``latest``, the anchor. A call whose views ran at
        another height is read again pinned to the header's hash, with the
        header read by number in the same batch; one at the header's height
        but in another block is a contradiction. Views from two blocks never
        form a snapshot. The implementation slots, which no contract exposes,
        are read with the header. One round trip while the calls fit one
        JSON-RPC batch (HttpJsonRpc: 32 calls and 1 MiB) and ran at the
        header's block; each further batch adds one, and calls that ran
        elsewhere are sent once more. Without verified Multicall3 code, or on
        a network without instant finality, this is the head and then the
        reads pinned to it.
        """
        return self._latest(providers, sessions)[:4]

    def _latest(
        self,
        providers: tuple[int, ...],
        sessions: tuple[bytes, ...],
        funding: tuple[int, bytes] | None = None,
    ) -> tuple[
        Block,
        PauseLevel,
        dict[int, ProviderIdentity],
        dict[bytes, ChainSession],
        FundingState | None,
    ]:
        """``latest``, and with ``funding``, an agent id and its funding wallet,
        their FundingState: the registry's binding for that id in the first
        multicall, and the wallet's code in the same batch, which no contract
        exposes either."""
        wanted = _funding_watch(funding)
        if not self.profile.instant_finality or self._multicall is False:
            (head,) = self._blocks((None,))
            found, _, pause, identities, states, state = self._observe_at(
                head.observation.block_number,
                head.observation.block_hash,
                providers=providers,
                sessions=sessions,
                funding=wanted,
            )
            return found, pause, identities, states, state
        domain = not self._domain_checked
        proxy, registry = self.profile.domain.verifying_contract, self.profile.registry_address
        fixed: list[_View] = []
        if domain:
            fixed.append((proxy, False, encode_call("voucherDigest", (bytes(32), 0))))
        fixed.append((proxy, False, encode_call("pauseLevel", ())))
        if wanted is not None:
            # A registry reverting for an unknown id binds none: allowed to fail.
            fixed.append((registry, True, _binding_data(wanted[0])))
        chunks = _snapshot_chunks(
            fixed,
            [
                [(registry, False, data) for data in self._identity_data(provider_id)]
                for provider_id in providers
            ],
            [
                (
                    proxy,
                    False,
                    encode_call("getSession", (fixed_bytes(sid, 32, field="session_id"),)),
                )
                for sid in sessions
            ],
        )
        aggregates = [_aggregate(chunk) for chunk in chunks]
        code = not self._code_checked
        unchecked = self._multicall is None
        calls: list[tuple[str, list[object]]] = self._network_calls()
        if code:
            calls += [("eth_getCode", [address_hex(pin.address), "latest"]) for pin in self._pins()]
        if unchecked:
            calls.append(("eth_getCode", [address_hex(_MULTICALL3), "latest"]))
        calls.append(("eth_getBlockByNumber", ["latest", False]))
        calls += [(method, [*params, "latest"]) for method, params in aggregates]
        calls += [
            ("eth_getStorageAt", [address_hex(pin.address), _IMPLEMENTATION_SLOT, "latest"])
            for pin in (self.profile.proxy, self.profile.registry)
        ]
        if wanted is not None:
            calls.append(("eth_getCode", [address_hex(wanted[1]), "latest"]))
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
                return self._latest(providers, sessions, wanted)
        head = self._parse_block(results[0], None)
        answered = [
            _answers(raw, chunk, head)
            for chunk, raw in zip(chunks, results[1 : 1 + len(chunks)], strict=True)
        ]
        results = results[1 + len(chunks) :]
        for implementation, raw in zip(
            (self.profile.implementation, self.profile.registry_implementation),
            results[:2],
            strict=True,
        ):
            if _data(raw, 32) != b"\0" * 12 + implementation.address:
                raise ChainContradictionError("proxy implementation differs from release profile")
        # Views that ran at another height than the header (sent in another
        # batch, or to another backend) are read again at the header's block.
        elsewhere = [index for index, found in enumerate(answered) if found is None]
        if elsewhere:
            again = self._read(head, [aggregates[index] for index in elsewhere])
            for index, raw in zip(elsewhere, again, strict=True):
                answered[index] = _answers(raw, chunks[index], head)
                if answered[index] is None:
                    raise ChainEvidenceError("RPC snapshot and head are from different blocks")
        values = [value for chunk in answered for value in chunk or ()]
        if domain:
            digest = decode_result("voucherDigest", _data(values.pop(0)))[0]
            if digest != voucher_digest(self.profile.domain, bytes(32), 0):
                raise ChainContradictionError("MPPI signing domain differs from release profile")
            self._domain_checked = True
        pause = _pause(values.pop(0))
        binding = values.pop(0) if wanted is not None else None
        identities = {}
        for provider_id in providers:
            identities[provider_id] = self._parse_identity(provider_id, head, values[:4], True)
            values = values[4:]
        states = {sid: _session(raw) for sid, raw in zip(sessions, values, strict=True)}
        state = None
        if wanted is not None:
            state = FundingState(*wanted, _bound(binding), bool(_data(results[2])))
        return head, pause, identities, states, state

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
        _, (raw,) = self._read_at(
            observation.block_number,
            observation.block_hash,
            [self._call_params(self.profile.registry_address, _binding_data(agent_id))],
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
        self._check_authorization(authorization, signature)
        owner = authorization.owner
        code, wallet, answer = self._read(
            block,
            [
                ("eth_getCode", [address_hex(owner)]),
                self._call_params(self.profile.registry_address, _binding_data(agent_id)),
                self._call_params(owner, _signature_check(authorization, signature)),
            ],
        )
        return FundingEvidence(
            block, _accepts(authorization, signature, code, answer), _bound(wallet)
        )

    def funding_signature(
        self, authorization: FundingAuthorization, signature: bytes, block: Block
    ) -> bool:
        """Whether USDC accepts ``signature`` for ``authorization`` at
        ``block``: ``funding`` without the registry's binding, which a
        snapshot already read (ChainSnapshot.funding), in one round trip."""
        self._check_authorization(authorization, signature)
        owner = authorization.owner
        code, answer = self._read(
            block,
            [
                ("eth_getCode", [address_hex(owner)]),
                self._call_params(owner, _signature_check(authorization, signature)),
            ],
        )
        return _accepts(authorization, signature, code, answer)

    def _check_authorization(self, authorization: FundingAuthorization, signature: bytes) -> None:
        if (
            not isinstance(authorization, FundingAuthorization)
            or authorization.chain_id != self.profile.domain.chain_id
            or type(signature) is not bytes
            or not 0 < len(signature) <= 65536
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "funding check differs from selected deployment"
            )

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
        found, _, pause, identities, states, _ = self._observe_at(
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
        funding: tuple[int, bytes] | None = None,
    ) -> tuple[
        Block,
        Block | None,
        PauseLevel,
        dict[int, ProviderIdentity],
        dict[bytes, ChainSession],
        FundingState | None,
    ]:
        """``observe`` at a block known by number and hash, optionally re-reading
        an earlier block by number, all in one ``call_many``; with ``funding``,
        its FundingState at that block too (see ``_latest``)."""
        wanted = _funding_watch(funding)
        with_code = not self._code_checked
        calls = self._deployment_calls(with_code)
        calls.append(
            self._call_params(self.profile.domain.verifying_contract, encode_call("pauseLevel", ()))
        )
        for provider_id in providers:
            calls += self._identity_calls(provider_id)
        calls += [self._session_call(session_id) for session_id in sessions]
        if wanted is not None:
            calls += [
                self._call_params(self.profile.registry_address, _binding_data(wanted[0])),
                ("eth_getCode", [address_hex(wanted[1])]),
            ]
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
            session_id: _session(raw)
            for session_id, raw in zip(sessions, results[: len(sessions)], strict=True)
        }
        state = None
        if wanted is not None:
            binding, code = results[len(sessions) :]
            state = FundingState(*wanted, _bound(binding), bool(_data(code)))
        return block, again, pause, identities, states, state

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

    def opening_transaction(self, session_id: bytes, first: int, last: int) -> bytes | None:
        """The transaction whose SessionOpened names ``session_id``, searched
        from block ``first`` through ``last`` but over no more than 9,999
        blocks (_LOG_SPAN), or ``None`` when there is none.

        One eth_getLogs filtered by the pinned proxy and the event's indexed
        sessionId. Its answer is a lead, never a proof: prove the transaction
        by its receipt (``reconcile``). A session opens once (MPPI.sol open
        refuses SessionExists), so two such transactions are inconsistent.
        """
        fixed_bytes(session_id, 32, field="session_id")
        uint64(first, field="first block")
        last = min(uint64(last, field="last block"), first + _LOG_SPAN - 1)
        if last < first:
            return None
        proxy, topic = self.profile.domain.verifying_contract, event_topic("SessionOpened")
        query = {
            "address": address_hex(proxy),
            "topics": ["0x" + topic.hex(), "0x" + session_id.hex()],
            "fromBlock": hex(first),
            "toBlock": hex(last),
        }
        (raw,) = self._check_network(self._many(self._network_calls() + [("eth_getLogs", [query])]))
        if not isinstance(raw, list):
            raise ChainEvidenceError("RPC logs missing")
        found = set()
        for value in raw:
            log = _object(value)
            if log.get("removed") is True:
                continue
            topics = log.get("topics")
            if (
                _data(log.get("address"), 20) != proxy
                or not isinstance(topics, list)
                or len(topics) < 2
                or _data(topics[0], 32) != topic
                or _data(topics[1], 32) != session_id
                or not first <= _quantity(log.get("blockNumber")) <= last
            ):
                raise ChainEvidenceError("RPC log differs from its filter")
            found.add(_data(log.get("transactionHash"), 32))
        if len(found) > 1:
            raise ChainEvidenceError("required MPPI event missing or ambiguous")
        return found.pop() if found else None

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
