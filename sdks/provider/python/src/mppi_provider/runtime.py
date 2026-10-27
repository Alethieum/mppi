"""Native provider payment services composed into a caller-owned asyncio server.

One Mppi instance is the authoritative live payment state for its sessions. The
host must route operations for those sessions to that state owner, including
when its frontend uses separate Control and Complete connections. This module
does not coordinate independent processes or recover missing private history.
"""

from __future__ import annotations

import asyncio
import contextvars
import math
import secrets
import threading
import time
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import grpc
from grpc.aio import Server, ServicerContext
from mppi.v1 import mppi_pb2 as pb
from mppi.v1 import mppi_pb2_grpc as services
from mppi_protocol._observers import Observers
from mppi_protocol.accounting import RunAccounting, RunStatus, report_transition
from mppi_protocol.amounts import (
    MAX_UINT64,
    RATE_SCALE,
    Rates,
    TokenCounts,
    cumulative_usage,
    priced_numerator,
    remaining_funding_tokens,
    required_coverage,
    uint64,
)
from mppi_protocol.capabilities import TransactionWallet, WalletCall
from mppi_protocol.catalog import Catalog, parse_catalog
from mppi_protocol.chain import ChainEvidenceError, DeploymentProfile, OpeningEvidence
from mppi_protocol.contract import ContractCalls, OperationKind, PreparedOperation
from mppi_protocol.errors import ValidationCode, ValidationError
from mppi_protocol.follower import ChainFollower, ChainSnapshot
from mppi_protocol.operations import reconcile_operation, submit_operation
from mppi_protocol.signing import (
    verify_close_auth,
    verify_reconnect_auth,
    verify_run_auth,
    verify_voucher,
)
from mppi_protocol.types import (
    AccountingResult,
    ChainObservation,
    OperationResult,
    OperationState,
    PauseLevel,
    PaymentCode,
    PaymentFailure,
    Readiness,
    RunContext,
    SessionPage,
    SessionStatus,
    SessionView,
    ShutdownResult,
    fixed_bytes,
)
from mppi_protocol.wire import (
    MAX_CONTROL_BYTES,
    MAX_MESSAGE_BYTES,
    ByteQueue,
    descriptor_from_wire,
    descriptor_to_wire,
    observation_to_wire,
    payment_status,
    validate_json,
    validate_message,
    validate_recovery_snapshot,
)

from .config import ProviderConfig
from .discovery import DiscoveryDocuments
from .hooks import HookError, Hooks, ObserverStats, OperationTiming
from .interfaces import (
    CoverageProvider,
    CoverageResult,
    DiscoveryResponse,
    ExecutionProvider,
    FinalAccountingResult,
    PreparedRequest,
    RecoveryEvidence,
    RecoveryOperation,
    RecoveryRun,
)

_ZERO = TokenCounts(0, 0, 0)
_RUN_STATUS = {
    RunStatus.COMPLETED: pb.RUN_STATUS_COMPLETED,
    RunStatus.FAILED: pb.RUN_STATUS_FAILED,
    RunStatus.CANCELLED: pb.RUN_STATUS_CANCELLED,
}
_SESSION_STATUS = {
    SessionStatus.OPEN: pb.SESSION_STATUS_OPEN,
    SessionStatus.CLOSING: pb.SESSION_STATUS_CLOSING,
    SessionStatus.CLOSED: pb.SESSION_STATUS_CLOSED,
}


# Calldata the runtime prepares, in bytes. Every argument has a fixed size, so
# these are exact for every session, the worst case included. One settle or
# close: the 4-byte selector, the descriptor's seven 32-byte words, the
# cumulative and capture words, the signature's offset and length words, and its
# 65 bytes padded to 96 (452; a refund-only close has no signature, 356). A
# settleBatch: the selector, four head offsets and four array lengths (260), then
# per session the descriptor's seven words, the two amount words, and the
# signature's offset, length and 96 padded bytes (448).
_SETTLE_CALL_BYTES = 4 + 7 * 32 + 2 * 32 + 2 * 32 + 96
_BATCH_CALL_BYTES = 4 + 4 * 32 + 4 * 32
_BATCH_ITEM_BYTES = 7 * 32 + 2 * 32 + 2 * 32 + 96
# A wallet that states no max_call_bytes gets the Circle adapter's bound.
_MAX_CALL_BYTES = 61_440


def _successor(reference: str) -> str:
    """The reference a close takes after one mined and reverted: the same close
    reference with its retry count after a dot, ``close:<id>.1``, ``.2`` and so
    on. It derives from the reverted reference alone, so a runtime restored from
    evidence older than a retry derives the same successor, and the wallet's
    binding of it continues that retry rather than sending another."""
    base, dot, count = reference.rpartition(".")
    if dot and count.isascii() and count.isdigit() and str(int(count)) == count:
        retries = int(count)
        if retries < 2**64 - 1:
            return f"{base}.{retries + 1}"
    return reference + ".1"


def _batch_sessions(max_call_bytes: int) -> int:
    """The most sessions one batch holds within ``max_call_bytes``: 260 + 448 n
    bytes. Circle's 61,440 hold (61,440 - 260) // 448 = 136 sessions in 61,188
    bytes; 137 would take 61,636. At least one: a single session is collected by
    settle, which fits every bound the runtime accepts."""
    return max(1, (max_call_bytes - _BATCH_CALL_BYTES) // _BATCH_ITEM_BYTES)


class UpstreamFailure(Exception):
    """Original provider error JSON, optionally with its HTTP error status.

    For an admitted request the provider must still report final usage/outcome
    through ProviderRuns before this error can carry a definitive Terminal.
    """

    def __init__(self, body: bytes, *, http_status: int | None = None) -> None:
        validate_json(body)
        if http_status is not None and (
            type(http_status) is not int or not 400 <= http_status <= 599
        ):
            raise ValidationError(ValidationCode.INVALID_VALUE, "invalid upstream HTTP status")
        self.body = body
        self.http_status = http_status
        super().__init__("provider inference failed")


class _RpcFailure(Exception):
    def __init__(
        self,
        code: grpc.StatusCode,
        reason: str,
        payment: PaymentFailure | None = None,
        detail: str | None = None,
    ) -> None:
        self.code, self.reason, self.payment, self.detail = code, reason, payment, detail
        super().__init__(reason if detail is None else f"{reason}: {detail}")


def _not_ready(reason: str, sid: bytes | None = None, rid: bytes | None = None) -> _RpcFailure:
    return _payment(PaymentFailure(PaymentCode.SESSION_NOT_READY, reason, sid, rid))


def _payment(failure: PaymentFailure) -> _RpcFailure:
    code, reason, _ = payment_status(failure)
    return _RpcFailure(code, reason, failure)


def _inconsistent(sid: bytes, check: str) -> _RpcFailure:
    """The evidence contradicts itself, the chain, the wallet or a session
    already held. The payment failure stays INCONSISTENT_STATE; the message
    names the check that failed, in the SDK's words, never remote text. Each
    check is listed in the provider runtime tests, which keep every refusal in
    recovery to one literal from that list."""
    return _labelled("INCONSISTENT_STATE", sid, check)


def _missing(sid: bytes, check: str) -> _RpcFailure:
    """Evidence recovery needs is missing or unavailable. The payment failure
    stays STATE_UNAVAILABLE; the message names the check that failed, in the
    SDK's words, never remote text."""
    return _labelled("STATE_UNAVAILABLE", sid, check)


def _invalid(error: ValidationError, check: str) -> ValidationError:
    """An invalid signature in recovery evidence. It stays the ValidationError
    0.1.2 raised, with its code; its message names the check in the form of the
    other refusals, ``SESSION_NOT_READY: <check>``, with Rust's label (where
    Rust refuses it INCONSISTENT_STATE)."""
    return ValidationError(error.code, f"SESSION_NOT_READY: {check}")


def _labelled(reason: str, sid: bytes, check: str) -> _RpcFailure:
    """A SESSION_NOT_READY refusal for ``reason`` whose message names ``check``."""
    failure = _not_ready(reason, sid)
    return _RpcFailure(failure.code, failure.reason, failure.payment, check)


def _unresolved(operation: _PendingOperation | None) -> bool:
    """An outcome still to reconcile: pending or unknown with no chain evidence."""
    return (
        operation is not None
        and operation.result.state in (OperationState.PENDING, OperationState.UNKNOWN)
        and operation.result.observation is None
    )


def _agrees(saved: OperationResult, bound: object) -> bool:
    """The wallet's record of a saved operation agrees with it: the same
    reference, the same transaction hash or none on both, the same wallet
    operation id when one was saved, and any id it names well formed."""
    return (
        isinstance(bound, OperationResult)
        and bound.operation_ref == saved.operation_ref
        and bound.tx_hash == saved.tx_hash
        and (
            saved.wallet_operation_id is None
            or bound.wallet_operation_id == saved.wallet_operation_id
        )
        and (
            bound.wallet_operation_id is None
            or (type(bound.wallet_operation_id) is str and bound.wallet_operation_id != "")
        )
    )


class _Timing:
    """One payment step's time by part, in seconds (see OperationTiming)."""

    def __init__(self, operation: str, start: float | None = None) -> None:
        self.operation = operation
        self.start = time.monotonic() if start is None else start
        self.parts = {"coverage": 0.0, "chain": 0.0, "wallet": 0.0}

    @contextmanager
    def part(self, name: str) -> Iterator[None]:
        """Add the time of this block to ``name``."""
        start = time.monotonic()
        try:
            yield
        finally:
            self.parts[name] += time.monotonic() - start

    def report(self, state: str | None) -> OperationTiming:
        # Parts are rounded down and the total to nearest: parts measured
        # within the step never add up to more than its total.
        ms = {part: int(seconds * 1000) for part, seconds in self.parts.items()}
        total = round((time.monotonic() - self.start) * 1000)
        return OperationTiming(
            self.operation,
            state,
            total,
            ms["coverage"],
            ms["chain"],
            ms["wallet"],
            max(0, total - sum(ms.values())),
        )


_TIMING: contextvars.ContextVar[_Timing | None] = contextvars.ContextVar(
    "mppi_provider_timing", default=None
)


@contextmanager
def _timed(part: str) -> Iterator[None]:
    """Add the time of this block to the current collection call's ``part``."""
    timing = _TIMING.get()
    if timing is None:
        yield
    else:
        with timing.part(part):
            yield


class _TimedWallet:
    """A wallet whose execute and reconcile call counts as wallet time."""

    def __init__(self, wallet: TransactionWallet) -> None:
        self._wallet = wallet
        self._started: float | None = None
        self._ended: float | None = None

    @property
    def address(self) -> bytes:
        return self._wallet.address

    def execute(self, call: WalletCall, *, operation_ref: str) -> OperationResult:
        self._started = time.monotonic()
        try:
            return self._wallet.execute(call, operation_ref=operation_ref)
        finally:
            self._ended = time.monotonic()

    def reconcile(self, *, operation_ref: str) -> OperationResult:
        self._started = time.monotonic()
        try:
            return self._wallet.reconcile(operation_ref=operation_ref)
        finally:
            self._ended = time.monotonic()

    def spent(self, now: float) -> float:
        """Its call's time so far: a cancelled caller leaves it running."""
        if self._started is None:
            return 0.0
        return (now if self._ended is None else self._ended) - self._started


class _Handoff:
    """Whether a wallet call handed to a worker thread began (_Sessions._operate).

    Exactly one of ``begin``, in the worker, and ``abandon``, by a cancelled
    caller, takes effect first; each reports whether it did.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._begun = False
        self._abandoned = False

    def begin(self) -> bool:
        with self._lock:
            self._begun = not self._abandoned
            return self._begun

    def abandon(self) -> bool:
        with self._lock:
            self._abandoned = not self._begun
            return self._abandoned


def _sum_counts(left: TokenCounts, right: TokenCounts) -> TokenCounts:
    return TokenCounts(
        left.input_tokens + right.input_tokens,
        left.cache_tokens + right.cache_tokens,
        left.output_tokens + right.output_tokens,
    )


@dataclass(slots=True)
class _Attachment:
    queue: ByteQueue
    nonce: bytes = field(default_factory=lambda: secrets.token_bytes(32))
    live: bool = True
    session: _Session | None = None
    generation: int = 0
    request: bytes | None = None
    challenges: dict[bytes, pb.Challenge] = field(default_factory=dict)
    challenge_bytes: int = 0
    catalogs: dict[bytes, Catalog] = field(default_factory=dict)
    setup_deadline: float | None = None


# How long a waiting run keeps its place ahead of later covered runs, and how
# often a head the agent has not covered repeats its request: a few voucher
# round trips. The turn is also at most half the authorization wait, so a
# covered run never waits out its turn into that wait's expiry.
_TURN = 2.0


@dataclass(slots=True)
class _Queued:
    position: int
    since: float
    # The last coverage request sent for it, when, and the voucher then.
    sent: tuple[bytes, float, int | None] | None = None


@dataclass(slots=True)
class _Run:
    context: RunContext
    signature: bytes
    request: PreparedRequest
    rates: Rates
    accounting: RunAccounting = field(default_factory=lambda: RunAccounting(_ZERO, 0))
    covered_counts: TokenCounts = _ZERO
    admitted: bool = False
    not_admitted: bool = False
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    terminal: pb.Terminal | None = None
    task: asyncio.Task[None] | None = None
    output: ByteQueue | None = None
    meter_seq: int = 0
    unresolved: bool = False
    low_credit_marker: tuple[int, int] | None = None
    # Its place among runs waiting for admission, from its first wait.
    queued: _Queued | None = None
    # This runtime's first refusal of the run's coverage. A host error that then
    # ends its execution reaches the agent as this typed failure.
    refusal: PaymentFailure | None = None


@dataclass(slots=True)
class _PendingOperation:
    prepared: PreparedOperation
    reference: str
    result: OperationResult
    units: dict[bytes, int]


@dataclass(slots=True)
class _Session:
    sid: bytes
    opening: OpeningEvidence
    catalogs: dict[bytes, Catalog]
    publications: dict[bytes, ChainObservation] = field(default_factory=dict)
    runs: dict[bytes, _Run] = field(default_factory=dict)
    voucher: pb.Voucher | None = None
    generation: int = 0
    owner: _Attachment | None = None
    meter_seq: int = 0
    close_pending: bool = False
    close_reference: str | None = None
    inconsistent: bool = False
    history_available: bool = True
    # Every admission is in ``runs``: the session was opened by this runtime, or
    # restored from history its host asserts is current. Only then does a pending
    # run absent from ``runs`` prove it was never admitted.
    admissions_known: bool = True
    operation: _PendingOperation | None = None
    baseline: _PendingOperation | None = None
    close_result: pb.CloseResult | None = None
    confirmed_units: int = 0
    last_collection: float = field(default_factory=time.monotonic)
    # Runs that have waited for admission so far: the next one's position.
    waited: int = 0
    # When maintenance first found this session final (Mppi._final).
    final_since: float | None = None
    # Wakes this session's runs waiting for admission (Mppi._wake). It shares
    # the runtime's lock: asyncio Condition objects may share one Lock
    # (https://docs.python.org/3.12/library/asyncio-sync.html#condition).
    wake: asyncio.Condition = field(kw_only=True)

    @property
    def usage_numerator(self) -> int:
        return sum(
            priced_numerator(run.accounting.counts, run.rates)
            for run in self.runs.values()
            if run.admitted
        )

    @property
    def usage(self) -> int:
        return cumulative_usage((self.usage_numerator,))

    @property
    def units(self) -> int:
        return sum(
            run.accounting.counts.input_tokens + run.accounting.counts.output_tokens
            for run in self.runs.values()
            if run.admitted
        )


class Mppi:
    """Payment runtime; construction/registration starts no listener or RPC.

    Create and use on the host's serving event-loop thread after worker creation.
    Pass GRPC_OPTIONS when the host constructs grpc.aio.server. The caller owns
    the server, wallet credentials, execution integration and durable recovery.
    control_auth_timeout bounds unauthenticated Control setup in seconds; only
    successful signed attachment removes it. It is independent of payment waits.
    closed_retention is how long, in seconds, a final session stays retained
    before it retires: one hour to seven days, 24 hours by default. Within it an
    agent can reattach to read the final state and the host can take a final
    checkpoint. No count of sessions is limited.
    """

    def __init__(
        self,
        config: ProviderConfig,
        *,
        chain: ChainFollower,
        contract: ContractCalls,
        wallet: TransactionWallet,
        coverage: CoverageProvider,
        execution: ExecutionProvider,
        control_auth_timeout: float = 60.0,
        closed_retention: float = 86400.0,
        hooks: Hooks | None = None,
    ) -> None:
        if not isinstance(config, ProviderConfig) or not isinstance(contract, ContractCalls):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "validated provider configuration is required"
            )
        if (
            contract.domain != chain.reader.profile.domain
            or config.chain.chain_id != contract.domain.chain_id
        ):
            raise ValidationError(ValidationCode.INVALID_VALUE, "provider deployment mismatch")
        if wallet.address != config.identity.wallet_address:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "provider wallet differs from configuration"
            )
        if (
            chain.max_age_ms != config.chain.max_age_ms
            or chain.collection_margin_ms != config.payment.collection_margin_ms
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "chain follower timing differs from configuration"
            )
        if (
            isinstance(control_auth_timeout, bool)
            or not isinstance(control_auth_timeout, (int, float))
            or not 0 < control_auth_timeout <= 3600
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE,
                "Control setup timeout must be positive and at most one hour",
            )
        if (
            isinstance(closed_retention, bool)
            or not isinstance(closed_retention, (int, float))
            or not 3600 <= closed_retention <= 7 * 86400
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE,
                "closed session retention must be one hour to seven days",
            )
        if hooks is not None and not isinstance(hooks, Hooks):
            raise TypeError("hooks must be provider Hooks")
        # The largest calldata the wallet accepts in one call, if it states one.
        max_call_bytes = getattr(wallet, "max_call_bytes", _MAX_CALL_BYTES)
        if type(max_call_bytes) is not int or max_call_bytes < _SETTLE_CALL_BYTES:
            raise ValidationError(
                ValidationCode.INVALID_VALUE,
                "wallet max_call_bytes is below one settlement call (452 bytes)",
            )
        self._max_call_bytes = max_call_bytes
        self._observers = Observers(hooks)
        self._control_auth_timeout = float(control_auth_timeout)
        self._closed_retention = float(closed_retention)
        self.config, self.chain, self.contract = config, chain, contract
        self.wallet, self.coverage, self.execution = wallet, coverage, execution
        self.runs = self
        self.sessions = _Sessions(self)
        self._sessions: dict[bytes, _Session] = {}
        # Payment state is read and changed only while holding this lock.
        self._lock = asyncio.Lock()
        # The chain follower's readiness at the last maintenance pass.
        self._readiness: Readiness | None = None
        self._started = False
        self._stopping = False
        self._start_block: int | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._maintenance: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._documents: DiscoveryDocuments | None = None
        self._collecting: set[bytes] = set()
        # Each operation reference prepared or restored, and for each session it
        # covers the Unix time from which that session no longer needs it: None
        # while its checkpoint may still name it; when it retired here; or
        # closed_retention after a later confirmed collection of it superseded
        # the reference. References no covered session needs, each with the
        # time the wallet may forget it from, the latest of those (_forget);
        # that call while it runs; when maintenance may call again after the
        # wallet refused; and the wallet's near_capacity at the last pass.
        self._references: dict[str, dict[bytes, float | None]] = {}
        self._retirable: list[tuple[str, float]] = []
        # The record of each reference no wallet answer in this process has
        # matched (_agrees): restored without a transaction hash, so not looked
        # up at recovery, or kept against a wallet answer that disagreed with
        # it. Its hand-over to retire takes recovery's wallet check (_forget);
        # a matching answer removes it.
        self._unconfirmed: dict[str, OperationResult] = {}
        self._retiring: asyncio.Task[None] | None = None
        self._retire_after: float | None = None
        self._near_capacity = False

    @classmethod
    async def from_config(
        cls,
        config_path: str | Path = "mppi.toml",
        *,
        coverage: CoverageProvider,
        execution: ExecutionProvider,
        testnet: bool = False,
        profile: DeploymentProfile | None = None,
        rpc_env: str | None = None,
        control_auth_timeout: float = 60.0,
        closed_retention: float = 86400.0,
        hooks: Hooks | None = None,
        wallet: TransactionWallet | None = None,
    ) -> Mppi:
        """Compose verified SDK adapters on the host's serving event loop.

        An omitted ``profile`` selects the packaged official Arc profile:
        mainnet when ``testnet=False``, testnet when ``testnet=True``.
        An explicit ``profile`` is a caller-trusted deployment override.
        Network selection must match both the profile and confirmed TOML config.
        Circle uses the authenticated official CLI and the original native
        onboarding wallet-operation record beside the config. ``wallet`` supplies
        another ``TransactionWallet`` instead, such as one whose operator key an
        HSM or KMS holds: its address must be the saved operator, and setup
        cannot verify its account, network or access. ``rpc_env`` selects an RPC
        URL from the provider's environment; otherwise the validated config's
        RPC URL is used.

        Only configuration and remote reads occur. Call ``start`` explicitly
        from the host lifecycle; the provider owns hosting, execution,
        usage reports, termination and retained state. Remote reads run in a
        worker thread, then loop-owned objects are constructed on this loop:
        https://docs.python.org/3.12/library/asyncio-task.html#asyncio.to_thread
        """
        from .setup import _prepare

        config, reader, operator, contract = await asyncio.to_thread(
            _prepare, config_path, testnet=testnet, profile=profile, rpc_env=rpc_env, wallet=wallet
        )
        return cls(
            config,
            chain=ChainFollower(
                reader,
                max_age_ms=config.chain.max_age_ms,
                collection_margin_ms=config.payment.collection_margin_ms,
            ),
            contract=contract,
            wallet=operator,
            coverage=coverage,
            execution=execution,
            control_auth_timeout=control_auth_timeout,
            closed_retention=closed_retention,
            hooks=hooks,
        )

    @property
    def hook_stats(self) -> ObserverStats:
        return self._observers.stats()

    def register_grpc(self, server: Server) -> None:
        """Register before the caller starts its grpc.aio.Server."""
        services.add_ChatServicer_to_server(self, server)
        services.add_ControlServicer_to_server(self, server)

    def discovery(self, path: str) -> DiscoveryResponse | None:
        return self._documents.get(path) if self._documents is not None else None

    async def start(self) -> Readiness:
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise RuntimeError("Mppi belongs to its original serving event loop")
        self._loop = loop
        if self._stopping:
            return Readiness(
                False, (PaymentFailure(PaymentCode.SESSION_NOT_READY, "CONTROL_NOT_READY"),)
            )
        if not self._started:
            try:
                await self.chain.start()
            except ChainEvidenceError:
                pass
            try:
                # Register the watch even when the first chain read failed;
                # the follower retries it without requiring a host restart.
                await self.chain.watch_provider(self.config.identity.provider_id)
                self._initialize()
            except ChainEvidenceError:
                pass
            if self._maintenance is None:
                self._maintenance = asyncio.create_task(self._maintain())
        if not self._started:
            return Readiness(
                False, (PaymentFailure(PaymentCode.SESSION_NOT_READY, "CHAIN_STATE_STALE"),)
            )
        return self.chain.readiness()

    def _initialize(self) -> None:
        snapshot = self.chain.snapshot()
        identity = snapshot.identities.get(self.config.identity.provider_id)
        if identity is None:
            raise ChainEvidenceError("provider identity not yet observed")
        self._documents = DiscoveryDocuments(self.config, identity)
        self._start_block = snapshot.block.observation.block_number
        self._started = True

    async def shutdown(self, deadline: float) -> ShutdownResult:
        """Drain until an absolute loop.time() deadline; do not close escrow."""
        if (
            isinstance(deadline, bool)
            or not isinstance(deadline, (int, float))
            or not 0 <= deadline < float("inf")
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "shutdown requires a finite monotonic deadline"
            )
        self._stopping = True
        async with self._lock:
            for session in self._sessions.values():
                self._stop_session(session)
            self._wake_all()
        while (
            any(self._active(session) for session in self._sessions.values())
            and asyncio.get_running_loop().time() < deadline
        ):
            await asyncio.sleep(min(0.01, max(0, deadline - asyncio.get_running_loop().time())))
        if self._maintenance is not None:
            self._maintenance.cancel()
            await asyncio.gather(self._maintenance, return_exceptions=True)
        for session in self._sessions.values():
            if session.owner is not None:
                session.owner.live = False
                session.owner.queue.close()
        # Execution tasks receive the provider cancellation signal. A task still
        # running at this boundary is not evidence that its reservation ended.
        for task in tuple(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.wait(
                self._tasks, timeout=max(0, deadline - asyncio.get_running_loop().time())
            )
        try:
            await self.chain.stop()
        finally:
            self._observers.close()
        unresolved = tuple(
            run.context for session in self._sessions.values() for run in self._active(session)
        )
        pending = tuple(
            session.operation.result
            for session in self._sessions.values()
            if session.operation is not None
            and session.operation.result.state in (OperationState.PENDING, OperationState.UNKNOWN)
        )
        self._observers.close()
        return ShutdownResult(
            tuple(
                session.sid
                for session in self._sessions.values()
                if self._active(session) or session.inconsistent
            ),
            unresolved,
            pending,
        )

    def _spawn(self, awaitable: Any) -> asyncio.Task[None]:
        task = asyncio.create_task(awaitable)
        self._tasks.add(task)

        def completed(done: asyncio.Task[None]) -> None:
            self._tasks.discard(done)
            if not done.cancelled():
                done.exception()

        task.add_done_callback(completed)
        return task

    @staticmethod
    def _active(session: _Session) -> tuple[_Run, ...]:
        return tuple(
            run
            for run in session.runs.values()
            if run.admitted and run.accounting.final_status is None
        )

    def _final(self, session: _Session) -> bool:
        """Nothing remains to serve, collect or reconcile.

        The follower shows the session closed, which is terminal on chain (every
        payment path reverts ``SessionIsClosed``, MPPI.sol ``_existingSession``);
        no run is active or preparing; every retained operation is resolved, and
        a confirmed close of ours has its result; and it is not inconsistent,
        which waits for its operator however long. A close another exit
        completed (``withdraw``, ``releaseIdle``) is final with no close result.
        """
        current = self.chain.current
        state = current.sessions.get(session.sid) if current is not None else None
        operation = session.operation
        awaiting_result = (
            session.close_result is None
            and operation is not None
            and operation.prepared.kind is OperationKind.CLOSE
            and operation.result.state is OperationState.CONFIRMED
        )
        return (
            state is not None
            and state.status is SessionStatus.CLOSED
            and all(
                run.not_admitted or (run.admitted and run.accounting.final_status is not None)
                for run in session.runs.values()
            )
            and not any(_unresolved(item) for item in (operation, session.baseline))
            and not awaiting_result
            and not session.inconsistent
        )

    def _retire(self) -> None:
        """Retire each session final for closed_retention that no live attachment owns.

        Its watch is released unless a recovery holds it; the host's checkpoints
        keep its history. A retired session is never reinstalled: openings and
        final recovery evidence for a closed session are refused SESSION_CLOSED.
        A session that retires no longer needs any reference it covers, from
        the time it retired: retirement counts as no delay (_references).
        """
        now = time.monotonic()
        retired: set[bytes] = set()
        for session in tuple(self._sessions.values()):
            if not self._final(session):
                session.final_since = None
                continue
            if session.final_since is None:
                session.final_since = now
            owned = session.owner is not None and session.owner.live
            if owned or now - session.final_since < self._closed_retention:
                continue
            del self._sessions[session.sid]
            retired.add(session.sid)
            if session.sid not in self.sessions._watching:
                self.chain.unwatch_session(session.sid)
            self._observers.emit("on_session_retired", session.sid)
        if retired:
            now = time.time()
            for covered in self._references.values():
                for sid in covered.keys() & retired:
                    covered[sid] = now
            self._hand_over()

    def _supersede(self, confirmed: str, sids: Iterable[bytes]) -> None:
        """A confirmed collection supersedes, for each session it covers, every
        other reference that session's checkpoint no longer names (neither its
        operation nor its baseline): a checkpoint taken before then may still
        present one, for closed_retention (the age rule, docs/python-runtime.md)."""
        at = time.time() + self._closed_retention
        held = [self._sessions[sid] for sid in set(sids) if sid in self._sessions]
        for reference, covered in self._references.items():
            if reference == confirmed:
                continue
            for session in held:
                named = (session.operation, session.baseline)
                if covered.get(session.sid, at) is None and all(
                    operation is None or operation.reference != reference for operation in named
                ):
                    covered[session.sid] = at
        self._hand_over()

    def _hand_over(self) -> None:
        """Hand each reference that no covered session needs any longer to
        retirement, due at the latest of their times, as soon as that is known:
        the wallet keeps the time durably."""
        for reference, covered in tuple(self._references.items()):
            times = [at for at in covered.values() if at is not None]
            if len(times) == len(covered):
                del self._references[reference]
                self._retirable.append((reference, max(times, default=0.0)))

    async def _forget(
        self, references: list[tuple[str, float]], unconfirmed: dict[str, OperationResult]
    ) -> None:
        """Have the wallet forget references no session needs, each from its
        due time, when it can (an optional ``retire``): one call at a time, for
        each whole second the references are due in, naming its latest time:
        for any wallet, never before a reference in it is due and less than a
        second after (the Circle adapter keeps whole seconds). After a refusal
        the rest wait for a later pass, at most once per settlement interval, so
        a wallet that refuses for a while loses none and is not asked every
        pass.

        A reference in ``unconfirmed``, whose record no wallet answer in this
        process matched, goes only if the wallet's record of it agrees now, by
        recovery's check (_agrees); otherwise it is kept. Every other one this
        process has reconciled already."""
        retire = getattr(self.wallet, "retire", None)
        if retire is None:
            return
        due: dict[int, list[tuple[str, float]]] = {}
        for reference, at in references:
            record = unconfirmed.get(reference)
            if record is not None:
                try:
                    answer = await asyncio.to_thread(self.wallet.reconcile, operation_ref=reference)
                except Exception:
                    continue
                if not _agrees(record, answer):
                    continue
            due.setdefault(math.floor(at), []).append((reference, at))
        refused: list[tuple[str, float]] = []
        for _, queued in sorted(due.items()):
            # A recovery since the hand-over may have named a reference again
            # (_references): a session needs it, so it is not passed. This reads
            # memory only.
            group = [
                (reference, at) for reference, at in queued if reference not in self._references
            ]
            if not group:
                continue
            if not refused:
                try:
                    await asyncio.to_thread(
                        retire,
                        operation_refs=tuple(reference for reference, _ in group),
                        at=max(at for _, at in group),
                    )
                    continue
                except Exception:
                    pass
            refused.extend(group)
        self._retirable.extend(refused)
        self._retire_after = (
            time.monotonic() + self.config.settlement.interval_ms / 1000 if refused else None
        )

    def _snapshot(self) -> ChainSnapshot:
        if not self._started or self._stopping:
            raise _not_ready("CONTROL_NOT_READY")
        try:
            return self.chain.snapshot()
        except ChainEvidenceError:
            raise _not_ready("CHAIN_STATE_STALE") from None

    def _gate(self, session: _Session, context: RunContext | None = None) -> ChainSnapshot:
        snapshot = self._snapshot()
        ready = self.chain.readiness(session.sid)
        if not ready.ready:
            failure = ready.failures[0]
            raise _payment(
                replace(failure, session_id=session.sid, run_id=context.run_id if context else None)
            )
        if not session.history_available:
            raise _not_ready("STATE_UNAVAILABLE", session.sid, context.run_id if context else None)
        if session.inconsistent:
            raise _not_ready("INCONSISTENT_USAGE", session.sid, context.run_id if context else None)
        if any(run.unresolved for run in session.runs.values()):
            raise _not_ready("STATE_UNAVAILABLE", session.sid, context.run_id if context else None)
        if session.usage < snapshot.sessions[session.sid].settled_cumulative:
            raise _not_ready("INCONSISTENT_STATE", session.sid, context.run_id if context else None)
        if session.close_pending:
            raise _not_ready("CLOSE_PENDING", session.sid, context.run_id if context else None)
        owner = session.owner
        if owner is None or not owner.live or owner.generation != session.generation:
            raise _not_ready("CONTROL_NOT_READY", session.sid, context.run_id if context else None)
        if context is not None and context.control_generation != session.generation:
            raise _not_ready("CONTROL_NOT_READY", session.sid, context.run_id)
        if any(
            run.context.control_generation != session.generation for run in self._active(session)
        ):
            raise _not_ready("STATE_UNAVAILABLE", session.sid, context.run_id if context else None)
        return snapshot

    def _send(self, attachment: _Attachment, message: pb.ControlMessage) -> None:
        validate_message(message)
        try:
            attachment.queue.put_nowait(message.SerializeToString())
        except (asyncio.QueueFull, EOFError):
            self._detach(attachment)
            attachment.queue.close(
                _RpcFailure(grpc.StatusCode.RESOURCE_EXHAUSTED, "Control queue limit")
            )
            raise _RpcFailure(grpc.StatusCode.RESOURCE_EXHAUSTED, "Control queue limit") from None
        kind = message.WhichOneof("kind")
        if kind in ("challenge", "low_credit", "drain"):
            self._observers.emit("on_" + kind, getattr(message, kind))

    def _detach(self, attachment: _Attachment) -> None:
        attachment.live = False
        if attachment.session is not None and attachment.session.owner is attachment:
            for run in self._active(attachment.session):
                run.stop.set()

    def _stop_session(self, session: _Session) -> None:
        for run in self._active(session):
            run.stop.set()
        if session.owner is not None and session.owner.live:
            try:
                self._send(
                    session.owner,
                    pb.ControlMessage(
                        drain=pb.Drain(
                            session_id=session.sid, reason=pb.DRAIN_REASON_PROVIDER_INITIATED
                        )
                    ),
                )
            except _RpcFailure:
                pass

    @staticmethod
    def _wake(session: _Session | None) -> None:
        """Wake the session's runs waiting for admission, holding the lock.

        A waiting run reads only its own session's state and the runtime-wide
        state, so call this after any change to the session; runtime-wide
        changes call ``_wake_all``.
        """
        if session is not None:
            session.wake.notify_all()

    def _wake_all(self) -> None:
        """Wake every waiting run: for shutdown and for a change in chain
        readiness, which every session's admission reads."""
        for session in self._sessions.values():
            session.wake.notify_all()

    async def Session(
        self, requests: AsyncIterator[pb.ControlMessage], rpc: ServicerContext[Any, Any]
    ) -> AsyncIterator[pb.ControlMessage]:
        attachment = _Attachment(
            ByteQueue(wait_ms=self.config.payment.authorization_wait_ms),
            setup_deadline=asyncio.get_running_loop().time() + self._control_auth_timeout,
        )
        self._send(
            attachment,
            pb.ControlMessage(reconnect_challenge=pb.ReconnectChallenge(nonce=attachment.nonce)),
        )
        reader = asyncio.create_task(self._receive_control(attachment, requests))
        try:
            while True:
                yield pb.ControlMessage.FromString(await attachment.queue.get())
        except EOFError:
            return
        except _RpcFailure as error:
            await self._abort(rpc, error)
        finally:
            async with self._lock:
                self._detach(attachment)
                self._wake(attachment.session)
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)

    async def _receive_control(
        self, attachment: _Attachment, requests: AsyncIterator[pb.ControlMessage]
    ) -> None:
        setup_timer = asyncio.timeout_at(attachment.setup_deadline)
        try:
            async with setup_timer:
                async for message in requests:
                    self._require_setup_time(attachment)
                    validate_message(message)
                    kind = message.WhichOneof("kind")
                    if kind == "challenge_request":
                        self._challenge(attachment, message.challenge_request)
                    elif kind == "session_open":
                        await self._opening(attachment, message.session_open)
                    elif kind == "reconnect_request":
                        await self._attach(attachment, message.reconnect_request)
                        setup_timer.reschedule(None)
                    elif kind == "voucher":
                        await self._receive_voucher(attachment, message.voucher)
                    elif kind in ("top_up_notice", "close_request"):
                        async with self._lock:
                            session = self._owner(attachment, getattr(message, kind).session_id)
                            if kind == "close_request":
                                try:
                                    verify_close_auth(
                                        self.contract.domain,
                                        session.opening.descriptor,
                                        message.close_request.signature,
                                    )
                                except ValidationError:
                                    raise _RpcFailure(
                                        grpc.StatusCode.UNAUTHENTICATED, "invalid close proof"
                                    ) from None
                                if session.close_result is not None:
                                    # A confirmed result may have been lost on the
                                    # previous stream. Replay only after checking
                                    # current ownership and the original CloseAuth;
                                    # no new operation or close event is needed.
                                    self._send(
                                        attachment,
                                        pb.ControlMessage(close_result=session.close_result),
                                    )
                                    continue
                                if session.close_reference is None:
                                    session.close_reference = "close:" + secrets.token_hex(32)
                                if not session.close_pending:
                                    # Accepted once: its runs stop and its owner
                                    # is told once (Drain).
                                    session.close_pending = True
                                    self._stop_session(session)
                                self._schedule_collection(session.sid, close=True)
                            # TopUpNotice is only a hint. The follower is the source
                            # of D; no credit or voucher acknowledgement is added.
                            self._wake(session)
                    else:
                        raise _RpcFailure(
                            grpc.StatusCode.INVALID_ARGUMENT, "invalid Control direction"
                        )
        except TimeoutError:
            self._observers.emit("on_error", HookError("control", "DEADLINE_EXCEEDED"))
            attachment.queue.close(
                _RpcFailure(
                    grpc.StatusCode.DEADLINE_EXCEEDED, "Control authentication deadline exceeded"
                )
                if setup_timer.expired()
                else _not_ready("STATE_UNAVAILABLE")
            )
        except _RpcFailure as error:
            self._observers.emit("on_error", HookError("control", error.code.name))
            attachment.queue.close(error)
        except ValidationError:
            self._observers.emit("on_error", HookError("control", "INVALID_ARGUMENT"))
            attachment.queue.close(
                _RpcFailure(grpc.StatusCode.INVALID_ARGUMENT, "invalid Control message")
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            self._observers.emit("on_error", HookError("control", "FAILED_PRECONDITION"))
            attachment.queue.close(_not_ready("STATE_UNAVAILABLE"))
        finally:
            async with self._lock:
                self._detach(attachment)
                self._wake(attachment.session)
            attachment.queue.close()

    @staticmethod
    def _require_setup_time(attachment: _Attachment) -> None:
        # asyncio may deliver an already-ready awaitable before polling its timer.
        # Check the absolute boundary explicitly before handling or committing it.
        if (
            attachment.setup_deadline is not None
            and asyncio.get_running_loop().time() >= attachment.setup_deadline
        ):
            raise _RpcFailure(
                grpc.StatusCode.DEADLINE_EXCEEDED, "Control authentication deadline exceeded"
            )

    def _challenge(self, attachment: _Attachment, request: pb.ChallengeRequest) -> None:
        old = attachment.challenges.get(request.request_id)
        if old is not None:
            if old.model != request.model:
                raise _RpcFailure(
                    grpc.StatusCode.INVALID_ARGUMENT, "conflicting challenge request id"
                )
            self._send(attachment, pb.ControlMessage(challenge=old))
            return
        snapshot = self._snapshot()
        identity = snapshot.identities[self.config.identity.provider_id]
        if identity.catalog is None:
            raise _not_ready("STATE_UNAVAILABLE")
        try:
            identity.catalog.rates_for(request.model)
        except KeyError:
            raise _RpcFailure(grpc.StatusCode.NOT_FOUND, "model unavailable") from None
        challenge = pb.Challenge(
            request_id=request.request_id,
            model=request.model,
            rate_card_hash=identity.catalog.rate_card_hash,
            provider_id=identity.provider_id.to_bytes(32, "big"),
            contract=self.contract.domain.verifying_contract,
            chain_id=self.contract.domain.chain_id,
            operator=identity.owner,
            cover_output_tokens=self.config.payment.output_buffer_tokens,
            suggest_session_tokens=self.config.payment.suggest_session_tokens,
        )
        if attachment.challenge_bytes + challenge.ByteSize() > MAX_CONTROL_BYTES:
            raise _RpcFailure(grpc.StatusCode.RESOURCE_EXHAUSTED, "challenge history limit")
        if identity.catalog.rate_card_hash not in attachment.catalogs and (
            sum(len(item.canonical_bytes) for item in attachment.catalogs.values())
            + len(identity.catalog.canonical_bytes)
            > MAX_MESSAGE_BYTES
        ):
            raise _RpcFailure(grpc.StatusCode.RESOURCE_EXHAUSTED, "challenge catalog limit")
        attachment.challenges[request.request_id] = challenge
        attachment.catalogs[identity.catalog.rate_card_hash] = identity.catalog
        attachment.challenge_bytes += challenge.ByteSize()
        self._send(attachment, pb.ControlMessage(challenge=challenge))

    async def _opening(self, attachment: _Attachment, notice: pb.SessionOpen) -> None:
        # An opening transaction can precede a later catalog update in its block.
        # Retain exact guidance bytes and test candidates against canonical
        # opening evidence instead of substituting the end-of-block catalog.
        evidence = None
        for catalog in (*attachment.catalogs.values(), None):
            try:
                evidence = await asyncio.to_thread(
                    self.chain.reader.opening,
                    notice.open_tx,
                    notice.session_id,
                    accepted_catalog=catalog,
                )
                break
            except ChainEvidenceError:
                continue
        if evidence is None:
            raise _not_ready("STATE_UNAVAILABLE", notice.session_id)
        descriptor = evidence.descriptor
        if (
            descriptor.provider_id != self.config.identity.provider_id
            or descriptor.operator != self.wallet.address
        ):
            raise _RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "opening is not for this provider")
        if notice.HasField("agent_id"):
            agent_wallet = await asyncio.to_thread(
                self.chain.reader.agent_wallet,
                int.from_bytes(notice.agent_id, "big"),
                evidence.observation,
            )
            if agent_wallet != descriptor.agent:
                raise _RpcFailure(
                    grpc.StatusCode.UNAUTHENTICATED, "agent identity differs from funding wallet"
                )
        # A refused opening releases the watch it took, unless the session is
        # held or another opening or recovery still holds it (_Sessions._watched).
        with self.sessions._watched(notice.session_id):
            state = await self.chain.watch_session(notice.session_id)
            async with self._lock:
                existing = self._sessions.get(notice.session_id)
                if existing is not None:
                    if (
                        existing.opening.descriptor != descriptor
                        or existing.opening.tx_hash != evidence.tx_hash
                    ):
                        raise _not_ready("INCONSISTENT_STATE", notice.session_id)
                    return
                if state.status is SessionStatus.CLOSED:
                    # Nothing about a closed session can be served or collected.
                    # Installing one again (it may have retired) would be a
                    # fresh session with no runs.
                    raise _not_ready("SESSION_CLOSED", notice.session_id)
                if attachment.session is not None:
                    raise _RpcFailure(
                        grpc.StatusCode.INVALID_ARGUMENT, "Control already bound to a session"
                    )
                initial = evidence.initial_catalog
                self._sessions[notice.session_id] = _Session(
                    notice.session_id,
                    evidence,
                    {initial.rate_card_hash: initial},
                    history_available=self._start_block is not None
                    and evidence.observation.block_number > self._start_block,
                    wake=asyncio.Condition(self._lock),
                )

        self._observers.emit("on_session_open", notice)

    def _owner(self, attachment: _Attachment, sid: bytes) -> _Session:
        session = attachment.session
        if (
            session is None
            or session.sid != sid
            or session.owner is not attachment
            or not attachment.live
            or attachment.generation != session.generation
        ):
            raise _RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "Control does not own session")
        return session

    async def _receive_voucher(self, attachment: _Attachment, voucher: pb.Voucher) -> None:
        async with self._lock:
            session = self._owner(attachment, voucher.session_id)
            if voucher.cumulative <= self._snapshot().sessions[session.sid].deposit:
                self._voucher(session, voucher)
                self._wake(session)
                return
            # A confirmed top-up can reach the agent before our periodic follower.
            # Authenticate before spending an RPC; neither a notice nor this proof
            # supplies deposit authority. Ordinary covered vouchers stay RPC-free.
            self._verify_voucher(session, voucher)
            self._gate(session)

        # Refresh once, bounded by the configured chain freshness policy. Do not
        # hold accounting's lock or wait for funding: funded execution and its
        # immediate coverage-denial boundary continue independently.
        async with asyncio.timeout(self.config.chain.max_age_ms / 1000):
            await self.chain.refresh()

        async with self._lock:
            session = self._owner(attachment, voucher.session_id)
            self._gate(session)
            self._voucher(session, voucher)
            self._wake(session)

    def _verify_voucher(self, session: _Session, voucher: pb.Voucher) -> None:
        if voucher.session_id != session.sid:
            raise _RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "voucher session mismatch")
        try:
            verify_voucher(
                self.contract.domain,
                session.opening.descriptor,
                voucher.cumulative,
                voucher.signature,
            )
        except ValidationError:
            raise _RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "invalid voucher proof") from None

    def _voucher(self, session: _Session, voucher: pb.Voucher, *, observe: bool = True) -> None:
        self._verify_voucher(session, voucher)
        snapshot = self._snapshot()
        if voucher.cumulative > snapshot.sessions[session.sid].deposit:
            raise _RpcFailure(grpc.StatusCode.INVALID_ARGUMENT, "voucher exceeds confirmed deposit")
        if session.voucher is None or voucher.cumulative > session.voucher.cumulative:
            if voucher.cumulative < max(
                session.usage, snapshot.sessions[session.sid].settled_cumulative
            ):
                raise _not_ready("INCONSISTENT_STATE", session.sid)
            session.voucher = pb.Voucher.FromString(voucher.SerializeToString())
            if observe:
                self._observers.emit("on_voucher", voucher)

    async def _attach(self, attachment: _Attachment, request: pb.ReconnectRequest) -> None:
        async with self._lock:
            self._require_setup_time(attachment)
            session = self._sessions.get(request.session_id)
            if session is None:
                # No public opening proof can establish private usage history.
                raise _not_ready("STATE_UNAVAILABLE", request.session_id)
            encoded = request.SerializeToString(deterministic=True)
            if attachment.request is not None:
                if attachment.request != encoded or session.owner is not attachment:
                    raise _RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "replayed attachment proof")
                self._send(
                    attachment,
                    pb.ControlMessage(session_state=self._session_state(session, request)),
                )
                return
            if request.nonce != attachment.nonce:
                raise _RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "invalid attachment nonce")
            try:
                verify_reconnect_auth(
                    self.contract.domain,
                    session.opening.descriptor,
                    request.nonce,
                    request.signature,
                )
            except ValidationError:
                raise _RpcFailure(
                    grpc.StatusCode.UNAUTHENTICATED, "invalid attachment proof"
                ) from None
            if not session.history_available:
                raise _not_ready("STATE_UNAVAILABLE", session.sid)
            # Unless every admission is known, an absent pending run is unknown, never
            # NOT_ADMITTED: a restored checkpoint older than the host's newest may
            # lack a run that was served. Nor before any Control generation:
            # Complete needs generation 1 or more, so a session a new process
            # re-created from its opening (a start block can lag a crashed one's)
            # never admitted a run an agent names then.
            if request.last_meter_seq > session.meter_seq or (
                (not session.admissions_known or session.generation == 0)
                and any(rid not in session.runs for rid in request.pending_run_ids)
            ):
                raise _not_ready("STATE_UNAVAILABLE", session.sid)
            generation = uint64(session.generation + 1, field="control generation", positive=True)
            # Stage every fallible check and queue admission before replacing a
            # live owner. A failed snapshot must not revoke its funded work.
            staged = replace(
                session,
                generation=generation,
                owner=replace(attachment, generation=generation),
            )
            if request.HasField("latest_voucher"):
                self._voucher(staged, request.latest_voucher, observe=False)
            response = pb.ControlMessage(session_state=self._session_state(staged, request))
            self._require_setup_time(attachment)
            self._send(attachment, response)
            if session.owner is not None:
                self._detach(session.owner)
                session.owner.queue.close(_not_ready("CONTROL_NOT_READY", session.sid))
            advanced = staged.voucher != session.voucher
            session.voucher = staged.voucher
            session.generation = generation
            session.owner = attachment
            attachment.session, attachment.generation, attachment.request = (
                session,
                generation,
                encoded,
            )
            attachment.setup_deadline = None
            if advanced and session.voucher is not None:
                self._observers.emit("on_voucher", session.voucher)
            self._wake(session)

    def _session_state(
        self, session: _Session, request: pb.ReconnectRequest, *, private: bool = False
    ) -> pb.SessionState:
        snapshot = self._snapshot()
        state = snapshot.sessions[session.sid]
        if self._active(session) and not private:
            raise _not_ready("STATE_UNAVAILABLE", session.sid)
        try:
            self._gate(session)
            ready = True
        except _RpcFailure:
            ready = False
        identity = snapshot.identities.get(self.config.identity.provider_id)
        catalogs = dict(session.catalogs)
        if identity is not None and identity.catalog is not None:
            catalogs[identity.catalog.rate_card_hash] = identity.catalog
        usage: dict[tuple[str, bytes], tuple[TokenCounts, _Run]] = {}
        for run in session.runs.values():
            if run.admitted:
                key = (run.context.model, run.context.rate_card_hash)
                counts, representative = usage.get(key, (_ZERO, run))
                usage[key] = (_sum_counts(counts, run.accounting.counts), representative)
        result = pb.SessionState(
            session_id=session.sid,
            nonce=request.nonce,
            control_generation=session.generation,
            ready=ready and not private,
            close_pending=session.close_pending,
            descriptor=descriptor_to_wire(session.opening.descriptor),
            meter_seq=session.meter_seq,
            initial_rate_card_hash=session.opening.initial_catalog.rate_card_hash,
            deposit=state.deposit,
            settled_cumulative=state.settled_cumulative,
            status=_SESSION_STATUS[state.status],
            chain_block_number=snapshot.block.observation.block_number,
            chain_block_hash=snapshot.block.observation.block_hash,
        )
        if session.voucher is not None:
            result.accepted_voucher.CopyFrom(session.voucher)
        if identity is not None and identity.catalog is not None:
            result.current_rate_card_hash = identity.catalog.rate_card_hash
        for catalog in catalogs.values():
            result.catalogs.add(
                rate_card_hash=catalog.rate_card_hash, catalog=catalog.canonical_bytes
            )
        for (model, rate_hash), (counts, run) in usage.items():
            result.usage_by_price.add(
                model=model,
                rate_card_hash=rate_hash,
                input_tokens=counts.input_tokens,
                cache_tokens=counts.cache_tokens,
                output_tokens=counts.output_tokens,
                acceptance_run_id=run.context.run_id,
                acceptance_control_generation=run.context.control_generation,
                acceptance_signature=run.signature,
            )
        # Return all known definitive outcomes. An unknown id is NOT_ADMITTED only
        # when every admission is known: the agent records a run before its
        # Complete arrives, so a refused or lost one stays pending there although
        # this provider never held it. Nothing is recorded for it, and the
        # reattachment's new generation refuses a late Complete at the gate.
        if session.admissions_known and not private:
            for rid in request.pending_run_ids:
                if rid not in session.runs:
                    result.run_states.add(
                        run_id=rid,
                        model="",
                        status=pb.RUN_STATUS_NOT_ADMITTED,
                        input_tokens=0,
                        cache_tokens=0,
                        output_tokens=0,
                    )
        for run in session.runs.values():
            if run.not_admitted:
                result.run_states.add(
                    run_id=run.context.run_id,
                    model="",
                    status=pb.RUN_STATUS_NOT_ADMITTED,
                    input_tokens=0,
                    cache_tokens=0,
                    output_tokens=0,
                )
            elif run.accounting.final_status is not None:
                counts = run.accounting.counts
                result.run_states.add(
                    run_id=run.context.run_id,
                    model=run.context.model,
                    rate_card_hash=run.context.rate_card_hash,
                    control_generation=run.context.control_generation,
                    auth_signature=run.signature,
                    status=_RUN_STATUS[run.accounting.final_status],
                    input_tokens=counts.input_tokens,
                    cache_tokens=counts.cache_tokens,
                    output_tokens=counts.output_tokens,
                )
        if private:
            validate_recovery_snapshot(result)
        else:
            validate_message(result)
        return result

    async def Completions(
        self, request: pb.CompleteRequest, rpc: ServicerContext[Any, Any]
    ) -> AsyncIterator[pb.ChatChunk]:
        arrived = time.monotonic()
        run: _Run | None = None
        try:
            validate_message(request)
            validate_json(request.payload, model=request.model, stream=request.stream)
            context = RunContext(
                request.session_id,
                request.run_id,
                request.control_generation,
                request.model,
                request.rate_card_hash,
            )
            async with self._lock:
                session = self._sessions.get(context.session_id)
                if session is None:
                    raise _RpcFailure(
                        grpc.StatusCode.UNAUTHENTICATED, "session authority unavailable"
                    )
                try:
                    verify_run_auth(
                        self.contract.domain,
                        session.opening.descriptor,
                        context,
                        request.auth_signature,
                    )
                except ValidationError:
                    raise _RpcFailure(
                        grpc.StatusCode.UNAUTHENTICATED, "invalid run proof"
                    ) from None
                if context.run_id in session.runs:
                    raise _RpcFailure(grpc.StatusCode.ALREADY_EXISTS, "run id already observed")
                snapshot = self._gate(session, context)
                rates = self._prices(session, context, snapshot)
                run = _Run(
                    context,
                    request.auth_signature,
                    PreparedRequest(request.payload, request.stream),
                    rates,
                )
                # Pending preparations hold caller JSON, but reserve no money.
                # Bound their retained input independently of network buffering.
                if (
                    sum(
                        len(item.request.payload)
                        for owner in self._sessions.values()
                        for item in owner.runs.values()
                    )
                    + len(request.payload)
                    > MAX_MESSAGE_BYTES
                ):
                    raise _RpcFailure(grpc.StatusCode.RESOURCE_EXHAUSTED, "retained request limit")
                self._history_fits(session, candidate=run)
                session.runs[context.run_id] = run
                catalog = snapshot.identities[self.config.identity.provider_id].catalog
                assert catalog is not None
                session.catalogs[context.rate_card_hash] = catalog
                session.publications[context.rate_card_hash] = snapshot.identities[
                    self.config.identity.provider_id
                ].observation
            admission = _Timing("admission", arrived)
            try:
                with admission.part("coverage"):
                    try:
                        bound = await self.coverage(run.request, context)
                    except UpstreamFailure:
                        raise
                    except Exception:
                        # A host error never reaches the agent as text, as in
                        # the Rust runtime (services.rs coverage).
                        raise _not_ready("STATE_UNAVAILABLE", session.sid, context.run_id) from None
                if (
                    not isinstance(bound, TokenCounts)
                    or bound.output_tokens < self.config.payment.output_buffer_tokens
                ):
                    raise ValidationError(
                        ValidationCode.INVALID_VALUE,
                        "provider must supply prefill and configured output coverage",
                    )
                await self._admit(session, run, bound)
            finally:
                self._observers.emit(
                    "on_timing", admission.report("admitted" if run.admitted else None)
                )
            run.output = ByteQueue(wait_ms=self.config.payment.authorization_wait_ms)
            run.task = self._spawn(self._execute(run))
            while True:
                yield pb.ChatChunk.FromString(await run.output.get())
        except EOFError:
            return
        except UpstreamFailure as error:
            self._observers.emit("on_error", HookError("complete", "FAILED_PRECONDITION"))
            yield self._upstream(error)
            await rpc.abort(
                grpc.StatusCode.FAILED_PRECONDITION, "provider request preparation failed"
            )
        except _RpcFailure as error:
            await self._abort(rpc, error)
        except ValidationError:
            self._observers.emit("on_error", HookError("complete", "INVALID_ARGUMENT"))
            await rpc.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "invalid request or provider requirement"
            )
        finally:
            if run is not None:
                async with self._lock:
                    if not run.admitted:
                        run.not_admitted = True
                        run.request = PreparedRequest(b"", run.request.stream)
                    elif run.accounting.final_status is None:
                        run.stop.set()
                    self._wake(session)

    def _prices(self, session: _Session, context: RunContext, snapshot: ChainSnapshot) -> Rates:
        identity = snapshot.identities.get(self.config.identity.provider_id)
        if identity is None or identity.catalog is None:
            raise _not_ready("CHAIN_STATE_STALE", session.sid, context.run_id)
        catalog = identity.catalog
        if catalog.rate_card_hash != context.rate_card_hash:
            raise _payment(
                PaymentFailure(
                    PaymentCode.SESSION_NOT_READY,
                    "PRICE_CHANGED",
                    session.sid,
                    context.run_id,
                    context.rate_card_hash,
                    catalog.rate_card_hash,
                )
            )
        try:
            return catalog.rates_for(context.model)
        except KeyError:
            raise _RpcFailure(grpc.StatusCode.NOT_FOUND, "model unavailable") from None

    def _history_fits(self, session: _Session, *, candidate: _Run | None = None) -> None:
        """Reserve enough wire space for every admitted run's eventual final.

        Upper-bound mutable uint64 fields at their largest encoding before
        granting work. No snapshot/history truncation is permitted later.
        """
        record = pb.SessionState(
            session_id=session.sid,
            nonce=bytes(32),
            control_generation=MAX_UINT64,
            ready=False,
            close_pending=False,
            descriptor=descriptor_to_wire(session.opening.descriptor),
            meter_seq=MAX_UINT64,
            initial_rate_card_hash=session.opening.initial_catalog.rate_card_hash,
            current_rate_card_hash=bytes(32),
            deposit=MAX_UINT64,
            settled_cumulative=MAX_UINT64,
            status=pb.SESSION_STATUS_OPEN,
            chain_block_number=MAX_UINT64,
            chain_block_hash=bytes(32),
            accepted_voucher=pb.Voucher(
                session_id=session.sid, cumulative=MAX_UINT64, signature=bytes(65)
            ),
        )
        catalogs = dict(session.catalogs)
        if candidate is not None:
            current = self._snapshot().identities[self.config.identity.provider_id].catalog
            if current is None:
                raise _not_ready("STATE_UNAVAILABLE", session.sid)
            catalogs[current.rate_card_hash] = current
        for catalog in catalogs.values():
            record.catalogs.add(
                rate_card_hash=catalog.rate_card_hash, catalog=catalog.canonical_bytes
            )
        buckets: set[tuple[str, bytes]] = set()
        runs = (*session.runs.values(), *((candidate,) if candidate is not None else ()))
        for run in runs:
            context = run.context
            record.run_states.add(
                run_id=context.run_id,
                model=context.model,
                rate_card_hash=context.rate_card_hash,
                control_generation=MAX_UINT64,
                auth_signature=run.signature,
                status=pb.RUN_STATUS_COMPLETED,
                input_tokens=MAX_UINT64,
                cache_tokens=MAX_UINT64,
                output_tokens=MAX_UINT64,
            )
            key = (context.model, context.rate_card_hash)
            if key not in buckets:
                buckets.add(key)
                record.usage_by_price.add(
                    model=context.model,
                    rate_card_hash=context.rate_card_hash,
                    input_tokens=MAX_UINT64,
                    cache_tokens=MAX_UINT64,
                    output_tokens=MAX_UINT64,
                    acceptance_run_id=context.run_id,
                    acceptance_control_generation=MAX_UINT64,
                    acceptance_signature=run.signature,
                )
        if pb.ControlMessage(session_state=record).ByteSize() > MAX_MESSAGE_BYTES:
            raise _RpcFailure(grpc.StatusCode.RESOURCE_EXHAUSTED, "complete recovery history limit")

    def _required_rows(
        self, session: _Session, candidate: _Run | None = None, bound: TokenCounts | None = None
    ) -> tuple[list[pb.RequiredTokens], int]:
        grouped: dict[tuple[str, bytes], tuple[TokenCounts, Rates]] = {}
        for run in session.runs.values():
            if run is candidate:
                assert bound is not None
                counts = bound
            elif run.admitted:
                counts = (
                    run.accounting.counts
                    if run.accounting.final_status is not None
                    else run.covered_counts
                )
            else:
                continue
            key = (run.context.model, run.context.rate_card_hash)
            previous, _ = grouped.get(key, (_ZERO, run.rates))
            grouped[key] = (_sum_counts(previous, counts), run.rates)
        rows = [
            pb.RequiredTokens(
                model=model,
                rate_card_hash=rate_hash,
                input_tokens=counts.input_tokens,
                cache_tokens=counts.cache_tokens,
                output_tokens=counts.output_tokens,
            )
            for (model, rate_hash), (counts, _) in grouped.items()
        ]
        amount = required_coverage(
            priced_numerator(counts, rates) for counts, rates in grouped.values()
        )
        return rows, amount

    def _guidance(
        self, session: _Session, run: _Run, rows: list[pb.RequiredTokens], snapshot: ChainSnapshot
    ) -> pb.ControlMessage:
        state = snapshot.sessions[session.sid]
        guidance = pb.LowCredit(
            session_id=session.sid,
            model=run.context.model,
            rate_card_hash=run.context.rate_card_hash,
        )
        available = remaining_funding_tokens(
            state.deposit,
            session.usage_numerator,
            (item.accounting.remaining_numerator for item in self._active(session)),
            run.rates,
        )
        if available is not None:
            guidance.remaining_tokens = available
            guidance.suggest_top_up_tokens = self.config.payment.suggest_session_tokens
        guidance.coverage_request.CopyFrom(
            pb.CoverageRequest(
                run_id=run.context.run_id,
                control_generation=run.context.control_generation,
                required_by_price=rows,
            )
        )
        result = pb.ControlMessage(low_credit=guidance)
        validate_message(result)
        return result

    def _ordinary_low_credit(self, session: _Session, run: _Run) -> pb.ControlMessage | None:
        """Warn at two ordinary output intervals; never wait for funding here."""
        if run.stop.is_set() or run.rates.output == 0:
            return None
        snapshot = self._gate(session, run.context)
        deposit = snapshot.sessions[session.sid].deposit
        available = max(
            0,
            deposit * RATE_SCALE
            - session.usage_numerator
            - sum(item.accounting.remaining_numerator for item in self._active(session)),
        )
        if available >= 2000 * run.rates.output:
            run.low_credit_marker = None
            return None
        marker = (deposit, available)
        if run.low_credit_marker == marker:
            return None
        result = pb.ControlMessage(
            low_credit=pb.LowCredit(
                session_id=session.sid,
                model=run.context.model,
                rate_card_hash=run.context.rate_card_hash,
                remaining_tokens=min(MAX_UINT64, available // run.rates.greatest),
                suggest_top_up_tokens=self.config.payment.suggest_session_tokens,
            )
        )
        validate_message(result)
        run.low_credit_marker = marker
        return result

    async def _admit(self, session: _Session, run: _Run, bound: TokenCounts) -> None:
        expires: float | None = None
        async with self._lock:
            while True:
                snapshot = self._gate(session, run.context)
                self._prices(session, run.context, snapshot)
                self._history_fits(session)
                rows, amount = self._required_rows(session, run, bound)
                # Validate full required-token representation even on a covered
                # fast path: later peers must be able to reconcile this history.
                guidance = self._guidance(session, run, rows, snapshot)
                if amount > snapshot.sessions[session.sid].deposit:
                    assert session.owner is not None
                    self._send(session.owner, guidance)
                    raise _payment(
                        PaymentFailure(
                            PaymentCode.INSUFFICIENT_DEPOSIT,
                            session_id=session.sid,
                            run_id=run.context.run_id,
                        )
                    )
                now = asyncio.get_running_loop().time()
                if expires is not None and now >= expires:
                    raise _not_ready("AUTHORIZATION_REQUIRED", session.sid, run.context.run_id)
                voucher = session.voucher.cumulative if session.voucher is not None else None
                covered = voucher is not None and voucher >= amount
                # Admit runs in the order they first waited, but only for a turn. A
                # later run admitted on headroom an earlier run is waiting for raises
                # that run's target, so under steady load the earlier run would chase
                # it until its wait ended. A turn ends after _TURN, so a run the agent
                # never covers cannot hold back covered work for longer than that.
                turn = min(_TURN, self.config.payment.authorization_wait_ms / 2000)
                position = run.queued.position if run.queued is not None else None
                earlier = [
                    other.queued.since
                    for other in session.runs.values()
                    if other is not run
                    and other.queued is not None
                    and not (other.admitted or other.not_admitted or other.stop.is_set())
                    and (position is None or other.queued.position < position)
                ]
                if covered and not any(now < since + turn for since in earlier):
                    if len(self._active(session)) >= 1024:
                        raise _RpcFailure(
                            grpc.StatusCode.RESOURCE_EXHAUSTED, "active run recovery limit"
                        )
                    run.covered_counts = bound
                    run.accounting = RunAccounting(_ZERO, priced_numerator(bound, run.rates))
                    run.admitted = True
                    self._wake(session)  # The next waiting run may now be covered.
                    return
                if run.queued is None:
                    run.queued = _Queued(session.waited, now)
                    session.waited += 1
                # A covered run waiting its turn needs no voucher, so no guidance,
                # and its turn comes within _TURN, before any authorization expiry.
                if not covered:
                    if expires is None:
                        expires = now + self.config.payment.authorization_wait_ms / 1000
                    # Send guidance when it changes. The head also repeats it when a
                    # new voucher still leaves it short, and once a turn: the agent may
                    # have declined it while its own view of the deposit was behind.
                    encoded = guidance.SerializeToString()
                    sent = run.queued.sent
                    if (
                        sent is None
                        or sent[0] != encoded
                        or (not earlier and (sent[2] != voucher or now >= sent[1] + turn))
                    ):
                        run.queued.sent = (encoded, now, voucher)
                        assert session.owner is not None
                        self._send(session.owner, guidance)
                # Its session's changes, shutdown and a change in chain readiness
                # wake it. The re-check covers what no event marks, such as a
                # turn ending or a new block.
                try:
                    timeout = 0.05 if expires is None else min(expires - now, 0.05)
                    await asyncio.wait_for(session.wake.wait(), timeout)
                except TimeoutError:
                    pass

    @staticmethod
    def _upstream(error: UpstreamFailure) -> pb.ChatChunk:
        message = pb.UpstreamError(body=error.body)
        if error.http_status is not None:
            message.http_status = error.http_status
        return pb.ChatChunk(upstream_error=message)

    async def _execute(self, run: _Run) -> None:
        assert run.output is not None
        count = 0
        error: Exception | None = None
        try:
            # Dispatch is a separate scheduling boundary after admission. A
            # detached Control, cancellation or changed chain readiness may
            # have invalidated the grant before this task first runs.
            async with self._lock:
                session, _ = self._run(run.context)
                self._gate(session, run.context)
                if run.stop.is_set():
                    raise _not_ready("CONTROL_NOT_READY", session.sid, run.context.run_id)
            async for payload in self.execution(run.request, run.context, self, run.stop):
                validate_json(payload)
                count += 1
                if not run.request.stream and count != 1:
                    raise ValidationError(
                        ValidationCode.INVALID_VALUE,
                        "nonstreaming execution returned multiple payloads",
                    )
                await run.output.put(pb.ChatChunk(payload=payload).SerializeToString())
            if (
                not run.request.stream
                and count != 1
                and run.accounting.final_status is RunStatus.COMPLETED
            ):
                raise ValidationError(
                    ValidationCode.INVALID_VALUE, "nonstreaming completion requires one response"
                )
        except UpstreamFailure as upstream:
            self._observers.emit("on_error", HookError("complete", "FAILED_PRECONDITION"))
            if (
                run.accounting.final_status is not None
                and run.accounting.final_status is not RunStatus.FAILED
            ):
                error = _not_ready("INCONSISTENT_USAGE", run.context.session_id, run.context.run_id)
            try:
                await run.output.put(self._upstream(upstream).SerializeToString())
            except Exception:
                run.stop.set()
                error = _RpcFailure(grpc.StatusCode.RESOURCE_EXHAUSTED, "Complete queue limit")
        except _RpcFailure as failure:
            run.stop.set()
            error = failure
        except asyncio.CancelledError:
            run.stop.set()
            error = _not_ready("STATE_UNAVAILABLE", run.context.session_id, run.context.run_id)
        except Exception:
            run.stop.set()
            # A host error after this runtime refused the run's coverage reaches
            # the agent as that refusal; arbitrary host errors stay sanitized.
            error = (
                _payment(run.refusal)
                if run.refusal is not None
                else _not_ready("STATE_UNAVAILABLE", run.context.session_id, run.context.run_id)
            )
        finally:
            if run.terminal is not None and error is None:
                try:
                    await run.output.put(pb.ChatChunk(terminal=run.terminal).SerializeToString())
                except Exception:
                    error = _RpcFailure(grpc.StatusCode.RESOURCE_EXHAUSTED, "Complete queue limit")
            elif error is None:
                run.stop.set()
                error = _not_ready("STATE_UNAVAILABLE", run.context.session_id, run.context.run_id)
            async with self._lock:
                if run.accounting.final_status is None:
                    run.unresolved = True
                    run.stop.set()
                run.request = PreparedRequest(b"", run.request.stream)
                self._wake(self._sessions.get(run.context.session_id))
            run.output.close(error)

    def _run(self, context: RunContext) -> tuple[_Session, _Run]:
        session = self._sessions.get(context.session_id)
        run = session.runs.get(context.run_id) if session is not None else None
        if session is None or run is None or not run.admitted or run.context != context:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "report has no matching admitted run"
            )
        return session, run

    async def require_coverage(
        self, context: RunContext, remaining_tokens: TokenCounts
    ) -> CoverageResult:
        """Request a full remaining bound; existing reserved cost cannot shrink."""
        async with self._lock:
            session, run = self._run(context)
            try:
                snapshot = self._gate(session, context)
                if run.stop.is_set() or run.accounting.final_status is not None:
                    raise _not_ready("CONTROL_NOT_READY", session.sid, context.run_id)
                remaining = priced_numerator(remaining_tokens, run.rates)
                if remaining <= run.accounting.remaining_numerator:
                    return CoverageResult(
                        True,
                        required_coverage(
                            (
                                session.usage_numerator,
                                *(
                                    item.accounting.remaining_numerator
                                    for item in self._active(session)
                                ),
                            )
                        ),
                        None,
                    )
                bound = _sum_counts(run.accounting.counts, remaining_tokens)
                rows, required = self._required_rows(session, run, bound)
                # Validate the complete representation; active runs renew via
                # early Meters, never a new request-start authorization wait.
                self._guidance(session, run, rows, snapshot)
                if required > snapshot.sessions[session.sid].deposit:
                    raise _payment(
                        PaymentFailure(
                            PaymentCode.INSUFFICIENT_DEPOSIT,
                            session_id=session.sid,
                            run_id=context.run_id,
                        )
                    )
                if session.voucher is None or required > session.voucher.cumulative:
                    raise _not_ready("AUTHORIZATION_REQUIRED", session.sid, context.run_id)
                run.covered_counts = bound
                run.accounting = replace(run.accounting, remaining_numerator=remaining)
                return CoverageResult(True, required, None)
            except _RpcFailure as error:
                run.stop.set()
                reason = (
                    pb.DRAIN_REASON_DEPOSIT_EXHAUSTED
                    if error.payment and error.payment.code is PaymentCode.INSUFFICIENT_DEPOSIT
                    else pb.DRAIN_REASON_AUTHORIZATION_EXHAUSTED
                    if error.payment and error.payment.reason == "AUTHORIZATION_REQUIRED"
                    else pb.DRAIN_REASON_PROVIDER_INITIATED
                )
                if session.owner is not None and session.owner.live:
                    self._send(
                        session.owner,
                        pb.ControlMessage(
                            drain=pb.Drain(
                                session_id=session.sid, run_id=context.run_id, reason=reason
                            )
                        ),
                    )
                failure = error.payment or PaymentFailure(
                    PaymentCode.SESSION_NOT_READY, "CONTROL_NOT_READY", session.sid, context.run_id
                )
                if run.refusal is None:
                    run.refusal = failure
                return CoverageResult(
                    False,
                    required_coverage(
                        (
                            session.usage_numerator,
                            *(
                                item.accounting.remaining_numerator
                                for item in self._active(session)
                            ),
                        )
                    ),
                    failure,
                )

    async def report_usage(self, context: RunContext, counts: TokenCounts) -> AccountingResult:
        async with self._lock:
            result, _ = self._report(context, counts, None)
            return result

    async def report_terminal(
        self, context: RunContext, counts: TokenCounts, status: RunStatus
    ) -> FinalAccountingResult:
        async with self._lock:
            result, _ = self._report(context, counts, status)
            return FinalAccountingResult(result, status)

    def _report(
        self, context: RunContext, counts: TokenCounts, status: RunStatus | None
    ) -> tuple[AccountingResult, _Run]:
        session, run = self._run(context)
        try:
            transition = report_transition(run.accounting, counts, run.rates, final_status=status)
            usage = cumulative_usage(
                (
                    session.usage_numerator
                    - priced_numerator(run.accounting.counts, run.rates)
                    + priced_numerator(counts, run.rates),
                )
            )
        except ValidationError:
            session.inconsistent = True
            self._stop_session(session)
            raise
        if transition.duplicate:
            seq = run.meter_seq
            # An initial zero report still needs a positive cursor to represent
            # an AccountingResult; publish its first meter once.
            if seq > 0:
                return AccountingResult(context, counts, seq, usage, True), run
        sequence = uint64(session.meter_seq + 1, field="meter sequence", positive=True)
        run.accounting = transition.state
        run.meter_seq = sequence
        session.meter_seq = sequence
        meter = pb.Meter(
            session_id=session.sid,
            run_id=context.run_id,
            seq=sequence,
            input_tokens=counts.input_tokens,
            cache_tokens=counts.cache_tokens,
            output_tokens=counts.output_tokens,
        )
        self._observers.emit("on_meter", meter)
        receipt: pb.Receipt | None = None
        if status is not None:
            run.unresolved = False
            run.terminal = pb.Terminal(
                input_tokens=counts.input_tokens,
                cache_tokens=counts.cache_tokens,
                output_tokens=counts.output_tokens,
                amount=priced_numerator(counts, run.rates) // RATE_SCALE,
                session_id=session.sid,
                run_id=context.run_id,
                model=context.model,
                rate_card_hash=context.rate_card_hash,
                status=_RUN_STATUS[status],
                final_meter_seq=sequence,
            )
            current = self.chain.current
            if current is not None and session.sid in current.sessions:
                receipt = pb.Receipt(
                    session_id=session.sid,
                    run_id=context.run_id,
                    model=context.model,
                    rate_card_hash=context.rate_card_hash,
                    input_tokens=counts.input_tokens,
                    cache_tokens=counts.cache_tokens,
                    output_tokens=counts.output_tokens,
                    amount=run.terminal.amount,
                    status=_RUN_STATUS[status],
                    final_meter_seq=sequence,
                    settled_cumulative=current.sessions[session.sid].settled_cumulative,
                    chain_observation=observation_to_wire(current.block.observation),
                )
        if session.owner is not None and session.owner.live:
            try:
                self._send(session.owner, pb.ControlMessage(meter=meter))
                if receipt is not None:
                    self._send(session.owner, pb.ControlMessage(receipt=receipt))
                if status is None:
                    warning = self._ordinary_low_credit(session, run)
                    if warning is not None:
                        self._send(session.owner, warning)
            except _RpcFailure:
                # Accounting is retained even if final delivery becomes lost.
                pass
        self._wake(session)
        return AccountingResult(context, counts, sequence, usage, False), run

    async def _maintain(self) -> None:
        while True:
            await asyncio.sleep(
                min(
                    self.config.settlement.interval_ms / 1000,
                    self.config.chain.max_age_ms / 2000,
                    0.05,
                )
            )
            if not self._started:
                try:
                    self._initialize()
                except (ChainEvidenceError, ValidationError):
                    continue
            async with self._lock:
                for session in self._sessions.values():
                    try:
                        self._gate(session)
                    except _RpcFailure:
                        if any(not run.stop.is_set() for run in self._active(session)):
                            self._stop_session(session)
                            self._wake(session)
                self._retire()
                if (
                    self._retirable
                    and (self._retiring is None or self._retiring.done())
                    and (self._retire_after is None or time.monotonic() >= self._retire_after)
                ):
                    unconfirmed = {
                        reference: self._unconfirmed.pop(reference)
                        for reference, _ in self._retirable
                        if reference in self._unconfirmed
                    }
                    self._retiring = self._spawn(self._forget(self._retirable, unconfirmed))
                    self._retirable = []
                near_capacity = getattr(self.wallet, "near_capacity", False) is True
                if near_capacity and not self._near_capacity:
                    self._observers.emit(
                        "on_error",
                        HookError(
                            "wallet",
                            "RESOURCE_EXHAUSTED",
                            detail="wallet: operation records near capacity",
                        ),
                    )
                self._near_capacity = near_capacity
                # Every session's admission reads chain readiness: a change can
                # end every wait at once.
                readiness = self.chain.readiness()
                if readiness != self._readiness:
                    self._readiness = readiness
                    self._wake_all()
            due_sessions: list[bytes] = []
            for session in tuple(self._sessions.values()):
                if self._final(session):
                    continue
                if session.close_pending:
                    # A close waits for its runs to end; one the wallet refused
                    # is prepared again at most once per settlement interval.
                    # Otherwise it continues on every pass, so a submitted
                    # close is reconciled promptly.
                    operation = session.operation
                    if self._active(session) or (
                        operation is not None
                        and operation.prepared.kind is OperationKind.CLOSE
                        and operation.result.state is OperationState.FAILED
                        and time.monotonic() - session.last_collection
                        < self.config.settlement.interval_ms / 1000
                    ):
                        continue
                    self._schedule_collection(session.sid, close=True)
                    continue
                try:
                    snapshot = self.chain.snapshot()
                    state = snapshot.sessions[session.sid]
                    amount = max(0, session.usage - state.settled_cumulative)
                    due = (
                        time.monotonic() - session.last_collection
                        >= self.config.settlement.interval_ms / 1000
                    )
                    # A capture whose outcome is unknown is reconciled whatever
                    # the session's state: an exit may have closed it since.
                    reconcile = due and _unresolved(session.operation)
                    due |= (
                        self.config.settlement.amount is not None
                        and amount >= self.config.settlement.amount
                    )
                    due |= (
                        self.config.settlement.units is not None
                        and session.units - session.confirmed_units >= self.config.settlement.units
                    )
                    deadline = (
                        state.last_activity + 86400
                        if state.status is SessionStatus.OPEN
                        else state.close_requested_at + 3600
                    )
                    due |= (
                        deadline - self.chain.estimated_timestamp()
                        <= 2 * self.config.payment.collection_margin_ms / 1000
                    )
                    if reconcile or (amount and due and state.status is SessionStatus.OPEN):
                        if session.sid not in self._collecting:
                            due_sessions.append(session.sid)
                except (ChainEvidenceError, _RpcFailure, ValidationError):
                    pass
            if due_sessions:
                self._schedule_batch_collection(tuple(due_sessions))

    def _schedule_batch_collection(self, ids: tuple[bytes, ...]) -> None:
        self._collecting.update(ids)

        async def group(values: tuple[bytes, ...]) -> None:
            try:
                if len(values) == 1:
                    await self.sessions.settle(values[0])
                else:
                    await self.sessions.settleBatch(values)
            except (ChainEvidenceError, _RpcFailure, ValidationError):
                # A rejected preparation/simulation has not submitted a
                # transaction. Smaller groups retain useful eligible work and
                # obtain their own exact simulations, without a gas stipend.
                if len(values) > 1:
                    middle = len(values) // 2
                    await group(values[:middle])
                    await group(values[middle:])

        async def collect() -> None:
            try:
                # Each batch's calldata fits the wallet's call bound; the rest of
                # the due sessions follow in further batches of this pass.
                size = _batch_sessions(self._max_call_bytes)
                for start in range(0, len(ids), size):
                    await group(ids[start : start + size])
            finally:
                self._collecting.difference_update(ids)

        self._spawn(collect())

    def _schedule_collection(self, sid: bytes, *, close: bool) -> None:
        if sid in self._collecting:
            return
        self._collecting.add(sid)

        async def collect() -> None:
            try:
                if close:
                    await self.sessions.close(sid)
                else:
                    await self.sessions.settle(sid)
            except (ChainEvidenceError, _RpcFailure, ValidationError):
                pass
            finally:
                self._collecting.discard(sid)

        self._spawn(collect())

    async def _abort(self, rpc: ServicerContext[Any, Any], error: _RpcFailure) -> None:
        self._observers.emit("on_error", HookError("rpc", error.code.name))
        if error.payment is not None:
            code, details, metadata = payment_status(error.payment)
            await rpc.abort(code, details, trailing_metadata=metadata)
        await rpc.abort(error.code, error.reason)


class _Sessions:
    def __init__(self, provider: Mppi) -> None:
        self._provider = provider
        self._operations = asyncio.Lock()
        self._empty_batch: _PendingOperation | None = None
        # Sessions whose chain watch an opening or a recovery holds, counted:
        # each holds it until it installs or is refused, and they run
        # concurrently (_watched).
        self._watching: dict[bytes, int] = {}

    async def get(self, sid: bytes) -> SessionView:
        fixed_bytes(sid, 32, field="session_id")
        provider = self._provider
        async with provider._lock:
            session = provider._sessions.get(sid)
            if session is None:
                raise _not_ready("STATE_UNAVAILABLE", sid)
            snapshot = provider.chain.current
            if snapshot is None or sid not in snapshot.sessions:
                raise _not_ready("CHAIN_STATE_STALE", sid)
            state = snapshot.sessions[sid]
            try:
                provider._gate(session)
                readiness = Readiness(True)
            except _RpcFailure as error:
                readiness = Readiness(
                    False,
                    (
                        error.payment
                        or PaymentFailure(PaymentCode.SESSION_NOT_READY, "STATE_UNAVAILABLE", sid),
                    ),
                )
            return SessionView(
                sid,
                session.opening.descriptor,
                state.deposit,
                state.settled_cumulative,
                session.voucher.cumulative if session.voucher is not None else None,
                session.usage if session.history_available else None,
                state.status,
                snapshot.block.observation,
                provider.chain.readiness().ready,
                readiness,
            )

    async def list(self, *, cursor: str | None = None, limit: int = 100) -> SessionPage:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "session page limit must be 1..1000"
            )
        keys = sorted(self._provider._sessions)
        if cursor is not None:
            if type(cursor) is not str or len(cursor) != 64:
                raise ValidationError(ValidationCode.INVALID_VALUE, "invalid session cursor")
            try:
                after = bytes.fromhex(cursor)
            except ValueError:
                raise ValidationError(
                    ValidationCode.INVALID_VALUE, "invalid session cursor"
                ) from None
            keys = [key for key in keys if key > after]
        selected = keys[:limit]
        return SessionPage(
            tuple([await self.get(sid) for sid in selected]),
            selected[-1].hex() if len(keys) > limit else None,
        )

    @staticmethod
    def _operation_evidence(value: _PendingOperation | None) -> RecoveryOperation | None:
        if value is None:
            return None
        return RecoveryOperation(value.prepared, value.result, tuple(value.units.items()))

    async def checkpoint(self, sid: bytes) -> RecoveryEvidence:
        """Copy live protocol facts; callers retain them using their own infrastructure."""
        fixed_bytes(sid, 32, field="session_id")
        provider = self._provider
        async with self._operations, provider._lock:
            session = provider._sessions.get(sid)
            if session is None:
                raise _missing(sid, "the session is not held")
            if not session.history_available:
                raise _missing(sid, "the session's history is unavailable")
            if any(not run.admitted and not run.not_admitted for run in session.runs.values()):
                raise _missing(sid, "a run is still preparing")
            snapshot = provider._session_state(
                session, pb.ReconnectRequest(nonce=bytes(32)), private=True
            )
            publications = dict(session.publications)
            current = provider.chain.snapshot()
            identity = current.identities.get(provider.config.identity.provider_id)
            if identity is not None and identity.catalog is not None:
                publications[identity.catalog.rate_card_hash] = identity.observation
            return RecoveryEvidence(
                snapshot,
                tuple((run.context, run.accounting) for run in provider._active(session)),
                current.block.observation,
                session.opening.tx_hash,
                tuple(
                    RecoveryRun(
                        run.context,
                        run.signature,
                        run.accounting,
                        run.covered_counts,
                        run.meter_seq,
                        run.admitted,
                        run.not_admitted,
                    )
                    for run in session.runs.values()
                ),
                tuple(publications.items()),
                self._operation_evidence(session.operation),
                self._operation_evidence(session.baseline),
                session.confirmed_units,
                session.close_reference,
                session.inconsistent,
            )

    def _collects(self, prepared: PreparedOperation, sid: bytes) -> bool:
        """A collection of this deployment's contract that covers ``sid``, the
        first check of any supplied operation."""
        domain = self._provider.contract.domain
        return (
            prepared.kind in (OperationKind.SETTLE, OperationKind.SETTLE_BATCH, OperationKind.CLOSE)
            and prepared.call.chain_id == domain.chain_id
            and prepared.call.to == domain.verifying_contract
            and sid in prepared.session_ids
        )

    def _restore_operation(
        self, evidence: RecoveryOperation | None, sid: bytes
    ) -> _PendingOperation | None:
        if evidence is None:
            return None
        provider = self._provider
        prepared, result = evidence.prepared, evidence.result
        if (
            not self._collects(prepared, sid)
            or len(dict(evidence.units)) != len(evidence.units)
            or set(dict(evidence.units)) != set(prepared.session_ids)
            or any(type(value) is not int or value < 0 for _, value in evidence.units)
        ):
            raise _inconsistent(
                sid,
                "a saved operation does not collect this session or misstates its units",
            )
        # Reconstructing rechecks exact ABI/descriptor/target consistency.
        prepared = PreparedOperation(
            prepared.kind,
            prepared.call,
            prepared.descriptors,
            prepared.amounts,
            prepared.expected_rate_card_hash,
        )
        verified = OperationResult(
            result.operation_ref, OperationState.UNKNOWN, result.wallet_operation_id, result.tx_hash
        )
        if (
            result.state is OperationState.FAILED
            and result.tx_hash is None
            and result.wallet_operation_id is None
            and result.observation is None
            and not result.sessions
        ):
            # A FAILED result with no handle at all is the wallet's attestation
            # that submission never began (NotSubmittedError): no call is bound
            # to its reference and there is nothing to read. It stays FAILED, so
            # the live rules apply again: a settlement is replaced by a new
            # reference, and a close continues under its own. Neither can pay
            # twice: captures are cumulative on chain, and the wallet checks a
            # reused close reference against the call bound to it.
            verified = OperationResult(
                result.operation_ref, OperationState.FAILED, cause=result.cause
            )
        if result.tx_hash is not None:
            # A saved hash is caller-supplied history. Bind it to the original
            # wallet intent before canonical receipts can establish failure
            # (including when a currently pending receipt later reverts).
            try:
                bound = provider.wallet.reconcile(operation_ref=result.operation_ref)
            except Exception as error:
                raise _missing(
                    sid,
                    f"the wallet could not look up a saved transaction ({type(error).__name__})",
                ) from None
            if not _agrees(result, bound):
                raise _inconsistent(
                    sid, "the wallet's record of a saved transaction differs from the evidence"
                )
            verified = provider.chain.reader.reconcile(
                prepared,
                operation_ref=result.operation_ref,
                tx_hash=result.tx_hash,
                wallet_operation_id=bound.wallet_operation_id,
            )
            if (
                verified.operation_ref != result.operation_ref
                or verified.tx_hash != result.tx_hash
                or verified.wallet_operation_id != bound.wallet_operation_id
            ):
                raise _inconsistent(
                    sid, "the chain's record of a saved transaction differs from the evidence"
                )
        return _PendingOperation(prepared, result.operation_ref, verified, dict(evidence.units))

    def _restore(self, evidence: RecoveryEvidence) -> _Session:
        """Validate an entire detached candidate before touching authoritative state."""
        provider = self._provider
        record, reader = evidence.snapshot, provider.chain.reader
        validate_recovery_snapshot(record)
        sid = record.session_id
        if record.control_generation == 0 and (
            evidence.runs or evidence.reservations or evidence.confirmed_units != 0
        ):
            raise _inconsistent(sid, "an unattached checkpoint has private accounting history")
        fixed_bytes(evidence.opening_tx, 32, field="opening_tx")
        descriptor = descriptor_from_wire(record.descriptor)
        if (
            descriptor.provider_id != provider.config.identity.provider_id
            or descriptor.operator != provider.wallet.address
            or type(evidence.inconsistent) is not bool
            or (record.close_pending != (evidence.close_reference is not None))
            or (evidence.close_reference is not None and not evidence.close_reference)
        ):
            raise _inconsistent(
                sid, "the descriptor is not this provider's, or the close state is malformed"
            )
        catalogs = {item.rate_card_hash: parse_catalog(item.catalog) for item in record.catalogs}
        if any(key != value.rate_card_hash for key, value in catalogs.items()):
            raise _inconsistent(sid, "a catalog does not match its hash or repeats")
        if record.initial_rate_card_hash not in catalogs:
            raise _missing(sid, "the evidence lacks the initial catalog")
        if (
            record.HasField("current_rate_card_hash")
            and record.current_rate_card_hash not in catalogs
        ):
            raise _missing(sid, "the evidence lacks the current catalog")
        opening = reader.opening(
            evidence.opening_tx, sid, accepted_catalog=catalogs[record.initial_rate_card_hash]
        )
        if opening.descriptor != descriptor:
            raise _inconsistent(sid, "the opening transaction shows another descriptor")
        block = reader.block(evidence.observation.block_number)
        if (
            block.observation != evidence.observation
            or record.chain_block_number != block.observation.block_number
            or record.chain_block_hash != block.observation.block_hash
            or block.observation.block_number < opening.observation.block_number
        ):
            raise _inconsistent(sid, "the checkpoint block contradicts the chain or the opening")
        chain_state = reader.session(sid, block)
        if (
            not chain_state.exists
            or chain_state.deposit != record.deposit
            or chain_state.settled_cumulative != record.settled_cumulative
            or _SESSION_STATUS[chain_state.status] != record.status
            or chain_state.initial_rate_card_hash != record.initial_rate_card_hash
        ):
            raise _inconsistent(
                sid, "the session's chain state at the checkpoint block differs from the evidence"
            )
        current_identity = reader.identity(
            descriptor.provider_id, block, allow_unavailable_catalog=True
        )
        current_catalog = current_identity.catalog
        if (
            record.HasField("current_rate_card_hash") != (current_catalog is not None)
            or current_catalog is not None
            and record.current_rate_card_hash != current_catalog.rate_card_hash
        ):
            raise _inconsistent(
                sid, "the current catalog differs from the provider's at the checkpoint block"
            )
        publications = dict(evidence.publications)
        if len(publications) != len(evidence.publications) or not set(publications) <= set(
            catalogs
        ):
            raise _inconsistent(sid, "a publication names no catalog of the evidence or repeats")
        for key, catalog in catalogs.items():
            if key == record.initial_rate_card_hash:
                continue
            observation = publications.get(key)
            if observation is None:
                raise _missing(sid, "a catalog has no publication evidence")
            if observation.block_number > block.observation.block_number:
                raise _missing(sid, "a catalog was published after the checkpoint block")
            published_block = reader.block(observation.block_number)
            if published_block.observation != observation:
                raise _inconsistent(sid, "a catalog's publication differs from the chain")
            identity = reader.identity(descriptor.provider_id, published_block)
            if identity.catalog != catalog:
                raise _inconsistent(sid, "a catalog's publication differs from the chain")
        session = _Session(
            sid,
            opening,
            catalogs,
            publications=publications,
            generation=record.control_generation,
            meter_seq=record.meter_seq,
            close_pending=record.close_pending,
            close_reference=evidence.close_reference,
            inconsistent=evidence.inconsistent,
            wake=asyncio.Condition(provider._lock),
        )
        totals: dict[tuple[str, bytes], TokenCounts] = {}
        cursors: set[int] = set()
        for item in evidence.runs:
            context = item.context
            if (
                context.session_id != sid
                or context.run_id in session.runs
                or context.control_generation > record.control_generation
                or context.rate_card_hash not in catalogs
                or type(item.admitted) is not bool
                or type(item.not_admitted) is not bool
                or item.admitted == item.not_admitted
            ):
                raise _inconsistent(
                    sid, "a run's context, admission or meter sequence contradicts the evidence"
                )
            try:
                verify_run_auth(provider.contract.domain, descriptor, context, item.signature)
            except ValidationError as error:
                raise _invalid(error, "a run's acceptance signature is invalid") from None
            try:
                rates = catalogs[context.rate_card_hash].rates_for(context.model)
            except KeyError:
                raise _missing(sid, "a run's model has no rates in its catalog") from None
            uint64(item.meter_seq, field="meter sequence")
            if item.meter_seq > record.meter_seq or (item.meter_seq and item.meter_seq in cursors):
                raise _inconsistent(
                    sid, "a run's context, admission or meter sequence contradicts the evidence"
                )
            if item.meter_seq:
                cursors.add(item.meter_seq)
            accounting = item.accounting
            used = priced_numerator(accounting.counts, rates)
            bound = priced_numerator(item.covered_counts, rates)
            if item.admitted:
                if (
                    used + accounting.remaining_numerator > bound
                    or (
                        accounting.final_status is None
                        and used + accounting.remaining_numerator != bound
                    )
                    or (accounting.counts != _ZERO and item.meter_seq == 0)
                    or (accounting.final_status is not None and item.meter_seq == 0)
                ):
                    raise _inconsistent(
                        sid,
                        "an admitted run's accounting contradicts its coverage or meter sequence",
                    )
                key = (context.model, context.rate_card_hash)
                totals[key] = _sum_counts(totals.get(key, _ZERO), accounting.counts)
            elif (
                accounting != RunAccounting(_ZERO, 0)
                or item.covered_counts != _ZERO
                or item.meter_seq
            ):
                raise _inconsistent(sid, "a run that was not admitted has accounting")
            run = _Run(
                context,
                item.signature,
                PreparedRequest(b"", True),
                rates,
                accounting=accounting,
                covered_counts=item.covered_counts,
                admitted=item.admitted,
                not_admitted=item.not_admitted,
                meter_seq=item.meter_seq,
                unresolved=item.admitted and accounting.final_status is None,
            )
            run.stop.set()
            session.runs[context.run_id] = run
        if record.meter_seq != max(cursors, default=0):
            raise _inconsistent(sid, "the session's meter sequence is not its runs' last")
        actual_reservations = tuple(
            (run.context, run.accounting) for run in provider._active(session)
        )
        if dict(evidence.reservations) != dict(actual_reservations) or len(
            evidence.reservations
        ) != len(actual_reservations):
            raise _inconsistent(sid, "the reservations differ from the active runs")
        stated_totals: dict[tuple[str, bytes], TokenCounts] = {}
        for row in record.usage_by_price:
            key = (row.model, row.rate_card_hash)
            representative = session.runs.get(row.acceptance_run_id)
            if (
                representative is None
                or not representative.admitted
                or representative.context
                != RunContext(
                    sid,
                    row.acceptance_run_id,
                    row.acceptance_control_generation,
                    row.model,
                    row.rate_card_hash,
                )
                or representative.signature != row.acceptance_signature
            ):
                raise _inconsistent(sid, "a priced usage row differs from its run")
            stated_totals[key] = TokenCounts(row.input_tokens, row.cache_tokens, row.output_tokens)
        if totals != stated_totals:
            raise _inconsistent(sid, "the priced usage differs from the runs' totals")
        finals = {row.run_id: row for row in record.run_states}
        for run in session.runs.values():
            final = finals.pop(run.context.run_id, None)
            if run.accounting.final_status is None and run.admitted:
                if final is not None:
                    raise _inconsistent(sid, "an unfinished run has a final state")
                continue
            if final is None:
                raise _missing(sid, "a finished run has no final state")
            if run.not_admitted:
                if final.status != pb.RUN_STATUS_NOT_ADMITTED:
                    raise _inconsistent(sid, "a run that was not admitted has another final status")
            elif (
                final.status
                != _RUN_STATUS.get(run.accounting.final_status or RunStatus.NOT_ADMITTED)
                or final.model != run.context.model
                or final.rate_card_hash != run.context.rate_card_hash
                or final.control_generation != run.context.control_generation
                or final.auth_signature != run.signature
                or TokenCounts(final.input_tokens, final.cache_tokens, final.output_tokens)
                != run.accounting.counts
            ):
                raise _inconsistent(sid, "a final state differs from its run")
        if finals:
            raise _inconsistent(sid, "a final state names no run of the evidence")
        required = required_coverage(
            (
                session.usage_numerator,
                *(run.accounting.remaining_numerator for run in provider._active(session)),
            )
        )
        if record.HasField("accepted_voucher"):
            voucher = record.accepted_voucher
            try:
                verify_voucher(
                    provider.contract.domain, descriptor, voucher.cumulative, voucher.signature
                )
            except ValidationError as error:
                raise _invalid(error, "the accepted voucher's signature is invalid") from None
            if voucher.session_id != sid or not required <= voucher.cumulative <= record.deposit:
                raise _inconsistent(
                    sid, "the accepted voucher does not cover the usage or exceeds the deposit"
                )
            session.voucher = pb.Voucher.FromString(voucher.SerializeToString())
        elif any(run.admitted for run in session.runs.values()) or record.settled_cumulative:
            raise _missing(sid, "admitted usage has no accepted voucher")
        if session.usage < record.settled_cumulative:
            raise _inconsistent(sid, "the usage is below the settled amount")
        session.operation = self._restore_operation(evidence.operation, sid)
        session.baseline = self._restore_operation(evidence.baseline, sid)
        for operation in (session.operation, session.baseline):
            if operation is None:
                continue
            if operation.units[sid] > session.units or any(
                owner != descriptor or target > session.usage
                for owner, target, operation_sid in zip(
                    operation.prepared.descriptors,
                    operation.prepared.amounts,
                    operation.prepared.session_ids,
                    strict=True,
                )
                if operation_sid == sid
            ):
                raise _inconsistent(
                    sid, "a saved operation exceeds the session's usage or names another descriptor"
                )
            if operation.prepared.kind is OperationKind.CLOSE:
                if not session.close_pending or session.close_reference != operation.reference:
                    raise _inconsistent(
                        sid, "a saved close does not match the session's close intent"
                    )
                if (
                    operation.result.state is OperationState.CONFIRMED
                    and operation.result.tx_hash is not None
                ):
                    closing = reader.closing(operation.result.tx_hash, sid)
                    session.close_result = pb.CloseResult(
                        session_id=sid,
                        tx_hash=closing.tx_hash,
                        capture_amount=closing.capture_amount,
                        refunded=closing.refunded,
                    )
        if (
            type(evidence.confirmed_units) is not int
            or not 0 <= evidence.confirmed_units <= session.units
        ):
            raise _inconsistent(sid, "the confirmed units exceed the session's units")
        if evidence.confirmed_units:
            baseline = session.baseline
            if (
                baseline is None
                or baseline.units.get(sid) != evidence.confirmed_units
                or not any(
                    outcome.session_id == sid and outcome.target_covered is True
                    for outcome in baseline.result.sessions
                )
            ):
                raise _missing(sid, "confirmed units have no covered baseline")
        session.confirmed_units = evidence.confirmed_units
        # Every independently observed historical block remains canonical.
        if reader.block(block.observation.block_number) != block:
            raise _inconsistent(sid, "the checkpoint block is no longer canonical")
        return session

    async def recover(self, evidence: RecoveryEvidence) -> SessionView:
        return await self._recover(evidence, history_current=False)

    async def recover_current(self, evidence: RecoveryEvidence) -> SessionView:
        """As ``recover``, for evidence the host asserts is the session's current
        history: checkpointed after every admission and before dispatch, and
        never older than its newest checkpoint. A pending run absent from that
        history was then never admitted, and a reattachment answers it
        NOT_ADMITTED. After ``recover`` such a run refuses the attachment
        instead, since an older checkpoint may lack a run that was served."""
        return await self._recover(evidence, history_current=True)

    async def _recover(self, evidence: RecoveryEvidence, *, history_current: bool) -> SessionView:
        provider = self._provider
        validate_recovery_snapshot(evidence.snapshot)
        if (
            type(evidence.runs) is not tuple
            or len(evidence.runs) > MAX_MESSAGE_BYTES // 32
            or type(evidence.reservations) is not tuple
            or len(evidence.reservations) > 1024
            or type(evidence.publications) is not tuple
            or len(evidence.publications) > len(evidence.snapshot.catalogs)
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "recovery evidence exceeds resource limits"
            )
        for retained in (evidence.operation, evidence.baseline):
            if retained is not None and (
                len(retained.prepared.call.data) > MAX_MESSAGE_BYTES
                or type(retained.units) is not tuple
                or len(retained.units) > len(retained.prepared.descriptors)
                or len(retained.prepared.descriptors) > MAX_MESSAGE_BYTES // 32
            ):
                raise ValidationError(
                    ValidationCode.INVALID_VALUE, "recovery operation exceeds resource limits"
                )
        # Copy mutable protobuf input before yielding to any external operation.
        evidence = replace(
            evidence, snapshot=pb.SessionState.FromString(evidence.snapshot.SerializeToString())
        )
        sid = evidence.snapshot.session_id
        # Every read comes first, holding only this session's watch, so sessions
        # recover concurrently while collections go on. Every check against held
        # state (the follower's snapshot, a session already held, other sessions'
        # operations) runs below against what was read, under the operation and
        # state locks with no read under them, and the candidate is installed
        # under the same locks.
        with self._watched(sid):
            # Nothing remains to serve or collect for final evidence of a closed
            # session: it is never reinstalled, whether or not it retired here.
            if evidence.is_final():
                state = await provider.chain.watch_session(sid)
                if state.status is SessionStatus.CLOSED:
                    # Hosts pass such records at startup: the session no longer
                    # needs the references it names, from now, as if it retired
                    # here. One only it covers goes at once; a batch waits for
                    # the rest. A reference goes only if the wallet's own record
                    # of it agrees with the record, as _restore_operation
                    # corroborates a saved hash: both name the same transaction,
                    # or neither names one. A mixed-up or corrupt record could
                    # pair this session's call with another session's reference,
                    # and a wallet record with a transaction the record lacks is
                    # traceable, so it stays for the wallet's own reconciliation.
                    # Keeping one is always safe.
                    bound = []
                    for named in (evidence.operation, evidence.baseline):
                        if named is None or not self._collects(named.prepared, sid):
                            continue
                        try:
                            answer = await asyncio.to_thread(
                                provider.wallet.reconcile, operation_ref=named.result.operation_ref
                            )
                        except Exception:
                            continue
                        if _agrees(named.result, answer):
                            bound.append(named)
                    async with provider._lock:
                        if sid not in provider._sessions:
                            now = time.time()
                            for named in bound:
                                covered = provider._references.setdefault(
                                    named.result.operation_ref,
                                    dict.fromkeys(named.prepared.session_ids),
                                )
                                if covered.get(sid) is None:
                                    covered[sid] = now
                            provider._hand_over()
                    raise _not_ready("SESSION_CLOSED", sid)
            candidate = await asyncio.to_thread(self._restore, evidence)
            candidate.admissions_known = history_current
            await provider.chain.watch_session(sid)
            async with self._operations, provider._lock:
                current = provider.chain.snapshot().sessions[sid]
                provider._history_fits(candidate)
                if (
                    candidate.usage < current.settled_cumulative
                    or current.deposit < evidence.snapshot.deposit
                    or current.initial_rate_card_hash
                    != candidate.opening.initial_catalog.rate_card_hash
                    or _SESSION_STATUS[current.status] < evidence.snapshot.status
                ):
                    raise _inconsistent(sid, "the chain follower's view contradicts the evidence")
                previous = provider._sessions.get(sid)
                if previous is not None:
                    if (previous.owner is not None and previous.owner.live) or provider._active(
                        previous
                    ):
                        raise _not_ready("CONTROL_NOT_READY", sid)
                    if (
                        previous.opening != candidate.opening
                        or previous.generation > candidate.generation
                        or previous.meter_seq > candidate.meter_seq
                        or previous.close_pending
                        and not candidate.close_pending
                        or previous.inconsistent
                        and not candidate.inconsistent
                        or previous.confirmed_units > candidate.confirmed_units
                        or previous.close_reference is not None
                        and previous.close_reference != candidate.close_reference
                        or previous.close_result is not None
                        and previous.close_result != candidate.close_result
                        or not set(previous.runs) <= set(candidate.runs)
                        or previous.voucher is not None
                        and (
                            candidate.voucher is None
                            or previous.voucher.cumulative > candidate.voucher.cumulative
                        )
                    ):
                        raise _inconsistent(
                            sid, "the evidence is older than the held session or contradicts it"
                        )
                    if (
                        previous.operation is not None
                        and (
                            previous.operation.result.state
                            in (OperationState.PENDING, OperationState.UNKNOWN)
                            or previous.operation.prepared.kind is OperationKind.CLOSE
                        )
                        and (
                            candidate.operation is None
                            or candidate.operation.reference != previous.operation.reference
                            or candidate.operation.prepared != previous.operation.prepared
                        )
                    ):
                        raise _missing(
                            sid,
                            "the evidence lacks the held session's unresolved or close operation",
                        )
                    for rid, run in previous.runs.items():
                        incoming = candidate.runs[rid]
                        if (
                            run.context != incoming.context
                            or run.signature != incoming.signature
                            or run.admitted != incoming.admitted
                        ):
                            raise _inconsistent(sid, "a run differs from the held session's")
                        report_transition(
                            run.accounting,
                            incoming.accounting.counts,
                            run.rates,
                            final_status=incoming.accounting.final_status,
                        )
                # A recovered batch shares one retained operation object across
                # all members, so overlapping operations cannot resubmit it.
                read = candidate.operation
                if candidate.operation is not None:
                    for other in provider._sessions.values():
                        operation = other.operation
                        if (
                            operation is not None
                            and operation.reference == candidate.operation.reference
                        ):
                            if (
                                operation.prepared != candidate.operation.prepared
                                or operation.units != candidate.operation.units
                            ):
                                raise _inconsistent(
                                    sid,
                                    "another held session holds this reference for a different operation",
                                )
                            candidate.operation = operation
                # A restored reference covers sessions this process may not hold
                # yet; each needs it until it retires here or a later confirmed
                # collection supersedes it. The installed session names it again,
                # so it leaves the retirement queue; a retire call already under
                # way checks again before it is made (_forget). Its wallet check
                # (_unconfirmed) is cleared only when the record restored here
                # was looked up in the wallet, having a transaction hash, and
                # agrees with the record kept, which may be a held one adopted
                # above. A record kept against that lookup is checked at its
                # hand-over, as is one restored without a hash; a held record
                # adopted for one keeps the check it had.
                for restored, looked_up in (
                    (candidate.operation, read),
                    (candidate.baseline, candidate.baseline),
                ):
                    if restored is not None and looked_up is not None:
                        provider._references.setdefault(
                            restored.reference, dict.fromkeys(restored.prepared.session_ids)
                        )[sid] = None
                        provider._retirable = [
                            (queued, at)
                            for queued, at in provider._retirable
                            if queued != restored.reference
                        ]
                        checked = looked_up.result.tx_hash is not None
                        if checked and _agrees(restored.result, looked_up.result):
                            provider._unconfirmed.pop(restored.reference, None)
                        elif checked or restored is looked_up:
                            provider._unconfirmed[restored.reference] = restored.result
                provider._sessions[sid] = candidate
                # A run still waiting on the replaced record now fails at once.
                provider._wake(previous)
        return await self.get(sid)

    @contextmanager
    def _watched(self, sid: bytes) -> Iterator[None]:
        """Hold the chain watch of a session an opening or a recovery reads.

        The last one out releases the watch of a session not held, as Rust's
        PendingWatch does: a refused opening or recovery leaves none behind.
        """
        self._watching[sid] = self._watching.get(sid, 0) + 1
        try:
            yield
        finally:
            self._watching[sid] -= 1
            if not self._watching[sid]:
                del self._watching[sid]
                if sid not in self._provider._sessions:
                    self._provider.chain.unwatch_session(sid)

    async def _measured(
        self, operation: str, work: Coroutine[Any, Any, OperationResult]
    ) -> OperationResult:
        """Run one collection call, then report where its time went (on_timing)."""
        timing = _Timing(operation)
        token = _TIMING.set(timing)
        state: str | None = None
        try:
            result = await work
            state = result.state.value
            return result
        finally:
            _TIMING.reset(token)
            self._provider._observers.emit("on_timing", timing.report(state))

    async def _operate(
        self,
        helper: Callable[..., OperationResult],
        prepared: PreparedOperation,
        reference: str,
        *,
        unsent: Callable[[], None] | None = None,
    ) -> OperationResult:
        """One submit or reconcile helper call: the wallet's own call is wallet
        time, and the receipt proof after it chain time.

        The call runs in a worker thread, and a caller cancelled while it waits
        for one would leave it queued. The worker marks the call begun under a
        lock before anything reaches the wallet; a caller cancelled first marks
        it abandoned, so it never begins, and ``unsent`` then removes the intent
        the wallet never saw, as a failed simulation does.
        """
        provider = self._provider
        wallet = _TimedWallet(provider.wallet)
        handoff = _Handoff()

        def call() -> OperationResult:
            if not handoff.begin():
                raise asyncio.CancelledError
            return helper(wallet, prepared, operation_ref=reference, chain=provider.chain.reader)

        started = time.monotonic()
        try:
            return await asyncio.to_thread(call)
        except asyncio.CancelledError:
            if handoff.abandon() and unsent is not None:
                unsent()
            raise
        finally:
            timing = _TIMING.get()
            if timing is not None:
                now = time.monotonic()
                spent = wallet.spent(now)
                timing.parts["wallet"] += spent
                timing.parts["chain"] += now - started - spent

    async def settle(self, sid: bytes) -> OperationResult:
        return await self._measured("settle", self._collect((sid,), close=False))

    async def settleBatch(self, session_ids: tuple[bytes, ...]) -> OperationResult:
        """One exact ordered batch; repeated ids preserve contract item semantics."""
        return await self._measured("settleBatch", self._settle_batch(session_ids))

    async def _settle_batch(self, session_ids: tuple[bytes, ...]) -> OperationResult:
        if type(session_ids) is not tuple:
            raise ValidationError(ValidationCode.INVALID_VALUE, "batch ids must be a tuple")
        if len(session_ids) > MAX_MESSAGE_BYTES // 512:
            raise ValidationError(ValidationCode.INVALID_VALUE, "batch exceeds resource limit")
        if not session_ids:
            provider = self._provider
            async with self._operations:
                pending = self._empty_batch
                if pending is not None and pending.result.observation is not None:
                    return pending.result
                if pending is None:
                    prepared = provider.contract.settle_batch(())
                    with _timed("chain"):
                        await asyncio.to_thread(
                            provider.chain.reader.simulate, prepared, sender=provider.wallet.address
                        )
                    reference = "settleBatch:" + secrets.token_hex(32)
                    pending = _PendingOperation(
                        prepared, reference, OperationResult(reference, OperationState.UNKNOWN), {}
                    )
                    self._empty_batch = pending
                    helper = submit_operation
                else:
                    helper = reconcile_operation

                def unsent() -> None:
                    if self._empty_batch is pending:
                        self._empty_batch = None

                result = await self._operate(
                    helper,
                    pending.prepared,
                    pending.reference,
                    unsent=unsent if helper is submit_operation else None,
                )
                known = pending.result
                conflict = (
                    known.wallet_operation_id is not None
                    and result.wallet_operation_id is not None
                    and result.wallet_operation_id != known.wallet_operation_id
                ) or (
                    known.tx_hash is not None
                    and result.tx_hash is not None
                    and result.tx_hash != known.tx_hash
                )
                if conflict or (known.tx_hash is not None and result.tx_hash is None):
                    result = OperationResult(
                        pending.reference,
                        OperationState.UNKNOWN,
                        known.wallet_operation_id,
                        known.tx_hash,
                    )
                    try:
                        with _timed("chain"):
                            observed = await asyncio.to_thread(
                                provider.chain.reader.reconcile,
                                pending.prepared,
                                operation_ref=pending.reference,
                                tx_hash=known.tx_hash,
                                wallet_operation_id=known.wallet_operation_id,
                            )
                        if (
                            observed.operation_ref == pending.reference
                            and observed.tx_hash == known.tx_hash
                            and observed.wallet_operation_id == known.wallet_operation_id
                        ):
                            result = observed
                    except Exception:
                        pass
                elif result.wallet_operation_id is None and known.wallet_operation_id is not None:
                    result = replace(result, wallet_operation_id=known.wallet_operation_id)
                pending.result = result
                if result.state is OperationState.CONFIRMED:
                    provider._observers.emit("on_settle", result)
                return pending.result
        return await self._collect(session_ids, close=False, batch=True)

    async def close(self, sid: bytes) -> OperationResult:
        return await self._measured("close", self._collect((sid,), close=True))

    async def _collect(
        self, ids: tuple[bytes, ...], *, close: bool, batch: bool = False
    ) -> OperationResult:
        provider = self._provider
        async with self._operations:
            # The call a reverted close's successor last simulated, below.
            simulated: WalletCall | None = None
            while True:
                next_close: PreparedOperation | None = None
                async with provider._lock:
                    sessions: list[_Session] = []
                    for sid in ids:
                        fixed_bytes(sid, 32, field="session_id")
                        if sid not in provider._sessions:
                            raise _not_ready("STATE_UNAVAILABLE", sid)
                        sessions.append(provider._sessions[sid])
                    if close:
                        completed = sessions[0]
                        if completed.close_result is not None and completed.operation is not None:
                            return completed.operation.result
                        if completed.close_reference is None:
                            completed.close_reference = "close:" + secrets.token_hex(32)
                        if not completed.close_pending:
                            # Close intent is accepted once: its runs stop and its
                            # owner is told once (Drain). A later call, the agent's
                            # or maintenance's, only continues the close.
                            completed.close_pending = True
                            provider._stop_session(completed)
                            provider._wake(completed)
                    unresolved = [
                        session.operation
                        for session in sessions
                        if session.operation is not None
                        and session.operation.result.state
                        in (OperationState.PENDING, OperationState.UNKNOWN)
                        and session.operation.result.observation is None
                    ]
                    # Reconcile one original intersecting intent before any new
                    # submission. Distinct completed captures may be superseded
                    # together; distinct unresolved calls retain their identities.
                    pending = unresolved[0] if unresolved else sessions[0].operation
                    covered = (
                        pending is not None
                        and pending.result.observation is not None
                        and bool(pending.result.sessions)
                        and all(item.target_covered is True for item in pending.result.sessions)
                    )
                    # A close mined and reverted changed no state; it is prepared
                    # again under its successor reference, at most once per
                    # settlement interval, since each attempt is a transaction.
                    retry = (
                        close
                        and pending is not None
                        and pending.prepared.kind is OperationKind.CLOSE
                        and pending.result.state is OperationState.FAILED
                        and pending.result.tx_hash is not None
                    )
                    if (
                        retry
                        and pending is not None
                        and time.monotonic() - sessions[0].last_collection
                        < provider.config.settlement.interval_ms / 1000
                    ):
                        return pending.result
                    if (
                        close
                        and pending is not None
                        and pending.prepared.kind is OperationKind.CLOSE
                        and pending.result.state is OperationState.CONFIRMED
                    ):
                        # Confirmed, but its closing evidence was never read: read it
                        # again, with no wallet call and never a second close.
                        prepared, reference = pending.prepared, pending.reference
                        submit = False
                    elif (
                        pending is not None
                        and pending.result.state in (OperationState.PENDING, OperationState.UNKNOWN)
                        and not covered
                        and pending.result.observation is None
                    ):
                        prepared, reference = pending.prepared, pending.reference
                        submit = False
                    else:
                        snapshot = provider.chain.snapshot()
                        if snapshot.pause_level is not PauseLevel.NONE:
                            raise _payment(
                                PaymentFailure(
                                    PaymentCode.SESSION_NOT_READY,
                                    "CONTRACT_PAUSED",
                                    ids[0],
                                    pause_level=snapshot.pause_level,
                                )
                            )
                        items = []
                        for session in sessions:
                            state = snapshot.sessions[session.sid]
                            if session.inconsistent or not session.history_available:
                                raise _not_ready("INCONSISTENT_STATE", session.sid)
                            if close and provider._active(session):
                                assert session.close_reference is not None
                                return OperationResult(
                                    session.close_reference, OperationState.UNKNOWN
                                )
                            if state.status is SessionStatus.CLOSED:
                                if (
                                    close
                                    and session.close_result is not None
                                    and session.operation is not None
                                ):
                                    return session.operation.result
                                raise _not_ready("SESSION_CLOSED", session.sid)
                            deadline = (
                                state.last_activity + 86400
                                if state.status is SessionStatus.OPEN
                                else state.close_requested_at + 3600
                            )
                            expired = provider.chain.estimated_timestamp() >= deadline
                            if not close and (expired or state.status is not SessionStatus.OPEN):
                                raise _not_ready("CLAIM_DEADLINE", session.sid)
                            capture = (
                                state.settled_cumulative
                                if close and expired
                                else max(session.usage, state.settled_cumulative)
                            )
                            voucher = session.voucher
                            if capture > state.settled_cumulative or not close:
                                if (
                                    voucher is None
                                    or capture > voucher.cumulative
                                    or capture > state.deposit
                                ):
                                    raise _not_ready("AUTHORIZATION_REQUIRED", session.sid)
                                cumulative, signature = voucher.cumulative, voucher.signature
                            else:
                                cumulative, signature = capture, b""
                            items.append(
                                (session.opening.descriptor, cumulative, capture, signature)
                            )
                        if (
                            not close
                            and pending is not None
                            and (pending.result.state is OperationState.CONFIRMED or covered)
                            and pending.prepared.session_ids == ids
                            and pending.prepared.amounts == tuple(item[2] for item in items)
                        ):
                            return pending.result
                        prepared = (
                            provider.contract.close(*items[0])
                            if close
                            else provider.contract.settle_batch(items)
                            if batch
                            else provider.contract.settle(*items[0])
                        )
                        # A call the wallet would refuse records no intent.
                        if len(prepared.call.data) > min(
                            MAX_MESSAGE_BYTES, provider._max_call_bytes
                        ):
                            raise ValidationError(
                                ValidationCode.INVALID_VALUE,
                                "batch calldata exceeds resource limit",
                            )
                        if retry and simulated != prepared.call:
                            # A reverted close's successor is recorded and sent
                            # only as the call last simulated, below.
                            next_close = prepared
                        else:
                            next_reference: str | None
                            if not close:
                                next_reference = "settle:" + secrets.token_hex(32)
                            elif retry and pending is not None:
                                next_reference = _successor(pending.reference)
                            else:
                                next_reference = sessions[0].close_reference
                            assert next_reference is not None
                            reference = next_reference
                            pending = _PendingOperation(
                                prepared,
                                reference,
                                OperationResult(reference, OperationState.UNKNOWN),
                                {session.sid: session.units for session in sessions},
                            )
                            provider._references.setdefault(
                                reference, dict.fromkeys(session.sid for session in sessions)
                            )
                            for session in sessions:
                                session.operation = pending
                                session.last_collection = time.monotonic()
                            if close:
                                # The close intent names its current reference.
                                sessions[0].close_reference = reference
                            submit = True
                if next_close is None:
                    break
                # Simulated outside the lock, as a batch is. One that fails
                # records no intent and sends nothing; the attempt counts toward
                # the interval. One that passes is prepared again under the
                # lock, with every check, and recorded only if it is still the
                # simulated call.
                try:
                    with _timed("chain"):
                        await asyncio.to_thread(
                            provider.chain.reader.simulate,
                            next_close,
                            sender=provider.wallet.address,
                        )
                except Exception:
                    sessions[0].last_collection = time.monotonic()
                    raise
                simulated = next_close.call
            if submit and prepared.kind is OperationKind.SETTLE_BATCH:
                try:
                    with _timed("chain"):
                        await asyncio.to_thread(
                            provider.chain.reader.simulate, prepared, sender=provider.wallet.address
                        )
                except BaseException:
                    # No wallet call happened. Do not export a hashless failed
                    # intent that recovery would correctly treat as uncertain.
                    # The separate confirmed baseline remains untouched.
                    for session in sessions:
                        if session.operation is pending:
                            session.operation = None
                    raise
            assert pending is not None

            def unsent() -> None:
                for session in sessions:
                    if session.operation is pending:
                        session.operation = None

            answer: OperationResult | None = None
            if pending.result.state is OperationState.CONFIRMED:
                result = pending.result
            else:
                result = answer = await self._operate(
                    submit_operation if submit else reconcile_operation,
                    prepared,
                    reference,
                    unsent=unsent if submit else None,
                )
            # Preserve identifiers immediately. A later failed refresh cannot
            # turn a known submitted transaction back into a hashless intent.
            if not submit:
                known = pending.result
                conflict = (
                    known.wallet_operation_id is not None
                    and result.wallet_operation_id is not None
                    and known.wallet_operation_id != result.wallet_operation_id
                ) or (
                    known.tx_hash is not None
                    and result.tx_hash is not None
                    and known.tx_hash != result.tx_hash
                )
                if conflict or (known.tx_hash is not None and result.tx_hash is None):
                    # A wallet outage cannot discard the original transaction.
                    # Verify that retained hash directly, without any submission
                    # or dependence on the wallet lookup becoming available.
                    result = OperationResult(
                        reference, OperationState.UNKNOWN, known.wallet_operation_id, known.tx_hash
                    )
                    if known.tx_hash is not None:
                        try:
                            with _timed("chain"):
                                observed = await asyncio.to_thread(
                                    provider.chain.reader.reconcile,
                                    prepared,
                                    operation_ref=reference,
                                    tx_hash=known.tx_hash,
                                    wallet_operation_id=known.wallet_operation_id,
                                )
                            if (
                                observed.operation_ref == reference
                                and observed.tx_hash == known.tx_hash
                                and observed.wallet_operation_id == known.wallet_operation_id
                            ):
                                result = observed
                        except Exception:
                            pass
                elif result.wallet_operation_id is None and known.wallet_operation_id is not None:
                    result = replace(result, wallet_operation_id=known.wallet_operation_id)
            pending.result = result
            if answer is not None:
                # A record kept against an answer that disagrees with it is
                # handed to retire only through the wallet check (_unconfirmed);
                # an answer that matches clears it.
                if _agrees(result, answer):
                    provider._unconfirmed.pop(reference, None)
                elif reference in provider._references:
                    provider._unconfirmed[reference] = result
            if result.state is OperationState.CONFIRMED and prepared.kind in (
                OperationKind.SETTLE,
                OperationKind.SETTLE_BATCH,
            ):
                provider._observers.emit("on_settle", result)
            try:
                with _timed("chain"):
                    await provider.chain.refresh()
            except ChainEvidenceError:
                if close and prepared.kind.value != "close":
                    assert sessions[0].close_reference is not None
                    return OperationResult(sessions[0].close_reference, OperationState.UNKNOWN)
                return result
            closing = None
            if (
                prepared.kind.value == "close"
                and result.state is OperationState.CONFIRMED
                and result.tx_hash is not None
            ):
                with _timed("chain"):
                    closing = await asyncio.to_thread(
                        provider.chain.reader.closing, result.tx_hash, prepared.session_ids[0]
                    )
            async with provider._lock:
                assert pending is not None
                pending.result = result
                affected = [
                    provider._sessions[sid]
                    for sid in dict.fromkeys(prepared.session_ids)
                    if sid in provider._sessions
                ]
                for session in affected:
                    session.last_collection = time.monotonic()
                    target = max(
                        (
                            amount
                            for op_sid, amount in zip(
                                prepared.session_ids, prepared.amounts, strict=True
                            )
                            if op_sid == session.sid
                        ),
                        default=0,
                    )
                    old_target = (
                        max(
                            (
                                amount
                                for op_sid, amount in zip(
                                    session.baseline.prepared.session_ids,
                                    session.baseline.prepared.amounts,
                                    strict=True,
                                )
                                if op_sid == session.sid
                            ),
                            default=0,
                        )
                        if session.baseline is not None
                        else 0
                    )
                    if target > old_target and any(
                        item.session_id == session.sid
                        and item.target_covered is True
                        and item.target == target
                        for item in result.sessions
                    ):
                        session.confirmed_units = pending.units[session.sid]
                        session.baseline = pending
                    if closing is not None:
                        session.close_result = pb.CloseResult(
                            session_id=session.sid,
                            tx_hash=closing.tx_hash,
                            capture_amount=closing.capture_amount,
                            refunded=closing.refunded,
                        )
                        provider._observers.emit("on_close", session.close_result)
                        if session.owner is not None and session.owner.live:
                            provider._send(
                                session.owner, pb.ControlMessage(close_result=session.close_result)
                            )
                if result.state is OperationState.CONFIRMED:
                    provider._supersede(reference, (session.sid for session in affected))
            if close and prepared.kind.value != "close":
                # A reconciled capture is not evidence that the requested
                # cooperative close happened. Keep its separate stable intent.
                assert sessions[0].close_reference is not None
                return OperationResult(sessions[0].close_reference, OperationState.UNKNOWN)
            return result
