"""Native provider payment services composed into a caller-owned asyncio server.

One Mppi instance is the authoritative live payment state for its sessions. The
host must route operations for those sessions to that state owner, including
when its frontend uses separate Control and Complete connections. This module
does not coordinate independent processes or recover missing private history.
"""

from __future__ import annotations

import asyncio
import secrets
import time
from collections.abc import AsyncIterator
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
from mppi_protocol.capabilities import TransactionWallet
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
from .hooks import HookError, Hooks, ObserverStats
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
    ) -> None:
        self.code, self.reason, self.payment = code, reason, payment
        super().__init__(reason)


def _not_ready(reason: str, sid: bytes | None = None, rid: bytes | None = None) -> _RpcFailure:
    return _payment(PaymentFailure(PaymentCode.SESSION_NOT_READY, reason, sid, rid))


def _payment(failure: PaymentFailure) -> _RpcFailure:
    code, reason, _ = payment_status(failure)
    return _RpcFailure(code, reason, failure)


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
    operation: _PendingOperation | None = None
    baseline: _PendingOperation | None = None
    close_result: pb.CloseResult | None = None
    confirmed_units: int = 0
    last_collection: float = field(default_factory=time.monotonic)

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
        if hooks is not None and not isinstance(hooks, Hooks):
            raise TypeError("hooks must be provider Hooks")
        self._observers = Observers(hooks)
        self._control_auth_timeout = float(control_auth_timeout)
        self.config, self.chain, self.contract = config, chain, contract
        self.wallet, self.coverage, self.execution = wallet, coverage, execution
        self.runs = self
        self.sessions = _Sessions(self)
        self._sessions: dict[bytes, _Session] = {}
        self._condition = asyncio.Condition()
        self._started = False
        self._stopping = False
        self._start_block: int | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._maintenance: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._documents: DiscoveryDocuments | None = None
        self._collecting: set[bytes] = set()

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
        hooks: Hooks | None = None,
    ) -> Mppi:
        """Compose verified SDK adapters on the host's serving event loop.

        ``testnet=True`` selects the packaged official Arc testnet profile when
        ``profile`` is omitted. Mainnet requires an explicitly trusted profile.
        Network selection must match both the profile and confirmed TOML config.
        Circle credentials come from CIRCLE_API_KEY and CIRCLE_ENTITY_SECRET
        (64 hex characters). ``rpc_env`` selects an RPC URL from the provider's
        secret environment; otherwise the validated config's RPC URL is used.

        Only configuration and remote reads occur. Call ``start`` explicitly
        from the host lifecycle; the provider owns hosting, execution,
        usage reports, termination and retained state. Remote reads run in a
        worker thread, then loop-owned objects are constructed on this loop:
        https://docs.python.org/3.12/library/asyncio-task.html#asyncio.to_thread
        """
        from .setup import _prepare

        config, reader, wallet, contract = await asyncio.to_thread(
            _prepare, config_path, testnet=testnet, profile=profile, rpc_env=rpc_env
        )
        return cls(
            config,
            chain=ChainFollower(
                reader,
                max_age_ms=config.chain.max_age_ms,
                collection_margin_ms=config.payment.collection_margin_ms,
            ),
            contract=contract,
            wallet=wallet,
            coverage=coverage,
            execution=execution,
            control_auth_timeout=control_auth_timeout,
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
        async with self._condition:
            for session in self._sessions.values():
                self._stop_session(session)
            self._condition.notify_all()
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
            async with self._condition:
                self._detach(attachment)
                self._condition.notify_all()
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
                        async with self._condition:
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
                                session.close_pending = True
                                if session.close_reference is None:
                                    session.close_reference = "close:" + secrets.token_hex(32)
                                self._stop_session(session)
                                self._schedule_collection(session.sid, close=True)
                            # TopUpNotice is only a hint. The follower is the source
                            # of D; no credit or voucher acknowledgement is added.
                            self._condition.notify_all()
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
            async with self._condition:
                self._detach(attachment)
                self._condition.notify_all()
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
                evidence.block,
            )
            if agent_wallet != descriptor.agent:
                raise _RpcFailure(
                    grpc.StatusCode.UNAUTHENTICATED, "agent identity differs from funding wallet"
                )
        await self.chain.watch_session(notice.session_id)
        async with self._condition:
            existing = self._sessions.get(notice.session_id)
            if existing is not None:
                if (
                    existing.opening.descriptor != descriptor
                    or existing.opening.tx_hash != evidence.tx_hash
                ):
                    raise _not_ready("INCONSISTENT_STATE", notice.session_id)
                return
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
                and evidence.block.observation.block_number > self._start_block,
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
        async with self._condition:
            session = self._owner(attachment, voucher.session_id)
            if voucher.cumulative <= self._snapshot().sessions[session.sid].deposit:
                self._voucher(session, voucher)
                self._condition.notify_all()
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

        async with self._condition:
            session = self._owner(attachment, voucher.session_id)
            self._gate(session)
            self._voucher(session, voucher)
            self._condition.notify_all()

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
        async with self._condition:
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
            if request.last_meter_seq > session.meter_seq or any(
                rid not in session.runs for rid in request.pending_run_ids
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
            self._condition.notify_all()

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
        # Return all known definitive outcomes. Never replace an unknown id by
        # NOT_ADMITTED; observed rejected requests alone establish that status.
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
            async with self._condition:
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
            bound = await self.coverage(run.request, context)
            if (
                not isinstance(bound, TokenCounts)
                or bound.output_tokens < self.config.payment.output_buffer_tokens
            ):
                raise ValidationError(
                    ValidationCode.INVALID_VALUE,
                    "provider must supply prefill and configured output coverage",
                )
            await self._admit(session, run, bound)
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
                async with self._condition:
                    if not run.admitted:
                        run.not_admitted = True
                        run.request = PreparedRequest(b"", run.request.stream)
                    elif run.accounting.final_status is None:
                        run.stop.set()
                    self._condition.notify_all()

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
        previous_guidance: bytes | None = None
        async with self._condition:
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
                if session.voucher is not None and session.voucher.cumulative >= amount:
                    if len(self._active(session)) >= 1024:
                        raise _RpcFailure(
                            grpc.StatusCode.RESOURCE_EXHAUSTED, "active run recovery limit"
                        )
                    run.covered_counts = bound
                    run.accounting = RunAccounting(_ZERO, priced_numerator(bound, run.rates))
                    run.admitted = True
                    return
                if expires is None:
                    expires = now + self.config.payment.authorization_wait_ms / 1000
                encoded = guidance.SerializeToString()
                if previous_guidance != encoded:
                    assert session.owner is not None
                    self._send(session.owner, guidance)
                    previous_guidance = encoded
                try:
                    await asyncio.wait_for(self._condition.wait(), min(expires - now, 0.05))
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
            async with self._condition:
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
            error = _not_ready("STATE_UNAVAILABLE", run.context.session_id, run.context.run_id)
        finally:
            if run.terminal is not None and error is None:
                try:
                    await run.output.put(pb.ChatChunk(terminal=run.terminal).SerializeToString())
                except Exception:
                    error = _RpcFailure(grpc.StatusCode.RESOURCE_EXHAUSTED, "Complete queue limit")
            elif error is None:
                run.stop.set()
                error = _not_ready("STATE_UNAVAILABLE", run.context.session_id, run.context.run_id)
            async with self._condition:
                if run.accounting.final_status is None:
                    run.unresolved = True
                    run.stop.set()
                run.request = PreparedRequest(b"", run.request.stream)
                self._condition.notify_all()
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
        async with self._condition:
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
        async with self._condition:
            result, _ = self._report(context, counts, None)
            return result

    async def report_terminal(
        self, context: RunContext, counts: TokenCounts, status: RunStatus
    ) -> FinalAccountingResult:
        async with self._condition:
            result, _ = self._report(context, counts, status)
            self._condition.notify_all()
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
        self._condition.notify_all()
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
            async with self._condition:
                for session in self._sessions.values():
                    try:
                        self._gate(session)
                    except _RpcFailure:
                        if any(not run.stop.is_set() for run in self._active(session)):
                            self._stop_session(session)
                self._condition.notify_all()
            due_sessions: list[bytes] = []
            for session in tuple(self._sessions.values()):
                if session.close_pending:
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
                    if amount and due and state.status is SessionStatus.OPEN:
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
                await group(ids)
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

    async def get(self, sid: bytes) -> SessionView:
        fixed_bytes(sid, 32, field="session_id")
        provider = self._provider
        async with provider._condition:
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
        async with self._operations, provider._condition:
            session = provider._sessions.get(sid)
            if session is None or not session.history_available:
                raise _not_ready("STATE_UNAVAILABLE", sid)
            if any(not run.admitted and not run.not_admitted for run in session.runs.values()):
                raise _not_ready("STATE_UNAVAILABLE", sid)
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

    def _restore_operation(
        self, evidence: RecoveryOperation | None, sid: bytes
    ) -> _PendingOperation | None:
        if evidence is None:
            return None
        provider = self._provider
        prepared, result = evidence.prepared, evidence.result
        if (
            prepared.kind
            not in (OperationKind.SETTLE, OperationKind.SETTLE_BATCH, OperationKind.CLOSE)
            or prepared.call.chain_id != provider.contract.domain.chain_id
            or prepared.call.to != provider.contract.domain.verifying_contract
            or sid not in prepared.session_ids
            or len(dict(evidence.units)) != len(evidence.units)
            or set(dict(evidence.units)) != set(prepared.session_ids)
            or any(type(value) is not int or value < 0 for _, value in evidence.units)
        ):
            raise _not_ready("INCONSISTENT_STATE", sid)
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
        if result.tx_hash is not None:
            # A saved hash is caller-supplied history. Bind it to the original
            # wallet intent before canonical receipts can establish failure
            # (including when a currently pending receipt later reverts).
            try:
                bound = provider.wallet.reconcile(operation_ref=result.operation_ref)
            except Exception:
                raise _not_ready("STATE_UNAVAILABLE", sid) from None
            if (
                not isinstance(bound, OperationResult)
                or bound.operation_ref != result.operation_ref
                or bound.tx_hash != result.tx_hash
                or (
                    result.wallet_operation_id is not None
                    and bound.wallet_operation_id != result.wallet_operation_id
                )
                or (
                    bound.wallet_operation_id is not None
                    and (
                        type(bound.wallet_operation_id) is not str or not bound.wallet_operation_id
                    )
                )
            ):
                raise _not_ready("INCONSISTENT_STATE", sid)
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
                raise _not_ready("INCONSISTENT_STATE", sid)
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
            raise _not_ready("INCONSISTENT_STATE", sid)
        fixed_bytes(evidence.opening_tx, 32, field="opening_tx")
        descriptor = descriptor_from_wire(record.descriptor)
        if (
            descriptor.provider_id != provider.config.identity.provider_id
            or descriptor.operator != provider.wallet.address
            or type(evidence.inconsistent) is not bool
            or (record.close_pending != (evidence.close_reference is not None))
            or (evidence.close_reference is not None and not evidence.close_reference)
        ):
            raise _not_ready("INCONSISTENT_STATE", sid)
        catalogs = {item.rate_card_hash: parse_catalog(item.catalog) for item in record.catalogs}
        if any(key != value.rate_card_hash for key, value in catalogs.items()):
            raise _not_ready("INCONSISTENT_STATE", sid)
        if record.initial_rate_card_hash not in catalogs or (
            record.HasField("current_rate_card_hash")
            and record.current_rate_card_hash not in catalogs
        ):
            raise _not_ready("STATE_UNAVAILABLE", sid)
        opening = reader.opening(
            evidence.opening_tx, sid, accepted_catalog=catalogs[record.initial_rate_card_hash]
        )
        if opening.descriptor != descriptor:
            raise _not_ready("INCONSISTENT_STATE", sid)
        block = reader.block(evidence.observation.block_number)
        if (
            block.observation != evidence.observation
            or record.chain_block_number != block.observation.block_number
            or record.chain_block_hash != block.observation.block_hash
            or block.observation.block_number < opening.block.observation.block_number
        ):
            raise _not_ready("INCONSISTENT_STATE", sid)
        chain_state = reader.session(sid, block)
        if (
            not chain_state.exists
            or chain_state.deposit != record.deposit
            or chain_state.settled_cumulative != record.settled_cumulative
            or _SESSION_STATUS[chain_state.status] != record.status
            or chain_state.initial_rate_card_hash != record.initial_rate_card_hash
        ):
            raise _not_ready("INCONSISTENT_STATE", sid)
        current_identity = reader.identity(
            descriptor.provider_id, block, allow_unavailable_catalog=True
        )
        current_catalog = current_identity.catalog
        if (
            record.HasField("current_rate_card_hash") != (current_catalog is not None)
            or current_catalog is not None
            and record.current_rate_card_hash != current_catalog.rate_card_hash
        ):
            raise _not_ready("INCONSISTENT_STATE", sid)
        publications = dict(evidence.publications)
        if len(publications) != len(evidence.publications) or not set(publications) <= set(
            catalogs
        ):
            raise _not_ready("INCONSISTENT_STATE", sid)
        for key, catalog in catalogs.items():
            if key == record.initial_rate_card_hash:
                continue
            observation = publications.get(key)
            if observation is None or observation.block_number > block.observation.block_number:
                raise _not_ready("STATE_UNAVAILABLE", sid)
            published_block = reader.block(observation.block_number)
            if published_block.observation != observation:
                raise _not_ready("INCONSISTENT_STATE", sid)
            identity = reader.identity(descriptor.provider_id, published_block)
            if identity.catalog != catalog:
                raise _not_ready("INCONSISTENT_STATE", sid)
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
                raise _not_ready("INCONSISTENT_STATE", sid)
            verify_run_auth(provider.contract.domain, descriptor, context, item.signature)
            rates = catalogs[context.rate_card_hash].rates_for(context.model)
            uint64(item.meter_seq, field="meter sequence")
            if item.meter_seq > record.meter_seq or (item.meter_seq and item.meter_seq in cursors):
                raise _not_ready("INCONSISTENT_STATE", sid)
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
                    raise _not_ready("INCONSISTENT_STATE", sid)
                key = (context.model, context.rate_card_hash)
                totals[key] = _sum_counts(totals.get(key, _ZERO), accounting.counts)
            elif (
                accounting != RunAccounting(_ZERO, 0)
                or item.covered_counts != _ZERO
                or item.meter_seq
            ):
                raise _not_ready("INCONSISTENT_STATE", sid)
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
            raise _not_ready("INCONSISTENT_STATE", sid)
        actual_reservations = tuple(
            (run.context, run.accounting) for run in provider._active(session)
        )
        if dict(evidence.reservations) != dict(actual_reservations) or len(
            evidence.reservations
        ) != len(actual_reservations):
            raise _not_ready("INCONSISTENT_STATE", sid)
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
                raise _not_ready("INCONSISTENT_STATE", sid)
            stated_totals[key] = TokenCounts(row.input_tokens, row.cache_tokens, row.output_tokens)
        if totals != stated_totals:
            raise _not_ready("INCONSISTENT_STATE", sid)
        finals = {row.run_id: row for row in record.run_states}
        for run in session.runs.values():
            final = finals.pop(run.context.run_id, None)
            if run.accounting.final_status is None and run.admitted:
                if final is not None:
                    raise _not_ready("INCONSISTENT_STATE", sid)
                continue
            if final is None:
                raise _not_ready("STATE_UNAVAILABLE", sid)
            if run.not_admitted:
                if final.status != pb.RUN_STATUS_NOT_ADMITTED:
                    raise _not_ready("INCONSISTENT_STATE", sid)
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
                raise _not_ready("INCONSISTENT_STATE", sid)
        if finals:
            raise _not_ready("INCONSISTENT_STATE", sid)
        required = required_coverage(
            (
                session.usage_numerator,
                *(run.accounting.remaining_numerator for run in provider._active(session)),
            )
        )
        if record.HasField("accepted_voucher"):
            voucher = record.accepted_voucher
            verify_voucher(
                provider.contract.domain, descriptor, voucher.cumulative, voucher.signature
            )
            if voucher.session_id != sid or not required <= voucher.cumulative <= record.deposit:
                raise _not_ready("INCONSISTENT_STATE", sid)
            session.voucher = pb.Voucher.FromString(voucher.SerializeToString())
        elif any(run.admitted for run in session.runs.values()) or record.settled_cumulative:
            raise _not_ready("STATE_UNAVAILABLE", sid)
        if session.usage < record.settled_cumulative:
            raise _not_ready("INCONSISTENT_STATE", sid)
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
                raise _not_ready("INCONSISTENT_STATE", sid)
            if operation.prepared.kind is OperationKind.CLOSE:
                if not session.close_pending or session.close_reference != operation.reference:
                    raise _not_ready("INCONSISTENT_STATE", sid)
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
            raise _not_ready("INCONSISTENT_STATE", sid)
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
                raise _not_ready("STATE_UNAVAILABLE", sid)
        session.confirmed_units = evidence.confirmed_units
        # Every independently observed historical block remains canonical.
        if reader.block(block.observation.block_number) != block:
            raise _not_ready("INCONSISTENT_STATE", sid)
        return session

    async def recover(self, evidence: RecoveryEvidence) -> SessionView:
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
        async with self._operations:
            candidate = await asyncio.to_thread(self._restore, evidence)
            await provider.chain.watch_session(sid)
            async with provider._condition:
                current = provider.chain.snapshot().sessions[sid]
                provider._history_fits(candidate)
                if (
                    candidate.usage < current.settled_cumulative
                    or current.deposit < evidence.snapshot.deposit
                    or current.initial_rate_card_hash
                    != candidate.opening.initial_catalog.rate_card_hash
                    or _SESSION_STATUS[current.status] < evidence.snapshot.status
                ):
                    raise _not_ready("INCONSISTENT_STATE", sid)
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
                        raise _not_ready("INCONSISTENT_STATE", sid)
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
                        raise _not_ready("STATE_UNAVAILABLE", sid)
                    for rid, run in previous.runs.items():
                        incoming = candidate.runs[rid]
                        if (
                            run.context != incoming.context
                            or run.signature != incoming.signature
                            or run.admitted != incoming.admitted
                        ):
                            raise _not_ready("INCONSISTENT_STATE", sid)
                        report_transition(
                            run.accounting,
                            incoming.accounting.counts,
                            run.rates,
                            final_status=incoming.accounting.final_status,
                        )
                # A recovered batch shares one retained operation object across
                # all members, so overlapping operations cannot resubmit it.
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
                                raise _not_ready("INCONSISTENT_STATE", sid)
                            candidate.operation = operation
                provider._sessions[sid] = candidate
                provider._condition.notify_all()
        return await self.get(sid)

    async def settle(self, sid: bytes) -> OperationResult:
        return await self._collect((sid,), close=False)

    async def settleBatch(self, session_ids: tuple[bytes, ...]) -> OperationResult:
        """One exact ordered batch; repeated ids preserve contract item semantics."""
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
                result = await asyncio.to_thread(
                    helper,
                    provider.wallet,
                    pending.prepared,
                    operation_ref=pending.reference,
                    chain=provider.chain.reader,
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
        return await self._collect((sid,), close=True)

    async def _collect(
        self, ids: tuple[bytes, ...], *, close: bool, batch: bool = False
    ) -> OperationResult:
        provider = self._provider
        async with self._operations:
            async with provider._condition:
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
                    sessions[0].close_pending = True
                    if sessions[0].close_reference is None:
                        sessions[0].close_reference = "close:" + secrets.token_hex(32)
                    provider._stop_session(sessions[0])
                    provider._condition.notify_all()
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
                if (
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
                            return OperationResult(session.close_reference, OperationState.UNKNOWN)
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
                        items.append((session.opening.descriptor, cumulative, capture, signature))
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
                    if len(prepared.call.data) > MAX_MESSAGE_BYTES:
                        raise ValidationError(
                            ValidationCode.INVALID_VALUE, "batch calldata exceeds resource limit"
                        )
                    next_reference = (
                        sessions[0].close_reference if close else "settle:" + secrets.token_hex(32)
                    )
                    assert next_reference is not None
                    reference = next_reference
                    pending = _PendingOperation(
                        prepared,
                        reference,
                        OperationResult(reference, OperationState.UNKNOWN),
                        {session.sid: session.units for session in sessions},
                    )
                    for session in sessions:
                        session.operation = pending
                    submit = True
            if submit and prepared.kind is OperationKind.SETTLE_BATCH:
                try:
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
            helper = submit_operation if submit else reconcile_operation
            result = await asyncio.to_thread(
                helper,
                provider.wallet,
                prepared,
                operation_ref=reference,
                chain=provider.chain.reader,
            )
            # Preserve identifiers immediately. A later failed refresh cannot
            # turn a known submitted transaction back into a hashless intent.
            assert pending is not None
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
            if result.state is OperationState.CONFIRMED and prepared.kind in (
                OperationKind.SETTLE,
                OperationKind.SETTLE_BATCH,
            ):
                provider._observers.emit("on_settle", result)
            try:
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
                closing = await asyncio.to_thread(
                    provider.chain.reader.closing, result.tx_hash, prepared.session_ids[0]
                )
            async with provider._condition:
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
                provider._condition.notify_all()
            if close and prepared.kind.value != "close":
                # A reconciled capture is not evidence that the requested
                # cooperative close happened. Keep its separate stable intent.
                assert sessions[0].close_reference is not None
                return OperationResult(sessions[0].close_reference, OperationState.UNKNOWN)
            return result
