"""Synchronous MPPI client with one owning background asyncio thread.

All grpc.aio objects live on that thread. Wallet, HTTP and chain I/O run outside
the event loop; Control progresses independently of caller output consumption.
No inference replay, implicit top-up, replacement session or automatic close.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import secrets
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Callable, Coroutine, Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType, TracebackType
from typing import Any, TypeVar, cast
from urllib.parse import urlsplit

import grpc
from mppi.v1 import mppi_pb2 as pb
from mppi.v1 import mppi_pb2_grpc as services
from mppi_protocol._observers import Observers
from mppi_protocol.amounts import (
    TokenCounts,
    funding_amount,
    priced_numerator,
    required_coverage,
    top_up_total,
    uint64,
)
from mppi_protocol.capabilities import (
    FundingAuthorization,
    FundingWallet,
    SessionSigner,
    WalletCall,
)
from mppi_protocol.catalog import Catalog, parse_catalog, validate_model
from mppi_protocol.chain import ChainEvidenceError, ChainLagError, ChainSession, DeploymentProfile
from mppi_protocol.contract import ContractCalls, OperationKind, PreparedOperation
from mppi_protocol.discovery import ProviderIdentity, normalize_origin, parse_document
from mppi_protocol.errors import ValidationError
from mppi_protocol.follower import ChainFollower, ChainSnapshot, RefreshFailure
from mppi_protocol.http import HttpTransport, TransportError
from mppi_protocol.operations import reconcile_operation, submit_operation
from mppi_protocol.signing import (
    verify_close_auth,
    verify_reconnect_auth,
    verify_run_auth,
)
from mppi_protocol.types import (
    ChainObservation,
    ConfirmedSettlementRef,
    OperationResult,
    OperationState,
    PauseLevel,
    PaymentCode,
    PaymentFailure,
    Readiness,
    RunContext,
    SessionDescriptor,
    SessionPage,
    SessionStatus,
    SessionView,
    ShutdownResult,
    fixed_bytes,
    uint256,
)
from mppi_protocol.wire import (
    GRPC_OPTIONS,
    ByteQueue,
    descriptor_from_wire,
    descriptor_to_wire,
    observation_from_wire,
    parse_payment_error,
    validate_json,
    validate_message,
)

from .checkpoint import Checkpoint
from .discovery import ProviderDiscovery, discover, verify_challenge
from .hooks import HookError, Hooks, ObserverStats, OperationTiming
from .interfaces import FundingNotice, OpenResult, RunOutcome, RunOutcomeStatus, SessionTerms
from .state import Ledger, Run, monotone

T = TypeVar("T")

# Control reports that are rejected alone. One such report that fails
# validation or cannot be proven now is set aside: nothing it carried is
# applied, and the attachment and every run continue. A rejected
# Meter also stops the session cursor; see Client._meter.
_SINGLE_REPORTS = frozenset({"challenge", "low_credit", "receipt", "drain"})
_PAYMENT_REPORTS = frozenset({"meter", "low_credit", "receipt", "drain", "close_result"})
# Settled final runs still waiting for their Receipt, kept before the oldest is
# pruned anyway: Receipt delivery is not guaranteed (protocol.md, Definitive
# outcomes and settlement).
_UNRECEIPTED_FINALS = 1024
# Longest a stuck attachment waits for live runs before ending them; see _meter.
_RESYNC_WAIT_S = 60.0
# Proven Receipt observations kept: the provider stamps each Receipt with its
# follower's current block, so a session's Receipts share few observations.
_PROVEN_OBSERVATIONS = 64
# Receipts waiting for proof: as many as runs may be pending (state.py register).
_RECEIPT_QUEUE = 1024
# Receipt proofs that fail after reading chain state, per attachment, before
# this attachment stops reading for Receipts (a provider wasting the agent's RPC).
_RECEIPT_FAILURES = 16


def _refused_before_admission(error: Exception) -> bool:
    """A Complete refusal that the protocol defines only before admission:
    INSUFFICIENT_DEPOSIT, and SESSION_NOT_READY for AUTHORIZATION_REQUIRED
    (the pre-admission wait expired) or PRICE_CHANGED (agent-sdk.md, Errors)."""
    if not isinstance(error, PaymentError):
        return False
    failure = error.failure
    return failure.code is PaymentCode.INSUFFICIENT_DEPOSIT or (
        failure.code is PaymentCode.SESSION_NOT_READY
        and failure.reason in ("AUTHORIZATION_REQUIRED", "PRICE_CHANGED")
    )


class _Timing:
    """One money step's time by part, in seconds (see OperationTiming)."""

    def __init__(self) -> None:
        self.start = time.monotonic()
        self.parts = {"chain": 0.0, "wallet_sign": 0.0, "wallet_submit": 0.0, "provider": 0.0}


_TIMING: contextvars.ContextVar[_Timing | None] = contextvars.ContextVar(
    "mppi_timing", default=None
)


@contextmanager
def _timed(part: str) -> Iterator[None]:
    """Add the time of this block to the current money step's ``part``."""
    timing = _TIMING.get()
    start = time.monotonic()
    try:
        yield
    finally:
        if timing is not None:
            timing.parts[part] += time.monotonic() - start


class _TimedWallet:
    """A wallet whose execute calls count as wallet time; nothing else changes."""

    def __init__(self, wallet: FundingWallet, timing: _Timing | None) -> None:
        self._wallet, self._timing = wallet, timing
        self.spent = 0.0

    @property
    def address(self) -> bytes:
        return self._wallet.address

    def execute(self, call: WalletCall, *, operation_ref: str) -> OperationResult:
        start = time.monotonic()
        try:
            return self._wallet.execute(call, operation_ref=operation_ref)
        finally:
            self.spent += time.monotonic() - start

    def reconcile(self, *, operation_ref: str) -> OperationResult:
        return self._wallet.reconcile(operation_ref=operation_ref)


class ClientError(RuntimeError):
    """Local eligibility, recovery or transport failure; no automatic retry."""


_REPORT_FAILURES = (ValidationError, ClientError, ChainEvidenceError, TransportError)


class PaymentError(ClientError):
    def __init__(self, failure: PaymentFailure) -> None:
        self.failure = failure
        super().__init__(failure.code.value + (": " + failure.reason if failure.reason else ""))


class FundingAuthorizationError(ClientError):
    """Funding refused before submission; nothing was sent, and it may be retried.

    ``reason`` is ``AGENT_ID_REQUIRED`` (the client has no ERC-8004 agent id),
    ``AGENT_WALLET_MISMATCH`` (the registry does not bind the id to this
    funding wallet, so the provider would refuse the session),
    ``INVALID_SIGNATURE`` (USDC would refuse the wallet's authorization) or
    ``SIGNATURE_DEFERRED_UNDECLARED`` (an all-zero placeholder from a wallet
    that has not declared ``signs_at_execute``).
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _detail(kind: str | None, error: BaseException) -> str | None:
    """The message kind and the SDK's own reason; never provider or endpoint text."""
    if isinstance(error, PaymentError) or not isinstance(error, _REPORT_FAILURES):
        return kind
    reason = str(error)[:200]
    return f"{kind}: {reason}" if kind else reason


class InferenceError(ClientError):
    def __init__(self, body: bytes, http_status: int | None = None) -> None:
        self.body, self.http_status = body, http_status
        super().__init__("provider inference failed")


class CloseAccountingError(ClientError):
    """A confirmed close exceeded validated usage; its payment is not unknown."""

    def __init__(self, operation: OperationResult, capture_amount: int, usage_amount: int) -> None:
        self.operation, self.capture_amount, self.usage_amount = (
            operation,
            capture_amount,
            usage_amount,
        )
        super().__init__("confirmed close captured more than validated usage")


@dataclass(slots=True)
class _Output:
    queue: ByteQueue
    task: asyncio.Task[None] | None = None
    call: Any = None
    terminal: pb.Terminal | None = None
    receipt: pb.Receipt | None = None


class CompletionStream(Iterator[bytes]):
    """Original provider JSON messages. Closing cancels work, never proves usage."""

    def __init__(self, client: Client, run_id: bytes, output: _Output | None = None) -> None:
        self._client, self.run_id = client, run_id
        # Its own record of the output: the client drops its index entry once
        # the run is settled and pruned, possibly before this stream is read.
        self._output = output if output is not None else client._outputs[run_id]
        self._closed = False
        self._read_lock = threading.Lock()

    def __iter__(self) -> CompletionStream:
        return self

    def __next__(self) -> bytes:
        with self._read_lock:
            if self._closed:
                raise StopIteration
            try:
                value = self._client._sync(self._client._next(self._output))
            except BaseException:
                self._closed = True
                raise
            if value is None:
                self._closed = True
                raise StopIteration
            return value

    @property
    def terminal(self) -> pb.Terminal | None:
        return cast(
            pb.Terminal | None,
            self._client._sync(self._client._final_record(self._output, receipt=False)),
        )

    @property
    def receipt(self) -> pb.Receipt | None:
        return cast(
            pb.Receipt | None,
            self._client._sync(self._client._final_record(self._output, receipt=True)),
        )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._client._sync(self._client._cancel_run(self.run_id, self._output))

    def __enter__(self) -> CompletionStream:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class _Completions:
    def __init__(self, client: Client) -> None:
        self._client = client

    def create(
        self, *, model: str, messages: list[object], stream: bool = False, **fields: object
    ) -> CompletionStream | bytes:
        if type(stream) is not bool:
            raise ClientError("stream must be a boolean")
        try:
            payload = json.dumps(
                fields | {"model": model, "messages": messages, "stream": stream},
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (ValueError, TypeError, UnicodeError):
            raise ClientError("inference request is not valid JSON") from None
        return self.create_raw(payload, model=model, stream=stream)

    def create_raw(self, payload: bytes, *, model: str, stream: bool) -> CompletionStream | bytes:
        validate_model(model)
        validate_json(payload, model=model, stream=stream)
        run_id, output = self._client._sync(self._client._create(model, stream, payload))
        result = CompletionStream(self._client, run_id, output)
        if stream:
            return result
        try:
            response = next(result)
            try:
                next(result)
            except StopIteration:
                return response
            raise ClientError("nonstreaming response contained more than one JSON object")
        except StopIteration:
            raise ClientError("nonstreaming response omitted its JSON object") from None
        finally:
            result.close()


class _Chat:
    def __init__(self, client: Client) -> None:
        self.completions = _Completions(client)


class Sessions:
    """Explicit economic operations for this Client's selected provider."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def open(
        self, provider: str | int, *, model: str, initial_session_tokens: int | None = None
    ) -> OpenResult:
        self._client._check_provider(provider)
        client = self._client
        return client._sync(client._measured("open", client._open(model, initial_session_tokens)))

    def topUp(self, session_id: bytes, amount: int) -> OperationResult:
        client = self._client
        return client._sync(client._measured("topUp", client._top_up(session_id, amount)))

    def close(self, session_id: bytes) -> OperationResult:
        client = self._client
        return client._sync(client._measured("close", client._cooperative_close(session_id)))

    def requestClose(self, session_id: bytes) -> OperationResult:
        return self._exit(session_id, OperationKind.REQUEST_CLOSE)

    def withdraw(self, session_id: bytes) -> OperationResult:
        return self._exit(session_id, OperationKind.WITHDRAW)

    def releaseIdle(self, session_id: bytes) -> OperationResult:
        return self._exit(session_id, OperationKind.RELEASE_IDLE)

    def _exit(self, session_id: bytes, kind: OperationKind) -> OperationResult:
        client = self._client
        return client._sync(client._measured(kind.value, client._exit(session_id, kind)))

    def get(self, session_id: bytes) -> SessionView:
        return self._client._sync(self._client._get(session_id))

    def list(self, *, cursor: str | None = None, limit: int = 100) -> SessionPage:
        if cursor is not None or type(limit) is not int or not 1 <= limit <= 1000:
            raise ClientError("invalid session page request")
        return self._client._sync(self._client._list())

    def reconcile(self, operation_ref: str) -> OperationResult:
        return self._client._sync(self._client._reconcile_ref(operation_ref))

    def terms(self, session_id: bytes) -> SessionTerms:
        """The session's accepted prices; reads no chain."""
        return self._client._sync(self._client._terms(session_id))

    def run(self, session_id: bytes, run_id: bytes) -> RunOutcome:
        """A retained run's outcome; reads no chain. A run that finished and
        was proven is forgotten after a while: its usage stays in the totals."""
        return self._client._sync(self._client._run_outcome(session_id, run_id))

    def recover(self, session_id: bytes) -> SessionView:
        """Authenticate and reconcile retained history without funding or inference."""
        return self._client._sync(self._client._recover(session_id))


class Client:
    """One provider, funding wallet and recoverable fixed signer.

    ``checkpoint_path`` must be reused across restarts for this payment scope.
    The caller keeps its signer credential in its wallet/secret service and
    supplies only its recoverable reference here. macOS and Linux are supported.
    The supplied ChainFollower is exclusively owned by this Client's loop.
    """

    @classmethod
    def from_config(
        cls,
        provider: str | int,
        *,
        config_path: Path = Path(".mppi/agent.json"),
        initial_session_tokens: int | None = None,
        ca_file: str | None = None,
        testnet: bool = True,
        profile: DeploymentProfile | None = None,
        max_age_ms: int = 60_000,
        poll_interval_ms: int | None = None,
    ) -> Client:
        """Use completed CLI onboarding; funding still requires an explicit choice.

        Opening, attachment, top-up and close use evidence younger than
        ``max_age_ms`` (default 60 s); the chain follower polls every
        ``poll_interval_ms`` (default ``max_age_ms``). Requests within an
        attached session read no chain.
        """
        from .setup import client_from_config

        return client_from_config(
            provider,
            config_path=config_path,
            initial_session_tokens=initial_session_tokens,
            ca_file=ca_file,
            testnet=testnet,
            profile=profile,
            max_age_ms=max_age_ms,
            poll_interval_ms=poll_interval_ms,
        )

    def __init__(
        self,
        provider: str | int,
        *,
        wallet: FundingWallet,
        signer: SessionSigner,
        chain: ChainFollower,
        contract: ContractCalls,
        checkpoint_path: Path,
        signer_reference: str,
        initial_session_tokens: int | None = None,
        agent_id: int | None = None,
        ca_file: str | None = None,
        timeout_ms: int = 10_000,
        request_timeout_ms: int | None = None,
        queue_wait_ms: int = 1000,
        queue_messages: int = 64,
        queue_bytes: int = 16 << 20,
        on_low_credit: Callable[[pb.LowCredit], None] | None = None,
        on_funding_notice: Callable[[FundingNotice], None] | None = None,
        hooks: Hooks | None = None,
    ) -> None:
        if isinstance(provider, str):
            provider = normalize_origin(provider)
        else:
            uint256(provider, field="provider")
        if contract.domain != chain.reader.profile.domain:
            raise ClientError("contract and chain profiles differ")
        fixed_bytes(wallet.address, 20, field="funding wallet", nonzero=True)
        fixed_bytes(signer.address, 20, field="session signer", nonzero=True)
        if (
            type(signer_reference) is not str
            or not signer_reference
            or len(signer_reference) > 4096
        ):
            raise ClientError("a recoverable signer reference is required")
        for name, value in (("timeout_ms", timeout_ms), ("queue_wait_ms", queue_wait_ms)):
            uint64(value, field=name, positive=True)
        if request_timeout_ms is not None:
            uint64(request_timeout_ms, field="request_timeout_ms", positive=True)
        if initial_session_tokens is not None:
            uint64(initial_session_tokens, field="initial_session_tokens", positive=True)
        if agent_id is not None:
            uint256(agent_id, field="agent_id")
        if on_low_credit is not None and not callable(on_low_credit):
            raise ClientError("on_low_credit must be callable")
        if on_funding_notice is not None and not callable(on_funding_notice):
            raise ClientError("on_funding_notice must be callable")
        # Validate queue settings before starting an owned thread.
        ByteQueue(max_messages=queue_messages, max_bytes=queue_bytes, wait_ms=queue_wait_ms)
        self.provider, self.wallet, self.signer = provider, wallet, signer
        self.chain, self.contract, self.signer_reference = chain, contract, signer_reference
        self.initial_session_tokens, self.agent_id = initial_session_tokens, agent_id
        self.timeout, self.request_timeout = (
            timeout_ms / 1000,
            None if request_timeout_ms is None else request_timeout_ms / 1000,
        )
        self.ca_file = ca_file
        if hooks is not None and not isinstance(hooks, Hooks):
            raise TypeError("hooks must be agent Hooks")
        self._observers = Observers(hooks)
        if isinstance(chain, ChainFollower) and chain.on_refresh_failure is None:
            chain.on_refresh_failure = self._refresh_failed
        self._on_low_credit = on_low_credit
        self._on_funding_notice = on_funding_notice
        # The latest undelivered funding notice: (LowCredit bytes, deposit then,
        # required). Only the latest matters; delivery works out the rest again.
        self._funding: tuple[bytes, int, int | None] | None = None
        self._low_credit_task: asyncio.Task[None] | None = None
        self._queue_options = {
            "max_messages": queue_messages,
            "max_bytes": queue_bytes,
            "wait_ms": queue_wait_ms,
        }
        self._http = HttpTransport(timeout_ms=timeout_ms, ca_file=ca_file)
        self._checkpoint = Checkpoint(checkpoint_path)
        self._saved = self._checkpoint.load() or {"version": 1}
        self._ledger: Ledger | None = None
        self._identity: ProviderIdentity | None = None
        self._discovery: ProviderDiscovery | None = None
        self._operations: dict[str, tuple[PreparedOperation, OperationResult]] = {}
        self._opening_ref: str | None = None
        self._outputs: dict[bytes, _Output] = {}
        self._channel: grpc.aio.Channel | None = None
        self._control: Any = None
        self._control_tasks: list[asyncio.Task[None]] = []
        self._background: set[asyncio.Task[None]] = set()
        self._renew_task: asyncio.Task[None] | None = None
        self._voucher_task: asyncio.Task[None] | None = None
        self._pending_vouchers: dict[bytes, tuple[int, int]] = {}
        # A confirmed top-up's transaction, to announce once Control is attached.
        self._top_up_hint: bytes | None = None
        # Runs registered and persisted whose Complete has not been sent yet.
        self._unsent: set[bytes] = set()
        # A rejected Meter stopped this attachment's session cursor, at _resync_at.
        self._resync: Exception | None = None
        self._resync_at = 0.0
        # Receipt proofs run one at a time from a bounded queue, once per run
        # per attachment, so they never crowd the follower's chain reads.
        self._receipt_queue: deque[tuple[pb.Receipt, int]] = deque()
        self._receipt_task: asyncio.Task[None] | None = None
        self._receipts_seen: set[bytes] = set()
        self._receipt_failures = 0
        # Runs whose Receipt was verified; see _prune.
        self._receipted: set[bytes] = set()
        # (session, block number, block hash) -> settled total this agent read
        # itself at that canonical block; see _verify_receipt.
        self._proven: OrderedDict[tuple[bytes, int, bytes], int] = OrderedDict()
        self._outbound: ByteQueue | None = None
        self._nonce: asyncio.Future[bytes] | None = None
        self._snapshot_waiter: asyncio.Future[pb.SessionState] | None = None
        self._snapshot_applied: asyncio.Event | None = None
        self._challenges: dict[bytes, tuple[str, asyncio.Future[pb.Challenge]]] = {}
        self._close_waiter: asyncio.Future[OperationResult] | None = None
        self._closed_result: OperationResult | None = None
        self._closed_error: CloseAccountingError | None = None
        self._control_failure: Exception | None = None
        self._attached_generation = 0
        self._started = False
        self._restored = False
        self._local_started = False
        self._closed = False
        self._shutdown_result: ShutdownResult | None = None
        self._thread_ready = threading.Event()
        self._thread = threading.Thread(target=self._thread_main, name="mppi-agent", daemon=True)
        self._thread.start()
        self._thread_ready.wait()
        self.sessions, self.chat = Sessions(self), _Chat(self)

    @property
    def hook_stats(self) -> ObserverStats:
        return self._observers.stats()

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._open_lock, self._checkpoint_lock, self._voucher_lock = (
            asyncio.Lock(),
            asyncio.Lock(),
            asyncio.Lock(),
        )
        self._start_lock = asyncio.Lock()
        # Serialises top-ups among themselves and with close/exit (taken after
        # _open_lock there). Requests never wait for a wallet operation.
        self._funding_lock = asyncio.Lock()
        self._operation_locks: dict[str, asyncio.Lock] = {}
        self._thread_ready.set()
        try:
            self._loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.run_until_complete(
                self._loop.shutdown_default_executor(timeout=self.timeout)  # type: ignore[call-arg]  # Python >=3.12; AbstractEventLoop stub omits this argument.
            )
            self._loop.close()

    def _sync(self, coroutine: Coroutine[Any, Any, T]) -> T:
        if self._closed or threading.current_thread() is self._thread:
            coroutine.close()
            raise ClientError("client is closed or called from its owning loop")
        try:
            return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result()
        except Exception as error:
            self._observers.emit("on_error", HookError("client", type(error).__name__))
            raise

    def _check_provider(self, provider: str | int) -> None:
        selected = normalize_origin(provider) if type(provider) is str else provider
        if selected != self.provider and (
            self._identity is None or selected != self._identity.provider_id
        ):
            raise ClientError("use a separate Client/checkpoint for another provider")

    def _scope(self, provider_id: int) -> dict[str, object]:
        return {
            "chain": self.contract.domain.chain_id,
            "proxy": self.contract.domain.verifying_contract.hex(),
            "provider": provider_id,
            "wallet": self.wallet.address.hex(),
            "signer": self.signer.address.hex(),
            "signer_reference": self.signer_reference,
        }

    async def _start(self) -> None:
        async with self._start_lock:
            if not self._started:
                await self._initialize()

    async def _local_start(self) -> None:
        """Restore known wallet operations independently of provider availability."""
        async with self._start_lock:
            if self._local_started:
                return
            if not self._restored:
                scope = self._saved.get("scope")
                if scope is not None:
                    provider_id = uint256(scope["provider"], field="checkpoint provider")
                    if scope != self._scope(provider_id):
                        raise ClientError("checkpoint payment scope or fixed signer differs")
                    if isinstance(self.provider, int):
                        if self.provider != provider_id:
                            raise ClientError("checkpoint provider differs")
                    elif self._saved.get("provider_origin") != self.provider:
                        raise ClientError("checkpoint provider origin differs")
                    self._restore_local(provider_id)
                elif self._saved.get("ledger") is not None:
                    raise ClientError("checkpoint ledger has no payment scope")
                self._restored = True
            if self._ledger is not None:
                self.chain.follow(sessions=(self._ledger.session_id,))
                await self.chain.start()
            self._local_started = True

    async def _initialize(self) -> None:
        if self._started:
            return
        if isinstance(self.provider, int):
            provider_id = self.provider
        else:
            raw = await asyncio.to_thread(self._http.request, self.provider + "/discover")
            document = parse_document(raw)
            registry = f"eip155:{self.contract.domain.chain_id}:0x{self.chain.reader.profile.registry_address.hex()}"
            registrations = document.get("registrations")
            if not isinstance(registrations, list):
                raise ClientError("provider registration is missing its identity")
            matched = [
                row.get("agentId")
                for row in registrations
                if isinstance(row, dict)
                and type(row.get("agentRegistry")) is str
                and row["agentRegistry"].lower() == registry.lower()
            ]
            if len(matched) != 1 or type(matched[0]) is not int:
                raise ClientError("provider registration has ambiguous registry identity")
            provider_id = uint256(matched[0], field="provider id")
        # Watched before the first refresh, so one refresh covers it.
        self.chain.follow(providers=(provider_id,))
        await self.chain.start()
        self._identity = self.chain.snapshot().identities[provider_id]
        self._discovery = await asyncio.to_thread(
            discover,
            self._identity,
            expected_origin=self.provider if type(self.provider) is str else None,
            transport=self._http,
        )
        if "scope" in self._saved and self._saved["scope"] != self._scope(provider_id):
            raise ClientError("checkpoint payment scope or fixed signer differs")
        self._saved["scope"] = self._scope(provider_id)
        self._saved["provider_origin"] = self._discovery.origin
        if not self._restored:
            self._restore_local(provider_id)
            self._restored = True
        await self._persist()
        self._started = True

    def _restore_local(self, provider_id: int) -> None:
        stored = self._saved.get("ledger")
        if stored is None:
            return
        descriptor = descriptor_from_wire(
            pb.SessionDescriptor.FromString(bytes.fromhex(stored["descriptor"]))
        )
        if (
            descriptor.agent != self.wallet.address
            or descriptor.session_signer != self.signer.address
            or descriptor.provider_id != provider_id
        ):
            raise ClientError("checkpoint descriptor differs from selected capabilities")
        initial = parse_catalog(bytes.fromhex(stored["initial_catalog"]))
        ledger = Ledger(self.contract.domain, descriptor, initial)
        for value in stored["catalogs"]:
            ledger.add_catalog(parse_catalog(bytes.fromhex(value)))
        accepted = stored.get("accepted_catalogs", [ledger.initial_hash.hex()])
        if not isinstance(accepted, list) or len(accepted) != len(set(accepted)):
            raise ClientError("checkpoint accepted catalogs are invalid")
        ledger.accepted_catalogs = {bytes.fromhex(value) for value in accepted}
        if not ledger.accepted_catalogs <= ledger.catalogs.keys():
            raise ClientError("checkpoint price acceptance has no catalog evidence")
        ledger.generation = uint64(stored["generation"], field="generation")
        for item in stored["runs"]:
            context = RunContext(
                ledger.session_id,
                bytes.fromhex(item["run_id"]),
                item["generation"],
                item["model"],
                bytes.fromhex(item["hash"]),
            )
            signature = bytes.fromhex(item["signature"])
            verify_run_auth(ledger.domain, descriptor, context, signature)
            record = Run(
                context,
                signature,
                TokenCounts(*item["counts"]),
                TokenCounts(*item["meter_counts"]),
                uint64(item["meter_seq"], field="meter seq"),
                tuple(item["final"]) if item["final"] is not None else None,
                True,
                uint64(item["recovered_through"], field="recovery cursor")
                if item.get("recovered_through") is not None
                else None,
            )
            if context.run_id in ledger.runs:
                raise ClientError("duplicate checkpoint run")
            ledger.runs[context.run_id] = record
        for model, version, value in stored["usage"]:
            key = (model, bytes.fromhex(version))
            if key in ledger.usage or not any(
                (run.context.model, run.context.rate_card_hash) == key
                for run in ledger.runs.values()
            ):
                raise ClientError("checkpoint usage has no accepted run")
            ledger.catalogs[key[1]].rates_for(model)
            ledger.usage[key] = TokenCounts(*value)
        ledger.meter_seq = uint64(stored["meter_seq"], field="meter seq")
        for run in ledger.runs.values():
            catalog = ledger.catalogs[run.context.rate_card_hash]
            monotone(run.meter_counts, run.counts, catalog, run.context.model)
            if (
                run.meter_seq > ledger.meter_seq
                or run.context.control_generation > ledger.generation
            ):
                raise ClientError("checkpoint run cursor or generation is inconsistent")
            if run.final is not None:
                final = run.final
                if (
                    len(final) != 6
                    or any(type(value) is not int for value in final)
                    or final[:3]
                    != (run.counts.input_tokens, run.counts.cache_tokens, run.counts.output_tokens)
                    or final[3]
                    not in (
                        pb.RUN_STATUS_COMPLETED,
                        pb.RUN_STATUS_FAILED,
                        pb.RUN_STATUS_CANCELLED,
                        pb.RUN_STATUS_NOT_ADMITTED,
                    )
                    or uint64(final[4], field="final cursor") < run.meter_seq
                ):
                    raise ClientError("checkpoint final is inconsistent")
                if (
                    final[5]
                    != priced_numerator(run.counts, catalog.rates_for(run.context.model))
                    // 1_000_000
                ):
                    raise ClientError("checkpoint final cost is inconsistent")
            if run.recovered_through is not None and (
                run.final is None
                or run.recovered_through > ledger.meter_seq
                or run.recovered_through < run.meter_seq
                or run.final[4] > run.recovered_through
            ):
                raise ClientError("checkpoint recovered final cursor is inconsistent")
        partition: dict[tuple[str, bytes], TokenCounts] = {}
        for run in ledger.runs.values():
            key = (run.context.model, run.context.rate_card_hash)
            old = partition.get(key, TokenCounts(0, 0, 0))
            value = run.counts
            partition[key] = TokenCounts(
                old.input_tokens + value.input_tokens,
                old.cache_tokens + value.cache_tokens,
                old.output_tokens + value.output_tokens,
            )
        for key, value in partition.items():
            monotone(
                value, ledger.usage.get(key, TokenCounts(0, 0, 0)), ledger.catalogs[key[1]], key[0]
            )
        if type(stored["close_pending"]) is not bool:
            raise ClientError("checkpoint close intent is invalid")
        ledger.close_pending = stored["close_pending"]
        if stored["voucher"] is not None:
            voucher = pb.Voucher.FromString(bytes.fromhex(stored["voucher"]))
            ledger.accept_voucher(voucher, (1 << 64) - 1)
        if ledger.usage_numerator and (
            ledger.voucher is None or ledger.usage_numerator > ledger.voucher.cumulative * 1_000_000
        ):
            raise ClientError("checkpoint usage exceeds signed authorization")
        self._ledger = ledger
        close_record = self._saved.get("confirmed_close")
        if close_record is not None:
            close_observation = ChainObservation(
                ledger.domain.chain_id,
                ledger.domain.verifying_contract,
                close_record["block"],
                bytes.fromhex(close_record["block_hash"]),
            )
            self._closed_result = OperationResult(
                "close-" + ledger.session_id.hex(),
                OperationState.CONFIRMED,
                tx_hash=bytes.fromhex(close_record["tx"]),
                observation=close_observation,
            )
            capture = uint64(close_record["capture"], field="close capture")
            uint64(close_record["refunded"], field="close refund")
            if capture > ledger.usage_amount:
                self._closed_error = CloseAccountingError(
                    self._closed_result, capture, ledger.usage_amount
                )
        self._opening_ref = self._saved.get("opening_ref")
        for item in self._saved.get("operations", []):
            operation = PreparedOperation(
                OperationKind(item["kind"]),
                WalletCall(
                    ledger.domain.chain_id,
                    ledger.domain.verifying_contract,
                    bytes.fromhex(item["data"]),
                ),
                (descriptor,),
                tuple(item["amounts"]),
                bytes.fromhex(item["hash"]) if item["hash"] is not None else None,
            )
            raw_observation = item.get("observation")
            observation = (
                ChainObservation(
                    raw_observation[0],
                    bytes.fromhex(raw_observation[1]),
                    raw_observation[2],
                    bytes.fromhex(raw_observation[3]),
                )
                if raw_observation is not None
                else None
            )
            result = OperationResult(
                item["ref"],
                OperationState(item.get("state", "unknown")),
                item["wallet_id"],
                bytes.fromhex(item["tx"]) if item["tx"] is not None else None,
                observation,
            )
            if item["ref"] in self._operations:
                raise ClientError("duplicate checkpoint operation reference")
            self._operations[item["ref"]] = (operation, result)

    def _record(self) -> dict[str, Any]:
        result = dict(self._saved)
        # Version 2 may omit forgotten runs (_prune): mppi-agent 0.1.2 refuses
        # it rather than misreading it. This release still reads version 1.
        result["version"] = 2
        if self._ledger is not None:
            ledger = self._ledger
            result["ledger"] = {
                "descriptor": descriptor_to_wire(ledger.descriptor).SerializeToString().hex(),
                "initial_catalog": ledger.catalogs[ledger.initial_hash].canonical_bytes.hex(),
                "catalogs": [value.canonical_bytes.hex() for value in ledger.catalogs.values()],
                "accepted_catalogs": sorted(value.hex() for value in ledger.accepted_catalogs),
                "generation": ledger.generation,
                "meter_seq": ledger.meter_seq,
                "close_pending": ledger.close_pending,
                "voucher": ledger.voucher.SerializeToString().hex()
                if ledger.voucher is not None
                else None,
                "usage": [
                    [
                        model,
                        version.hex(),
                        [value.input_tokens, value.cache_tokens, value.output_tokens],
                    ]
                    for (model, version), value in ledger.usage.items()
                ],
                "runs": [
                    {
                        "run_id": run.context.run_id.hex(),
                        "generation": run.context.control_generation,
                        "model": run.context.model,
                        "hash": run.context.rate_card_hash.hex(),
                        "signature": run.signature.hex(),
                        "counts": [
                            run.counts.input_tokens,
                            run.counts.cache_tokens,
                            run.counts.output_tokens,
                        ],
                        "meter_counts": [
                            run.meter_counts.input_tokens,
                            run.meter_counts.cache_tokens,
                            run.meter_counts.output_tokens,
                        ],
                        "meter_seq": run.meter_seq,
                        "final": run.final,
                        "recovered_through": run.recovered_through,
                    }
                    for run in ledger.runs.values()
                ],
            }
        result["opening_ref"] = self._opening_ref
        result["operations"] = [
            {
                "ref": ref,
                "kind": op.kind.value,
                "data": op.call.data.hex(),
                "amounts": op.amounts,
                "hash": op.expected_rate_card_hash.hex() if op.expected_rate_card_hash else None,
                "wallet_id": state.wallet_operation_id,
                "tx": state.tx_hash.hex() if state.tx_hash else None,
                "state": state.state.value,
                "observation": [
                    state.observation.chain_id,
                    state.observation.contract_address.hex(),
                    state.observation.block_number,
                    state.observation.block_hash.hex(),
                ]
                if state.observation
                else None,
            }
            for ref, (op, state) in self._operations.items()
        ]
        return result

    def _prune(self) -> None:
        """Forget final runs that nothing can still change (memory and checkpoint).

        A run goes once it has its final; the session cursor has reached that
        final's Meter sequence, so a later Meter for it is an ignored duplicate;
        its Complete stream here has ended, so no Terminal can follow; and its
        Receipt was verified, a snapshot resolved it, the session closed, or
        more than _UNRECEIPTED_FINALS settled runs already wait for Receipts.
        Usage stays exact in the per-bucket totals. The first run of each price
        bucket stays as the signed price acceptance a checkpoint restore needs.
        A late Receipt for a forgotten run is rejected alone (no retained run).
        """
        ledger = self._ledger
        if ledger is None:
            return
        closed = self._closed_result is not None
        buckets: set[tuple[str, bytes]] = set()
        ready: list[bytes] = []
        waiting: list[bytes] = []
        for identifier, run in ledger.runs.items():
            bucket = (run.context.model, run.context.rate_card_hash)
            if bucket not in buckets:
                buckets.add(bucket)
                continue
            output = self._outputs.get(identifier)
            if (
                run.final is None
                or run.final[4] > ledger.meter_seq
                or (output is not None and output.task is not None and not output.task.done())
            ):
                continue
            if identifier in self._receipted or run.recovered_through is not None or closed:
                ready.append(identifier)
            else:
                waiting.append(identifier)
        ready.extend(waiting[: max(0, len(waiting) - _UNRECEIPTED_FINALS)])
        for identifier in ready:
            ledger.forget(identifier)
            self._outputs.pop(identifier, None)
            self._receipted.discard(identifier)
            self._receipts_seen.discard(identifier)

    async def _persist(self) -> None:
        async with self._checkpoint_lock:
            self._prune()
            record = self._record()
            await asyncio.to_thread(self._checkpoint.save, record)
            self._saved = record

    def _require_ledger(self, identifier: bytes | None = None) -> Ledger:
        ledger = self._ledger
        if ledger is None or (identifier is not None and identifier != ledger.session_id):
            raise ClientError("session is not retained in this checkpoint")
        return ledger

    def _chain_state(self) -> ChainSession:
        ledger = self._require_ledger()
        try:
            return self.chain.snapshot().sessions[ledger.session_id]
        except KeyError:
            raise ClientError("session has no fresh chain observation") from None

    @staticmethod
    def _timed_read(read: Callable[..., T], *args: Any) -> T:
        """A blocking chain read, counted as chain time."""
        with _timed("chain"):
            return read(*args)

    @staticmethod
    async def _timed_await(read: Coroutine[Any, Any, T]) -> T:
        with _timed("chain"):
            return await read

    async def _measured(self, operation: str, work: Coroutine[Any, Any, T]) -> T:
        """Run one money step, then report where its time went (on_timing)."""
        timing = _Timing()
        token = _TIMING.set(timing)
        state: str | None = None
        try:
            result = await work
            outcome = getattr(result, "operation", result)
            found = getattr(outcome, "state", None)
            state = found.value if isinstance(found, OperationState) else None
            return result
        finally:
            _TIMING.reset(token)
            ms = {part: round(seconds * 1000) for part, seconds in timing.parts.items()}
            total = round((time.monotonic() - timing.start) * 1000)
            self._observers.emit(
                "on_timing",
                OperationTiming(
                    operation,
                    state,
                    total,
                    ms["chain"],
                    ms["wallet_sign"],
                    ms["wallet_submit"],
                    max(0, total - ms["chain"] - ms["wallet_sign"] - ms["wallet_submit"]),
                ),
            )

    async def _fresh(self) -> ChainSnapshot:
        """Fresh confirmed evidence for a money step: the follower's snapshot
        while it is fresh, otherwise one refresh now."""
        try:
            return self.chain.snapshot()
        except ChainEvidenceError:
            with _timed("chain"):
                return await asyncio.wait_for(self.chain.refresh(), self.timeout)

    def _refresh_failed(self, failure: RefreshFailure) -> None:
        """A failed background refresh. Requests continue on confirmed evidence
        unless it contradicted the deployment (ChainFollower.confirmed_readiness)."""
        self._observers.emit(
            "on_error",
            HookError("chain", failure.kind, detail="background refresh: " + failure.reason),
        )

    def _observed_state(self) -> ChainSession:
        """Last confirmed deposit; deposits only grow, so it never overstates one."""
        ledger = self._require_ledger()
        current = self.chain.current
        if current is None or ledger.session_id not in current.sessions:
            raise ClientError("session has no confirmed chain observation")
        return current.sessions[ledger.session_id]

    def _observed_identity(self, provider_id: int) -> ProviderIdentity:
        current = self.chain.current
        if current is None or provider_id not in current.identities:
            raise ClientError("provider has no confirmed chain observation")
        return current.identities[provider_id]

    def _require_ready(self) -> Ledger:
        """Ready for a request from local state only: the attachment, the close
        intent and the last confirmed evidence, whatever its age. Opening,
        attachment, top-up and close read fresh evidence themselves."""
        ledger = self._require_ledger()
        readiness = self.chain.confirmed_readiness(ledger.session_id)
        if not readiness.ready:
            raise PaymentError(readiness.failures[0])
        if (
            not ledger.ready
            or ledger.close_pending
            or self._control_failure is not None
            or self._resync is not None
            or self._attached_generation == 0
            or self._attached_generation != ledger.generation
        ):
            raise ClientError("authenticated Control is not ready")
        if self.signer.address != ledger.descriptor.session_signer:
            raise ClientError("fixed session signer is unavailable or changed")
        return ledger

    async def _connect(self) -> None:
        if self._control is not None and not self._control.done() and self._control_failure is None:
            return
        # Drain every old attachment task before creating another owner. A
        # cancelled to_thread read may finish externally, but its coroutine
        # cannot later apply reports or fail the replacement Control stream.
        # Complete streams are not attachment tasks: each ends on its own terms
        # on the shared channel and applies its final to the current ledger,
        # whose snapshot already holds every pending run's recovered outcome.
        old_tasks = self._control_tasks + list(self._background)
        for task in old_tasks:
            task.cancel()
        await asyncio.gather(*old_tasks, return_exceptions=True)
        self._pending_vouchers.clear()
        self._renew_task = self._voucher_task = self._receipt_task = None
        self._receipt_queue.clear()
        self._receipts_seen.clear()
        self._receipt_failures = 0
        self._funding = None
        self._low_credit_task = None
        if self._ledger is not None:
            self._ledger.ready = False
        self._attached_generation = 0
        if self._channel is None:
            assert self._discovery is not None
            roots = await asyncio.to_thread(Path(self.ca_file).read_bytes) if self.ca_file else None
            self._channel = grpc.aio.secure_channel(
                urlsplit(self._discovery.origin).netloc,
                grpc.ssl_channel_credentials(root_certificates=roots),
                options=GRPC_OPTIONS + (("grpc.enable_retries", 0), ("grpc.enable_http_proxy", 0)),
            )
        self._control = services.ControlStub(self._channel).Session()
        self._control_failure = self._resync = None
        self._outbound = ByteQueue(**self._queue_options)
        self._nonce = self._loop.create_future()
        self._snapshot_waiter = None
        self._snapshot_applied = None
        self._control_tasks = [
            asyncio.create_task(self._control_read()),
            asyncio.create_task(self._control_write()),
        ]

    async def _send(self, message: pb.ControlMessage) -> None:
        validate_message(message)
        if self._outbound is None or self._control_failure is not None:
            raise ClientError("Control is unavailable")
        await self._outbound.put(message.SerializeToString())

    async def _control_write(self) -> None:
        assert self._outbound is not None
        try:
            while True:
                data = await self._outbound.get()
                await asyncio.wait_for(
                    self._control.write(pb.ControlMessage.FromString(data)), self.timeout
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._control_lost(error)

    def _control_lost(self, error: Exception, kind: str | None = None) -> None:
        """End this attachment only; every Complete stream continues.

        The provider stops work it can no longer fund. A run that already
        completed still delivers its answer and final, on its own stream.
        """
        if self._control_failure is not None:
            return
        self._observers.emit(
            "on_error", HookError("control", type(error).__name__, detail=_detail(kind, error))
        )
        self._control_failure = error
        self._attached_generation = 0
        if self._low_credit_task is not None:
            self._low_credit_task.cancel()
        self._funding = None
        if self._ledger is not None:
            self._ledger.ready = False
        if self._outbound is not None:
            self._outbound.close(error)
        for future in (
            self._nonce,
            self._snapshot_waiter,
            self._close_waiter,
            *(item[1] for item in self._challenges.values()),
        ):
            if future is not None and not future.done():
                future.set_exception(error)
        if self._control is not None:
            self._control.cancel()

    async def _control_read(self) -> None:
        kind: str | None = None
        try:
            async for message in self._control:
                kind = message.WhichOneof("kind")
                if kind in _PAYMENT_REPORTS:
                    ledger = self._require_ledger()
                    if (
                        self._attached_generation == 0
                        or self._attached_generation != ledger.generation
                        or self._control_failure is not None
                    ):
                        raise ClientError("payment report arrived before authenticated cutover")
                if kind in _SINGLE_REPORTS:
                    try:
                        await self._control_report(kind, message)
                    except _REPORT_FAILURES as error:
                        self._reject(kind, error)
                    kind = None
                    continue
                if kind == "meter":
                    await self._meter(message)
                    kind = None
                    continue
                validate_message(message)
                if kind == "reconnect_challenge":
                    if self._nonce is None or self._nonce.done():
                        raise ClientError("duplicate reconnect challenge")
                    self._nonce.set_result(message.reconnect_challenge.nonce)
                elif kind == "session_state":
                    if self._snapshot_waiter is None or self._snapshot_waiter.done():
                        raise ClientError("unsolicited session snapshot")
                    self._snapshot_waiter.set_result(message.session_state)
                    if self._snapshot_applied is None:
                        raise ClientError("snapshot has no pending validation cutover")
                    await self._snapshot_applied.wait()
                    if self._control_failure is not None:
                        return
                elif kind == "close_result":
                    self._spawn(self._close_result(message.close_result))
                else:
                    raise ClientError("provider sent a wrong-direction Control message")
                kind = None
            raise ClientError("Control ended")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._control_lost(self._rpc_error(error), kind)

    async def _meter(self, message: pb.ControlMessage) -> None:
        """Apply one Meter in session order, or stop the cursor at a rejected one.

        Every later Meter would gap, so none applies until a re-sync. Closing
        Control now would make the provider cancel every active run (the
        provider SDK's detach_locked), so active runs continue to their own
        finals and the next request re-syncs once no stream is live, or after
        _RESYNC_WAIT_S at most; the reattach snapshot then supplies the
        accounting through its cursor.
        Until then no new run starts and Meter-driven renewal stops, so a run
        needing more output coverage ends within its funded bound.
        """
        if self._resync is not None:
            return
        try:
            validate_message(message)
            ledger = self._require_ledger(message.meter.session_id)
            changed = ledger.apply_meter(message.meter, self._observed_state().deposit)
        except _REPORT_FAILURES as error:
            self._reject("meter", error)
            self._resync, self._resync_at = error, asyncio.get_running_loop().time()
            return
        if changed:
            await self._persist()
            self._observers.emit("on_meter", message.meter)
            if self._renew_task is None or self._renew_task.done():
                self._renew_task = self._spawn(self._renew())

    def _resync_if_idle(self) -> bool:
        """End a stuck attachment once no Complete stream is live; see _meter.

        After _RESYNC_WAIT_S a stream still live is ended too, so a provider
        cannot keep the session unusable by holding a silent stream open.
        """
        if self._resync is None:
            return False
        live = [
            output
            for output in self._outputs.values()
            if output.task is not None and not output.task.done()
        ]
        if live and asyncio.get_running_loop().time() - self._resync_at < _RESYNC_WAIT_S:
            return False
        for output in live:
            if output.call is not None:
                output.call.cancel()
            assert output.task is not None
            output.task.cancel()
        self._control_lost(self._resync, "meter")
        return True

    async def _control_report(self, kind: str, message: pb.ControlMessage) -> None:
        """Apply one report that may be rejected alone; see ``_SINGLE_REPORTS``."""
        validate_message(message)
        if kind == "challenge":
            entry = self._challenges.get(message.challenge.request_id)
            if entry is None:
                raise ClientError("unsolicited payment challenge")
            assert self._identity is not None
            verify_challenge(
                message.challenge,
                self._identity,
                request_id=message.challenge.request_id,
                model=entry[0],
            )
            if not entry[1].done():
                entry[1].set_result(message.challenge)
            elif entry[1].result() != message.challenge:
                raise ClientError("conflicting repeated challenge")
            self._observers.emit("on_challenge", message.challenge)
        elif kind == "low_credit":
            ledger = self._require_ledger(message.low_credit.session_id)
            deposit = self._observed_state().deposit
            coverage = ledger.coverage(message.low_credit, deposit)  # Records nothing.
            if coverage.superseded:
                # Routine under concurrency; persistent ones expose a stale peer.
                self._observers.emit(
                    "on_error",
                    HookError(
                        "control",
                        "SupersededCoverage",
                        ledger.session_id,
                        message.low_credit.coverage_request.run_id,
                        "low_credit: coverage request is priced below usage already applied",
                    ),
                )
            # LowCredit both requests a voucher and signals low funding. The SDK
            # signs every coverage the confirmed deposit covers; only what it
            # cannot cover, or a plain low-credit notice, reaches funding policy.
            # hooks.on_low_credit still observes every notice.
            if not message.low_credit.HasField("coverage_request") or (
                coverage.required is not None and coverage.required > deposit
            ):
                self._notify_low_credit(message.low_credit, deposit, coverage.required)
            self._observers.emit("on_low_credit", message.low_credit)
            target = coverage.target
            if target is not None:
                request = message.low_credit.coverage_request
                if (
                    request.run_id not in self._pending_vouchers
                    and len(self._pending_vouchers) >= self._queue_options["max_messages"]
                ):
                    # The ledger recorded nothing, and any funding notice went out.
                    raise ClientError("pending voucher queue is full")
                ledger.cover(coverage)
                self._pending_vouchers[request.run_id] = (target, request.control_generation)
                if self._voucher_task is None or self._voucher_task.done():
                    self._voucher_task = self._spawn(self._voucher_worker())
        elif kind == "receipt":
            # Cheap checks first, with no chain read: a Receipt for an unknown
            # run, other prices or an already proven run costs the agent nothing.
            receipt = message.receipt
            ledger = self._require_ledger(receipt.session_id)
            run = ledger._run(receipt.run_id)
            if (receipt.model, receipt.rate_card_hash) != (
                run.context.model,
                run.context.rate_card_hash,
            ):
                raise ClientError("receipt differs from accepted run prices")
            if receipt.run_id in self._receipts_seen:
                return
            if self._receipt_failures >= _RECEIPT_FAILURES:
                raise ClientError("receipt proofs are suspended on this attachment")
            if len(self._receipt_queue) >= _RECEIPT_QUEUE:
                raise ClientError("receipt proof queue is full")
            if self._receipt_task is None or self._receipt_task.done():
                # The worker first runs after this step, so it sees the new entry.
                self._receipt_task = self._spawn(self._receipt_worker())
            self._receipts_seen.add(receipt.run_id)
            self._receipt_queue.append((pb.Receipt.FromString(receipt.SerializeToString()), 0))
        else:
            ledger = self._require_ledger(message.drain.session_id)
            targets = (
                [message.drain.run_id] if message.drain.HasField("run_id") else list(ledger.runs)
            )
            if not message.drain.HasField("run_id"):
                ledger.ready = False
            for identifier in targets:
                if identifier in ledger.runs:
                    ledger.runs[identifier].failed_transport = True
            await self._persist()
            self._observers.emit("on_drain", message.drain)

    def _reject(self, kind: str, error: Exception) -> None:
        # Nothing the rejected report carried was applied; the attachment and
        # every run continue. The record names the kind and the SDK's reason.
        self._observers.emit(
            "on_error", HookError("control", type(error).__name__, detail=_detail(kind, error))
        )

    def _spawn(self, coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        if len(self._background) >= self._queue_options["max_messages"]:
            coroutine.close()
            raise ClientError("bounded payment proof queue is full")
        task = asyncio.create_task(coroutine)
        self._background.add(task)

        def done(completed: asyncio.Task[None]) -> None:
            self._background.discard(completed)
            if not completed.cancelled() and completed.exception() is not None:
                self._control_lost(cast(Exception, completed.exception()))

        task.add_done_callback(done)
        return task

    async def _voucher_worker(self) -> None:
        while self._pending_vouchers:
            identifier = next(iter(self._pending_vouchers))
            target, generation = self._pending_vouchers.pop(identifier)
            await self._voucher(target, generation=generation, run_id=identifier)

    def _notify_low_credit(self, notice: pb.LowCredit, deposit: int, required: int | None) -> None:
        if self._on_low_credit is None and self._on_funding_notice is None:
            return
        # Never wait for user code on Control. A newer notice replaces one not
        # yet delivered, and delivery works out the deposit and shortfall again,
        # so a notice a confirmed top-up has since covered cannot over-fund. A
        # plain notice never replaces one carrying a requirement: that shortfall
        # still needs the caller's funding decision.
        pending = self._funding
        if required is not None or pending is None or pending[2] is None:
            self._funding = (notice.SerializeToString(), deposit, required)
        if self._low_credit_task is None or self._low_credit_task.done():
            self._low_credit_task = self._spawn(self._low_credit_worker())

    async def _low_credit_worker(self) -> None:
        while self._funding is not None:
            data, deposit, required = self._funding
            self._funding = None
            try:
                deposit = max(deposit, self._observed_state().deposit)
            except ClientError:
                pass
            shortfall = None if required is None else required - deposit
            if shortfall is not None and shortfall <= 0:
                continue  # Confirmed funding already covers it.
            # Each callback receives a detached record and may enqueue caller-owned
            # funding work. Its return value never authorizes an SDK top-up.
            if self._on_low_credit is not None:
                await asyncio.to_thread(self._on_low_credit, pb.LowCredit.FromString(data))
            if self._on_funding_notice is not None:
                await asyncio.to_thread(
                    self._on_funding_notice,
                    FundingNotice(pb.LowCredit.FromString(data), deposit, required, shortfall),
                )

    def _rpc_error(self, error: Exception) -> Exception:
        if isinstance(error, grpc.aio.AioRpcError):
            failure = parse_payment_error(error.code(), error.trailing_metadata())
            if failure is not None:
                return PaymentError(failure)
        return error

    async def _challenge(self, model: str) -> pb.Challenge:
        request_id = secrets.token_bytes(32)
        future: asyncio.Future[pb.Challenge] = self._loop.create_future()
        self._challenges[request_id] = (model, future)
        await self._send(
            pb.ControlMessage(
                challenge_request=pb.ChallengeRequest(request_id=request_id, model=model)
            )
        )
        return await asyncio.wait_for(asyncio.shield(future), self.timeout)

    async def _open(self, model: str, choice: int | None) -> OpenResult:
        validate_model(model)
        if choice is not None:
            uint64(choice, field="initial_session_tokens", positive=True)
        async with self._open_lock:
            if self._ledger is None and self._saved.get("ledger") is None:
                # Nothing retained, so this opening funds a new session: refuse
                # before any discovery, chain read or provider exchange.
                self._require_agent_id()
            await self._start()
            if self._resync is not None and not self._resync_if_idle():
                raise ClientError("Control re-sync is pending until active runs end (at most 60 s)")
            if (
                self._ledger is not None
                and self._ledger.ready
                and not self._ledger.close_pending
                and self._control_failure is None
            ):
                self._require_ready()
                if self._opening_ref is None:
                    raise ClientError("confirmed opening reference is missing")
                return OpenResult(
                    self._operations[self._opening_ref][1], await self._get(self._ledger.session_id)
                )
            if self._ledger is not None and self._ledger.close_pending:
                raise ClientError("session close is pending; new inference is ineligible")
            if self._ledger is not None and self._snapshot_waiter is not None:
                # An attachment nonce is single-use, including failed attempts.
                self._control_lost(ClientError("opening a fresh Control attachment"))
            await self._connect()
            retained = self._ledger is not None
            if self._ledger is None:
                self._require_agent_id()
                selected = self.initial_session_tokens if choice is None else choice
                if selected is None:
                    raise ClientError("initial_session_tokens is required before funding")
                challenge = await self._challenge(model)
                await self._fresh()
                readiness = self.chain.readiness()
                if not readiness.ready:
                    raise PaymentError(readiness.failures[0])
                assert self._identity is not None
                identity = self.chain.snapshot().identities[self._identity.provider_id]
                if identity.catalog is None:
                    raise ClientError("provider has no valid current catalog")
                verify_challenge(challenge, identity, request_id=challenge.request_id, model=model)
                amount = funding_amount(selected, identity.catalog.rates_for(model))
                descriptor = SessionDescriptor(
                    self.wallet.address,
                    self.signer.address,
                    identity.provider_id,
                    identity.recipient,
                    identity.owner,
                    bytes.fromhex("3600000000000000000000000000000000000000"),
                    secrets.token_bytes(32),
                )
                ledger = Ledger(self.contract.domain, descriptor, identity.catalog)
                self._ledger = ledger
                self._opening_ref = "open-" + secrets.token_hex(32)
                auth = self._funding_authorization(amount)
                self._saved["funding_intent"] = {
                    "model": model,
                    "tokens": selected,
                    "amount": amount,
                    "nonce": auth.nonce.hex(),
                    "valid_after": auth.valid_after,
                    "valid_before": auth.valid_before,
                }
                await self._persist()
                outcome = await self._submit_open_intent(fresh=True)
            else:
                if self._opening_ref is None:
                    raise ClientError("retained opening reference is missing")
                previous = self._operations.get(self._opening_ref)
                if previous is None:
                    outcome = await self._submit_open_intent()
                elif previous[1].state is not OperationState.FAILED:
                    outcome = await self._reconcile_ref(self._opening_ref)
                elif not self._intent_expired():
                    # It definitely failed, so nothing took effect: send the same
                    # intent again under a new reference. Its salt and nonce cannot
                    # fund twice, and the existence read refuses an earlier success.
                    self._opening_ref = "open-" + secrets.token_hex(32)
                    outcome = await self._submit_open_intent()
                else:
                    outcome = previous[1]
                if outcome.state is OperationState.FAILED and await self._opening_void():
                    # Its authorization has expired and its session holds nothing at
                    # a canonical block, so it can never fund one: forget it, and the
                    # next open() starts a fresh intent. Unproven, the intent stays.
                    self._retire_opening()
                    await self._persist()
            if outcome.state is not OperationState.CONFIRMED:
                return OpenResult(outcome, None)
            assert outcome.tx_hash is not None
            ledger = self._require_ledger()
            if retained:
                # A retained result may come from an earlier process: prove it again.
                evidence = await asyncio.wait_for(
                    asyncio.to_thread(
                        self.chain.reader.opening,
                        outcome.tx_hash,
                        ledger.session_id,
                        accepted_catalog=ledger.catalogs[ledger.initial_hash],
                    ),
                    self.timeout,
                )
                if (
                    evidence.descriptor != ledger.descriptor
                    or evidence.initial_catalog != ledger.catalogs[ledger.initial_hash]
                ):
                    raise ClientError("canonical opening differs from retained intent")
            # A fresh CONFIRMED result is this call's canonical reconcile, which
            # already matched every descriptor field, the deposit and the initial
            # catalog hash against the SessionOpened event and session state.
            self._local_started = True
            if ledger.ready:
                await self.chain.watch_session(ledger.session_id)
            else:
                # The attachment reads the session with the provider's snapshot.
                self.chain.follow(sessions=(ledger.session_id,))
                self._confirm_opened(ledger, outcome)
                await self._connect()
                await self._attach(outcome.tx_hash)
            self._observers.emit(
                "on_session_open",
                pb.SessionOpen(session_id=ledger.session_id, open_tx=outcome.tx_hash),
            )
            return OpenResult(outcome, await self._get(ledger.session_id))

    async def _submit_open_intent(self, *, fresh: bool = False) -> OperationResult:
        """Explicitly continue an opening that never reached wallet execution.

        Once a PreparedOperation is journaled, even an unknown result takes the
        reconcile-only branch. Reuse the original nonce, deadline and reference;
        expired or changed terms do not authorize replacement funding. A
        ``fresh`` intent, created by this call with a new random salt, cannot
        name an existing session, so its existence is not read; the contract
        would refuse one anyway (``SessionExists``, before any token call).
        """
        self._require_agent_id()
        ledger = self._require_ledger()
        ref = self._opening_ref
        if ref is None or ref in self._operations:
            raise ClientError("opening already has a submitted operation or lacks its reference")
        intent = self._saved.get("funding_intent")
        if not isinstance(intent, dict):
            raise ClientError("retained opening funding intent is missing")
        try:
            initial = ledger.catalogs[ledger.initial_hash]
            amount = funding_amount(intent["tokens"], initial.rates_for(intent["model"]))
            if amount != uint64(intent["amount"], field="retained deposit", positive=True):
                raise ClientError("retained opening funding amount contradicts its token choice")
            auth = FundingAuthorization(
                ledger.domain.chain_id,
                ledger.descriptor.token,
                self.contract.token_domain_name,
                self.contract.token_domain_version,
                ledger.descriptor.agent,
                ledger.domain.verifying_contract,
                amount,
                intent["valid_after"],
                intent["valid_before"],
                bytes.fromhex(intent["nonce"]),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise ClientError("retained opening funding intent is invalid") from error

        async def eligible() -> None:
            current = await self._fresh()
            readiness = self.chain.readiness()
            if not readiness.ready:
                raise PaymentError(readiness.failures[0])
            identity = current.identities[ledger.descriptor.provider_id]
            if (
                ledger.close_pending
                or self.signer.address != ledger.descriptor.session_signer
                or self.wallet.address != ledger.descriptor.agent
                or (identity.owner, identity.recipient, identity.catalog)
                != (ledger.descriptor.operator, ledger.descriptor.recipient, initial)
                or (self._identity is not None and identity.agent_uri != self._identity.agent_uri)
                or not auth.valid_after < current.block.timestamp < auth.valid_before
            ):
                raise ClientError("retained opening is expired or its original terms changed")

        await eligible()
        if not fresh:
            current = self.chain.snapshot()
            existing = await asyncio.wait_for(
                asyncio.to_thread(self.chain.reader.session, ledger.session_id, current.block),
                self.timeout,
            )
            if existing.exists:
                raise ClientError("session already exists; recover its original opening evidence")
        proof_nonce = secrets.token_bytes(32)
        proof = await asyncio.wait_for(
            asyncio.to_thread(
                self.signer.sign_reconnect, ledger.domain, ledger.session_id, proof_nonce
            ),
            self.timeout,
        )
        verify_reconnect_auth(ledger.domain, ledger.descriptor, proof_nonce, proof)
        with _timed("wallet_sign"):
            signature = await asyncio.wait_for(
                asyncio.to_thread(self.wallet.sign_funding_authorization, auth), self.timeout
            )
        if type(signature) is not bytes or not 0 < len(signature) <= 65536:
            raise FundingAuthorizationError("INVALID_SIGNATURE")
        if getattr(self.wallet, "signs_at_execute", False) is True:
            # Its execute replaces the placeholder with the real signature; USDC
            # verifies that on chain. The agent id must still bind this wallet.
            await self._check_binding()
        else:
            await self._check_funding(auth, signature)
        await eligible()
        operation = self.contract.open(
            ledger.descriptor, amount, ledger.initial_hash, auth, signature
        )
        return await self._submit(operation, ref)

    def _confirm_opened(self, ledger: Ledger, outcome: OperationResult) -> None:
        """Requests can use the opening its SessionOpened event proved.

        Its activity time is that of the evidence read before submission,
        which is no later than the opening's block, so the deadline it implies
        is never late.
        """
        current = self.chain.current
        proven = [o.deposit for o in outcome.sessions if o.session_id == ledger.session_id]
        if current is None or not proven or proven[0] is None:
            return
        self.chain.confirm(
            ledger.session_id,
            ChainSession(
                proven[0],
                0,
                SessionStatus.OPEN,
                0,
                current.block.timestamp,
                ledger.initial_hash,
            ),
        )

    def _require_agent_id(self) -> None:
        """Funding needs the agent id its provider verifies; exits never do."""
        if self.agent_id is None:
            raise FundingAuthorizationError("AGENT_ID_REQUIRED")

    async def _check_funding(self, auth: FundingAuthorization, signature: bytes) -> None:
        """Refuse an opening the provider or USDC would refuse, before submission.

        One read at the current snapshot block, as the provider checks the id
        at SessionOpen and as USDC checks the signature (ChainReader.funding).
        """
        assert self.agent_id is not None
        with _timed("chain"):
            evidence = await asyncio.wait_for(
                asyncio.to_thread(
                    self.chain.reader.funding,
                    auth,
                    signature,
                    self.agent_id,
                    self.chain.snapshot().block,
                ),
                self.timeout,
            )
        if evidence.agent_wallet != self.wallet.address:
            raise FundingAuthorizationError("AGENT_WALLET_MISMATCH")
        if not evidence.signature_valid:
            raise FundingAuthorizationError(
                "SIGNATURE_DEFERRED_UNDECLARED"
                if signature == bytes(len(signature))
                else "INVALID_SIGNATURE"
            )

    async def _check_binding(self) -> None:
        """The registry binds this agent's id to the funding wallet; one read."""
        assert self.agent_id is not None
        with _timed("chain"):
            bound = await asyncio.wait_for(
                asyncio.to_thread(
                    self.chain.reader.agent_wallet,
                    self.agent_id,
                    self.chain.snapshot().block.observation,
                ),
                self.timeout,
            )
        if bound != self.wallet.address:
            raise FundingAuthorizationError("AGENT_WALLET_MISMATCH")

    def _intent_expired(self) -> bool:
        """The retained authorization's valid_before is reached at the fresh head."""
        intent = self._saved.get("funding_intent")
        try:
            return isinstance(intent, dict) and (
                self.chain.snapshot().block.timestamp >= intent["valid_before"]
            )
        except ChainEvidenceError:
            return False  # Unknown: keep the intent.

    def _retire_opening(self) -> None:
        """Forget an opening that definitely failed and can never fund a session."""
        self._ledger = None
        self._opening_ref = None
        self._saved.pop("ledger", None)
        self._saved.pop("funding_intent", None)

    async def _opening_void(self) -> bool:
        """Whether the retained opening can never fund its session.

        At a canonical block whose timestamp has reached the authorization's
        valid_before, the session holds no deposit. Opening funds and creates
        the session in one call, sessions are never deleted, and USDC refuses
        an authorization once block.timestamp >= validBefore, so it never
        happened and never can.
        """
        ledger = self._ledger
        if ledger is None or not self._intent_expired():
            return False
        block = self.chain.snapshot().block
        try:
            _, state = await asyncio.wait_for(
                asyncio.to_thread(
                    self.chain.reader.session_at,
                    block.observation.block_number,
                    block.observation.block_hash,
                    ledger.session_id,
                ),
                self.timeout,
            )
        except (ChainEvidenceError, TransportError, TimeoutError):
            return False  # Not proven: it stays uncertain.
        return not state.exists

    def _funding_authorization(self, amount: int) -> FundingAuthorization:
        ledger = self._require_ledger()
        timestamp = self.chain.snapshot().block.timestamp
        return FundingAuthorization(
            ledger.domain.chain_id,
            ledger.descriptor.token,
            self.contract.token_domain_name,
            self.contract.token_domain_version,
            ledger.descriptor.agent,
            ledger.domain.verifying_contract,
            amount,
            max(0, timestamp - 1),
            timestamp + 3600,
            secrets.token_bytes(32),
        )

    async def _submit(self, operation: PreparedOperation, ref: str) -> OperationResult:
        if ref in self._operations:
            raise ClientError("operation reference already submitted; reconcile it")
        self._operations[ref] = (operation, OperationResult(ref, OperationState.UNKNOWN))
        await self._persist()
        timing = _TIMING.get()
        wallet = _TimedWallet(self.wallet, timing)
        started = time.monotonic()
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    submit_operation,
                    wallet,
                    operation,
                    operation_ref=ref,
                    chain=self.chain.reader,
                ),
                self.timeout,
            )
        except TimeoutError:
            # The wallet may have broadcast. Retain this exact reference; only
            # an explicit reconcile may resolve it. Never submit it again.
            return self._operations[ref][1]
        finally:
            if timing is not None:
                # The receipt proof after the wallet returned is chain time.
                timing.parts["wallet_submit"] += wallet.spent
                timing.parts["chain"] += time.monotonic() - started - wallet.spent
        self._operations[ref] = (operation, result)
        await self._persist()
        if result.cause is not None:
            self._observers.emit(
                "on_error",
                HookError(
                    operation.kind.value,
                    result.cause.split(":", 1)[0],
                    operation.session_ids[0] if operation.session_ids else None,
                    detail=f"wallet {result.state.value}: {result.cause}",
                ),
            )
        return result

    async def _reconcile_ref(self, ref: str) -> OperationResult:
        await self._local_start()
        if self._ledger is not None and ref == "close-" + self._ledger.session_id.hex():
            # A cooperative close has no wallet operation: its reference is
            # the reported close, completed by proving it on chain.
            if self._closed_error is not None:
                raise self._closed_error
            if self._closed_result is not None:
                return self._closed_result
            if "reported_close" in self._saved:
                return await self._prove_close()
        if ref not in self._operations:
            raise ClientError("unknown retained operation reference")
        async with self._operation_locks.setdefault(ref, asyncio.Lock()):
            operation, previous = self._operations[ref]
            try:
                if previous.tx_hash is not None:
                    result = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.chain.reader.reconcile,
                            operation,
                            operation_ref=ref,
                            tx_hash=previous.tx_hash,
                            wallet_operation_id=previous.wallet_operation_id,
                        ),
                        self.timeout,
                    )
                else:
                    result = await asyncio.wait_for(
                        asyncio.to_thread(
                            reconcile_operation,
                            self.wallet,
                            operation,
                            operation_ref=ref,
                            chain=self.chain.reader,
                        ),
                        self.timeout,
                    )
            except Exception:
                result = OperationResult(ref, OperationState.UNKNOWN)
            if (
                operation.kind is OperationKind.OPEN
                and ref == self._opening_ref
                and previous.state not in (OperationState.CONFIRMED, OperationState.FAILED)
                and result.state in (OperationState.UNKNOWN, OperationState.PENDING)
                and await self._opening_void()
            ):
                result = OperationResult(
                    ref,
                    OperationState.FAILED,
                    result.wallet_operation_id,
                    result.tx_hash,
                    cause="authorization expired before any opening",
                )
            if (
                result.operation_ref != ref
                or (
                    previous.tx_hash is not None
                    and result.tx_hash is not None
                    and result.tx_hash != previous.tx_hash
                )
                or (
                    previous.wallet_operation_id is not None
                    and result.wallet_operation_id is not None
                    and result.wallet_operation_id != previous.wallet_operation_id
                )
            ):
                raise ClientError("reconciliation changed the retained operation identity")
            if previous.state in (OperationState.CONFIRMED, OperationState.FAILED):
                if result.state in (OperationState.UNKNOWN, OperationState.PENDING):
                    return previous
                if result.state is not previous.state:
                    raise ClientError("reconciliation contradicts a terminal operation result")
            result = OperationResult(
                ref,
                result.state,
                result.wallet_operation_id or previous.wallet_operation_id,
                result.tx_hash or previous.tx_hash,
                result.observation,
                result.sessions,
                result.cause,
            )
            self._operations[ref] = (operation, result)
            await self._persist()
            if operation.kind is OperationKind.TOP_UP and result.state is OperationState.CONFIRMED:
                await self._top_up_confirmed(result)
            return result

    async def _attach(self, opening_tx: bytes, *, require_ready: bool = True) -> None:
        ledger = self._require_ledger()
        notice = pb.SessionOpen(session_id=ledger.session_id, open_tx=opening_tx)
        if self.agent_id is not None:
            notice.agent_id = self.agent_id.to_bytes(32, "big")
        await self._send(pb.ControlMessage(session_open=notice))
        assert self._nonce is not None
        nonce = await asyncio.wait_for(asyncio.shield(self._nonce), self.timeout)
        signature = await asyncio.wait_for(
            asyncio.to_thread(self.signer.sign_reconnect, ledger.domain, ledger.session_id, nonce),
            self.timeout,
        )
        verify_reconnect_auth(ledger.domain, ledger.descriptor, nonce, signature)
        # A run registered but not yet sent cannot be pending at the provider,
        # and its Complete would now carry a stale generation. Record it
        # NOT_ADMITTED first, or a provider may refuse this reattach.
        unsent = [identifier for identifier in self._unsent if identifier in ledger.pending]
        self._unsent.clear()
        if unsent:
            for identifier in unsent:
                ledger.record_not_admitted(identifier)
            await self._persist()
        request = pb.ReconnectRequest(
            session_id=ledger.session_id,
            nonce=nonce,
            signature=signature,
            last_meter_seq=ledger.meter_seq,
            pending_run_ids=ledger.pending,
        )
        if ledger.voucher is not None:
            request.latest_voucher.CopyFrom(ledger.voucher)
        self._snapshot_waiter = self._loop.create_future()
        self._snapshot_applied = asyncio.Event()
        await self._send(pb.ControlMessage(reconnect_request=request))
        try:
            snapshot = await asyncio.wait_for(asyncio.shield(self._snapshot_waiter), self.timeout)
            current, observed = await self._snapshot_evidence(
                snapshot.chain_block_number, snapshot.chain_block_hash, ledger.session_id
            )
            if (snapshot.deposit, snapshot.settled_cumulative, snapshot.status) != (
                observed.deposit,
                observed.settled_cumulative,
                {
                    SessionStatus.OPEN: pb.SESSION_STATUS_OPEN,
                    SessionStatus.CLOSING: pb.SESSION_STATUS_CLOSING,
                    SessionStatus.CLOSED: pb.SESSION_STATUS_CLOSED,
                }[observed.status],
            ):
                raise ClientError("snapshot chain fields differ from canonical state")
            latest = current.sessions[ledger.session_id]
            identity = current.identities[ledger.descriptor.provider_id]
            candidate = deepcopy(ledger)
            if identity.catalog is not None:
                # Publication evidence is not the caller's new-price acceptance.
                candidate.add_catalog(identity.catalog)
            candidate.restore_snapshot(
                snapshot, nonce=nonce, deposit=latest.deposit, settled=latest.settled_cumulative
            )
            if snapshot.HasField("current_rate_card_hash") and (
                identity.catalog is None
                or snapshot.current_rate_card_hash != identity.catalog.rate_card_hash
            ):
                raise ClientError("snapshot current catalog differs from confirmed offering")
            if not self.chain.readiness(ledger.session_id).ready:
                candidate.ready = False
            if self._control_failure is not None:
                raise ClientError("Control was lost during snapshot verification")
            self._ledger = candidate
            try:
                await self._persist()
            except BaseException:
                self._ledger = ledger
                raise
            self._attached_generation = candidate.generation
        except BaseException as error:
            self._control_lost(
                error if isinstance(error, Exception) else ClientError("attachment cancelled")
            )
            raise
        finally:
            self._snapshot_applied.set()
        await self._announce_top_up()
        # A top-up confirmed while attaching, or usage restored by the snapshot,
        # may already need a higher voucher: renew once on the attached stream.
        if self._renew_task is None or self._renew_task.done():
            self._renew_task = self._spawn(self._renew())
        if require_ready:
            self._require_ready()

    async def _snapshot_evidence(
        self, number: int, block_hash: bytes, session_id: bytes
    ) -> tuple[ChainSnapshot, ChainSession]:
        """Fresh confirmed evidence at or after a provider snapshot's block, and
        the session's state at that block, which must be canonical.

        The provider may have observed a settlement or top-up after our follower
        did, so evidence older than its block is never used. Usually one round
        trip: everything is read at its block, or only the session when the
        follower is already further on. A load-balanced RPC may briefly not
        serve that block yet: wait for it.
        """
        try:
            current: ChainSnapshot | None = self.chain.snapshot()
        except ChainEvidenceError:
            current = None
        head = current.block.observation.block_number if current is not None else -1
        if current is not None and head > number and session_id not in current.sessions:
            current = await asyncio.wait_for(self.chain.refresh(), self.timeout)
            head = current.block.observation.block_number
        if current is not None and head >= number and session_id in current.sessions:
            if head == number:
                if current.block.observation.block_hash != block_hash:
                    raise ClientError("snapshot block is not canonical")
                return current, current.sessions[session_id]
            try:
                _, observed = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._timed_read,
                        self.chain.reader.session_at,
                        number,
                        block_hash,
                        session_id,
                    ),
                    self.timeout,
                )
            except ChainLagError:
                raise
            except ChainEvidenceError as error:
                raise ClientError("snapshot block is not canonical") from error
            return current, observed
        for delay in (0.25, 1.0, None):
            try:
                current = await asyncio.wait_for(
                    self._timed_await(self.chain.refresh_at(number, block_hash)), self.timeout
                )
                return current, current.sessions[session_id]
            except ChainLagError:
                if delay is None:
                    raise ClientError("confirmed chain observation precedes snapshot") from None
                await asyncio.sleep(delay)
            except ChainEvidenceError as error:
                raise ClientError("snapshot block is not canonical") from error
        raise AssertionError("unreachable")

    async def _recover(self, identifier: bytes) -> SessionView:
        await self._local_start()
        self._require_ledger(identifier)
        async with self._open_lock:
            # A caller-chosen recovery replaces a live owner as well as a lost
            # one. It never chooses a model, raises funding or replays Complete.
            if self._control is not None:
                self._control_lost(ClientError("caller requested authenticated recovery"))
            await self._ensure_close_attachment()
            return await self._get(identifier)

    async def _ensure_close_attachment(self) -> None:
        ledger = self._require_ledger()
        if (
            self._control is not None
            and not self._control.done()
            and self._control_failure is None
            and self._attached_generation == ledger.generation
            and ledger.generation > 0
        ):
            return
        # This never opens or funds a session. A provider outage must fail
        # before a new cooperative-close intent changes local eligibility.
        await self._start()
        if self._opening_ref is None or self._opening_ref not in self._operations:
            raise ClientError("retained opening proof is required for close attachment")
        outcome = await self._reconcile_ref(self._opening_ref)
        if outcome.state is not OperationState.CONFIRMED or outcome.tx_hash is None:
            raise ClientError("opening remains unresolved before close attachment")
        proof = await asyncio.wait_for(
            asyncio.to_thread(
                self.chain.reader.opening,
                outcome.tx_hash,
                ledger.session_id,
                accepted_catalog=ledger.catalogs[ledger.initial_hash],
            ),
            self.timeout,
        )
        if (
            proof.descriptor != ledger.descriptor
            or proof.initial_catalog != ledger.catalogs[ledger.initial_hash]
        ):
            raise ClientError("close attachment opening differs from retained intent")
        if self._control is not None:
            self._control_lost(ClientError("replacing incomplete Control attachment"))
        await self._connect()
        await self._attach(outcome.tx_hash, require_ready=False)
        if self._control_failure is not None:
            raise ClientError("close attachment was lost")
        self._close_waiter = None

    async def _voucher(self, target: int, *, generation: int, run_id: bytes | None = None) -> None:
        async with self._voucher_lock:
            ledger = self._require_ledger()

            def eligible() -> bool:
                return (
                    ledger.ready
                    and not ledger.close_pending
                    and self._control_failure is None
                    and ledger.generation == generation
                    and self._attached_generation == generation
                    and self.chain.confirmed_readiness(ledger.session_id).ready
                    and (
                        ledger.runs[run_id].active
                        if run_id is not None
                        else any(run.active for run in ledger.runs.values())
                    )
                )

            if not eligible():
                return
            self._require_ready()
            amount = max(target, ledger.voucher.cumulative if ledger.voucher is not None else 0)
            if amount > self._observed_state().deposit:
                return
            if ledger.voucher is None or ledger.voucher.cumulative < amount:
                signature = await asyncio.wait_for(
                    asyncio.to_thread(
                        self.signer.sign_voucher, ledger.domain, ledger.session_id, amount
                    ),
                    self.timeout,
                )
                if not eligible():
                    return
                self._require_ready()
                voucher = pb.Voucher(
                    session_id=ledger.session_id, cumulative=amount, signature=signature
                )
                ledger.accept_voucher(voucher, self._observed_state().deposit)
                await self._persist()
            assert ledger.voucher is not None
            if eligible():
                await self._send(pb.ControlMessage(voucher=ledger.voucher))
                self._observers.emit("on_voucher", ledger.voucher)

    async def _renew(self) -> None:
        while True:
            ledger = self._require_ledger()
            if (
                not ledger.ready
                or ledger.close_pending
                or not any(run.active for run in ledger.runs.values())
                or not self.chain.confirmed_readiness(ledger.session_id).ready
            ):
                return
            target = required_coverage((max(ledger.usage_numerator, ledger.coverage_numerator),))
            signed = ledger.voucher.cumulative if ledger.voucher is not None else None
            if target > self._observed_state().deposit or (signed is not None and target <= signed):
                return
            await self._voucher(target, generation=ledger.generation)
            if (ledger.voucher.cumulative if ledger.voucher is not None else None) == signed:
                # Not eligible now (for example, an attachment not yet complete):
                # stop rather than spin. A later Meter or top-up renews again.
                return

    def accept_catalog(self, rate_card_hash: bytes) -> Catalog:
        """Accept an exact currently observed publication for future runs only."""
        return self._sync(self._accept_catalog(rate_card_hash))

    async def _accept_catalog(self, rate_card_hash: bytes) -> Catalog:
        fixed_bytes(rate_card_hash, 32, field="rate_card_hash")
        ledger = self._require_ledger()
        identity = (await self._fresh()).identities[ledger.descriptor.provider_id]
        if identity.catalog is None or identity.catalog.rate_card_hash != rate_card_hash:
            raise ClientError("catalog does not match the fresh provider publication")
        ledger.add_catalog(identity.catalog)
        ledger.accepted_catalogs.add(rate_card_hash)
        await self._persist()
        return identity.catalog

    async def _create(self, model: str, stream: bool, payload: bytes) -> tuple[bytes, _Output]:
        opening = self._ledger is None or not self._ledger.ready
        result = await (
            self._measured("open", self._open(model, None)) if opening else self._open(model, None)
        )
        if result.session is None:
            raise ClientError("opening is pending or unknown; reconcile before inference")
        ledger = self._require_ready()
        # The last confirmed publication: a provider that has since changed its
        # prices refuses the run with PRICE_CHANGED, before admission.
        identity = self._observed_identity(ledger.descriptor.provider_id)
        if identity.catalog is None:
            raise ClientError("provider has no valid current catalog")
        # Reusing accepted bytes is permitted; an observed price change requires
        # the caller to explicitly refresh/accept before another signed request.
        if identity.catalog.rate_card_hash not in ledger.accepted_catalogs:
            raise PaymentError(
                PaymentFailure(
                    PaymentCode.SESSION_NOT_READY,
                    "PRICE_CHANGED",
                    ledger.session_id,
                    requested_rate_card_hash=ledger.initial_hash,
                    current_rate_card_hash=identity.catalog.rate_card_hash,
                )
            )
        identity.catalog.rates_for(model)
        context = RunContext(
            ledger.session_id,
            secrets.token_bytes(32),
            ledger.generation,
            model,
            identity.catalog.rate_card_hash,
        )
        signature = await asyncio.wait_for(
            asyncio.to_thread(self.signer.sign_run, ledger.domain, context), self.timeout
        )
        if self._require_ready() is not ledger:
            raise ClientError("attachment changed while the run was signed")
        request = pb.CompleteRequest(
            model=model,
            stream=stream,
            session_id=context.session_id,
            run_id=context.run_id,
            control_generation=context.control_generation,
            rate_card_hash=context.rate_card_hash,
            auth_signature=signature,
            payload=payload,
        )
        validate_message(request)
        ledger.register(context, signature)
        self._unsent.add(context.run_id)
        try:
            await self._persist()
            if self._require_ready() is not ledger:
                raise ClientError("attachment changed before Complete submission")
        except BaseException:
            # Nothing was sent, so the provider never held this run. Record it
            # NOT_ADMITTED with no usage so no reattach names it pending.
            if context.run_id in self._unsent:
                self._unsent.discard(context.run_id)
                current = self._ledger
                if current is not None and context.run_id in current.pending:
                    current.record_not_admitted(context.run_id)
                    await self._persist()
            raise
        self._unsent.discard(context.run_id)
        output = _Output(ByteQueue(**self._queue_options))
        self._outputs[context.run_id] = output
        output.call = services.ChatStub(self._channel).Completions(
            request, timeout=self.request_timeout
        )
        output.task = asyncio.create_task(self._read_complete(context.run_id, stream))
        return context.run_id, output

    async def _read_complete(self, identifier: bytes, stream: bool) -> None:
        # This stream ends on its own terms. Control loss or a reattach never
        # cancels it, and a failure here fails only this run. Reports
        # apply to the current ledger, which a reattach may have replaced.
        output = self._outputs[identifier]
        upstream: InferenceError | None = None
        payloads = 0
        try:
            async for chunk in output.call:
                validate_message(chunk)
                if output.terminal is not None:
                    raise ClientError("Complete sent data after its terminal")
                kind = chunk.WhichOneof("kind")
                if kind == "payload":
                    if upstream is not None or (not stream and payloads):
                        raise ClientError("unexpected inference payload ordering")
                    validate_json(chunk.payload)
                    payloads += 1
                    await output.queue.put(chunk.payload)
                elif kind == "upstream_error":
                    if upstream is not None:
                        raise ClientError("duplicate upstream error")
                    upstream = InferenceError(
                        chunk.upstream_error.body,
                        chunk.upstream_error.http_status
                        if chunk.upstream_error.HasField("http_status")
                        else None,
                    )
                elif kind == "terminal":
                    if chunk.terminal.run_id != identifier:
                        raise ClientError("terminal names another run")
                    await self._verify_reference(chunk.terminal)
                    self._require_ledger().apply_final(
                        chunk.terminal, self._observed_state().deposit
                    )
                    output.terminal = pb.Terminal.FromString(chunk.terminal.SerializeToString())
                    await self._persist()
                else:
                    raise ClientError("unknown Complete message")
            if await output.call.code() is not grpc.StatusCode.OK:
                raise ClientError("Complete did not finish with gRPC OK")
            if upstream is not None:
                raise upstream
            if output.terminal is None or output.terminal.status != pb.RUN_STATUS_COMPLETED:
                raise ClientError("Complete lacks a successful definitive final")
            if not stream and payloads != 1:
                raise ClientError("nonstreaming response requires one JSON object")
            output.queue.close()
        except asyncio.CancelledError:
            self._require_ledger().runs[identifier].failed_transport = True
            output.queue.close(
                ClientError("inference was cancelled; final accounting may remain unresolved")
            )
            raise
        except Exception as error:
            ledger = self._require_ledger()
            self._observers.emit(
                "on_error",
                HookError(
                    "complete",
                    type(error).__name__,
                    ledger.session_id,
                    identifier,
                    _detail(None, error),
                ),
            )
            ledger.runs[identifier].failed_transport = True
            failure = self._rpc_error(error)
            if (
                payloads == 0
                and output.terminal is None
                and upstream is None
                and _refused_before_admission(failure)
            ):
                # The provider refused it before admission and sent nothing:
                # a definite outcome (protocol.md, Request admission, steps 5
                # and 6), so it is never left unresolved.
                try:
                    ledger.record_not_admitted(identifier)
                except ValidationError:
                    pass  # Reports arrived for it after all: it stays unresolved.
            output.queue.close(upstream if upstream is not None else failure)
            if not output.call.done():
                output.call.cancel()
            await self._persist()

    async def _verify_reference(self, message: pb.Terminal | pb.Receipt) -> None:
        if message.HasField("settlement_reference"):
            reference = message.settlement_reference
            observed = ConfirmedSettlementRef(
                observation_from_wire(reference.observation),
                reference.tx_hash,
                reference.session_id,
                reference.settled_cumulative,
                reference.delta,
            )
            ledger = self._require_ledger(message.session_id)
            await asyncio.wait_for(
                asyncio.to_thread(self.chain.reader.verify_settlement, observed, ledger.descriptor),
                self.timeout,
            )

    async def _receipt_worker(self) -> None:
        while self._receipt_queue:
            await self._receipt(*self._receipt_queue.popleft())

    async def _receipt(self, receipt: pb.Receipt, attempt: int = 0) -> None:
        # A Receipt that cannot be validated or proven now is set aside alone;
        # the run's Terminal on Complete, or a later snapshot, still accounts it.
        try:
            await self._verify_receipt(receipt)
        except (TransportError, ChainLagError) as error:
            # A transient or lagging chain read failed, not the Receipt: try
            # once more after the queue, then let a later copy of it be proven.
            if (
                attempt == 0
                and (isinstance(error, ChainLagError) or error.transient)
                and len(self._receipt_queue) < _RECEIPT_QUEUE
            ):
                self._receipt_queue.append((receipt, 1))
                return
            self._receipts_seen.discard(receipt.run_id)
            self._receipt_failures += 1
            self._reject("receipt", error)
        except (*_REPORT_FAILURES, TimeoutError) as error:
            self._receipt_failures += 1
            self._reject("receipt", error)

    async def _verify_receipt(self, receipt: pb.Receipt) -> None:
        ledger = self._require_ledger(receipt.session_id)
        observation = observation_from_wire(receipt.chain_observation)
        if (
            observation.chain_id != ledger.domain.chain_id
            or observation.contract_address != ledger.domain.verifying_contract
        ):
            raise ClientError("receipt chain observation differs from session domain")
        # The provider stamps every Receipt with its follower's current block,
        # so most Receipts repeat an observation already proven here. The
        # cached total is one this agent read itself at that exact canonical
        # block of this session; a provider cannot add an entry.
        key = (ledger.session_id, observation.block_number, observation.block_hash)
        settled = self._proven.get(key)
        if settled is None:
            block = await asyncio.wait_for(
                asyncio.to_thread(self.chain.reader.block, observation.block_number), self.timeout
            )
            state = await asyncio.wait_for(
                asyncio.to_thread(self.chain.reader.session, ledger.session_id, block),
                self.timeout,
            )
            if block.observation != observation:
                raise ClientError("receipt settlement total contradicts chain or accepted usage")
            canonical = await asyncio.wait_for(
                asyncio.to_thread(self.chain.reader.block, observation.block_number), self.timeout
            )
            if canonical != block:
                raise ClientError("receipt observation became noncanonical")
            settled = state.settled_cumulative
            self._proven[key] = settled
            if len(self._proven) > _PROVEN_OBSERVATIONS:
                self._proven.popitem(last=False)
        else:
            self._proven.move_to_end(key)
        if settled != receipt.settled_cumulative:
            raise ClientError("receipt settlement total contradicts chain or accepted usage")
        await self._verify_reference(receipt)
        run = ledger._run(receipt.run_id)
        rates = ledger.catalogs[run.context.rate_card_hash].rates_for(run.context.model)
        reported = TokenCounts(receipt.input_tokens, receipt.cache_tokens, receipt.output_tokens)
        prospective = (
            ledger.usage_numerator
            - priced_numerator(run.counts, rates)
            + priced_numerator(reported, rates)
        ) // 1_000_000
        if receipt.settled_cumulative > prospective:
            raise ClientError("receipt settlement total exceeds accepted usage")
        ledger.apply_final(receipt, self._observed_state().deposit)
        self._receipted.add(receipt.run_id)
        if receipt.run_id in self._outputs:
            self._outputs[receipt.run_id].receipt = pb.Receipt.FromString(
                receipt.SerializeToString()
            )
        await self._persist()
        self._observers.emit("on_receipt", receipt)

    async def _next(self, output: _Output) -> bytes | None:
        return await output.queue.get_or_end()

    async def _final_record(self, output: _Output, *, receipt: bool) -> Any:
        value = output.receipt if receipt else output.terminal
        return type(value).FromString(value.SerializeToString()) if value is not None else None

    async def _cancel_run(self, identifier: bytes, output: _Output) -> None:
        run = self._require_ledger().runs.get(identifier)
        if run is not None:  # A settled, pruned run has nothing left to cancel.
            run.failed_transport = True
        if output.call is not None:
            output.call.cancel()
        if output.task is not None and not output.task.done():
            output.task.cancel()
            await asyncio.gather(output.task, return_exceptions=True)
        output.queue.close(ClientError("caller cancelled inference"))
        await self._persist()

    async def _terms(self, identifier: bytes) -> SessionTerms:
        await self._local_start()
        ledger = self._require_ledger(identifier)
        current = self.chain.current
        identity = (
            current.identities.get(ledger.descriptor.provider_id) if current is not None else None
        )
        return SessionTerms(
            ledger.session_id,
            ledger.initial_hash,
            identity.catalog.rate_card_hash
            if identity is not None and identity.catalog is not None
            else None,
            MappingProxyType({h: ledger.catalogs[h] for h in ledger.accepted_catalogs}),
        )

    async def _run_outcome(self, identifier: bytes, run_id: bytes) -> RunOutcome:
        await self._local_start()
        ledger = self._require_ledger(identifier)
        run = ledger.runs.get(fixed_bytes(run_id, 32, field="run_id"))
        if run is None:
            raise ClientError("run is not retained in this session")
        if run.final is None:
            status, amount = RunOutcomeStatus.PENDING, None
        else:
            status = RunOutcomeStatus(pb.RunStatus.Name(run.final[3]).removeprefix("RUN_STATUS_"))
            amount = run.final[5]
        return RunOutcome(
            run_id,
            run.context.model,
            run.context.rate_card_hash,
            status,
            run.counts,
            amount,
        )

    async def _get(self, identifier: bytes) -> SessionView:
        await self._local_start()
        ledger = self._require_ledger(identifier)
        current = self.chain.current
        if current is None or identifier not in current.sessions:
            raise ClientError("session has no chain observation")
        state = current.sessions[identifier]
        readiness = self.chain.confirmed_readiness(identifier)
        try:
            self.chain.snapshot()
            fresh = True
        except ChainEvidenceError:
            fresh = False
        if readiness.ready and (
            not ledger.ready
            or ledger.close_pending
            or self._control_failure is not None
            or self._resync is not None
            or self._attached_generation == 0
            or self._attached_generation != ledger.generation
        ):
            readiness = Readiness(
                False,
                (
                    PaymentFailure(
                        PaymentCode.SESSION_NOT_READY,
                        "CLOSE_PENDING" if ledger.close_pending else "CONTROL_NOT_READY",
                        identifier,
                    ),
                ),
            )
        return SessionView(
            identifier,
            ledger.descriptor,
            state.deposit,
            state.settled_cumulative,
            ledger.voucher.cumulative if ledger.voucher else None,
            ledger.usage_amount,
            state.status,
            current.block.observation,
            fresh,
            readiness,
        )

    async def _list(self) -> SessionPage:
        await self._local_start()
        if self._ledger is None:
            return SessionPage((), None)
        return SessionPage((await self._get(self._ledger.session_id),), None)

    async def _top_up(self, identifier: bytes, amount: int) -> OperationResult:
        self._require_agent_id()
        await self._local_start()
        ledger = self._require_ledger(identifier)
        uint64(amount, field="top-up amount", positive=True)
        async with self._funding_lock:
            await self._fresh()
            readiness = self.chain.readiness(identifier)
            if not readiness.ready or ledger.close_pending:
                raise ClientError("session is ineligible for top-up")
            if any(
                op.kind in (OperationKind.OPEN, OperationKind.TOP_UP)
                and result.state in (OperationState.UNKNOWN, OperationState.PENDING)
                for op, result in self._operations.values()
            ):
                raise ClientError("reconcile pending funding before a new top-up")
            top_up_total(self._chain_state().deposit, amount)
            auth = self._retained_top_up(ledger, amount) or self._funding_authorization(amount)
            ref = "top-up-" + secrets.token_hex(32)
            self._saved["funding_intent"] = {
                "ref": ref,
                "amount": amount,
                "nonce": auth.nonce.hex(),
                "valid_after": auth.valid_after,
                "valid_before": auth.valid_before,
            }
            await self._persist()
            # The opening proved this wallet's binding and signature; USDC still
            # checks this one, and a refusal is a definite failed top-up.
            with _timed("wallet_sign"):
                signature = await asyncio.wait_for(
                    asyncio.to_thread(self.wallet.sign_funding_authorization, auth), self.timeout
                )
            await self._fresh()
            if not self.chain.readiness(identifier).ready or ledger.close_pending:
                raise ClientError("session became ineligible while funding was signed")
            top_up_total(self._chain_state().deposit, amount)
            operation = self.contract.top_up(ledger.descriptor, amount, auth, signature)
            result = await self._submit(operation, ref)
            if result.state is OperationState.CONFIRMED:
                await self._top_up_confirmed(result)
            return result

    def _retained_top_up(self, ledger: Ledger, amount: int) -> FundingAuthorization | None:
        """The authorization of an earlier top-up that never took effect, while
        it is still valid: a retry sends that same authorization, so even an
        earlier attempt that somehow landed cannot fund twice (its nonce is
        spent). An expired one is retired; a fresh top-up gets a new nonce.
        """
        intent = self._saved.get("funding_intent")
        if not isinstance(intent, dict) or not str(intent.get("ref", "")).startswith("top-up-"):
            return None
        previous = self._operations.get(intent["ref"])
        if (previous is not None and previous[1].state is OperationState.CONFIRMED) or (
            self.chain.snapshot().block.timestamp >= intent["valid_before"]
        ):
            return None
        if intent["amount"] != amount:
            raise ClientError(
                f"an earlier top-up of {intent['amount']} is still valid; retry that amount"
            )
        return FundingAuthorization(
            ledger.domain.chain_id,
            ledger.descriptor.token,
            self.contract.token_domain_name,
            self.contract.token_domain_version,
            ledger.descriptor.agent,
            ledger.domain.verifying_contract,
            amount,
            intent["valid_after"],
            intent["valid_before"],
            bytes.fromhex(intent["nonce"]),
        )

    async def _top_up_confirmed(self, result: OperationResult) -> None:
        ledger = self._require_ledger()
        proven = [o.deposit for o in result.sessions if o.session_id == ledger.session_id]
        if proven and proven[0] is not None:
            # The ToppedUp event names the new deposit: no refresh is needed.
            prior = self._observed_state()
            self.chain.confirm(
                ledger.session_id, replace(prior, deposit=max(prior.deposit, proven[0]))
            )
        else:
            await self.chain.refresh()
        intent = self._saved.get("funding_intent")
        if isinstance(intent, dict) and intent.get("ref") == result.operation_ref:
            del self._saved["funding_intent"]  # Spent: the next top-up gets a new nonce.
            await self._persist()
        self._top_up_hint = result.tx_hash
        if self._control is not None and self._control_failure is None:
            await self._announce_top_up()
            # A prior Meter may already require more than the former deposit.
            # Recheck that retained target against fresh confirmed funding now;
            # a hint or pending wallet result never changes the spending bound.
            # An attachment still in progress renews once it has attached.
            ledger = self._require_ledger()
            if self._attached_generation != 0 and self._attached_generation == ledger.generation:
                if self._renew_task is None or self._renew_task.done():
                    self._renew_task = self._spawn(self._renew())

    async def _announce_top_up(self) -> None:
        """Send TopUpNotice only on an attached stream (a hint, never credit).

        Before ReconnectRequest has attached a new stream, a provider refuses
        any other message and ends the stream, so the hint waits for _attach.
        """
        ledger = self._require_ledger()
        if (
            self._top_up_hint is None
            or self._attached_generation == 0
            or self._attached_generation != ledger.generation
        ):
            return
        tx, self._top_up_hint = self._top_up_hint, None
        await self._send(
            pb.ControlMessage(top_up_notice=pb.TopUpNotice(session_id=ledger.session_id, tx=tx))
        )

    async def _cooperative_close(self, identifier: bytes) -> OperationResult:
        await self._local_start()
        ledger = self._require_ledger(identifier)
        if self._closed_error is not None:
            raise self._closed_error
        if self._closed_result is not None:
            return self._closed_result
        if "reported_close" in self._saved:
            # The provider already reported this close: prove it again, and
            # never request a close it may already have made canonical.
            return await self._prove_close()
        async with self._open_lock, self._funding_lock:
            await self._ensure_close_attachment()
            ledger = self._require_ledger(identifier)
            ledger.close_pending, ledger.ready = True, False
            await self._persist()
            if self._close_waiter is None:
                self._close_waiter = self._loop.create_future()
                try:
                    signature = await asyncio.wait_for(
                        asyncio.to_thread(self.signer.sign_close, ledger.domain, identifier),
                        self.timeout,
                    )
                    verify_close_auth(ledger.domain, ledger.descriptor, signature)
                    await self._send(
                        pb.ControlMessage(
                            close_request=pb.CloseRequest(
                                session_id=identifier, signature=signature
                            )
                        )
                    )
                except Exception as error:
                    self._close_waiter.cancel()
                    self._close_waiter = None
                    self._control_lost(error)
                    raise
        assert self._close_waiter is not None
        try:
            return await asyncio.wait_for(asyncio.shield(self._close_waiter), self.timeout)
        except TimeoutError:
            return OperationResult("close-" + identifier.hex(), OperationState.UNKNOWN)

    async def _close_result(self, message: pb.CloseResult) -> None:
        ledger = self._require_ledger(message.session_id)
        if not ledger.close_pending or self._close_waiter is None:
            raise ClientError("unsolicited cooperative close result")
        # Retained before any read: a close the chain may already hold must
        # never be requested again, and a later close() proves this one.
        self._saved["reported_close"] = {
            "tx": message.tx_hash.hex(),
            "capture": message.capture_amount,
            "refunded": message.refunded,
        }
        await self._persist()
        result = await self._prove_close()
        if not self._close_waiter.done():
            self._close_waiter.set_result(result)

    async def _prove_close(self) -> OperationResult:
        """Prove the reported close on chain; PENDING while its proof is unreadable.

        CONFIRMED needs the canonical SessionClosed event and state with the
        reported capture and refund. A read that fails leaves the close
        PENDING with its transaction, so calling close() again completes it.
        """
        ledger = self._require_ledger()
        record = self._saved["reported_close"]
        tx = bytes.fromhex(record["tx"])
        ref = "close-" + ledger.session_id.hex()
        try:
            proof = await asyncio.wait_for(
                asyncio.to_thread(
                    self._timed_read, self.chain.reader.closing, tx, ledger.session_id
                ),
                self.timeout,
            )
        except (TransportError, ChainEvidenceError, TimeoutError) as error:
            self._observers.emit(
                "on_error",
                HookError(
                    "close",
                    type(error).__name__,
                    ledger.session_id,
                    detail="closing proof is unavailable; the close stays pending",
                ),
            )
            return OperationResult(ref, OperationState.PENDING, tx_hash=tx)
        if (proof.capture_amount, proof.refunded) != (record["capture"], record["refunded"]):
            raise ClientError("close result differs from canonical payment and refund")
        result = OperationResult(
            ref, OperationState.CONFIRMED, tx_hash=tx, observation=proof.observation
        )
        self._closed_result = result
        self._saved["confirmed_close"] = {
            "tx": record["tx"],
            "block": proof.observation.block_number,
            "block_hash": proof.observation.block_hash.hex(),
            "capture": record["capture"],
            "refunded": record["refunded"],
        }
        del self._saved["reported_close"]
        if record["capture"] > ledger.usage_amount:
            self._closed_error = CloseAccountingError(
                result, record["capture"], ledger.usage_amount
            )
        await self._persist()
        # SessionClosed proves the capture; the next refresh replaces this view.
        prior = self._observed_state()
        self.chain.confirm(
            ledger.session_id,
            replace(
                prior,
                status=SessionStatus.CLOSED,
                settled_cumulative=max(prior.settled_cumulative, record["capture"]),
            ),
        )
        if self._closed_error is not None:
            raise self._closed_error
        return result

    async def _exit(self, identifier: bytes, kind: OperationKind) -> OperationResult:
        await self._local_start()
        ledger = self._require_ledger(identifier)
        async with self._open_lock, self._funding_lock:
            ref = kind.value + "-" + identifier.hex()
            if ref in self._operations:
                # Current Closed/paused state cannot erase this operation's
                # original confirmed, pending or unknown outcome.
                return await self._reconcile_ref(ref)
            current = await self._fresh()
            state = current.sessions[identifier]
            now = current.block.timestamp
            if (
                not state.exists
                or state.status is SessionStatus.CLOSED
                or current.pause_level is PauseLevel.ALL
            ):
                raise ClientError("session exit is not currently eligible")
            if kind is OperationKind.REQUEST_CLOSE:
                if state.status is not SessionStatus.OPEN or now >= state.last_activity + 86400:
                    raise ClientError("requestClose is not eligible")
                operation = self.contract.request_close(ledger.descriptor)
            elif kind is OperationKind.WITHDRAW:
                if (
                    state.status is not SessionStatus.CLOSING
                    or now < state.close_requested_at + 3600
                ):
                    raise ClientError("withdrawal deadline has not arrived")
                operation = self.contract.withdraw(ledger.descriptor)
            else:
                if now < state.last_activity + 86400:
                    raise ClientError("idle deadline has not arrived")
                operation = self.contract.release_idle(ledger.descriptor)
            ledger.close_pending, ledger.ready = True, False
            result = await self._submit(operation, ref)
            if result.state is OperationState.CONFIRMED:
                await self.chain.refresh()
            return result

    async def _shutdown(self) -> ShutdownResult:
        if self._ledger is not None:
            self._ledger.ready = False
        for identifier, output in self._outputs.items():
            if self._ledger is not None and self._ledger.runs[identifier].final is None:
                self._ledger.runs[identifier].failed_transport = True
            if output.call is not None:
                output.call.cancel()
            if output.task is not None:
                output.task.cancel()
            output.queue.close(ClientError("client shut down"))
        if self._control is not None:
            self._control.cancel()
        if self._outbound is not None:
            self._outbound.close()
        for task in self._control_tasks + list(self._background):
            task.cancel()
        await asyncio.gather(
            *self._control_tasks,
            *self._background,
            *(output.task for output in self._outputs.values() if output.task is not None),
            return_exceptions=True,
        )
        try:
            if self._channel is not None:
                await self._channel.close()
        finally:
            await self.chain.stop()
            # Kept-alive connections end with the client; a later read reopens one.
            reader = getattr(self.chain, "reader", None)
            for owner in (getattr(self, "_http", None), getattr(reader, "rpc", None)):
                close = getattr(owner, "close", None)
                if callable(close):
                    close()
        # Unused clients and rejected/partial restorations must preserve the
        # existing journal, including operations not yet loaded into memory.
        if self._restored:
            await self._persist()
        ledger = self._ledger
        pending = tuple(
            result
            for _, result in self._operations.values()
            if result.state in (OperationState.PENDING, OperationState.UNKNOWN)
        )
        runs = (
            tuple(run.context for run in ledger.runs.values() if run.final is None)
            if ledger
            else ()
        )
        current = self.chain.current if ledger is not None else None
        unresolved_close = (
            ledger is not None
            and ledger.close_pending
            and self._closed_result is None
            and (
                current is None
                or ledger.session_id not in current.sessions
                or current.sessions[ledger.session_id].status is not SessionStatus.CLOSED
            )
        )
        self._observers.close()
        return ShutdownResult(
            (ledger.session_id,) if ledger and (runs or pending or unresolved_close) else (),
            runs,
            pending,
        )

    def close(self) -> ShutdownResult:
        if self._shutdown_result is not None:
            return self._shutdown_result
        if self._closed:
            raise ClientError("client shut down after a checkpoint failure")
        try:
            self._shutdown_result = self._sync(self._shutdown())
            return self._shutdown_result
        finally:
            self._observers.close()
            self._closed = True
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=self.timeout + 5)
            self._checkpoint.close()
            if self._thread.is_alive():
                raise ClientError("client loop did not stop within its cleanup deadline")

    def __enter__(self) -> Client:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
