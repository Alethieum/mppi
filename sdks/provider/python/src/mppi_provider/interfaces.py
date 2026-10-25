"""Provider-owned facts and SDK lifecycle contracts; no storage interface."""

from __future__ import annotations

from asyncio import Event
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from mppi_protocol import RunAccounting, RunStatus, TokenCounts
from mppi_protocol.contract import PreparedOperation
from mppi_protocol.types import (
    AccountingResult,
    ChainObservation,
    OperationResult,
    PaymentFailure,
    Readiness,
    RunContext,
    SessionPage,
    SessionView,
    ShutdownResult,
)

if TYPE_CHECKING:
    from grpc.aio import Server
    from mppi.v1.mppi_pb2 import SessionState


@dataclass(frozen=True, slots=True)
class PreparedRequest:
    """Validated unchanged inference payload; preparation performs no paid work."""

    payload: bytes
    stream: bool


@dataclass(frozen=True, slots=True)
class CoverageResult:
    granted: bool
    required_amount: int
    failure: PaymentFailure | None


@dataclass(frozen=True, slots=True)
class FinalAccountingResult:
    accounting: AccountingResult
    status: RunStatus


@dataclass(frozen=True, slots=True)
class RecoveryRun:
    """Original signed identity and provider-reported accounting for one run."""

    context: RunContext
    signature: bytes
    accounting: RunAccounting
    covered_counts: TokenCounts
    meter_seq: int
    admitted: bool
    not_admitted: bool


@dataclass(frozen=True, slots=True)
class RecoveryOperation:
    """Exact retained submission intent; supplied results are reverified."""

    prepared: PreparedOperation
    result: OperationResult
    units: tuple[tuple[bytes, int], ...]


@dataclass(frozen=True, slots=True)
class RecoveryEvidence:
    """Detached payment checkpoint; the provider owns retention and availability.

    ``snapshot`` is accounting evidence, never proof of readiness. ``runs``
    preserves all observed request identities, including unresolved reservations.
    No object supplied here is trusted as canonical chain evidence.
    Before first attachment, a private generation-zero snapshot has no accepted
    voucher, usage, runs or reservations. The provider must establish that this
    history is complete; chain records alone cannot establish an unused session.
    Recovery installs no owner, and fresh authentication assigns generation one.
    """

    snapshot: SessionState
    reservations: tuple[tuple[RunContext, RunAccounting], ...]
    observation: ChainObservation
    opening_tx: bytes = b""
    runs: tuple[RecoveryRun, ...] = ()
    publications: tuple[tuple[bytes, ChainObservation], ...] = ()
    operation: RecoveryOperation | None = None
    baseline: RecoveryOperation | None = None
    confirmed_units: int = 0
    close_reference: str | None = None
    inconsistent: bool = False


@dataclass(frozen=True, slots=True)
class DiscoveryResponse:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes


class CoverageProvider(Protocol):
    async def __call__(self, request: PreparedRequest, context: RunContext) -> TokenCounts: ...


class ProviderRuns(Protocol):
    async def require_coverage(
        self, context: RunContext, remaining_tokens: TokenCounts
    ) -> CoverageResult: ...

    async def report_usage(self, context: RunContext, counts: TokenCounts) -> AccountingResult: ...

    async def report_terminal(
        self, context: RunContext, counts: TokenCounts, status: RunStatus
    ) -> FinalAccountingResult: ...


class ExecutionProvider(Protocol):
    """Provider execution integration, invoked only after payment admission.

    Yield original JSON objects after consuming any upstream framing. Supply
    cumulative usage and definitive termination through ``runs``. The provider
    terminates its job when ``stop_requested`` becomes set; returning from this
    iterator, a cancellation signal and transport EOF are not final reports.
    No engine adapter, token measurement or persistence is provided by MPPI.
    """

    def __call__(
        self,
        request: PreparedRequest,
        context: RunContext,
        runs: ProviderRuns,
        stop_requested: Event,
    ) -> AsyncIterator[bytes]: ...


class ProviderSessions(Protocol):
    async def checkpoint(self, session_id: bytes) -> RecoveryEvidence: ...

    async def recover(self, evidence: RecoveryEvidence) -> SessionView: ...

    async def recover_current(self, evidence: RecoveryEvidence) -> SessionView: ...

    async def settle(self, session_id: bytes) -> OperationResult: ...

    async def settleBatch(self, session_ids: tuple[bytes, ...]) -> OperationResult: ...

    async def close(self, session_id: bytes) -> OperationResult: ...

    async def get(self, session_id: bytes) -> SessionView: ...

    async def list(self, *, cursor: str | None = None, limit: int = 100) -> SessionPage: ...


class ProviderLifecycle(Protocol):
    """Embedded server lifetime; callers retain their listener and event loop."""

    @property
    def runs(self) -> ProviderRuns: ...

    @property
    def sessions(self) -> ProviderSessions: ...

    def register_grpc(self, server: Server) -> None: ...

    def discovery(self, path: str) -> DiscoveryResponse | None: ...

    async def start(self) -> Readiness: ...

    async def shutdown(self, deadline: float) -> ShutdownResult: ...
