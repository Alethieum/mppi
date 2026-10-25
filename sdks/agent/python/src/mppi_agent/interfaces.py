"""Typed agent integration contracts implemented by the network Client."""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import TracebackType
from typing import Protocol

from mppi.v1 import mppi_pb2 as pb
from mppi_protocol import Catalog, Rates, TokenCounts, ValidationCode, ValidationError
from mppi_protocol.capabilities import FundingWallet, SessionSigner
from mppi_protocol.types import OperationResult, OperationState, SessionPage, SessionView


@dataclass(frozen=True, slots=True)
class SessionTerms:
    """The prices this agent accepted for a session, by rate-card hash.

    ``initial_rate_card_hash`` funded the session; ``catalogs`` holds it and
    every catalog accepted since. ``current_rate_card_hash`` is the provider's
    last confirmed publication, or ``None`` when it has none. A new run uses it
    only if it is accepted; an admitted run keeps the prices it signed.
    """

    session_id: bytes
    initial_rate_card_hash: bytes
    current_rate_card_hash: bytes | None
    catalogs: Mapping[bytes, Catalog]

    def rates(self, model: str, rate_card_hash: bytes | None = None) -> Rates:
        """A model's rates in an accepted catalog, the initial one by default."""
        catalog = self.catalogs[
            self.initial_rate_card_hash if rate_card_hash is None else rate_card_hash
        ]
        return catalog.rates_for(model)


class RunOutcomeStatus(StrEnum):
    PENDING = "PENDING"  # No final yet: running, or still to be reconciled.
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    NOT_ADMITTED = "NOT_ADMITTED"  # The provider never held it: no usage.


@dataclass(frozen=True, slots=True)
class RunOutcome:
    """One retained run: its signed prices, accepted usage and outcome.

    ``counts`` are the tokens accepted so far; ``amount`` is the run's charge in
    USDC base units once it has a final, else ``None``.
    """

    run_id: bytes
    model: str
    rate_card_hash: bytes
    status: RunOutcomeStatus
    counts: TokenCounts
    amount: int | None


@dataclass(frozen=True, slots=True)
class OpenResult:
    operation: OperationResult
    session: SessionView | None

    def __post_init__(self) -> None:
        if self.session is not None and (
            self.operation.state is not OperationState.CONFIRMED or not self.session.readiness.ready
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE,
                "usable opening requires confirmed funding and a ready attachment",
            )


@dataclass(frozen=True, slots=True)
class FundingNotice:
    """A requirement the confirmed deposit cannot cover, for funding policy.

    ``notice`` is the provider's validated ``pb.LowCredit``, a detached copy.
    ``deposit`` is the confirmed deposit and ``required`` the amount the
    notice's coverage request needs, both in six-decimal USDC base units, and
    ``shortfall`` is ``required - deposit``: an amount for ``sessions.topUp``.
    Both are worked out when the notice is delivered, not when it arrived. A
    plain low-credit notice has no coverage request: ``required`` and
    ``shortfall`` are ``None``. The SDK never tops up by itself.
    """

    notice: pb.LowCredit
    deposit: int
    required: int | None
    shortfall: int | None


class CompletionStream(Protocol):
    """One Complete's unchanged JSON payloads, with deterministic cleanup."""

    def __iter__(self) -> Iterator[bytes]: ...

    def __next__(self) -> bytes: ...

    def close(self) -> None: ...

    def __enter__(self) -> "CompletionStream": ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class AgentSessions(Protocol):
    """Explicit economic actions; none implies automatic funding or replay."""

    def open(
        self, provider: str | int, *, model: str, initial_session_tokens: int | None = None
    ) -> OpenResult: ...

    def topUp(self, session_id: bytes, amount: int) -> OperationResult: ...

    def close(self, session_id: bytes) -> OperationResult: ...

    def requestClose(self, session_id: bytes) -> OperationResult: ...

    def withdraw(self, session_id: bytes) -> OperationResult: ...

    def releaseIdle(self, session_id: bytes) -> OperationResult: ...

    def get(self, session_id: bytes) -> SessionView: ...

    def recover(self, session_id: bytes) -> SessionView: ...

    def reconcile(self, operation_ref: str) -> OperationResult: ...

    def list(self, *, cursor: str | None = None, limit: int = 100) -> SessionPage: ...

    def terms(self, session_id: bytes) -> SessionTerms: ...

    def run(self, session_id: bytes, run_id: bytes) -> RunOutcome: ...


__all__ = [
    "AgentSessions",
    "CompletionStream",
    "FundingWallet",
    "OpenResult",
    "RunOutcome",
    "RunOutcomeStatus",
    "SessionSigner",
    "SessionTerms",
]
