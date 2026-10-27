"""Typed, optional lifecycle observers. See the public lifecycle hook profile."""

from collections.abc import Callable
from dataclasses import dataclass

from mppi.v1 import mppi_pb2 as pb
from mppi_protocol._observers import HookError, ObserverStats
from mppi_protocol.types import OperationResult


@dataclass(frozen=True, slots=True)
class OperationTiming:
    """Where one payment step's time went, in milliseconds.

    ``operation`` is ``admission`` for a paid request, from its arrival until its
    run is admitted or refused (only a request whose run was registered
    reports), or ``settle``, ``settleBatch`` or ``close`` for one collection
    call, by the host or by maintenance. ``state`` is ``admitted``, or the
    collection result's state; ``None`` when the step was refused, raised or was
    cancelled. ``coverage_ms`` is the host's coverage callback; ``chain_ms``
    this SDK's chain reads: a batch's simulation, the transaction's proofs and
    the refresh after it; ``wallet_ms`` the wallet adapter's ``execute`` or
    ``reconcile`` until it returned. The measured parts are whole milliseconds
    rounded down; ``waiting_ms`` is the rest of ``total_ms``, never negative: the
    wait for the agent's voucher, an earlier run's turn or an earlier wallet
    operation, and local work. No prompt, payload or identifier is included.
    """

    operation: str
    state: str | None
    total_ms: int
    coverage_ms: int
    chain_ms: int
    wallet_ms: int
    waiting_ms: int


@dataclass(frozen=True, slots=True)
class Hooks:
    on_challenge: Callable[[pb.Challenge], None] | None = None
    on_session_open: Callable[[pb.SessionOpen], None] | None = None
    on_voucher: Callable[[pb.Voucher], None] | None = None
    on_meter: Callable[[pb.Meter], None] | None = None
    on_low_credit: Callable[[pb.LowCredit], None] | None = None
    on_drain: Callable[[pb.Drain], None] | None = None
    on_settle: Callable[[OperationResult], None] | None = None
    on_close: Callable[[pb.CloseResult], None] | None = None
    on_error: Callable[[HookError], None] | None = None
    # Hooks added after 0.1.2 follow its fields, so a host that passes
    # callbacks by position keeps each one bound to its own hook.
    # The session id of a final session that left the runtime after its window.
    on_session_retired: Callable[[bytes], None] | None = None
    on_timing: Callable[[OperationTiming], None] | None = None


__all__ = ["HookError", "Hooks", "ObserverStats", "OperationTiming"]
