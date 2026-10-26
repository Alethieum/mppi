"""Typed, optional lifecycle observers. See the public lifecycle hook profile."""

from collections.abc import Callable
from dataclasses import dataclass

from mppi.v1 import mppi_pb2 as pb
from mppi_protocol._observers import HookError, ObserverStats


@dataclass(frozen=True, slots=True)
class OperationTiming:
    """Where one money step's time went, in milliseconds.

    ``operation`` is ``open``, ``topUp``, ``close``, ``requestClose``,
    ``withdraw`` or ``releaseIdle``; ``state`` is its result's state, or
    ``None`` when it raised. ``chain_ms`` is this SDK's own chain reads:
    snapshots, the funding check and its transaction's receipt. ``wallet_sign_ms`` is the
    wallet adapter's signing; ``wallet_submit_ms`` its execute call until it
    returned, which for a custodial wallet service includes its own
    submission and any wait for the transaction to be mined.
    ``provider_ms`` is the provider exchange (challenge, attachment, close).
    The measured parts are whole milliseconds rounded down; ``provider_ms`` is
    the rest of ``total_ms``, never negative.
    """

    operation: str
    state: str | None
    total_ms: int
    chain_ms: int
    wallet_sign_ms: int
    wallet_submit_ms: int
    provider_ms: int


@dataclass(frozen=True, slots=True)
class Hooks:
    on_challenge: Callable[[pb.Challenge], None] | None = None
    on_session_open: Callable[[pb.SessionOpen], None] | None = None
    on_voucher: Callable[[pb.Voucher], None] | None = None
    on_meter: Callable[[pb.Meter], None] | None = None
    on_low_credit: Callable[[pb.LowCredit], None] | None = None
    on_drain: Callable[[pb.Drain], None] | None = None
    on_receipt: Callable[[pb.Receipt], None] | None = None
    on_error: Callable[[HookError], None] | None = None
    on_timing: Callable[[OperationTiming], None] | None = None


__all__ = ["HookError", "Hooks", "ObserverStats", "OperationTiming"]
