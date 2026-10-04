"""Typed, optional lifecycle observers. See the public lifecycle hook profile."""

from collections.abc import Callable
from dataclasses import dataclass

from mppi.v1 import mppi_pb2 as pb
from mppi_protocol._observers import HookError, ObserverStats
from mppi_protocol.types import OperationResult


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


__all__ = ["HookError", "Hooks", "ObserverStats"]
