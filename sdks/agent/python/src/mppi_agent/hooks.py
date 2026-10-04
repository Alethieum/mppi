"""Typed, optional lifecycle observers. See the public lifecycle hook profile."""

from collections.abc import Callable
from dataclasses import dataclass

from mppi.v1 import mppi_pb2 as pb
from mppi_protocol._observers import HookError, ObserverStats


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


__all__ = ["HookError", "Hooks", "ObserverStats"]
