"""Bounded, best-effort lifecycle observers; never a payment control path."""

from collections import deque
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, fields, is_dataclass
from inspect import iscoroutinefunction
from threading import Condition, Thread
from typing import Any


@dataclass(frozen=True, slots=True)
class ObserverStats:
    delivered: int
    dropped: int
    failed: int
    queued: int


@dataclass(frozen=True, slots=True)
class HookError:
    """Sanitized SDK boundary error; no prompt, credentials or upstream body."""

    operation: str
    code: str
    session_id: bytes | None = None
    run_id: bytes | None = None


class Observers:
    """One worker, at most 64 queued records/1 MiB, plus one running callback.

    Records are copied before enqueue. Full queues drop the newest record.
    close discards pending records and never waits for caller code. Python
    cannot terminate a blocked callback: it may retain this one daemon thread.
    """

    def __init__(self, hooks: Any) -> None:
        self._callbacks: dict[str, Callable[[Any], None]] = {}
        if hooks is not None:
            for name in hooks.__dataclass_fields__:
                value = getattr(hooks, name)
                if value is not None:
                    if (
                        not callable(value)
                        or iscoroutinefunction(value)
                        or iscoroutinefunction(type(value).__call__)
                    ):
                        raise ValueError(f"{name} must be a synchronous callable")
                    self._callbacks[name] = value
        self._condition = Condition()
        self._queue: deque[tuple[str, Any, int]] = deque()
        self._bytes = self._delivered = self._dropped = self._failed = 0
        self._started = self._closed = False

    def emit(self, name: str, value: Any) -> None:
        if name not in self._callbacks:
            return
        # Protobuf records obey the wire limit. Other records are bounded SDK
        # value objects. Do not serialize arbitrary application objects here.
        size = _size(value)
        with self._condition:
            if self._closed or len(self._queue) >= 64 or self._bytes + size > 1048576:
                self._dropped += 1
                return
            if not self._started:
                try:
                    Thread(target=self._run, name="mppi-observers", daemon=True).start()
                except RuntimeError:
                    self._failed += 1
                    return
                self._started = True
            self._queue.append((name, deepcopy(value), size))
            self._bytes += size
            self._condition.notify()

    def _run(self) -> None:
        while True:
            with self._condition:
                while not self._queue and not self._closed:
                    self._condition.wait()
                if self._closed:
                    return
                name, value, size = self._queue.popleft()
                self._bytes -= size
            try:
                self._callbacks[name](value)
            except BaseException:
                # Caller callback failures, including SystemExit, cannot affect
                # the SDK event loop, payment state or other queued callbacks.
                with self._condition:
                    self._failed += 1
            else:
                with self._condition:
                    self._delivered += 1

    def stats(self) -> ObserverStats:
        with self._condition:
            return ObserverStats(self._delivered, self._dropped, self._failed, len(self._queue))

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._dropped += len(self._queue)
            self._queue.clear()
            self._bytes = 0
            self._condition.notify_all()


def _size(value: Any) -> int:
    if hasattr(value, "ByteSize"):
        return int(value.ByteSize())
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, str):
        return len(value) * 4
    if isinstance(value, tuple):
        return sum(_size(item) for item in value)
    if is_dataclass(value) and not isinstance(value, type):
        return sum(_size(getattr(value, field.name)) for field in fields(value))
    return 32
