"""The SDK observer boundary cannot mutate or block payment execution."""

from dataclasses import dataclass
from threading import Event

import pytest
from mppi.v1 import mppi_pb2 as pb
from mppi_protocol._observers import Observers


@dataclass
class Callbacks:
    on_meter: object = None


def test_observer_copies_payload_and_continues_after_callback_failure():
    done = Event()
    calls = []

    def callback(value):
        calls.append(value.seq)
        value.seq = 999
        if len(calls) == 1:
            raise RuntimeError("application callback failed")
        done.set()

    observer = Observers(Callbacks(callback))
    original = pb.Meter(seq=1)
    observer.emit("on_meter", original)
    observer.emit("on_meter", pb.Meter(seq=2))
    assert done.wait(1)
    assert original.seq == 1 and calls == [1, 2]
    assert observer.stats().failed == 1
    observer.close()


def test_blocked_observer_has_count_and_byte_bounds_and_nonblocking_shutdown():
    entered, release = Event(), Event()

    def callback(value):
        entered.set()
        release.wait(2)

    observer = Observers(Callbacks(callback))
    try:
        observer.emit("on_meter", pb.Meter(seq=1))
        assert entered.wait(1)
        for n in range(65):
            observer.emit("on_meter", pb.Meter(seq=n + 2))
        assert observer.stats().queued == 64
        assert observer.stats().dropped == 1
        observer.close()
        assert observer.stats().queued == 0
        assert observer.stats().dropped == 65
    finally:
        release.set()

    observer = Observers(Callbacks(callback))
    try:
        observer.emit("on_meter", pb.Meter(session_id=b"x" * 1048577))
        assert observer.stats().dropped == 1
        assert observer.stats().queued == 0
    finally:
        observer.close()


def test_invalid_observer_callback_is_rejected_before_start():
    with pytest.raises(ValueError, match="callable"):
        Observers(Callbacks("invalid"))
