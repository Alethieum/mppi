"""Typed observer-family dispatch; payment validation remains in Client tests."""

from threading import Event

from mppi.v1 import mppi_pb2 as pb
from mppi_agent import HookError, Hooks
from mppi_protocol._observers import Observers


def test_agent_observer_family_delivers_owned_typed_records_in_enqueue_order():
    received, done = [], Event()
    names = (
        "challenge",
        "session_open",
        "voucher",
        "meter",
        "low_credit",
        "drain",
        "receipt",
        "error",
    )

    def observe(name):
        def callback(value):
            received.append((name, value))
            if name == "error":
                done.set()

        return callback

    hooks = Hooks(**{"on_" + name: observe(name) for name in names})
    values = (
        pb.Challenge(),
        pb.SessionOpen(),
        pb.Voucher(),
        pb.Meter(),
        pb.LowCredit(),
        pb.Drain(),
        pb.Receipt(),
        HookError("control", "UNAVAILABLE"),
    )
    observer = Observers(hooks)
    try:
        for name, value in zip(names, values, strict=True):
            observer.emit("on_" + name, value)
        assert done.wait(1)
        assert tuple(name for name, _ in received) == names
        for (_, received_value), original in zip(received, values, strict=True):
            assert received_value == original and received_value is not original
    finally:
        observer.close()
