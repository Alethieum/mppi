import asyncio

import grpc
import pytest
from google.protobuf import any_pb2
from google.rpc import status_pb2
from mppi.v1 import mppi_pb2 as pb
from mppi_protocol.errors import ValidationError
from mppi_protocol.types import PauseLevel, PaymentCode, PaymentFailure
from mppi_protocol.wire import (
    ByteQueue,
    parse_payment_error,
    payment_status,
    validate_json,
    validate_message,
)


@pytest.mark.parametrize(
    "payload",
    [
        b"{}\xef\xbb\xbf",
        b"\xef\xbb\xbf{}",
        b'{"a":1,"\\u0061":2}',
        b'{"x":NaN}',
        b'{"x":"\\ud800"}',
        b"[]",
        b'{"x":\xff}',
        b'{"a":' + b"[" * 128 + b"0" + b"]" * 128 + b"}",
    ],
)
def test_invalid_json_rejected(payload):
    with pytest.raises(ValidationError):
        validate_json(payload)


def test_numeric_text_is_not_coerced_and_envelope_is_bound():
    raw = b'{"model":"example/model","stream":true,"seed":9007199254740993,"n":1e10000000000}'
    value = validate_json(raw, model="example/model", stream=True)
    assert value["seed"] == "9007199254740993" and value["n"] == "1e10000000000"
    with pytest.raises(ValidationError):
        validate_json(raw, model="example/other", stream=True)
    with pytest.raises(ValidationError):
        validate_json(raw, model="example/model", stream=False)


def test_required_presence_direction_independent_width_and_enum():
    for message in [
        pb.ControlMessage(),
        pb.Meter(session_id=b"s" * 32),
        pb.ReconnectChallenge(nonce=b"x"),
        pb.Drain(session_id=b"s" * 32, reason=99),
    ]:
        with pytest.raises(ValidationError):
            validate_message(message)
    validate_message(pb.ReconnectChallenge(nonce=b"n" * 32))
    with pytest.raises(ValidationError):
        validate_message(pb.UpstreamError(body=b"{}", http_status=200))


@pytest.mark.parametrize(
    "failure",
    [
        PaymentFailure(PaymentCode.INSUFFICIENT_DEPOSIT, session_id=b"s" * 32, run_id=b"r" * 32),
        PaymentFailure(
            PaymentCode.SESSION_NOT_READY, "CONTRACT_PAUSED", pause_level=PauseLevel.PAYMENTS
        ),
        PaymentFailure(
            PaymentCode.SESSION_NOT_READY,
            "PRICE_CHANGED",
            requested_rate_card_hash=b"a" * 32,
            current_rate_card_hash=b"b" * 32,
        ),
    ],
)
def test_payment_errors_round_trip_with_exact_status(failure):
    status, _, metadata = payment_status(failure)
    assert parse_payment_error(status, metadata) == failure
    with pytest.raises(ValidationError):
        parse_payment_error(grpc.StatusCode.OK, metadata)
    with pytest.raises(ValidationError):
        parse_payment_error(status, metadata + metadata)


def test_wrong_any_and_oversized_error_are_not_payment_evidence():
    data = status_pb2.Status(
        code=9, details=[any_pb2.Any(type_url="wrong", value=b"")]
    ).SerializeToString()
    with pytest.raises(ValidationError):
        parse_payment_error(
            grpc.StatusCode.FAILED_PRECONDITION, (("grpc-status-details-bin", data),)
        )
    _, _, metadata = payment_status(PaymentFailure(PaymentCode.SESSION_NOT_READY, "x" * 3000))
    assert metadata == ()


def test_queue_full_byte_limit_timeout_cancellation_and_close():
    async def run():
        queue = ByteQueue(max_messages=2, max_bytes=4, wait_ms=10)
        queue.put_nowait(b"1234")
        with pytest.raises(asyncio.QueueFull):
            queue.put_nowait(b"x")
        with pytest.raises(TimeoutError):
            await queue.put(b"x")
        blocked = asyncio.create_task(queue.put(b"y"))
        await asyncio.sleep(0)
        blocked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await blocked
        assert await queue.get() == b"1234"
        queue.put_nowait(b"z")
        queue.close(RuntimeError("failed"))
        assert await queue.get() == b"z"
        with pytest.raises(RuntimeError, match="failed"):
            await queue.get()
        with pytest.raises(EOFError):
            queue.put_nowait(b"")
        normal = ByteQueue()
        waiting = asyncio.create_task(normal.get())
        await asyncio.sleep(0)
        normal.close()
        with pytest.raises(EOFError):
            await waiting

    asyncio.run(run())
