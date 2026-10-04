"""Normal completion EOF is not an SDK error; retained transport errors are."""

import asyncio
from contextlib import contextmanager
from threading import Event, Thread
from unittest.mock import AsyncMock

import pytest
from mppi.v1 import mppi_pb2 as pb
from mppi_agent import Client, ClientError, Hooks
from mppi_agent.client import CompletionStream, _Completions, _Output
from mppi_protocol._observers import Observers
from mppi_protocol.wire import ByteQueue


@contextmanager
def completion_client(error=None):
    """Exercise real queue, loop handoff, stream adapter and observer dispatch."""
    client = object.__new__(Client)
    client._closed = False
    client._loop = asyncio.new_event_loop()
    errors, delivered = [], Event()
    client._observers = Observers(
        Hooks(on_error=errors.append, on_receipt=lambda _: delivered.set())
    )
    client._thread = Thread(target=client._loop.run_forever)
    client._thread.start()
    identifier = b"r" * 32
    queue = ByteQueue()
    queue.put_nowait(b'{"text":"complete"}')
    queue.close(error)
    client._outputs = {identifier: _Output(queue)}
    client._create = AsyncMock(return_value=identifier)

    def observed_errors():
        client._observers.emit("on_receipt", pb.Receipt())
        assert delivered.wait(2)
        return errors

    try:
        yield client, identifier, observed_errors
    finally:
        client._observers.close()
        client._loop.call_soon_threadsafe(client._loop.stop)
        client._thread.join(timeout=2)
        assert not client._thread.is_alive()
        client._loop.close()


@pytest.mark.parametrize("streaming", [True, False])
def test_successful_completion_does_not_emit_error_hook(streaming):
    with completion_client() as (client, identifier, observed_errors):
        if streaming:
            stream = CompletionStream(client, identifier)
            assert list(stream) == [b'{"text":"complete"}']
            assert list(stream) == []
        else:
            response = _Completions(client).create(
                model="test/model", messages=[{"role": "user", "content": "hello"}], stream=False
            )
            assert response == b'{"text":"complete"}'
        assert observed_errors() == []


@pytest.mark.parametrize("error", [ClientError("transport failed"), EOFError("transport EOF")])
def test_completion_failure_remains_an_error_and_emits_hook(error):
    with completion_client(error) as (client, identifier, observed_errors):
        stream = CompletionStream(client, identifier)
        assert next(stream) == b'{"text":"complete"}'
        with pytest.raises(type(error)) as result:
            next(stream)
        assert result.value is error
        failures = observed_errors()
        assert len(failures) == 1
        assert failures[0].operation == "client"
        assert failures[0].code == type(error).__name__
