"""Packaged application-test helper: actual loopback gRPC, no chain acceptance."""

import asyncio

import grpc
import pytest
from mppi.v1 import mppi_pb2 as pb
from mppi_agent.testing import MockProvider


def test_mock_provider_preserves_wire_bytes_and_interactive_control():
    async def scenario():
        original = b'{"value":9007199254740993,"answer":"Inference successful"}'
        received = []

        async def complete(request, context):
            received.append(request)
            yield pb.ChatChunk(payload=original)
            yield pb.ChatChunk(
                terminal=pb.Terminal(
                    session_id=request.session_id,
                    run_id=request.run_id,
                    model=request.model,
                    rate_card_hash=request.rate_card_hash,
                    input_tokens=0,
                    cache_tokens=0,
                    output_tokens=0,
                    amount=0,
                    status=pb.RUN_STATUS_COMPLETED,
                    final_meter_seq=0,
                )
            )

        async def control(requests, context):
            challenge = await anext(requests)
            received.append(challenge)
            yield pb.ControlMessage(reconnect_challenge=pb.ReconnectChallenge(nonce=b"n" * 32))
            received.append(await anext(requests))

        peer = MockProvider(completions=complete, control=control)
        with pytest.raises(RuntimeError, match="async with"):
            _ = peer.chat
        async with peer:
            request = pb.CompleteRequest(
                model="test/model",
                stream=True,
                payload=original,
                session_id=b"s" * 32,
                run_id=b"r" * 32,
                rate_card_hash=b"h" * 32,
            )
            chunks = [item async for item in peer.chat.Completions(request, timeout=2)]
            assert [chunk.WhichOneof("kind") for chunk in chunks] == ["payload", "terminal"]
            assert chunks[0].payload == original and chunks[1].terminal.run_id == request.run_id
            assert received[0] == request and received[0] is not request

            call = peer.control.Session(timeout=2)
            opening = pb.ControlMessage(session_open=pb.SessionOpen(session_id=b"s" * 32))
            await call.write(opening)
            nonce = await call.read()
            assert nonce.reconnect_challenge.nonce == b"n" * 32
            voucher = pb.ControlMessage(voucher=pb.Voucher(session_id=b"s" * 32, cumulative=10))
            await call.write(voucher)
            await call.done_writing()
            assert await call.read() is grpc.aio.EOF
            assert received[1:] == [opening, voucher]
        with pytest.raises(RuntimeError, match="async with"):
            _ = peer.control
        with pytest.raises(RuntimeError, match="new MockProvider"):
            async with peer:
                pytest.fail("must not reuse a closed peer")

    asyncio.run(scenario())


def test_mock_provider_preserves_rpc_errors_and_closes_unfinished_calls():
    async def scenario():
        active = asyncio.Event()
        cancelled = asyncio.Event()

        async def complete(request, context):
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "scripted rejection")
            yield pb.ChatChunk()  # pragma: no cover; defines an async generator

        async def control(requests, context):
            active.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
            yield pb.ControlMessage()  # pragma: no cover

        async with MockProvider(completions=complete, control=control) as peer:
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await peer.chat.Completions(pb.CompleteRequest(), timeout=2).read()
            assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
            assert error.value.details() == "scripted rejection"
            call = peer.control.Session(timeout=2)
            await call.write(pb.ControlMessage())
            await asyncio.wait_for(active.wait(), 2)
        assert call.done()
        await asyncio.wait_for(cancelled.wait(), 2)

    asyncio.run(scenario())
