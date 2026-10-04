"""In-process wire peer for application unit tests; no chain, wallet or TLS.

Handlers supply the exact protobuf messages and errors for a test. This helper
uses the production generated services, without inventing payment acceptance,
funding, signature verification or final usage. It binds only to loopback.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from types import TracebackType
from typing import Any, Self

import grpc
from mppi.v1 import mppi_pb2 as pb
from mppi.v1 import mppi_pb2_grpc as services
from mppi_protocol.wire import GRPC_OPTIONS

type CompletionHandler = Callable[
    [pb.CompleteRequest, grpc.aio.ServicerContext[Any, Any]], AsyncIterator[pb.ChatChunk]
]
type ControlHandler = Callable[
    [AsyncIterator[pb.ControlMessage], grpc.aio.ServicerContext[Any, Any]],
    AsyncIterator[pb.ControlMessage],
]

__all__ = ["CompletionHandler", "ControlHandler", "MockProvider"]


class _Chat:
    def __init__(self, handler: CompletionHandler) -> None:
        self.handler = handler

    async def Completions(
        self, request: pb.CompleteRequest, context: grpc.aio.ServicerContext[Any, Any]
    ) -> AsyncIterator[pb.ChatChunk]:
        async for message in self.handler(request, context):
            yield message


class _Control:
    def __init__(self, handler: ControlHandler) -> None:
        self.handler = handler

    async def Session(
        self,
        requests: AsyncIterator[pb.ControlMessage],
        context: grpc.aio.ServicerContext[Any, Any],
    ) -> AsyncIterator[pb.ControlMessage]:
        async for message in self.handler(requests, context):
            yield message


class MockProvider:
    """Run scripted Chat and Control peers inside ``async with``.

    ``chat`` and ``control`` expose the generated asynchronous gRPC stubs while
    the context is active. Callers should set RPC deadlines in their tests.
    A fresh context owns its server/channel, and exit cancels unfinished RPCs.
    Use a new instance per context. No network host is configurable: this peer
    is deliberately limited to unauthenticated loopback unit tests.
    """

    def __init__(self, *, completions: CompletionHandler, control: ControlHandler) -> None:
        self._completions = completions
        self._control = control
        self._server: grpc.aio.Server | None = None
        self._channel: grpc.aio.Channel | None = None
        self._used = False

    @property
    def chat(self) -> services.ChatStub:
        if self._channel is None:
            raise RuntimeError("MockProvider must be used inside async with")
        return services.ChatStub(self._channel)

    @property
    def control(self) -> services.ControlStub:
        if self._channel is None:
            raise RuntimeError("MockProvider must be used inside async with")
        return services.ControlStub(self._channel)

    async def __aenter__(self) -> Self:
        if self._used:
            raise RuntimeError("use a new MockProvider instance for each context")
        self._used = True
        server = grpc.aio.server(options=GRPC_OPTIONS)
        self._server = server
        services.add_ChatServicer_to_server(_Chat(self._completions), server)
        services.add_ControlServicer_to_server(_Control(self._control), server)
        try:
            port = server.add_insecure_port("127.0.0.1:0")
            if port == 0:
                raise RuntimeError("MockProvider could not bind its loopback port")
            await server.start()
            self._channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}", options=GRPC_OPTIONS)
            return self
        except BaseException:
            await server.stop(0)
            self._server = None
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        channel, server = self._channel, self._server
        self._channel = self._server = None
        try:
            if channel is not None:
                await channel.close()
        finally:
            if server is not None:
                await server.stop(0)
