"""Provider-only native gRPC tests driven by an independent protobuf peer.

No agent SDK is imported. Chain/wallet fixtures supply explicit verified facts
for payment-state unit tests; they do not establish live contract integration.
Provider execution supplies every count and final outcome through public hooks.
"""

import asyncio
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import grpc
import pytest
from mppi.v1 import mppi_pb2 as pb
from mppi.v1 import mppi_pb2_grpc as stubs
from mppi_protocol import RunAccounting, TokenCounts
from mppi_protocol.accounting import RunStatus
from mppi_protocol.catalog import CatalogRow, publish_catalog
from mppi_protocol.chain import (
    Block,
    ChainEvidenceError,
    ChainReader,
    ChainSession,
    ClosingEvidence,
    OpeningEvidence,
)
from mppi_protocol.contract import ARC_USDC, ContractCalls
from mppi_protocol.discovery import ProviderIdentity
from mppi_protocol.errors import ValidationError
from mppi_protocol.follower import ChainSnapshot
from mppi_protocol.signing import LocalSessionSigner, session_id
from mppi_protocol.types import (
    ChainObservation,
    Domain,
    OperationResult,
    OperationState,
    PauseLevel,
    PaymentCode,
    PaymentFailure,
    Readiness,
    RunContext,
    SessionDescriptor,
    SessionOperationOutcome,
    SessionStatus,
)
from mppi_protocol.wire import (
    GRPC_OPTIONS,
    parse_payment_error,
    validate_message,
    validate_recovery_snapshot,
)
from mppi_provider import runtime
from mppi_provider.config import ProviderConfig
from mppi_provider.interfaces import RecoveryOperation, RecoveryRun
from mppi_provider.runtime import Mppi, UpstreamFailure, _RpcFailure

DOMAIN = Domain(5042002, b"\x99" * 20)
AGENT = b"\x11" * 20
OPERATOR = b"\x33" * 20
RECIPIENT = b"\x22" * 20
# Public, insecure conformance key. Never used on a public chain.
SIGNER = LocalSessionSigner((1).to_bytes(32, "big"))
OTHER = LocalSessionSigner((2).to_bytes(32, "big"))
DESCRIPTOR = SessionDescriptor(
    AGENT, SIGNER.address, 42, RECIPIENT, OPERATOR, ARC_USDC, b"\x44" * 32
)
SID = session_id(DOMAIN, DESCRIPTOR)
OPEN_TX = b"\x55" * 32


def config(*, wait=200, buffer=1000, input_rate=1000000, output_rate=1000000):
    return ProviderConfig.from_mapping(
        {
            "provider": {
                "name": "Provider",
                "description": "Owned counts",
                "public_origin": "https://provider.example",
                "recipient": "0x" + RECIPIENT.hex(),
            },
            "identity": {"provider_id": "42", "wallet_address": "0x" + OPERATOR.hex()},
            "chain": {"network": "testnet", "max_age_ms": 10000},
            "upstream": {"url": "http://private.example/v1"},
            "models": [
                {
                    "model": "example/model",
                    "description": "Text",
                    "context_window": 10000,
                    "max_output": 1000,
                    "modalities": ["text"],
                    "embeddings": False,
                    "streaming": True,
                    "tools": False,
                    "structured_output": False,
                    "rates": {"input": str(input_rate), "output": str(output_rate)},
                }
            ],
            "payment": {
                "output_buffer_tokens": buffer,
                "collection_margin_ms": 1000,
                "authorization_wait_ms": wait,
            },
            "settlement": {"interval_ms": 1000000},
        }
    )


@pytest.fixture
def bounded_control_timeout(monkeypatch):
    original = Mppi.__init__

    def bounded(self, *args, **kwargs):
        original(self, *args, **kwargs)
        # Accelerate the server's local setup policy, independently of client
        # metadata and Complete's authorization wait. The original vulnerable
        # runtime ignores this field and demonstrably retains the native RPC.
        self._control_auth_timeout = 0.05

    monkeypatch.setattr(Mppi, "__init__", bounded)


@pytest.mark.parametrize("client_timeout", (None, 1_000_000))
def test_unauthenticated_control_has_server_deadline(bounded_control_timeout, client_timeout):
    async def check():
        async with serving() as peer:
            call = stubs.ControlStub(peer.channel).Session(timeout=client_timeout)
            try:
                nonce = await asyncio.wait_for(call.read(), 1)
                assert nonce.WhichOneof("kind") == "reconnect_challenge"
                with pytest.raises(grpc.aio.AioRpcError) as failure:
                    await asyncio.wait_for(call.read(), 0.5)
                assert failure.value.code() is grpc.StatusCode.DEADLINE_EXCEEDED
                assert not peer.work.calls
                assert not peer.wallet.calls
            finally:
                call.cancel()

    asyncio.run(check())


def test_authenticated_control_outlives_setup_deadline(bounded_control_timeout):
    async def check():
        async with serving() as peer:
            assert (await peer.attach()).ready
            await asyncio.sleep(0.15)
            assert (await peer.guidance()).model == "example/model"
            assert not peer.work.calls
            assert not peer.wallet.calls

    asyncio.run(check())


def test_unconsumed_control_does_not_retain_unauthenticated_reader(bounded_control_timeout):
    async def check():
        async with serving() as peer:
            call = stubs.ControlStub(peer.channel).Session()
            try:
                # Leave this response unread while both client stream objects
                # remain alive. Reader cleanup must be driven by the server.
                await asyncio.sleep(0.15)
                readers = [
                    task
                    for task in asyncio.all_tasks()
                    if task.get_coro().__qualname__.endswith("._receive_control")
                ]
                assert not readers, "unauthenticated readers survived their server setup deadline"
            finally:
                call.cancel()

    asyncio.run(check())


@pytest.mark.parametrize("value", (True, False, 0, -1, float("inf"), float("nan"), 3600.01, "60"))
def test_control_setup_policy_rejects_invalid_timeouts(value):
    async def check():
        with pytest.raises(ValidationError):
            async with serving(control_auth_timeout=value):
                pytest.fail("invalid local policy started a listener")

    asyncio.run(check())


def test_control_setup_policy_default_and_maximum_are_explicit():
    async def check():
        async with serving() as peer:
            assert peer.provider._control_auth_timeout == 60
        async with serving(control_auth_timeout=3600) as peer:
            assert peer.provider._control_auth_timeout == 3600

    asyncio.run(check())


def test_control_guidance_cannot_reset_server_setup_deadline(bounded_control_timeout):
    async def check():
        async with serving() as peer:
            assert (await peer.guidance()).model == "example/model"
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await asyncio.wait_for(peer.control.read(), 0.5)
            assert failure.value.code() is grpc.StatusCode.DEADLINE_EXCEEDED
            assert not peer.work.calls and not peer.wallet.calls

    asyncio.run(check())


def test_control_setup_timeout_cancels_blocked_opening_lookup(bounded_control_timeout, monkeypatch):
    async def check():
        async with serving() as peer:
            entered = threading.Event()
            release = threading.Event()
            original = peer.facts.reader.opening

            def blocked(*args, **kwargs):
                entered.set()
                assert release.wait(1)
                return original(*args, **kwargs)

            monkeypatch.setattr(peer.facts.reader, "opening", blocked)
            try:
                await peer.control.write(
                    pb.ControlMessage(session_open=pb.SessionOpen(session_id=SID, open_tx=OPEN_TX))
                )
                with pytest.raises(grpc.aio.AioRpcError) as failure:
                    await asyncio.wait_for(peer.control.read(), 0.5)
                assert entered.is_set()
                assert failure.value.code() is grpc.StatusCode.DEADLINE_EXCEEDED
                assert not peer.provider._sessions
            finally:
                release.set()

    asyncio.run(check())


@pytest.mark.parametrize("expires_during_snapshot", (False, True))
def test_expired_control_cannot_commit_ready_signed_attachment(
    expires_during_snapshot, monkeypatch
):
    async def check():
        async with serving() as peer:
            await peer.attach(voucher=2000)
            session = peer.provider._sessions[SID]
            owner = session.owner
            candidate = runtime._Attachment(runtime.ByteQueue(), setup_deadline=100.0)
            request = pb.ReconnectRequest(
                session_id=SID,
                nonce=candidate.nonce,
                signature=SIGNER.sign_reconnect(DOMAIN, SID, candidate.nonce),
                last_meter_seq=0,
            )
            clock = SimpleNamespace(now=99.0 if expires_during_snapshot else 100.0)
            original = peer.provider._session_state

            def snapshot(*args, **kwargs):
                result = original(*args, **kwargs)
                clock.now = 100.0
                return result

            patched_asyncio = dict(vars(asyncio))
            patched_asyncio["get_running_loop"] = lambda: SimpleNamespace(time=lambda: clock.now)
            with monkeypatch.context() as patch:
                patch.setattr(runtime, "asyncio", SimpleNamespace(**patched_asyncio))
                patch.setattr(peer.provider, "_session_state", snapshot)
                with pytest.raises(_RpcFailure) as failure:
                    await peer.provider._attach(candidate, request)
            assert failure.value.code is grpc.StatusCode.DEADLINE_EXCEEDED
            assert session.owner is owner and owner.live and session.generation == 1
            assert candidate.session is None and candidate.request is None
            assert candidate.setup_deadline == 100.0

    asyncio.run(check())


class Facts:
    def __init__(self, config, *, deposit=100000):
        self.config = config
        self.now = int(time.time())
        self.stale = False
        self.stopped = False
        self.pause = PauseLevel.NONE
        self.max_age_ms = config.chain.max_age_ms
        self.collection_margin_ms = config.payment.collection_margin_ms
        self.identity = ProviderIdentity(
            self.block(100).observation,
            b"\x88" * 20,
            42,
            OPERATOR,
            RECIPIENT,
            "https://provider.example/discover",
            config.catalog,
        )
        self.state = ChainSession(
            deposit, 0, SessionStatus.OPEN, 0, self.now, config.catalog.rate_card_hash
        )
        self.watched = False
        self.height = 100
        self.opening_block = 101
        self.operations = []
        self.reader = SimpleNamespace(
            profile=SimpleNamespace(domain=DOMAIN, instant_finality=True),  # Arc.
            opening=self.opening,
            agent_wallet=lambda *_: AGENT,
            reconcile=self.reconcile,
            closing=self.closing,
        )

    def block(self, number):
        return Block(
            ChainObservation(
                DOMAIN.chain_id, DOMAIN.verifying_contract, number, number.to_bytes(32, "big")
            ),
            self.now,
            (number - 1).to_bytes(32, "big"),
        )

    @property
    def current(self):
        return ChainSnapshot(
            self.block(self.height),
            self.pause,
            {42: self.identity},
            {SID: self.state} if self.watched else {},
            time.monotonic(),
        )

    def snapshot(self):
        if self.stale:
            raise ChainEvidenceError("test source unavailable")
        return self.current

    async def start(self):
        pass

    async def stop(self):
        self.stopped = True

    async def watch_provider(self, provider_id):
        assert provider_id == 42
        return self.identity

    async def watch_session(self, sid):
        assert sid == SID
        self.watched = True
        self.height = max(self.height, 101)
        return self.state

    async def refresh(self):
        return self.snapshot()

    def estimated_timestamp(self):
        return self.now

    def readiness(self, sid=None):
        failure = None
        if self.stale:
            failure = PaymentFailure(PaymentCode.SESSION_NOT_READY, "CHAIN_STATE_STALE", sid)
        elif self.pause is not PauseLevel.NONE:
            failure = PaymentFailure(
                PaymentCode.SESSION_NOT_READY, "CONTRACT_PAUSED", sid, pause_level=self.pause
            )
        elif sid is not None:
            if self.state.status is SessionStatus.CLOSED:
                failure = PaymentFailure(PaymentCode.SESSION_NOT_READY, "SESSION_CLOSED", sid)
            elif self.now + 1 >= self.state.last_activity + 86400:
                failure = PaymentFailure(PaymentCode.SESSION_NOT_READY, "CLAIM_DEADLINE", sid)
        return Readiness(failure is None, () if failure is None else (failure,))

    def opening(self, tx, sid, *, accepted_catalog=None):
        assert tx == OPEN_TX and sid == SID
        if accepted_catalog is not None:
            assert accepted_catalog.rate_card_hash == self.config.catalog.rate_card_hash
        return OpeningEvidence(
            DESCRIPTOR,
            self.block(self.opening_block).observation,
            self.state.deposit,
            self.config.catalog,
            OPEN_TX,
        )

    def reconcile(self, operation, *, operation_ref, tx_hash, wallet_operation_id=None):
        self.operations.append(operation)
        target = operation.amounts[0]
        self.state = replace(
            self.state,
            settled_cumulative=target,
            status=SessionStatus.CLOSED if operation.kind.value == "close" else self.state.status,
        )
        return OperationResult(
            operation_ref,
            OperationState.CONFIRMED,
            wallet_operation_id,
            tx_hash,
            self.block(self.height).observation,
        )

    def closing(self, tx, sid):
        assert sid == SID and self.state.status is SessionStatus.CLOSED
        return ClosingEvidence(
            self.block(self.height).observation,
            self.state.settled_cumulative,
            self.state.deposit - self.state.settled_cumulative,
            tx,
        )


class Wallet:
    address = OPERATOR

    def __init__(self):
        self.calls = []
        self.lookups = []

    def execute(self, call, *, operation_ref):
        self.calls.append((call, operation_ref))
        return OperationResult(operation_ref, OperationState.PENDING, "wallet-123", b"\xaa" * 32)

    def reconcile(self, *, operation_ref):
        self.lookups.append(operation_ref)
        return OperationResult(operation_ref, OperationState.PENDING, "wallet-123", b"\xaa" * 32)


class Work:
    def __init__(self, *, hold=False, final=True, error=False):
        self.calls = []
        self.preparations = []
        self.release = asyncio.Event()
        self.started = asyncio.Event()
        self.stop_seen = asyncio.Event()
        self.hold, self.final, self.error = hold, final, error
        self.context = None
        self.reports = None
        self.stop = None
        self.final_counts = TokenCounts(10, 0, 10)

    async def coverage(self, request, context):
        self.preparations.append(context)
        return TokenCounts(10, 0, 1000)

    async def execute(self, request, context, reports, stop):
        self.calls.append(context)
        self.context, self.reports, self.stop = context, reports, stop
        self.started.set()
        await reports.report_usage(context, TokenCounts(10, 0, 0))
        yield b'{"choices":[],"large":9007199254740993}'
        if self.hold:
            await self.release.wait()
        if stop.is_set():
            self.stop_seen.set()
        if self.final:
            status = (
                RunStatus.FAILED
                if self.error
                else RunStatus.CANCELLED
                if stop.is_set()
                else RunStatus.COMPLETED
            )
            await reports.report_terminal(context, self.final_counts, status)
        if self.error:
            raise UpstreamFailure(b'{"error":{"message":"upstream capacity"}}', http_status=503)


@asynccontextmanager
async def serving(*, work=None, cfg=None, deposit=100000, control_auth_timeout=60.0, hooks=None):
    cfg = cfg or config()
    facts, wallet, work = Facts(cfg, deposit=deposit), Wallet(), work or Work()
    provider = Mppi(
        cfg,
        chain=facts,
        contract=ContractCalls(DOMAIN, "USDC", "2"),
        wallet=wallet,
        coverage=work.coverage,
        execution=work.execute,
        control_auth_timeout=control_auth_timeout,
        hooks=hooks,
    )
    assert provider.discovery("/discover") is None
    server = grpc.aio.server(options=GRPC_OPTIONS)
    provider.register_grpc(server)
    port = server.add_insecure_port("127.0.0.1:0")
    assert (await provider.start()).ready
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}", options=GRPC_OPTIONS)
    peer = Peer(channel, provider, facts, wallet, work)
    peer.target = f"127.0.0.1:{port}"
    try:
        await peer.start()
        yield peer
    finally:
        work.release.set()
        peer.control.cancel()
        await channel.close()
        await provider.shutdown(asyncio.get_running_loop().time() + 0.2)
        await server.stop(None)


class Peer:
    def __init__(self, channel, provider, facts, wallet, work):
        self.chat = stubs.ChatStub(channel)
        self.channel = channel
        self.control = stubs.ControlStub(channel).Session()
        self.provider, self.facts, self.wallet, self.work = provider, facts, wallet, work
        self.generation = 0

    async def start(self):
        self.nonce = (await asyncio.wait_for(self.control.read(), 1)).reconnect_challenge.nonce

    async def guidance(self, rid=b"\x01" * 32, model="example/model"):
        await self.control.write(
            pb.ControlMessage(challenge_request=pb.ChallengeRequest(request_id=rid, model=model))
        )
        return (await asyncio.wait_for(self.control.read(), 1)).challenge

    async def attach(self, *, voucher=None, signer=SIGNER):
        await self.control.write(
            pb.ControlMessage(session_open=pb.SessionOpen(session_id=SID, open_tx=OPEN_TX))
        )
        request = pb.ReconnectRequest(
            session_id=SID,
            nonce=self.nonce,
            signature=signer.sign_reconnect(DOMAIN, SID, self.nonce),
            last_meter_seq=0,
        )
        if voucher is not None:
            request.latest_voucher.CopyFrom(self.voucher(voucher))
        await self.control.write(pb.ControlMessage(reconnect_request=request))
        state = (await asyncio.wait_for(self.control.read(), 1)).session_state
        self.generation = state.control_generation
        return state

    @staticmethod
    def voucher(amount, signer=SIGNER):
        return pb.Voucher(
            session_id=SID, cumulative=amount, signature=signer.sign_voucher(DOMAIN, SID, amount)
        )

    def request(self, rid=b"\x02" * 32, *, signer=SIGNER, generation=None, rate_hash=None):
        context = RunContext(
            SID,
            rid,
            self.generation if generation is None else generation,
            "example/model",
            rate_hash or self.facts.config.catalog.rate_card_hash,
        )
        return pb.CompleteRequest(
            model=context.model,
            stream=True,
            session_id=SID,
            run_id=rid,
            control_generation=context.control_generation,
            rate_card_hash=context.rate_card_hash,
            auth_signature=signer.sign_run(DOMAIN, context),
            payload=b'{"model":"example/model","stream":true,"messages":[],"integer":9007199254740993}',
        )

    async def complete(self, request=None):
        return [
            message async for message in self.chat.Completions(request or self.request(), timeout=2)
        ]

    async def next_kind(self, kind):
        while True:
            message = await asyncio.wait_for(self.control.read(), 1)
            if message.WhichOneof("kind") == kind:
                return getattr(message, kind)


def test_unpaid_setup_is_correlated_and_does_not_execute_or_fund():
    async def scenario():
        async with serving() as peer:
            first = await peer.guidance()
            assert first.model == "example/model" and first.provider_id == (42).to_bytes(32, "big")
            assert await peer.guidance() == first
            assert (
                peer.work.preparations == [] and peer.work.calls == [] and peer.wallet.calls == []
            )
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await peer.guidance(model="other/model")
            assert failure.value.code() is grpc.StatusCode.INVALID_ARGUMENT

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("model", "status"),
    (("bad", grpc.StatusCode.INVALID_ARGUMENT), ("other/model", grpc.StatusCode.NOT_FOUND)),
)
def test_setup_model_errors_are_native_without_payment_detail(model, status):
    async def scenario():
        async with serving() as peer:
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await peer.guidance(model=model)
            assert failure.value.code() is status
            assert parse_payment_error(status, failure.value.trailing_metadata()) is None
            assert peer.work.calls == []

    asyncio.run(scenario())


def test_pending_complete_uses_control_voucher_and_definitive_finals_once():
    async def scenario():
        async with serving() as peer:
            state = await peer.attach()
            assert state.ready and not state.HasField("accepted_voucher")
            completion = asyncio.create_task(peer.complete())
            guidance = (await asyncio.wait_for(peer.control.read(), 1)).low_credit
            assert guidance.coverage_request.run_id == b"\x02" * 32
            assert [
                (row.input_tokens, row.output_tokens)
                for row in guidance.coverage_request.required_by_price
            ] == [(10, 1000)]
            assert peer.work.calls == []
            await peer.control.write(pb.ControlMessage(voucher=peer.voucher(1010)))
            chunks = await completion
            assert chunks[0].payload == b'{"choices":[],"large":9007199254740993}'
            terminal = chunks[-1].terminal
            assert terminal.status == pb.RUN_STATUS_COMPLETED and terminal.amount == 20
            final_receipt = None
            while final_receipt is None:
                message = await asyncio.wait_for(peer.control.read(), 1)
                if message.WhichOneof("kind") == "receipt":
                    final_receipt = message.receipt
            assert final_receipt.final_meter_seq == terminal.final_meter_seq
            assert final_receipt.amount == terminal.amount == 20
            assert (await peer.provider.sessions.get(SID)).usage_amount == 20
            assert len(peer.work.calls) == 1
            duplicate = await peer.provider.runs.report_terminal(
                peer.work.context, TokenCounts(10, 0, 10), RunStatus.COMPLETED
            )
            assert duplicate.accounting.duplicate
            assert duplicate.accounting.meter_seq == terminal.final_meter_seq

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ("signature", "generation", "price", "payload"))
def test_invalid_complete_never_reaches_preparation(kind):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=2000)
            request = peer.request(
                signer=OTHER if kind == "signature" else SIGNER,
                generation=2 if kind == "generation" else 1,
                rate_hash=b"\xfe" * 32 if kind == "price" else None,
            )
            if kind == "payload":
                request.payload = b'{"model":"other/model","stream":true}'
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await peer.complete(request)
            expected = (
                grpc.StatusCode.UNAUTHENTICATED
                if kind == "signature"
                else grpc.StatusCode.INVALID_ARGUMENT
                if kind == "payload"
                else grpc.StatusCode.FAILED_PRECONDITION
            )
            assert failure.value.code() is expected
            assert peer.work.preparations == [] and peer.work.calls == []

    asyncio.run(scenario())


def test_invalid_attachment_cannot_establish_generation():
    async def scenario():
        async with serving() as peer:
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await peer.attach(signer=OTHER)
            assert failure.value.code() is grpc.StatusCode.UNAUTHENTICATED
            assert peer.provider._sessions[SID].generation == 0
            assert peer.work.calls == []

    asyncio.run(scenario())


def test_verified_duplicate_vouchers_are_noops_but_invalid_old_proof_is_not():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=2000)
            for amount in (2000, 1000):
                await peer.control.write(pb.ControlMessage(voucher=peer.voucher(amount)))
            await peer.guidance()
            assert (await peer.provider.sessions.get(SID)).accepted_authorization == 2000
            await peer.control.write(pb.ControlMessage(voucher=peer.voucher(1000, OTHER)))
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await peer.control.read()
            assert failure.value.code() is grpc.StatusCode.UNAUTHENTICATED

    asyncio.run(scenario())


def test_authorization_timeout_keeps_no_candidate_reservation_and_no_replay():
    async def scenario():
        async with serving(cfg=config(wait=40)) as peer:
            await peer.attach()
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await peer.complete()
            detail = parse_payment_error(failure.value.code(), failure.value.trailing_metadata())
            assert detail.reason == "AUTHORIZATION_REQUIRED"
            run = peer.provider._sessions[SID].runs[b"\x02" * 32]
            assert run.not_admitted and not run.admitted and run.accounting.remaining_numerator == 0
            await peer.control.write(pb.ControlMessage(voucher=peer.voucher(2000)))
            with pytest.raises(grpc.aio.AioRpcError) as repeated:
                await peer.complete()
            assert repeated.value.code() is grpc.StatusCode.ALREADY_EXISTS
            assert peer.work.calls == []

    asyncio.run(scenario())


def test_concurrent_runs_cannot_reuse_reserved_deposit():
    async def scenario():
        async with serving(work=Work(hold=True), deposit=1010) as peer:
            await peer.attach(voucher=1010)
            first = asyncio.create_task(peer.complete())
            await asyncio.wait_for(peer.work.started.wait(), 1)
            with pytest.raises(grpc.aio.AioRpcError) as second:
                await peer.complete(peer.request(b"\x03" * 32))
            assert second.value.code() is grpc.StatusCode.RESOURCE_EXHAUSTED
            detail = parse_payment_error(second.value.code(), second.value.trailing_metadata())
            assert detail.code is PaymentCode.INSUFFICIENT_DEPOSIT
            assert len(peer.work.calls) == 1
            peer.work.release.set()
            assert (await first)[-1].terminal.status == pb.RUN_STATUS_COMPLETED

    asyncio.run(scenario())


class SteadyLoad(Work):
    """The first run needs ``first`` output tokens; every later one needs 1,000."""

    def __init__(self, *, first=3000, **options):
        super().__init__(**options)
        self.first, self.admitted_at = first, {}

    async def coverage(self, request, context):
        self.preparations.append(context)
        return TokenCounts(10, 0, self.first if context.run_id == b"\x01" * 32 else 1000)

    async def execute(self, request, context, reports, stop):
        self.admitted_at[context.run_id] = asyncio.get_running_loop().time()
        async for chunk in super().execute(request, context, reports, stop):
            yield chunk


def test_concurrent_runs_are_admitted_in_the_order_they_first_waited_under_steady_load():
    """A new run arrives every round trip while the first, larger one waits.

    The agent answers each coverage request one round trip later, signing
    exactly its target. Admitted runs hold their reservations. A later run
    admitted on headroom the first run is waiting for would raise that run's
    target again, so it would chase it while runs keep arriving.
    """
    rtt = 0.1

    async def scenario():
        work = SteadyLoad(hold=True)
        work.final_counts = TokenCounts(10, 0, 10)
        cfg = config(wait=5000, input_rate=0, output_rate=1000)  # 1,000 output tokens: 1 unit
        async with serving(work=work, cfg=cfg) as peer:
            await peer.attach(voucher=1)
            signed, asked, writing = [1], {}, asyncio.Lock()

            async def answer(target):
                await asyncio.sleep(rtt)
                async with writing:
                    if target > signed[0]:
                        signed[0] = target
                        await peer.control.write(pb.ControlMessage(voucher=peer.voucher(target)))

            async def agent():
                replies = set()
                while True:
                    message = await peer.control.read()
                    if message.WhichOneof("kind") != "low_credit":
                        continue
                    request = message.low_credit.coverage_request
                    asked[request.run_id] = asked.get(request.run_id, 0) + 1
                    tokens = sum(row.output_tokens for row in request.required_by_price)
                    reply = asyncio.create_task(answer(-(-tokens // 1000)))
                    replies.add(reply)
                    reply.add_done_callback(replies.discard)

            async def arrive(rid, delay):
                await asyncio.sleep(delay)
                return await peer.complete(peer.request(rid))

            responder = asyncio.create_task(agent())
            ids = [bytes([n]) * 32 for n in range(1, 10)]
            calls = [
                asyncio.create_task(arrive(rid, 0 if n == 0 else 0.01 + rtt * (n - 1)))
                for n, rid in enumerate(ids)
            ]
            async with asyncio.timeout(1.8):
                while len(work.calls) < len(ids):
                    await asyncio.sleep(0.01)
            assert [context.run_id for context in work.calls] == ids
            assert asked[ids[0]] == 1  # Its target never moved.
            work.release.set()
            for call in calls:
                assert (await call)[-1].terminal.status == pb.RUN_STATUS_COMPLETED
            responder.cancel()

    asyncio.run(scenario())


def test_a_covered_run_behind_a_silent_head_is_admitted_within_its_turn():
    """Nobody answers the first run's coverage request, as when an agent will
    not sign it. Later runs the voucher covers must not wait on it for long, and
    must never fail for it. A 1 s authorization wait gives a 0.5 s turn.
    """

    async def scenario():
        work = SteadyLoad(first=9000, hold=True)
        work.final_counts = TokenCounts(10, 0, 10)
        cfg = config(wait=1000, input_rate=0, output_rate=1000)  # 1,000 output tokens: 1 unit
        async with serving(work=work, cfg=cfg) as peer:
            await peer.attach(voucher=5)
            loop = asyncio.get_running_loop()
            start = loop.time()

            async def arrive(rid, delay):
                await asyncio.sleep(delay)
                arrival = loop.time()
                try:
                    return arrival, await peer.complete(peer.request(rid))
                except grpc.aio.AioRpcError as error:
                    return arrival, error

            ids = [bytes([n]) * 32 for n in range(1, 6)]
            calls = [
                asyncio.create_task(arrive(rid, delay))
                for rid, delay in zip(ids, (0, 0.001, 0.002, 0.2, 0.8), strict=True)
            ]
            async with asyncio.timeout(1.8):
                while len(work.calls) < 4:
                    await asyncio.sleep(0.01)
            work.release.set()
            results = [await call for call in calls]
            _, head = results[0]
            assert isinstance(head, grpc.aio.AioRpcError)
            failure = parse_payment_error(head.code(), head.trailing_metadata())
            assert failure.reason == "AUTHORIZATION_REQUIRED"
            for rid, (arrival, outcome) in zip(ids[1:], results[1:], strict=True):
                assert not isinstance(outcome, grpc.aio.AioRpcError), outcome
                assert work.admitted_at[rid] - arrival <= 0.5 + 0.2
            assert loop.time() - start < 1.8

    asyncio.run(scenario())


def test_a_run_waiting_its_turn_asks_for_nothing_and_moves_up_when_the_head_leaves():
    from mppi_provider import Hooks

    async def scenario():
        asked = []
        hooks = Hooks(on_low_credit=lambda notice: asked.append(notice.coverage_request.run_id))
        work = SteadyLoad()
        work.final_counts = TokenCounts(10, 0, 10)
        cfg = config(wait=5000, input_rate=0, output_rate=1000)  # 1,000 output tokens: 1 unit
        async with serving(work=work, cfg=cfg, hooks=hooks) as peer:
            await peer.attach(voucher=1)
            first, second = b"\x01" * 32, b"\x02" * 32
            head = asyncio.create_task(peer.complete(peer.request(first)))
            assert (await peer.next_kind("low_credit")).coverage_request.run_id == first
            # Covered on its own, the next run waits its turn without guidance.
            later = asyncio.create_task(peer.complete(peer.request(second)))
            runs = peer.provider._sessions[SID].runs
            while second not in runs:
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.2)  # Several rechecks.
            assert work.calls == [] and asked == [first]
            head.cancel()
            assert (await later)[-1].terminal.status == pb.RUN_STATUS_COMPLETED
            assert [context.run_id for context in work.calls] == [second] and asked == [first]

    asyncio.run(scenario())


def queued_scenario(*, first, deposit=100000, wait=1000):
    """A first run of ``first`` output tokens; every later one needs 1,000.

    The turn is half the authorization wait; 1,000 output tokens cost 1 unit.
    """
    work = SteadyLoad(first=first, hold=True)
    work.final_counts = TokenCounts(10, 0, 10)
    return work, config(wait=wait, input_rate=0, output_rate=1000), deposit


def test_a_waiting_run_is_not_overtaken_by_a_covered_run_that_first_waits_after_it():
    async def scenario():
        work, cfg, deposit = queued_scenario(first=3000)
        async with serving(work=work, cfg=cfg, deposit=deposit) as peer:
            await peer.attach(voucher=1)
            first, later = b"\x01" * 32, b"\x02" * 32
            calls = [asyncio.create_task(peer.complete(peer.request(first)))]
            assert (await peer.next_kind("low_credit")).coverage_request.run_id == first
            calls.append(asyncio.create_task(peer.complete(peer.request(later))))
            runs = peer.provider._sessions[SID].runs
            while later not in runs:
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.1)  # Covered alone, but behind a run within its turn.
            assert work.calls == []
            await peer.control.write(pb.ControlMessage(voucher=peer.voucher(3)))
            async with asyncio.timeout(1):
                while not work.calls:
                    await asyncio.sleep(0.01)
            assert work.calls[0].run_id == first
            work.release.set()
            await asyncio.gather(*calls, return_exceptions=True)

    asyncio.run(scenario())


def test_an_uncovered_head_repeats_its_request_after_a_short_voucher_or_a_turn():
    from mppi_provider import Hooks

    async def scenario():
        asked = []
        hooks = Hooks(on_low_credit=lambda notice: asked.append(notice.coverage_request.run_id))
        work, cfg, deposit = queued_scenario(first=3000, wait=3000)  # A 1.5 s turn.
        async with serving(work=work, cfg=cfg, deposit=deposit, hooks=hooks) as peer:
            await peer.attach(voucher=1)
            first = b"\x01" * 32
            call = asyncio.create_task(peer.complete(peer.request(first)))
            async with asyncio.timeout(1):
                while not asked:
                    await asyncio.sleep(0.01)
            await asyncio.sleep(0.3)  # Several rechecks within the turn.
            assert asked == [first]  # Unchanged, so not repeated.
            await peer.control.write(pb.ControlMessage(voucher=peer.voucher(2)))
            async with asyncio.timeout(1):
                while len(asked) < 2:  # A voucher that still leaves it short.
                    await asyncio.sleep(0.01)
            await asyncio.sleep(1.7)  # A turn later it asks again.
            assert len(asked) == 3 and set(asked) == {first}
            call.cancel()
            await asyncio.gather(call, return_exceptions=True)

    asyncio.run(scenario())


def test_a_head_refused_for_its_deposit_on_recheck_leaves_the_queue():
    async def scenario():
        # Alone, the head needs 99.5 -> 100 units of a 100-unit deposit. Once the
        # later run is admitted after the head's turn, it needs 101.
        work, cfg, deposit = queued_scenario(first=99_500, deposit=100)
        async with serving(work=work, cfg=cfg, deposit=deposit) as peer:
            await peer.attach(voucher=1)
            first, later = b"\x01" * 32, b"\x02" * 32
            head = asyncio.create_task(peer.complete(peer.request(first)))
            assert (await peer.next_kind("low_credit")).coverage_request.run_id == first
            behind = asyncio.create_task(peer.complete(peer.request(later)))
            with pytest.raises(grpc.aio.AioRpcError) as refused:
                await asyncio.wait_for(head, 2)
            failure = parse_payment_error(refused.value.code(), refused.value.trailing_metadata())
            assert failure.code is PaymentCode.INSUFFICIENT_DEPOSIT
            assert [context.run_id for context in work.calls] == [later]
            work.release.set()
            assert (await behind)[-1].terminal.status == pb.RUN_STATUS_COMPLETED

    asyncio.run(scenario())


def test_control_loss_ends_every_run_waiting_in_the_queue_at_once():
    async def scenario():
        work, cfg, deposit = queued_scenario(first=9000)
        async with serving(work=work, cfg=cfg, deposit=deposit) as peer:
            await peer.attach(voucher=5)
            first, later = b"\x01" * 32, b"\x02" * 32
            calls = [asyncio.create_task(peer.complete(peer.request(first)))]
            assert (await peer.next_kind("low_credit")).coverage_request.run_id == first
            calls.append(asyncio.create_task(peer.complete(peer.request(later))))
            runs = peer.provider._sessions[SID].runs
            while later not in runs:
                await asyncio.sleep(0.01)
            lost = asyncio.get_running_loop().time()
            peer.control.cancel()
            for call in calls:
                with pytest.raises(grpc.aio.AioRpcError) as ended:
                    await call
                failure = parse_payment_error(ended.value.code(), ended.value.trailing_metadata())
                assert failure.reason == "CONTROL_NOT_READY"
            assert asyncio.get_running_loop().time() - lost < 0.3  # Not after a turn.
            assert work.calls == []

    asyncio.run(scenario())


@pytest.mark.parametrize("condition", ("stale", "pause", "expired"))
def test_freshness_pause_deadline_stop_grants_and_signal_provider(condition):
    async def scenario():
        async with serving(work=Work(hold=True)) as peer:
            await peer.attach(voucher=10000)
            first = asyncio.create_task(peer.complete())
            await asyncio.wait_for(peer.work.started.wait(), 1)
            if condition == "stale":
                peer.facts.stale = True
            elif condition == "pause":
                peer.facts.pause = PauseLevel.PAYMENTS
            else:
                peer.facts.now += 86400
            result = await peer.provider.runs.require_coverage(
                peer.work.context, TokenCounts(0, 0, 1000)
            )
            assert not result.granted and peer.work.stop.is_set()
            assert (
                result.failure.reason
                == {
                    "stale": "CHAIN_STATE_STALE",
                    "pause": "CONTRACT_PAUSED",
                    "expired": "CLAIM_DEADLINE",
                }[condition]
            )
            peer.work.release.set()
            assert (await first)[-1].terminal.status == pb.RUN_STATUS_CANCELLED

    asyncio.run(scenario())


def test_control_half_close_requests_termination_without_releasing_reservation():
    async def scenario():
        async with serving(work=Work(hold=True)) as peer:
            await peer.attach(voucher=2000)
            first = asyncio.create_task(peer.complete())
            await asyncio.wait_for(peer.work.started.wait(), 1)
            await peer.control.done_writing()
            await asyncio.wait_for(peer.work.stop.wait(), 1)
            run = peer.provider._sessions[SID].runs[peer.work.context.run_id]
            assert run.accounting.remaining_numerator == 1000 * 1000000
            assert run.accounting.final_status is None
            peer.work.release.set()
            assert (await first)[-1].terminal.status == pb.RUN_STATUS_CANCELLED
            assert run.accounting.remaining_numerator == 0

    asyncio.run(scenario())


def test_invalid_usage_retains_last_good_accounting_and_blocks_new_grants():
    async def scenario():
        async with serving(work=Work(hold=True)) as peer:
            await peer.attach(voucher=2000)
            first = asyncio.create_task(peer.complete())
            await asyncio.wait_for(peer.work.started.wait(), 1)
            session = peer.provider._sessions[SID]
            run = session.runs[peer.work.context.run_id]
            before = run.accounting
            with pytest.raises(ValidationError):
                await peer.provider.runs.report_usage(peer.work.context, TokenCounts(10, 0, 1001))
            assert run.accounting == before
            assert session.inconsistent and peer.work.stop.is_set()
            peer.work.release.set()
            await first

    asyncio.run(scenario())


def test_active_run_retains_original_prices_after_catalog_change():
    async def scenario():
        async with serving(work=Work(hold=True)) as peer:
            await peer.attach(voucher=10000)
            first = asyncio.create_task(peer.complete())
            await asyncio.wait_for(peer.work.started.wait(), 1)
            original = peer.facts.config.catalog
            changed = publish_catalog(
                (CatalogRow("example/model", replace(original.rows[0].rates, output=5000000)),)
            )
            peer.facts.identity = replace(peer.facts.identity, catalog=changed)
            result = await peer.provider.runs.require_coverage(
                peer.work.context, TokenCounts(0, 0, 1100)
            )
            assert result.granted and result.required_amount == 1110
            with pytest.raises(grpc.aio.AioRpcError) as new:
                await peer.complete(peer.request(b"\x03" * 32))
            assert (
                parse_payment_error(new.value.code(), new.value.trailing_metadata()).reason
                == "PRICE_CHANGED"
            )
            peer.work.release.set()
            terminal = (await first)[-1].terminal
            assert terminal.amount == 20 and terminal.rate_card_hash == original.rate_card_hash

    asyncio.run(scenario())


def test_admitted_upstream_error_preserves_json_and_definitive_failure():
    async def scenario():
        async with serving(work=Work(error=True)) as peer:
            await peer.attach(voucher=2000)
            chunks = await peer.complete()
            assert chunks[-2].upstream_error.body == b'{"error":{"message":"upstream capacity"}}'
            assert chunks[-2].upstream_error.http_status == 503
            assert chunks[-1].terminal.status == pb.RUN_STATUS_FAILED

    asyncio.run(scenario())


def test_missing_final_report_is_not_invented_and_shutdown_reports_unresolved():
    async def scenario():
        async with serving(work=Work(final=False)) as peer:
            await peer.attach(voucher=2000)
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await peer.complete()
            assert (
                parse_payment_error(failure.value.code(), failure.value.trailing_metadata()).reason
                == "STATE_UNAVAILABLE"
            )
            with pytest.raises(grpc.aio.AioRpcError) as following:
                await peer.complete(peer.request(b"\x03" * 32))
            assert (
                parse_payment_error(
                    following.value.code(), following.value.trailing_metadata()
                ).reason
                == "STATE_UNAVAILABLE"
            )
            assert len(peer.work.calls) == 1
            result = await peer.provider.shutdown(asyncio.get_running_loop().time())
            assert result.unresolved_session_ids == (SID,)
            assert result.unresolved_runs == (peer.work.context,)
            assert peer.wallet.calls == []

    asyncio.run(scenario())


def test_capture_and_cooperative_close_use_actual_usage_not_voucher_buffer():
    async def scenario():
        async with serving(deposit=5510) as peer:
            await peer.attach(voucher=5510)
            await peer.complete()
            settled = await peer.provider.sessions.settle(SID)
            assert settled.state is OperationState.CONFIRMED
            assert peer.facts.operations[-1].amounts == (20,)
            assert peer.facts.state.status is SessionStatus.OPEN
            await peer.control.write(
                pb.ControlMessage(
                    close_request=pb.CloseRequest(
                        session_id=SID, signature=SIGNER.sign_close(DOMAIN, SID)
                    )
                )
            )
            while True:
                response = await asyncio.wait_for(peer.control.read(), 1)
                if response.WhichOneof("kind") == "close_result":
                    assert response.close_result.capture_amount == 20
                    assert response.close_result.refunded == 5490
                    break
            assert peer.facts.state.status is SessionStatus.CLOSED
            assert len(peer.wallet.calls) == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("reconnect", (False, True))
def test_control_replays_confirmed_close_without_new_payment_or_close_event(reconnect):
    from mppi_provider import Hooks

    async def scenario():
        closed = []
        observed = threading.Event()
        barrier = b"\x7f" * 32
        hooks = Hooks(
            on_close=lambda result: closed.append(result.SerializeToString()),
            on_challenge=lambda challenge: (
                observed.set() if challenge.request_id == barrier else None
            ),
        )
        async with serving(deposit=5510, hooks=hooks) as peer:
            await peer.attach(voucher=5510)
            await peer.complete()
            request = pb.ControlMessage(
                close_request=pb.CloseRequest(
                    session_id=SID, signature=SIGNER.sign_close(DOMAIN, SID)
                )
            )
            await peer.control.write(request)
            original = await peer.next_kind("close_result")
            expected = pb.ControlMessage(
                close_result=pb.CloseResult(
                    session_id=SID, tx_hash=b"\xaa" * 32, capture_amount=20, refunded=5490
                )
            ).SerializeToString()
            assert pb.ControlMessage(close_result=original).SerializeToString() == expected
            session = peer.provider._sessions[SID]
            operation, reference = session.operation, session.close_reference
            assert operation.result.state is OperationState.CONFIRMED
            assert reference == operation.reference == peer.wallet.calls[0][1]
            recipient = peer
            if reconnect:
                peer.control.cancel()
                recipient = Peer(peer.channel, peer.provider, peer.facts, peer.wallet, peer.work)
                await recipient.start()
                state = await recipient.attach()
                assert state.status == pb.SESSION_STATUS_CLOSED and not state.ready
                assert state.control_generation == 2
            try:
                # The original wire reply is deliberately not treated as the
                # caller's completion. Each repeated authenticated request must
                # replay that exact confirmed result, including after reconnect.
                for _ in range(2):
                    await recipient.control.write(request)
                    replay = await asyncio.wait_for(recipient.control.read(), 1)
                    assert replay.SerializeToString() == expected
                assert (await recipient.guidance(rid=barrier)).request_id == barrier
                assert await asyncio.to_thread(observed.wait, 1)
                assert closed == [original.SerializeToString()]
                assert session.operation is operation and session.close_reference == reference
                assert len(peer.wallet.calls) == len(peer.facts.operations) == 1
                assert not peer.wallet.lookups
            finally:
                if reconnect:
                    recipient.control.cancel()

    asyncio.run(scenario())


@pytest.mark.parametrize("invalid", ("signature", "unattached", "session"))
def test_confirmed_close_replay_requires_current_owner_and_close_auth(invalid):
    async def scenario():
        async with serving(deposit=5510) as peer:
            await peer.attach(voucher=5510)
            await peer.complete()
            await peer.provider.sessions.close(SID)
            await peer.next_kind("close_result")
            session = peer.provider._sessions[SID]
            operation, reference = session.operation, session.close_reference
            recipient = Peer(peer.channel, peer.provider, peer.facts, peer.wallet, peer.work)
            await recipient.start()
            try:
                if invalid != "unattached":
                    await recipient.attach()
                sid = b"\xfe" * 32 if invalid == "session" else SID
                signer = OTHER if invalid == "signature" else SIGNER
                await recipient.control.write(
                    pb.ControlMessage(
                        close_request=pb.CloseRequest(
                            session_id=sid, signature=signer.sign_close(DOMAIN, sid)
                        )
                    )
                )
                # No response frame (especially a retained CloseResult) may be
                # sent before the authentication error terminates this stream.
                with pytest.raises(grpc.aio.AioRpcError) as failure:
                    await asyncio.wait_for(recipient.control.read(), 1)
                assert failure.value.code() is grpc.StatusCode.UNAUTHENTICATED
                assert session.operation is operation and session.close_reference == reference
                assert len(peer.wallet.calls) == len(peer.facts.operations) == 1
                assert not peer.wallet.lookups
            finally:
                recipient.control.cancel()

    asyncio.run(scenario())


def test_old_opening_after_restart_cannot_initialize_zero_usage():
    async def scenario():
        async with serving() as peer:
            peer.facts.opening_block = 99
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await peer.attach()
            assert (
                parse_payment_error(failure.value.code(), failure.value.trailing_metadata()).reason
                == "STATE_UNAVAILABLE"
            )
            assert peer.work.calls == []

    asyncio.run(scenario())


@pytest.mark.parametrize("field", ("max_age_ms", "collection_margin_ms"))
def test_follower_policy_must_match_provider_configuration(field):
    cfg = config()
    facts = Facts(cfg)
    setattr(facts, field, getattr(facts, field) + 1)
    with pytest.raises(ValidationError, match="timing differs"):
        Mppi(
            cfg,
            chain=facts,
            contract=ContractCalls(DOMAIN, "USDC", "2"),
            wallet=Wallet(),
            coverage=None,
            execution=None,
        )


def test_unavailable_start_returns_not_ready_and_recovers_without_restart():
    async def scenario():
        class Unavailable(Facts):
            async def start(self):
                self.retrying = True
                raise ChainEvidenceError("startup RPC unavailable")

            async def watch_provider(self, provider_id):
                self.provider_watched = provider_id
                raise ChainEvidenceError("watched RPC unavailable")

        cfg = config()
        facts, work = Unavailable(cfg), Work()
        facts.stale = True
        provider = Mppi(
            cfg,
            chain=facts,
            contract=ContractCalls(DOMAIN, "USDC", "2"),
            wallet=Wallet(),
            coverage=work.coverage,
            execution=work.execute,
        )
        try:
            readiness = await provider.start()
            assert not readiness.ready
            assert readiness.failures[0].reason == "CHAIN_STATE_STALE"
            assert facts.retrying and facts.provider_watched == 42
            assert provider.discovery("/discover") is None
            facts.stale = False
            async with asyncio.timeout(1):
                while provider.discovery("/discover") is None:
                    await asyncio.sleep(0.01)
            assert provider._started and provider._start_block == 100
        finally:
            await provider.shutdown(asyncio.get_running_loop().time())

    asyncio.run(scenario())


def test_ordinary_meter_voucher_renewal_grants_only_the_next_covered_work():
    async def scenario():
        work = Work(hold=True)
        work.final_counts = TokenCounts(10, 0, 110)
        async with serving(work=work) as peer:
            await peer.attach(voucher=1010)
            completion = asyncio.create_task(peer.complete())
            await asyncio.wait_for(work.started.wait(), 1)
            assert (await peer.next_kind("meter")).output_tokens == 0
            await peer.provider.runs.report_usage(work.context, TokenCounts(10, 0, 100))
            meter = await peer.next_kind("meter")
            assert meter.output_tokens == 100
            await peer.control.write(pb.ControlMessage(voucher=peer.voucher(1110)))
            # Setup response is only a processing barrier, not a voucher ACK.
            await peer.guidance()
            renewal = await peer.provider.runs.require_coverage(
                work.context, TokenCounts(0, 0, 1000)
            )
            assert renewal.granted and renewal.required_amount == 1110
            assert not work.stop.is_set()
            await peer.provider.runs.report_usage(work.context, TokenCounts(10, 0, 110))
            assert (await peer.next_kind("meter")).output_tokens == 110
            work.release.set()
            assert (await completion)[-1].terminal.amount == 120
            assert len(work.calls) == 1 and peer.wallet.calls == []

    asyncio.run(scenario())


@pytest.mark.parametrize("notice", (False, True))
def test_above_deposit_voucher_refreshes_confirmed_top_up_without_holding_accounting_lock(
    notice, monkeypatch
):
    async def scenario():
        work = Work(hold=True)
        work.final_counts = TokenCounts(10, 0, 110)
        async with serving(work=work, deposit=1010) as peer:
            await peer.attach(voucher=1010)
            completion = asyncio.create_task(peer.complete())
            entered, release = asyncio.Event(), asyncio.Event()
            calls = []

            async def refresh():
                calls.append(True)
                entered.set()
                await release.wait()
                peer.facts.state = replace(peer.facts.state, deposit=2010)
                return peer.facts.snapshot()

            monkeypatch.setattr(peer.facts, "refresh", refresh)
            try:
                await asyncio.wait_for(work.started.wait(), 1)
                await peer.next_kind("meter")
                if notice:
                    await peer.control.write(
                        pb.ControlMessage(
                            top_up_notice=pb.TopUpNotice(session_id=SID, tx=b"\x88" * 32)
                        )
                    )
                    await peer.control.write(
                        pb.ControlMessage(
                            challenge_request=pb.ChallengeRequest(
                                request_id=b"\x71" * 32, model="example/model"
                            )
                        )
                    )
                    await peer.next_kind("challenge")
                    assert peer.facts.state.deposit == 1010 and calls == []
                await peer.control.write(pb.ControlMessage(voucher=peer.voucher(1110)))
                await asyncio.wait_for(entered.wait(), 1)
                assert peer.provider._sessions[SID].voucher.cumulative == 1010
                # Provider-owned reports must progress while the chain read is
                # pending; no unconfirmed deposit or candidate proof is accepted.
                await asyncio.wait_for(
                    peer.provider.runs.report_usage(work.context, TokenCounts(10, 0, 100)), 0.2
                )
                assert (await peer.next_kind("meter")).output_tokens == 100
                assert not work.stop.is_set()
                release.set()
                await peer.control.write(
                    pb.ControlMessage(
                        challenge_request=pb.ChallengeRequest(
                            request_id=b"\x72" * 32, model="example/model"
                        )
                    )
                )
                await peer.next_kind("challenge")
                assert peer.provider._sessions[SID].voucher.cumulative == 1110
                grant = await peer.provider.runs.require_coverage(
                    work.context, TokenCounts(0, 0, 1000)
                )
                assert grant.granted and grant.required_amount == 1110
                assert calls == [True] and peer.wallet.calls == []
                work.release.set()
                assert (await completion)[-1].terminal.status == pb.RUN_STATUS_COMPLETED
            finally:
                release.set()
                work.release.set()
                await asyncio.gather(completion, return_exceptions=True)

    asyncio.run(scenario())


def test_covered_and_repeated_vouchers_do_not_refresh_chain(monkeypatch):
    async def scenario():
        async with serving(deposit=2000) as peer:
            await peer.attach(voucher=1010)
            calls = []

            async def refresh():
                calls.append(True)
                raise AssertionError("covered voucher triggered a chain read")

            monkeypatch.setattr(peer.facts, "refresh", refresh)
            for amount in (1010, 1000, 1110):
                await peer.control.write(pb.ControlMessage(voucher=peer.voucher(amount)))
            assert (await peer.guidance()).model == "example/model"
            assert peer.provider._sessions[SID].voucher.cumulative == 1110
            assert calls == [] and peer.wallet.calls == []

    asyncio.run(scenario())


@pytest.mark.parametrize("fault", ("signature", "owner"))
def test_above_deposit_voucher_authenticates_before_refresh(fault, monkeypatch):
    async def scenario():
        async with serving(deposit=1010) as peer:
            await peer.attach(voucher=1010)
            calls = []

            async def refresh():
                calls.append(True)
                raise AssertionError("unauthenticated candidate triggered a chain read")

            monkeypatch.setattr(peer.facts, "refresh", refresh)
            call = peer.control
            if fault == "owner":
                call = stubs.ControlStub(peer.channel).Session()
                assert (await call.read()).WhichOneof("kind") == "reconnect_challenge"
            try:
                await call.write(
                    pb.ControlMessage(
                        voucher=peer.voucher(1110, signer=OTHER if fault == "signature" else SIGNER)
                    )
                )
                with pytest.raises(grpc.aio.AioRpcError) as failure:
                    await asyncio.wait_for(call.read(), 1)
                assert failure.value.code() is grpc.StatusCode.UNAUTHENTICATED
                assert calls == [] and peer.provider._sessions[SID].voucher.cumulative == 1010
                if fault == "owner":
                    assert (await peer.guidance()).model == "example/model"
            finally:
                if call is not peer.control:
                    call.cancel()

    asyncio.run(scenario())


@pytest.mark.parametrize("fault", ("failure", "underfunded", "paused", "closed", "stale"))
def test_above_deposit_voucher_failed_refresh_cannot_authorize(fault, monkeypatch):
    async def scenario():
        async with serving(deposit=1010) as peer:
            await peer.attach(voucher=1010)
            calls = []

            async def refresh():
                calls.append(True)
                if fault == "failure":
                    raise ChainEvidenceError("canonical source unavailable")
                if fault != "underfunded":
                    peer.facts.state = replace(peer.facts.state, deposit=2010)
                if fault == "paused":
                    peer.facts.pause = PauseLevel.ALL
                if fault == "closed":
                    peer.facts.state = replace(peer.facts.state, status=SessionStatus.CLOSED)
                if fault == "stale":
                    peer.facts.stale = True
                return peer.facts.current

            monkeypatch.setattr(peer.facts, "refresh", refresh)
            await peer.control.write(pb.ControlMessage(voucher=peer.voucher(1110)))
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await asyncio.wait_for(peer.control.read(), 1)
            expected = (
                grpc.StatusCode.INVALID_ARGUMENT
                if fault == "underfunded"
                else grpc.StatusCode.FAILED_PRECONDITION
            )
            assert failure.value.code() is expected
            if fault != "underfunded":
                payment = parse_payment_error(
                    failure.value.code(), failure.value.trailing_metadata()
                )
                assert (
                    payment is not None
                    and payment.reason
                    == {
                        "failure": "STATE_UNAVAILABLE",
                        "paused": "CONTRACT_PAUSED",
                        "closed": "SESSION_CLOSED",
                        "stale": "CHAIN_STATE_STALE",
                    }[fault]
                )
            assert calls == [True] and peer.provider._sessions[SID].voucher.cumulative == 1010
            assert not peer.work.calls and not peer.wallet.calls

    asyncio.run(scenario())


def test_above_deposit_voucher_refresh_has_chain_policy_deadline(monkeypatch):
    async def scenario():
        cfg = config()
        cfg = replace(cfg, chain=replace(cfg.chain, max_age_ms=50))
        async with serving(cfg=cfg, deposit=1010) as peer:
            await peer.attach(voucher=1010)
            cancelled = asyncio.Event()
            calls = []

            async def refresh():
                calls.append(True)
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

            monkeypatch.setattr(peer.facts, "refresh", refresh)
            await peer.control.write(pb.ControlMessage(voucher=peer.voucher(1110)))
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await asyncio.wait_for(peer.control.read(), 1)
            assert failure.value.code() is grpc.StatusCode.FAILED_PRECONDITION
            payment = parse_payment_error(failure.value.code(), failure.value.trailing_metadata())
            assert payment is not None and payment.reason == "STATE_UNAVAILABLE"
            assert cancelled.is_set() and calls == [True]
            assert peer.provider._sessions[SID].voucher.cumulative == 1010
            async with asyncio.timeout(0.2), peer.provider._condition:
                assert not peer.provider._sessions[SID].owner.live

    asyncio.run(scenario())


def test_above_deposit_voucher_rechecks_attachment_after_refresh(monkeypatch):
    async def scenario():
        async with serving(deposit=1010) as peer:
            await peer.attach(voucher=1010)
            entered, release = asyncio.Event(), asyncio.Event()
            completed = asyncio.Event()

            async def refresh():
                entered.set()
                try:
                    await release.wait()
                    peer.facts.state = replace(peer.facts.state, deposit=2010)
                    return peer.facts.snapshot()
                finally:
                    completed.set()

            monkeypatch.setattr(peer.facts, "refresh", refresh)
            await peer.control.write(pb.ControlMessage(voucher=peer.voucher(1110)))
            await asyncio.wait_for(entered.wait(), 1)
            replacement = Peer(peer.channel, peer.provider, peer.facts, peer.wallet, peer.work)
            try:
                await replacement.start()
                assert (await replacement.attach(voucher=1010)).ready
                assert replacement.generation == peer.generation + 1
                release.set()
                with pytest.raises(grpc.aio.AioRpcError):
                    await asyncio.wait_for(peer.control.read(), 1)
                await asyncio.wait_for(completed.wait(), 1)
                assert (await replacement.guidance()).model == "example/model"
                session = peer.provider._sessions[SID]
                assert session.generation == replacement.generation and session.owner.live
                assert session.voucher.cumulative == 1010
            finally:
                release.set()
                replacement.control.cancel()

    asyncio.run(scenario())


def test_above_deposit_voucher_refresh_does_not_delay_or_revive_denied_execution(monkeypatch):
    async def scenario():
        work = Work(hold=True)
        work.final_counts = TokenCounts(10, 0, 100)
        async with serving(work=work, deposit=1010) as peer:
            await peer.attach(voucher=1010)
            completion = asyncio.create_task(peer.complete())
            entered, release = asyncio.Event(), asyncio.Event()

            async def refresh():
                entered.set()
                await release.wait()
                peer.facts.state = replace(peer.facts.state, deposit=2010)
                return peer.facts.snapshot()

            monkeypatch.setattr(peer.facts, "refresh", refresh)
            try:
                await asyncio.wait_for(work.started.wait(), 1)
                await peer.provider.runs.report_usage(work.context, TokenCounts(10, 0, 100))
                await peer.control.write(pb.ControlMessage(voucher=peer.voucher(1110)))
                await asyncio.wait_for(entered.wait(), 1)
                grant = await asyncio.wait_for(
                    peer.provider.runs.require_coverage(work.context, TokenCounts(0, 0, 1000)),
                    0.2,
                )
                assert not grant.granted and work.stop.is_set()
                assert peer.provider._sessions[SID].voucher.cumulative == 1010
                work.release.set()
                assert (await completion)[-1].terminal.status == pb.RUN_STATUS_CANCELLED
                release.set()
                await peer.control.write(
                    pb.ControlMessage(
                        challenge_request=pb.ChallengeRequest(
                            request_id=b"\x73" * 32, model="example/model"
                        )
                    )
                )
                await peer.next_kind("challenge")
                assert peer.provider._sessions[SID].voucher.cumulative == 1110
                grant = await peer.provider.runs.require_coverage(
                    work.context, TokenCounts(0, 0, 1000)
                )
                assert not grant.granted and work.stop.is_set()
                assert len(work.calls) == 1 and not peer.wallet.calls
            finally:
                release.set()
                work.release.set()
                await asyncio.gather(completion, return_exceptions=True)

    asyncio.run(scenario())


def test_ordinary_midstream_low_credit_warns_before_the_funded_boundary():
    async def scenario():
        work = Work(hold=True)
        async with serving(work=work, deposit=3009) as peer:
            await peer.attach(voucher=2000)
            completion = asyncio.create_task(peer.complete())
            await asyncio.wait_for(work.started.wait(), 1)
            assert (await peer.next_kind("meter")).output_tokens == 0
            warning = await peer.next_kind("low_credit")
            assert not warning.HasField("coverage_request")
            assert warning.remaining_tokens == 1999
            assert warning.model == "example/model"
            assert warning.rate_card_hash == peer.facts.config.catalog.rate_card_hash
            assert not work.stop.is_set() and peer.wallet.calls == []
            work.release.set()
            assert (await completion)[-1].terminal.status == pb.RUN_STATUS_COMPLETED

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("input_rate", "output_rate", "deposit", "expected"),
    (
        (1000000, 1000000, 3010, None),
        (10000000, 1000000, 3099, 199),
        (10000000, 1000000, 3100, None),
        (1000000, 0, 10, None),
    ),
)
def test_low_credit_threshold_uses_output_price_but_guidance_uses_maximum_rate(
    input_rate, output_rate, deposit, expected
):
    async def scenario():
        cfg = config(input_rate=input_rate, output_rate=output_rate)
        async with serving(work=Work(hold=True), cfg=cfg, deposit=deposit) as peer:
            await peer.attach(voucher=deposit)
            completion = asyncio.create_task(peer.complete())
            await asyncio.wait_for(peer.work.started.wait(), 1)
            await peer.next_kind("meter")
            if expected is not None:
                warning = await peer.next_kind("low_credit")
                assert warning.remaining_tokens == expected
                assert not warning.HasField("coverage_request")
            # An ordered same-stream challenge is a barrier. No ordinary
            # warning is expected at exact equality or for free output.
            assert (await peer.guidance()).model == "example/model"
            peer.work.release.set()
            await completion

    asyncio.run(scenario())


def test_ordinary_low_credit_suppresses_unchanged_headroom_and_rearms_after_top_up():
    async def scenario():
        work = Work(hold=True)
        work.final_counts = TokenCounts(10, 0, 500)
        async with serving(work=work, deposit=3009) as peer:
            await peer.attach(voucher=3009)
            completion = asyncio.create_task(peer.complete())
            await asyncio.wait_for(work.started.wait(), 1)
            await peer.next_kind("meter")
            assert (await peer.next_kind("low_credit")).remaining_tokens == 1999
            await peer.provider.runs.report_usage(work.context, TokenCounts(10, 0, 100))
            await peer.next_kind("meter")
            assert (await peer.guidance()).model == "example/model"
            peer.facts.state = replace(peer.facts.state, deposit=4000)
            await peer.provider.runs.report_usage(work.context, TokenCounts(10, 0, 200))
            await peer.next_kind("meter")
            assert (await peer.guidance()).model == "example/model"
            grant = await peer.provider.runs.require_coverage(work.context, TokenCounts(0, 0, 2000))
            assert grant.granted
            await peer.provider.runs.report_usage(work.context, TokenCounts(10, 0, 300))
            await peer.next_kind("meter")
            assert (await peer.next_kind("low_credit")).remaining_tokens == 1790
            work.release.set()
            await completion
            assert (await peer.next_kind("meter")).output_tokens == 500
            await peer.next_kind("receipt")
            assert (await peer.guidance()).model == "example/model"

    asyncio.run(scenario())


@pytest.mark.parametrize("case", ("direction", "session", "close"))
def test_invalid_control_direction_or_session_stops_owned_work(case):
    async def scenario():
        async with serving(work=Work(hold=True)) as peer:
            await peer.attach(voucher=2000)
            completion = asyncio.create_task(peer.complete())
            await asyncio.wait_for(peer.work.started.wait(), 1)
            message = pb.ControlMessage(reconnect_challenge=pb.ReconnectChallenge(nonce=bytes(32)))
            if case == "session":
                voucher = peer.voucher(2000)
                voucher.session_id = b"\xfe" * 32
                message = pb.ControlMessage(voucher=voucher)
            elif case == "close":
                message = pb.ControlMessage(
                    close_request=pb.CloseRequest(
                        session_id=SID, signature=OTHER.sign_close(DOMAIN, SID)
                    )
                )
            await peer.control.write(message)
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                while True:
                    await peer.control.read()
            assert failure.value.code() is (
                grpc.StatusCode.INVALID_ARGUMENT
                if case == "direction"
                else grpc.StatusCode.UNAUTHENTICATED
            )
            await asyncio.wait_for(peer.work.stop.wait(), 1)
            peer.work.release.set()
            assert (await completion)[-1].terminal.status == pb.RUN_STATUS_CANCELLED

    asyncio.run(scenario())


def test_new_attachment_fences_old_generation_across_separate_connections():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=2000)
            await peer.complete()
            second_channel = grpc.aio.insecure_channel(peer.target, options=GRPC_OPTIONS)
            second = Peer(second_channel, peer.provider, peer.facts, peer.wallet, peer.work)
            try:
                await second.start()
                state = await second.attach()
                assert state.control_generation == 2 and state.accepted_voucher.cumulative == 2000
                old = peer.request(b"\x03" * 32)
                with pytest.raises(grpc.aio.AioRpcError) as failure:
                    await second.complete(old)
                assert (
                    parse_payment_error(
                        failure.value.code(), failure.value.trailing_metadata()
                    ).reason
                    == "CONTROL_NOT_READY"
                )
                assert len(peer.work.preparations) == 1
                assert (await second.complete(second.request(b"\x04" * 32)))[
                    -1
                ].terminal.status == pb.RUN_STATUS_COMPLETED
                assert len(peer.work.calls) == 2
                assert peer.provider._sessions[SID].generation == 2
            finally:
                second.control.cancel()
                await second_channel.close()

    asyncio.run(scenario())


def test_rejected_active_reconnect_preserves_live_owner_authorization_and_work():
    async def scenario():
        async with serving(work=Work(hold=True)) as peer:
            await peer.attach(voucher=2000)
            completion = asyncio.create_task(peer.complete())
            await asyncio.wait_for(peer.work.started.wait(), 1)
            session = peer.provider._sessions[SID]
            owner = session.owner
            second_channel = grpc.aio.insecure_channel(peer.target, options=GRPC_OPTIONS)
            second = Peer(second_channel, peer.provider, peer.facts, peer.wallet, peer.work)
            try:
                await second.start()
                with pytest.raises(grpc.aio.AioRpcError) as failure:
                    await second.attach(voucher=3000)
                assert (
                    parse_payment_error(
                        failure.value.code(), failure.value.trailing_metadata()
                    ).reason
                    == "STATE_UNAVAILABLE"
                )
                assert session.owner is owner and owner.live
                assert session.generation == 1 and session.voucher.cumulative == 2000
                assert not peer.work.stop.is_set()
                assert (
                    await peer.provider.runs.require_coverage(
                        peer.work.context, TokenCounts(0, 0, 1000)
                    )
                ).granted
                peer.work.release.set()
                assert (await completion)[-1].terminal.status == pb.RUN_STATUS_COMPLETED
            finally:
                peer.work.release.set()
                await asyncio.gather(completion, return_exceptions=True)
                second.control.cancel()
                await second_channel.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ("full", "closed", "snapshot", "invalid_voucher"))
def test_failed_reconnect_snapshot_commit_preserves_owner_and_voucher(failure, monkeypatch):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=2000)
            session = peer.provider._sessions[SID]
            owner = session.owner
            candidate = runtime._Attachment(runtime.ByteQueue(max_messages=1))
            request = pb.ReconnectRequest(
                session_id=SID,
                nonce=candidate.nonce,
                signature=SIGNER.sign_reconnect(DOMAIN, SID, candidate.nonce),
                latest_voucher=peer.voucher(3000),
                last_meter_seq=0,
            )
            if failure == "full":
                candidate.queue.put_nowait(b"occupied")
            elif failure == "closed":
                candidate.queue.close()
            elif failure == "snapshot":

                def unavailable(*args, **kwargs):
                    raise runtime._not_ready("STATE_UNAVAILABLE", SID)

                monkeypatch.setattr(peer.provider, "_session_state", unavailable)
            else:
                request.latest_voucher.CopyFrom(peer.voucher(3000, OTHER))
            with pytest.raises(_RpcFailure):
                await peer.provider._attach(candidate, request)
            assert session.owner is owner and owner.live
            assert session.generation == 1 and session.voucher.cumulative == 2000
            assert candidate.session is None and candidate.generation == 0
            assert candidate.request is None
            # A surviving live flag alone would not prove the original queue survived.
            peer.provider._send(
                owner,
                pb.ControlMessage(reconnect_challenge=pb.ReconnectChallenge(nonce=owner.nonce)),
            )
            assert (await peer.next_kind("reconnect_challenge")).nonce == owner.nonce

    asyncio.run(scenario())


@pytest.mark.parametrize("elapsed", (0.999, 1.0, 1.001))
def test_authorization_wait_deadline_is_checked_before_late_voucher_admission(elapsed, monkeypatch):
    async def scenario():
        async with serving(cfg=config(wait=1000)) as peer:
            await peer.attach()
            session = peer.provider._sessions[SID]
            clock = SimpleNamespace(now=100.0)
            patched_asyncio = dict(vars(asyncio))
            patched_asyncio["get_running_loop"] = lambda: SimpleNamespace(time=lambda: clock.now)
            waits = 0

            async def receive_voucher_after_wait():
                nonlocal waits
                waits += 1
                assert waits == 1
                # The runtime sees advanced monotonic time while the real event
                # loop and gRPC transport retain their own unmodified clocks.
                clock.now += elapsed
                peer.provider._voucher(session, peer.voucher(2000))
                return True

            with monkeypatch.context() as patch:
                patch.setattr(runtime, "asyncio", SimpleNamespace(**patched_asyncio))
                patch.setattr(peer.provider._condition, "wait", receive_voucher_after_wait)
                if elapsed < 1:
                    assert (await peer.complete())[-1].terminal.status == pb.RUN_STATUS_COMPLETED
                    assert len(peer.work.calls) == 1
                else:
                    with pytest.raises(grpc.aio.AioRpcError) as failure:
                        await peer.complete()
                    assert (
                        parse_payment_error(
                            failure.value.code(), failure.value.trailing_metadata()
                        ).reason
                        == "AUTHORIZATION_REQUIRED"
                    )
                    run = session.runs[b"\x02" * 32]
                    assert run.not_admitted and not run.admitted
                    assert run.accounting.remaining_numerator == 0
                    assert run.accounting.counts == TokenCounts(0, 0, 0)
                    assert run.covered_counts == TokenCounts(0, 0, 0)
                    assert peer.work.calls == []
                assert waits == 1 and session.voucher.cumulative == 2000
                assert session.owner.live and session.generation == 1

    asyncio.run(scenario())


def test_successful_reconnect_snapshot_commits_once_with_staged_authorization():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=2000)
            session = peer.provider._sessions[SID]
            owner = session.owner
            candidate = runtime._Attachment(runtime.ByteQueue())
            request = pb.ReconnectRequest(
                session_id=SID,
                nonce=candidate.nonce,
                signature=SIGNER.sign_reconnect(DOMAIN, SID, candidate.nonce),
                latest_voucher=peer.voucher(3000),
                last_meter_seq=0,
            )
            await peer.provider._attach(candidate, request)
            first = pb.ControlMessage.FromString(await candidate.queue.get()).session_state
            assert first.ready and first.control_generation == 2
            assert first.accepted_voucher == request.latest_voucher
            assert session.owner is candidate and not owner.live
            assert candidate.session is session and candidate.generation == session.generation == 2
            assert candidate.request == request.SerializeToString(deterministic=True)
            await peer.provider._attach(candidate, request)
            repeated = pb.ControlMessage.FromString(await candidate.queue.get()).session_state
            assert repeated == first and session.generation == 2
            with pytest.raises(_RpcFailure):
                peer.provider._owner(owner, SID)
            assert session.usage == 0 and peer.work.calls == []

    asyncio.run(scenario())


def test_exhausted_history_budget_does_not_retain_a_rejected_candidate(monkeypatch):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=2000)
            request = peer.request()
            # Payload fits; the larger complete recovery record does not.
            monkeypatch.setattr(runtime, "MAX_MESSAGE_BYTES", len(request.payload) + 1)
            before = dict(peer.provider._sessions[SID].catalogs)
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await peer.complete(request)
            assert failure.value.code() is grpc.StatusCode.RESOURCE_EXHAUSTED
            assert peer.provider._sessions[SID].runs == {}
            assert peer.provider._sessions[SID].catalogs == before
            assert peer.work.preparations == [] and peer.work.calls == []

    asyncio.run(scenario())


def test_control_queue_exhaustion_preserves_usage_and_requests_termination():
    async def scenario():
        work = Work(hold=True)
        work.final_counts = TokenCounts(10, 0, 65)
        async with serving(work=work) as peer:
            await peer.attach(voucher=2000)
            completion = asyncio.create_task(peer.complete())
            await asyncio.wait_for(work.started.wait(), 1)
            await peer.next_kind("meter")
            # These uncontended report calls do not yield to the writer. Exceed
            # the documented 64-message queue without transport timing guesses.
            for output in range(1, 66):
                await peer.provider.runs.report_usage(work.context, TokenCounts(10, 0, output))
            session = peer.provider._sessions[SID]
            run = session.runs[work.context.run_id]
            assert work.stop.is_set() and not session.owner.live
            assert run.accounting.counts == TokenCounts(10, 0, 65)
            assert run.accounting.remaining_numerator == 935 * 1000000
            work.release.set()
            assert (await completion)[-1].terminal.status == pb.RUN_STATUS_CANCELLED

    asyncio.run(scenario())


def test_unread_complete_cannot_block_control_voucher_processing():
    async def scenario():
        class Burst(Work):
            async def execute(self, request, context, reports, stop):
                self.context, self.stop = context, stop
                self.started.set()
                await reports.report_usage(context, TokenCounts(10, 0, 0))
                payload = b'{"content":"' + b"x" * (1024 * 1024) + b'"}'
                for _ in range(80):
                    yield payload
                await reports.report_terminal(context, self.final_counts, RunStatus.COMPLETED)

        work = Burst()
        async with serving(work=work, cfg=config(wait=500)) as peer:
            await peer.attach(voucher=1010)
            call = peer.chat.Completions(peer.request(), timeout=3)
            try:
                await asyncio.wait_for(work.started.wait(), 1)
                run = peer.provider._sessions[SID].runs[work.context.run_id]
                async with asyncio.timeout(1):
                    while run.output._size < 15 * 1024 * 1024:
                        await asyncio.sleep(0.001)
                await peer.control.write(pb.ControlMessage(voucher=peer.voucher(2000)))
                await peer.control.write(
                    pb.ControlMessage(
                        challenge_request=pb.ChallengeRequest(
                            request_id=b"\x77" * 32, model="example/model"
                        )
                    )
                )
                assert (await peer.next_kind("challenge")).request_id == b"\x77" * 32
                assert (await peer.provider.sessions.get(SID)).accepted_authorization == 2000
                await asyncio.wait_for(work.stop.wait(), 1)
                assert run.accounting.final_status is None
                assert run.accounting.remaining_numerator == 1000 * 1000000
            finally:
                call.cancel()

    asyncio.run(scenario())


@pytest.mark.parametrize("covered", (False, True))
def test_pending_capture_is_reconciled_before_distinct_close_without_resubmission(covered):
    async def scenario():
        async with serving(deposit=5510) as peer:
            await peer.attach(voucher=5510)
            await peer.complete()
            original = peer.facts.reconcile
            lookups = 0

            def delayed(operation, *, operation_ref, tx_hash, wallet_operation_id=None):
                nonlocal lookups
                lookups += 1
                if lookups == 1:
                    return OperationResult(
                        operation_ref, OperationState.PENDING, wallet_operation_id, tx_hash
                    )
                confirmed = original(
                    operation,
                    operation_ref=operation_ref,
                    tx_hash=tx_hash,
                    wallet_operation_id=wallet_operation_id,
                )
                if covered and operation.kind.value == "settle":
                    return OperationResult(
                        operation_ref,
                        OperationState.UNKNOWN,
                        wallet_operation_id,
                        tx_hash,
                        confirmed.observation,
                        (SessionOperationOutcome(SID, 20, True),),
                    )
                return confirmed

            peer.facts.reader.reconcile = delayed
            pending = await peer.provider.sessions.settle(SID)
            assert pending.state is OperationState.PENDING and len(peer.wallet.calls) == 1
            closing = await peer.provider.sessions.close(SID)
            assert closing.state is OperationState.UNKNOWN
            assert closing.operation_ref != pending.operation_ref
            assert len(peer.wallet.calls) == 1 and peer.wallet.lookups == [pending.operation_ref]
            final = await peer.provider.sessions.close(SID)
            assert final.state is OperationState.CONFIRMED
            assert final.operation_ref == closing.operation_ref
            assert peer.facts.state.status is SessionStatus.CLOSED
            assert len(peer.wallet.calls) == 2
            assert await peer.provider.sessions.close(SID) == final
            assert len(peer.wallet.calls) == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("wallet_failure", ("exception", "missing", "conflict"))
def test_wallet_lookup_failure_preserves_known_transaction_and_chain_can_finish(wallet_failure):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=2000)
            await peer.complete()
            original = peer.facts.reconcile

            def unavailable(operation, *, operation_ref, tx_hash, wallet_operation_id=None):
                raise ChainEvidenceError("receipt unavailable")

            peer.facts.reader.reconcile = unavailable
            first = await peer.provider.sessions.settle(SID)
            assert first.state is OperationState.UNKNOWN
            assert first.wallet_operation_id == "wallet-123" and first.tx_hash == b"\xaa" * 32

            def lookup(*, operation_ref):
                peer.wallet.lookups.append(operation_ref)
                if wallet_failure == "exception":
                    raise TimeoutError("wallet lookup unavailable")
                if wallet_failure == "conflict":
                    return OperationResult(
                        operation_ref, OperationState.UNKNOWN, "different", b"\xbb" * 32
                    )
                return OperationResult(operation_ref, OperationState.UNKNOWN)

            peer.wallet.reconcile = lookup
            uncertain = await peer.provider.sessions.settle(SID)
            assert uncertain == first
            assert len(peer.wallet.calls) == 1

            def confirmed(operation, *, operation_ref, tx_hash, wallet_operation_id=None):
                if tx_hash != first.tx_hash:
                    raise ChainEvidenceError("different transaction is not the original operation")
                assert wallet_operation_id == first.wallet_operation_id
                return original(
                    operation,
                    operation_ref=operation_ref,
                    tx_hash=tx_hash,
                    wallet_operation_id=wallet_operation_id,
                )

            peer.facts.reader.reconcile = confirmed
            final = await peer.provider.sessions.settle(SID)
            assert final.state is OperationState.CONFIRMED
            assert final.operation_ref == first.operation_ref
            assert (
                final.tx_hash == first.tx_hash
                and final.wallet_operation_id == first.wallet_operation_id
            )
            assert len(peer.wallet.calls) == 1
            lookup_count = len(peer.wallet.lookups)
            assert await peer.provider.sessions.settle(SID) == final
            assert len(peer.wallet.lookups) == lookup_count and len(peer.wallet.calls) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("cause", ("detach", "generation", "stale", "pause", "cancel"))
def test_invalidated_admission_cannot_dispatch_execution(cause, monkeypatch):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=10000)
            scheduled, resume = asyncio.Event(), asyncio.Event()
            execute = peer.provider._execute

            async def delayed(run):
                scheduled.set()
                await resume.wait()
                await execute(run)

            monkeypatch.setattr(peer.provider, "_execute", delayed)
            complete = asyncio.create_task(peer.complete())
            await asyncio.wait_for(scheduled.wait(), 1)
            session = peer.provider._sessions[SID]
            run = next(iter(session.runs.values()))
            assert run.admitted and not peer.work.calls
            if cause == "detach":
                peer.provider._detach(session.owner)
            elif cause == "generation":
                session.generation += 1
            elif cause == "stale":
                peer.facts.stale = True
            elif cause == "pause":
                peer.facts.pause = PauseLevel.PAYMENTS
            else:
                run.stop.set()
            resume.set()
            with pytest.raises(grpc.aio.AioRpcError):
                await complete
            assert not peer.work.calls
            assert run.unresolved and run.accounting.final_status is None
            assert run.accounting.remaining_numerator > 0

    asyncio.run(scenario())


def restore_reader(facts):
    facts.reader.block = facts.block
    facts.reader.session = lambda sid, block: facts.state
    facts.reader.identity = lambda pid, block, **kwargs: facts.identity
    facts.reader.simulate = lambda prepared, *, sender: None


def replacement(peer, cfg=None):
    restore_reader(peer.facts)
    provider = Mppi(
        cfg or peer.provider.config,
        chain=peer.facts,
        contract=ContractCalls(DOMAIN, "USDC", "2"),
        wallet=peer.wallet,
        coverage=peer.work.coverage,
        execution=peer.work.execute,
    )
    return provider


def test_checkpoint_recovery_preserves_actual_usage_and_requires_new_attachment():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            assert len([chunk async for chunk in peer.chat.Completions(peer.request())]) == 2
            checkpoint = await peer.provider.sessions.checkpoint(SID)
            restored = replacement(peer)
            await restored.start()
            try:
                view = await restored.sessions.recover(checkpoint)
                assert view.usage_amount == 20
                assert view.accepted_authorization == 1010
                assert not view.readiness.ready
                assert view.readiness.failures[0].reason == "CONTROL_NOT_READY"
                assert len(peer.work.calls) == 1
                again = await restored.sessions.checkpoint(SID)
                assert again.runs == checkpoint.runs
                assert again.snapshot.meter_seq == checkpoint.snapshot.meter_seq
                assert peer.wallet.calls == []
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


def reconnect(provider, pending, meter_seq=0):
    """A fresh Control attachment that names ``pending`` runs."""
    candidate = runtime._Attachment(runtime.ByteQueue())
    request = pb.ReconnectRequest(
        session_id=SID,
        nonce=candidate.nonce,
        signature=SIGNER.sign_reconnect(DOMAIN, SID, candidate.nonce),
        last_meter_seq=meter_seq,
        pending_run_ids=list(pending),
    )
    return candidate, provider._attach(candidate, request)


async def answered(candidate, rid):
    state = pb.ControlMessage.FromString(await candidate.queue.get()).session_state
    (row,) = [row for row in state.run_states if row.run_id == rid]
    return row


def test_a_session_this_runtime_opened_answers_a_never_held_pending_run_not_admitted():
    never = b"\x09" * 32

    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            candidate, attaching = reconnect(peer.provider, [never])
            await attaching
            row = await answered(candidate, never)
            assert row.status == pb.RUN_STATUS_NOT_ADMITTED and row.model == ""
            assert (row.input_tokens, row.cache_tokens, row.output_tokens) == (0, 0, 0)
            assert never not in peer.provider._sessions[SID].runs

    asyncio.run(scenario())


def test_a_session_never_attached_here_refuses_an_unknown_pending_run():
    """A new process re-creates a session from its opening before any Control
    generation; a start block lagging the crashed one's counts every admission
    as known. An agent cannot hold pending runs at generation 0, so one it names
    was admitted earlier: never NOT_ADMITTED."""
    never = b"\x09" * 32

    async def scenario():
        async with serving() as peer:
            await unused_checkpoint(peer)
            session = peer.provider._sessions[SID]
            assert session.generation == 0 and session.admissions_known
            _, attaching = reconnect(peer.provider, [never])
            with pytest.raises(_RpcFailure) as refused:
                await attaching
            assert refused.value.payment.reason == "STATE_UNAVAILABLE"
            assert peer.provider._sessions[SID].owner is None
            assert peer.provider._sessions[SID].generation == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("asserted_current", [False, True])
def test_a_run_served_after_the_restored_checkpoint_is_never_answered_not_admitted(
    asserted_current,
):
    """The host restores a checkpoint older than its newest; run 3 was served after it."""
    served = b"\x03" * 32

    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=2020)
            assert len([chunk async for chunk in peer.chat.Completions(peer.request())]) == 2
            older = await peer.provider.sessions.checkpoint(SID)
            chunks = [chunk async for chunk in peer.chat.Completions(peer.request(served))]
            assert chunks[-1].terminal.status == pb.RUN_STATUS_COMPLETED
            restored = replacement(peer)
            await restored.start()
            try:
                if asserted_current:
                    # A host that asserts its history is current takes that on.
                    await restored.sessions.recover_current(older)
                    candidate, attaching = reconnect(restored, [served], older.snapshot.meter_seq)
                    await attaching
                    row = await answered(candidate, served)
                    assert row.status == pb.RUN_STATUS_NOT_ADMITTED
                else:
                    await restored.sessions.recover(older)
                    _, attaching = reconnect(restored, [served], older.snapshot.meter_seq)
                    with pytest.raises(_RpcFailure) as refused:
                        await attaching
                    assert refused.value.payment.reason == "STATE_UNAVAILABLE"
                    assert restored._sessions[SID].owner is None
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "change",
    [
        "drop_run",
        "drop_usage",
        "bad_signature",
        "bad_cursor",
        "bad_bound",
        "bad_opening",
        "bad_observation",
        "invent_reservation",
        "drop_final",
    ],
)
def test_recovery_rejects_incomplete_or_contradictory_evidence_atomically(change):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            _ = [chunk async for chunk in peer.chat.Completions(peer.request())]
            evidence = await peer.provider.sessions.checkpoint(SID)
            if change == "drop_run":
                evidence = replace(evidence, runs=())
            elif change == "drop_usage":
                del evidence.snapshot.usage_by_price[:]
            elif change == "bad_signature":
                evidence = replace(evidence, runs=(replace(evidence.runs[0], signature=bytes(65)),))
            elif change == "bad_cursor":
                evidence = replace(evidence, runs=(replace(evidence.runs[0], meter_seq=0),))
            elif change == "bad_bound":
                evidence = replace(
                    evidence, runs=(replace(evidence.runs[0], covered_counts=TokenCounts(0, 0, 0)),)
                )
            elif change == "bad_opening":
                evidence.snapshot.descriptor.agent = bytes([9]) * 20
            elif change == "bad_observation":
                evidence = replace(
                    evidence, observation=replace(evidence.observation, block_hash=bytes(32))
                )
            elif change == "invent_reservation":
                evidence = replace(
                    evidence,
                    reservations=(
                        (evidence.runs[0].context, RunAccounting(TokenCounts(0, 0, 0), 10)),
                    ),
                )
            elif change == "drop_final":
                del evidence.snapshot.run_states[:]
            restored = replacement(peer)
            await restored.start()
            try:
                with pytest.raises((_RpcFailure, ValidationError)):
                    await restored.sessions.recover(evidence)
                assert SID not in restored._sessions
                assert not peer.wallet.calls
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


def test_recovery_cannot_replace_live_owner():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            evidence = await peer.provider.sessions.checkpoint(SID)
            restore_reader(peer.facts)
            owner = peer.provider._sessions[SID].owner
            with pytest.raises(_RpcFailure) as failure:
                await peer.provider.sessions.recover(evidence)
            assert failure.value.payment.reason == "CONTROL_NOT_READY"
            assert peer.provider._sessions[SID].owner is owner

    asyncio.run(scenario())


def test_recovery_retains_unresolved_reservation_and_accepts_original_final_once():
    async def scenario():
        async with serving(work=Work(hold=True)) as peer:
            await peer.attach(voucher=1010)
            call = peer.chat.Completions(peer.request())
            await call.read()
            evidence = await peer.provider.sessions.checkpoint(SID)
            remaining = evidence.reservations[0][1].remaining_numerator
            restored = replacement(peer)
            await restored.start()
            try:
                view = await restored.sessions.recover(evidence)
                assert not view.readiness.ready
                assert view.readiness.failures[0].reason == "STATE_UNAVAILABLE"
                run = restored._sessions[SID].runs[evidence.runs[0].context.run_id]
                assert run.stop.is_set() and run.task is None
                assert run.accounting.remaining_numerator == remaining
                result = await restored.runs.report_terminal(
                    run.context, TokenCounts(10, 0, 10), RunStatus.CANCELLED
                )
                assert result.accounting.session_usage_amount == 20
                duplicate = await restored.runs.report_terminal(
                    run.context, TokenCounts(10, 0, 10), RunStatus.CANCELLED
                )
                assert duplicate.accounting.duplicate
                assert run.accounting.remaining_numerator == 0
                assert len(peer.work.calls) == 1
            finally:
                call.cancel()
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


def test_recovery_preserves_pending_capture_identity_without_execution():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            _ = [chunk async for chunk in peer.chat.Completions(peer.request())]

            def pending(prepared, *, operation_ref, tx_hash, wallet_operation_id=None):
                return OperationResult(
                    operation_ref, OperationState.PENDING, wallet_operation_id, tx_hash
                )

            peer.facts.reader.reconcile = pending
            first = await peer.provider.sessions.settle(SID)
            evidence = await peer.provider.sessions.checkpoint(SID)
            restored = replacement(peer)
            await restored.start()
            try:
                await restored.sessions.recover(evidence)
                after = await restored.sessions.settle(SID)
                assert after == first
                assert len(peer.wallet.calls) == 1
                assert peer.wallet.lookups == [first.operation_ref, first.operation_ref]
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


class ReceiptRpc:
    """Canonical receipt fixture consumed by the production ChainReader."""

    def __init__(self, facts, reverted_hash):
        self.facts = facts
        self.reverted_hash = reverted_hash
        self.receipt_reads = []

    def call(self, method, params):
        if method == "eth_chainId":
            return hex(DOMAIN.chain_id)
        if method == "eth_getBlockByNumber":
            block = self.facts.block(int(params[0], 16))
            return {
                "number": hex(block.observation.block_number),
                "hash": "0x" + block.observation.block_hash.hex(),
                "parentHash": "0x" + block.parent_hash.hex(),
                "timestamp": hex(block.timestamp),
            }
        assert method == "eth_getTransactionReceipt"
        self.receipt_reads.append(params[0])
        if params[0] != "0x" + self.reverted_hash.hex():
            return None
        block = self.facts.block(self.facts.height)
        return {
            "transactionHash": params[0],
            "blockNumber": hex(block.observation.block_number),
            "blockHash": "0x" + block.observation.block_hash.hex(),
            "status": "0x0",
        }


@pytest.mark.parametrize("receipt", ("reverted", "pending"))
@pytest.mark.parametrize(
    "lookup",
    ("different_hash", "unavailable", "missing_hash", "wrong_ref", "wrong_id", "missing_id"),
)
def test_recovery_binds_saved_hash_to_original_wallet_reference(receipt, lookup):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            await peer.complete()
            unrelated = b"\xbb" * 32
            rpc = ReceiptRpc(peer.facts, unrelated if receipt == "reverted" else b"\xcc" * 32)
            chain = ChainReader(rpc, peer.facts.reader.profile)
            peer.facts.reader.reconcile = chain.reconcile
            original = await peer.provider.sessions.settle(SID)
            assert original.state is OperationState.PENDING
            evidence = await peer.provider.sessions.checkpoint(SID)
            assert evidence.operation is not None
            substituted = replace(
                evidence,
                operation=replace(
                    evidence.operation,
                    result=replace(evidence.operation.result, tx_hash=unrelated),
                ),
            )
            # Demonstrate that canonical receipt evidence alone accepts the
            # substituted hash: the original wallet reference supplies binding.
            observed = chain.reconcile(
                evidence.operation.prepared,
                operation_ref=original.operation_ref,
                tx_hash=unrelated,
                wallet_operation_id=original.wallet_operation_id,
            )
            assert observed.state is (
                OperationState.FAILED if receipt == "reverted" else OperationState.PENDING
            )
            wallet_lookup = peer.wallet.reconcile

            def lookup_result(*, operation_ref):
                peer.wallet.lookups.append(operation_ref)
                if lookup == "unavailable":
                    raise RuntimeError("wallet unavailable")
                return OperationResult(
                    "other-reference" if lookup == "wrong_ref" else operation_ref,
                    OperationState.PENDING,
                    "other-wallet-id"
                    if lookup == "wrong_id"
                    else None
                    if lookup == "missing_id"
                    else original.wallet_operation_id,
                    None
                    if lookup == "missing_hash"
                    else original.tx_hash
                    if lookup == "different_hash"
                    else unrelated,
                )

            peer.wallet.reconcile = lookup_result
            restored = replacement(peer)
            await restored.start()
            try:
                rpc.receipt_reads.clear()
                with pytest.raises(_RpcFailure):
                    await restored.sessions.recover(substituted)
                assert SID not in restored._sessions
                assert not rpc.receipt_reads
                assert len(peer.wallet.calls) == 1
                peer.wallet.reconcile = wallet_lookup
                await restored.sessions.recover(evidence)
                retained = await restored.sessions.settle(SID)
                assert retained == original
                assert len(peer.wallet.calls) == 1
            finally:
                peer.wallet.reconcile = wallet_lookup
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


@pytest.mark.parametrize("saved_wallet_id", (None, "wallet-123"))
def test_recovery_accepts_wallet_bound_canonical_revert(saved_wallet_id):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            await peer.complete()
            rpc = ReceiptRpc(peer.facts, b"\xbb" * 32)
            chain = ChainReader(rpc, peer.facts.reader.profile)
            peer.facts.reader.reconcile = chain.reconcile
            original = await peer.provider.sessions.settle(SID)
            evidence = await peer.provider.sessions.checkpoint(SID)
            evidence = replace(
                evidence,
                operation=replace(
                    evidence.operation,
                    result=replace(evidence.operation.result, wallet_operation_id=saved_wallet_id),
                ),
            )
            rpc.reverted_hash = original.tx_hash
            restored = replacement(peer)
            await restored.start()
            try:
                await restored.sessions.recover(evidence)
                result = restored._sessions[SID].operation.result
                assert result.state is OperationState.FAILED
                assert result.operation_ref == original.operation_ref
                assert result.tx_hash == original.tx_hash
                assert result.wallet_operation_id == original.wallet_operation_id
                assert peer.wallet.lookups == [original.operation_ref]
                assert len(peer.wallet.calls) == 1
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


def test_closed_recovery_returns_original_cooperative_close_result():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            _ = [chunk async for chunk in peer.chat.Completions(peer.request())]
            first = await peer.provider.sessions.close(SID)
            evidence = await peer.provider.sessions.checkpoint(SID)
            restored = replacement(peer)
            await restored.start()
            try:
                await restored.sessions.recover(evidence)
                assert await restored.sessions.close(SID) == first
                assert len(peer.wallet.calls) == 1
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


@pytest.mark.parametrize("retain_result", (False, True))
def test_closed_recovery_cannot_forget_a_retained_cooperative_close_operation(retain_result):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            await peer.complete()
            first = await peer.provider.sessions.close(SID)
            evidence = await peer.provider.sessions.checkpoint(SID)
            restored = replacement(peer)
            await restored.start()
            try:
                await restored.sessions.recover(evidence)
                previous = restored._sessions[SID]
                assert previous.close_result is not None
                assert previous.operation.result == first
                missing = replace(
                    evidence,
                    operation=None,
                    baseline=evidence.operation if retain_result else evidence.baseline,
                )
                with pytest.raises(_RpcFailure):
                    await restored.sessions.recover(missing)
                assert restored._sessions[SID] is previous
                assert await restored.sessions.close(SID) == first
                assert len(peer.wallet.calls) == 1
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


def test_batch_preserves_duplicate_order_and_simulates_exact_sender_before_submission():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            _ = [chunk async for chunk in peer.chat.Completions(peer.request())]
            simulated = []

            def simulate(prepared, *, sender):
                assert not peer.wallet.calls
                simulated.append((prepared, sender))

            peer.facts.reader.simulate = simulate
            await peer.provider.sessions.settleBatch((SID, SID))
            prepared, sender = simulated[0]
            assert prepared.kind.value == "settleBatch"
            assert prepared.session_ids == (SID, SID)
            assert prepared.amounts == (20, 20)
            assert sender == peer.wallet.address
            assert len(peer.wallet.calls) == 1
            assert peer.wallet.calls[0][0] == prepared.call

    asyncio.run(scenario())


def test_empty_batch_is_valid_and_reconciles_original_intent():
    async def scenario():
        async with serving() as peer:
            calls = []
            peer.facts.reader.simulate = lambda prepared, *, sender: calls.append(prepared)
            peer.facts.reader.reconcile = lambda prepared, **kwargs: OperationResult(
                kwargs["operation_ref"],
                OperationState.UNKNOWN,
                kwargs["wallet_operation_id"],
                kwargs["tx_hash"],
            )
            first = await peer.provider.sessions.settleBatch(())
            assert (
                await peer.provider.sessions.settleBatch(())
            ).operation_ref == first.operation_ref
            assert len(peer.wallet.calls) == len(calls) == 1
            assert calls[0].session_ids == ()

    asyncio.run(scenario())


def test_batch_simulation_rejection_never_submits():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            _ = [chunk async for chunk in peer.chat.Completions(peer.request())]

            def fail(*args, **kwargs):
                raise RuntimeError("simulation failed")

            peer.facts.reader.simulate = fail
            with pytest.raises(RuntimeError, match="simulation failed"):
                await peer.provider.sessions.settleBatch((SID,))
            assert not peer.wallet.calls
            assert peer.provider._sessions[SID].operation is None

    asyncio.run(scenario())


def test_missing_current_offering_preserves_historical_capture_and_close():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=1010)
            _ = [chunk async for chunk in peer.chat.Completions(peer.request())]
            peer.facts.identity = replace(peer.facts.identity, catalog=None)
            with pytest.raises(grpc.aio.AioRpcError):
                _ = [chunk async for chunk in peer.chat.Completions(peer.request(rid=b"x" * 32))]
            assert len(peer.work.calls) == 1
            assert (await peer.provider.sessions.settle(SID)).state is OperationState.CONFIRMED
            assert (await peer.provider.sessions.close(SID)).state is OperationState.CONFIRMED

    asyncio.run(scenario())


def test_duplicate_batch_target_coverage_updates_unit_baseline_without_attributed_payment():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=10000)
            await peer.complete()
            peer.facts.reader.simulate = lambda *args, **kwargs: None

            def covered(prepared, *, operation_ref, tx_hash, wallet_operation_id=None):
                peer.facts.state = replace(peer.facts.state, settled_cumulative=20)
                return OperationResult(
                    operation_ref,
                    OperationState.UNKNOWN,
                    wallet_operation_id,
                    tx_hash,
                    peer.facts.block(peer.facts.height).observation,
                    tuple(
                        SessionOperationOutcome(sid, amount, True)
                        for sid, amount in zip(prepared.session_ids, prepared.amounts, strict=True)
                    ),
                )

            peer.facts.reader.reconcile = covered
            result = await peer.provider.sessions.settleBatch((SID, SID))
            assert result.state is OperationState.UNKNOWN
            assert all(item.settlement_reference is None for item in result.sessions)
            assert peer.provider._sessions[SID].confirmed_units == 20
            assert await peer.provider.sessions.settleBatch((SID, SID)) == result
            assert len(peer.wallet.calls) == 1
            checkpoint = await peer.provider.sessions.checkpoint(SID)
            restored = replacement(peer)
            await restored.start()
            try:
                await restored.sessions.recover(checkpoint)
                assert restored._sessions[SID].confirmed_units == 20
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


def test_pending_capture_preserves_original_unit_snapshot_while_new_usage_arrives():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=10000)
            await peer.complete()
            confirmed = False

            def reconcile(prepared, *, operation_ref, tx_hash, wallet_operation_id=None):
                if not confirmed:
                    return OperationResult(
                        operation_ref, OperationState.PENDING, wallet_operation_id, tx_hash
                    )
                peer.facts.state = replace(peer.facts.state, settled_cumulative=20)
                return OperationResult(
                    operation_ref,
                    OperationState.CONFIRMED,
                    wallet_operation_id,
                    tx_hash,
                    peer.facts.block(peer.facts.height).observation,
                    (SessionOperationOutcome(SID, 20, True),),
                )

            peer.facts.reader.reconcile = reconcile
            first = await peer.provider.sessions.settle(SID)
            assert first.state is OperationState.PENDING
            await peer.complete(peer.request(b"n" * 32))
            assert peer.provider._sessions[SID].units == 40
            confirmed = True
            second = await peer.provider.sessions.settle(SID)
            assert second.operation_ref == first.operation_ref
            assert peer.provider._sessions[SID].confirmed_units == 20
            assert len(peer.wallet.calls) == 1

    asyncio.run(scenario())


def second_session(peer):
    """Explicit independent canonical/session fixtures for batch coordinator tests."""
    first = peer.provider._sessions[SID]
    descriptor = replace(DESCRIPTOR, salt=b"s" * 32)
    second_id = session_id(DOMAIN, descriptor)
    runs = {}
    for rid, old in first.runs.items():
        context = replace(old.context, session_id=second_id)
        runs[rid] = replace(
            old,
            context=context,
            signature=SIGNER.sign_run(DOMAIN, context),
            accounting=RunAccounting(TokenCounts(20, 0, 20), 0, RunStatus.COMPLETED),
        )
    second = replace(
        first,
        sid=second_id,
        opening=replace(first.opening, descriptor=descriptor),
        runs=runs,
        voucher=pb.Voucher(
            session_id=second_id,
            cumulative=10000,
            signature=SIGNER.sign_voucher(DOMAIN, second_id, 10000),
        ),
        owner=None,
        operation=None,
        baseline=None,
        confirmed_units=0,
    )
    peer.provider._sessions[second_id] = second
    states = {SID: peer.facts.state, second_id: replace(peer.facts.state, settled_cumulative=0)}
    peer.facts.snapshot = lambda: replace(peer.facts.current, sessions=states)
    peer.facts.reader.simulate = lambda *args, **kwargs: None

    def reconcile(prepared, *, operation_ref, tx_hash, wallet_operation_id=None):
        for sid, amount in zip(prepared.session_ids, prepared.amounts, strict=True):
            states[sid] = replace(states[sid], settled_cumulative=amount)
            if sid == SID:
                peer.facts.state = states[sid]
        return OperationResult(
            operation_ref,
            OperationState.CONFIRMED,
            wallet_operation_id,
            tx_hash,
            peer.facts.block(peer.facts.height).observation,
            tuple(
                SessionOperationOutcome(sid, amount, True)
                for sid, amount in zip(prepared.session_ids, prepared.amounts, strict=True)
            ),
        )

    peer.facts.reader.reconcile = reconcile
    return second_id, states


def test_batch_supersedes_distinct_completed_captures_and_preserves_per_session_baselines():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=10000)
            await peer.complete()
            other, _ = second_session(peer)
            one = await peer.provider.sessions.settle(SID)
            two = await peer.provider.sessions.settle(other)
            assert one.operation_ref != two.operation_ref
            assert peer.provider._sessions[SID].confirmed_units == 20
            assert peer.provider._sessions[other].confirmed_units == 40
            result = await peer.provider.sessions.settleBatch((SID, other, SID))
            assert [item.session_id for item in result.sessions] == [SID, other, SID]
            assert len(peer.wallet.calls) == 3
            assert peer.provider._sessions[SID].confirmed_units == 20
            assert peer.provider._sessions[other].confirmed_units == 40

    asyncio.run(scenario())


def test_batch_reconciles_each_intersecting_pending_intent_before_new_execution():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=10000)
            await peer.complete()
            other, _ = second_session(peer)
            real = peer.facts.reader.reconcile
            peer.facts.reader.reconcile = lambda prepared, **kwargs: OperationResult(
                kwargs["operation_ref"],
                OperationState.PENDING,
                kwargs["wallet_operation_id"],
                kwargs["tx_hash"],
            )
            one = await peer.provider.sessions.settle(SID)
            two = await peer.provider.sessions.settle(other)
            peer.facts.reader.reconcile = real
            first = await peer.provider.sessions.settleBatch((SID, other))
            second = await peer.provider.sessions.settleBatch((SID, other))
            assert first.operation_ref == one.operation_ref
            assert second.operation_ref == two.operation_ref
            assert len(peer.wallet.calls) == 2
            third = await peer.provider.sessions.settleBatch((SID, other))
            assert third.operation_ref not in (one.operation_ref, two.operation_ref)
            assert len(peer.wallet.calls) == 3

    asyncio.run(scenario())


def test_automatic_batch_splits_rejected_simulation_and_collects_eligible_sessions():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=10000)
            await peer.complete()
            other, states = second_session(peer)

            def simulate(*args, **kwargs):
                raise ChainEvidenceError("batch exceeds available gas")

            peer.facts.reader.simulate = simulate
            peer.provider._schedule_batch_collection((SID, other))
            async with asyncio.timeout(1):
                while peer.provider._collecting:
                    await asyncio.sleep(0.01)
            assert len(peer.wallet.calls) == 2
            assert states[SID].settled_cumulative == 20
            assert states[other].settled_cumulative == 40

    asyncio.run(scenario())


def test_confirmed_close_remains_observable_when_contract_later_pauses():
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=10000)
            await peer.complete()
            result = await peer.provider.sessions.close(SID)
            peer.facts.pause = PauseLevel.ALL
            peer.facts.stale = True
            assert await peer.provider.sessions.close(SID) == result
            assert len(peer.wallet.calls) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ("runs", "reservations", "publications"))
def test_oversized_recovery_inputs_reject_before_chain_reads(kind):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=10000)
            await peer.complete()
            evidence = await peer.provider.sessions.checkpoint(SID)
            if kind == "runs":
                evidence = replace(evidence, runs=evidence.runs * ((16 << 20) // 32 + 1))
            elif kind == "reservations":
                evidence = replace(
                    evidence,
                    reservations=((evidence.runs[0].context, evidence.runs[0].accounting),) * 1025,
                )
            else:
                evidence = replace(
                    evidence,
                    publications=evidence.publications * (len(evidence.snapshot.catalogs) + 1),
                )
            restored = replacement(peer)
            await restored.start()
            peer.facts.reader.opening = lambda *args, **kwargs: pytest.fail(
                "resource overflow reached RPC"
            )
            try:
                with pytest.raises(ValidationError, match="resource"):
                    await restored.sessions.recover(evidence)
                assert not restored._sessions
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


@pytest.mark.parametrize("corrupt", (None, "publication", "current_hash", "current_catalog"))
def test_recovery_verifies_each_historical_price_publication_and_frozen_run(corrupt):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=10000)
            await peer.complete()
            original = peer.facts.identity
            changed_rates = replace(peer.facts.config.models[0].rates, output=5000000)
            changed = publish_catalog((CatalogRow("example/model", changed_rates),))
            peer.facts.height = 102
            second_identity = replace(
                original, observation=peer.facts.block(102).observation, catalog=changed
            )
            peer.facts.identity = second_identity
            await peer.complete(peer.request(b"b" * 32, rate_hash=changed.rate_card_hash))
            latest_rates = replace(changed_rates, output=7000000)
            latest = publish_catalog((CatalogRow("example/model", latest_rates),))
            peer.facts.height = 103
            peer.facts.identity = replace(
                original, observation=peer.facts.block(103).observation, catalog=latest
            )
            evidence = await peer.provider.sessions.checkpoint(SID)
            if corrupt == "publication":
                evidence = replace(
                    evidence,
                    publications=tuple(
                        row for row in evidence.publications if row[0] != changed.rate_card_hash
                    ),
                )
            elif corrupt == "current_hash":
                evidence.snapshot.current_rate_card_hash = changed.rate_card_hash
            elif corrupt == "current_catalog":
                for index, catalog in enumerate(evidence.snapshot.catalogs):
                    if catalog.rate_card_hash == latest.rate_card_hash:
                        del evidence.snapshot.catalogs[index]
                        break
                evidence = replace(
                    evidence,
                    publications=tuple(
                        row for row in evidence.publications if row[0] != latest.rate_card_hash
                    ),
                )
            cfg = replace(
                peer.provider.config,
                models=(replace(peer.provider.config.models[0], rates=latest_rates),),
            )
            restored = replacement(peer, cfg)
            peer.facts.reader.identity = lambda pid, block, **kwargs: (
                second_identity if block.observation.block_number == 102 else peer.facts.identity
            )
            await restored.start()
            try:
                if corrupt is not None:
                    with pytest.raises(_RpcFailure):
                        await restored.sessions.recover(evidence)
                    assert not restored._sessions
                else:
                    view = await restored.sessions.recover(evidence)
                    assert view.usage_amount == 80
                    assert {run.rates.output for run in restored._sessions[SID].runs.values()} == {
                        1000000,
                        5000000,
                    }
                    assert len(peer.work.calls) == 2
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


def test_empty_batch_retains_original_transaction_during_wallet_lookup_loss():
    async def scenario():
        async with serving() as peer:
            peer.facts.reader.simulate = lambda *args, **kwargs: None
            peer.facts.reader.reconcile = lambda prepared, **kwargs: OperationResult(
                kwargs["operation_ref"],
                OperationState.PENDING,
                kwargs["wallet_operation_id"],
                kwargs["tx_hash"],
            )
            first = await peer.provider.sessions.settleBatch(())

            def unavailable(**kwargs):
                raise RuntimeError("wallet unavailable")

            peer.wallet.reconcile = unavailable
            second = await peer.provider.sessions.settleBatch(())
            assert second.tx_hash == first.tx_hash
            assert second.wallet_operation_id == first.wallet_operation_id
            assert len(peer.wallet.calls) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("phase", ("simulation", "submission"))
def test_cancelled_collection_distinguishes_pre_submit_from_uncertain_execution(phase):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=10000)
            await peer.complete()
            entered, release = threading.Event(), threading.Event()
            peer.facts.reader.simulate = lambda *args, **kwargs: None
            if phase == "simulation":

                def simulate(*args, **kwargs):
                    entered.set()
                    assert release.wait(2)

                peer.facts.reader.simulate = simulate
            else:
                execute = peer.wallet.execute

                def submit(*args, **kwargs):
                    result = execute(*args, **kwargs)
                    entered.set()
                    assert release.wait(2)
                    return result

                peer.wallet.execute = submit
            task = asyncio.create_task(peer.provider.sessions.settleBatch((SID,)))
            try:
                assert await asyncio.to_thread(entered.wait, 2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                pending = peer.provider._sessions[SID].operation
                if phase == "simulation":
                    assert pending is None
                    assert not peer.wallet.calls
                else:
                    assert pending.result.state is OperationState.UNKNOWN
                    assert len(peer.wallet.calls) == 1
                    result = await peer.provider.sessions.settle(SID)
                    assert result.operation_ref == pending.reference
                    assert len(peer.wallet.calls) == 1
            finally:
                release.set()

    asyncio.run(scenario())


@pytest.mark.parametrize("baseline", (False, True))
def test_unsubmitted_batch_does_not_restore_a_phantom_wallet_operation(baseline):
    async def scenario():
        async with serving() as peer:
            await peer.attach(voucher=10000)
            await peer.complete()
            if baseline:
                second_session(peer)
                await peer.provider.sessions.settle(SID)
            previous = peer.provider._sessions[SID].baseline
            before_calls = len(peer.wallet.calls)

            def simulation_rejected(*args, **kwargs):
                raise ChainEvidenceError("pre-submission simulation rejected")

            peer.facts.reader.simulate = simulation_rejected
            with pytest.raises(ChainEvidenceError):
                await peer.provider.sessions.settleBatch((SID, SID))
            assert len(peer.wallet.calls) == before_calls
            checkpoint = await peer.provider.sessions.checkpoint(SID)
            assert checkpoint.operation is None
            assert peer.provider._sessions[SID].baseline is previous
            restored = replacement(peer)
            await restored.start()
            try:
                await restored.sessions.recover(checkpoint)
                assert restored._sessions[SID].operation is None
                if baseline:
                    assert restored._sessions[SID].confirmed_units == 20
                    assert checkpoint.baseline is not None
                    assert restored._sessions[SID].baseline.prepared == checkpoint.baseline.prepared
                else:
                    result = await restored.sessions.settle(SID)
                    assert result.state is OperationState.CONFIRMED
                    assert len(peer.wallet.calls) == before_calls + 1
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


@pytest.mark.parametrize("lookup", ("unavailable", "empty", "conflicting"))
def test_empty_batch_preserves_wallet_operation_identity_before_transaction_hash(lookup):
    async def scenario():
        async with serving() as peer:
            peer.facts.reader.simulate = lambda *args, **kwargs: None
            execute = peer.wallet.execute

            def hashless(*args, **kwargs):
                result = execute(*args, **kwargs)
                return replace(result, tx_hash=None)

            peer.wallet.execute = hashless
            first = await peer.provider.sessions.settleBatch(())
            assert first.wallet_operation_id == "wallet-123" and first.tx_hash is None

            def reconcile(*, operation_ref):
                if lookup == "unavailable":
                    raise RuntimeError("wallet unavailable")
                return OperationResult(
                    operation_ref,
                    OperationState.UNKNOWN,
                    "unrelated" if lookup == "conflicting" else None,
                )

            peer.wallet.reconcile = reconcile
            second = await peer.provider.sessions.settleBatch(())
            assert second.operation_ref == first.operation_ref
            assert second.wallet_operation_id == first.wallet_operation_id
            assert second.tx_hash is None
            assert len(peer.wallet.calls) == 1

    asyncio.run(scenario())


async def unused_checkpoint(peer):
    """Observe opening on real Control without authenticating an attachment."""
    await peer.control.write(
        pb.ControlMessage(session_open=pb.SessionOpen(session_id=SID, open_tx=OPEN_TX))
    )
    # The correlated reply is ordered after the opening on this stream.
    await peer.guidance(rid=b"\xf1" * 32)
    return await peer.provider.sessions.checkpoint(SID)


def test_unattached_checkpoint_restores_zero_until_first_authenticated_control():
    async def scenario():
        async with serving() as peer:
            evidence = await unused_checkpoint(peer)
            assert evidence.snapshot.control_generation == 0
            assert not evidence.snapshot.ready
            assert evidence.snapshot.meter_seq == 0
            assert not evidence.snapshot.HasField("accepted_voucher")
            assert not evidence.runs and not evidence.reservations
            peer.control.cancel()
            await peer.provider.shutdown(asyncio.get_running_loop().time() + 0.2)
            restored = replacement(peer)
            await restored.start()
            server = grpc.aio.server(options=GRPC_OPTIONS)
            restored.register_grpc(server)
            port = server.add_insecure_port("127.0.0.1:0")
            await server.start()
            channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}", options=GRPC_OPTIONS)
            fresh = Peer(channel, restored, peer.facts, peer.wallet, peer.work)
            try:
                view = await restored.sessions.recover(evidence)
                assert not view.readiness.ready and view.usage_amount == 0
                assert view.accepted_authorization is None
                assert restored._sessions[SID].generation == 0
                assert restored._sessions[SID].owner is None
                exported = await restored.sessions.checkpoint(SID)
                assert exported.snapshot == evidence.snapshot
                exported.snapshot.deposit += 1
                assert (await restored.sessions.checkpoint(SID)).snapshot == evidence.snapshot
                await fresh.start()
                assert fresh.nonce != peer.nonce
                with pytest.raises(grpc.aio.AioRpcError):
                    await fresh.complete(fresh.request(generation=1))
                assert not peer.work.calls
                state = await fresh.attach(voucher=1010)
                assert state.control_generation == 1 and state.ready
                chunks = await fresh.complete()
                assert chunks[-1].terminal.status == pb.RUN_STATUS_COMPLETED
                assert len(peer.work.calls) == 1
                assert not peer.wallet.calls
            finally:
                fresh.control.cancel()
                await channel.close()
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)
                await server.stop(None)

    asyncio.run(scenario())


def test_unattached_checkpoint_is_never_a_valid_wire_snapshot():
    async def scenario():
        async with serving() as peer:
            evidence = await unused_checkpoint(peer)
            validate_recovery_snapshot(evidence.snapshot)
            with pytest.raises(ValidationError):
                validate_message(evidence.snapshot)
            wrapped = pb.ControlMessage(session_state=evidence.snapshot)
            with pytest.raises(ValidationError):
                validate_message(wrapped)
            with pytest.raises(ValidationError):
                validate_recovery_snapshot(wrapped)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "change",
    (
        "ready",
        "nonce",
        "meter",
        "settled",
        "voucher",
        "usage",
        "run_state",
        "runs",
        "reservations",
        "confirmed_units",
        "missing_generation",
        "duplicate_catalog",
        "descriptor",
        "block",
        "chain_settlement",
    ),
)
def test_unattached_recovery_rejects_contradictory_history_atomically(change):
    async def scenario():
        async with serving() as peer:
            evidence = await unused_checkpoint(peer)
            context = RunContext(
                SID, b"\xe1" * 32, 1, "example/model", peer.facts.config.catalog.rate_card_hash
            )
            signature = SIGNER.sign_run(DOMAIN, context)
            if change == "ready":
                evidence.snapshot.ready = True
            elif change == "nonce":
                evidence.snapshot.nonce = b"\x01" * 32
            elif change == "meter":
                evidence.snapshot.meter_seq = 1
            elif change == "settled":
                evidence.snapshot.settled_cumulative = 1
            elif change == "voucher":
                evidence.snapshot.accepted_voucher.CopyFrom(peer.voucher(10))
            elif change == "usage":
                evidence.snapshot.usage_by_price.add(
                    model=context.model,
                    rate_card_hash=context.rate_card_hash,
                    input_tokens=0,
                    cache_tokens=0,
                    output_tokens=0,
                    acceptance_run_id=context.run_id,
                    acceptance_control_generation=1,
                    acceptance_signature=signature,
                )
            elif change == "run_state":
                evidence.snapshot.run_states.add(
                    run_id=context.run_id,
                    model="",
                    status=pb.RUN_STATUS_NOT_ADMITTED,
                    input_tokens=0,
                    cache_tokens=0,
                    output_tokens=0,
                )
            elif change == "runs":
                evidence = replace(
                    evidence,
                    runs=(
                        RecoveryRun(
                            context,
                            signature,
                            RunAccounting(TokenCounts(0, 0, 0), 0),
                            TokenCounts(0, 0, 0),
                            0,
                            False,
                            True,
                        ),
                    ),
                )
            elif change == "reservations":
                evidence = replace(
                    evidence, reservations=((context, RunAccounting(TokenCounts(0, 0, 0), 0)),)
                )
            elif change == "confirmed_units":
                evidence = replace(evidence, confirmed_units=1)
            elif change == "missing_generation":
                evidence.snapshot.ClearField("control_generation")
            elif change == "duplicate_catalog":
                evidence.snapshot.catalogs.add().CopyFrom(evidence.snapshot.catalogs[0])
            elif change == "descriptor":
                evidence.snapshot.descriptor.agent = b"\x12" * 20
            elif change == "block":
                evidence = replace(
                    evidence, observation=replace(evidence.observation, block_hash=b"\x12" * 32)
                )
            elif change == "chain_settlement":
                peer.facts.state = replace(peer.facts.state, settled_cumulative=1)
            restored = replacement(peer)
            await restored.start()
            try:
                with pytest.raises((ValidationError, _RpcFailure)):
                    await restored.sessions.recover(evidence)
                assert SID not in restored._sessions
                assert not peer.work.calls and not peer.wallet.calls
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


def test_unattached_checkpoint_cannot_replace_a_later_generation_or_close_intent():
    async def scenario():
        async with serving() as peer:
            evidence = await unused_checkpoint(peer)
            restore_reader(peer.facts)
            session = peer.provider._sessions[SID]
            session.close_pending, session.close_reference = True, "retained-close"
            with pytest.raises(_RpcFailure):
                await peer.provider.sessions.recover(evidence)
            assert session.close_pending and session.close_reference == "retained-close"
            closing = await peer.provider.sessions.checkpoint(SID)
            restored = replacement(peer)
            await restored.start()
            try:
                view = await restored.sessions.recover(closing)
                assert not view.readiness.ready
                assert restored._sessions[SID].close_reference == "retained-close"
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)
            session.close_pending, session.close_reference = False, None
            await peer.attach()
            with pytest.raises(_RpcFailure):
                await peer.provider.sessions.recover(evidence)
            assert peer.provider._sessions[SID].generation == 1
            peer.provider._detach(session.owner)
            with pytest.raises(_RpcFailure):
                await peer.provider.sessions.recover(evidence)
            assert peer.provider._sessions[SID].generation == 1
            assert not peer.wallet.calls and not peer.work.calls

    asyncio.run(scenario())


def test_unattached_historical_opening_without_private_history_stays_unavailable():
    async def scenario():
        async with serving() as peer:
            peer.facts.opening_block = 100
            with pytest.raises(grpc.aio.AioRpcError) as failure:
                await peer.attach()
            assert (
                parse_payment_error(failure.value.code(), failure.value.trailing_metadata()).reason
                == "STATE_UNAVAILABLE"
            )
            with pytest.raises(_RpcFailure):
                await peer.provider.sessions.checkpoint(SID)
            assert not peer.wallet.calls and not peer.work.calls

    asyncio.run(scenario())


def test_unattached_recovery_preserves_refund_only_operation_identity():
    async def scenario():
        async with serving() as peer:
            evidence = await unused_checkpoint(peer)
            evidence.snapshot.close_pending = True
            prepared = peer.provider.contract.close(DESCRIPTOR, 0, 0, b"")
            original = RecoveryOperation(
                prepared, OperationResult("unused-close", OperationState.UNKNOWN), ((SID, 0),)
            )
            evidence = replace(evidence, close_reference="unused-close", operation=original)
            restored = replacement(peer)
            await restored.start()
            try:
                view = await restored.sessions.recover(evidence)
                assert not view.readiness.ready
                retained = await restored.sessions.checkpoint(SID)
                assert retained.snapshot.control_generation == 0
                assert retained.snapshot.close_pending
                assert retained.close_reference == "unused-close"
                assert retained.operation == original
                assert not peer.wallet.calls and not peer.wallet.lookups
            finally:
                await restored.shutdown(asyncio.get_running_loop().time() + 0.2)

    asyncio.run(scenario())


def test_lifecycle_hooks_observe_committed_payment_flow_without_mutating_state():
    from mppi_provider import Hooks

    async def scenario():
        records = []

        def observe(name):
            def callback(value):
                records.append((name, value))
                if name == "voucher":
                    value.cumulative = 0

            return callback

        hooks = Hooks(
            **{
                "on_" + name: observe(name)
                for name in (
                    "challenge",
                    "session_open",
                    "voucher",
                    "meter",
                    "low_credit",
                    "drain",
                    "settle",
                    "close",
                    "error",
                )
            }
        )
        async with serving(hooks=hooks) as peer:
            await peer.guidance()
            await peer.attach()
            completion = asyncio.create_task(peer.complete())
            await peer.next_kind("low_credit")
            await peer.control.write(pb.ControlMessage(voucher=peer.voucher(1010)))
            await completion
            await peer.provider.sessions.settle(SID)
            with pytest.raises(grpc.aio.AioRpcError):
                await peer.complete()  # Same run id is rejected, never dispatched twice.
            await peer.provider.sessions.close(SID)
            for _ in range(100):
                if len({name for name, _ in records}) == 9:
                    break
                await asyncio.sleep(0.01)
            assert {name for name, _ in records} == {
                "challenge",
                "session_open",
                "voucher",
                "meter",
                "low_credit",
                "drain",
                "settle",
                "close",
                "error",
            }
            assert peer.provider._sessions[SID].voucher.cumulative == 1010
            assert (await peer.provider.sessions.get(SID)).usage_amount == 20
            assert len(peer.work.calls) == 1
            settled = next(value for name, value in records if name == "settle")
            assert settled.state is OperationState.CONFIRMED
            closed = next(value for name, value in records if name == "close")
            assert closed.capture_amount == 20
            assert peer.provider.hook_stats.failed == 0

    asyncio.run(scenario())
