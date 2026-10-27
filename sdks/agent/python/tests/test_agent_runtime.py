"""Agent Control independently exercised against a native gRPC wire peer.

These focused tests install a known signed local ledger to isolate renewal and
loss behavior. Full discovery/funding/attachment is covered by the separately
installed two-role acceptance test; these are not chain or wallet integration.
"""

import asyncio
import copy
import hashlib
import json
import os
import pickle
import shutil
import threading
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import astuple, replace
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, Mock, call, patch

import grpc
import pytest
from mppi.v1 import mppi_pb2 as pb
from mppi.v1 import mppi_pb2_grpc as services
from mppi_agent import (
    Client,
    ClientError,
    CloseAccountingError,
    CompletionStream,
    FundingAuthorizationError,
    FundingNotice,
    HookError,
    Hooks,
    PaymentError,
)
from mppi_agent import client as client_module
from mppi_agent.checkpoint import Checkpoint
from mppi_agent.client import Sessions, _Output
from mppi_agent.state import Ledger
from mppi_protocol import follower as follower_module
from mppi_protocol._observers import Observers
from mppi_protocol.amounts import Rates, TokenCounts, priced_numerator, required_coverage
from mppi_protocol.capabilities import FundingAuthorization
from mppi_protocol.catalog import CatalogRow, publish_catalog
from mppi_protocol.chain import (
    Block,
    ChainContradictionError,
    ChainEvidenceError,
    ChainLagError,
    ChainSession,
    ClosingEvidence,
    FundingState,
    OpeningEvidence,
)
from mppi_protocol.contract import ARC_USDC, ContractCalls, OperationKind
from mppi_protocol.discovery import ProviderIdentity
from mppi_protocol.errors import ValidationCode, ValidationError
from mppi_protocol.follower import ChainFollower, ChainSnapshot
from mppi_protocol.http import TransportError
from mppi_protocol.signing import LocalSessionSigner, verify_close_auth, verify_voucher
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
from mppi_protocol.wire import GRPC_OPTIONS, ByteQueue, payment_status

DOMAIN = Domain(5042002, b"p" * 20)
SIGNER = LocalSessionSigner(bytes.fromhex("11" * 32))
CATALOG = publish_catalog((CatalogRow("test/model", Rates(500_000, 500_000)),))
DESCRIPTOR = SessionDescriptor(
    b"a" * 20, SIGNER.address, 7, b"r" * 20, b"o" * 20, bytes.fromhex("36" + "00" * 19), b"s" * 32
)


def fundable(client: Client) -> Mock:
    """Give a component client its agent id and a funding check that passes:
    the snapshot binds the id to a funding wallet with code, which approves."""
    client.agent_id = 43
    check = Mock(return_value=True)
    client.chain.reader = SimpleNamespace(funding_signature=check)
    client.chain.current.funding = FundingState(43, DESCRIPTOR.agent, DESCRIPTOR.agent, True)
    return check


def component_client() -> Client:
    client = object.__new__(Client)
    client._observers = Observers(None)
    ledger = Ledger(DOMAIN, DESCRIPTOR, CATALOG)
    ledger.generation, ledger.ready = 1, True
    context = RunContext(ledger.session_id, b"r" * 32, 1, "test/model", CATALOG.rate_card_hash)
    ledger.register(context, SIGNER.sign_run(DOMAIN, context))
    state = ChainSession(100, 0, SessionStatus.OPEN, 0, 100, CATALOG.rate_card_hash)
    snapshot = SimpleNamespace(sessions={ledger.session_id: state})
    client.chain = SimpleNamespace(
        current=snapshot,
        snapshot=lambda: snapshot,
        readiness=lambda _: Readiness(True),
        confirmed_readiness=lambda _: Readiness(True),
        confirm=Mock(),
        follow=Mock(),
        start=AsyncMock(),
        unwatch_funding=Mock(),
    )
    client.signer = SIGNER
    client.timeout = 1
    client._ledger = ledger
    client._persist = AsyncMock()
    client._control_failure = None
    client._attached_generation = 1
    client._outbound = ByteQueue()
    client._voucher_lock = asyncio.Lock()
    client._voucher_headroom = 0
    client._background = set()
    client._pending_vouchers = {}
    client._voucher_task = client._renew_task = None
    client._queue_options = {"max_messages": 4, "max_bytes": 16384, "wait_ms": 50}
    client._nonce = client._snapshot_waiter = client._close_waiter = None
    client._snapshot_applied = None
    client._operation_locks, client._retiring = {}, None
    client._challenges = {}
    client._outputs = {}
    client._control = None
    client._on_low_credit = None
    client._funding = client._low_credit_task = None
    client._on_funding_notice = client._top_up_hint = None
    client._unsent = set()
    client._resync = None
    client._receipted, client._closed_result = set(), None
    client._funding_lock = asyncio.Lock()
    return client


def test_native_control_meter_renews_fixed_signer_authority_without_output_reads() -> None:
    async def scenario() -> None:
        client = component_client()
        ledger = client._ledger
        received = []
        finished = asyncio.Event()
        hold = asyncio.Event()

        class Peer(services.ControlServicer):
            async def Session(self, requests, context):
                yield pb.ControlMessage(
                    low_credit=pb.LowCredit(
                        session_id=ledger.session_id,
                        model="test/model",
                        rate_card_hash=CATALOG.rate_card_hash,
                        remaining_tokens=0,
                        suggest_top_up_tokens=10,
                        coverage_request=pb.CoverageRequest(
                            run_id=b"r" * 32,
                            control_generation=1,
                            required_by_price=[
                                pb.RequiredTokens(
                                    model="test/model",
                                    rate_card_hash=CATALOG.rate_card_hash,
                                    input_tokens=4,
                                    cache_tokens=0,
                                    output_tokens=3,
                                )
                            ],
                        ),
                    )
                )
                first = await anext(requests)
                received.append(first.voucher)
                assert first.voucher.cumulative == 4
                verify_voucher(DOMAIN, DESCRIPTOR, 4, first.voucher.signature)
                yield pb.ControlMessage(
                    meter=pb.Meter(
                        session_id=ledger.session_id,
                        run_id=b"r" * 32,
                        seq=1,
                        input_tokens=4,
                        cache_tokens=0,
                        output_tokens=3,
                    )
                )
                second = await anext(requests)
                received.append(second.voucher)
                assert second.voucher.cumulative == 5
                verify_voucher(DOMAIN, DESCRIPTOR, 5, second.voucher.signature)
                finished.set()
                await hold.wait()

        server = grpc.aio.server(options=GRPC_OPTIONS)
        services.add_ControlServicer_to_server(Peer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}", options=GRPC_OPTIONS)
        client._control = services.ControlStub(channel).Session()
        tasks = [
            asyncio.create_task(client._control_read()),
            asyncio.create_task(client._control_write()),
        ]
        try:
            await asyncio.wait_for(finished.wait(), 5)
            assert [voucher.cumulative for voucher in received] == [4, 5]
            assert ledger.usage_amount == 3
            assert client._persist.await_count >= 3
        finally:
            client._control.cancel()
            for task in tasks + list(client._background):
                task.cancel()
            await asyncio.gather(*tasks, *client._background, return_exceptions=True)
            await channel.close()
            await server.stop(0)

    asyncio.run(scenario())


def test_native_control_session_drain_emits_typed_hook_and_stops_authorization() -> None:
    async def scenario() -> None:
        client = component_client()
        ledger = client._ledger
        ledger.coverage_numerator = 4_000_000
        assert client._require_ready() is ledger
        observed, delivered = [], threading.Event()
        hold = asyncio.Event()

        def on_drain(notice):
            observed.append(notice)
            delivered.set()

        client._observers = Observers(Hooks(on_drain=on_drain))
        drain = pb.Drain(
            session_id=ledger.session_id,
            reason=pb.DRAIN_REASON_PROVIDER_INITIATED,
        )

        class Peer(services.ControlServicer):
            async def Session(self, requests, context):
                yield pb.ControlMessage(drain=drain)
                await hold.wait()  # Keep Control alive: EOF must not cause the denial.

        server = grpc.aio.server(options=GRPC_OPTIONS)
        services.add_ControlServicer_to_server(Peer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}", options=GRPC_OPTIONS)
        client._control = services.ControlStub(channel).Session()
        reader = asyncio.create_task(client._control_read())
        try:
            assert await asyncio.to_thread(delivered.wait, 2)
            assert len(observed) == 1 and isinstance(observed[0], pb.Drain)
            assert observed[0] == drain
            assert observed[0].session_id == ledger.session_id
            assert not observed[0].HasField("run_id")
            assert client._control_failure is None and not reader.done()
            assert not ledger.ready and ledger.runs[b"r" * 32].failed_transport
            client._persist.assert_awaited_once()
            with pytest.raises(ClientError, match="authenticated Control is not ready"):
                client._require_ready()
            client._voucher = AsyncMock()
            await client._renew()
            client._voucher.assert_not_awaited()
        finally:
            client._control.cancel()
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
            client._observers.close()
            await channel.close()
            await server.stop(0)

    asyncio.run(scenario())


def test_low_credit_hook_cannot_stall_control_or_mutate_its_payment_records() -> None:
    async def scenario() -> None:
        client = component_client()
        entered, release = threading.Event(), threading.Event()
        observed = []

        def callback(notice):
            observed.append(notice.remaining_tokens)
            notice.session_id = bytes(32)
            entered.set()
            assert release.wait(3)

        client._on_low_credit = callback
        ledger = client._ledger
        ledger.accept_voucher(
            pb.Voucher(
                session_id=ledger.session_id,
                cumulative=4,
                signature=SIGNER.sign_voucher(DOMAIN, ledger.session_id, 4),
            ),
            100,
        )
        ledger.coverage_numerator = 4_000_000
        renewed = asyncio.Event()
        finish = asyncio.Event()

        class Peer(services.ControlServicer):
            async def Session(self, requests, context):
                yield pb.ControlMessage(
                    low_credit=pb.LowCredit(
                        session_id=ledger.session_id,
                        model="test/model",
                        rate_card_hash=CATALOG.rate_card_hash,
                        remaining_tokens=1,
                        suggest_top_up_tokens=10,
                    )
                )
                assert await asyncio.to_thread(entered.wait, 1)
                yield pb.ControlMessage(
                    meter=pb.Meter(
                        session_id=ledger.session_id,
                        run_id=b"r" * 32,
                        seq=1,
                        input_tokens=4,
                        cache_tokens=0,
                        output_tokens=2,
                    )
                )
                voucher = (await anext(requests)).voucher
                assert voucher.session_id == ledger.session_id and voucher.cumulative == 5
                renewed.set()
                await finish.wait()

        server = grpc.aio.server(options=GRPC_OPTIONS)
        services.add_ControlServicer_to_server(Peer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}", options=GRPC_OPTIONS)
        client._control = services.ControlStub(channel).Session()
        tasks = [
            asyncio.create_task(client._control_read()),
            asyncio.create_task(client._control_write()),
        ]
        try:
            await asyncio.wait_for(renewed.wait(), 2)
            assert not release.is_set() and observed == [1]
            assert client._control_failure is None and ledger.voucher.cumulative == 5
        finally:
            release.set()
            client._control.cancel()
            for task in tasks + list(client._background):
                task.cancel()
            await asyncio.gather(*tasks, *client._background, return_exceptions=True)
            await channel.close()
            await server.stop(0)

    asyncio.run(scenario())


def test_undelivered_low_credit_notices_coalesce_without_waiting_on_control() -> None:
    async def scenario() -> None:
        client = component_client()
        entered, release, received = threading.Event(), threading.Event(), []

        def callback(notice):
            received.append(notice.remaining_tokens)
            entered.set()
            assert release.wait(2)

        client._on_low_credit = callback
        notice = pb.LowCredit(
            session_id=client._ledger.session_id,
            model="test/model",
            rate_card_hash=CATALOG.rate_card_hash,
            remaining_tokens=1,
            suggest_top_up_tokens=10,
        )
        client._notify_low_credit(notice, 100, None)
        assert await asyncio.to_thread(entered.wait, 1)
        try:
            for remaining in range(2, 202):  # Never blocks, never fills a queue.
                notice.remaining_tokens = remaining
                client._notify_low_credit(notice, 100, None)
            assert len(client._background) == 1
        finally:
            release.set()
        await asyncio.wait_for(client._low_credit_task, 2)
        assert received == [1, 201]  # Only the latest undelivered notice.

    asyncio.run(scenario())


def test_control_loss_discards_queued_low_credit_callbacks() -> None:
    async def scenario() -> None:
        client = component_client()
        entered, release = threading.Event(), threading.Event()
        received = []

        def callback(notice):
            received.append(notice.remaining_tokens)
            entered.set()
            assert release.wait(2)

        client._on_low_credit = callback
        notice = pb.LowCredit(
            session_id=client._ledger.session_id,
            model="test/model",
            rate_card_hash=CATALOG.rate_card_hash,
            remaining_tokens=1,
        )
        client._notify_low_credit(notice, 100, None)
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            notice.remaining_tokens = 2
            client._notify_low_credit(notice, 100, None)
            client._control_lost(ClientError("fixture disconnect"))
            release.set()
            await asyncio.gather(client._low_credit_task, return_exceptions=True)
            assert received == [1] and not client._ledger.ready
        finally:
            release.set()

    asyncio.run(scenario())


@pytest.mark.parametrize("reconciled", (False, True))
@pytest.mark.parametrize(
    "state", (OperationState.CONFIRMED, OperationState.PENDING, OperationState.UNKNOWN)
)
def test_top_up_renews_retained_meter_target_only_after_confirmation(state, reconciled) -> None:
    async def scenario() -> None:
        client = component_client()
        ledger = client._ledger
        ledger.accept_voucher(
            pb.Voucher(
                session_id=ledger.session_id,
                cumulative=1,
                signature=SIGNER.sign_voucher(DOMAIN, ledger.session_id, 1),
            ),
            1,
        )
        ledger.coverage_numerator = 2_000_000
        block = Block(
            ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32),
            100,
            b"c" * 32,
        )
        snapshot = client.chain.current
        snapshot.block = block
        snapshot.sessions[ledger.session_id] = ChainSession(
            1, 0, SessionStatus.OPEN, 0, 100, CATALOG.rate_card_hash
        )

        async def refresh():
            snapshot.sessions[ledger.session_id] = ChainSession(
                2, 0, SessionStatus.OPEN, 0, 100, CATALOG.rate_card_hash
            )

        client.chain.refresh = AsyncMock(side_effect=refresh)
        client._local_start = AsyncMock()
        client._open_lock = asyncio.Lock()
        client._operations, client._saved = {}, {}
        client._control = object()
        client.contract = ContractCalls(DOMAIN, "USDC", "2")
        client.wallet = SimpleNamespace(
            address=DESCRIPTOR.agent, sign_funding_authorization=lambda _: b"token-signature"
        )
        fundable(client)
        result = OperationResult(
            "top-up-test",
            state,
            "wallet",
            b"t" * 32,
            block.observation if state is OperationState.CONFIRMED else None,
        )
        client._submit = AsyncMock(return_value=result)
        await client._renew()
        assert ledger.voucher.cumulative == 1
        if reconciled:
            auth = client._funding_authorization(1)
            operation = client.contract.top_up(ledger.descriptor, 1, auth, b"token-signature")
            client._operations[result.operation_ref] = (
                operation,
                OperationResult(result.operation_ref, OperationState.PENDING, "wallet", b"t" * 32),
            )
            client.chain.reader = SimpleNamespace(reconcile=Mock(return_value=result))
            assert await client._reconcile_ref(result.operation_ref) == result
            client._submit.assert_not_awaited()
        else:
            assert await client._top_up(ledger.session_id, 1) == result
        if state is OperationState.CONFIRMED:
            assert client._renew_task is not None
            await client._renew_task
            notice = pb.ControlMessage.FromString(await client._outbound.get())
            voucher = pb.ControlMessage.FromString(await client._outbound.get()).voucher
            assert notice.top_up_notice.tx == result.tx_hash
            assert voucher.cumulative == 2
            verify_voucher(DOMAIN, DESCRIPTOR, 2, voucher.signature)
        else:
            assert ledger.voucher.cumulative == 1 and client._renew_task is None
            client.chain.refresh.assert_not_awaited()

    asyncio.run(scenario())


@pytest.mark.parametrize("cause", ["cancel", "generation", "unready", "close"])
def test_late_signer_response_cannot_authorize_ineligible_run(cause: str) -> None:
    async def scenario() -> None:
        client = component_client()
        entered, release = threading.Event(), threading.Event()

        class DelayedSigner:
            address = SIGNER.address

            def sign_voucher(self, domain, session_id, amount):
                entered.set()
                assert release.wait(2)
                return SIGNER.sign_voucher(domain, session_id, amount)

        client.signer = DelayedSigner()
        task = asyncio.create_task(client._voucher(4, generation=1, run_id=b"r" * 32))
        assert await asyncio.to_thread(entered.wait, 1)
        if cause == "cancel":
            client._ledger.runs[b"r" * 32].failed_transport = True
        elif cause == "generation":
            client._ledger.generation = 2
        elif cause == "unready":  # The last confirmed state refuses, such as a close.
            client.chain.confirmed_readiness = lambda _: Readiness(False)
        else:
            client._ledger.close_pending = True
        release.set()
        await task
        assert client._ledger.voucher is None
        client._persist.assert_not_awaited()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(client._outbound.get(), 0.01)

    asyncio.run(scenario())


def test_background_proof_tasks_have_a_hard_capacity() -> None:
    async def scenario() -> None:
        client = component_client()
        gate = asyncio.Event()
        for _ in range(4):
            client._spawn(gate.wait())
        with pytest.raises(ClientError, match="bounded"):
            client._spawn(gate.wait())
        assert len(client._background) == 4
        gate.set()
        await asyncio.gather(*client._background)

    asyncio.run(scenario())


def test_client_owns_loop_and_cleanup_releases_checkpoint(tmp_path: Path) -> None:
    stopped = threading.Event()

    async def stop() -> None:
        stopped.set()

    chain = SimpleNamespace(
        reader=SimpleNamespace(profile=SimpleNamespace(domain=DOMAIN)), stop=stop
    )
    client = Client(
        7,
        wallet=SimpleNamespace(address=DESCRIPTOR.agent),
        signer=SIGNER,
        chain=chain,
        contract=ContractCalls(DOMAIN, "USDC", "2"),
        checkpoint_path=tmp_path / "agent.json",
        signer_reference="fixture:public-test-key",
    )
    assert client._thread.is_alive()
    result = client.close()
    assert result.unresolved_runs == () and result.pending_operations == ()
    assert stopped.is_set() and not client._thread.is_alive()
    assert client.close() == result
    checkpoint = Checkpoint(tmp_path / "agent.json")
    checkpoint.close()
    with pytest.raises(ClientError):
        client.sessions.list()


def test_cleanup_still_stops_loop_and_unlocks_after_persistence_failure(tmp_path: Path) -> None:
    stopped = threading.Event()

    async def stop() -> None:
        stopped.set()

    chain = SimpleNamespace(
        reader=SimpleNamespace(profile=SimpleNamespace(domain=DOMAIN)), stop=stop
    )
    client = Client(
        7,
        wallet=SimpleNamespace(address=DESCRIPTOR.agent),
        signer=SIGNER,
        chain=chain,
        contract=ContractCalls(DOMAIN, "USDC", "2"),
        checkpoint_path=tmp_path / "agent.json",
        signer_reference="fixture:public-test-key",
    )
    client._persist = AsyncMock(side_effect=OSError("disk"))
    client._restored = True
    with pytest.raises(OSError):
        client.close()
    assert stopped.is_set() and not client._thread.is_alive()
    checkpoint = Checkpoint(tmp_path / "agent.json")
    checkpoint.close()


@pytest.mark.parametrize("attempt_restore", [False, True])
def test_unused_or_rejected_restoration_preserves_existing_checkpoint_bytes(
    tmp_path: Path, attempt_restore: bool
) -> None:
    path = tmp_path / "agent.json"
    checkpoint = Checkpoint(path)
    checkpoint.save(
        {
            "version": 1,
            "scope": {"provider": 8},
            "operations": [{"ref": "unresolved-funding-must-survive"}],
        }
    )
    checkpoint.close()
    original = path.read_bytes()
    chain = SimpleNamespace(
        reader=SimpleNamespace(profile=SimpleNamespace(domain=DOMAIN)), stop=AsyncMock()
    )
    client = Client(
        7,
        wallet=SimpleNamespace(address=DESCRIPTOR.agent),
        signer=SIGNER,
        chain=chain,
        contract=ContractCalls(DOMAIN, "USDC", "2"),
        checkpoint_path=path,
        signer_reference="fixture:public-test-key",
    )
    try:
        if attempt_restore:
            with pytest.raises(ClientError, match="scope"):
                client.sessions.list()
    finally:
        client.close()
    assert path.read_bytes() == original
    assert not client._thread.is_alive()


@pytest.mark.parametrize("capture", [1, 2, 3])
def test_canonical_close_excess_is_reported_without_erasing_confirmation(capture: int) -> None:
    async def scenario() -> None:
        client = component_client()
        ledger = client._ledger
        ledger.accept_voucher(
            pb.Voucher(
                session_id=ledger.session_id,
                cumulative=100,
                signature=SIGNER.sign_voucher(DOMAIN, ledger.session_id, 100),
            ),
            100,
        )
        ledger.apply_meter(
            pb.Meter(
                session_id=ledger.session_id,
                run_id=b"r" * 32,
                seq=1,
                input_tokens=4,
                cache_tokens=0,
                output_tokens=0,
            ),
            100,
        )
        ledger.close_pending = True
        observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32)
        block = Block(observation, 100, b"c" * 32)
        tx = b"t" * 32
        proof = ClosingEvidence(block.observation, capture, 100 - capture, tx)
        client.chain.reader = SimpleNamespace(closing=lambda *_: proof)
        client.chain.refresh = AsyncMock()
        client._close_waiter = asyncio.get_running_loop().create_future()
        client._saved = {"version": 1}
        client._closed_error = client._closed_result = None
        message = pb.CloseResult(
            session_id=ledger.session_id, tx_hash=tx, capture_amount=capture, refunded=100 - capture
        )
        if capture > 2:
            with pytest.raises(CloseAccountingError) as failure:
                await client._close_result(message)
            assert failure.value.operation.state is OperationState.CONFIRMED
            assert failure.value.operation.tx_hash == tx
            assert failure.value.capture_amount == 3 and failure.value.usage_amount == 2
        else:
            await client._close_result(message)
            assert client._close_waiter.result().state is OperationState.CONFIRMED
        assert client._closed_result.state is OperationState.CONFIRMED
        assert client._saved["confirmed_close"]["capture"] == capture
        assert "reported_close" not in client._saved
        # Once on arrival, before any read; once confirmed.
        assert client._persist.await_count == 2
        # The SessionClosed event proves the close: no refresh follows it.
        client.chain.refresh.assert_not_awaited()
        ((sid, closed), _) = client.chain.confirm.call_args
        assert sid == ledger.session_id and closed.status is SessionStatus.CLOSED
        assert closed.settled_cumulative == capture

    asyncio.run(scenario())


def test_a_close_whose_proof_cannot_be_read_stays_pending_and_is_never_requested_again():
    """The provider made the close canonical, but the proof read failed."""

    async def scenario() -> None:
        client = component_client()
        ledger = client._ledger
        identifier = ledger.session_id
        observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32)
        tx = b"t" * 32
        proof = ClosingEvidence(observation, 0, 100, tx)
        reads = Mock(side_effect=[TransportError("HTTP request failed", transient=True), proof])
        client.chain.reader = SimpleNamespace(closing=reads)
        client.chain.refresh = AsyncMock()
        client._saved = {"version": 1}
        client._closed_error = client._closed_result = None
        client._local_start = client._ensure_close_attachment = AsyncMock()
        client._open_lock, client._funding_lock = asyncio.Lock(), asyncio.Lock()
        client._loop = asyncio.get_running_loop()
        requests, reply = (
            [],
            pb.CloseResult(session_id=identifier, tx_hash=tx, capture_amount=0, refunded=100),
        )

        async def send(message):
            if message.WhichOneof("kind") == "close_request":
                requests.append(message)
                client._spawn(client._close_result(reply))  # The provider answers.

        client._send = AsyncMock(side_effect=send)
        first = await client._cooperative_close(identifier)
        assert (first.state, first.tx_hash) == (OperationState.PENDING, tx)
        assert ledger.close_pending and client._saved["reported_close"]["tx"] == tx.hex()
        again = await client._cooperative_close(identifier)
        assert again.state is OperationState.CONFIRMED and again.tx_hash == tx
        assert len(requests) == 1  # The repeat proved it; it sent nothing.
        assert await client._cooperative_close(identifier) is again
        assert await client._reconcile_ref("close-" + identifier.hex()) is again

    asyncio.run(scenario())


class _FreshOpening(Exception):
    """The opening after a retirement reached its new Challenge."""


def exit_attempt(ref: str, attempt: int, state: OperationState) -> OperationResult:
    """The result an exit attempt was saved with: CONFIRMED with its receipt's
    block, FAILED mined and reverted, PENDING at the wallet with no hash yet,
    or UNKNOWN with no hash, as after a crash between journal and wallet."""
    tx = b"e" * 32 if attempt == 0 else b"e" * 31 + bytes([attempt])
    observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32)
    if state is OperationState.CONFIRMED:
        return OperationResult(ref, state, tx_hash=tx, observation=observation)
    if state is OperationState.FAILED:
        return OperationResult(ref, state, f"w-{attempt + 1}", tx)
    if state is OperationState.PENDING:
        return OperationResult(ref, state, f"w-{attempt + 1}")
    return OperationResult(ref, state)


def closed_client(
    tmp_path: Path,
    kind: str,
    settled: int = 0,
    exits: tuple[OperationState, ...] = (OperationState.CONFIRMED,),
) -> tuple[Client, bytes, Path]:
    """A restored client whose session this agent itself closed (or did not).
    For an exit ``kind``, ``exits`` are its attempts' states in order, under
    <kind>-<session id hex>, then .1, .2 and so on (exit_attempt)."""
    path = tmp_path / "agent.json"
    contract = ContractCalls(DOMAIN, "USDC", "2")
    observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32)
    client = component_client()
    client.contract, client.wallet = contract, SimpleNamespace(address=DESCRIPTOR.agent)
    client.signer_reference = "fixture:public-test-key"
    ledger = client._ledger
    ledger.close_pending, ledger.ready = True, False
    client._saved = {"version": 2, "scope": client._scope(7)}
    opening = OperationResult(
        "open-x", OperationState.CONFIRMED, tx_hash=b"o" * 32, observation=observation
    )
    auth = FundingAuthorization(
        DOMAIN.chain_id,
        ARC_USDC,
        "USDC",
        "2",
        DESCRIPTOR.agent,
        DOMAIN.verifying_contract,
        100,
        0,
        2_000_000_000,
        b"n" * 32,
    )
    client._opening_ref = "open-x"
    client._operations = {
        "open-x": (contract.open(DESCRIPTOR, 100, CATALOG.rate_card_hash, auth, bytes(65)), opening)
    }
    if kind == "cooperative":
        client._saved["confirmed_close"] = {
            "tx": (b"c" * 32).hex(),
            "block": 10,
            "block_hash": (b"b" * 32).hex(),
            "capture": 0,
            "refunded": 100,
        }
    elif kind == "excess":
        client._saved["confirmed_close"] = {
            "tx": (b"c" * 32).hex(),
            "block": 10,
            "block_hash": (b"b" * 32).hex(),
            "capture": 1,
            "refunded": 99,
        }
    elif kind == "reported":
        client._saved["reported_close"] = {"tx": (b"c" * 32).hex(), "capture": 0, "refunded": 100}
    elif kind in ("withdraw", "releaseIdle", "requestClose"):
        call = getattr(
            contract,
            {"releaseIdle": "release_idle", "requestClose": "request_close"}.get(kind, kind),
        )(DESCRIPTOR)
        base = kind + "-" + ledger.session_id.hex()
        for attempt, state in enumerate(exits):
            ref = base if attempt == 0 else f"{base}.{attempt}"
            client._operations[ref] = (call, exit_attempt(ref, attempt, state))
    checkpoint = Checkpoint(path)
    checkpoint.save(client._record())
    checkpoint.close()
    return restored_client(path, settled), ledger.session_id, path


def restored_client(path: Path, settled: int = 0) -> Client:
    """A new process restoring the checkpoint at ``path``, as from a CLI run
    after the close, with fresh evidence that shows the session Closed."""
    contract = ContractCalls(DOMAIN, "USDC", "2")
    observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32)
    restored = component_client()
    restored.contract = contract
    restored.wallet = SimpleNamespace(address=DESCRIPTOR.agent)
    restored.signer_reference = "fixture:public-test-key"
    restored._checkpoint, restored._checkpoint_lock = Checkpoint(path), asyncio.Lock()
    restored._saved = restored._checkpoint.load()
    restored._ledger, restored._operations = None, {}
    restored._closed_error = None
    restored._restore_local(7)
    del restored._persist  # The real journal writes.
    restored.agent_id, restored.initial_session_tokens = 43, 10
    restored._start = restored._local_start = restored._connect = AsyncMock()
    restored._challenge = AsyncMock(side_effect=_FreshOpening)
    restored._open_lock = asyncio.Lock()
    # Fresh evidence at the exit's block, where the closed session's settled total
    # is final; a closed session never changes, so a later block shows the same.
    closed = ChainSession(100, settled, SessionStatus.CLOSED, 0, 100, CATALOG.rate_card_hash)
    restored.chain.current = SimpleNamespace(
        block=Block(observation, 100, b"c" * 32),
        sessions={restored._ledger.session_id: closed},
        pause_level=PauseLevel.NONE,
    )
    restored.chain.snapshot = lambda: restored.chain.current
    restored.chain.refresh = AsyncMock(return_value=restored.chain.current)
    restored.chain.unwatch_session = Mock()
    restored.chain.reader = SimpleNamespace()
    return restored


async def explicitly_open(client: Client):
    """client.sessions.open, run on the test's own loop."""
    client.provider, client._identity = 7, None
    client._sync = lambda work: work
    return await Sessions(client).open(7, model="test/model")


@pytest.mark.parametrize("kind", ["cooperative", "withdraw", "releaseIdle"])
@pytest.mark.parametrize("archived_before", [False, True], ids=["first", "retried"])
def test_an_agents_own_close_retires_its_session_so_the_next_opening_funds_another(
    tmp_path: Path, kind: str, archived_before: bool
) -> None:
    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, kind)
        history = tmp_path / f"agent.{identifier.hex()}.closed.json"
        before = json.loads(path.read_text())
        if archived_before:  # A crash after the link, before the journal dropped it.
            history.hardlink_to(path)
        with pytest.raises(_FreshOpening):
            await client._open("test/model", None)
        # Its whole last record stays readable beside the checkpoint.
        assert json.loads(history.read_text()) == before
        assert history.stat().st_mode & 0o777 == 0o600
        after = json.loads(path.read_text())
        assert "ledger" not in after and "confirmed_close" not in after
        assert after["operations"] == [] and after["opening_ref"] is None
        assert after["scope"] == before["scope"]
        assert client._ledger is None and client._closed_result is None
        assert client._operations == {} and client._opening_ref is None
        # Refreshes stop reading it; only a new Control stream carries the next one.
        client.chain.unwatch_session.assert_called_once_with(identifier)
        assert isinstance(client._control_failure, ClientError)
        client._connect.assert_awaited()
        client._challenge.assert_awaited_once_with("test/model")
        client._checkpoint.close()

    asyncio.run(scenario())


def test_an_operation_without_a_transaction_hash_is_never_handed_to_the_wallets_retire(
    tmp_path: Path,
) -> None:
    """A retirement hands the wallet's retire only references that hold a
    transaction hash. A top-up that FAILED before any transaction stays in the
    wallet's journal, which may yet hold a hash the agent never learned."""

    async def scenario() -> None:
        client, _, _ = closed_client(tmp_path, "cooperative")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        auth = FundingAuthorization(
            DOMAIN.chain_id,
            ARC_USDC,
            "USDC",
            "2",
            DESCRIPTOR.agent,
            DOMAIN.verifying_contract,
            10,
            0,
            2_000_000_000,
            b"t" * 32,
        )
        client._operations["top-up-x"] = (
            client.contract.top_up(DESCRIPTOR, 10, auth, bytes(65)),
            OperationResult("top-up-x", OperationState.FAILED),
        )
        await client._persist()
        forgotten: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: forgotten.append(set(operation_refs))
        with pytest.raises(_FreshOpening):
            await client._open("test/model", None)
        assert forgotten == [{"open-x"}]
        assert client._operations == {}
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("refused", [False, True], ids=["forgotten", "kept"])
def test_an_agents_retired_session_leaves_its_wallet_journal_too(
    tmp_path: Path, refused: bool
) -> None:
    """Once the checkpoint names none of its operations, a wallet that can
    forget them does: its journal no longer grows with every closed session.
    A wallet that cannot leaves them, and the next opening goes on."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "withdraw")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        references = set(client._operations)
        assert len(references) == 2  # The opening and the withdrawal.
        forgotten: list[tuple[set[str], float]] = []

        def retire(*, operation_refs: Iterable[str], at: float) -> None:
            # After the save that drops them, before the new Control stream.
            assert json.loads(path.read_text())["operations"] == []
            client._connect.assert_not_awaited()
            forgotten.append((set(operation_refs), at))
            if refused:
                raise OSError("journal held elsewhere")

        client.wallet.retire = retire
        client._observers = Mock()
        before = time.time()
        with pytest.raises(_FreshOpening):
            await client._open("test/model", None)
        ((refs, at),) = forgotten
        assert refs == references and before <= at <= time.time()  # Unix seconds.
        errors = [
            item.args[1]
            for item in client._observers.emit.call_args_list
            if item.args[0] == "on_error"
        ]
        assert errors == ([HookError("retire", "OSError", identifier)] if refused else [])
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_retirement_whose_new_control_stream_fails_has_already_retired_its_records(
    tmp_path: Path,
) -> None:
    """The save and the wallet's retire come before the new Control stream:
    one that cannot begin (here its CA file is unreadable) leaves the session
    retired and its records forgotten, and the next opening begins it. Before,
    the stream came first, and its failure left the references unretired for
    good once the next opening saved a checkpoint without them."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "withdraw")
        references = set(client._operations)
        forgotten: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: forgotten.append(set(operation_refs))
        client._connect = AsyncMock(side_effect=[OSError("CA file unreadable"), None])
        with pytest.raises(OSError, match="CA file unreadable"):
            await client._open("test/model", None)
        assert forgotten == [references]
        saved = json.loads(path.read_text())
        assert "ledger" not in saved and saved["operations"] == []
        assert (tmp_path / f"agent.{identifier.hex()}.closed.json").exists()
        with pytest.raises(_FreshOpening):
            await client._open("test/model", None)
        assert forgotten == [references]  # Nothing is left to retire.
        assert client._connect.await_count == 2
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["withdraw", "none"], ids=["own_exit", "closed_by_other"])
def test_a_retirement_whose_save_fails_keeps_the_session_for_the_next_open(
    tmp_path: Path, kind: str
) -> None:
    """The save that drops the closed session fails. Memory is restored, as an
    attachment's failed save restores its ledger, so every later save still
    names the session and the next open retries the whole retirement,
    retiring the wallet's references. Before, memory was already cleared: the
    next save wrote a checkpoint without the session, and its references were
    never retired."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, kind)
        if kind == "none":
            client._ledger.close_pending = False
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        references = set(client._operations)
        forgotten: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: forgotten.append(set(operation_refs))
        save, failing = client._checkpoint.save, [True]

        def save_once_failing(record: dict) -> None:
            if "ledger" not in record and failing:  # The save that drops the session.
                failing.clear()
                raise ValidationError(
                    ValidationCode.INVALID_VALUE, "checkpoint could not be durably written"
                )
            save(record)

        client._checkpoint.save = save_once_failing

        def opening():
            return explicitly_open(client) if kind == "none" else client._open("test/model", None)

        with pytest.raises(ValidationError, match="durably written"):
            await opening()
        assert forgotten == []
        assert client._ledger is not None and client._ledger.session_id == identifier
        assert set(client._operations) == references and client._opening_ref == "open-x"
        assert "ledger" in client._saved
        client.chain.unwatch_session.assert_not_called()
        await client._persist()  # Any later save, as the next opening's.
        saved = json.loads(path.read_text())
        assert "ledger" in saved and {item["ref"] for item in saved["operations"]} == references
        with pytest.raises(_FreshOpening):
            await opening()
        assert forgotten == [references]
        saved = json.loads(path.read_text())
        assert "ledger" not in saved and saved["operations"] == []
        assert (tmp_path / f"agent.{identifier.hex()}.closed.json").exists()
        client.chain.unwatch_session.assert_called_once_with(identifier)
        client._challenge.assert_awaited_once_with("test/model")
        client._checkpoint.close()

    asyncio.run(scenario())


def hold_a_report_save_across_the_drop(client: Client, loop, report) -> list:
    """When the retirement makes its first save, start ``report`` on the loop:
    a report on the old attachment, whose own save is then held until the
    retirement has dropped the session from memory, so it spans the drop.
    Returns the report's future."""
    real_save, saves, started = client._checkpoint.save, [], []

    def save(record: dict) -> None:
        saves.append(record)
        if len(saves) == 1:
            started.append(asyncio.run_coroutine_threadsafe(report(), loop))
        elif len(saves) == 2:  # The report's, behind the retirement's first.
            deadline = time.monotonic() + 5
            while client._ledger is not None and time.monotonic() < deadline:
                time.sleep(0.001)
            assert client._ledger is None, "the retirement never dropped the session"
        real_save(record)

    client._checkpoint.save = save
    return started


def test_a_report_saved_across_a_retirement_never_brings_the_session_back(
    tmp_path: Path,
) -> None:
    """A Drain on the old attachment (any report that saves: Meter, Receipt,
    CloseResult, a close proof) is being saved while the retirement drops the
    session. That record was built before the drop, and installing it as the
    saved state brought the retired ledger and confirmed close back into the
    retirement's own drop-save: after a restart the next session, which its
    provider then closed capturing above validated usage, was taken for the
    agent's own close. Now no save writes the session back, and the next
    session is a close by someone else: a request gets SESSION_CLOSED,
    sessions.open the CloseAccountingError, and close() sends its own
    CloseRequest."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "cooperative")
        drain = pb.ControlMessage(
            drain=pb.Drain(session_id=identifier, reason=pb.DRAIN_REASON_PROVIDER_INITIATED)
        )
        started = hold_a_report_save_across_the_drop(
            client, asyncio.get_running_loop(), lambda: client._control_report("drain", drain)
        )
        with pytest.raises(_FreshOpening):
            await client._open("test/model", None)
        assert started and started[0].result(5) is None  # The report was saved.
        saved = json.loads(path.read_text())
        assert "ledger" not in saved and "confirmed_close" not in saved
        # The next session's first save, as _open makes it.
        client._ledger = Ledger(DOMAIN, replace(DESCRIPTOR, salt=b"z" * 32), CATALOG)
        client._opening_ref = "open-y"
        client._saved["funding_intent"] = {"model": "test/model"}
        await client._persist()
        client._checkpoint.close()
        # After a restart; the provider has since closed it, capturing 5.
        restarted = restored_client(path, settled=5)
        new_id = restarted._ledger.session_id
        assert new_id != identifier
        assert restarted._closed_result is None and "confirmed_close" not in restarted._saved
        with pytest.raises(PaymentError) as refused:
            await restarted._open("test/model", None)
        assert refused.value.failure.reason == "SESSION_CLOSED"
        with pytest.raises(CloseAccountingError) as excess:
            await explicitly_open(restarted)
        assert (excess.value.capture_amount, excess.value.usage_amount) == (5, 0)
        restarted.timeout, restarted._loop = 0.05, asyncio.get_running_loop()
        restarted._ensure_close_attachment, restarted._send = AsyncMock(), AsyncMock()
        result = await restarted._cooperative_close(new_id)
        assert result.state is OperationState.UNKNOWN  # Its own request, unanswered.
        ((message,), _) = restarted._send.await_args
        assert message.close_request.session_id == new_id
        restarted._checkpoint.close()

    asyncio.run(scenario())


def test_a_report_saved_while_a_retirement_records_its_proof_keeps_that_proof(
    tmp_path: Path,
) -> None:
    """A Drain's save is under way when sessions.open records the foreign
    proof it retires on. Installing the Drain's record, built before that,
    as the saved state lost closed_by_other, so the archived record did not
    say how the session was retired."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "none")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        real_save = client._checkpoint.save
        held = threading.Event()
        drain = pb.ControlMessage(
            drain=pb.Drain(session_id=identifier, reason=pb.DRAIN_REASON_PROVIDER_INITIATED)
        )

        def save(record: dict) -> None:
            if not held.is_set():  # The Drain's: held until the proof is recorded.
                held.set()
                deadline = time.monotonic() + 5
                while "closed_by_other" not in client._saved and time.monotonic() < deadline:
                    time.sleep(0.001)
            real_save(record)

        client._checkpoint.save = save
        reporting = asyncio.create_task(client._control_report("drain", drain))
        await asyncio.sleep(0)  # The Drain holds the checkpoint lock, saving.
        with pytest.raises(_FreshOpening):
            await explicitly_open(client)
        await reporting
        archived = json.loads((tmp_path / f"agent.{identifier.hex()}.closed.json").read_text())
        assert archived["closed_by_other"] == foreign_proof(0, [])
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_record_with_no_session_retained_names_none_of_its_keys(tmp_path: Path) -> None:
    """The session's keys come from live state: with no ledger retained, a
    save writes no ledger, funding intent, close record or foreign proof,
    whatever the saved state still held."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "cooperative")
        client._saved = {
            **client._saved,
            "funding_intent": {"model": "test/model"},
            "reported_close": {"tx": (b"c" * 32).hex(), "capture": 0, "refunded": 100},
            "closed_by_other": foreign_proof(0, []),
        }
        assert "ledger" in client._saved and "confirmed_close" in client._saved
        client._ledger, client._opening_ref, client._operations = None, None, {}
        await client._persist()
        saved = json.loads(path.read_text())
        for key in ("ledger", "funding_intent", "confirmed_close", "reported_close"):
            assert key not in saved
        assert "closed_by_other" not in saved and saved["scope"] == client._saved["scope"]
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_retried_retirement_replaces_an_archive_that_carries_less_evidence(
    tmp_path: Path,
) -> None:
    """The first retirement of an untraceable withdrawal goes on the foreign
    proof, archives its record and fails its drop-save. The wallet then holds
    the withdrawal's hash, it is proven, and the retry retires the session as
    the agent's own close. Its record carries all the first one did and more,
    so it replaces the archive: the history says how the session was retired
    and holds the withdrawal's hash, which the wallet's retire now forgets."""

    async def scenario() -> None:
        client, identifier, path, ref = untraceable_exit_client(tmp_path)
        await client._persist()
        client.wallet.reconcile = Mock(return_value=OperationResult(ref, OperationState.UNKNOWN))
        forgotten: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: forgotten.append(set(operation_refs))
        save, failing = client._checkpoint.save, [True]

        def save_once_failing(record: dict) -> None:
            if "ledger" not in record and failing:  # The save that drops the session.
                failing.clear()
                raise ValidationError(
                    ValidationCode.INVALID_VALUE, "checkpoint could not be durably written"
                )
            save(record)

        client._checkpoint.save = save_once_failing
        with pytest.raises(ValidationError, match="durably written"):
            await explicitly_open(client)
        history = tmp_path / f"agent.{identifier.hex()}.closed.json"
        assert "closed_by_other" in json.loads(history.read_text())
        tx = b"e" * 32
        observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32)
        client.wallet.reconcile = Mock(
            return_value=OperationResult(ref, OperationState.UNKNOWN, "w-1", tx)
        )
        client.chain.reader.reconcile = Mock(
            return_value=OperationResult(ref, OperationState.CONFIRMED, "w-1", tx, observation)
        )
        with pytest.raises(_FreshOpening):
            await explicitly_open(client)
        archived = json.loads(history.read_text())
        assert "closed_by_other" not in archived
        (exit_record,) = [item for item in archived["operations"] if item["ref"] == ref]
        assert (exit_record["state"], exit_record["tx"]) == ("confirmed", tx.hex())
        assert forgotten == [{"open-x", ref}]
        assert sorted(item.name for item in tmp_path.iterdir() if item.name.startswith(".")) == []
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["first_save", "link"])
def test_a_retry_that_retires_on_the_agent_s_own_close_drops_an_earlier_foreign_proof(
    monkeypatch, tmp_path: Path, failure: str
) -> None:
    """The first retirement of an untraceable withdrawal decided on the foreign
    proof and recorded closed_by_other, then failed at its first save or at
    linking the archive, before anything was archived or dropped. The wallet
    then holds the withdrawal's hash, and the retry retires the session as the
    agent's own close: its archived record names no foreign proof, since the
    retry clears an earlier attempt's before recording its own."""

    async def scenario() -> None:
        client, identifier, path, ref = untraceable_exit_client(tmp_path)
        await client._persist()
        client.wallet.reconcile = Mock(return_value=OperationResult(ref, OperationState.UNKNOWN))
        client.wallet.retire = Mock()
        if failure == "first_save":
            save, failing = client._checkpoint.save, [True]

            def save_once_failing(record: dict) -> None:
                if "closed_by_other" in record and failing:
                    failing.clear()
                    raise ValidationError(
                        ValidationCode.INVALID_VALUE, "checkpoint could not be durably written"
                    )
                save(record)

            client._checkpoint.save = save_once_failing
            expected = (ValidationError, "durably written")
        else:
            link, failing = os.link, [True]

            def link_once_failing(source, target) -> None:
                if failing:
                    failing.clear()
                    raise OSError("no space left on device")
                link(source, target)

            monkeypatch.setattr(os, "link", link_once_failing)
            expected = (ClientError, "could not be kept")
        with pytest.raises(expected[0], match=expected[1]):
            await explicitly_open(client)
        history = tmp_path / f"agent.{identifier.hex()}.closed.json"
        assert not history.exists() and client._ledger is not None
        assert "closed_by_other" in client._saved  # The first attempt's, in memory.
        tx = b"e" * 32
        observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32)
        client.wallet.reconcile = Mock(
            return_value=OperationResult(ref, OperationState.UNKNOWN, "w-1", tx)
        )
        client.chain.reader.reconcile = Mock(
            return_value=OperationResult(ref, OperationState.CONFIRMED, "w-1", tx, observation)
        )
        with pytest.raises(_FreshOpening):
            await explicitly_open(client)
        archived = json.loads(history.read_text())
        assert "closed_by_other" not in archived
        (exit_record,) = [item for item in archived["operations"] if item["ref"] == ref]
        assert (exit_record["state"], exit_record["tx"]) == ("confirmed", tx.hex())
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_retried_retirement_never_replaces_an_archive_with_less_evidence(
    tmp_path: Path,
) -> None:
    """An archive an earlier attempt left holds an operation the record this
    retirement keeps does not. It stays as it was, and the retirement goes
    on: a record carrying less never replaces one carrying more."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "withdraw")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        earlier = json.loads(path.read_text())
        earlier["operations"].append(
            {**earlier["operations"][0], "ref": "top-up-z", "tx": "7a" * 32, "state": "failed"}
        )
        history = tmp_path / f"agent.{identifier.hex()}.closed.json"
        history.write_text(json.dumps(earlier))
        history.chmod(0o600)
        before = history.read_bytes()
        with pytest.raises(_FreshOpening):
            await client._open("test/model", None)
        assert history.read_bytes() == before
        assert client._ledger is None and "ledger" not in json.loads(path.read_text())
        assert sorted(item.name for item in tmp_path.iterdir() if item.name.startswith(".")) == []
        client._checkpoint.close()

    asyncio.run(scenario())


def evidence_record(**changes) -> dict:
    """A closed session's record, as an archive holds it, with ``changes``."""
    record = {
        "operations": [
            {
                "ref": "open-x",
                "kind": "open",
                "wallet_id": None,
                "tx": "6f" * 32,
                "state": "confirmed",
                "observation": [5042002, "70" * 20, 10, "62" * 32],
            },
            {
                "ref": "withdraw-x",
                "kind": "withdraw",
                "wallet_id": "w-1",
                "tx": None,
                "state": "unknown",
                "observation": None,
            },
        ],
        "reported_close": {"tx": "63" * 32, "capture": 0, "refunded": 100},
        "closed_by_other": {"superseded": ["reported_close", "withdraw-x"]},
    }
    for key, value in changes.items():
        if value is None:
            record.pop(key)
        else:
            record[key] = value
    return record


def withdrawal(**changes) -> list[dict]:
    """evidence_record's operations, its withdrawal changed by ``changes``."""
    operations = evidence_record()["operations"]
    operations[1] = {**operations[1], **changes}
    return operations


@pytest.mark.parametrize(
    ("new", "kept"),
    [
        ({}, True),
        ({"operations": withdrawal(tx="65" * 32, state="confirmed")}, True),
        ({"operations": withdrawal(state="failed")}, True),
        ({"operations": evidence_record()["operations"][:1]}, False),
        ({"operations": withdrawal(wallet_id=None)}, False),
        ({"operations": withdrawal(wallet_id="w-2")}, False),
        ({"reported_close": None}, False),
        ({"reported_close": None, "confirmed_close": {"tx": "63" * 32}}, True),
        ({"reported_close": None, "confirmed_close": {"tx": "64" * 32}}, False),
        ({"closed_by_other": None}, False),
        ({"closed_by_other": None, "confirmed_close": {"tx": "63" * 32}}, True),
        (
            {
                "closed_by_other": None,
                "operations": withdrawal(tx="65" * 32, state="confirmed"),
            },
            True,
        ),
    ],
    ids=[
        "the_same",
        "exit_proven",
        "exit_failed",
        "an_operation_lost",
        "a_wallet_id_lost",
        "a_wallet_id_changed",
        "the_report_lost",
        "the_report_confirmed",
        "another_close_confirmed",
        "the_foreign_proof_lost",
        "a_confirmed_close_instead",
        "an_own_exit_instead",
    ],
)
def test_a_record_carries_another_s_evidence_only_when_it_keeps_each_part(new, kept) -> None:
    """At least the old record's evidence: every operation it holds, with
    its wallet operation id, hash and receipt block where it had one and its
    state where final; its confirmed close; its reported close, or that
    close confirmed; and its foreign proof, or a proof of the agent's own
    close in its place."""
    old = evidence_record()
    assert client_module._evidence_kept(evidence_record(**new), old) is kept
    final = evidence_record(operations=withdrawal(tx="65" * 32, state="confirmed"))
    assert not client_module._evidence_kept(
        evidence_record(operations=withdrawal(tx="65" * 32, state="failed")), final
    )


def test_a_wallet_retire_that_does_not_return_in_time_is_reported_and_the_opening_goes_on(
    tmp_path: Path,
) -> None:
    """A third-party wallet's retire must not hold the open, funding and
    operation locks: past the client's timeout it is reported as an on_error
    HookError("retire", "TimeoutError", session id), and the opening goes on."""

    async def scenario() -> None:
        client, identifier, _ = closed_client(tmp_path, "withdraw")
        client.timeout = 0.05
        release = threading.Event()
        client.wallet.retire = lambda *, operation_refs, at: release.wait(2.5)
        client._observers = Mock()
        started = time.monotonic()
        try:
            with pytest.raises(_FreshOpening):
                await client._open("test/model", None)
            assert time.monotonic() - started < 2
        finally:
            release.set()
        errors = [
            item.args[1]
            for item in client._observers.emit.call_args_list
            if item.args[0] == "on_error"
        ]
        assert errors == [HookError("retire", "TimeoutError", identifier)]
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("explicit", [False, True], ids=["request", "sessions_open"])
@pytest.mark.parametrize("kind", ["excess", "active", "no_tokens", "no_agent_id"])
def test_a_close_not_yet_final_or_not_within_usage_keeps_its_session(
    tmp_path: Path, kind: str, explicit: bool
) -> None:
    """The agent closed it itself, so an explicit open keeps the same rule
    though the chain shows the session Closed (closed_client). A reported
    close not yet proven is decided at open, by its proof
    (test_a_reported_close_proven_at_open_decides_whose_close_it_was)."""

    async def scenario() -> None:
        client, identifier, path = closed_client(
            tmp_path, "cooperative" if kind in ("active", "no_tokens", "no_agent_id") else kind
        )
        before = path.read_bytes()
        if kind == "active":
            output = SimpleNamespace(task=asyncio.get_running_loop().create_future())
            client._outputs[b"q" * 32] = output
        elif kind == "no_tokens":
            client.initial_session_tokens = None  # No new session could be funded.
        elif kind == "no_agent_id":
            client.agent_id = None
        expected = {
            "excess": (CloseAccountingError, None),
            "active": (ClientError, "still ending"),
            "no_tokens": (ClientError, "initial_session_tokens is required"),
            "no_agent_id": (FundingAuthorizationError, "AGENT_ID_REQUIRED"),
        }[kind]
        with pytest.raises(expected[0], match=expected[1]):
            await (explicitly_open(client) if explicit else client._open("test/model", None))
        assert path.read_bytes() == before
        assert not (tmp_path / f"agent.{identifier.hex()}.closed.json").exists()
        assert client._ledger is not None and client._ledger.session_id == identifier
        client._challenge.assert_not_awaited()
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["withdraw", "releaseIdle"])
def test_an_exit_whose_settled_total_exceeds_usage_is_never_retired(
    tmp_path: Path, kind: str
) -> None:
    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, kind, settled=1)
        before = path.read_bytes()
        with pytest.raises(CloseAccountingError) as failure:
            await client._open("test/model", None)
        assert (failure.value.capture_amount, failure.value.usage_amount) == (1, 0)
        assert failure.value.operation.operation_ref == kind + "-" + identifier.hex()
        # Fresh evidence already at the exit's block: no read; it fails closed again.
        client.chain.refresh.assert_not_awaited()
        with pytest.raises(CloseAccountingError):
            await client._open("test/model", None)
        assert path.read_bytes() == before
        assert not (tmp_path / f"agent.{identifier.hex()}.closed.json").exists()
        assert client._ledger is not None and client._ledger.session_id == identifier
        client._challenge.assert_not_awaited()
        client._checkpoint.close()

    asyncio.run(scenario())


def test_an_exit_is_retired_only_on_evidence_at_or_after_its_block(tmp_path: Path) -> None:
    """Current state, not the exit's historical block: a node that prunes
    history still serves it, and a closed session never changes."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "withdraw")
        before = path.read_bytes()
        behind = SimpleNamespace(
            block=Block(
                ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 9, b"9" * 32),
                90,
                b"8" * 32,
            ),
            sessions=client.chain.current.sessions,
        )
        client.chain.snapshot = lambda: behind
        client.chain.refresh = AsyncMock(return_value=behind)
        with pytest.raises(ClientError, match="does not yet show the exited session closed"):
            await client._open("test/model", None)
        client.chain.refresh.assert_awaited_once()
        assert path.read_bytes() == before
        later = SimpleNamespace(
            block=Block(
                ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 40, b"4" * 32),
                400,
                b"3" * 32,
            ),
            sessions=client.chain.current.sessions,
        )
        client.chain.refresh = AsyncMock(return_value=later)
        with pytest.raises(_FreshOpening):
            await client._open("test/model", None)
        assert (tmp_path / f"agent.{identifier.hex()}.closed.json").exists()
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_retirement_keeps_a_stale_reported_close_only_in_its_history(tmp_path: Path) -> None:
    """A provider's CloseResult whose transaction never closed the session stays
    PENDING; the agent then withdrew. The next session's close must send its
    own CloseRequest, never prove the old transaction again."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "withdraw")
        reported = {"tx": (b"x" * 32).hex(), "capture": 0, "refunded": 100}
        client._saved["reported_close"] = reported
        with pytest.raises(_FreshOpening):
            await client._open("test/model", None)
        assert "reported_close" not in json.loads(path.read_text())
        assert "reported_close" not in client._saved
        history = tmp_path / f"agent.{identifier.hex()}.closed.json"
        assert json.loads(history.read_text())["reported_close"] == reported
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_reconcile_that_started_during_a_retirement_never_resurrects_its_operation(
    tmp_path: Path,
) -> None:
    """The reconcile arrives while retirement refreshes stale evidence, after it
    took the operation locks, before any lock of its own existed: it waits for
    the retirement to end (_operation_lock), then finds its operation gone."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "withdraw")
        ref = "withdraw-" + identifier.hex()
        reading, exit_read, reconciled = asyncio.Event(), asyncio.Event(), threading.Event()
        current = client.chain.current

        def stale():
            raise ChainEvidenceError("stale")

        async def refresh():
            reading.set()
            await exit_read.wait()
            return current

        client.chain.snapshot = stale
        client.chain.refresh = AsyncMock(side_effect=refresh)
        previous = client._operations[ref][1]
        client.chain.reader.reconcile = Mock(
            side_effect=lambda *_, **__: reconciled.wait(2) and previous
        )
        retiring = asyncio.create_task(client._open("test/model", None))
        await asyncio.wait_for(reading.wait(), 2)
        assert ref not in client._operation_locks
        reconciling = asyncio.create_task(client._reconcile_ref(ref))
        await asyncio.sleep(0.05)
        exit_read.set()
        with pytest.raises(_FreshOpening):
            await retiring
        reconciled.set()
        with pytest.raises(ClientError, match="unknown retained operation reference"):
            await reconciling
        assert client._operations == {}
        assert json.loads(path.read_text())["operations"] == []
        client._checkpoint.close()

    asyncio.run(scenario())


def retirement_at_its_barrier(client: Client) -> tuple[asyncio.Event, asyncio.Event]:
    """Hold a retirement between taking its locks and committing: its proof
    of the agent's own exit refreshes stale evidence and waits there."""
    reading, release = asyncio.Event(), asyncio.Event()
    current = client.chain.current

    def stale():
        if not release.is_set():
            raise ChainEvidenceError("chain observation is unavailable or stale")
        return current

    async def refresh():
        reading.set()
        await release.wait()
        return current

    client.chain.snapshot = stale
    client.chain.refresh = AsyncMock(side_effect=refresh)
    return reading, release


async def settle_tasks() -> None:
    """Let every ready task run until it waits on something; no clock time."""
    for _ in range(20):
        await asyncio.sleep(0)


@pytest.mark.parametrize("operation", ["reconcile", "top_up"])
def test_an_operation_that_starts_during_a_retirement_waits_and_then_finds_it_retired(
    tmp_path: Path, operation: str
) -> None:
    """A reconcile or top-up that starts once a retirement holds its locks,
    even of an operation whose lock did not exist yet, waits for the
    retirement to end, then finds the session retired and writes nothing."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "withdraw")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        previous = client._operations["open-x"][1]
        client.chain.reader.reconcile = Mock(return_value=previous)
        reading, release = retirement_at_its_barrier(client)
        retiring = asyncio.create_task(client._open("test/model", None))
        await reading.wait()
        assert "open-x" not in client._operation_locks
        waiting = asyncio.create_task(
            client._reconcile_ref("open-x")
            if operation == "reconcile"
            else client._top_up(identifier, 10)
        )
        await settle_tasks()
        # It waits before its lock exists, so the retirement's commit sees none.
        assert not waiting.done() and "open-x" not in client._operation_locks
        release.set()
        with pytest.raises(_FreshOpening):
            await retiring
        expected = (
            "unknown retained operation reference" if operation == "reconcile" else "not retained"
        )
        with pytest.raises(ClientError, match=expected):
            await waiting
        client.chain.reader.reconcile.assert_not_called()
        assert client._ledger is None and client._operations == {}
        saved = json.loads(path.read_text())
        assert saved["operations"] == [] and "funding_intent" not in saved
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("call", ["close", "reconcile"])
def test_a_close_proof_that_starts_during_a_retirement_waits_and_then_finds_it_retired(
    tmp_path: Path, call: str
) -> None:
    """sessions.open found the provider's reported close unprovable and is
    retiring the session on the foreign proof. A close() or
    reconcile("close-...") that starts meanwhile proves that close only under
    the close's operation lock, so it waits for the retirement, then finds
    the session retired. Before, it proved the close inside the retirement,
    whose archive then held both the agent's confirmed close and
    closed_by_other superseding a report it no longer held."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "reported")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        observation = client.chain.current.block.observation
        reads = iter(["unreadable", "proven"])

        def closing(*_):
            if next(reads) == "unreadable":
                raise ChainEvidenceError("successful canonical receipt required")
            return ClosingEvidence(observation, 0, 100, b"c" * 32)

        client.chain.reader.closing = Mock(side_effect=closing)
        reading, release = retirement_at_its_barrier(client)
        retiring = asyncio.create_task(explicitly_open(client))
        await reading.wait()  # Decided on the foreign proof, reading it.
        waiting = asyncio.create_task(
            client._cooperative_close(identifier)
            if call == "close"
            else client._reconcile_ref("close-" + identifier.hex())
        )
        await settle_tasks()
        assert not waiting.done()
        assert client.chain.reader.closing.call_count == 1  # The retirement's alone.
        release.set()
        with pytest.raises(_FreshOpening):
            await retiring
        with pytest.raises(ClientError, match="not retained"):
            await waiting
        archived = json.loads((tmp_path / f"agent.{identifier.hex()}.closed.json").read_text())
        assert "confirmed_close" not in archived and "reported_close" in archived
        assert archived["closed_by_other"]["superseded"] == ["reported_close"]
        assert client._closed_result is None and "confirmed_close" not in client._saved
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["withdraw", "releaseIdle", "requestClose"])
def test_an_exit_waiting_for_the_open_lock_finds_its_session_retired(
    tmp_path: Path, kind: str
) -> None:
    """An exit call passed its retained-session check, then waited for the
    open lock while sessions.open retired the session. Under the lock it
    checks again, as a top-up does: "session is not retained in this
    checkpoint". Before, it went on with the retired ledger and, once the
    follower no longer read the session, failed with a raw KeyError."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "cooperative")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        await client._open_lock.acquire()  # sessions.open, about to retire it.
        exiting = asyncio.create_task(client._exit(identifier, OperationKind(kind)))
        await settle_tasks()
        assert not exiting.done()
        await client._retire_closed(None, explicit=True)
        client.chain.current.sessions.pop(identifier)  # A refresh no longer reads it.
        client._open_lock.release()
        with pytest.raises(ClientError, match="session is not retained in this checkpoint"):
            await exiting
        assert client._operations == {}
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_close_waiting_for_the_open_lock_never_attaches_the_next_session(
    tmp_path: Path,
) -> None:
    """A close() passed its retained-session check, then waited for the open
    lock while sessions.open retired the session and began the next one.
    Under the lock it checks again before any close attachment: "session is
    not retained in this checkpoint". Before, it began attaching the next
    session instead: it started the provider connection and reconciled that
    session's opening before it refused the old identifier."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "withdraw")
        client._connect, client._start = AsyncMock(), AsyncMock()  # Not _local_start's.
        await client._open_lock.acquire()  # sessions.open, about to retire it.
        closing = asyncio.create_task(client._cooperative_close(identifier))
        await settle_tasks()
        assert not closing.done()
        await client._retire_closed(None, explicit=True)
        client._ledger = Ledger(DOMAIN, replace(DESCRIPTOR, salt=b"z" * 32), CATALOG)
        client._opening_ref = "open-next"
        pending = OperationResult("open-next", OperationState.PENDING)
        client._operations["open-next"] = (None, pending)
        client._reconcile_ref = AsyncMock(return_value=pending)
        client._open_lock.release()
        with pytest.raises(ClientError, match="session is not retained in this checkpoint"):
            await closing
        client._start.assert_not_awaited()
        client._reconcile_ref.assert_not_awaited()
        assert not client._ledger.close_pending and client._close_waiter is None
        client._checkpoint.close()

    asyncio.run(scenario())


def test_an_operation_waiting_on_a_retirement_whose_save_fails_goes_on_against_the_session(
    tmp_path: Path,
) -> None:
    """The retirement's save that drops the session fails: memory is restored
    and the guard ends with it, so the waiting reconcile then runs against
    the restored session and records its result."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "withdraw")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        previous = client._operations["open-x"][1]
        client.chain.reader.reconcile = Mock(return_value=previous)
        save, failing = client._checkpoint.save, [True]

        def save_once_failing(record: dict) -> None:
            if "ledger" not in record and failing:  # The save that drops the session.
                failing.clear()
                raise ValidationError(
                    ValidationCode.INVALID_VALUE, "checkpoint could not be durably written"
                )
            save(record)

        client._checkpoint.save = save_once_failing
        reading, release = retirement_at_its_barrier(client)
        retiring = asyncio.create_task(client._open("test/model", None))
        await reading.wait()
        waiting = asyncio.create_task(client._reconcile_ref("open-x"))
        await settle_tasks()
        assert not waiting.done() and "open-x" not in client._operation_locks
        release.set()
        with pytest.raises(ValidationError, match="durably written"):
            await retiring
        assert await waiting == previous
        client.chain.reader.reconcile.assert_called_once()
        assert client._retiring is None
        assert client._ledger is not None and client._ledger.session_id == identifier
        saved = json.loads(path.read_text())
        assert "ledger" in saved and "open-x" in {item["ref"] for item in saved["operations"]}
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_recovery_waiting_while_its_session_retired_leaves_the_next_attachment(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client, identifier, _ = closed_client(tmp_path, "cooperative")
        client._control_lost = Mock()
        client._ensure_close_attachment = AsyncMock()
        await client._open_lock.acquire()
        recovering = asyncio.create_task(client._recover(identifier))
        await asyncio.sleep(0)
        await client._retire_closed(None, explicit=False)
        client._open_lock.release()
        with pytest.raises(ClientError, match="not retained"):
            await recovering
        client._control_lost.assert_not_called()
        client._ensure_close_attachment.assert_not_awaited()
        client._checkpoint.close()

    asyncio.run(scenario())


def test_closing_a_stream_after_its_session_retired_only_ends_the_stream(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client, _, _ = closed_client(tmp_path, "cooperative")
        await client._retire_closed(None, explicit=False)
        output = _Output(ByteQueue())
        await client._cancel_run(b"r" * 32, output)
        with pytest.raises(ClientError, match="caller cancelled inference"):
            await output.queue.get_or_end()
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_session_closed_by_someone_else_is_refused_without_reattaching(tmp_path: Path) -> None:
    """A provider-initiated close or a third party's idle release: the session
    is not this agent's to retire, and it never reopens on chain."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "none")
        client._ledger.close_pending = False
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        before = path.read_bytes()
        for _ in range(2):  # Every request sees the same refusal; nothing is resent.
            with pytest.raises(PaymentError) as failure:
                await client._open("test/model", None)
            assert failure.value.failure.reason == "SESSION_CLOSED"
            assert failure.value.failure.session_id == identifier
        # A request opens lazily, and only an explicit open retires the session.
        with pytest.raises(PaymentError) as failure:
            await client._create("test/model", False, b'{"model":"test/model"}')
        assert failure.value.failure.reason == "SESSION_CLOSED"
        client._connect.assert_not_awaited()
        assert path.read_bytes() == before
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["none", "requestClose"], ids=["provider_pending", "own_request"])
def test_a_close_pending_that_someone_else_made_is_a_close_by_someone_else(
    tmp_path: Path, kind: str
) -> None:
    """close_pending as the provider's snapshot reported it, or as the agent's
    own requestClose set it (which only opens the close window), and then the
    session Closed with no close of the agent's own to explain it: a request
    gets the terminal SESSION_CLOSED, and sessions.open retires the session and
    funds a new one."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, kind)
        assert client._ledger.close_pending
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        before = path.read_bytes()
        with pytest.raises(PaymentError) as failure:
            await client._open("test/model", None)
        assert failure.value.failure.reason == "SESSION_CLOSED"
        assert failure.value.failure.session_id == identifier
        with pytest.raises(PaymentError) as failure:
            await client._create("test/model", False, b'{"model":"test/model"}')
        assert failure.value.failure.reason == "SESSION_CLOSED"
        client._connect.assert_not_awaited()
        assert path.read_bytes() == before
        references = set(client._operations)
        forgotten: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: forgotten.append(set(operation_refs))
        with pytest.raises(_FreshOpening):
            await explicitly_open(client)
        assert client._ledger is None and client._operations == {}
        assert (tmp_path / f"agent.{identifier.hex()}.closed.json").exists()
        assert forgotten == [references]
        client._challenge.assert_awaited_once_with("test/model")
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("waiter", ["awaited", "failed"])
def test_a_close_request_explains_a_closed_session_only_while_its_result_is_awaited(
    tmp_path: Path, waiter: str
) -> None:
    """The chain shows the session Closed after the agent's cooperative close
    request. While its result is awaited, that is the agent's own close in
    flight, for a request and an explicit open alike. Once Control was lost,
    which fails the waiter and keeps it (_end_control), nothing of the
    agent's own explains the close: a request gets the terminal
    SESSION_CLOSED, and sessions.open retires the session and funds another."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "none")
        assert client._ledger.close_pending
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        client._close_waiter = asyncio.get_running_loop().create_future()
        if waiter == "failed":
            failed = client._close_waiter
            client._end_control(ClientError("Control stream ended"))
            assert client._close_waiter is failed
            assert isinstance(failed.exception(), ClientError)
        before = path.read_bytes()
        if waiter == "awaited":
            for opening in (client._open("test/model", None), explicitly_open(client)):
                with pytest.raises(ClientError, match="session close is pending"):
                    await opening
            assert client._ledger is not None and path.read_bytes() == before
            client._challenge.assert_not_awaited()
        else:
            with pytest.raises(PaymentError) as failure:
                await client._open("test/model", None)
            assert failure.value.failure.reason == "SESSION_CLOSED"
            assert path.read_bytes() == before
            client._connect.assert_not_awaited()
            with pytest.raises(_FreshOpening):
                await explicitly_open(client)
            assert client._ledger is None and client._close_waiter is None
            assert (tmp_path / f"agent.{identifier.hex()}.closed.json").exists()
            client._challenge.assert_awaited_once_with("test/model")
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("caller", ["request", "sessions_open"])
@pytest.mark.parametrize("kind", ["withdraw", "releaseIdle"])
def test_an_own_exit_that_failed_explains_no_closed_session(
    tmp_path: Path, kind: str, caller: str
) -> None:
    """The agent's own withdraw or releaseIdle FAILED, and the chain shows the
    session Closed: that exit took no effect, so nothing of the agent's own
    explains the close. A request gets the terminal SESSION_CLOSED with no
    chain read or reconnect; sessions.open retires the session on the foreign
    proof, with nothing left unresolved."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, kind)
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        ref = kind + "-" + identifier.hex()
        client._operations[ref] = (
            client._operations[ref][0],
            OperationResult(ref, OperationState.FAILED, tx_hash=b"e" * 32),
        )
        await client._persist()
        before = path.read_bytes()
        history = tmp_path / f"agent.{identifier.hex()}.closed.json"
        if caller == "request":
            with pytest.raises(PaymentError) as failure:
                await client._open("test/model", None)
            assert failure.value.failure.reason == "SESSION_CLOSED"
            assert path.read_bytes() == before and not history.exists()
            client.chain.refresh.assert_not_awaited()
            client._connect.assert_not_awaited()
            client._challenge.assert_not_awaited()
        else:
            with pytest.raises(_FreshOpening):
                await explicitly_open(client)
            assert json.loads(history.read_text())["closed_by_other"] == foreign_proof(0, [])
            assert client._ledger is None
            client._challenge.assert_awaited_once_with("test/model")
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("change", ["reconcile_failed", "refresh_closed"])
def test_a_request_waiting_on_a_reconcile_never_retires_a_session_someone_else_closed(
    tmp_path: Path, change: str
) -> None:
    """A request takes the own-close path, then waits for the operation lock a
    reconcile of the agent's own withdrawal holds. Meanwhile the close turns
    out to be someone else's: the withdrawal reconciles FAILED (it reverted,
    as the provider closed first), or, after a withdrawal that had failed, a
    refresh first shows the session Closed. Decided again under the locks,
    the request is refused SESSION_CLOSED and nothing changes; only
    sessions.open retires the session."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "withdraw")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        ref = "withdraw-" + identifier.hex()
        operation = client._operations[ref][0]
        closed = client.chain.current.sessions[identifier]
        failed = OperationResult(ref, OperationState.FAILED, tx_hash=b"e" * 32)
        if change == "reconcile_failed":
            uncertain = OperationResult(ref, OperationState.UNKNOWN, tx_hash=b"e" * 32)
            client._operations[ref] = (operation, uncertain)
        else:
            client._operations[ref] = (operation, failed)
            client.chain.current.sessions[identifier] = replace(
                closed, status=SessionStatus.CLOSING
            )
        await client._persist()
        assert client._ledger.close_pending and not client._closed_by_other()
        references = set(client._operations)
        retired: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: retired.append(set(operation_refs))
        lock = client._operation_locks.setdefault(ref, asyncio.Lock())
        await lock.acquire()  # A reconcile of the withdrawal, in flight.

        async def reconcile() -> None:
            await asyncio.sleep(0.05)
            if change == "reconcile_failed":
                client._operations[ref] = (operation, failed)
                await client._persist()
            else:
                client.chain.current.sessions[identifier] = closed  # A refresh shows it.
            lock.release()

        reconciling = asyncio.create_task(reconcile())
        with pytest.raises(PaymentError) as failure:
            await client._open("test/model", None)  # As a request's lazy create.
        await reconciling
        assert failure.value.failure.code is PaymentCode.SESSION_NOT_READY
        assert failure.value.failure.reason == "SESSION_CLOSED"
        assert failure.value.failure.session_id == identifier
        assert client._ledger is not None and client._ledger.session_id == identifier
        saved = json.loads(path.read_text())
        assert "ledger" in saved and len(saved["operations"]) == 2
        assert not (tmp_path / f"agent.{identifier.hex()}.closed.json").exists()
        assert retired == []
        client.chain.unwatch_session.assert_not_called()
        client._connect.assert_not_awaited()
        client._challenge.assert_not_awaited()
        with pytest.raises(_FreshOpening):
            await explicitly_open(client)
        assert retired == [references]
        client._challenge.assert_awaited_once_with("test/model")
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("stale", [False, True], ids=["fresh", "stale"])
def test_an_explicit_open_retires_a_session_closed_by_someone_else_and_funds_another(
    tmp_path: Path, stale: bool
) -> None:
    """Its provider's close or anyone's releaseIdle, with no close of the
    agent's own begun: sessions.open retires it as after the agent's own close
    and funds a new session with the same signer. Its settled total, here within usage, is the Closed state's in
    fresh evidence, which a stale follower refreshes first."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "none")
        client._ledger.close_pending = False
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        before = json.loads(path.read_text())
        references = set(client._operations)
        forgotten: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: forgotten.append(set(operation_refs))
        if stale:
            current = client.chain.current

            def snapshot():
                if not client.chain.refresh.await_count:
                    raise ChainEvidenceError("chain observation is unavailable or stale")
                return current

            client.chain.snapshot = snapshot
        with pytest.raises(_FreshOpening):
            await explicitly_open(client)
        assert client.chain.refresh.await_count == (1 if stale else 0)
        # Its last record stays readable beside the checkpoint.
        history = json.loads((tmp_path / f"agent.{identifier.hex()}.closed.json").read_text())
        assert history["ledger"]["descriptor"] == before["ledger"]["descriptor"]
        assert history["operations"] == before["operations"]
        after = json.loads(path.read_text())
        assert "ledger" not in after and "funding_intent" not in after
        assert after["operations"] == [] and after["opening_ref"] is None
        assert after["scope"] == before["scope"]
        assert client._ledger is None and client._operations == {}
        client.chain.unwatch_session.assert_called_once_with(identifier)
        assert forgotten == [references]
        client._connect.assert_awaited()
        client._challenge.assert_awaited_once_with("test/model")
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["excess", "active", "no_tokens", "no_agent_id"])
def test_an_explicit_open_keeps_a_session_closed_by_someone_else_it_cannot_retire(
    tmp_path: Path, kind: str
) -> None:
    """Settled above validated usage, as the existing accounting rule refuses
    for the agent's own close; a request on it still ending; or no new session
    that could be funded: nothing is dropped, archived or retired."""

    async def scenario() -> None:
        client, identifier, path = closed_client(
            tmp_path, "none", settled=1 if kind == "excess" else 0
        )
        client._ledger.close_pending = False
        client._connect = AsyncMock()
        client.wallet.retire = Mock()
        if kind == "active":
            output = SimpleNamespace(task=asyncio.get_running_loop().create_future())
            client._outputs[b"q" * 32] = output
        elif kind == "no_tokens":
            client.initial_session_tokens = None
        elif kind == "no_agent_id":
            client.agent_id = None
        before = path.read_bytes()
        expected = {
            "excess": (CloseAccountingError, None),
            "active": (ClientError, "still ending"),
            "no_tokens": (ClientError, "initial_session_tokens is required"),
            "no_agent_id": (FundingAuthorizationError, "AGENT_ID_REQUIRED"),
        }[kind]
        with pytest.raises(expected[0], match=expected[1]) as failure:
            await explicitly_open(client)
        if kind == "excess":
            # No operation of the agent's closed it: there is no transaction to keep.
            assert failure.value.operation is None
            assert (failure.value.capture_amount, failure.value.usage_amount) == (1, 0)
        assert path.read_bytes() == before
        assert not (tmp_path / f"agent.{identifier.hex()}.closed.json").exists()
        assert client._ledger is not None and client._ledger.session_id == identifier
        client.chain.unwatch_session.assert_not_called()
        client.wallet.retire.assert_not_called()
        client._connect.assert_not_awaited()
        client._challenge.assert_not_awaited()
        client._checkpoint.close()

    asyncio.run(scenario())


def foreign_proof(settled: int, superseded: list[str]) -> dict[str, object]:
    """The record a retirement on the foreign proof archives (closed_client's
    fresh evidence: block 10, no validated usage)."""
    return {
        "block": 10,
        "block_hash": (b"b" * 32).hex(),
        "settled": settled,
        "usage": 0,
        "superseded": superseded,
    }


@pytest.mark.parametrize("caller", ["request", "sessions_open"])
@pytest.mark.parametrize("proof", ["unavailable", "differs", "proven"])
def test_a_reported_close_proven_at_open_decides_whose_close_it_was(
    tmp_path: Path, proof: str, caller: str
) -> None:
    """The provider reported a close, and the chain shows the session Closed.
    Opening proves the report. Proven, it is the agent's own close, which a
    request or sessions.open retires. When its transaction is not that
    session's SessionClosed (the proof stays PENDING) or its amounts differ,
    the agent cannot show a close of its own: a request gets the terminal
    SESSION_CLOSED, and sessions.open retires the session on the Closed
    state's settled total, its archived record naming the unresolved report."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "reported")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        tx, observation = b"c" * 32, client.chain.current.block.observation
        if proof == "unavailable":
            closing = Mock(side_effect=ChainEvidenceError("successful canonical receipt required"))
        else:
            capture = 0 if proof == "proven" else 1
            closing = Mock(return_value=ClosingEvidence(observation, capture, 100 - capture, tx))
        client.chain.reader.closing = closing
        references = set(client._operations)
        forgotten: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: forgotten.append(set(operation_refs))
        before = json.loads(path.read_text())
        history = tmp_path / f"agent.{identifier.hex()}.closed.json"
        opening = (
            explicitly_open(client)
            if caller == "sessions_open"
            else client._open("test/model", None)
        )
        if proof != "proven" and caller == "request":
            with pytest.raises(PaymentError) as failure:
                await opening
            assert failure.value.failure.reason == "SESSION_CLOSED"
            assert failure.value.failure.session_id == identifier
            assert json.loads(path.read_text()) == before
            assert not history.exists() and forgotten == []
            client._connect.assert_not_awaited()
            client._challenge.assert_not_awaited()
        else:
            with pytest.raises(_FreshOpening):
                await opening
            archived = json.loads(history.read_text())
            if proof == "proven":
                assert "closed_by_other" not in archived and "reported_close" not in archived
                assert archived["confirmed_close"]["tx"] == tx.hex()
            else:
                assert archived["closed_by_other"] == foreign_proof(0, ["reported_close"])
                assert archived["reported_close"] == before["reported_close"]
            saved = json.loads(path.read_text())
            for key in ("ledger", "reported_close", "confirmed_close", "closed_by_other"):
                assert key not in saved
            assert forgotten == [references]
            client._challenge.assert_awaited_once_with("test/model")
        closing.assert_called_once_with(tx, identifier)
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("caller", ["request", "sessions_open"])
@pytest.mark.parametrize("tx", ["none", "held"])
@pytest.mark.parametrize("kind", ["withdraw", "releaseIdle"])
def test_an_own_exit_left_unknown_with_no_transaction_is_a_close_by_someone_else(
    tmp_path: Path, kind: str, tx: str, caller: str
) -> None:
    """The agent's own withdraw or releaseIdle is UNKNOWN, and the chain shows
    the session Closed. With no transaction hash, as after a crash between its
    journal save and execute, nothing the agent holds proves the exit: a
    request gets the terminal SESSION_CLOSED, and sessions.open retires the
    session on the Closed state's settled total, its archived record naming
    the unresolved exit, whose reference the wallet keeps (its record is
    never handed to retire). Opening asks the wallet for that record first;
    here it holds no hash either. With a hash a reconcile can prove it:
    opening reads its receipt first, unreadable here, so it stays the agent's
    own close in flight."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, kind)
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        ref = kind + "-" + identifier.hex()
        held = b"e" * 32 if tx == "held" else None
        client._operations[ref] = (
            client._operations[ref][0],
            OperationResult(ref, OperationState.UNKNOWN, tx_hash=held),
        )
        await client._persist()
        before = json.loads(path.read_text())
        references = set(client._operations)
        forgotten: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: forgotten.append(set(operation_refs))
        client.chain.reader.reconcile = Mock(
            side_effect=TransportError("HTTP request failed", transient=True)
        )
        client.chain.reader.profile = SimpleNamespace(domain=DOMAIN)
        client.wallet.reconcile = Mock(return_value=OperationResult(ref, OperationState.UNKNOWN))
        history = tmp_path / f"agent.{identifier.hex()}.closed.json"
        opening = (
            explicitly_open(client)
            if caller == "sessions_open"
            else client._open("test/model", None)
        )
        if tx == "none":
            client.wallet.reconcile.assert_not_called()
        if tx == "held" or caller == "request":
            if tx == "held":
                with pytest.raises(ClientError, match="session close is pending"):
                    await opening
                client.chain.reader.reconcile.assert_called_once()
                assert client.chain.reader.reconcile.call_args.kwargs["tx_hash"] == held
            else:
                with pytest.raises(PaymentError) as failure:
                    await opening
                assert failure.value.failure.reason == "SESSION_CLOSED"
                client._connect.assert_not_awaited()
            assert json.loads(path.read_text()) == before
            assert not history.exists() and forgotten == []
            assert client._ledger is not None and client._ledger.session_id == identifier
            client._challenge.assert_not_awaited()
        else:
            with pytest.raises(_FreshOpening):
                await opening
            archived = json.loads(history.read_text())
            assert archived["closed_by_other"] == foreign_proof(0, [ref])
            (exit_record,) = [item for item in archived["operations"] if item["ref"] == ref]
            assert (exit_record["state"], exit_record["tx"]) == ("unknown", None)
            saved = json.loads(path.read_text())
            assert "ledger" not in saved and "closed_by_other" not in saved
            assert saved["operations"] == []
            assert forgotten == [references - {ref}]
            client._challenge.assert_awaited_once_with("test/model")
        if tx == "none":
            client.wallet.reconcile.assert_called_once_with(operation_ref=ref)
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("caller", ["request", "sessions_open"])
@pytest.mark.parametrize("answer", ["failed", "unknown", "pending"])
@pytest.mark.parametrize("kind", ["withdraw", "releaseIdle"])
def test_an_own_exit_pending_with_no_hash_is_reconciled_before_its_close_is_decided(
    tmp_path: Path, kind: str, answer: str, caller: str
) -> None:
    """After a restart (closed_client restores the journal an earlier process
    saved), the agent's own exit is PENDING at its wallet with no hash yet and
    the chain shows the session Closed. Opening reconciles the exit first,
    under the retirement's locks, and the rule for what the wallet then
    reports applies: FAILED is the failed-exit rule and UNKNOWN with no hash
    the foreign rule, so a request gets SESSION_CLOSED and sessions.open
    retires the session on the foreign proof; still PENDING keeps the
    own-close rule, and the next open asks again."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, kind, exits=(OperationState.PENDING,))
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        ref = kind + "-" + identifier.hex()
        assert client._operations[ref][1] == OperationResult(ref, OperationState.PENDING, "w-1")
        client.chain.reader.profile = SimpleNamespace(domain=DOMAIN)
        state = {
            "failed": OperationState.FAILED,
            "unknown": OperationState.UNKNOWN,
            "pending": OperationState.PENDING,
        }[answer]
        client.wallet.reconcile = Mock(return_value=OperationResult(ref, state, "w-1"))
        client.wallet.retire = Mock()
        history = tmp_path / f"agent.{identifier.hex()}.closed.json"

        def opening():
            return (
                explicitly_open(client)
                if caller == "sessions_open"
                else client._open("test/model", None)
            )

        if answer == "pending":
            for count in (1, 2):
                with pytest.raises(ClientError, match="session close is pending"):
                    await opening()
                assert client.wallet.reconcile.call_count == count
            assert client._operations[ref][1].state is OperationState.PENDING
            assert not history.exists()
        elif caller == "request":
            with pytest.raises(PaymentError) as failure:
                await opening()
            assert failure.value.failure.reason == "SESSION_CLOSED"
            assert client._operations[ref][1].state is state
            saved = {item["ref"]: item for item in json.loads(path.read_text())["operations"]}
            assert saved[ref]["state"] == state.value  # What the wallet reported is kept.
            assert not history.exists()
            client._connect.assert_not_awaited()
        else:
            with pytest.raises(_FreshOpening):
                await opening()
            superseded = [ref] if answer == "unknown" else []
            assert json.loads(history.read_text())["closed_by_other"] == foreign_proof(
                0, superseded
            )
            client.wallet.retire.assert_called_once()
            client._challenge.assert_awaited_once_with("test/model")
        client.wallet.reconcile.assert_called_with(operation_ref=ref)
        client._checkpoint.close()

    asyncio.run(scenario())


def exit_due(client: Client, kind: str) -> None:
    """Fresh evidence in which a new attempt of this exit kind is eligible:
    withdraw an hour into Closing, releaseIdle a day after the session's last
    activity, requestClose while Open and before that deadline."""
    status, requested_at, now = {
        "withdraw": (SessionStatus.CLOSING, 100, 100 + 3600),
        "releaseIdle": (SessionStatus.OPEN, 0, 100 + 86400),
        "requestClose": (SessionStatus.OPEN, 0, 200),
    }[kind]
    state = ChainSession(100, 0, status, requested_at, 100, CATALOG.rate_card_hash)
    client.chain.current = SimpleNamespace(
        block=Block(
            ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 20, b"t" * 32),
            now,
            b"c" * 32,
        ),
        sessions={client._ledger.session_id: state},
        pause_level=PauseLevel.NONE,
    )


def attempts_land(client: Client, outcomes: dict[str, OperationState]) -> list[str]:
    """The wallet sends each new attempt, and its receipt shows the outcome
    named for its reference: CONFIRMED, or FAILED (mined and reverted).
    Returns the references sent, in order."""
    sent: list[str] = []
    observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 21, b"u" * 32)

    def execute(call, *, operation_ref):
        sent.append(operation_ref)
        tx = hashlib.sha256(operation_ref.encode()).digest()
        return OperationResult(operation_ref, OperationState.PENDING, "w-" + operation_ref, tx)

    def reconcile(operation, *, operation_ref, tx_hash, wallet_operation_id):
        state = outcomes[operation_ref]
        proof = observation if state is OperationState.CONFIRMED else None
        return OperationResult(operation_ref, state, wallet_operation_id, tx_hash, proof)

    client.wallet.execute = execute
    client.chain.reader.reconcile = Mock(side_effect=reconcile)
    client.chain.reader.profile = SimpleNamespace(domain=DOMAIN)
    return sent


def saved_states(path: Path) -> dict[str, str]:
    return {item["ref"]: item["state"] for item in json.loads(path.read_text())["operations"]}


@pytest.mark.parametrize("kind", ["withdraw", "releaseIdle", "requestClose"])
def test_a_failed_exit_is_sent_again_under_its_next_reference(tmp_path: Path, kind: str) -> None:
    """The agent's withdraw, releaseIdle or requestClose FAILED. Called again,
    the exit is sent as a new attempt under the next reference, .1, and after
    that one FAILED too, .2; a confirmed attempt is final, so a call after it
    only reconciles it. Each failed attempt stays in the journal."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, kind, exits=(OperationState.FAILED,))
        base = kind + "-" + identifier.hex()
        exit_due(client, kind)
        sent = attempts_land(
            client, {base + ".1": OperationState.FAILED, base + ".2": OperationState.CONFIRMED}
        )
        expected = [
            (base + ".1", OperationState.FAILED),
            (base + ".2", OperationState.CONFIRMED),
            (base + ".2", OperationState.CONFIRMED),
        ]
        for ref, state in expected:
            result = await client._exit(identifier, OperationKind(kind))
            assert (result.operation_ref, result.state) == (ref, state)
        assert sent == [base + ".1", base + ".2"]
        assert saved_states(path) == {
            "open-x": "confirmed",
            base: "failed",
            base + ".1": "failed",
            base + ".2": "confirmed",
        }
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "latest", ["pending", "unknown_with_hash", "unknown_without_hash", "reconciled_failed"]
)
def test_an_exit_attempt_still_pending_or_unknown_admits_no_new_one(
    tmp_path: Path, latest: str
) -> None:
    """After a withdrawal FAILED, its next attempt is PENDING or UNKNOWN, so
    it could still land: a call reconciles it and returns what it shows, with
    nothing new sent. An UNKNOWN one with no hash, which the wallet still
    cannot place, is never retried blind. Only once the reconcile shows the
    attempt FAILED is the one after it sent."""

    async def scenario() -> None:
        state = (
            OperationState.PENDING
            if "pending" in latest or "failed" in latest
            else (OperationState.UNKNOWN)
        )
        client, identifier, path = closed_client(
            tmp_path, "withdraw", exits=(OperationState.FAILED, state)
        )
        base = "withdraw-" + identifier.hex()
        ref = base + ".1"
        exit_due(client, "withdraw")
        sent = attempts_land(client, {base + ".2": OperationState.CONFIRMED})
        if latest == "unknown_with_hash":
            client._operations[ref] = (
                client._operations[ref][0],
                OperationResult(ref, OperationState.UNKNOWN, "w-2", b"h" * 32),
            )
            client.chain.reader.reconcile = Mock(
                side_effect=TransportError("HTTP request failed", transient=True)
            )
        answers = {
            "pending": OperationResult(ref, OperationState.PENDING, "w-2"),
            "unknown_with_hash": None,
            "unknown_without_hash": OperationResult(ref, OperationState.UNKNOWN),
            "reconciled_failed": OperationResult(ref, OperationState.FAILED, "w-2"),
        }
        client.wallet.reconcile = Mock(return_value=answers[latest])
        result = await client._exit(identifier, OperationKind.WITHDRAW)
        if latest == "reconciled_failed":
            assert (result.operation_ref, result.state) == (base + ".2", OperationState.CONFIRMED)
            assert sent == [base + ".2"]
            assert saved_states(path)[ref] == "failed"
        else:
            assert (result.operation_ref, result.state) == (ref, state)
            assert sent == []
            assert base + ".2" not in client._operations
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("caller", ["request", "sessions_open"])
@pytest.mark.parametrize("latest", ["confirmed", "pending"])
def test_the_latest_exit_attempt_decides_whose_close_it_was(
    tmp_path: Path, latest: str, caller: str
) -> None:
    """A withdrawal FAILED and its next attempt is the one that counts, and
    the chain shows the session Closed. Confirmed, it is the agent's own
    exit: a request or sessions.open retires the session as after its own
    close, and the archived record keeps both attempts. Still pending, it is
    the agent's own exit in flight, which opening reconciles first."""

    async def scenario() -> None:
        state = OperationState.CONFIRMED if latest == "confirmed" else OperationState.PENDING
        client, identifier, path = closed_client(
            tmp_path, "withdraw", exits=(OperationState.FAILED, state)
        )
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        base = "withdraw-" + identifier.hex()
        client.chain.reader.profile = SimpleNamespace(domain=DOMAIN)
        client.wallet.reconcile = Mock(
            return_value=OperationResult(base + ".1", OperationState.PENDING, "w-2")
        )
        forgotten: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: forgotten.append(set(operation_refs))
        history = tmp_path / f"agent.{identifier.hex()}.closed.json"
        opening = (
            explicitly_open(client)
            if caller == "sessions_open"
            else client._open("test/model", None)
        )
        if latest == "confirmed":
            with pytest.raises(_FreshOpening):
                await opening
            archived = json.loads(history.read_text())
            assert "closed_by_other" not in archived
            states = {item["ref"]: item["state"] for item in archived["operations"]}
            assert (states[base], states[base + ".1"]) == ("failed", "confirmed")
            assert forgotten == [{"open-x", base, base + ".1"}]
            client.wallet.reconcile.assert_not_called()
        else:
            with pytest.raises(ClientError, match="session close is pending"):
                await opening
            client.wallet.reconcile.assert_called_once_with(operation_ref=base + ".1")
            assert not history.exists() and forgotten == []
        client._checkpoint.close()

    asyncio.run(scenario())


DATA = Path(__file__).parent / "data"


@pytest.mark.parametrize("then", ["same_process", "restart"])
def test_an_attempt_whose_journal_save_failed_is_dropped_and_its_exit_stays_retriable(
    tmp_path: Path, then: str
) -> None:
    """The journal save that precedes a new withdrawal attempt's wallet call
    fails, so the wallet is never called. The attempt is dropped from
    memory, so no later save makes it durable, and the next call sends it, in
    the same process or after a restart. Before, the unsent attempt stayed
    UNKNOWN with no hash and the next save kept it: from then on the exit
    only reconciled it, and with no record at the wallet it was never sent."""

    async def scenario() -> None:
        client, identifier, path = closed_client(
            tmp_path, "withdraw", exits=(OperationState.FAILED,)
        )
        base = "withdraw-" + identifier.hex()
        ref = base + ".1"
        exit_due(client, "withdraw")
        sent = attempts_land(client, {ref: OperationState.CONFIRMED})
        client.wallet.reconcile = Mock(return_value=OperationResult(ref, OperationState.UNKNOWN))
        real_save, failing = client._checkpoint.save, [True]

        def save(record: dict) -> None:
            if failing and ref in {item["ref"] for item in record["operations"]}:
                failing.clear()  # The save before the wallet call.
                raise ValidationError(
                    ValidationCode.INVALID_VALUE, "checkpoint could not be durably written"
                )
            real_save(record)

        client._checkpoint.save = save
        with pytest.raises(ValidationError, match="durably written"):
            await client._exit(identifier, OperationKind.WITHDRAW)
        assert sent == [] and ref not in client._operations
        await client._persist()  # Any later save, as a report's.
        assert ref not in saved_states(path)
        if then == "restart":
            client._checkpoint.close()
            client = restored_client(path)
            exit_due(client, "withdraw")
            sent = attempts_land(client, {ref: OperationState.CONFIRMED})
            client.wallet.reconcile = Mock(
                return_value=OperationResult(ref, OperationState.UNKNOWN)
            )
        result = await client._exit(identifier, OperationKind.WITHDRAW)
        assert (result.operation_ref, result.state) == (ref, OperationState.CONFIRMED)
        assert sent == [ref]
        client.wallet.reconcile.assert_not_called()
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_withdrawal_a_0_1_5_journal_holds_failed_is_sent_again_after_the_upgrade(
    tmp_path: Path,
) -> None:
    """mppi-agent 0.1.5 saved this journal (data/agent-0.1.5-failed-withdraw.json,
    written with the released sources by data/agent_0_1_5_failed_withdraw.py):
    its withdrawal FAILED under the one fixed reference 0.1.5 gives an exit,
    which 0.1.5 returned forever. Restored here, the withdrawal is sent again
    under withdraw-<session id hex>.1 and confirmed, and the failed attempt
    stays in the journal."""

    async def scenario() -> None:
        path = tmp_path / "agent.json"
        shutil.copyfile(DATA / "agent-0.1.5-failed-withdraw.json", path)
        path.chmod(0o600)
        client = restored_client(path)
        identifier = client._ledger.session_id
        base = "withdraw-" + identifier.hex()
        assert client._operations[base][1] == OperationResult(base, OperationState.FAILED)
        exit_due(client, "withdraw")
        sent = attempts_land(client, {base + ".1": OperationState.CONFIRMED})
        result = await client._exit(identifier, OperationKind.WITHDRAW)
        assert (result.operation_ref, result.state) == (base + ".1", OperationState.CONFIRMED)
        assert sent == [base + ".1"]
        assert saved_states(path) == {
            "open-x": "confirmed",
            base: "failed",
            base + ".1": "confirmed",
        }
        client._checkpoint.close()

    asyncio.run(scenario())


def untraceable_exit_client(tmp_path: Path) -> tuple[Client, bytes, Path, str]:
    """closed_client after a crash between the withdrawal's journal save and
    the wallet call: the agent's journal holds it UNKNOWN with no hash."""
    client, identifier, path = closed_client(tmp_path, "withdraw")
    client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
    ref = "withdraw-" + identifier.hex()
    client._operations[ref] = (
        client._operations[ref][0],
        OperationResult(ref, OperationState.UNKNOWN),
    )
    client.chain.reader.profile = SimpleNamespace(domain=DOMAIN)
    return client, identifier, path, ref


@pytest.mark.parametrize("caller", ["request", "sessions_open"])
def test_an_untraceable_exit_whose_wallet_holds_a_hash_is_reconciled_not_retired(
    tmp_path: Path, caller: str
) -> None:
    """The wallet holds a hash for the withdrawal the agent's journal lacks:
    asked first, the exit is traceable after all. It is reconciled normally,
    its receipt not served yet, so it is the agent's own exit in flight: both
    callers keep "session close is pending", and nothing is archived or
    handed to the wallet's retire."""

    async def scenario() -> None:
        client, identifier, path, ref = untraceable_exit_client(tmp_path)
        await client._persist()
        tx = b"e" * 32
        client.wallet.reconcile = Mock(
            return_value=OperationResult(ref, OperationState.UNKNOWN, "w-1", tx)
        )
        client.chain.reader.reconcile = Mock(
            side_effect=TransportError("HTTP request failed", transient=True)
        )
        client.wallet.retire = Mock()
        opening = (
            explicitly_open(client)
            if caller == "sessions_open"
            else client._open("test/model", None)
        )
        with pytest.raises(ClientError, match="session close is pending"):
            await opening
        client.wallet.reconcile.assert_called_once_with(operation_ref=ref)
        assert client.chain.reader.reconcile.call_args.kwargs["tx_hash"] == tx
        assert client._operations[ref][1] == OperationResult(ref, OperationState.UNKNOWN, "w-1", tx)
        saved = {item["ref"]: item for item in json.loads(path.read_text())["operations"]}
        assert (saved[ref]["tx"], saved[ref]["wallet_id"]) == (tx.hex(), "w-1")
        client.wallet.retire.assert_not_called()
        assert not (tmp_path / f"agent.{identifier.hex()}.closed.json").exists()
        assert client._ledger is not None and client._ledger.session_id == identifier
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("answer", ["no_hash", "no_answer"])
def test_an_untraceable_exits_reference_is_never_handed_to_the_wallets_retire(
    tmp_path: Path, answer: str
) -> None:
    """Asked first, the wallet holds no hash for the withdrawal either, or
    cannot answer. sessions.open retires the session on the foreign proof, and
    every other reference goes to the wallet's retire; the exit's does not, so
    a record the wallet may hold with a hash is never forgotten. The archived
    record keeps the exit, superseded."""

    async def scenario() -> None:
        client, identifier, path, ref = untraceable_exit_client(tmp_path)
        await client._persist()
        if answer == "no_hash":
            client.wallet.reconcile = Mock(
                return_value=OperationResult(ref, OperationState.UNKNOWN)
            )
        else:
            client.wallet.reconcile = Mock(side_effect=RuntimeError("wallet service unavailable"))
        forgotten: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: forgotten.append(set(operation_refs))
        with pytest.raises(_FreshOpening):
            await explicitly_open(client)
        client.wallet.reconcile.assert_called_once_with(operation_ref=ref)
        assert forgotten == [{"open-x"}]
        archived = json.loads((tmp_path / f"agent.{identifier.hex()}.closed.json").read_text())
        assert archived["closed_by_other"]["superseded"] == [ref]
        (exit_record,) = [item for item in archived["operations"] if item["ref"] == ref]
        assert (exit_record["state"], exit_record["tx"]) == ("unknown", None)
        client._checkpoint.close()

    asyncio.run(scenario())


def test_an_exit_hash_that_turns_up_after_a_foreign_retirement_counts_nothing(
    tmp_path: Path,
) -> None:
    """sessions.open retired the session on the foreign proof while the
    agent's own withdrawal was UNKNOWN with no hash. The wallet then learns
    the withdrawal's hash: on the Closed session it could only have reverted
    or been the close itself, whose settled total the foreign proof already
    bounded. Nothing reads or presents it again: the next opening funds,
    signs and counts nothing for it, and its history stays archived."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "withdraw")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        ref = "withdraw-" + identifier.hex()
        client._operations[ref] = (
            client._operations[ref][0],
            OperationResult(ref, OperationState.UNKNOWN),
        )
        await client._persist()
        references = set(client._operations)
        forgotten: list[set[str]] = []
        client.wallet.retire = lambda *, operation_refs, at: forgotten.append(set(operation_refs))
        client.chain.reader.profile = SimpleNamespace(domain=DOMAIN)
        client.wallet.reconcile = Mock(return_value=OperationResult(ref, OperationState.UNKNOWN))
        client.signer = Mock(wraps=SIGNER, address=SIGNER.address)
        with pytest.raises(_FreshOpening):
            await explicitly_open(client)
        client.wallet.reconcile.assert_called_once_with(operation_ref=ref)  # Asked first.
        late = OperationResult(ref, OperationState.PENDING, tx_hash=b"e" * 32)
        client.wallet.reconcile = Mock(return_value=late)
        client.wallet.execute = Mock(side_effect=AssertionError("nothing is funded again"))
        client.chain.reader.reconcile = Mock(side_effect=AssertionError("nothing is read again"))
        with pytest.raises(ClientError, match="unknown retained operation reference"):
            await client._reconcile_ref(ref)
        with pytest.raises(_FreshOpening):
            await explicitly_open(client)  # The next opening's Challenge, nothing before it.
        client.wallet.reconcile.assert_not_called()
        client.wallet.execute.assert_not_called()
        client.chain.reader.reconcile.assert_not_called()
        assert client.signer.method_calls == []
        assert forgotten == [references - {ref}]  # Retired once; the wallet keeps the exit.
        assert client._ledger is None and client._operations == {}
        assert client._challenge.await_count == 2
        saved = json.loads(path.read_text())
        assert "ledger" not in saved and saved["operations"] == []
        archived = json.loads((tmp_path / f"agent.{identifier.hex()}.closed.json").read_text())
        assert archived["closed_by_other"]["superseded"] == [ref]
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_close_proof_in_flight_when_a_retirement_begins_finishes_before_it_decides(
    tmp_path: Path,
) -> None:
    """close() began proving the provider's reported close, and its read of
    the chain is slow, when sessions.open begins retiring the session. The
    proof holds the close's operation lock, which the retirement takes too,
    so the retirement decides only once the proof is done: here the read
    matches, and the session retires as the agent's own close. Before, the
    retirement found the proof unreadable and retired it on the foreign
    proof while the slow read could still match."""

    async def scenario() -> None:
        client, identifier, path = closed_client(tmp_path, "reported")
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        tx, observation = b"c" * 32, client.chain.current.block.observation
        entered, release = threading.Event(), threading.Event()

        def closing(*_):
            entered.set()
            release.wait(5)
            return ClosingEvidence(observation, 0, 100, tx)

        client.chain.reader.closing = Mock(side_effect=closing)
        closing_call = asyncio.create_task(client._cooperative_close(identifier))
        await asyncio.to_thread(entered.wait, 5)
        retiring = asyncio.create_task(explicitly_open(client))
        await settle_tasks()
        assert not retiring.done()
        release.set()
        with pytest.raises(_FreshOpening):
            await retiring
        proven = await closing_call
        assert (proven.state, proven.tx_hash) == (OperationState.CONFIRMED, tx)
        client.chain.reader.closing.assert_called_once()
        archived = json.loads((tmp_path / f"agent.{identifier.hex()}.closed.json").read_text())
        assert archived["confirmed_close"]["tx"] == tx.hex()
        assert "closed_by_other" not in archived and "reported_close" not in archived
        saved = json.loads(path.read_text())
        assert "confirmed_close" not in saved and "ledger" not in saved
        client._checkpoint.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("marker", ["reported", "exit"])
def test_a_close_the_agent_cannot_prove_whose_settled_total_exceeds_usage_is_never_retired(
    tmp_path: Path, marker: str
) -> None:
    """The foreign rule's accounting holds for a close the agent cannot show
    as its own: settled above validated usage, sessions.open raises
    CloseAccountingError every time, a request gets SESSION_CLOSED, and the
    session stays retained with nothing archived or retired."""

    async def scenario() -> None:
        client, identifier, path = closed_client(
            tmp_path, "reported" if marker == "reported" else "withdraw", settled=1
        )
        client._connect = AsyncMock()  # Its own: the fixture shares one with _start.
        if marker == "reported":
            client.chain.reader.closing = Mock(
                side_effect=ChainEvidenceError("successful canonical receipt required")
            )
        else:
            ref = "withdraw-" + identifier.hex()
            client._operations[ref] = (
                client._operations[ref][0],
                OperationResult(ref, OperationState.UNKNOWN),
            )
            await client._persist()
            client.chain.reader.profile = SimpleNamespace(domain=DOMAIN)
            client.wallet.reconcile = Mock(
                return_value=OperationResult(ref, OperationState.UNKNOWN)
            )
        client.wallet.retire = Mock()
        before = path.read_bytes()
        for _ in range(2):
            with pytest.raises(CloseAccountingError) as failure:
                await explicitly_open(client)
            assert failure.value.operation is None
            assert (failure.value.capture_amount, failure.value.usage_amount) == (1, 0)
        with pytest.raises(PaymentError) as refused:
            await client._open("test/model", None)
        assert refused.value.failure.reason == "SESSION_CLOSED"
        assert path.read_bytes() == before
        assert not (tmp_path / f"agent.{identifier.hex()}.closed.json").exists()
        assert client._ledger is not None and client._ledger.session_id == identifier
        client.wallet.retire.assert_not_called()
        client.chain.unwatch_session.assert_not_called()
        client._connect.assert_not_awaited()
        client._challenge.assert_not_awaited()
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_reconcile_waiting_during_a_retirement_never_writes_into_the_next_journal(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        client, identifier, _ = closed_client(tmp_path, "withdraw")
        ref = "withdraw-" + identifier.hex()
        client.chain.reader.reconcile = Mock(side_effect=AssertionError)
        held = client._operation_locks.setdefault(ref, asyncio.Lock())
        await held.acquire()
        retiring = asyncio.create_task(client._open("test/model", None))
        await asyncio.sleep(0)
        reconciling = asyncio.create_task(client._reconcile_ref(ref))
        await asyncio.sleep(0)
        held.release()
        with pytest.raises(_FreshOpening):
            await retiring
        with pytest.raises(ClientError, match="unknown retained operation reference"):
            await reconciling
        assert client._operations == {}
        client._checkpoint.close()

    asyncio.run(scenario())


def test_restarted_public_inspection_reconcile_and_exit_restore_without_open(
    tmp_path: Path,
) -> None:
    path = tmp_path / "agent.json"
    observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32)
    block = Block(observation, 200, b"c" * 32)
    contract = ContractCalls(DOMAIN, "USDC", "2")

    async def prepare() -> bytes:
        client = component_client()
        client.contract = contract
        client.wallet = SimpleNamespace(address=DESCRIPTOR.agent)
        client.signer_reference = "fixture:public-test-key"
        client._saved = {"version": 1, "scope": client._scope(7)}
        client._opening_ref = None
        operation = contract.request_close(DESCRIPTOR)
        result = OperationResult(
            "retained-operation",
            OperationState.CONFIRMED,
            tx_hash=b"t" * 32,
            observation=observation,
        )
        client._operations = {result.operation_ref: (operation, result)}
        checkpoint = Checkpoint(path)
        checkpoint.save(client._record())
        checkpoint.close()
        return client._ledger.session_id

    identifier = asyncio.run(prepare())
    identity = ProviderIdentity(
        observation,
        b"g" * 20,
        7,
        DESCRIPTOR.operator,
        DESCRIPTOR.recipient,
        "https://provider.test/discover",
        CATALOG,
    )
    state = ChainSession(100, 0, SessionStatus.OPEN, 0, 100, CATALOG.rate_card_hash)
    snapshot = SimpleNamespace(
        block=block,
        sessions={identifier: state},
        identities={7: identity},
        pause_level=PauseLevel.NONE,
    )
    retained = OperationResult(
        "retained-operation", OperationState.CONFIRMED, tx_hash=b"t" * 32, observation=observation
    )
    reader = SimpleNamespace(
        profile=SimpleNamespace(domain=DOMAIN, registry_address=b"g" * 20),
        reconcile=Mock(return_value=retained),
        opening=Mock(side_effect=AssertionError("inspection must not open")),
    )
    chain = SimpleNamespace(
        reader=reader,
        current=snapshot,
        snapshot=lambda: snapshot,
        readiness=lambda _: Readiness(True),
        confirmed_readiness=lambda _: Readiness(True),
        start=AsyncMock(),
        follow=Mock(),
        watch_session=AsyncMock(return_value=state),
        watch_provider=AsyncMock(return_value=identity),
        stop=AsyncMock(),
    )
    wallet = SimpleNamespace(
        address=DESCRIPTOR.agent,
        execute=Mock(
            side_effect=lambda call, *, operation_ref: OperationResult(
                operation_ref, OperationState.UNKNOWN
            )
        ),
    )
    with patch("mppi_agent.client.discover", side_effect=AssertionError("provider is offline")):
        client = Client(
            7,
            wallet=wallet,
            signer=SIGNER,
            chain=chain,
            contract=contract,
            checkpoint_path=path,
            signer_reference="fixture:public-test-key",
        )
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                get = pool.submit(client.sessions.get, identifier)
                listing = pool.submit(client.sessions.list)
                assert get.result().session_id == identifier
                assert listing.result().sessions[0].session_id == identifier
            assert client.sessions.reconcile("retained-operation") == retained
            assert client.sessions.requestClose(identifier).state is OperationState.UNKNOWN
            assert wallet.execute.call_args.args[0] == contract.request_close(DESCRIPTOR).call
            # One start watches the restored session; no provider is contacted.
            chain.start.assert_awaited_once()
            chain.follow.assert_called_once_with(sessions=(identifier,))
            chain.watch_provider.assert_not_awaited()
            chain.watch_session.assert_not_awaited()
            reader.opening.assert_not_called()
            assert client._channel is None
        finally:
            client.close()


@pytest.mark.parametrize("provider_available", [True, False])
def test_restored_cooperative_close_attaches_before_new_close_intent(
    provider_available: bool,
) -> None:
    async def scenario() -> None:
        client = component_client()
        ledger = client._ledger
        ledger.ready = False
        client._loop = asyncio.get_running_loop()
        client._open_lock = asyncio.Lock()
        client._attached_generation = 0
        client._closed_error = client._closed_result = None
        client._saved = {"version": 1}
        client._opening_ref = "original-opening"
        client._local_start = AsyncMock()
        client._start = AsyncMock(
            side_effect=None if provider_available else OSError("provider offline")
        )
        observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32)
        block = Block(observation, 100, b"c" * 32)
        original = OperationResult(
            "original-opening", OperationState.CONFIRMED, tx_hash=b"t" * 32, observation=observation
        )
        client._operations = {"original-opening": (None, original)}
        client._reconcile_ref = AsyncMock(return_value=original)
        client.chain.reader = SimpleNamespace(
            opening=Mock(
                return_value=OpeningEvidence(DESCRIPTOR, block.observation, 100, CATALOG, b"t" * 32)
            )
        )
        events = []

        async def connect() -> None:
            events.append("connect")
            assert not ledger.close_pending
            client._control = SimpleNamespace(done=lambda: False)

        async def attach(tx, *, require_ready) -> None:
            events.append("attach")
            assert not ledger.close_pending and not require_ready and tx == original.tx_hash
            ledger.generation, ledger.ready = 2, True
            client._attached_generation = 2

        async def send(message) -> None:
            events.append("send")
            assert ledger.close_pending and client._attached_generation == ledger.generation
            verify_close_auth(DOMAIN, DESCRIPTOR, message.close_request.signature)
            client._close_waiter.set_result(
                OperationResult(
                    "closed", OperationState.CONFIRMED, tx_hash=b"z" * 32, observation=observation
                )
            )

        client._connect, client._attach, client._send = (
            AsyncMock(side_effect=connect),
            AsyncMock(side_effect=attach),
            AsyncMock(side_effect=send),
        )
        if provider_available:
            result = await client._cooperative_close(ledger.session_id)
            assert result.state is OperationState.CONFIRMED
            assert events == ["connect", "attach", "send"]
            client._reconcile_ref.assert_awaited_once_with("original-opening")
            client.chain.reader.opening.assert_called_once_with(
                original.tx_hash, ledger.session_id, accepted_catalog=CATALOG
            )
            client._persist.assert_awaited_once()
        else:
            with pytest.raises(OSError, match="offline"):
                await client._cooperative_close(ledger.session_id)
            assert not ledger.close_pending and client._close_waiter is None
            client._persist.assert_not_awaited()
            client._send.assert_not_awaited()
            client._reconcile_ref.assert_not_awaited()

    asyncio.run(scenario())


def test_failed_close_send_preserves_intent_for_explicit_reconnect_retry() -> None:
    async def scenario() -> None:
        client = component_client()
        client._loop = asyncio.get_running_loop()
        client._open_lock = asyncio.Lock()
        client._closed_error = client._closed_result = None
        client._saved = {"version": 1}
        client._local_start = AsyncMock()
        client._ensure_close_attachment = AsyncMock()
        client._send = AsyncMock(side_effect=OSError("lost before send"))
        with pytest.raises(OSError):
            await client._cooperative_close(client._ledger.session_id)
        assert client._ledger.close_pending and not client._ledger.ready
        assert client._close_waiter is None and client._attached_generation == 0
        client._persist.assert_awaited_once()
        assert isinstance(client._control_failure, OSError)

    asyncio.run(scenario())


RUN = b"r" * 32
ANSWER = b'{"text":"answer"}'


def accept_voucher(ledger, amount):
    ledger.accept_voucher(
        pb.Voucher(
            session_id=ledger.session_id,
            cumulative=amount,
            signature=SIGNER.sign_voucher(DOMAIN, ledger.session_id, amount),
        ),
        100,
    )


def completed(ledger):
    return pb.Terminal(
        session_id=ledger.session_id,
        run_id=RUN,
        model="test/model",
        rate_card_hash=CATALOG.rate_card_hash,
        input_tokens=4,
        cache_tokens=0,
        output_tokens=0,
        amount=2,
        status=pb.RUN_STATUS_COMPLETED,
        final_meter_seq=1,
    )


class Errors:
    def __init__(self):
        self.records, self.seen = [], threading.Event()

    def __call__(self, error):
        self.records.append(error)
        self.seen.set()


async def serve(control, release):
    """One loopback server for Control and the run's Complete stream."""

    class Chat(services.ChatServicer):
        async def Completions(self, request, context):
            yield pb.ChatChunk(payload=ANSWER)
            await release.wait()
            yield pb.ChatChunk(terminal=completed(LEDGER[0]))

    class Peer(services.ControlServicer):
        async def Session(self, requests, context):
            async for message in control():
                yield message

    server = grpc.aio.server(options=GRPC_OPTIONS)
    services.add_ChatServicer_to_server(Chat(), server)
    services.add_ControlServicer_to_server(Peer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, grpc.aio.insecure_channel(f"127.0.0.1:{port}", options=GRPC_OPTIONS)


LEDGER: list = []


def start_run(client, channel):
    output = _Output(ByteQueue())
    client._outputs[RUN] = output
    output.call = services.ChatStub(channel).Completions(pb.CompleteRequest(run_id=RUN))
    output.task = asyncio.create_task(client._read_complete(RUN, True))
    return output


@pytest.mark.parametrize("kind", ["low_credit", "meter"])
def test_one_rejected_control_report_keeps_control_or_resyncs_without_losing_answers(kind):
    """One invalid report fails no run, and a bad one never loses an answer."""

    async def scenario():
        client = component_client()
        ledger = client._ledger
        LEDGER[:] = [ledger]
        accept_voucher(ledger, 4)
        errors = Errors()
        client._observers = Observers(Hooks(on_error=errors))
        bad = (
            pb.ControlMessage(
                low_credit=pb.LowCredit(
                    session_id=ledger.session_id,
                    model="other/model",
                    rate_card_hash=CATALOG.rate_card_hash,
                    remaining_tokens=1,
                    suggest_top_up_tokens=10,
                )
            )
            if kind == "low_credit"
            else pb.ControlMessage(
                meter=pb.Meter(
                    session_id=ledger.session_id,
                    run_id=b"u" * 32,
                    seq=1,
                    input_tokens=1,
                    cache_tokens=0,
                    output_tokens=0,
                )
            )
        )
        sent, release, hold = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def control():
            await sent.wait()
            yield bad
            await hold.wait()

        server, channel = await serve(control, release)
        client._channel = channel
        output = start_run(client, channel)
        client._control = services.ControlStub(channel).Session()
        reader = asyncio.create_task(client._control_read())
        try:
            assert await asyncio.wait_for(client._next(output), 2) == ANSWER
            sent.set()
            assert await asyncio.to_thread(errors.seen.wait, 2)
            # A live run keeps Control open: a provider cancels every active run
            # when Control closes, so nothing re-syncs while this stream is live.
            assert client._control_failure is None and not reader.done()
            if kind == "meter":
                with pytest.raises(ClientError, match="not ready"):
                    client._require_ready()  # No new run on the stuck cursor.
            release.set()
            assert await asyncio.wait_for(client._next(output), 2) is None  # Completed OK.
            assert output.terminal.status == pb.RUN_STATUS_COMPLETED
            assert client._ledger.runs[RUN].final is not None
            assert client._ledger.usage_amount == 2
            error = errors.records[0]
            assert (error.operation, error.code) == ("control", "ValidationError")
            if kind == "low_credit":
                assert client._control_failure is None and not reader.done()
                assert (
                    error.detail == "low_credit: funding guidance has no signed run price binding"
                )
            else:
                # The cursor cannot pass a rejected Meter: once no stream is live,
                # the next request re-syncs before it starts.
                assert error.detail == "meter: report has no retained signed run"
                await asyncio.sleep(0)
                assert client._resync_if_idle() and client._control_failure is not None
        finally:
            hold.set()
            release.set()
            client._control.cancel()
            reader.cancel()
            output.task.cancel()
            await asyncio.gather(reader, output.task, return_exceptions=True)
            client._observers.close()
            await channel.close()
            await server.stop(0)

    asyncio.run(scenario())


def test_control_loss_and_reattach_keep_in_flight_complete_streams():
    async def scenario():
        client = component_client()
        ledger = client._ledger
        LEDGER[:] = [ledger]
        accept_voucher(ledger, 4)
        release, hold, streams = asyncio.Event(), asyncio.Event(), []

        async def control():
            streams.append(True)
            await hold.wait()
            yield pb.ControlMessage()

        server, channel = await serve(control, release)
        client._channel, client._loop = channel, asyncio.get_running_loop()
        client._control_tasks = []
        client._discovery, client.ca_file = SimpleNamespace(origin="https://127.0.0.1:9"), None
        output = start_run(client, channel)
        try:
            assert await asyncio.wait_for(client._next(output), 2) == ANSWER
            client._control_lost(ClientError("fixture Control loss"))
            await client._connect()  # Reattach reuses the channel the run rides on.
            await asyncio.sleep(0.2)
            assert client._channel is channel and streams
            release.set()
            assert await asyncio.wait_for(client._next(output), 2) is None
            assert output.terminal.status == pb.RUN_STATUS_COMPLETED
            assert client._ledger.runs[RUN].final is not None
        finally:
            hold.set()
            release.set()
            for task in [*client._control_tasks, output.task]:
                task.cancel()
            await asyncio.gather(*client._control_tasks, output.task, return_exceptions=True)
            await channel.close()
            await server.stop(0)

    asyncio.run(scenario())


def test_a_receipt_contradicting_its_runs_final_is_refused_and_changes_nothing():
    async def scenario():
        client = component_client()
        ledger = client._ledger
        accept_voucher(ledger, 4)
        output = client._outputs[RUN] = _Output(ByteQueue())
        assert ledger.apply_final(completed(ledger), 100)  # Its Terminal came first.
        message = fixture_receipt(ledger, RUN)  # No tokens: another final.
        assert await offer(client, message) == "conflicting definitive final reports"
        assert ledger.runs[RUN].final[:3] == (4, 0, 0) and ledger.usage_amount == 2
        assert RUN not in client._receipted and output.receipt is None
        client._persist.assert_not_awaited()
        assert client._control_failure is None

    asyncio.run(scenario())


def test_superseded_coverage_is_reported_and_signs_nothing():
    async def scenario():
        client = component_client()
        ledger = client._ledger
        accept_voucher(ledger, 10)
        context = RunContext(ledger.session_id, b"q" * 32, 1, "test/model", CATALOG.rate_card_hash)
        ledger.register(context, SIGNER.sign_run(DOMAIN, context))
        final = completed(ledger)
        final.run_id, final.input_tokens, final.amount = b"q" * 32, 8, 4
        assert ledger.apply_final(final, 100)  # Its Complete stream overtook Control.
        errors = Errors()
        client._observers = Observers(Hooks(on_error=errors))
        notice = pb.LowCredit(
            session_id=ledger.session_id,
            model="test/model",
            rate_card_hash=CATALOG.rate_card_hash,
            remaining_tokens=0,
            suggest_top_up_tokens=10,
            coverage_request=pb.CoverageRequest(
                run_id=RUN,
                control_generation=1,
                required_by_price=[
                    pb.RequiredTokens(
                        model="test/model",
                        rate_card_hash=CATALOG.rate_card_hash,
                        input_tokens=4,
                        cache_tokens=0,
                        output_tokens=3,
                    )
                ],
            ),
        )
        await client._control_report("low_credit", pb.ControlMessage(low_credit=notice))
        assert await asyncio.to_thread(errors.seen.wait, 2)
        record = errors.records[0]
        assert (record.operation, record.code, record.run_id) == (
            "control",
            "SupersededCoverage",
            RUN,
        )
        assert record.detail == "low_credit: coverage request is priced below usage already applied"
        assert not client._pending_vouchers and client._voucher_task is None
        assert client._control_failure is None
        client._observers.close()

    asyncio.run(scenario())


CANARY = "prompt-canary-5f1e9a"


@pytest.mark.parametrize("failure", ["invalid_json", "upstream_error", "bad_terminal", "control"])
def test_request_and_response_content_never_reaches_errors_or_the_checkpoint(failure):
    """Privacy: error records, exceptions and the checkpoint hold only SDK text."""

    async def scenario():
        client = component_client()
        ledger = client._ledger
        accept_voucher(ledger, 4)
        client._saved, client._opening_ref, client._operations = {"version": 1}, None, {}
        errors = Errors()
        client._observers = Observers(Hooks(on_error=errors))
        body = json.dumps({"model": "test/model", "content": CANARY}).encode()

        class Chat(services.ChatServicer):
            async def Completions(self, request, context):
                assert CANARY.encode() in request.payload
                if failure == "invalid_json":
                    yield pb.ChatChunk(payload=b'{"content":"' + CANARY.encode())
                elif failure == "upstream_error":
                    yield pb.ChatChunk(upstream_error=pb.UpstreamError(body=body, http_status=400))
                    terminal = completed(ledger)
                    terminal.status = pb.RUN_STATUS_FAILED
                    yield pb.ChatChunk(terminal=terminal)
                else:
                    yield pb.ChatChunk(payload=body)
                    terminal = completed(ledger)
                    if failure == "bad_terminal":
                        terminal.model = "canary/" + CANARY
                    yield pb.ChatChunk(terminal=terminal)

        class Peer(services.ControlServicer):
            async def Session(self, requests, context):
                yield pb.ControlMessage(
                    low_credit=pb.LowCredit(
                        session_id=ledger.session_id,
                        model="canary/" + CANARY,
                        rate_card_hash=CATALOG.rate_card_hash,
                        remaining_tokens=1,
                        suggest_top_up_tokens=10,
                    )
                )
                await asyncio.Event().wait()

        server = grpc.aio.server(options=GRPC_OPTIONS)
        services.add_ChatServicer_to_server(Chat(), server)
        services.add_ControlServicer_to_server(Peer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}", options=GRPC_OPTIONS)
        output = _Output(ByteQueue())
        client._outputs[RUN] = output
        output.call = services.ChatStub(channel).Completions(
            pb.CompleteRequest(
                run_id=RUN, payload=b'{"model":"test/model","q":"' + CANARY.encode() + b'"}'
            )
        )
        reader = None
        try:
            if failure == "control":
                client._control = services.ControlStub(channel).Session()
                reader = asyncio.create_task(client._control_read())
                assert await asyncio.to_thread(errors.seen.wait, 2)
                raised = []
            else:
                output.task = asyncio.create_task(client._read_complete(RUN, True))
                raised = []
                while True:
                    try:
                        if await asyncio.wait_for(client._next(output), 2) is None:
                            break
                    except Exception as error:
                        raised.append(error)
                        break
                assert raised and await asyncio.to_thread(errors.seen.wait, 2)
            texts = [str(error) for error in raised] + [
                repr((record.operation, record.code, record.detail)) for record in errors.records
            ]
            assert errors.records and not any(CANARY in text for text in texts), texts
            assert CANARY not in json.dumps(client._record())
        finally:
            if reader is not None:
                client._control.cancel()
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
            if output.task is not None:
                output.task.cancel()
                await asyncio.gather(output.task, return_exceptions=True)
            client._observers.close()
            await channel.close()
            await server.stop(0)

    asyncio.run(scenario())


def fixture_receipt(ledger, run_id):
    return pb.ControlMessage(
        receipt=pb.Receipt(
            session_id=ledger.session_id,
            run_id=run_id,
            model="test/model",
            rate_card_hash=CATALOG.rate_card_hash,
            input_tokens=0,
            cache_tokens=0,
            output_tokens=0,
            amount=0,
            status=pb.RUN_STATUS_COMPLETED,
            final_meter_seq=1,
            settled_cumulative=0,
            chain_observation=pb.ChainObservation(
                chain_id=DOMAIN.chain_id,
                contract_address=DOMAIN.verifying_contract,
                block_number=1,
                block_hash=b"b" * 32,
            ),
        )
    )


class CountingReader:
    """Counts chain reads and the most that ever ran at once."""

    def __init__(self, failure=None):
        self.failure, self.reads, self.active, self.most = failure, 0, 0, 0
        self.lock = threading.Lock()

    def _read(self):
        with self.lock:
            self.reads += 1
            self.active += 1
            self.most = max(self.most, self.active)
        try:
            threading.Event().wait(0.002)
            if self.failure is not None:
                raise self.failure
        finally:
            with self.lock:
                self.active -= 1

    def block(self, number=None):
        self._read()
        return Block(
            ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 1, b"b" * 32),
            100,
            b"c" * 32,
        )

    def session(self, session_id, block):
        self._read()
        return ChainSession(100, 0, SessionStatus.OPEN, 0, 100, CATALOG.rate_card_hash)


async def offer(client, message):
    try:
        await client._control_report("receipt", message)
        return None
    except (ValidationError, ClientError) as error:
        return str(error)


def test_a_receipt_is_applied_at_once_with_no_chain_read():
    """Checked against its run's accepted terms only, so it applies while the
    RPC is down. Its settled total is the provider's report: the agent proves
    the settled total at close and reads it at each attachment."""

    async def scenario():
        client = component_client()
        ledger = client._ledger
        accept_voucher(ledger, 4)
        client.chain.reader = reader = CountingReader(TransportError("RPC is down"))
        output = client._outputs[RUN] = _Output(ByteQueue())
        client._observers = Mock()
        # Receipts for runs this agent never signed are refused.
        reasons = [
            await offer(client, fixture_receipt(ledger, n.to_bytes(32, "big"))) for n in range(200)
        ]
        assert set(reasons) == {"report has no retained signed run"}
        message = fixture_receipt(ledger, RUN)
        for _ in range(50):
            assert await offer(client, message) is None
        assert reader.reads == 0 and ledger.runs[RUN].final is not None
        assert RUN in client._receipted and output.receipt == message.receipt
        # Copies change nothing more.
        assert client._observers.emit.call_args_list == [call("on_receipt", message.receipt)]
        assert client._persist.await_count == 1
        assert client._control_failure is None and client._resync is None

    asyncio.run(scenario())


def test_a_low_credit_rejected_for_a_full_voucher_queue_changes_nothing():
    async def scenario():
        client = component_client()
        ledger = client._ledger
        client._pending_vouchers = {bytes([n]) * 32: (1, 1) for n in range(4)}
        notice = pb.LowCredit(
            session_id=ledger.session_id,
            model="test/model",
            rate_card_hash=CATALOG.rate_card_hash,
            remaining_tokens=0,
            suggest_top_up_tokens=10,
            coverage_request=pb.CoverageRequest(
                run_id=RUN,
                control_generation=1,
                required_by_price=[
                    pb.RequiredTokens(
                        model="test/model",
                        rate_card_hash=CATALOG.rate_card_hash,
                        input_tokens=4,
                        cache_tokens=0,
                        output_tokens=3,
                    )
                ],
            ),
        )
        with pytest.raises(ClientError, match="pending voucher queue is full"):
            await client._control_report("low_credit", pb.ControlMessage(low_credit=notice))
        assert ledger.coverage_numerator == 0 and RUN not in client._pending_vouchers

    asyncio.run(scenario())


def test_a_top_up_never_blocks_requests_and_top_ups_stay_serial():
    """A wallet operation (about 1-2 minutes with Circle) must not stall requests."""

    async def scenario():
        client = component_client()
        ledger = client._ledger
        client._open_lock, client._funding_lock = asyncio.Lock(), asyncio.Lock()
        client._local_start = client._start = AsyncMock()
        client._saved = {}
        opening = OperationResult(
            "open-x",
            OperationState.CONFIRMED,
            tx_hash=b"t" * 32,
            observation=ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 1, b"b" * 32),
        )
        client._operations = {"open-x": (SimpleNamespace(kind=OperationKind.OPEN), opening)}
        client._opening_ref = "open-x"
        view = SimpleNamespace(readiness=Readiness(True))
        client._get = AsyncMock(return_value=view)
        client.contract = ContractCalls(DOMAIN, "USDC", "2")
        state = client.chain.snapshot().sessions
        current = SimpleNamespace(sessions=state, block=SimpleNamespace(timestamp=1_800_000_000))
        client.chain.snapshot = lambda: current
        entered, release, signed = threading.Event(), threading.Event(), []

        def sign(authorization):
            signed.append(authorization.nonce)
            entered.set()
            assert release.wait(5)
            return b"\x01" * 65

        client.wallet = SimpleNamespace(address=DESCRIPTOR.agent, sign_funding_authorization=sign)
        fundable(client)
        client._submit = AsyncMock(return_value=OperationResult("top-up", OperationState.PENDING))
        first = asyncio.create_task(client._top_up(ledger.session_id, 10))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            opened = await asyncio.wait_for(client._open("test/model", None), 1)
            assert opened.session is view  # Not held behind the wallet.
            second = asyncio.create_task(client._top_up(ledger.session_id, 10))
            await asyncio.sleep(0.1)
            assert len(signed) == 1  # Top-ups still run one at a time.
        finally:
            release.set()
        await first
        await second
        assert len(signed) == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("agent_id", [None, 43])
def test_a_top_up_needs_the_agent_id_and_relies_on_the_opening_checks(agent_id):
    async def scenario():
        client = component_client()
        ledger = client._ledger
        client._funding_lock = asyncio.Lock()
        client._local_start = AsyncMock()
        client._operations, client._saved = {}, {}
        client.contract = ContractCalls(DOMAIN, "USDC", "2")
        block = SimpleNamespace(timestamp=1_800_000_000)
        current = SimpleNamespace(sessions=client.chain.snapshot().sessions, block=block)
        client.chain.snapshot = lambda: current
        sign = Mock(return_value=b"\x01" * 65)
        client.wallet = SimpleNamespace(address=DESCRIPTOR.agent, sign_funding_authorization=sign)
        check = fundable(client)
        client.agent_id = agent_id
        pending = OperationResult("top-up", OperationState.PENDING)
        client._submit = AsyncMock(return_value=pending)
        if agent_id is None:
            with pytest.raises(FundingAuthorizationError, match="^AGENT_ID_REQUIRED$"):
                await client._top_up(ledger.session_id, 10)
            sign.assert_not_called()
            client._local_start.assert_not_awaited()
            client._submit.assert_not_awaited()
        else:
            # The opening proved the binding and signature; USDC checks this one.
            assert await client._top_up(ledger.session_id, 10) == pending
            check.assert_not_called()
            client._submit.assert_awaited_once()

    asyncio.run(scenario())


def test_a_silent_stream_cannot_hold_a_resync_past_its_deadline(monkeypatch):
    """A provider must not keep the session unusable with a stream that sends nothing."""

    async def scenario():
        monkeypatch.setattr(client_module, "_RESYNC_WAIT_S", 0.05, raising=False)
        client = component_client()
        client._open_lock, client._start = asyncio.Lock(), AsyncMock()
        client._resync = ValidationError(ValidationCode.INCONSISTENT_USAGE, "fixture")
        client._resync_at = asyncio.get_running_loop().time()
        silent = _Output(ByteQueue())
        silent.task = asyncio.create_task(asyncio.Event().wait())
        client._outputs[RUN] = silent
        with pytest.raises(ClientError, match="re-sync is pending"):
            await client._open("test/model", None)
        await asyncio.sleep(0.06)
        client._connect = AsyncMock(side_effect=RuntimeError("reattaching"))
        with pytest.raises(RuntimeError, match="reattaching"):
            await client._open("test/model", None)  # The next request re-syncs.
        await asyncio.gather(silent.task, return_exceptions=True)
        assert silent.task.cancelled() and client._control_failure is not None

    asyncio.run(scenario())


def test_a_stream_holds_the_output_record_create_handed_it():
    client = component_client()
    output = _Output(ByteQueue())
    stream = CompletionStream(client, RUN, output)  # Not looked up in _outputs.
    assert stream._output is output and RUN not in client._outputs


def coverage_low_credit(ledger, *, run=RUN, inputs=4, outputs=3):
    return pb.ControlMessage(
        low_credit=pb.LowCredit(
            session_id=ledger.session_id,
            model="test/model",
            rate_card_hash=CATALOG.rate_card_hash,
            remaining_tokens=7,
            suggest_top_up_tokens=200_000,
            coverage_request=pb.CoverageRequest(
                run_id=run,
                control_generation=1,
                required_by_price=[
                    pb.RequiredTokens(
                        model="test/model",
                        rate_card_hash=CATALOG.rate_card_hash,
                        input_tokens=inputs,
                        cache_tokens=0,
                        output_tokens=outputs,
                    )
                ],
            ),
        )
    )


class Funding:
    """A 0.1.2-style on_low_credit callback: it gets a real pb.LowCredit."""

    def __init__(self):
        self.notices, self.seen = [], threading.Event()

    def __call__(self, notice):
        assert isinstance(notice, pb.LowCredit)
        copied = pb.LowCredit()
        copied.CopyFrom(notice)
        assert copy.copy(notice) == copy.deepcopy(notice) == copied
        assert pickle.loads(pickle.dumps(notice)) == notice
        self.notices.append((notice, notice.remaining_tokens, notice.suggest_top_up_tokens))
        notice.session_id = bytes(32)  # A detached record, as before.
        self.seen.set()


class Notices:
    """An on_funding_notice callback, the new way to read the shortfall."""

    def __init__(self):
        self.notices, self.seen = [], threading.Event()

    def __call__(self, notice):
        assert copy.deepcopy(notice) == pickle.loads(pickle.dumps(notice)) == notice
        self.notices.append(notice)
        self.seen.set()


def test_a_covered_coverage_request_is_signed_without_reaching_funding_policy():
    """A routine voucher request no longer reads as "top up"."""

    async def scenario():
        client = component_client()
        client._on_low_credit = funding = Funding()
        await client._control_report("low_credit", coverage_low_credit(client._ledger))
        assert RUN in client._pending_vouchers  # The SDK signs it itself.
        await asyncio.sleep(0.05)
        assert client._low_credit_task is None and not funding.notices

    asyncio.run(scenario())


@pytest.mark.parametrize("case", ["shortfall", "ended_run", "full_voucher_queue", "plain"])
def test_only_what_the_deposit_cannot_cover_reaches_funding_policy(case):
    async def scenario():
        client = component_client()
        ledger = client._ledger
        client._on_low_credit = funding = Funding()
        client._on_funding_notice = notices = Notices()
        if case == "plain":
            message = coverage_low_credit(ledger)
            message.low_credit.ClearField("coverage_request")
        else:
            message = coverage_low_credit(ledger, inputs=150, outputs=150)  # 150 > deposit 100.
        if case == "ended_run":
            ledger.runs[RUN].failed_transport = True  # Its Complete already failed.
        if case == "full_voucher_queue":
            client._pending_vouchers = {bytes([n]) * 32: (1, 1) for n in range(4)}
        await client._control_report("low_credit", message)
        assert await asyncio.to_thread(funding.seen.wait, 2)
        assert await asyncio.to_thread(notices.seen.wait, 2)
        ((notice, remaining, suggested),) = funding.notices
        assert (remaining, suggested) == (7, 200_000)
        assert notice.HasField("coverage_request") is (case != "plain")
        (funding_notice,) = notices.notices
        assert isinstance(funding_notice, FundingNotice)
        assert funding_notice.notice.remaining_tokens == 7
        amounts = (100, None, None) if case == "plain" else (100, 150, 50)
        assert (
            funding_notice.deposit,
            funding_notice.required,
            funding_notice.shortfall,
        ) == amounts
        assert RUN not in client._pending_vouchers and ledger.coverage_numerator == 0
        await asyncio.sleep(0)
        assert message.low_credit.session_id == ledger.session_id  # Copy was detached.
        client._low_credit_task.cancel()
        await asyncio.gather(client._low_credit_task, return_exceptions=True)

    asyncio.run(scenario())


def test_a_plain_notice_never_replaces_an_undelivered_shortfall():
    """While the callback is busy, a plain low-credit notice arrives after a
    shortfall: the shortfall is what the caller still has to decide on."""

    async def scenario():
        client = component_client()
        ledger = client._ledger
        entered, release, delivered = threading.Event(), threading.Event(), []

        def callback(notice):
            delivered.append((notice.required, notice.shortfall))
            entered.set()
            assert release.wait(2)

        client._on_funding_notice = callback
        shortfall = coverage_low_credit(ledger, inputs=150, outputs=150)
        await client._control_report("low_credit", shortfall)
        assert await asyncio.to_thread(entered.wait, 2)
        await client._control_report("low_credit", shortfall)  # Waits its turn.
        plain = pb.ControlMessage(
            low_credit=pb.LowCredit(
                session_id=ledger.session_id,
                model="test/model",
                rate_card_hash=CATALOG.rate_card_hash,
                remaining_tokens=7,
                suggest_top_up_tokens=200_000,
            )
        )
        await client._control_report("low_credit", plain)
        release.set()
        await asyncio.wait_for(client._low_credit_task, 2)
        assert delivered == [(150, 50), (150, 50)]

    asyncio.run(scenario())


def test_a_shortfall_is_worked_out_at_delivery_so_a_covered_one_never_over_funds():
    """Final RC review: three notices queued at deposit 100, then a top-up to 150."""

    async def scenario():
        client = component_client()
        ledger = client._ledger
        entered, release, delivered = threading.Event(), threading.Event(), []

        def callback(notice):
            delivered.append((notice.deposit, notice.required, notice.shortfall))
            entered.set()
            assert release.wait(2)

        client._on_funding_notice = callback
        message = coverage_low_credit(ledger, inputs=150, outputs=150)
        await client._control_report("low_credit", message)
        assert await asyncio.to_thread(entered.wait, 2)
        for _ in range(2):
            await client._control_report("low_credit", message)
        sessions = client.chain.current.sessions
        sessions[ledger.session_id] = replace(sessions[ledger.session_id], deposit=150)
        release.set()
        await asyncio.wait_for(client._low_credit_task, 2)
        assert delivered == [(100, 150, 50)]  # The covered later ones are dropped.

    asyncio.run(scenario())


def test_a_top_up_notice_waits_for_an_attached_stream():
    """A provider ends a new stream that sends anything before ReconnectRequest."""

    async def scenario():
        client = component_client()
        client.chain.refresh = AsyncMock()
        client._send = AsyncMock()
        client._saved = {}
        client._control, client._attached_generation = object(), 0  # Mid-reattach.
        confirmed = OperationResult(
            "top-up",
            OperationState.CONFIRMED,
            tx_hash=b"t" * 32,
            observation=ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 1, b"b" * 32),
        )
        await client._top_up_confirmed(confirmed)
        client._send.assert_not_awaited()
        client._attached_generation = 1  # _attach completes and announces it.
        await client._announce_top_up()
        (message,) = [call.args[0] for call in client._send.await_args_list]
        assert message.top_up_notice.tx == b"t" * 32 and client._top_up_hint is None
        for task in list(client._background):
            task.cancel()
        await asyncio.gather(*client._background, return_exceptions=True)

    asyncio.run(scenario())


def test_renewal_stops_instead_of_spinning_when_it_cannot_sign():
    """With top-ups off _open_lock, a renewal can start while an attachment
    is still completing: ready, but not yet attached. It must not spin."""

    async def scenario():
        client = component_client()
        client._attached_generation = 0  # Swapped ledger, attachment not yet recorded.
        client._ledger.coverage_numerator = 4_000_000
        await asyncio.wait_for(client._renew(), 1)
        assert client._ledger.voucher is None

    asyncio.run(scenario())


def added(*counts):
    return TokenCounts(*(sum(column) for column in zip(*map(astuple, counts), strict=True)))


class AdmittingProvider(services.ChatServicer, services.ControlServicer):
    """A provider following the admission rule of both provider SDKs.

    A request is admitted at once when the accepted voucher covers usage, every
    admitted reservation and its own bound. Otherwise it sends that complete
    target as a LowCredit coverage request and waits for a covering voucher
    (mppi_provider ``Mppi._admit``; the Rust ``attempt_admission_before``).
    Each run then reports one Meter, waits for the agent's renewal and ends.
    """

    BOUND, USED = TokenCounts(4, 0, 10), TokenCounts(4, 0, 6)

    def __init__(self, ledger):
        self.ledger = ledger
        self.accepted, self.vouchers, self.coverage_requests = None, [], 0
        self.usage, self.reserved, self.seq = TokenCounts(0, 0, 0), {}, 0
        self.changed, self.outbound = asyncio.Condition(), asyncio.Queue()

    def covered(self, amount):
        return self.accepted is not None and self.accepted >= amount

    async def Session(self, requests, context):
        async def receive():
            async for message in requests:
                amount = message.voucher.cumulative
                verify_voucher(DOMAIN, DESCRIPTOR, amount, message.voucher.signature)
                async with self.changed:
                    self.vouchers.append(amount)
                    if not self.covered(amount):
                        self.accepted = amount
                    self.changed.notify_all()

        reader = asyncio.create_task(receive())
        try:
            while True:
                yield await self.outbound.get()
        finally:
            reader.cancel()

    async def Completions(self, request, context):
        sid = self.ledger.session_id
        async with self.changed:
            target = added(self.usage, *self.reserved.values(), self.BOUND)
            required = required_coverage(
                (priced_numerator(target, CATALOG.rates_for(request.model)),)
            )
            if not self.covered(required):
                self.coverage_requests += 1
                await self.outbound.put(
                    pb.ControlMessage(
                        low_credit=pb.LowCredit(
                            session_id=sid,
                            model=request.model,
                            rate_card_hash=request.rate_card_hash,
                            remaining_tokens=0,
                            suggest_top_up_tokens=10,
                            coverage_request=pb.CoverageRequest(
                                run_id=request.run_id,
                                control_generation=request.control_generation,
                                required_by_price=[
                                    pb.RequiredTokens(
                                        model=request.model,
                                        rate_card_hash=request.rate_card_hash,
                                        input_tokens=target.input_tokens,
                                        cache_tokens=target.cache_tokens,
                                        output_tokens=target.output_tokens,
                                    )
                                ],
                            ),
                        )
                    )
                )
                await asyncio.wait_for(self.changed.wait_for(lambda: self.covered(required)), 2)
            self.reserved[request.run_id], self.seq = self.BOUND, self.seq + 1
            signed = len(self.vouchers)
            await self.outbound.put(
                pb.ControlMessage(
                    meter=pb.Meter(
                        session_id=sid,
                        run_id=request.run_id,
                        seq=self.seq,
                        input_tokens=self.USED.input_tokens,
                        cache_tokens=self.USED.cache_tokens,
                        output_tokens=self.USED.output_tokens,
                    )
                )
            )
            await asyncio.wait_for(self.changed.wait_for(lambda: len(self.vouchers) > signed), 2)
            del self.reserved[request.run_id]
            self.usage = added(self.usage, self.USED)
        yield pb.ChatChunk(payload=ANSWER)
        yield pb.ChatChunk(
            terminal=pb.Terminal(
                session_id=sid,
                run_id=request.run_id,
                model=request.model,
                rate_card_hash=request.rate_card_hash,
                input_tokens=self.USED.input_tokens,
                cache_tokens=self.USED.cache_tokens,
                output_tokens=self.USED.output_tokens,
                amount=5,
                status=pb.RUN_STATUS_COMPLETED,
                final_meter_seq=self.seq,
            )
        )


@pytest.mark.parametrize(
    ("headroom", "coverage_requests", "vouchers"),
    # Each request reserves 4 input and 10 output tokens (7 base units) and
    # uses 4 and 6 (5); the renewal after its Meter adds 6 output tokens (3).
    ((0, 2, [7, 10, 12, 15]), (20, 1, [27, 30, 33])),
)
def test_voucher_headroom_admits_the_next_request_without_a_voucher_round_trip(
    headroom, coverage_requests, vouchers
):
    """Without headroom every request waits for LowCredit and its Voucher
    before its first byte. With it the first voucher leaves room for the next
    request, so the provider admits it at once; amounts stay at most the
    requirement plus the headroom."""

    async def scenario():
        client = component_client()
        client._voucher_headroom = headroom
        ledger = client._ledger
        ledger.runs.clear()
        client.chain.current.identities = {DESCRIPTOR.provider_id: SimpleNamespace(catalog=CATALOG)}
        client._open = AsyncMock(return_value=SimpleNamespace(session=object()))
        provider = AdmittingProvider(ledger)
        server = grpc.aio.server(options=GRPC_OPTIONS)
        services.add_ChatServicer_to_server(provider, server)
        services.add_ControlServicer_to_server(provider, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}", options=GRPC_OPTIONS)
        client._channel, client.request_timeout = channel, None
        client._control = services.ControlStub(channel).Session()
        tasks = [
            asyncio.create_task(client._control_read()),
            asyncio.create_task(client._control_write()),
        ]
        payload = b'{"model":"test/model","messages":[],"stream":true}'
        try:
            for _ in range(2):
                _, output = await asyncio.wait_for(client._create("test/model", True, payload), 2)
                assert await asyncio.wait_for(client._next(output), 3) == ANSWER
                assert await asyncio.wait_for(client._next(output), 3) is None
            assert provider.coverage_requests == coverage_requests
            assert provider.vouchers == vouchers
            assert ledger.usage_amount == 10 and client._control_failure is None
        finally:
            client._control.cancel()
            for task in tasks + list(client._background):
                task.cancel()
            await asyncio.gather(*tasks, *client._background, return_exceptions=True)
            await channel.close()
            await server.stop(0)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("signed", "headroom", "sent"),
    (
        (None, 0, 7),  # Exactly the requirement, as without headroom.
        (None, 20, 27),
        (10, 20, 27),  # Raised to the requirement plus the headroom.
        (30, 20, 30),  # Already sufficient: resent, never raised.
        (None, 1000, 100),  # Never above the confirmed deposit.
    ),
)
def test_a_voucher_covers_its_requirement_plus_the_headroom_within_the_deposit(
    signed, headroom, sent
):
    async def scenario():
        client = component_client()
        client._voucher_headroom = headroom
        client._send = AsyncMock()
        ledger = client._ledger
        if signed is not None:
            accept_voucher(ledger, signed)
        # The provider requires 4 input and 10 output tokens: 7 base units.
        await client._control_report("low_credit", coverage_low_credit(ledger, outputs=10))
        await asyncio.wait_for(client._voucher_task, 1)
        (message,) = [call.args[0] for call in client._send.await_args_list]
        assert message.voucher.cumulative == ledger.voucher.cumulative == sent
        verify_voucher(DOMAIN, DESCRIPTOR, sent, message.voucher.signature)
        await asyncio.wait_for(client._renew(), 1)  # The same requirement: nothing more.
        await client._voucher(101, generation=1, run_id=RUN)  # Above the deposit of 100.
        assert client._send.await_count == 1 and ledger.voucher.cumulative == sent

    asyncio.run(scenario())


@pytest.mark.parametrize("value", (-1, True, 1.0, "20", 1 << 64))
def test_voucher_headroom_is_an_explicit_uint64_choice(tmp_path: Path, value) -> None:
    async def stop() -> None:
        pass

    chain = SimpleNamespace(
        reader=SimpleNamespace(profile=SimpleNamespace(domain=DOMAIN)), stop=stop
    )
    options = {
        "wallet": SimpleNamespace(address=DESCRIPTOR.agent),
        "signer": SIGNER,
        "chain": chain,
        "contract": ContractCalls(DOMAIN, "USDC", "2"),
        "checkpoint_path": tmp_path / "agent.json",
        "signer_reference": "fixture:public-test-key",
    }
    with pytest.raises(ValidationError, match="voucher_headroom"):
        Client(7, voucher_headroom=value, **options)
    client = Client(7, **options)
    assert client._voucher_headroom == 0  # Off unless the caller chooses it.
    client.close()
    options["checkpoint_path"] = tmp_path / "other.json"
    client = Client(7, voucher_headroom=20, **options)
    assert client._voucher_headroom == 20
    client.close()


REFUSALS = {
    "deposit": PaymentFailure(PaymentCode.INSUFFICIENT_DEPOSIT),
    "authorization": PaymentFailure(PaymentCode.SESSION_NOT_READY, "AUTHORIZATION_REQUIRED"),
    "price": PaymentFailure(
        PaymentCode.SESSION_NOT_READY, "PRICE_CHANGED", None, None, b"\x01" * 32, b"\x02" * 32
    ),
    "after_output": PaymentFailure(PaymentCode.INSUFFICIENT_DEPOSIT),
}


@pytest.mark.parametrize("refusal", sorted(REFUSALS))
def test_a_run_refused_before_admission_is_not_admitted_so_a_closed_session_shuts_down_clean(
    refusal,
):
    """A provider refused the run at prepare. After a confirmed close the
    shutdown must not list the session or the run as unresolved."""

    async def scenario() -> None:
        failure = REFUSALS[refusal]

        class Refusing(services.ChatServicer):
            async def Completions(self, request, context):
                if refusal == "after_output":
                    # Output arrived, so it was admitted: the error proves nothing.
                    yield pb.ChatChunk(payload=b'{"choices":[]}')
                code, message, metadata = payment_status(failure)
                await context.abort(code, message, trailing_metadata=metadata)

        server = grpc.aio.server(options=GRPC_OPTIONS)
        services.add_ChatServicer_to_server(Refusing(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}", options=GRPC_OPTIONS)
        client = component_client()
        ledger = client._ledger
        (run,) = ledger.runs.values()
        try:
            output = _Output(ByteQueue(max_messages=4, max_bytes=16384, wait_ms=50))
            client._outputs[run.context.run_id] = output
            output.call = services.ChatStub(channel).Completions(pb.CompleteRequest())
            await client._read_complete(run.context.run_id, True)
        finally:
            await channel.close()
            await server.stop(0)
        if refusal == "after_output":
            assert run.final is None and ledger.pending == (run.context.run_id,)
            return
        assert run.final == (0, 0, 0, pb.RUN_STATUS_NOT_ADMITTED, 0, 0)
        assert ledger.pending == ()
        # The caller still sees the provider's refusal.
        with pytest.raises(PaymentError):
            await output.queue.get()
        # The session then closes cooperatively and the client shuts down.
        ledger.close_pending = True
        observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32)
        client._closed_result = OperationResult(
            "close-x", OperationState.CONFIRMED, tx_hash=b"t" * 32, observation=observation
        )
        client._outputs.clear()
        client._control = client._outbound = client._channel = None
        client._control_tasks, client._operations, client._restored = [], {}, False
        client.chain.stop = AsyncMock()
        result = await client._shutdown()
        assert result.unresolved_session_ids == () and result.unresolved_runs == ()

    asyncio.run(scenario())


def stale_follower(client: Client, monkeypatch) -> list[float]:
    """A real follower whose RPC has been down for 120 s since its last evidence."""
    monkeypatch.setattr(follower_module, "_sleep", AsyncMock())
    clock = [1_000.0, 1_800_000_000.0]
    ledger = client._ledger
    block = Block(
        ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32),
        int(clock[1]),
        b"c" * 32,
    )
    state = ChainSession(100, 0, SessionStatus.OPEN, 0, int(clock[1]) - 10, CATALOG.rate_card_hash)
    down = Mock(side_effect=TransportError("HTTP request failed", transient=True))
    follower = ChainFollower(
        SimpleNamespace(_blocks=down),
        max_age_ms=60_000,
        collection_margin_ms=1_000,
        monotonic=lambda: clock[0],
        wall_clock=lambda: clock[1],
    )
    follower._running = True
    follower._accept(
        ChainSnapshot(
            block,
            PauseLevel.NONE,
            MappingProxyType({}),
            MappingProxyType({ledger.session_id: state}),
            clock[0],
        )
    )
    clock[0] += 120
    clock[1] += 120
    client.chain = follower
    return clock


def test_an_attached_session_serves_within_its_deposit_while_the_rpc_is_down(monkeypatch):
    """Requests read no chain (only local state); money steps still need fresh evidence."""

    async def scenario() -> None:
        client = component_client()
        stale_follower(client, monkeypatch)
        ledger = client._ledger
        assert client.chain.readiness(ledger.session_id).failures[0].reason == "CHAIN_STATE_STALE"
        assert client._require_ready() is ledger
        await client._voucher(4, generation=1, run_id=b"r" * 32)
        voucher = pb.ControlMessage.FromString(await client._outbound.get()).voucher
        assert voucher.cumulative == 4
        verify_voucher(DOMAIN, DESCRIPTOR, 4, voucher.signature)
        await client._voucher(101, generation=1, run_id=b"r" * 32)  # Beyond the deposit.
        assert ledger.voucher.cumulative == 4
        # Top-up and exits move money on chain: they need fresh evidence first.
        sign = Mock(return_value=b"\x01" * 65)
        client.wallet = SimpleNamespace(address=DESCRIPTOR.agent, sign_funding_authorization=sign)
        client.agent_id, client._local_start = 43, AsyncMock()
        client._open_lock, client._operations, client._saved = asyncio.Lock(), {}, {}
        client.contract = ContractCalls(DOMAIN, "USDC", "2")
        client._submit = AsyncMock()
        with pytest.raises(TransportError):
            await client._top_up(ledger.session_id, 10)
        with pytest.raises(TransportError):
            await client._exit(ledger.session_id, OperationKind.REQUEST_CLOSE)
        sign.assert_not_called()
        client._submit.assert_not_awaited()
        assert not ledger.close_pending

    asyncio.run(scenario())


def test_contradicting_chain_evidence_stops_requests(monkeypatch):
    """Unavailability never stops requests; evidence against the deployment does."""

    async def scenario() -> None:
        client = component_client()
        stale_follower(client, monkeypatch)
        client.chain.reader._blocks = Mock(
            side_effect=ChainContradictionError("RPC chain differs from the selected deployment")
        )
        with pytest.raises(ChainContradictionError):
            await client.chain.refresh()
        with pytest.raises(PaymentError) as refused:
            client._require_ready()
        assert refused.value.failure.reason == "INCONSISTENT_STATE"

    asyncio.run(scenario())


def test_a_failed_background_refresh_reaches_the_error_hook():
    errors = []

    class Errors:
        seen = threading.Event()

        def __call__(self, error):
            errors.append(error)
            self.seen.set()

    hook = Errors()
    client = component_client()
    client._observers = Observers(Hooks(on_error=hook))
    client._refresh_failed(
        follower_module.RefreshFailure("TransportError", "HTTP request failed", 1.0)
    )
    assert hook.seen.wait(2)
    assert (errors[0].operation, errors[0].code) == ("chain", "TransportError")
    assert errors[0].detail == "background refresh: HTTP request failed"
    client._observers.close()


@pytest.mark.parametrize("own", [False, True])
def test_a_follower_keeps_the_refresh_failure_callback_its_caller_set(tmp_path, own):
    reader = SimpleNamespace(profile=SimpleNamespace(domain=DOMAIN))
    follower = ChainFollower(reader, max_age_ms=60_000, collection_margin_ms=1_000)
    callback = Mock()
    if own:
        follower.on_refresh_failure = callback
    client = Client(
        7,
        wallet=SimpleNamespace(address=DESCRIPTOR.agent),
        signer=SIGNER,
        chain=follower,
        contract=ContractCalls(DOMAIN, "USDC", "2"),
        checkpoint_path=tmp_path / "agent.json",
        signer_reference="fixture:public-test-key",
    )
    try:
        assert follower.on_refresh_failure == (callback if own else client._refresh_failed)
    finally:
        client.close()


def top_up_client(timestamp=1_800_000_000):
    """A component client that can top up, with a wallet that records its signing.

    Its follower accepted block 10 of a final network, where USDC holds every
    authorization unused.
    """
    client = component_client()
    client._funding_lock = asyncio.Lock()
    client._local_start = AsyncMock()
    client._operations, client._saved = {}, {}
    client._control = None
    client.contract = ContractCalls(DOMAIN, "USDC", "2")
    block = SimpleNamespace(observation=SimpleNamespace(block_number=10), timestamp=timestamp)
    client.chain.current = current = SimpleNamespace(
        sessions=client.chain.snapshot().sessions, block=block
    )
    client.chain.snapshot = lambda: current
    client.chain.refresh = AsyncMock()
    sign = Mock(return_value=b"\x01" * 65)
    client.wallet = SimpleNamespace(address=DESCRIPTOR.agent, sign_funding_authorization=sign)
    fundable(client)
    client.chain.reader.profile = SimpleNamespace(domain=DOMAIN, instant_finality=True)
    client.chain.reader._call = Mock(return_value=bytes(32))  # authorizationState: false
    client.chain.reader._check_canonical = Mock()
    return client, current, sign


def authorization_state(nonce):
    """USDC authorizationState(agent, nonce) calldata (EIP3009.sol:64)."""
    return bytes.fromhex("e94a0102") + bytes(12) + DESCRIPTOR.agent + nonce


def test_a_failed_top_up_is_retried_with_its_same_authorization_until_it_expires():
    """A definite failure is retried with the same amount and EIP-3009 nonce, so an
    earlier attempt that somehow landed could never fund twice."""

    async def scenario():
        client, current, sign = top_up_client()
        ledger = client._ledger

        def failed(operation, ref):
            result = OperationResult(ref, OperationState.FAILED, tx_hash=b"t" * 32)
            client._operations[ref] = (operation, result)
            return result

        client._submit = AsyncMock(side_effect=failed)
        await client._top_up(ledger.session_id, 10)
        first = sign.call_args.args[0]
        await client._top_up(ledger.session_id, 10)
        again = sign.call_args.args[0]
        assert again == first  # Same amount, nonce and validity window.
        # USDC held it unused at the follower's block before it was resent: one
        # call pinned to that final block, which is not read again.
        assert client.chain.reader._call.call_args.args == (
            DESCRIPTOR.token,
            authorization_state(first.nonce),
            current.block,
        )
        client.chain.reader._check_canonical.assert_not_called()
        refs = [call.args[1] for call in client._submit.call_args_list]
        assert len(set(refs)) == 2  # Each attempt has its own reference.
        with pytest.raises(ClientError, match="earlier top-up of 10 is still valid"):
            await client._top_up(ledger.session_id, 11)
        assert sign.call_count == 2
        client.chain.confirm.assert_not_called()  # No deposit from a failure.
        # Once it has expired, a top-up starts over with a new nonce.
        current.block = SimpleNamespace(timestamp=first.valid_before)
        await client._top_up(ledger.session_id, 11)
        fresh = sign.call_args.args[0]
        assert fresh.nonce != first.nonce and fresh.value == 11

    asyncio.run(scenario())


def test_a_wallet_that_signs_at_execute_tops_up_with_its_placeholder():
    async def scenario():
        client, _, sign = top_up_client()
        sign.return_value = bytes(65)
        client.wallet.signs_at_execute = True
        ledger = client._ledger
        client._submit = AsyncMock(
            side_effect=lambda operation, ref: OperationResult(ref, OperationState.UNKNOWN)
        )
        await client._top_up(ledger.session_id, 10)
        operation, _ = client._submit.call_args.args
        auth = sign.call_args.args[0]
        assert operation == client.contract.top_up(ledger.descriptor, 10, auth, bytes(65))

    asyncio.run(scenario())


def test_a_confirmed_top_up_takes_its_new_deposit_from_its_event():
    async def scenario():
        client, _, sign = top_up_client()
        ledger = client._ledger
        outcome = SessionOperationOutcome(ledger.session_id, 10, deposit=110)

        async def confirmed(operation, ref):
            observation = ChainObservation(
                DOMAIN.chain_id, DOMAIN.verifying_contract, 11, b"n" * 32
            )
            return OperationResult(
                ref, OperationState.CONFIRMED, tx_hash=b"t" * 32, observation=observation,
                sessions=(outcome,),
            )  # fmt: skip

        client._submit = AsyncMock(side_effect=confirmed)
        await client._top_up(ledger.session_id, 10)
        client.chain.refresh.assert_not_awaited()
        ((sid, state), _) = client.chain.confirm.call_args
        assert sid == ledger.session_id and state.deposit == 110
        # The next top-up is a new authorization, never the confirmed one again.
        first = sign.call_args.args[0]
        await client._top_up(ledger.session_id, 10)
        assert sign.call_args.args[0].nonce != first.nonce

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "case", ["contradicted", "concurrent", "follower_past", "reconciled_again"]
)
def test_a_top_up_event_shows_at_least_the_deposit_confirmed_before_it_plus_its_amount(case):
    """topUp sets the deposit before it plus its amount, and deposits only grow
    (MPPI.sol open, topUp). The deposit confirmed when the intent was recorded,
    before anything was sent, is the prior: the follower's later view may
    already hold this top-up, from a refresh or from its own event."""

    async def scenario():
        client, current, sign = top_up_client()
        ledger = client._ledger
        # Deposit 100 confirmed before sending; the top-up of 10 lands at block 11.
        deposit = 105 if case == "contradicted" else 130 if case == "concurrent" else 110
        observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 11, b"n" * 32)
        outcome = SessionOperationOutcome(ledger.session_id, 10, deposit=deposit)

        async def confirmed(operation, ref):
            if case == "follower_past":
                # A background refresh accepted block 11 during the wallet call.
                current.block.observation.block_number = 11
                state = current.sessions[ledger.session_id]
                current.sessions[ledger.session_id] = replace(state, deposit=110)
            result = OperationResult(
                ref, OperationState.CONFIRMED, tx_hash=b"t" * 32, observation=observation,
                sessions=(outcome,),
            )  # fmt: skip
            client._operations[ref] = (operation, result)
            return result

        client._submit = AsyncMock(side_effect=confirmed)
        if case == "contradicted":
            with pytest.raises(ClientError, match="contradicts the confirmed deposit"):
                await client._top_up(ledger.session_id, 10)
            # Fail closed: the event raises no spending bound and sends no hint;
            # its proven record and intent stay, and a reconcile checks it again.
            client.chain.confirm.assert_not_called()
            assert client._top_up_hint is None
            ((ref, (_, result)),) = client._operations.items()
            assert result.state is OperationState.CONFIRMED
            assert client._saved["funding_intent"]["ref"] == ref
            with pytest.raises(ClientError, match="contradicts the confirmed deposit"):
                await client._top_up_confirmed(result)
            # Its own authorization may still land: a retry reads it and resends
            # it while unused, never a new nonce, and refuses once it is used.
            first = sign.call_args.args[0]
            await_count = client._submit.await_count
            # A restart keeps no event: the restored record names no deposit.
            client._operations[ref] = (client._operations[ref][0], replace(result, sessions=()))
            with pytest.raises(ClientError, match="contradicts the confirmed deposit"):
                await client._top_up(ledger.session_id, 10)
            assert sign.call_args.args[0].nonce == first.nonce
            assert client.chain.reader._call.call_args.args[1] == authorization_state(first.nonce)
            assert client._submit.await_count == await_count + 1
            client.chain.reader._call.return_value = (1).to_bytes(32, "big")
            with pytest.raises(ClientError, match="earlier top-up of 10 took effect"):
                await client._top_up(ledger.session_id, 10)
            assert client._submit.await_count == await_count + 1
        else:
            result = await client._top_up(ledger.session_id, 10)
            assert result.state is OperationState.CONFIRMED
            ((_, state), _) = client.chain.confirm.call_args
            assert state.deposit == deposit  # Another top-up may have landed first.
            if case == "reconciled_again":
                # Its own event is now in the follower's view of block 10.
                current.sessions[ledger.session_id] = state
                await client._top_up_confirmed(result)
                assert client.chain.confirm.call_args.args[1].deposit == 110

    asyncio.run(scenario())


def test_an_intent_outliving_its_confirmation_is_read_before_any_new_top_up():
    """A crash after a top-up was persisted CONFIRMED but before its intent was
    spent: the intent alone cannot say whether that event was its own, so the
    authorization is read, and once used every top-up waits for it to expire."""

    async def scenario():
        client, current, sign = top_up_client()
        ledger = client._ledger
        observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 11, b"n" * 32)

        def submitted(operation, ref):
            # Persisted CONFIRMED; the process stops before the intent is spent.
            result = OperationResult(
                ref, OperationState.CONFIRMED, tx_hash=b"t" * 32, observation=observation
            )
            client._operations[ref] = (operation, result)
            return OperationResult(ref, OperationState.UNKNOWN)

        client._submit = AsyncMock(side_effect=submitted)
        await client._top_up(ledger.session_id, 10)
        first = sign.call_args.args[0]
        assert client._saved["funding_intent"]["nonce"] == first.nonce.hex()
        client.chain.reader._call.return_value = (1).to_bytes(32, "big")
        with pytest.raises(ClientError, match="earlier top-up of 10 took effect"):
            await client._top_up(ledger.session_id, 10)
        assert client.chain.reader._call.call_args.args[1] == authorization_state(first.nonce)
        assert sign.call_count == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("case", ["used", "unreadable"])
def test_a_retry_never_resends_an_authorization_usdc_already_used(case):
    """A FAILED top-up can still have taken effect: a status-0 bundle can be
    front-run while the same authorization funds the session. Before a retry
    sends it again, USDC's authorizationState is read no earlier than where that
    failure was seen. A used one took effect: nothing is sent, and every top-up
    is refused the same way until it expires, so a caller retrying the error
    never funds twice. An unreadable state sends nothing either."""

    async def scenario():
        client, current, sign = top_up_client()
        ledger = client._ledger
        seen = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 12, b"f" * 32)

        def failed(operation, ref):
            result = OperationResult(
                ref, OperationState.FAILED, tx_hash=b"t" * 32, observation=seen,
                cause="the MPPI call did not take effect in its transaction",
            )  # fmt: skip
            client._operations[ref] = (operation, result)
            return result

        client._submit = AsyncMock(side_effect=failed)
        await client._top_up(ledger.session_id, 10)
        first = sign.call_args.args[0]
        later = SimpleNamespace(observation=seen, timestamp=current.block.timestamp + 6)

        async def refresh():
            current.block = later

        client.chain.refresh = AsyncMock(side_effect=refresh)
        client.chain.reader._call.return_value = (1).to_bytes(32, "big")
        if case == "used":
            with pytest.raises(ClientError, match="earlier top-up of 10 took effect"):
                await client._top_up(ledger.session_id, 11)
        else:
            client.chain.reader._call.side_effect = ChainEvidenceError(
                "RPC request returned no usable result"
            )
            with pytest.raises(ChainEvidenceError):
                await client._top_up(ledger.session_id, 10)
        # The follower was behind that failure's block: it refreshed, then read there.
        client.chain.refresh.assert_awaited_once()
        assert client.chain.reader._call.call_args.args == (
            DESCRIPTOR.token,
            authorization_state(first.nonce),
            later,
        )
        assert sign.call_count == 1 and client._submit.await_count == 1  # Nothing resent.
        assert client._saved["funding_intent"]["nonce"] == first.nonce.hex()
        if case == "used":
            with pytest.raises(ClientError, match="earlier top-up of 10 took effect"):
                await client._top_up(ledger.session_id, 10)  # The same amount again.
            assert sign.call_count == 1 and client._submit.await_count == 1
            # Once it has expired, a new top-up gets a new nonce.
            current.block = SimpleNamespace(observation=seen, timestamp=first.valid_before)
            client._submit = AsyncMock(
                side_effect=lambda operation, ref: OperationResult(ref, OperationState.UNKNOWN)
            )
            await client._top_up(ledger.session_id, 10)
            assert sign.call_args.args[0].nonce != first.nonce

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "case", ["at_valid_before", "not_final", "before", "used", "noncanonical", "another_intent"]
)
def test_an_uncertain_top_up_is_void_only_at_a_canonical_block_past_its_authorization(case):
    """An uncertain top-up blocks every later one. At a canonical block at or after
    its valid_before, an authorization USDC holds unused never took effect and
    never can (EIP3009.sol:313-324), so reconciling it reports FAILED."""

    async def scenario():
        client, current, sign = top_up_client()
        ledger = client._ledger

        def unknown(operation, ref):
            result = OperationResult(ref, OperationState.UNKNOWN)
            client._operations[ref] = (operation, result)
            return result

        client._submit = AsyncMock(side_effect=unknown)
        await client._top_up(ledger.session_id, 10)
        ((ref, _),) = client._operations.items()
        auth = sign.call_args.args[0]
        with pytest.raises(ClientError, match="reconcile pending funding"):
            await client._top_up(ledger.session_id, 10)
        client.wallet.reconcile = Mock(return_value=OperationResult(ref, OperationState.UNKNOWN))
        current.block.timestamp = auth.valid_before  # Reached exactly.
        if case == "before":
            current.block.timestamp = auth.valid_before - 1
        elif case == "used":
            client.chain.reader._call.return_value = (1).to_bytes(32, "big")
        elif case in ("not_final", "noncanonical"):
            # Without instant finality the block is read again by number.
            client.chain.reader.profile.instant_finality = False
            if case == "noncanonical":
                client.chain.reader._check_canonical.side_effect = ChainEvidenceError(
                    "canonical block evidence changed during observation"
                )
        elif case == "another_intent":
            client._saved["funding_intent"]["ref"] = "top-up-another"
        result = await client._reconcile_ref(ref)
        if case in ("at_valid_before", "not_final"):
            assert result.state is OperationState.FAILED
            assert result.cause == "authorization expired before any top-up"
            assert client._operations[ref][1].state is OperationState.FAILED
            assert client.chain.reader._call.call_args.args == (
                DESCRIPTOR.token,
                authorization_state(auth.nonce),
                current.block,
            )
            if case == "not_final":
                client.chain.reader._check_canonical.assert_called_once_with(current.block)
            else:
                client.chain.reader._check_canonical.assert_not_called()
            # It no longer blocks: the next top-up is a new authorization.
            await client._top_up(ledger.session_id, 10)
            assert sign.call_args.args[0].nonce != auth.nonce
        else:
            assert result.state is OperationState.UNKNOWN
            assert client._operations[ref][1].state is OperationState.UNKNOWN
            if case in ("before", "another_intent"):
                client.chain.reader._call.assert_not_called()

    asyncio.run(scenario())


def test_a_fresh_opening_records_its_proven_session_for_requests():
    client = component_client()
    ledger = client._ledger
    # The evidence read before submission, at a block before the opening's.
    block = SimpleNamespace(observation=SimpleNamespace(block_number=11), timestamp=1_799_999_990)
    client.chain.current = SimpleNamespace(sessions={}, block=block)
    observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 12, b"o" * 32)
    outcome = OperationResult(
        "open-x",
        OperationState.CONFIRMED,
        tx_hash=b"t" * 32,
        observation=observation,
        sessions=(SessionOperationOutcome(ledger.session_id, 100, deposit=100),),
    )
    client._confirm_opened(ledger, outcome)
    ((sid, state), proving) = client.chain.confirm.call_args
    assert sid == ledger.session_id and proving == {"observation": observation}
    assert (state.deposit, state.settled_cumulative, state.status) == (100, 0, SessionStatus.OPEN)
    # An activity time no later than the opening's block: never a late deadline.
    assert state.last_activity == 1_799_999_990
    assert state.initial_rate_card_hash == ledger.initial_hash


class _OpenedChain:
    """A final network: the session opened at block 10; block n is at 1_800_000_000 + n // 2."""

    profile = SimpleNamespace(instant_finality=True, domain=DOMAIN)

    def __init__(self, opened: ChainSession, head: int) -> None:
        self.opened, self.head, self.latest_reads = opened, head, 0

    @staticmethod
    def block(number: int) -> Block:
        return Block(
            ChainObservation(
                DOMAIN.chain_id, DOMAIN.verifying_contract, number, bytes([number]) * 32
            ),
            1_800_000_000 + number // 2,
            bytes([number - 1]) * 32,
        )

    def state(self, number: int) -> ChainSession:
        return (
            self.opened if number >= 10 else ChainSession(0, 0, SessionStatus.OPEN, 0, 0, bytes(32))
        )

    def _latest(self, providers, sessions, funding):
        assert funding is None
        self.latest_reads += 1
        states = {sid: self.state(self.head) for sid in sessions}
        return self.block(self.head), PauseLevel.NONE, {}, states, None

    def _observe_at(self, number, block_hash, *, providers, sessions, recheck, funding):
        assert block_hash == bytes([number]) * 32 and funding is None
        states = {sid: self.state(number) for sid in sessions}
        return self.block(number), None, PauseLevel.NONE, {}, states, None

    def session_at(self, number, block_hash, session_id):
        return self.block(number), self.state(number)


@pytest.mark.parametrize(("head", "provider"), [(12, 11), (12, 12), (12, 14), (10, 12), (8, 12)])
def test_an_opening_a_background_refresh_overtook_keeps_requests_ready(monkeypatch, head, provider):
    """The opening lands at block 10. While the wallet signed and submitted it,
    a background refresh accepted block `head`; the provider's snapshot names
    block `provider`. The session's recorded activity is never later than the
    chain's, the attachment adds no whole refresh, and the next refresh is
    accepted."""

    async def scenario() -> None:
        monkeypatch.setattr(follower_module, "_sleep", AsyncMock())
        client = component_client()
        ledger = client._ledger
        opened = ChainSession(100, 0, SessionStatus.OPEN, 0, 1_800_000_005, ledger.initial_hash)
        chain = _OpenedChain(opened, head)
        clock = [1_000.0, 1_800_000_010.0]
        follower = ChainFollower(
            chain,
            max_age_ms=60_000,
            collection_margin_ms=1_000,
            monotonic=lambda: clock[0],
            wall_clock=lambda: clock[1],
        )
        follower._running = True
        await follower.refresh()  # Before the session is watched.
        client.chain = follower
        follower.follow(sessions=(ledger.session_id,))
        client._confirm_opened(
            ledger,
            OperationResult(
                "open-x",
                OperationState.CONFIRMED,
                tx_hash=b"t" * 32,
                observation=chain.block(10).observation,
                sessions=(SessionOperationOutcome(ledger.session_id, 100, deposit=100),),
            ),
        )
        recorded = follower.current.sessions.get(ledger.session_id)
        assert recorded is None or recorded.last_activity <= opened.last_activity
        reads = chain.latest_reads
        current, observed = await client._snapshot_evidence(
            provider, bytes([provider]) * 32, ledger.session_id
        )
        assert chain.latest_reads == reads
        assert current.sessions[ledger.session_id] == observed == opened
        clock[0] += 15
        clock[1] += 10
        chain.head = 27
        await follower.refresh()
        assert follower.confirmed_readiness(ledger.session_id).ready
        assert follower.current.sessions[ledger.session_id] == opened

    asyncio.run(scenario())


def test_a_refresh_accepted_during_the_attachment_read_keeps_its_newer_state(monkeypatch):
    """The follower is past the provider's block without the new session. A
    refresh that reads it, topped up since, is accepted while the attachment
    reads it at the provider's block: the newer state stays."""

    async def scenario() -> None:
        monkeypatch.setattr(follower_module, "_sleep", AsyncMock())
        client = component_client()
        ledger = client._ledger
        opened = ChainSession(100, 0, SessionStatus.OPEN, 0, 1_800_000_005, ledger.initial_hash)
        topped = replace(opened, deposit=150, last_activity=1_800_000_006)
        chain = _OpenedChain(opened, 12)
        follower = ChainFollower(
            chain,
            max_age_ms=60_000,
            collection_margin_ms=1_000,
            monotonic=lambda: 1_000.0,
            wall_clock=lambda: 1_800_000_010.0,
        )
        follower._running = True
        await follower.refresh()
        client.chain = follower
        follower.follow(sessions=(ledger.session_id,))
        newer = ChainSnapshot(
            chain.block(13),
            PauseLevel.NONE,
            MappingProxyType({}),
            MappingProxyType({ledger.session_id: topped}),
            1_000.0,
        )

        def session_at(number, block_hash, session_id):
            follower._accept(newer)  # A background refresh, meanwhile.
            return chain.block(number), opened

        chain.session_at = session_at
        current, observed = await client._snapshot_evidence(11, bytes([11]) * 32, ledger.session_id)
        assert observed == opened
        assert current.sessions[ledger.session_id] == topped
        assert follower.current.sessions[ledger.session_id] == topped

    asyncio.run(scenario())


@pytest.mark.parametrize("served", [True, False], ids=["third_read", "never"])
def test_a_snapshot_block_not_yet_served_is_read_again_after_jittered_waits(
    monkeypatch, served
) -> None:
    """Agents refused together must not repeat in step: each repeat waits a
    uniformly random share of 250 ms, then of 1 s, as chain reads do."""
    waits: list[float] = []

    async def sleep(delay: float) -> None:
        waits.append(delay)

    monkeypatch.setattr(client_module, "_sleep", sleep)
    monkeypatch.setattr(client_module, "_jitter", lambda: 0.5)

    async def scenario() -> None:
        client = component_client()
        ledger = client._ledger
        state = client.chain.current.sessions[ledger.session_id]
        reached = SimpleNamespace(sessions={ledger.session_id: state})
        lag = ChainLagError("RPC has not served that block yet")

        def stale():
            raise ChainEvidenceError("chain observation is unavailable or stale")

        client.chain.snapshot = stale
        client.chain.refresh_at = AsyncMock(side_effect=[lag, lag, reached if served else lag])
        if served:
            current, observed = await client._snapshot_evidence(12, b"h" * 32, ledger.session_id)
            assert current is reached and observed == state
        else:
            with pytest.raises(
                ClientError, match="^confirmed chain observation precedes snapshot$"
            ):
                await client._snapshot_evidence(12, b"h" * 32, ledger.session_id)
        assert client.chain.refresh_at.await_count == 3
        assert waits == [0.125, 0.5]

    asyncio.run(scenario())


@pytest.mark.parametrize("served", [True, False], ids=["third_read", "never"])
def test_a_snapshot_block_behind_the_head_not_yet_imported_is_read_again_after_jittered_waits(
    monkeypatch, served
) -> None:
    """The follower is already past the provider's snapshot block, so only the
    session is read at that block. A backend that has not imported it yet
    (-32014 outlasting the reader's repeats is ChainLagError) is read again
    after the same jittered waits as a refresh at that block. A block never
    served ends in a ClientError that says so, not a protocol error; the
    refresh's own says its evidence precedes the snapshot (pinned above)."""
    waits: list[float] = []

    async def sleep(delay: float) -> None:
        waits.append(delay)

    monkeypatch.setattr(client_module, "_sleep", sleep)
    monkeypatch.setattr(client_module, "_jitter", lambda: 0.5)

    async def scenario() -> None:
        client = component_client()
        ledger = client._ledger
        state = client.chain.current.sessions[ledger.session_id]
        head = Block(
            ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 20, b"h" * 32),
            100,
            b"p" * 32,
        )
        current = SimpleNamespace(block=head, sessions={ledger.session_id: state})
        client.chain.snapshot = lambda: current
        lag = ChainLagError("RPC backend has not imported that block yet")
        reads = Mock(side_effect=[lag, lag, (head, state) if served else lag])
        client.chain.reader = SimpleNamespace(session_at=reads)
        if served:
            evidence, observed = await client._snapshot_evidence(12, b"s" * 32, ledger.session_id)
            assert evidence is current and observed == state
        else:
            with pytest.raises(ClientError, match="^snapshot block was not served by the RPC$"):
                await client._snapshot_evidence(12, b"s" * 32, ledger.session_id)
        assert reads.call_args_list == [call(12, b"s" * 32, ledger.session_id)] * 3
        assert waits == [0.125, 0.5]

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["top_up", "close"])
def test_a_lagging_read_after_a_confirmed_top_up_or_close_is_lag(monkeypatch, operation):
    """The follower accepted block 12; the agent's top-up or close lands at
    block 20 and is proven from its event. A backend that has reached block
    15 but not 20 serves the earlier deposit or status: lag, and requests keep
    the proven state. A refresh at or after block 20 replaces it."""

    async def scenario() -> None:
        monkeypatch.setattr(follower_module, "_sleep", AsyncMock())
        client = component_client()
        ledger = client._ledger
        opened = ChainSession(100, 0, SessionStatus.OPEN, 0, 1_800_000_005, ledger.initial_hash)
        chain = _OpenedChain(opened, 12)
        follower = ChainFollower(
            chain,
            max_age_ms=60_000,
            collection_margin_ms=1_000,
            monotonic=lambda: 1_000.0,
            wall_clock=lambda: 1_800_000_010.0,
        )
        follower._running = True
        follower.follow(sessions=(ledger.session_id,))
        await follower.refresh()
        client.chain = follower
        client._saved = {"version": 1}
        proving = chain.block(20).observation
        if operation == "top_up":
            outcome = SessionOperationOutcome(ledger.session_id, 10, deposit=110)
            result = OperationResult(
                "top-up-x", OperationState.CONFIRMED, tx_hash=b"t" * 32, observation=proving,
                sessions=(outcome,),
            )  # fmt: skip
            await client._top_up_confirmed(result)
            proven = replace(opened, deposit=110)
        else:
            client._saved["reported_close"] = {"tx": "74" * 32, "capture": 0, "refunded": 100}
            client._closed_error = None
            chain.closing = lambda tx, sid: ClosingEvidence(proving, 0, 100, tx)
            await client._prove_close()
            proven = replace(opened, status=SessionStatus.CLOSED)
        readiness = follower.confirmed_readiness(ledger.session_id)
        chain.head = 15
        await follower.refresh()
        assert follower.current.sessions[ledger.session_id] == proven
        assert follower.confirmed_readiness(ledger.session_id) == readiness
        chain.opened, chain.head = replace(proven, last_activity=1_800_000_010), 21
        await follower.refresh()
        assert follower.current.sessions[ledger.session_id] == chain.opened

    asyncio.run(scenario())


def test_a_session_s_accepted_terms_and_run_outcomes_are_public():
    """An integration reads prices and outcomes without SDK internals or chain reads."""
    from mppi_agent import RunOutcomeStatus

    async def scenario():
        client = component_client()
        client._local_start = AsyncMock()
        ledger = client._ledger
        changed = publish_catalog((CatalogRow("test/model", Rates(600_000, 700_000)),))
        identity = SimpleNamespace(catalog=changed)
        client.chain.current = SimpleNamespace(
            sessions=client.chain.snapshot().sessions, identities={7: identity}
        )
        terms = await client._terms(ledger.session_id)
        assert terms.initial_rate_card_hash == CATALOG.rate_card_hash
        assert set(terms.catalogs) == {CATALOG.rate_card_hash}  # Accepted only.
        assert terms.current_rate_card_hash == changed.rate_card_hash  # Not yet accepted.
        assert terms.rates("test/model") == Rates(500_000, 500_000)
        pending = await client._run_outcome(ledger.session_id, b"r" * 32)
        assert pending.status is RunOutcomeStatus.PENDING and pending.amount is None
        assert (pending.model, pending.rate_card_hash) == ("test/model", CATALOG.rate_card_hash)
        ledger.record_not_admitted(b"r" * 32)
        never = await client._run_outcome(ledger.session_id, b"r" * 32)
        assert never.status is RunOutcomeStatus.NOT_ADMITTED
        assert never.amount == 0 and never.counts == TokenCounts(0, 0, 0)
        with pytest.raises(ClientError, match="not retained"):
            await client._run_outcome(ledger.session_id, b"x" * 32)

    asyncio.run(scenario())


def test_a_money_steps_measured_parts_never_add_up_to_more_than_its_total():
    """Parts of 36.7, 27.7 and 58.7 ms in a step of 123.2 ms: each rounded to
    nearest, they summed to 124, more than the step's 123."""

    async def scenario():
        seen = []
        done = threading.Event()

        def record(timing):
            seen.append(timing)
            done.set()

        client = component_client()
        client._observers = Observers(Hooks(on_timing=record))

        async def step():
            timing = client_module._TIMING.get()
            timing.parts.update(chain=0.0367, wallet_sign=0.0277, wallet_submit=0.0587)
            timing.start = time.monotonic() - 0.1232
            return OperationResult("requestClose-x", OperationState.PENDING)

        await client._measured("requestClose", step())
        assert await asyncio.to_thread(done.wait, 2)
        (timing,) = seen
        parts = timing.chain_ms + timing.wallet_sign_ms + timing.wallet_submit_ms
        assert timing.provider_ms >= 0 and parts + timing.provider_ms == timing.total_ms
        assert (timing.chain_ms, timing.wallet_sign_ms, timing.wallet_submit_ms) == (36, 27, 58)

    asyncio.run(scenario())


def test_each_money_step_reports_where_its_time_went():
    """on_timing splits a step into this SDK's chain reads, the wallet's signing and
    submission, and the rest (the provider exchange and local work)."""
    from mppi_agent import OperationTiming

    async def scenario():
        seen = []

        class Record:
            done = threading.Event()

            def __call__(self, timing):
                seen.append(timing)
                self.done.set()

        record = Record()
        client = component_client()
        client._observers = Observers(Hooks(on_timing=record))
        client._operations = {}
        client._persist = AsyncMock()

        class SlowWallet:
            address = DESCRIPTOR.agent

            def execute(self, call, *, operation_ref):
                time.sleep(0.05)  # The wallet service, until it returns.
                return OperationResult(operation_ref, OperationState.PENDING, "wallet-1")

        client.wallet = SlowWallet()
        client.chain.reader = SimpleNamespace(profile=SimpleNamespace(domain=DOMAIN))
        operation = ContractCalls(DOMAIN, "USDC", "2").request_close(DESCRIPTOR)

        async def step():
            with client_module._timed("chain"):
                await asyncio.sleep(0.03)
            with client_module._timed("wallet_sign"):
                await asyncio.sleep(0.02)
            return await client._submit(operation, "requestClose-x")

        result = await client._measured("requestClose", step())
        assert result.state is OperationState.PENDING
        assert await asyncio.to_thread(record.done.wait, 2)
        (timing,) = seen
        assert isinstance(timing, OperationTiming)
        assert (timing.operation, timing.state) == ("requestClose", "pending")
        assert timing.chain_ms >= 30 and timing.wallet_sign_ms >= 20
        assert timing.wallet_submit_ms >= 50
        assert timing.total_ms >= timing.chain_ms + timing.wallet_sign_ms + timing.wallet_submit_ms
        client._observers.close()

    asyncio.run(scenario())
