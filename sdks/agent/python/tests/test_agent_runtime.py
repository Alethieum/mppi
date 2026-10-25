"""Agent Control independently exercised against a native gRPC wire peer.

These focused tests install a known signed local ledger to isolate renewal and
loss behavior. Full discovery/funding/attachment is covered by the separately
installed two-role acceptance test; these are not chain or wallet integration.
"""

import asyncio
import copy
import json
import pickle
import threading
import time
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

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
    Hooks,
    PaymentError,
)
from mppi_agent import client as client_module
from mppi_agent.checkpoint import Checkpoint
from mppi_agent.client import _Output
from mppi_agent.state import Ledger
from mppi_protocol import follower as follower_module
from mppi_protocol._observers import Observers
from mppi_protocol.amounts import Rates, TokenCounts
from mppi_protocol.catalog import CatalogRow, publish_catalog
from mppi_protocol.chain import (
    Block,
    ChainContradictionError,
    ChainSession,
    ClosingEvidence,
    FundingEvidence,
    OpeningEvidence,
)
from mppi_protocol.contract import ContractCalls, OperationKind
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
    """Give a component client its agent id and a funding check that passes."""
    client.agent_id = 43
    check = Mock(
        side_effect=lambda auth, signature, agent_id, block: FundingEvidence(
            block, True, DESCRIPTOR.agent
        )
    )
    client.chain.reader = SimpleNamespace(funding=check)
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
    )
    client.signer = SIGNER
    client.timeout = 1
    client._ledger = ledger
    client._persist = AsyncMock()
    client._control_failure = None
    client._attached_generation = 1
    client._outbound = ByteQueue()
    client._voucher_lock = asyncio.Lock()
    client._background = set()
    client._pending_vouchers = {}
    client._voucher_task = client._renew_task = None
    client._queue_options = {"max_messages": 4, "max_bytes": 16384, "wait_ms": 50}
    client._nonce = client._snapshot_waiter = client._close_waiter = None
    client._snapshot_applied = None
    client._operation_locks = {}
    client._challenges = {}
    client._outputs = {}
    client._control = None
    client._on_low_credit = None
    client._funding = client._low_credit_task = None
    client._on_funding_notice = client._top_up_hint = None
    client._unsent = set()
    client._resync = None
    client._receipt_queue, client._receipt_task = deque(), None
    client._receipts_seen, client._receipt_failures = set(), 0
    client._receipted, client._closed_result = set(), None
    client._funding_lock = asyncio.Lock()
    client._proven = OrderedDict()
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


def test_failed_receipt_proof_is_quarantined_without_failing_control():
    async def scenario():
        client = component_client()
        ledger = client._ledger
        errors = Errors()
        client._observers = Observers(Hooks(on_error=errors))
        client.chain.reader = SimpleNamespace(
            block=Mock(side_effect=TransportError("HTTP endpoint returned status 429"))
        )
        receipt = pb.Receipt(
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
            settled_cumulative=0,
            chain_observation=pb.ChainObservation(
                chain_id=DOMAIN.chain_id,
                contract_address=DOMAIN.verifying_contract,
                block_number=1,
                block_hash=b"b" * 32,
            ),
        )
        task = client._spawn(client._receipt(receipt))
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)
        assert client._control_failure is None and ledger.runs[RUN].final is None
        assert await asyncio.to_thread(errors.seen.wait, 2)
        assert errors.records[0].detail == "receipt: HTTP endpoint returned status 429"
        client._observers.close()

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


def test_receipt_flood_is_checked_without_chain_reads_and_proven_once_per_run():
    async def scenario():
        client = component_client()
        ledger = client._ledger
        accept_voucher(ledger, 4)
        client.chain.reader = reader = CountingReader()
        # Receipts for runs this agent never signed cost no read at all.
        reasons = [
            await offer(client, fixture_receipt(ledger, n.to_bytes(32, "big"))) for n in range(200)
        ]
        assert set(reasons) == {"report has no retained signed run"} and reader.reads == 0
        # Fifty copies of one run's Receipt are proven once.
        for _ in range(50):
            assert await offer(client, fixture_receipt(ledger, RUN)) is None
        await client._receipt_task
        assert reader.reads == 3 and ledger.runs[RUN].final is not None
        assert client._control_failure is None and client._resync is None

    asyncio.run(scenario())


def test_failing_receipt_proofs_are_bounded_and_never_read_concurrently():
    async def scenario():
        client = component_client()
        ledger = client._ledger
        client._queue_options["max_messages"] = 64
        accept_voucher(ledger, 4)
        client.chain.reader = reader = CountingReader(
            TransportError("HTTP endpoint returned status 429", transient=True)
        )
        runs = []
        for number in range(40):
            identifier = bytes([number + 1]) * 32
            context = RunContext(
                ledger.session_id, identifier, 1, "test/model", CATALOG.rate_card_hash
            )
            ledger.register(context, SIGNER.sign_run(DOMAIN, context))
            runs.append(identifier)
        reasons = []
        for identifier in runs:
            reasons.append(await offer(client, fixture_receipt(ledger, identifier)))
            if client._receipt_task is not None:
                await client._receipt_task
        assert reasons[:16] == [None] * 16
        assert set(reasons[16:]) == {"receipt proofs are suspended on this attachment"}
        # Each transient failure is tried once more before it counts.
        assert reader.reads == 32 and reader.most == 1
        assert client._receipt_failures == 16 and client._control_failure is None
        # Queued proofs still run one at a time.
        client._receipt_failures, client._receipts_seen = 0, set()
        reader.failure = None
        for identifier in runs[:10]:
            assert await offer(client, fixture_receipt(ledger, identifier)) is None
        await client._receipt_task
        assert reader.most == 1

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


def test_receipts_sharing_a_proven_observation_need_no_chain_reads():
    """One proof per provider observation, not five reads per Receipt."""

    async def scenario():
        client = component_client()
        ledger = client._ledger
        client._queue_options["max_messages"] = 64
        accept_voucher(ledger, 4)
        client.chain.reader = reader = CountingReader()
        client._proven = OrderedDict()
        runs = []
        for number in range(5):
            identifier = bytes([number + 1]) * 32
            context = RunContext(
                ledger.session_id, identifier, 1, "test/model", CATALOG.rate_card_hash
            )
            ledger.register(context, SIGNER.sign_run(DOMAIN, context))
            runs.append(identifier)
        for identifier in runs[:4]:
            message = fixture_receipt(ledger, identifier)
            message.receipt.final_meter_seq = runs.index(identifier) + 1
            assert await offer(client, message) is None
        await client._receipt_task
        assert reader.reads == 3 and all(ledger.runs[run].final for run in runs[:4])
        # A different settled total at a proven observation is refused, still without reads.
        errors = Errors()
        client._observers = Observers(Hooks(on_error=errors))
        message = fixture_receipt(ledger, runs[4])
        message.receipt.final_meter_seq, message.receipt.settled_cumulative = 5, 1
        assert await offer(client, message) is None
        await client._receipt_task
        assert reader.reads == 3 and ledger.runs[runs[4]].final is None
        assert await asyncio.to_thread(errors.seen.wait, 2)
        assert errors.records[0].detail == (
            "receipt: receipt settlement total contradicts chain or accepted usage"
        )
        client._observers.close()

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


def test_a_transient_receipt_proof_failure_is_retried_and_can_be_offered_again():
    async def scenario():
        client = component_client()
        ledger = client._ledger
        accept_voucher(ledger, 4)

        class Flaky(CountingReader):
            def __init__(self, failures):
                super().__init__()
                self.left = failures

            def _read(self):
                super()._read()
                if self.left:
                    self.left -= 1
                    raise TransportError("HTTP endpoint returned status 503", transient=True)

        client.chain.reader = reader = Flaky(1)
        assert await offer(client, fixture_receipt(ledger, RUN)) is None
        await client._receipt_task
        assert ledger.runs[RUN].final is not None and client._receipt_failures == 0
        assert reader.reads == 4  # One failed read, then the full proof.
        # Two failures in a row give up for now, but a later copy is proven.
        context = RunContext(ledger.session_id, b"q" * 32, 1, "test/model", CATALOG.rate_card_hash)
        ledger.register(context, SIGNER.sign_run(DOMAIN, context))
        client.chain.reader = reader = Flaky(2)
        client._proven.clear()
        message = fixture_receipt(ledger, b"q" * 32)
        message.receipt.final_meter_seq = 2
        assert await offer(client, message) is None
        await client._receipt_task
        assert ledger.runs[b"q" * 32].final is None and client._receipt_failures == 1
        assert await offer(client, message) is None
        await client._receipt_task
        assert ledger.runs[b"q" * 32].final is not None

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
    """A component client that can top up, with a wallet that records its signing."""
    client = component_client()
    client._funding_lock = asyncio.Lock()
    client._local_start = AsyncMock()
    client._operations, client._saved = {}, {}
    client._control = None
    client.contract = ContractCalls(DOMAIN, "USDC", "2")
    block = SimpleNamespace(timestamp=timestamp)
    client.chain.current = current = SimpleNamespace(
        sessions=client.chain.snapshot().sessions, block=block
    )
    client.chain.snapshot = lambda: current
    client.chain.refresh = AsyncMock()
    sign = Mock(return_value=b"\x01" * 65)
    client.wallet = SimpleNamespace(address=DESCRIPTOR.agent, sign_funding_authorization=sign)
    fundable(client)
    return client, current, sign


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


def test_a_fresh_opening_records_its_proven_session_for_requests():
    client = component_client()
    ledger = client._ledger
    block = SimpleNamespace(timestamp=1_799_999_990)  # The evidence read before submission.
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
    ((sid, state), _) = client.chain.confirm.call_args
    assert sid == ledger.session_id
    assert (state.deposit, state.settled_cumulative, state.status) == (100, 0, SessionStatus.OPEN)
    # An activity time no later than the opening's block: never a late deadline.
    assert state.last_activity == 1_799_999_990
    assert state.initial_rate_card_hash == ledger.initial_hash


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
