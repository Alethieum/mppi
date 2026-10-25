"""Agent recovery against independent wire and retained-operation evidence.

No provider implementation is imported. Counts remain provider assertions;
these cases check price authority, cursor cutover and economic idempotence.
"""

import asyncio
import threading
from collections import OrderedDict, deque
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import grpc
import pytest
from mppi.v1 import mppi_pb2 as pb
from mppi.v1 import mppi_pb2_grpc as services
from mppi_agent import Client, ClientError, PaymentError
from mppi_agent.checkpoint import Checkpoint
from mppi_agent.client import _Output
from mppi_agent.state import Ledger
from mppi_protocol import CatalogRow, Rates, ValidationError, publish_catalog
from mppi_protocol._observers import Observers
from mppi_protocol.chain import Block, ChainSession
from mppi_protocol.contract import ContractCalls, OperationKind
from mppi_protocol.discovery import ProviderIdentity
from mppi_protocol.signing import LocalSessionSigner, verify_reconnect_auth
from mppi_protocol.types import (
    ChainObservation,
    Domain,
    OperationResult,
    OperationState,
    PauseLevel,
    Readiness,
    RunContext,
    SessionDescriptor,
    SessionStatus,
)
from mppi_protocol.wire import GRPC_OPTIONS, ByteQueue, descriptor_to_wire

DOMAIN = Domain(5042002, b"p" * 20)
SIGNER = LocalSessionSigner(bytes.fromhex("11" * 32))  # Public unfunded fixture.
MODEL = "test/model"
CATALOG = publish_catalog((CatalogRow(MODEL, Rates(500_000, 500_000)),))
DESCRIPTOR = SessionDescriptor(
    b"a" * 20, SIGNER.address, 7, b"r" * 20, b"o" * 20, bytes.fromhex("36" + "00" * 19), b"s" * 32
)
NONCE = b"n" * 32


def ledger_with_runs(count=1):
    ledger = Ledger(DOMAIN, DESCRIPTOR, CATALOG)
    ledger.generation, ledger.ready = 1, True
    for number in range(count):
        context = RunContext(
            ledger.session_id, number.to_bytes(32, "big"), 1, MODEL, CATALOG.rate_card_hash
        )
        ledger.register(context, SIGNER.sign_run(DOMAIN, context))
    ledger.accept_voucher(
        pb.Voucher(
            session_id=ledger.session_id,
            cumulative=100,
            signature=SIGNER.sign_voucher(DOMAIN, ledger.session_id, 100),
        ),
        100,
    )
    return ledger


def component_client():
    client = object.__new__(Client)
    client._observers = Observers(None)
    client._ledger = ledger_with_runs()
    state = ChainSession(100, 0, SessionStatus.OPEN, 0, 100, CATALOG.rate_card_hash)
    current = SimpleNamespace(sessions={client._ledger.session_id: state})
    client.chain = SimpleNamespace(
        current=current, snapshot=lambda: current, readiness=lambda _=None: Readiness(True)
    )
    client.signer, client.timeout = SIGNER, 1
    client._persist = AsyncMock()
    client._control_failure = None
    client._attached_generation = 1
    client._outbound = ByteQueue()
    client._voucher_lock = asyncio.Lock()
    client._background, client._pending_vouchers = set(), {}
    client._voucher_task = client._renew_task = None
    client._queue_options = {"max_messages": 4, "max_bytes": 16384, "wait_ms": 50}
    client._nonce = client._snapshot_waiter = client._close_waiter = None
    client._snapshot_applied = None
    client._operation_locks = {}
    client._challenges, client._outputs = {}, {}
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


def meter(ledger, *, number=0, seq=1, inputs=0):
    return pb.Meter(
        session_id=ledger.session_id,
        run_id=number.to_bytes(32, "big"),
        seq=seq,
        input_tokens=inputs,
        cache_tokens=0,
        output_tokens=0,
    )


def final(ledger, *, number=0, seq=1, inputs=0):
    return pb.Terminal(
        session_id=ledger.session_id,
        run_id=number.to_bytes(32, "big"),
        model=MODEL,
        rate_card_hash=CATALOG.rate_card_hash,
        input_tokens=inputs,
        cache_tokens=0,
        output_tokens=0,
        amount=inputs // 2,
        status=pb.RUN_STATUS_COMPLETED,
        final_meter_seq=seq,
    )


def snapshot(ledger, *, seq=1, inputs=4):
    runs = list(ledger.runs.values())
    return pb.SessionState(
        session_id=ledger.session_id,
        nonce=NONCE,
        control_generation=2,
        ready=True,
        close_pending=False,
        descriptor=descriptor_to_wire(DESCRIPTOR),
        accepted_voucher=ledger.voucher,
        meter_seq=seq,
        usage_by_price=[
            pb.PricedUsage(
                model=MODEL,
                rate_card_hash=CATALOG.rate_card_hash,
                input_tokens=inputs,
                cache_tokens=0,
                output_tokens=0,
                acceptance_run_id=runs[0].context.run_id,
                acceptance_control_generation=1,
                acceptance_signature=runs[0].signature,
            )
        ],
        run_states=[
            pb.RunState(
                run_id=run.context.run_id,
                model=MODEL,
                rate_card_hash=CATALOG.rate_card_hash,
                control_generation=1,
                auth_signature=run.signature,
                status=pb.RUN_STATUS_CANCELLED,
                input_tokens=inputs,
                cache_tokens=0,
                output_tokens=0,
            )
            for run in runs
        ],
        catalogs=[
            pb.CatalogVersion(
                rate_card_hash=CATALOG.rate_card_hash, catalog=CATALOG.canonical_bytes
            )
        ],
        initial_rate_card_hash=CATALOG.rate_card_hash,
        current_rate_card_hash=CATALOG.rate_card_hash,
        deposit=100,
        settled_cumulative=0,
        status=pb.SESSION_STATUS_OPEN,
        chain_block_number=1,
        chain_block_hash=b"b" * 32,
    )


def attachment_client(*, catalog=CATALOG):
    client = component_client()
    client._ledger = ledger_with_runs()
    client._ledger.ready = False
    client._attached_generation = 0
    client._loop = asyncio.get_running_loop()
    client.agent_id = None
    client._nonce = client._loop.create_future()
    client._nonce.set_result(NONCE)
    block = Block(
        ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 1, b"b" * 32), 100, b"c" * 32
    )
    identity = ProviderIdentity(
        block.observation,
        b"g" * 20,
        7,
        DESCRIPTOR.operator,
        DESCRIPTOR.recipient,
        "https://provider.test/discover",
        catalog,
    )
    state = ChainSession(100, 0, SessionStatus.OPEN, 0, 100, CATALOG.rate_card_hash)
    current = SimpleNamespace(
        block=block,
        identities={7: identity},
        sessions={client._ledger.session_id: state},
        pause_level=PauseLevel.NONE,
    )
    client.chain = SimpleNamespace(
        current=current,
        snapshot=lambda: current,
        refresh=AsyncMock(return_value=current),
        readiness=lambda _=None: Readiness(True),
        reader=SimpleNamespace(block=Mock(return_value=block), session=Mock(return_value=state)),
    )
    client._identity = identity
    return client


def deliver_snapshot(client, message):
    async def send(control):
        if control.WhichOneof("kind") == "reconnect_request":
            request = control.reconnect_request
            verify_reconnect_auth(DOMAIN, DESCRIPTOR, request.nonce, request.signature)
            assert tuple(request.pending_run_ids) == client._ledger.pending
            client._snapshot_waiter.set_result(message)

    client._send = AsyncMock(side_effect=send)


def test_recovered_finals_keep_unknown_individual_cursors_and_charge_once():
    ledger = ledger_with_runs(2)
    message = snapshot(ledger, seq=2, inputs=4)
    message.usage_by_price[0].input_tokens = 8
    ledger.restore_snapshot(message, nonce=NONCE, deposit=100, settled=0)
    assert ledger.usage_amount == 4
    assert all(run.recovered_through == 2 for run in ledger.runs.values())
    for number, seq in ((0, 1), (1, 2)):
        report = final(ledger, number=number, seq=seq, inputs=4)
        report.status = pb.RUN_STATUS_CANCELLED
        assert not ledger.apply_final(report, 100)
    assert ledger.usage_amount == 4
    assert all(run.recovered_through is None for run in ledger.runs.values())
    assert not ledger.apply_meter(meter(ledger, seq=1, inputs=4), 100)


@pytest.mark.parametrize("tamper", ["counts", "status", "after_cutover"])
def test_recovery_does_not_weaken_definitive_final_validation(tamper):
    ledger = ledger_with_runs()
    ledger.restore_snapshot(snapshot(ledger, seq=3), nonce=NONCE, deposit=100, settled=0)
    report = final(ledger, seq=2, inputs=4)
    report.status = pb.RUN_STATUS_CANCELLED
    if tamper == "counts":
        report.input_tokens, report.amount = 6, 3
    elif tamper == "status":
        report.status = pb.RUN_STATUS_COMPLETED
    else:
        report.final_meter_seq = 4
    before = deepcopy(ledger.__dict__)
    with pytest.raises(ValidationError):
        ledger.apply_final(report, 100)
    assert ledger.__dict__ == before


def test_recovered_cursor_and_explicit_price_acceptance_survive_checkpoint(tmp_path: Path):
    async def scenario():
        client = component_client()
        ledger = ledger_with_runs()
        ledger.restore_snapshot(snapshot(ledger, seq=3), nonce=NONCE, deposit=100, settled=0)
        changed = publish_catalog((CatalogRow(MODEL, Rates(1, 2)),))
        ledger.add_catalog(changed)
        client._ledger = ledger
        client._saved, client._operations, client._opening_ref = {"version": 1}, {}, None
        path = tmp_path / "checkpoint.json"
        checkpoint = Checkpoint(path)
        checkpoint.save(client._record())
        client._saved = checkpoint.load()
        checkpoint.close()
        client.wallet = SimpleNamespace(address=DESCRIPTOR.agent)
        client.contract = ContractCalls(DOMAIN, "USDC", "2")
        client._restore_local(7)
        restored = client._ledger
        assert restored.accepted_catalogs == {CATALOG.rate_card_hash}
        assert changed.rate_card_hash in restored.catalogs
        assert restored.runs[bytes(32)].recovered_through == 3
        report = final(restored, seq=1, inputs=4)
        report.status = pb.RUN_STATUS_CANCELLED
        assert not restored.apply_final(report, 100)
        assert restored.usage_amount == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("tamper", ["current_price", "missing_pending", "late_chain_change"])
def test_attachment_failure_does_not_commit_partial_snapshot(tamper):
    async def scenario():
        client = attachment_client()
        original = client._ledger
        message = snapshot(original)
        if tamper == "current_price":
            changed = publish_catalog((CatalogRow(MODEL, Rates(1, 2)),))
            original.add_catalog(changed)
            message.catalogs.add(
                rate_card_hash=changed.rate_card_hash, catalog=changed.canonical_bytes
            )
            message.current_rate_card_hash = changed.rate_card_hash
        elif tamper == "missing_pending":
            del message.run_states[:]
        else:
            block = client.chain.reader.block.return_value
            client.chain.reader.block.side_effect = [block, replace(block, timestamp=101)]
        before = deepcopy(original.__dict__)
        deliver_snapshot(client, message)
        with pytest.raises((ClientError, ValidationError)):
            await client._attach(b"t" * 32)
        assert client._ledger is original and original.__dict__ == before
        assert client._attached_generation == 0
        client._persist.assert_not_awaited()
        assert client._snapshot_applied.is_set()

    asyncio.run(scenario())


@pytest.mark.parametrize("change", ("capture", "top_up", "pause"))
def test_attachment_refreshes_chain_evidence_newer_than_cached_follower(change):
    async def scenario():
        client = attachment_client()
        original = client._ledger
        old = client.chain.current
        message = snapshot(original)
        newer_block = replace(
            old.block,
            observation=replace(old.block.observation, block_number=2, block_hash=b"d" * 32),
            parent_hash=old.block.observation.block_hash,
        )
        state = replace(old.sessions[original.session_id], settled_cumulative=2)
        if change == "top_up":
            state = replace(state, deposit=110)
        message.chain_block_number = 2
        message.chain_block_hash = newer_block.observation.block_hash
        message.deposit = state.deposit
        message.settled_cumulative = state.settled_cumulative
        refreshed = SimpleNamespace(
            block=newer_block,
            identities={7: replace(old.identities[7], observation=newer_block.observation)},
            sessions={original.session_id: state},
            pause_level=PauseLevel.PAYMENTS if change == "pause" else PauseLevel.NONE,
        )

        async def refresh():
            client.chain.current = refreshed
            return refreshed

        client.chain.snapshot = lambda: client.chain.current
        client.chain.refresh = AsyncMock(side_effect=refresh)
        client.chain.readiness = lambda _=None: Readiness(
            client.chain.current.pause_level is PauseLevel.NONE
        )
        client.chain.reader.block.return_value = newer_block
        client.chain.reader.session.return_value = state
        deliver_snapshot(client, message)
        await client._attach(b"t" * 32, require_ready=False)
        client.chain.refresh.assert_awaited_once()
        assert client._ledger is not original
        assert client._ledger.generation == 2 and client._ledger.usage_amount == 2
        assert client._ledger.ready is (change != "pause")
        assert client._attached_generation == 2
        client._persist.assert_awaited_once()

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ("behind", "conflict", "unavailable", "reorg"))
def test_attachment_rejects_unusable_refreshed_chain_evidence_atomically(failure):
    async def scenario():
        client = attachment_client()
        original = client._ledger
        before = deepcopy(original.__dict__)
        old = client.chain.current
        message = snapshot(original)
        newer_block = replace(
            old.block,
            observation=replace(old.block.observation, block_number=2, block_hash=b"d" * 32),
            parent_hash=old.block.observation.block_hash,
        )
        message.chain_block_number = 2
        message.chain_block_hash = newer_block.observation.block_hash
        client.chain.reader.block.return_value = newer_block
        refreshed = SimpleNamespace(**vars(old))
        refreshed.block = newer_block
        if failure == "behind":
            refreshed.block = old.block
        elif failure == "conflict":
            refreshed.block = replace(
                newer_block,
                observation=replace(newer_block.observation, block_hash=b"x" * 32),
            )
        elif failure == "reorg":
            client.chain.reader.block.side_effect = [
                newer_block,
                replace(newer_block, timestamp=newer_block.timestamp + 1),
            ]
        client.chain.refresh = AsyncMock(
            side_effect=ClientError("refresh unavailable") if failure == "unavailable" else None,
            return_value=refreshed,
        )
        deliver_snapshot(client, message)
        with pytest.raises(ClientError):
            await client._attach(b"t" * 32)
        assert client._ledger is original and original.__dict__ == before
        assert client._attached_generation == 0
        client._persist.assert_not_awaited()
        assert client._snapshot_applied.is_set()

    asyncio.run(scenario())


def test_reconnect_observes_changed_prices_without_accepting_them():
    async def scenario():
        changed = publish_catalog((CatalogRow(MODEL, Rates(1, 2)),))
        client = attachment_client(catalog=changed)
        message = snapshot(client._ledger)
        message.catalogs.add(rate_card_hash=changed.rate_card_hash, catalog=changed.canonical_bytes)
        message.current_rate_card_hash = changed.rate_card_hash
        deliver_snapshot(client, message)
        await client._attach(b"t" * 32)
        assert changed.rate_card_hash in client._ledger.catalogs
        assert changed.rate_card_hash not in client._ledger.accepted_catalogs
        client._open = AsyncMock(return_value=SimpleNamespace(session=True))
        with pytest.raises(PaymentError, match="PRICE_CHANGED"):
            await client._create(MODEL, True, b"{}")
        assert await client._accept_catalog(changed.rate_card_hash) == changed
        assert changed.rate_card_hash in client._ledger.accepted_catalogs

    asyncio.run(scenario())


def test_historical_recovery_without_current_offering_cannot_submit_new_run():
    async def scenario():
        client = attachment_client(catalog=None)
        message = snapshot(client._ledger)
        message.ClearField("current_rate_card_hash")
        deliver_snapshot(client, message)
        await client._attach(b"t" * 32)
        assert client._ledger.usage_amount == 2
        client._open = AsyncMock(return_value=SimpleNamespace(session=True))
        with pytest.raises(ClientError, match="current catalog"):
            await client._create(MODEL, True, b"{}")

    asyncio.run(scenario())


def test_native_control_waits_for_snapshot_commit_before_later_messages():
    async def scenario():
        client = attachment_client()
        message = snapshot(client._ledger)
        entered, release = threading.Event(), threading.Event()
        original_block = client.chain.reader.block.return_value

        def read_block(_):
            entered.set()
            assert release.wait(3)
            return original_block

        client.chain.reader.block.side_effect = read_block
        finished, hold = asyncio.Event(), asyncio.Event()

        class Peer(services.ControlServicer):
            async def Session(self, requests, context):
                opening, reconnect = await anext(requests), await anext(requests)
                assert opening.WhichOneof("kind") == "session_open"
                verify_reconnect_auth(
                    DOMAIN, DESCRIPTOR, NONCE, reconnect.reconnect_request.signature
                )
                yield pb.ControlMessage(session_state=message)
                # Delayed meter at the snapshot cutover must be discarded only
                # after successful recovery. Before it, this would exceed A.
                yield pb.ControlMessage(meter=meter(client._ledger, inputs=999))
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
        attach = asyncio.create_task(client._attach(b"t" * 32))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            assert client._ledger.meter_seq == 0 and client._ledger.generation == 1
            assert client._control_failure is None
            release.set()
            await asyncio.wait_for(attach, 3)
            await asyncio.wait_for(finished.wait(), 2)
            await asyncio.sleep(0.02)
            assert client._ledger.meter_seq == 1 and client._ledger.generation == 2
            assert client._ledger.usage_amount == 2 and client._control_failure is None
        finally:
            release.set()
            client._control.cancel()
            for task in tasks + [attach]:
                task.cancel()
            await asyncio.gather(*tasks, attach, return_exceptions=True)
            await channel.close()
            await server.stop(0)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "kind", [OperationKind.REQUEST_CLOSE, OperationKind.WITHDRAW, OperationKind.RELEASE_IDLE]
)
def test_repeated_exit_reconciles_original_even_after_closed_or_paused(kind):
    async def scenario():
        client = component_client()
        client._local_start = AsyncMock()
        client._open_lock = asyncio.Lock()
        identifier = client._ledger.session_id
        ref = kind.value + "-" + identifier.hex()
        result = OperationResult(ref, OperationState.UNKNOWN, "wallet-id", b"t" * 32)
        client._operations = {ref: (None, result)}
        client._reconcile_ref = AsyncMock(return_value=result)
        client.chain.snapshot = Mock(
            side_effect=AssertionError("eligibility cannot replace reconciliation")
        )
        assert await client._exit(identifier, kind) == result
        client._reconcile_ref.assert_awaited_once_with(ref)

    asyncio.run(scenario())


@pytest.mark.parametrize("reply", ["missing", "timeout", "wrong_ref", "wrong_tx", "wrong_wallet"])
def test_reconciliation_retains_original_identifiers_and_rejects_substitution(reply):
    async def scenario():
        client = component_client()
        client._local_start = AsyncMock()
        client.timeout = 0.01
        original = OperationResult("retained", OperationState.UNKNOWN, "wallet-id", b"t" * 32)
        operation = ContractCalls(DOMAIN, "USDC", "2").request_close(DESCRIPTOR)
        client._operations = {"retained": (operation, original)}
        result = OperationResult(
            "wrong" if reply == "wrong_ref" else "retained",
            OperationState.UNKNOWN,
            "different" if reply == "wrong_wallet" else None,
            b"z" * 32 if reply == "wrong_tx" else None,
        )
        client.chain.reader = SimpleNamespace(reconcile=Mock(return_value=result))
        if reply == "timeout":
            client.chain.reader.reconcile.side_effect = TimeoutError
        if reply.startswith("wrong"):
            with pytest.raises(ClientError, match="identity"):
                await client._reconcile_ref("retained")
            client._persist.assert_not_awaited()
        else:
            assert await client._reconcile_ref("retained") == original
        assert client._operations["retained"] == (operation, original)

    asyncio.run(scenario())


def test_explicit_recover_never_opens_funds_or_replays_and_preserves_close_intent():
    async def scenario():
        client = component_client()
        identifier = client._ledger.session_id
        client._ledger.close_pending = True
        client._local_start, client._open_lock = AsyncMock(), asyncio.Lock()
        client._ensure_close_attachment = AsyncMock()
        view = object()
        client._get = AsyncMock(return_value=view)
        client._open = AsyncMock(side_effect=AssertionError("recovery cannot open"))
        client._submit = AsyncMock(side_effect=AssertionError("recovery cannot fund"))
        client._create = AsyncMock(side_effect=AssertionError("recovery cannot infer"))
        assert await client._recover(identifier) is view
        assert client._ledger.close_pending
        client._ensure_close_attachment.assert_awaited_once()

    asyncio.run(scenario())


def opening_intent_client():
    client = attachment_client()
    client._operations = {}
    client._opening_ref = "original-opening-reference"
    client._saved = {
        "version": 1,
        "funding_intent": {
            "model": MODEL,
            "tokens": 200,
            "amount": 100,
            "nonce": (b"u" * 32).hex(),
            "valid_after": 99,
            "valid_before": 200,
        },
    }
    client.wallet = SimpleNamespace(
        address=DESCRIPTOR.agent, sign_funding_authorization=Mock(return_value=b"w" * 65)
    )
    client.contract = ContractCalls(DOMAIN, "USDC", "2")
    client.chain.reader.session.return_value = ChainSession(
        0, 0, SessionStatus.OPEN, 0, 0, bytes(32)
    )
    client._submit = AsyncMock(
        return_value=OperationResult(client._opening_ref, OperationState.UNKNOWN)
    )
    return client


def test_never_submitted_opening_reuses_exact_authorization_and_reference():
    async def scenario():
        client = opening_intent_client()
        original = deepcopy(client._saved)
        assert (await client._submit_open_intent()).state is OperationState.UNKNOWN
        auth = client.wallet.sign_funding_authorization.call_args.args[0]
        assert (auth.value, auth.nonce, auth.valid_after, auth.valid_before) == (
            100,
            b"u" * 32,
            99,
            200,
        )
        operation, ref = client._submit.call_args.args
        assert ref == "original-opening-reference"
        assert operation.descriptors == (DESCRIPTOR,) and operation.amounts == (100,)
        assert operation.expected_rate_card_hash == CATALOG.rate_card_hash
        assert client._saved == original

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "cause",
    [
        "journaled",
        "expired",
        "changed_price",
        "changed_endpoint",
        "exists",
        "bad_amount",
        "bad_signer",
    ],
)
def test_retained_opening_never_replaces_uncertain_expired_or_changed_intent(cause):
    async def scenario():
        client = opening_intent_client()
        if cause == "journaled":
            client._operations[client._opening_ref] = (
                None,
                OperationResult(client._opening_ref, OperationState.UNKNOWN),
            )
        elif cause == "expired":
            client._saved["funding_intent"]["valid_before"] = 100
        elif cause == "changed_price":
            old = client.chain.current.identities[7]
            client.chain.current.identities[7] = replace(
                old, catalog=publish_catalog((CatalogRow(MODEL, Rates(1, 2)),))
            )
        elif cause == "changed_endpoint":
            client.chain.current.identities[7] = replace(
                client._identity, agent_uri="https://changed.test/discover"
            )
        elif cause == "exists":
            client.chain.reader.session.return_value = ChainSession(
                100, 0, SessionStatus.OPEN, 0, 100, CATALOG.rate_card_hash
            )
        elif cause == "bad_amount":
            client._saved["funding_intent"]["amount"] = 99
        else:
            client.signer = SimpleNamespace(address=b"z" * 20)
        original = deepcopy(client._saved)
        with pytest.raises(ClientError):
            await client._submit_open_intent()
        client.wallet.sign_funding_authorization.assert_not_called()
        client._submit.assert_not_awaited()
        assert client._saved == original

    asyncio.run(scenario())


def test_opening_signer_failure_remains_the_same_never_submitted_intent():
    async def scenario():
        client = opening_intent_client()
        client.wallet.sign_funding_authorization.side_effect = [
            OSError("wallet unavailable"),
            b"w" * 65,
        ]
        with pytest.raises(OSError):
            await client._submit_open_intent()
        client._submit.assert_not_awaited()
        await client._submit_open_intent()
        first, second = client.wallet.sign_funding_authorization.call_args_list
        assert first == second
        client._submit.assert_awaited_once()

    asyncio.run(scenario())


@pytest.mark.parametrize("cause", ["missing_final", "cancelled", "bad_final"])
def test_native_partial_complete_keeps_unknown_accounting_without_replay(cause):
    async def scenario():
        client = component_client()
        identifier = bytes(32)
        release, emitted = asyncio.Event(), asyncio.Event()
        calls = []

        class Peer(services.ChatServicer):
            async def Completions(self, request, context):
                calls.append(request.run_id)
                yield pb.ChatChunk(payload=b'{"text":"partial"}')
                emitted.set()
                await release.wait()
                if cause == "bad_final":
                    report = final(client._ledger, inputs=4)
                    report.amount = 999
                    yield pb.ChatChunk(terminal=report)

        server = grpc.aio.server(options=GRPC_OPTIONS)
        services.add_ChatServicer_to_server(Peer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}", options=GRPC_OPTIONS)
        output = _Output(ByteQueue())
        client._outputs[identifier] = output
        output.call = services.ChatStub(channel).Completions(pb.CompleteRequest(run_id=identifier))
        output.task = asyncio.create_task(client._read_complete(identifier, True))
        try:
            await asyncio.wait_for(emitted.wait(), 2)
            assert await asyncio.wait_for(client._next(output), 2) == b'{"text":"partial"}'
            if cause == "cancelled":
                await client._cancel_run(identifier, output)
            else:
                release.set()
                await asyncio.wait_for(output.task, 2)
            with pytest.raises((ClientError, ValidationError)):
                await asyncio.wait_for(client._next(output), 2)
            assert calls == [identifier]
            assert client._ledger.pending == (identifier,)
            assert client._ledger.usage_amount == 0
            assert client._ledger.runs[identifier].failed_transport
            assert output.terminal is None
        finally:
            release.set()
            output.call.cancel()
            output.task.cancel()
            await asyncio.gather(output.task, return_exceptions=True)
            await channel.close()
            await server.stop(0)

    asyncio.run(scenario())


def test_snapshot_not_ready_until_durable_cutover_completes():
    async def scenario():
        client = attachment_client()
        deliver_snapshot(client, snapshot(client._ledger))
        writing, durable = asyncio.Event(), asyncio.Event()

        async def persist():
            writing.set()
            await durable.wait()

        client._persist = AsyncMock(side_effect=persist)
        client._local_start = AsyncMock()
        task = asyncio.create_task(client._attach(b"t" * 32))
        try:
            await asyncio.wait_for(writing.wait(), 2)
            assert client._ledger.ready and client._attached_generation == 0
            with pytest.raises(ClientError, match="not ready"):
                client._require_ready()
            view = await client._get(client._ledger.session_id)
            assert not view.readiness.ready
            durable.set()
            await asyncio.wait_for(task, 2)
            assert client._require_ready().generation == 2
        finally:
            durable.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_shutdown_reports_unresolved_close_intent_without_inventing_transaction():
    async def scenario():
        client = component_client()
        client._ledger.runs.clear()
        client._ledger.close_pending = True
        client._operations, client._control_tasks = {}, []
        client._channel = client._closed_result = None
        client._restored = True
        client.chain.stop = AsyncMock()
        result = await client._shutdown()
        assert result.unresolved_session_ids == (client._ledger.session_id,)
        assert result.unresolved_runs == () and result.pending_operations == ()
        assert client._ledger.close_pending

    asyncio.run(scenario())


def test_repeated_snapshot_preserves_not_admitted_cutover():
    ledger = ledger_with_runs(2)
    message = snapshot(ledger, seq=1)
    run = ledger.runs[bytes(32)]
    message.run_states[0].CopyFrom(
        pb.RunState(
            run_id=bytes(32),
            model="",
            status=pb.RUN_STATUS_NOT_ADMITTED,
            input_tokens=0,
            cache_tokens=0,
            output_tokens=0,
        )
    )
    second = ledger.runs[(1).to_bytes(32, "big")]
    message.usage_by_price[0].acceptance_run_id = second.context.run_id
    message.usage_by_price[0].acceptance_signature = second.signature
    ledger.restore_snapshot(message, nonce=NONCE, deposit=100, settled=0)
    prior = run.final
    message.control_generation = 3
    message.meter_seq = 2
    ledger.restore_snapshot(message, nonce=NONCE, deposit=100, settled=0)
    assert run.final == prior and run.recovered_through == 1
    assert ledger.usage_amount == 2


def test_open_reconnects_after_funding_outlives_control_setup(monkeypatch):
    """Real dead Control while the original fake custody operation confirms.

    Provider setup expiry: provider/runtime.py:494,518,563. Retained opening:
    agent/client.py:1088-1113. No real wallet, chain, TLS or inference execution.
    """
    from mppi_protocol.chain import OpeningEvidence

    async def scenario():
        client = opening_intent_client()
        client._ledger = Ledger(DOMAIN, DESCRIPTOR, CATALOG)
        client._open_lock = asyncio.Lock()
        client._start, client._local_start = AsyncMock(), AsyncMock()
        client._control_tasks, client._channel = [], None
        client.ca_file = None
        original_intent = deepcopy(client._saved)
        block = client.chain.current.block
        funded = client.chain.current.sessions[client._ledger.session_id]
        original_ref = client._opening_ref
        original_tx = b"t" * 32
        client.chain.watch_session = AsyncMock(return_value=funded)
        client.chain.reader.opening = Mock(
            return_value=OpeningEvidence(DESCRIPTOR, block, 100, CATALOG, original_tx)
        )
        finish = asyncio.Event()
        connections, notices, authentications = [], [], []
        observed_expiry = []

        class Peer(services.ControlServicer):
            async def Session(self, requests, context):
                connections.append(len(connections) + 1)
                connection = connections[-1]
                nonce = connection.to_bytes(32, "big")
                yield pb.ControlMessage(reconnect_challenge=pb.ReconnectChallenge(nonce=nonce))
                if connection == 1:
                    # Exercise an actual terminal native RPC. Fake custody only
                    # confirms after the installed client has observed this end.
                    await asyncio.sleep(0.05)
                    await context.abort(
                        grpc.StatusCode.DEADLINE_EXCEEDED,
                        "Control authentication deadline exceeded",
                    )
                    return
                assert connection == 2
                notice = (await anext(requests)).session_open
                assert (notice.session_id, notice.open_tx) == (
                    client._ledger.session_id,
                    original_tx,
                )
                notices.append(notice)
                proof = (await anext(requests)).reconnect_request
                assert proof.nonce == nonce
                assert proof.last_meter_seq == 0 and not proof.pending_run_ids
                assert not proof.HasField("latest_voucher")
                verify_reconnect_auth(DOMAIN, DESCRIPTOR, nonce, proof.signature)
                authentications.append(proof)
                yield pb.ControlMessage(
                    session_state=pb.SessionState(
                        session_id=client._ledger.session_id,
                        nonce=nonce,
                        control_generation=1,
                        ready=True,
                        close_pending=False,
                        descriptor=descriptor_to_wire(DESCRIPTOR),
                        meter_seq=0,
                        catalogs=[
                            pb.CatalogVersion(
                                rate_card_hash=CATALOG.rate_card_hash,
                                catalog=CATALOG.canonical_bytes,
                            )
                        ],
                        initial_rate_card_hash=CATALOG.rate_card_hash,
                        current_rate_card_hash=CATALOG.rate_card_hash,
                        deposit=100,
                        settled_cumulative=0,
                        status=pb.SESSION_STATUS_OPEN,
                        chain_block_number=block.observation.block_number,
                        chain_block_hash=block.observation.block_hash,
                    )
                )
                await finish.wait()

        async def submitted(operation, reference):
            assert reference == original_ref
            assert operation.kind is OperationKind.OPEN
            assert operation.descriptors == (DESCRIPTOR,) and operation.amounts == (100,)
            client._operations[reference] = (
                operation,
                OperationResult(reference, OperationState.UNKNOWN, "fixture-wallet", original_tx),
            )
            async with asyncio.timeout(2):
                while client._control_failure is None:
                    await asyncio.sleep(0.001)
            failure = client._control_failure
            assert isinstance(failure, grpc.aio.AioRpcError)
            assert failure.code() is grpc.StatusCode.DEADLINE_EXCEEDED
            observed_expiry.append(failure.code())
            client.chain.reader.session.return_value = funded
            confirmed = OperationResult(
                reference,
                OperationState.CONFIRMED,
                "fixture-wallet",
                original_tx,
                block.observation,
            )
            client._operations[reference] = (operation, confirmed)
            return confirmed

        client._submit = AsyncMock(side_effect=submitted)
        server = grpc.aio.server(options=GRPC_OPTIONS)
        services.add_ControlServicer_to_server(Peer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        client._discovery = SimpleNamespace(origin=f"https://127.0.0.1:{port}")
        # Only loopback transport credentials are substituted. _connect,
        # _control_read, _control_write, _open and _attach are real SDK methods.
        monkeypatch.setattr(
            "mppi_agent.client.grpc.aio.secure_channel",
            lambda target, credentials, *, options: grpc.aio.insecure_channel(
                target, options=options
            ),
        )
        try:
            opened = await asyncio.wait_for(client._open(MODEL, None), 3)
            assert opened.operation.state is OperationState.CONFIRMED
            assert opened.operation.operation_ref == original_ref
            assert opened.operation.tx_hash == original_tx
            assert opened.session is not None and opened.session.readiness.ready
            assert opened.session.descriptor == DESCRIPTOR
            assert opened.session.deposit == 100
            assert connections == [1, 2]
            assert len(notices) == len(authentications) == 1
        finally:
            finish.set()
            if client._control is not None:
                client._control.cancel()
            tasks = client._control_tasks + list(client._background)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if client._channel is not None:
                await client._channel.close()
            await server.stop(0)
            assert observed_expiry == [grpc.StatusCode.DEADLINE_EXCEEDED]
            client._submit.assert_awaited_once()
            client.wallet.sign_funding_authorization.assert_called_once()
            assert client._saved == original_intent
            assert client._opening_ref == original_ref
            assert len(client._operations) == 1
            assert client._operations[original_ref][1].state is OperationState.CONFIRMED
            assert not client._outputs and not client._ledger.runs

    asyncio.run(scenario())


def test_unsent_run_is_not_admitted_when_control_is_lost_before_submission():
    """MPPI-38/41: a run registered but never sent must not stay pending."""

    async def scenario():
        client = component_client()
        ledger = client._ledger
        ledger.runs.clear()
        current = SimpleNamespace(
            sessions=client.chain.snapshot().sessions,
            identities={DESCRIPTOR.provider_id: SimpleNamespace(catalog=CATALOG)},
        )
        client.chain.snapshot = lambda: current
        client._open = AsyncMock(return_value=SimpleNamespace(session=object()))

        async def persist():
            if not client._persist.await_count - 1:  # Lost while registering.
                client._control_failure = ClientError("fixture Control loss")

        client._persist = AsyncMock(side_effect=persist)
        payload = b'{"model":"test/model","messages":[],"stream":true}'
        with pytest.raises(ClientError, match="not ready"):
            await client._create(MODEL, True, payload)
        (run,) = ledger.runs.values()
        assert run.final == (0, 0, 0, pb.RUN_STATUS_NOT_ADMITTED, 0, 0)
        assert ledger.pending == () and client._persist.await_count == 2
        assert not client._outputs and not client._unsent

    asyncio.run(scenario())


def test_attachment_records_a_registered_but_unsent_run_not_admitted():
    async def scenario():
        client = attachment_client()
        ledger = client._ledger
        message = snapshot(ledger)
        context = RunContext(ledger.session_id, b"u" * 32, 1, MODEL, CATALOG.rate_card_hash)
        ledger.register(context, SIGNER.sign_run(DOMAIN, context))
        client._unsent = {b"u" * 32}
        deliver_snapshot(client, message)
        await client._attach(b"t" * 32, require_ready=False)
        request = client._send.await_args_list[-1].args[0].reconnect_request
        assert b"u" * 32 not in request.pending_run_ids
        assert client._ledger.runs[b"u" * 32].final[3] == pb.RUN_STATUS_NOT_ADMITTED
        assert client._ledger.generation == 2 and not client._unsent

    asyncio.run(scenario())


def test_attachment_waits_briefly_for_a_lagging_rpc_to_reach_the_snapshot_block():
    """A load-balanced RPC may first serve a head below the provider's snapshot."""

    async def scenario():
        client = attachment_client()
        old = client.chain.current
        message = snapshot(client._ledger)
        newer_block = replace(
            old.block,
            observation=replace(old.block.observation, block_number=2, block_hash=b"d" * 32),
            parent_hash=old.block.observation.block_hash,
        )
        message.chain_block_number = 2
        message.chain_block_hash = newer_block.observation.block_hash
        client.chain.reader.block.return_value = newer_block
        behind, caught_up = SimpleNamespace(**vars(old)), SimpleNamespace(**vars(old))
        caught_up.block = newer_block
        client.chain.refresh = AsyncMock(side_effect=[behind, caught_up])
        deliver_snapshot(client, message)
        await client._attach(b"t" * 32, require_ready=False)
        assert client._attached_generation == 2 and client.chain.refresh.await_count == 2

    asyncio.run(scenario())


def test_a_pending_top_up_notice_follows_the_reconnect_request():
    async def scenario():
        client = attachment_client()
        client._top_up_hint = b"t" * 32
        deliver_snapshot(client, snapshot(client._ledger))
        await client._attach(b"t" * 32, require_ready=False)
        kinds = [call.args[0].WhichOneof("kind") for call in client._send.await_args_list]
        assert kinds == ["session_open", "reconnect_request", "top_up_notice"]
        assert client._top_up_hint is None

    asyncio.run(scenario())
