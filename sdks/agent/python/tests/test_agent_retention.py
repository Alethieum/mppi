"""Settled runs are forgotten, so a long session's memory and checkpoint stay bounded."""

import asyncio
from collections import deque
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from mppi.v1 import mppi_pb2 as pb
from mppi_agent import Client
from mppi_agent import client as client_module
from mppi_agent import state as state_module
from mppi_agent.client import _Output
from mppi_agent.state import Ledger
from mppi_protocol import ValidationError
from mppi_protocol._observers import Observers
from mppi_protocol.amounts import Rates
from mppi_protocol.catalog import CatalogRow, publish_catalog
from mppi_protocol.chain import ChainSession
from mppi_protocol.local_state import Checkpoint
from mppi_protocol.signing import LocalSessionSigner
from mppi_protocol.types import Domain, Readiness, RunContext, SessionDescriptor, SessionStatus
from mppi_protocol.wire import ByteQueue, descriptor_to_wire

DOMAIN = Domain(5042002, b"p" * 20)
SIGNER = LocalSessionSigner(bytes.fromhex("11" * 32))  # Public unfunded fixture.
MODEL = "test/model"
CATALOG = publish_catalog((CatalogRow(MODEL, Rates(500_000, 500_000)),))
DESCRIPTOR = SessionDescriptor(
    b"a" * 20, SIGNER.address, 7, b"r" * 20, b"o" * 20, bytes.fromhex("36" + "00" * 19), b"s" * 32
)
DEPOSIT = 1_000_000


def retention_client(path: Path) -> Client:
    client = object.__new__(Client)
    client._observers = Observers(None)
    ledger = Ledger(DOMAIN, DESCRIPTOR, CATALOG)
    ledger.generation, ledger.ready = 1, True
    ledger.accept_voucher(
        pb.Voucher(
            session_id=ledger.session_id,
            cumulative=DEPOSIT,
            signature=SIGNER.sign_voucher(DOMAIN, ledger.session_id, DEPOSIT),
        ),
        DEPOSIT,
    )
    client._ledger = ledger
    state = ChainSession(DEPOSIT, 0, SessionStatus.OPEN, 0, 100, CATALOG.rate_card_hash)
    current = SimpleNamespace(sessions={ledger.session_id: state})
    client.chain = SimpleNamespace(
        current=current, snapshot=lambda: current, readiness=lambda _=None: Readiness(True)
    )
    client.contract = SimpleNamespace(domain=DOMAIN)
    client.wallet, client.signer = SimpleNamespace(address=DESCRIPTOR.agent), SIGNER
    client._checkpoint = Checkpoint(path / "payments.json")
    client._checkpoint_lock = asyncio.Lock()
    client._saved, client._opening_ref, client._operations = {"version": 1}, None, {}
    client._outputs, client._closed_result = {}, None
    client._receipted, client._receipts_seen = set(), set()
    client._receipt_queue, client._receipt_task, client._receipt_failures = deque(), None, 0
    client._queue_options = {"max_messages": 64, "max_bytes": 16384, "wait_ms": 50}
    client._resync = client._control_failure = None
    client._attached_generation = 1
    return client


def settle(client: Client, number: int, *, receipt: bool = True, signature=None) -> bytes:
    """One run through register, its final Meter and its Terminal."""
    ledger = client._ledger
    identifier = number.to_bytes(32, "big")
    context = RunContext(ledger.session_id, identifier, 1, MODEL, CATALOG.rate_card_hash)
    ledger.register(context, signature or SIGNER.sign_run(DOMAIN, context))
    seq = ledger.meter_seq + 1
    assert ledger.apply_meter(
        pb.Meter(
            session_id=ledger.session_id,
            run_id=identifier,
            seq=seq,
            input_tokens=2,
            cache_tokens=0,
            output_tokens=0,
        ),
        DEPOSIT,
    )
    assert ledger.apply_final(terminal(ledger, identifier, seq), DEPOSIT)
    if receipt:
        client._receipted.add(identifier)
    return identifier


def terminal(ledger: Ledger, identifier: bytes, seq: int) -> pb.Terminal:
    return pb.Terminal(
        session_id=ledger.session_id,
        run_id=identifier,
        model=MODEL,
        rate_card_hash=CATALOG.rate_card_hash,
        input_tokens=2,
        cache_tokens=0,
        output_tokens=0,
        amount=1,
        status=pb.RUN_STATUS_COMPLETED,
        final_meter_seq=seq,
    )


def test_twenty_thousand_settled_runs_keep_the_checkpoint_bounded(tmp_path, monkeypatch):
    # 20,000 ECDSA run proofs are not what this measures; one real proof is reused.
    monkeypatch.setattr(state_module, "verify_run_auth", lambda *_: None)
    client = retention_client(tmp_path)
    ledger = client._ledger
    first = RunContext(ledger.session_id, bytes(32), 1, MODEL, CATALOG.rate_card_hash)
    signature = SIGNER.sign_run(DOMAIN, first)
    sizes = []
    for number in range(1, 20_001):
        settle(client, number, signature=signature)
        if number % 1_000 == 0:
            asyncio.run(client._persist())
            sizes.append((tmp_path / "payments.json").stat().st_size)
    # One representative run per price bucket remains; usage stays exact.
    assert len(ledger.runs) == 1 and ledger.meter_seq == 20_000
    assert ledger.usage[(MODEL, CATALOG.rate_card_hash)].input_tokens == 40_000
    assert ledger.usage_amount == 20_000
    assert max(sizes) - min(sizes) < 64 and max(sizes) < 4_096, sizes
    client._checkpoint.close()


def test_a_forgotten_runs_late_duplicates_change_nothing_and_keep_control(tmp_path):
    async def scenario():
        client = retention_client(tmp_path)
        ledger = client._ledger
        settle(client, 1)
        identifier = settle(client, 2)
        await client._persist()
        assert identifier not in ledger.runs
        usage = ledger.usage_amount
        # A duplicate Meter at or below the cursor is ignored before any lookup.
        late = pb.Meter(
            session_id=ledger.session_id,
            run_id=identifier,
            seq=2,
            input_tokens=2,
            cache_tokens=0,
            output_tokens=0,
        )
        assert not ledger.apply_meter(late, DEPOSIT)
        # A late Receipt is rejected alone by Control's per-message handling; it
        # cannot reach a re-sync and costs no chain read.
        receipt = pb.ControlMessage(
            receipt=pb.Receipt(
                session_id=ledger.session_id,
                run_id=identifier,
                model=MODEL,
                rate_card_hash=CATALOG.rate_card_hash,
                input_tokens=2,
                cache_tokens=0,
                output_tokens=0,
                amount=1,
                status=pb.RUN_STATUS_COMPLETED,
                final_meter_seq=2,
                settled_cumulative=0,
                chain_observation=pb.ChainObservation(
                    chain_id=DOMAIN.chain_id,
                    contract_address=DOMAIN.verifying_contract,
                    block_number=1,
                    block_hash=b"b" * 32,
                ),
            )
        )
        with pytest.raises(ValidationError, match="no retained signed run"):
            await client._control_report("receipt", receipt)
        assert ledger.usage_amount == usage and client._resync is None
        assert client._control_failure is None
        client._checkpoint.close()

    asyncio.run(scenario())


def test_restoring_a_pruned_checkpoint_restores_exact_usage(tmp_path):
    async def scenario():
        client = retention_client(tmp_path)
        for number in range(1, 31):
            settle(client, number, receipt=number % 3 != 0)
        await client._persist()
        before = client._ledger
        client._checkpoint.close()
        restored = retention_client(tmp_path)
        restored._ledger = None
        restored._saved = restored._checkpoint.load()
        restored._restore_local(DESCRIPTOR.provider_id)
        after = restored._ledger
        assert len(after.runs) == len(before.runs) == 11  # Representative + 10 waiting.
        assert after.usage == before.usage and after.usage_amount == before.usage_amount == 30
        assert after.meter_seq == before.meter_seq == 30
        assert after.voucher == before.voucher
        restored._checkpoint.close()

    asyncio.run(scenario())


def test_receipt_wait_is_bounded_and_unsettled_runs_are_kept(tmp_path, monkeypatch):
    async def scenario():
        monkeypatch.setattr(client_module, "_UNRECEIPTED_FINALS", 8)
        client = retention_client(tmp_path)
        ledger = client._ledger
        for number in range(1, 51):
            settle(client, number, receipt=False)
        # A Terminal that overtook its final Meter, and a run still streaming.
        context = RunContext(ledger.session_id, b"t" * 32, 1, MODEL, CATALOG.rate_card_hash)
        ledger.register(context, SIGNER.sign_run(DOMAIN, context))
        assert ledger.apply_final(terminal(ledger, b"t" * 32, ledger.meter_seq + 1), DEPOSIT)
        client._receipted.add(b"t" * 32)
        context = RunContext(ledger.session_id, b"l" * 32, 1, MODEL, CATALOG.rate_card_hash)
        ledger.register(context, SIGNER.sign_run(DOMAIN, context))
        live = _Output(ByteQueue())
        live.task = asyncio.create_task(asyncio.Event().wait())
        client._outputs[b"l" * 32] = live
        await client._persist()
        assert len(ledger.runs) == 1 + 8 + 2
        assert b"t" * 32 in ledger.runs and b"l" * 32 in ledger.runs
        assert (42).to_bytes(32, "big") not in ledger.runs  # The oldest waiting ones go.
        assert (43).to_bytes(32, "big") in ledger.runs
        live.task.cancel()
        await asyncio.gather(live.task, return_exceptions=True)
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_forgotten_runs_stream_still_reads_its_answer_and_receipt(tmp_path):
    async def scenario():
        client = retention_client(tmp_path)
        settle(client, 1)
        identifier = (2).to_bytes(32, "big")
        output = _Output(ByteQueue())
        output.queue.put_nowait(b'{"answer":true}')
        output.queue.close()
        output.receipt = pb.Receipt(run_id=identifier)
        client._outputs[identifier] = output
        settle(client, 2)
        await client._persist()
        assert identifier not in client._outputs and identifier not in client._ledger.runs
        assert await client._next(output) == b'{"answer":true}'
        assert (await client._final_record(output, receipt=True)).run_id == identifier
        await client._cancel_run(identifier, output)  # Closing it later is harmless.
        client._checkpoint.close()

    asyncio.run(scenario())


def test_a_snapshot_cannot_undercount_a_forgotten_runs_usage(tmp_path):
    """Known usage plus each retained run's increase floors the snapshot totals."""
    client = retention_client(tmp_path)
    ledger = client._ledger
    forgotten = settle(client, 1)
    context = RunContext(ledger.session_id, b"r" * 32, 1, MODEL, CATALOG.rate_card_hash)
    signature = SIGNER.sign_run(DOMAIN, context)
    ledger.register(context, signature)
    assert ledger.apply_meter(
        pb.Meter(
            session_id=ledger.session_id,
            run_id=b"r" * 32,
            seq=2,
            input_tokens=2,
            cache_tokens=0,
            output_tokens=0,
        ),
        DEPOSIT,
    )
    settle(client, 3)  # Keeps run 1 as the bucket's representative.
    ledger.forget((3).to_bytes(32, "big"))
    assert (
        forgotten in ledger.runs and ledger.usage[(MODEL, CATALOG.rate_card_hash)].input_tokens == 6
    )

    def message(total):
        return pb.SessionState(
            session_id=ledger.session_id,
            nonce=b"n" * 32,
            control_generation=2,
            ready=True,
            close_pending=False,
            descriptor=descriptor_to_wire(DESCRIPTOR),
            accepted_voucher=ledger.voucher,
            meter_seq=4,
            usage_by_price=[
                pb.PricedUsage(
                    model=MODEL,
                    rate_card_hash=CATALOG.rate_card_hash,
                    input_tokens=total,
                    cache_tokens=0,
                    output_tokens=0,
                    acceptance_run_id=b"r" * 32,
                    acceptance_control_generation=1,
                    acceptance_signature=signature,
                )
            ],
            run_states=[
                pb.RunState(
                    run_id=b"r" * 32,
                    model=MODEL,
                    rate_card_hash=CATALOG.rate_card_hash,
                    control_generation=1,
                    auth_signature=signature,
                    status=pb.RUN_STATUS_COMPLETED,
                    input_tokens=6,
                    cache_tokens=0,
                    output_tokens=0,
                )
            ],
            catalogs=[
                pb.CatalogVersion(
                    rate_card_hash=CATALOG.rate_card_hash, catalog=CATALOG.canonical_bytes
                )
            ],
            initial_rate_card_hash=CATALOG.rate_card_hash,
            current_rate_card_hash=CATALOG.rate_card_hash,
            deposit=DEPOSIT,
            settled_cumulative=0,
            status=pb.SESSION_STATUS_OPEN,
            chain_block_number=1,
            chain_block_hash=b"b" * 32,
        )

    # True total: 6 known (2 + 2 forgotten + 2 retained) plus run r's increase of 4.
    with pytest.raises(ValidationError):
        deepcopy(ledger).restore_snapshot(message(8), nonce=b"n" * 32, deposit=DEPOSIT, settled=0)
    restored = deepcopy(ledger)
    restored.restore_snapshot(message(10), nonce=b"n" * 32, deposit=DEPOSIT, settled=0)
    assert restored.usage[(MODEL, CATALOG.rate_card_hash)].input_tokens == 10
    client._checkpoint.close()


def test_the_payment_checkpoint_is_version_two_and_version_one_still_loads(tmp_path):
    async def scenario():
        client = retention_client(tmp_path)
        settle(client, 1)
        await client._persist()
        assert client._checkpoint.load()["version"] == 2
        client._checkpoint.save({"version": 1})
        assert client._checkpoint.load() == {"version": 1}
        with pytest.raises(ValidationError):
            client._checkpoint.save({"version": 3})
        client._checkpoint.close()

    asyncio.run(scenario())
