"""Accounting uses independently signed intent, never provider-supplied authority."""

from dataclasses import replace

import pytest
from mppi.v1 import mppi_pb2 as pb
from mppi_agent.state import Ledger
from mppi_protocol.amounts import Rates
from mppi_protocol.catalog import CatalogRow, publish_catalog
from mppi_protocol.errors import ValidationError
from mppi_protocol.signing import LocalSessionSigner
from mppi_protocol.types import Domain, RunContext, SessionDescriptor
from mppi_protocol.wire import descriptor_to_wire

SIGNER = LocalSessionSigner(bytes.fromhex("11" * 32))  # Unfunded public fixture.
DOMAIN = Domain(5042002, b"p" * 20)
MODEL = "test/model"
CATALOG = publish_catalog((CatalogRow(MODEL, Rates(500_000, 500_000)),))
DESCRIPTOR = SessionDescriptor(
    b"a" * 20, SIGNER.address, 7, b"r" * 20, b"o" * 20, bytes.fromhex("36" + "00" * 19), b"s" * 32
)
NONCE = b"n" * 32


def ledger_with_runs(count: int = 1) -> Ledger:
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


@pytest.mark.parametrize("field", ("model", "rate_card_hash"))
def test_ordinary_low_credit_must_bind_to_a_locally_signed_run(field):
    ledger = ledger_with_runs()
    notice = pb.LowCredit(
        session_id=ledger.session_id,
        model=MODEL,
        rate_card_hash=CATALOG.rate_card_hash,
        remaining_tokens=1,
        suggest_top_up_tokens=10,
    )
    assert ledger.coverage(notice, 100) is None
    setattr(notice, field, "other/model" if field == "model" else b"x" * 32)
    with pytest.raises(ValidationError, match="signed run price binding"):
        ledger.coverage(notice, 100)


def meter(
    ledger: Ledger, *, number: int = 0, seq: int = 1, inputs: int = 0, outputs: int = 0
) -> pb.Meter:
    return pb.Meter(
        session_id=ledger.session_id,
        run_id=number.to_bytes(32, "big"),
        seq=seq,
        input_tokens=inputs,
        cache_tokens=0,
        output_tokens=outputs,
    )


def final(
    ledger: Ledger, *, number: int = 0, seq: int = 1, inputs: int = 0, outputs: int = 0
) -> pb.Terminal:
    return pb.Terminal(
        session_id=ledger.session_id,
        run_id=number.to_bytes(32, "big"),
        model=MODEL,
        rate_card_hash=CATALOG.rate_card_hash,
        input_tokens=inputs,
        cache_tokens=0,
        output_tokens=outputs,
        amount=(inputs + outputs) // 2,
        status=pb.RUN_STATUS_COMPLETED,
        final_meter_seq=seq,
    )


def snapshot(ledger: Ledger, *, seq: int = 1, inputs: int = 4) -> pb.SessionState:
    runs = list(ledger.runs.values())
    return pb.SessionState(
        session_id=ledger.session_id,
        nonce=NONCE,
        control_generation=2,
        ready=True,
        close_pending=False,
        descriptor=descriptor_to_wire(ledger.descriptor),
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


def test_terminal_before_control_meter_and_duplicate_final_charge_once() -> None:
    ledger = ledger_with_runs()
    terminal = final(ledger, seq=2, inputs=4, outputs=2)
    assert ledger.apply_final(terminal, 100)
    assert ledger.usage_amount == 3 and ledger.meter_seq == 0
    assert ledger.apply_meter(meter(ledger, inputs=4), 100)
    assert ledger.apply_meter(meter(ledger, seq=2, inputs=4, outputs=2), 100)
    assert not ledger.apply_final(terminal, 100)
    assert ledger.usage_amount == 3 and ledger.meter_seq == 2


def test_equal_cursor_cannot_change_counts_and_invalid_report_is_atomic() -> None:
    ledger = ledger_with_runs()
    ledger.apply_meter(meter(ledger, inputs=4), 100)
    with pytest.raises(ValidationError):
        ledger.apply_final(final(ledger, inputs=6), 100)
    assert ledger.usage_amount == 2
    assert ledger.runs[bytes(32)].final is None
    with pytest.raises(ValidationError):
        ledger.apply_meter(meter(ledger, seq=3, inputs=6), 100)
    assert ledger.meter_seq == 1


def test_session_floor_happens_once_across_runs() -> None:
    ledger = ledger_with_runs(2)
    ledger.apply_final(final(ledger, number=0, seq=1, inputs=1), 100)
    ledger.apply_final(final(ledger, number=1, seq=2, inputs=1), 100)
    assert ledger.usage_amount == 1
    assert all(run.final[5] == 0 for run in ledger.runs.values())


def test_zero_final_requires_signed_authorization_even_when_ceiling_is_zero() -> None:
    ledger = ledger_with_runs()
    ledger.voucher = None
    with pytest.raises(ValidationError):
        ledger.apply_final(final(ledger), 100)
    ledger.accept_voucher(
        pb.Voucher(
            session_id=ledger.session_id,
            cumulative=0,
            signature=SIGNER.sign_voucher(DOMAIN, ledger.session_id, 0),
        ),
        100,
    )
    assert ledger.apply_final(final(ledger), 100)
    assert ledger.usage_amount == 0


def test_zero_usage_snapshot_cannot_claim_admission_without_a_voucher() -> None:
    ledger = ledger_with_runs()
    ledger.voucher = None
    message = snapshot(ledger, inputs=0)
    message.ClearField("accepted_voucher")
    with pytest.raises(ValidationError):
        ledger.restore_snapshot(message, nonce=NONCE, deposit=100, settled=0)
    assert ledger.generation == 1 and ledger.usage == {}


def test_counts_regression_and_over_authorization_rejected() -> None:
    ledger = ledger_with_runs()
    ledger.apply_meter(meter(ledger, inputs=4), 100)
    for report in (meter(ledger, seq=2, inputs=3), meter(ledger, seq=2, inputs=201)):
        with pytest.raises(ValidationError):
            ledger.apply_meter(report, 100)
    assert ledger.usage_amount == 2 and ledger.meter_seq == 1


def test_complete_coverage_and_output_renewal_use_exact_accepted_price() -> None:
    ledger = ledger_with_runs()
    ledger.voucher = None
    notice = pb.LowCredit(
        session_id=ledger.session_id,
        model=MODEL,
        rate_card_hash=CATALOG.rate_card_hash,
        remaining_tokens=0,
        suggest_top_up_tokens=10,
        coverage_request=pb.CoverageRequest(
            run_id=bytes(32),
            control_generation=1,
            required_by_price=[
                pb.RequiredTokens(
                    model=MODEL,
                    rate_card_hash=CATALOG.rate_card_hash,
                    input_tokens=4,
                    cache_tokens=0,
                    output_tokens=3,
                )
            ],
        ),
    )
    assert ledger.coverage(notice, 100) == 4  # ceil(3.5), not individual floors.
    ledger.accept_voucher(
        pb.Voucher(
            session_id=ledger.session_id,
            cumulative=4,
            signature=SIGNER.sign_voucher(DOMAIN, ledger.session_id, 4),
        ),
        100,
    )
    ledger.apply_meter(meter(ledger, inputs=4, outputs=1), 100)
    assert ledger.coverage_numerator == 4_000_000
    ledger.apply_meter(meter(ledger, seq=2, inputs=4, outputs=2), 100)
    assert ledger.coverage_numerator == 4_500_000
    notice.coverage_request.control_generation = 2
    with pytest.raises(ValidationError):
        ledger.coverage(notice, 100)
    ledger.runs[bytes(32)].failed_transport = True
    assert ledger.coverage(notice, 100) is None


def test_snapshot_replaces_totals_and_requires_all_pending_outcomes() -> None:
    ledger = ledger_with_runs()
    ledger.apply_meter(meter(ledger, inputs=2), 100)
    message = snapshot(ledger, seq=2)
    ledger.restore_snapshot(message, nonce=NONCE, deposit=100, settled=0)
    assert ledger.usage_amount == 2 and ledger.generation == 2
    assert not ledger.pending
    with pytest.raises(ValidationError):
        ledger.restore_snapshot(message, nonce=NONCE, deposit=100, settled=0)


def test_snapshot_keeps_local_unsent_close_intent_without_requiring_provider_ack() -> None:
    ledger = ledger_with_runs()
    ledger.close_pending = True
    message = snapshot(ledger)
    assert not message.close_pending
    ledger.restore_snapshot(message, nonce=NONCE, deposit=100, settled=0)
    assert ledger.close_pending and not ledger.ready and ledger.generation == 2


def test_snapshot_rejects_double_allocated_partition_before_mutating() -> None:
    ledger = ledger_with_runs(2)
    message = snapshot(ledger)
    with pytest.raises(ValidationError):
        ledger.restore_snapshot(message, nonce=NONCE, deposit=100, settled=0)
    assert ledger.generation == 1 and ledger.usage == {}
    assert len(ledger.pending) == 2


def test_snapshot_cannot_forge_price_acceptance_or_omit_pending_run() -> None:
    for tamper in ("signature", "pending", "catalog"):
        ledger = ledger_with_runs()
        message = snapshot(ledger)
        if tamper == "signature":
            other = LocalSessionSigner(bytes.fromhex("22" * 32))
            message.usage_by_price[0].acceptance_signature = other.sign_run(
                DOMAIN, next(iter(ledger.runs.values())).context
            )
        elif tamper == "pending":
            del message.run_states[:]
        else:
            message.catalogs[0].catalog = publish_catalog(
                (CatalogRow(MODEL, Rates(1, 1)),)
            ).canonical_bytes
        with pytest.raises(ValidationError):
            ledger.restore_snapshot(message, nonce=NONCE, deposit=100, settled=0)
        assert ledger.usage == {} and ledger.generation == 1


def test_fixed_signer_and_price_identity_survive_new_publication() -> None:
    ledger = ledger_with_runs()
    changed = publish_catalog((CatalogRow(MODEL, Rates(10_000_000, 10_000_000)),))
    ledger.add_catalog(changed)
    ledger.apply_final(final(ledger, inputs=4), 100)
    assert ledger.usage_amount == 2
    with pytest.raises(ValidationError):
        ledger.register(
            replace(next(iter(ledger.runs.values())).context, run_id=b"x" * 32), bytes(65)
        )


def test_unadmitted_parallel_candidate_is_not_required_in_other_coverage() -> None:
    ledger = ledger_with_runs()
    ledger.voucher = None
    second = publish_catalog((CatalogRow("test/second", Rates(1, 1)),))
    ledger.add_catalog(second)
    context = RunContext(ledger.session_id, b"b" * 32, 1, "test/second", second.rate_card_hash)
    ledger.register(context, SIGNER.sign_run(DOMAIN, context))
    notice = pb.LowCredit(
        session_id=ledger.session_id,
        model=MODEL,
        rate_card_hash=CATALOG.rate_card_hash,
        remaining_tokens=0,
        suggest_top_up_tokens=10,
        coverage_request=pb.CoverageRequest(
            run_id=bytes(32),
            control_generation=1,
            required_by_price=[
                pb.RequiredTokens(
                    model=MODEL,
                    rate_card_hash=CATALOG.rate_card_hash,
                    input_tokens=4,
                    cache_tokens=0,
                    output_tokens=3,
                )
            ],
        ),
    )
    assert ledger.coverage(notice, 100) == 4


def test_final_cursor_cannot_be_assigned_to_another_run() -> None:
    ledger = ledger_with_runs(2)
    ledger.apply_meter(meter(ledger, number=0, seq=1, inputs=4), 100)
    with pytest.raises(ValidationError):
        ledger.apply_final(final(ledger, number=1, seq=1, inputs=4), 100)
    ledger.apply_final(final(ledger, number=0, seq=2, inputs=4), 100)
    with pytest.raises(ValidationError):
        ledger.apply_meter(meter(ledger, number=1, seq=2, inputs=4), 100)
    assert ledger.meter_seq == 1 and ledger.runs[(1).to_bytes(32, "big")].counts.input_tokens == 0
