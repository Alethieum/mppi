import pytest
from mppi_protocol import ValidationError
from mppi_protocol.types import (
    ChainObservation,
    OperationResult,
    OperationState,
    PauseLevel,
    PaymentCode,
    PaymentFailure,
    RunContext,
    SessionDescriptor,
)


def test_unknown_is_preserved_without_fabricated_chain_evidence() -> None:
    result = OperationResult("opening-1", OperationState.UNKNOWN)
    assert result.tx_hash is None
    assert result.observation is None
    with pytest.raises(ValidationError, match="requires chain evidence"):
        OperationResult("opening-1", OperationState.CONFIRMED)


def test_descriptor_has_distinct_fixed_signer_and_exact_uint256_id() -> None:
    descriptor = SessionDescriptor(
        b"a" * 20, b"s" * 20, 2**255, b"r" * 20, b"o" * 20, b"t" * 20, b"z" * 32
    )
    assert descriptor.agent != descriptor.session_signer
    assert descriptor.provider_id == 2**255
    with pytest.raises(ValidationError):
        SessionDescriptor(b"a" * 20, b"\x00" * 20, 1, b"r" * 20, b"o" * 20, b"t" * 20, b"z" * 32)


def test_run_context_generation_and_fixed_widths() -> None:
    with pytest.raises(ValidationError):
        RunContext(b"s" * 32, b"r" * 32, 0, "x/y", b"h" * 32)
    with pytest.raises(ValidationError):
        ChainObservation(5042002, b"a" * 19, 1, b"h" * 32)


def test_payment_diagnostics_preserve_required_price_and_pause_evidence() -> None:
    with pytest.raises(ValidationError, match="requires a reason"):
        PaymentFailure(PaymentCode.SESSION_NOT_READY)
    changed = PaymentFailure(
        PaymentCode.SESSION_NOT_READY,
        reason="PRICE_CHANGED",
        session_id=b"s" * 32,
        run_id=b"r" * 32,
        requested_rate_card_hash=b"a" * 32,
        current_rate_card_hash=b"b" * 32,
    )
    assert changed.requested_rate_card_hash != changed.current_rate_card_hash
    with pytest.raises(ValidationError, match="both catalog hashes"):
        PaymentFailure(PaymentCode.SESSION_NOT_READY, reason="PRICE_CHANGED")
    with pytest.raises(ValidationError, match="session_id"):
        PaymentFailure(PaymentCode.INSUFFICIENT_DEPOSIT, run_id=b"r" * 32)
    for level in (PauseLevel.PAYMENTS, PauseLevel.ALL):
        failure = PaymentFailure(
            PaymentCode.SESSION_NOT_READY, reason="CONTRACT_PAUSED", pause_level=level
        )
        assert int(failure.pause_level) in (1, 2)
    for invalid in (None, PauseLevel.NONE, True, 1):
        with pytest.raises(ValidationError, match="active observed pause"):
            PaymentFailure(
                PaymentCode.SESSION_NOT_READY, reason="CONTRACT_PAUSED", pause_level=invalid
            )  # type: ignore[arg-type]
    unknown = PaymentFailure(PaymentCode.SESSION_NOT_READY, reason="FUTURE_REASON")
    assert unknown.code is PaymentCode.SESSION_NOT_READY
