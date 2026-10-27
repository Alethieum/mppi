"""Wallet handoff failures must never turn reconciliation into another payment."""

from dataclasses import replace
from unittest.mock import Mock

import pytest
from mppi_protocol.capabilities import FundingAuthorization, TransactionWallet
from mppi_protocol.chain import ChainReader, CodePin, DeploymentProfile
from mppi_protocol.contract import ContractCalls
from mppi_protocol.errors import ValidationError
from mppi_protocol.operations import reconcile_operation, submit_operation
from mppi_protocol.types import (
    ChainObservation,
    Domain,
    OperationResult,
    OperationState,
    SessionDescriptor,
)

DOMAIN = Domain(5042002, b"\x99" * 20)
TOKEN = bytes.fromhex("3600000000000000000000000000000000000000")
DESCRIPTOR = SessionDescriptor(
    b"\x11" * 20, b"\x12" * 20, 123, b"\x22" * 20, b"\x33" * 20, TOKEN, b"\x44" * 32
)
AUTH = FundingAuthorization(
    DOMAIN.chain_id,
    TOKEN,
    "USDC",
    "2",
    DESCRIPTOR.agent,
    DOMAIN.verifying_contract,
    100,
    1,
    1000,
    b"\x55" * 32,
)
CALLS = ContractCalls(DOMAIN, "USDC", "2")
TX = b"\xaa" * 32
OBSERVATION = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 100, b"\xbb" * 32)
REF = "original-economic-operation"
WALLET_ID = "wallet-operation-123"
PROFILE = DeploymentProfile(
    DOMAIN,
    b"\x97" * 20,
    CodePin(DOMAIN.verifying_contract, b"\x01" * 32),
    CodePin(b"\x98" * 20, b"\x02" * 32),
    CodePin(b"\x97" * 20, b"\x03" * 32),
    CodePin(b"\x96" * 20, b"\x04" * 32),
)
SIGNATURE = b"\x01" * 64 + b"\x1b"
OPEN = CALLS.open(DESCRIPTOR, 100, b"\x66" * 32, AUTH, b"opaque token proof")
OPERATIONS = (
    OPEN,
    CALLS.top_up(DESCRIPTOR, 100, AUTH, b"opaque token proof"),
    CALLS.request_close(DESCRIPTOR),
    CALLS.withdraw(DESCRIPTOR),
    CALLS.settle(DESCRIPTOR, 100, 50, SIGNATURE),
    CALLS.settle_batch(((DESCRIPTOR, 100, 50, SIGNATURE),)),
    CALLS.close(DESCRIPTOR, 100, 50, SIGNATURE),
    CALLS.release_idle(DESCRIPTOR),
)


@pytest.fixture
def wallet() -> Mock:
    result = Mock(spec=TransactionWallet)
    result.address = DESCRIPTOR.agent
    result.execute.return_value = OperationResult(REF, OperationState.PENDING, WALLET_ID)
    result.reconcile.return_value = result.execute.return_value
    return result


@pytest.fixture
def chain() -> Mock:
    result = Mock(spec=ChainReader)
    result.profile = PROFILE
    result.reconcile.return_value = OperationResult(
        REF, OperationState.CONFIRMED, WALLET_ID, TX, OBSERVATION
    )
    return result


@pytest.mark.parametrize("helper", (submit_operation, reconcile_operation))
@pytest.mark.parametrize("operation", OPERATIONS, ids=lambda item: item.kind)
@pytest.mark.parametrize(
    "address", (DESCRIPTOR.agent, DESCRIPTOR.recipient, DESCRIPTOR.operator, b"\xee" * 20)
)
def test_caller_authority_for_each_operation(helper, operation, address, wallet, chain) -> None:
    wallet.address = address
    if operation.kind.value in ("open", "topUp", "requestClose", "withdraw"):
        allowed = address == DESCRIPTOR.agent
    elif operation.kind.value == "releaseIdle":
        allowed = True
    else:
        allowed = address in (DESCRIPTOR.recipient, DESCRIPTOR.operator)
    if allowed:
        result = helper(wallet, operation, operation_ref=REF, chain=chain)
        assert result.state is OperationState.PENDING
        if helper is submit_operation:
            wallet.execute.assert_called_once_with(operation.call, operation_ref=REF)
            wallet.reconcile.assert_not_called()
        else:
            wallet.reconcile.assert_called_once_with(operation_ref=REF)
            wallet.execute.assert_not_called()
    else:
        with pytest.raises(ValidationError):
            helper(wallet, operation, operation_ref=REF, chain=chain)
        wallet.execute.assert_not_called()
        wallet.reconcile.assert_not_called()
    chain.reconcile.assert_not_called()


@pytest.mark.parametrize("helper", (submit_operation, reconcile_operation))
def test_batch_requires_authority_for_every_descriptor(helper, wallet, chain) -> None:
    wallet.address = DESCRIPTOR.operator
    other = replace(DESCRIPTOR, operator=b"\xab" * 20, recipient=b"\xcd" * 20)
    operation = CALLS.settle_batch(((DESCRIPTOR, 100, 50, SIGNATURE), (other, 100, 50, SIGNATURE)))
    with pytest.raises(ValidationError):
        helper(wallet, operation, operation_ref=REF, chain=chain)
    wallet.execute.assert_not_called()
    wallet.reconcile.assert_not_called()


@pytest.mark.parametrize("helper", (submit_operation, reconcile_operation))
@pytest.mark.parametrize("ref", ("", None, 1, b"not-a-string"))
def test_reference_invalid_before_wallet_call(helper, ref, wallet, chain) -> None:
    with pytest.raises(ValidationError):
        helper(wallet, OPEN, operation_ref=ref, chain=chain)
    wallet.execute.assert_not_called()
    wallet.reconcile.assert_not_called()


@pytest.mark.parametrize("helper", (submit_operation, reconcile_operation))
@pytest.mark.parametrize("address", (bytes(20), b"\x01" * 19, "0x" + "11" * 20))
def test_permissionless_exit_still_requires_nonzero_wallet(helper, address, wallet, chain) -> None:
    wallet.address = address
    with pytest.raises(ValidationError):
        helper(wallet, OPERATIONS[-1], operation_ref=REF, chain=chain)
    wallet.execute.assert_not_called()
    wallet.reconcile.assert_not_called()


@pytest.mark.parametrize("helper", (submit_operation, reconcile_operation))
@pytest.mark.parametrize("change", ({"chain_id": 5042}, {"to": b"\xee" * 20}))
def test_network_and_destination_mismatch_before_wallet_call(helper, change, wallet, chain) -> None:
    operation = replace(OPEN, call=replace(OPEN.call, **change))
    with pytest.raises(ValidationError):
        helper(wallet, operation, operation_ref=REF, chain=chain)
    wallet.execute.assert_not_called()
    wallet.reconcile.assert_not_called()


@pytest.mark.parametrize("helper", (submit_operation, reconcile_operation))
def test_wrong_operation_type_before_wallet_call(helper, wallet, chain) -> None:
    with pytest.raises(ValidationError):
        helper(wallet, OPEN.call, operation_ref=REF, chain=chain)
    wallet.execute.assert_not_called()
    wallet.reconcile.assert_not_called()


@pytest.mark.parametrize("state", tuple(OperationState))
def test_wallet_state_with_hash_always_uses_canonical_chain_result(state, wallet, chain) -> None:
    wallet.execute.return_value = OperationResult(REF, state, WALLET_ID, TX, OBSERVATION)
    # Even a wallet success/observed block is not independent MPPI evidence.
    chain.reconcile.return_value = OperationResult(REF, OperationState.UNKNOWN, WALLET_ID, TX)
    result = submit_operation(wallet, OPEN, operation_ref=REF, chain=chain)
    assert result == chain.reconcile.return_value
    chain.reconcile.assert_called_once_with(
        OPEN, operation_ref=REF, tx_hash=TX, wallet_operation_id=WALLET_ID
    )
    wallet.execute.assert_called_once_with(OPEN.call, operation_ref=REF)
    wallet.reconcile.assert_not_called()


def test_receipt_confirmation_wins_over_wallet_failure(wallet, chain) -> None:
    wallet.reconcile.return_value = OperationResult(REF, OperationState.FAILED, WALLET_ID, TX)
    result = reconcile_operation(wallet, OPEN, operation_ref=REF, chain=chain)
    assert result is chain.reconcile.return_value
    assert result.state is OperationState.CONFIRMED
    wallet.execute.assert_not_called()


@pytest.mark.parametrize(
    ("state", "wallet_id", "expected"),
    (
        (OperationState.PENDING, WALLET_ID, OperationState.PENDING),
        (OperationState.PENDING, None, OperationState.UNKNOWN),
        (OperationState.UNKNOWN, WALLET_ID, OperationState.UNKNOWN),
        (OperationState.UNKNOWN, None, OperationState.UNKNOWN),
        (OperationState.FAILED, WALLET_ID, OperationState.FAILED),
        (OperationState.FAILED, None, OperationState.FAILED),
    ),
)
def test_hashless_adapter_result_keeps_known_identity(
    state, wallet_id, expected, wallet, chain
) -> None:
    wallet.reconcile.return_value = OperationResult(REF, state, wallet_id)
    result = reconcile_operation(wallet, OPEN, operation_ref=REF, chain=chain)
    assert result == OperationResult(REF, expected, wallet_id)
    chain.reconcile.assert_not_called()
    wallet.execute.assert_not_called()


@pytest.mark.parametrize("helper", (submit_operation, reconcile_operation))
@pytest.mark.parametrize("error", (TimeoutError("reply lost"), RuntimeError("unavailable")))
def test_lost_wallet_reply_preserves_ref_and_never_retries(helper, error, wallet, chain) -> None:
    wallet.execute.side_effect = error
    wallet.reconcile.side_effect = error
    result = helper(wallet, OPEN, operation_ref=REF, chain=chain)
    assert result == OperationResult(REF, OperationState.UNKNOWN)
    assert wallet.execute.call_count + wallet.reconcile.call_count == 1
    chain.reconcile.assert_not_called()


def test_lost_submit_reply_is_later_reconciled_without_resubmission(wallet, chain) -> None:
    wallet.execute.side_effect = TimeoutError("remote submission may have succeeded")
    first = submit_operation(wallet, OPEN, operation_ref=REF, chain=chain)
    assert first.state is OperationState.UNKNOWN
    wallet.reconcile.return_value = OperationResult(REF, OperationState.UNKNOWN, WALLET_ID, TX)
    second = reconcile_operation(wallet, OPEN, operation_ref=first.operation_ref, chain=chain)
    assert second.state is OperationState.CONFIRMED
    assert second.wallet_operation_id == WALLET_ID and second.tx_hash == TX
    wallet.execute.assert_called_once_with(OPEN.call, operation_ref=REF)
    wallet.reconcile.assert_called_once_with(operation_ref=REF)


def test_unavailable_chain_retains_all_submission_identifiers(wallet, chain) -> None:
    wallet.execute.return_value = OperationResult(REF, OperationState.PENDING, WALLET_ID, TX)
    chain.reconcile.side_effect = TimeoutError("RPC unavailable")
    assert submit_operation(wallet, OPEN, operation_ref=REF, chain=chain) == OperationResult(
        REF, OperationState.UNKNOWN, WALLET_ID, TX
    )
    wallet.execute.assert_called_once()


@pytest.mark.parametrize(
    "change",
    (
        {"operation_ref": "foreign-ref"},
        {"tx_hash": b"\xff" * 32},
        {"wallet_operation_id": "foreign-id"},
    ),
)
def test_mismatched_chain_result_keeps_original_handles(change, wallet, chain) -> None:
    wallet.reconcile.return_value = OperationResult(REF, OperationState.PENDING, WALLET_ID, TX)
    chain.reconcile.return_value = replace(chain.reconcile.return_value, **change)
    assert reconcile_operation(wallet, OPEN, operation_ref=REF, chain=chain) == OperationResult(
        REF, OperationState.UNKNOWN, WALLET_ID, TX
    )
    wallet.execute.assert_not_called()


@pytest.mark.parametrize(
    "response",
    (
        None,
        {"operation_ref": REF, "state": "pending"},
        OperationResult("another-operation", OperationState.PENDING, "foreign-id", b"\xff" * 32),
        OperationResult(REF, OperationState.PENDING, ""),
        OperationResult(REF, OperationState.PENDING, 123),
    ),
)
def test_malformed_wallet_response_never_misattributed(response, wallet, chain) -> None:
    wallet.reconcile.return_value = response
    assert reconcile_operation(wallet, OPEN, operation_ref=REF, chain=chain) == OperationResult(
        REF, OperationState.UNKNOWN
    )
    wallet.execute.assert_not_called()
    chain.reconcile.assert_not_called()


def test_wallet_claim_of_inclusion_is_pending_when_actual_receipt_missing(wallet) -> None:
    # A real ChainReader handles the missing canonical receipt, independent of
    # the adapter's claimed block. No wallet observation enters its RPC path.
    rpc = Mock()
    rpc.call.side_effect = (hex(DOMAIN.chain_id), None)
    chain = ChainReader(rpc, PROFILE)
    wallet.execute.return_value = OperationResult(
        REF, OperationState.CONFIRMED, WALLET_ID, TX, OBSERVATION
    )
    result = submit_operation(wallet, OPEN, operation_ref=REF, chain=chain)
    assert result == OperationResult(REF, OperationState.PENDING, WALLET_ID, TX)
    assert rpc.call.call_args_list[-1].args == ("eth_getTransactionReceipt", ["0x" + TX.hex()])
    wallet.execute.assert_called_once()


def test_wallet_address_backend_failure_is_redacted_before_handoff(chain) -> None:
    class UnavailableWallet:
        @property
        def address(self):
            raise RuntimeError("credential=secret-token")

        def execute(self, *args, **kwargs):
            raise AssertionError("must not execute")

    with pytest.raises(ValidationError) as error:
        submit_operation(UnavailableWallet(), OPEN, operation_ref=REF, chain=chain)
    assert "secret" not in str(error.value)
    assert error.value.__suppress_context__


@pytest.mark.parametrize("helper", (submit_operation, reconcile_operation))
@pytest.mark.parametrize(
    ("response", "expected"),
    (
        (
            OperationResult(REF, OperationState.UNKNOWN, cause="WalletError: lost"),
            OperationState.UNKNOWN,
        ),
        (
            OperationResult(REF, OperationState.FAILED, cause="WalletError: refused"),
            OperationState.FAILED,
        ),
        (
            OperationResult(REF, OperationState.PENDING, WALLET_ID, cause="queued"),
            OperationState.PENDING,
        ),
        (
            OperationResult(REF, OperationState.UNKNOWN, WALLET_ID, TX, cause="lost"),
            OperationState.UNKNOWN,
        ),
    ),
)
def test_a_wallets_cause_survives_an_unproven_result(
    helper, response, expected, wallet, chain
) -> None:
    # Diagnostic only and never compared, as in the Rust SDK (operations.rs).
    wallet.execute.return_value = wallet.reconcile.return_value = response
    chain.reconcile.side_effect = TimeoutError("RPC unavailable")
    result = helper(wallet, OPEN, operation_ref=REF, chain=chain)
    assert result.state is expected
    assert result.cause == response.cause
