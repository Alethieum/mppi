"""Explicit wallet submission and read-only reconciliation of one MPPI operation.

The wallet adapter must durably bind ``operation_ref`` to the exact wallet,
network, destination and calldata before submitting. It must reject reuse for a
different call and retain its wallet operation id and transaction hash. Its
``reconcile`` implementation only queries that original operation: a timeout,
missing result or expired caller wait must never create a replacement payment.

Adapters return known identifiers even when their service becomes unavailable.
An exception carrying no structured result can preserve only the supplied ref.
The caller retains the original PreparedOperation for reconciliation. These
helpers maintain no storage and do not prescribe provider persistence.

An adapter's FAILED result asserts a verified terminal rejection. Its success
signal only triggers canonical MPPI receipt verification through ChainReader.
No vendor-specific wallet adapter is implemented here.
"""

from .capabilities import TransactionWallet
from .chain import ChainReader
from .contract import OperationKind, PreparedOperation
from .errors import ValidationCode, ValidationError
from .types import OperationResult, OperationState, fixed_bytes


def _validate(
    wallet: TransactionWallet,
    operation: PreparedOperation,
    operation_ref: str,
    chain: ChainReader,
) -> None:
    if type(operation_ref) is not str or not operation_ref:
        raise ValidationError(ValidationCode.INVALID_VALUE, "operation_ref is required")
    if not isinstance(operation, PreparedOperation):
        raise ValidationError(ValidationCode.INVALID_VALUE, "a prepared MPPI operation is required")
    domain = chain.profile.domain
    if operation.call.chain_id != domain.chain_id or operation.call.to != domain.verifying_contract:
        raise ValidationError(
            ValidationCode.INVALID_VALUE, "operation differs from the selected MPPI deployment"
        )
    try:
        address = fixed_bytes(wallet.address, 20, field="wallet address", nonzero=True)
    except Exception:
        raise ValidationError(
            ValidationCode.INVALID_VALUE, "wallet address is unavailable or invalid"
        ) from None
    for descriptor in operation.descriptors:
        if operation.kind in (
            OperationKind.OPEN,
            OperationKind.TOP_UP,
            OperationKind.REQUEST_CLOSE,
            OperationKind.WITHDRAW,
        ):
            permitted = address == descriptor.agent
        elif operation.kind is OperationKind.RELEASE_IDLE:
            permitted = True
        else:
            permitted = address in (descriptor.recipient, descriptor.operator)
        if not permitted:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "wallet lacks authority for an operation descriptor"
            )


def _result(
    response: OperationResult, operation: PreparedOperation, operation_ref: str, chain: ChainReader
) -> OperationResult:
    # Do not attach another operation's identifiers to the caller's intent.
    if (
        not isinstance(response, OperationResult)
        or response.operation_ref != operation_ref
        or (
            response.wallet_operation_id is not None
            and (type(response.wallet_operation_id) is not str or not response.wallet_operation_id)
        )
    ):
        return OperationResult(operation_ref, OperationState.UNKNOWN)
    if response.tx_hash is not None:
        try:
            result = chain.reconcile(
                operation,
                operation_ref=operation_ref,
                tx_hash=response.tx_hash,
                wallet_operation_id=response.wallet_operation_id,
            )
            if (
                result.operation_ref == operation_ref
                and result.tx_hash == response.tx_hash
                and result.wallet_operation_id == response.wallet_operation_id
            ):
                return result
        except Exception:
            # The payment may have succeeded. Retain the reconciliation handles.
            pass
        return OperationResult(
            operation_ref, OperationState.UNKNOWN, response.wallet_operation_id, response.tx_hash
        )
    state = OperationState.UNKNOWN
    if response.state is OperationState.FAILED:
        state = OperationState.FAILED
    elif response.state is OperationState.PENDING and response.wallet_operation_id is not None:
        state = OperationState.PENDING
    # Wallet-provided observations/session outcomes are never chain evidence.
    return OperationResult(operation_ref, state, response.wallet_operation_id)


def submit_operation(
    wallet: TransactionWallet,
    operation: PreparedOperation,
    *,
    operation_ref: str,
    chain: ChainReader,
) -> OperationResult:
    """Submit explicitly once; reconcile this ref after any uncertain outcome.

    This function performs one adapter execute call, with no automatic retry.
    It does not prove live session eligibility or grant permission to replace a
    previous operation. Adapters must enforce their durable reference binding.
    """
    _validate(wallet, operation, operation_ref, chain)
    try:
        response = wallet.execute(operation.call, operation_ref=operation_ref)
    except Exception:
        return OperationResult(operation_ref, OperationState.UNKNOWN)
    return _result(response, operation, operation_ref, chain)


def reconcile_operation(
    wallet: TransactionWallet,
    operation: PreparedOperation,
    *,
    operation_ref: str,
    chain: ChainReader,
) -> OperationResult:
    """Read the original wallet operation and verify any available receipt.

    Pass the original prepared call and reference. This path never invokes
    execute, retries submission or creates a new funding authorization.
    """
    _validate(wallet, operation, operation_ref, chain)
    try:
        response = wallet.reconcile(operation_ref=operation_ref)
    except Exception:
        return OperationResult(operation_ref, OperationState.UNKNOWN)
    return _result(response, operation, operation_ref, chain)
