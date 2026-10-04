"""Wallet capability contracts. No wallet adapter is supplied by this module."""

from dataclasses import dataclass
from typing import Protocol

from .amounts import uint64
from .errors import ValidationCode, ValidationError
from .types import Domain, OperationResult, RunContext, fixed_bytes, uint256


@dataclass(frozen=True, slots=True)
class WalletCall:
    """A nonpayable MPPI/registry call made by the configured wallet."""

    chain_id: int
    to: bytes
    data: bytes

    def __post_init__(self) -> None:
        uint64(self.chain_id, field="chain_id", positive=True)
        fixed_bytes(self.to, 20, field="to", nonzero=True)
        if type(self.data) is not bytes:
            raise ValidationError(ValidationCode.INVALID_VALUE, "calldata must be bytes")


@dataclass(frozen=True, slots=True)
class FundingAuthorization:
    """USDC receiveWithAuthorization input, in the token's separate domain."""

    chain_id: int
    token: bytes
    domain_name: str
    domain_version: str
    owner: bytes
    recipient: bytes
    value: int
    valid_after: int
    valid_before: int
    nonce: bytes

    def __post_init__(self) -> None:
        uint64(self.chain_id, field="chain_id", positive=True)
        for name in ("token", "owner", "recipient"):
            fixed_bytes(getattr(self, name), 20, field=name, nonzero=True)
        for name in ("value", "valid_after", "valid_before"):
            uint256(getattr(self, name), field=name)
        fixed_bytes(self.nonce, 32, field="nonce")
        if not self.domain_name or not self.domain_version or self.valid_after >= self.valid_before:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "invalid token domain or authorization interval"
            )


class TransactionWallet(Protocol):
    @property
    def address(self) -> bytes: ...

    def execute(self, call: WalletCall, *, operation_ref: str) -> OperationResult: ...

    def reconcile(self, *, operation_ref: str) -> OperationResult: ...


class FundingWallet(TransactionWallet, Protocol):
    def sign_funding_authorization(self, authorization: FundingAuthorization) -> bytes: ...


class SessionSigner(Protocol):
    """The fixed ECDSA capability, separate from wallet identity and authority."""

    @property
    def address(self) -> bytes: ...

    def sign_voucher(self, domain: Domain, session_id: bytes, cumulative_amount: int) -> bytes: ...

    def sign_run(self, domain: Domain, context: RunContext) -> bytes: ...

    def sign_close(self, domain: Domain, session_id: bytes) -> bytes: ...

    def sign_reconnect(self, domain: Domain, session_id: bytes, nonce: bytes) -> bytes: ...
