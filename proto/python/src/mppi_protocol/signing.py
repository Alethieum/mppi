"""MPPI EIP-712 hashing and fixed ECDSA session authority.

Hashing and signature checks do not establish chain identity, session readiness,
replay eligibility, current generation, available deposit or actual usage.
Callers supply the independently verified domain and confirmed descriptor.

Encoding matches contracts/src/MPPISigning.sol and EIP-712 encodeData:
https://eips.ethereum.org/EIPS/eip-712#definition-of-encodedata
The fixed words below are the complete MPPI schema, not a general ABI encoder.
Account APIs verified against eth-account v0.14.0 account.py:274,414,547 and
messages.py:31-62; LocalAccount.sign_message in signers/local.py:107:
https://github.com/ethereum/eth-account/tree/v0.14.0/eth_account
Signature restrictions match the vendored OpenZeppelin v5.7.0 ECDSA.tryRecover.
"""

from Crypto.Hash import keccak
from eth_account import Account
from eth_account.messages import SignableMessage
from eth_account.signers.local import LocalAccount

from .amounts import uint64
from .errors import ValidationCode, ValidationError
from .types import Domain, RunContext, SessionDescriptor, fixed_bytes

_ORDER = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
_HALF_ORDER = _ORDER // 2


def _keccak(value: bytes) -> bytes:
    return keccak.new(digest_bits=256, data=value).digest()


_DOMAIN_TYPEHASH = _keccak(
    b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
)
_NAME_HASH = _keccak(b"MPPI")
_VERSION_HASH = _keccak(b"1")
_VOUCHER_TYPEHASH = _keccak(b"Voucher(bytes32 sessionId,uint64 cumulativeAmount)")
_RUN_TYPEHASH = _keccak(
    b"RunAuth(bytes32 sessionId,uint64 controlGeneration,bytes32 runId,string model,bytes32 rateCardHash)"
)
_RECONNECT_TYPEHASH = _keccak(b"ReconnectAuth(bytes32 sessionId,bytes32 nonce)")
_CLOSE_TYPEHASH = _keccak(b"CloseAuth(bytes32 sessionId)")


def _check_domain(domain: Domain) -> None:
    if type(domain) is not Domain:
        raise ValidationError(ValidationCode.INVALID_VALUE, "domain must be Domain")
    domain.__post_init__()


def _check_descriptor(descriptor: SessionDescriptor) -> None:
    if type(descriptor) is not SessionDescriptor:
        raise ValidationError(ValidationCode.INVALID_VALUE, "descriptor must be SessionDescriptor")
    descriptor.__post_init__()


def _check_run(context: RunContext) -> None:
    if type(context) is not RunContext:
        raise ValidationError(ValidationCode.INVALID_VALUE, "context must be RunContext")
    context.__post_init__()


def domain_separator(domain: Domain) -> bytes:
    """Hash the fixed MPPI/1 domain in the validated uint64 chain profile."""
    _check_domain(domain)
    return _keccak(
        _DOMAIN_TYPEHASH
        + _NAME_HASH
        + _VERSION_HASH
        + domain.chain_id.to_bytes(32, "big")
        + domain.verifying_contract.rjust(32, b"\0")
    )


def session_id(domain: Domain, descriptor: SessionDescriptor) -> bytes:
    """Hash all seven descriptor fields, then canonical proxy and chain id."""
    _check_domain(domain)
    _check_descriptor(descriptor)
    return _keccak(
        descriptor.agent.rjust(32, b"\0")
        + descriptor.session_signer.rjust(32, b"\0")
        + descriptor.provider_id.to_bytes(32, "big")
        + descriptor.recipient.rjust(32, b"\0")
        + descriptor.operator.rjust(32, b"\0")
        + descriptor.token.rjust(32, b"\0")
        + descriptor.salt
        + domain.verifying_contract.rjust(32, b"\0")
        + domain.chain_id.to_bytes(32, "big")
    )


def voucher_struct_hash(session_id: bytes, cumulative_amount: int) -> bytes:
    fixed_bytes(session_id, 32, field="session_id")
    uint64(cumulative_amount, field="cumulative_amount")
    return _keccak(_VOUCHER_TYPEHASH + session_id + cumulative_amount.to_bytes(32, "big"))


def run_auth_struct_hash(
    session_id: bytes,
    control_generation: int,
    run_id: bytes,
    model: str,
    rate_card_hash: bytes,
) -> bytes:
    """Encode RunAuth, including exact UTF-8, without claiming model admission.

    This primitive accepts every uint64 generation and UTF-8 string, matching
    the Solidity hash helper. Signing/verification takes a validated RunContext
    and requires the MPPI model profile and a positive generation.
    """
    fixed_bytes(session_id, 32, field="session_id")
    uint64(control_generation, field="control_generation")
    fixed_bytes(run_id, 32, field="run_id")
    fixed_bytes(rate_card_hash, 32, field="rate_card_hash")
    if type(model) is not str:
        raise ValidationError(ValidationCode.INVALID_VALUE, "model must be a UTF-8 string")
    try:
        encoded_model = model.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        raise ValidationError(
            ValidationCode.INVALID_VALUE, "model must be a UTF-8 string"
        ) from None
    return _keccak(
        _RUN_TYPEHASH
        + session_id
        + control_generation.to_bytes(32, "big")
        + run_id
        + _keccak(encoded_model)
        + rate_card_hash
    )


def reconnect_auth_struct_hash(session_id: bytes, nonce: bytes) -> bytes:
    fixed_bytes(session_id, 32, field="session_id")
    fixed_bytes(nonce, 32, field="nonce")
    return _keccak(_RECONNECT_TYPEHASH + session_id + nonce)


def close_auth_struct_hash(session_id: bytes) -> bytes:
    fixed_bytes(session_id, 32, field="session_id")
    return _keccak(_CLOSE_TYPEHASH + session_id)


def _run_struct_hash(context: RunContext) -> bytes:
    _check_run(context)
    return run_auth_struct_hash(
        context.session_id,
        context.control_generation,
        context.run_id,
        context.model,
        context.rate_card_hash,
    )


def _message(domain: Domain, struct_hash: bytes) -> SignableMessage:
    fixed_bytes(struct_hash, 32, field="struct_hash")
    return SignableMessage(b"\x01", domain_separator(domain), struct_hash)


def _digest(domain: Domain, struct_hash: bytes) -> bytes:
    message = _message(domain, struct_hash)
    return _keccak(b"\x19" + message.version + message.header + message.body)


def voucher_digest(domain: Domain, session_id: bytes, cumulative_amount: int) -> bytes:
    return _digest(domain, voucher_struct_hash(session_id, cumulative_amount))


def run_auth_digest(domain: Domain, context: RunContext) -> bytes:
    return _digest(domain, _run_struct_hash(context))


def reconnect_auth_digest(domain: Domain, session_id: bytes, nonce: bytes) -> bytes:
    return _digest(domain, reconnect_auth_struct_hash(session_id, nonce))


def close_auth_digest(domain: Domain, session_id: bytes) -> bytes:
    return _digest(domain, close_auth_struct_hash(session_id))


def validate_signature(signature: bytes) -> bytes:
    """Require canonical MPPI wire encoding; this does not recover authority."""
    fixed_bytes(signature, 65, field="signature")
    r = int.from_bytes(signature[:32], "big")
    s = int.from_bytes(signature[32:64], "big")
    if not (0 < r < _ORDER and 0 < s <= _HALF_ORDER and signature[64] in (27, 28)):
        raise ValidationError(ValidationCode.INVALID_VALUE, "invalid canonical MPPI signature")
    return signature


def _verify(domain: Domain, struct_hash: bytes, signature: bytes, signer: bytes) -> None:
    fixed_bytes(signer, 20, field="session_signer", nonzero=True)
    validate_signature(signature)
    message = _message(domain, struct_hash)
    try:
        recovered = bytes.fromhex(Account.recover_message(message, signature=signature)[2:])
    except Exception:
        # The backend may raise BadSignature as well as ValueError. No backend
        # failure grants authority or leaks an untrusted signature in diagnostics.
        raise ValidationError(
            ValidationCode.INVALID_VALUE, "MPPI signature recovery failed"
        ) from None
    if recovered != signer:
        raise ValidationError(
            ValidationCode.INVALID_VALUE, "signature does not match session signer"
        )


def verify_voucher(
    domain: Domain, descriptor: SessionDescriptor, cumulative_amount: int, signature: bytes
) -> None:
    _verify(
        domain,
        voucher_struct_hash(session_id(domain, descriptor), cumulative_amount),
        signature,
        descriptor.session_signer,
    )


def verify_run_auth(
    domain: Domain, descriptor: SessionDescriptor, context: RunContext, signature: bytes
) -> None:
    _check_run(context)
    if context.session_id != session_id(domain, descriptor):
        raise ValidationError(ValidationCode.INVALID_VALUE, "run session does not match descriptor")
    _verify(domain, _run_struct_hash(context), signature, descriptor.session_signer)


def verify_reconnect_auth(
    domain: Domain, descriptor: SessionDescriptor, nonce: bytes, signature: bytes
) -> None:
    _verify(
        domain,
        reconnect_auth_struct_hash(session_id(domain, descriptor), nonce),
        signature,
        descriptor.session_signer,
    )


def verify_close_auth(domain: Domain, descriptor: SessionDescriptor, signature: bytes) -> None:
    _verify(
        domain,
        close_auth_struct_hash(session_id(domain, descriptor)),
        signature,
        descriptor.session_signer,
    )


class LocalSessionSigner:
    """Fixed in-memory ECDSA capability for the four MPPI message types.

    Supply exactly 32 private-key bytes obtained through the caller's secure
    credential integration. This object retains the key in process memory;
    it does not generate, persist, log, rotate or securely erase credentials.
    It has no wallet execution, funding or ERC-8004 authority of its own.
    """

    __slots__ = ("_account",)

    def __init__(self, private_key: bytes) -> None:
        fixed_bytes(private_key, 32, field="private_key")
        if not 0 < int.from_bytes(private_key, "big") < _ORDER:
            raise ValidationError(ValidationCode.INVALID_VALUE, "invalid secp256k1 private key")
        try:
            self._account: LocalAccount = Account.from_key(private_key)
        except Exception:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "session signer setup failed"
            ) from None

    @property
    def address(self) -> bytes:
        return bytes.fromhex(self._account.address[2:])

    def __repr__(self) -> str:
        return f"LocalSessionSigner(address=0x{self.address.hex()})"

    def _sign(self, domain: Domain, struct_hash: bytes) -> bytes:
        message = _message(domain, struct_hash)
        try:
            signature = bytes(self._account.sign_message(message).signature)
        except Exception:
            raise ValidationError(ValidationCode.INVALID_VALUE, "MPPI signing failed") from None
        validate_signature(signature)
        return signature

    def sign_voucher(self, domain: Domain, session_id: bytes, cumulative_amount: int) -> bytes:
        return self._sign(domain, voucher_struct_hash(session_id, cumulative_amount))

    def sign_run(self, domain: Domain, context: RunContext) -> bytes:
        return self._sign(domain, _run_struct_hash(context))

    def sign_close(self, domain: Domain, session_id: bytes) -> bytes:
        return self._sign(domain, close_auth_struct_hash(session_id))

    def sign_reconnect(self, domain: Domain, session_id: bytes, nonce: bytes) -> bytes:
        return self._sign(domain, reconnect_auth_struct_hash(session_id, nonce))


__all__ = [
    "LocalSessionSigner",
    "close_auth_digest",
    "close_auth_struct_hash",
    "domain_separator",
    "reconnect_auth_digest",
    "reconnect_auth_struct_hash",
    "run_auth_digest",
    "run_auth_struct_hash",
    "session_id",
    "validate_signature",
    "verify_close_auth",
    "verify_reconnect_auth",
    "verify_run_auth",
    "verify_voucher",
    "voucher_digest",
    "voucher_struct_hash",
]
