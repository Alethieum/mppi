"""Production MPPI signing against shared contract/Rust/independent ABI vectors."""

import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest
from eth_account import Account
from eth_account.messages import SignableMessage, encode_typed_data
from eth_utils import keccak
from mppi_protocol import MAX_UINT64, ValidationCode, ValidationError
from mppi_protocol.capabilities import SessionSigner
from mppi_protocol.signing import (
    LocalSessionSigner,
    close_auth_digest,
    close_auth_struct_hash,
    domain_separator,
    reconnect_auth_digest,
    reconnect_auth_struct_hash,
    run_auth_digest,
    run_auth_struct_hash,
    session_id,
    validate_signature,
    verify_close_auth,
    verify_reconnect_auth,
    verify_run_auth,
    verify_voucher,
    voucher_digest,
    voucher_struct_hash,
)
from mppi_protocol.types import Domain, RunContext, SessionDescriptor

FIXTURE = json.loads((Path(__file__).resolve().parents[3] / "testvectors/signing.json").read_text())
CASES = {case["name"]: case for case in FIXTURE["cases"]}
ORDER = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141


def raw(value: str) -> bytes:
    return bytes.fromhex(value[2:])


DOMAIN = Domain(int(FIXTURE["domain"]["chainId"]), raw(FIXTURE["domain"]["verifyingContract"]))
DESCRIPTOR = SessionDescriptor(
    raw(FIXTURE["descriptor"]["agent"]),
    raw(FIXTURE["descriptor"]["sessionSigner"]),
    int(FIXTURE["descriptor"]["providerId"]),
    raw(FIXTURE["descriptor"]["recipient"]),
    raw(FIXTURE["descriptor"]["operator"]),
    raw(FIXTURE["descriptor"]["token"]),
    raw(FIXTURE["descriptor"]["salt"]),
)
SESSION_ID = raw(FIXTURE["sessionId"])
KEY = raw(FIXTURE["privateKey"])  # Public fixture key: never funded.
RUN = RunContext(
    SESSION_ID,
    raw(CASES["run_auth"]["message"]["runId"]),
    int(CASES["run_auth"]["message"]["controlGeneration"]),
    CASES["run_auth"]["message"]["model"],
    raw(CASES["run_auth"]["message"]["rateCardHash"]),
)
NONCE = raw(CASES["reconnect_auth"]["message"]["nonce"])


def test_domain_and_full_descriptor_match_independent_vectors() -> None:
    assert domain_separator(DOMAIN) == raw(FIXTURE["domainSeparator"])
    assert session_id(DOMAIN, DESCRIPTOR) == SESSION_ID
    assert DESCRIPTOR.agent != DESCRIPTOR.session_signer
    # The provider identity is uint256, not an imprecise float or uint64 cast.
    wide = replace(DESCRIPTOR, provider_id=(1 << 256) - 1)
    assert session_id(DOMAIN, wide) != SESSION_ID
    assert domain_separator(Domain(MAX_UINT64, DOMAIN.verifying_contract)) != domain_separator(
        DOMAIN
    )


@pytest.mark.parametrize("name", ["voucher", "voucher_zero", "voucher_max"])
def test_voucher_amount_vectors_are_exact(name: str) -> None:
    case = CASES[name]
    amount = int(case["message"]["cumulativeAmount"])
    signer: SessionSigner = LocalSessionSigner(KEY)
    assert voucher_struct_hash(SESSION_ID, amount) == raw(case["structHash"])
    assert voucher_digest(DOMAIN, SESSION_ID, amount) == raw(case["digest"])
    signature = signer.sign_voucher(DOMAIN, SESSION_ID, amount)
    assert signature == raw(case["signature"])
    assert signer.address == DESCRIPTOR.session_signer
    assert validate_signature(signature) == signature
    verify_voucher(DOMAIN, DESCRIPTOR, amount, signature)


def test_other_three_proofs_match_independent_vectors_and_verify() -> None:
    signer = LocalSessionSigner(KEY)
    run_case, reconnect_case, close_case = [
        CASES[name] for name in ("run_auth", "reconnect_auth", "close_auth")
    ]
    assert run_auth_struct_hash(
        RUN.session_id, RUN.control_generation, RUN.run_id, RUN.model, RUN.rate_card_hash
    ) == raw(run_case["structHash"])
    assert run_auth_digest(DOMAIN, RUN) == raw(run_case["digest"])
    assert signer.sign_run(DOMAIN, RUN) == raw(run_case["signature"])
    verify_run_auth(DOMAIN, DESCRIPTOR, RUN, signer.sign_run(DOMAIN, RUN))
    assert reconnect_auth_struct_hash(SESSION_ID, NONCE) == raw(reconnect_case["structHash"])
    assert reconnect_auth_digest(DOMAIN, SESSION_ID, NONCE) == raw(reconnect_case["digest"])
    assert signer.sign_reconnect(DOMAIN, SESSION_ID, NONCE) == raw(reconnect_case["signature"])
    verify_reconnect_auth(
        DOMAIN, DESCRIPTOR, NONCE, signer.sign_reconnect(DOMAIN, SESSION_ID, NONCE)
    )
    assert close_auth_struct_hash(SESSION_ID) == raw(close_case["structHash"])
    assert close_auth_digest(DOMAIN, SESSION_ID) == raw(close_case["digest"])
    assert signer.sign_close(DOMAIN, SESSION_ID) == raw(close_case["signature"])
    verify_close_auth(DOMAIN, DESCRIPTOR, signer.sign_close(DOMAIN, SESSION_ID))


def test_utf8_hash_vector_does_not_bypass_model_validation() -> None:
    case = CASES["run_auth_utf8"]
    model = case["message"]["model"]
    struct_hash = run_auth_struct_hash(SESSION_ID, 7, RUN.run_id, model, RUN.rate_card_hash)
    assert struct_hash == raw(case["structHash"])
    assert keccak(b"\x19\x01" + domain_separator(DOMAIN) + struct_hash) == raw(case["digest"])
    with pytest.raises(ValidationError):
        replace(RUN, model=model)
    with pytest.raises(ValidationError):
        run_auth_struct_hash(SESSION_ID, 7, RUN.run_id, "x/\ud800", RUN.rate_card_hash)


@pytest.mark.parametrize(
    "field,value",
    [
        ("agent", b"a" * 20),
        ("session_signer", b"s" * 20),
        ("provider_id", DESCRIPTOR.provider_id + 1),
        ("recipient", b"r" * 20),
        ("operator", b"o" * 20),
        ("token", b"t" * 20),
        ("salt", b"z" * 32),
    ],
)
def test_every_descriptor_field_is_bound(field: str, value: object) -> None:
    changed = replace(DESCRIPTOR, **{field: value})
    assert session_id(DOMAIN, changed) != SESSION_ID
    with pytest.raises(ValidationError):
        verify_voucher(DOMAIN, changed, 5510000, raw(CASES["voucher"]["signature"]))


@pytest.mark.parametrize(
    "domain",
    [replace(DOMAIN, chain_id=1), replace(DOMAIN, verifying_contract=b"c" * 20)],
)
def test_all_proofs_reject_wrong_chain_or_proxy(domain: Domain) -> None:
    with pytest.raises(ValidationError):
        verify_voucher(domain, DESCRIPTOR, 5510000, raw(CASES["voucher"]["signature"]))
    with pytest.raises(ValidationError):
        verify_run_auth(domain, DESCRIPTOR, RUN, raw(CASES["run_auth"]["signature"]))
    with pytest.raises(ValidationError):
        verify_close_auth(domain, DESCRIPTOR, raw(CASES["close_auth"]["signature"]))
    with pytest.raises(ValidationError):
        verify_reconnect_auth(domain, DESCRIPTOR, NONCE, raw(CASES["reconnect_auth"]["signature"]))


@pytest.mark.parametrize(
    "context",
    [
        replace(RUN, session_id=b"s" * 32),
        replace(RUN, run_id=b"r" * 32),
        replace(RUN, control_generation=8),
        replace(RUN, model="example-org/changed-model"),
        replace(RUN, rate_card_hash=b"h" * 32),
    ],
)
def test_every_run_authority_field_is_bound(context: RunContext) -> None:
    with pytest.raises(ValidationError):
        verify_run_auth(DOMAIN, DESCRIPTOR, context, raw(CASES["run_auth"]["signature"]))


def test_nonce_amount_type_and_signer_substitutions_fail() -> None:
    with pytest.raises(ValidationError):
        verify_voucher(DOMAIN, DESCRIPTOR, 5510001, raw(CASES["voucher"]["signature"]))
    with pytest.raises(ValidationError):
        verify_reconnect_auth(
            DOMAIN, DESCRIPTOR, b"n" * 32, raw(CASES["reconnect_auth"]["signature"])
        )
    with pytest.raises(ValidationError):
        verify_close_auth(DOMAIN, DESCRIPTOR, raw(CASES["reconnect_auth"]["signature"]))
    stranger = LocalSessionSigner((2).to_bytes(32, "big"))
    for action in (
        lambda: verify_voucher(DOMAIN, DESCRIPTOR, 1, stranger.sign_voucher(DOMAIN, SESSION_ID, 1)),
        lambda: verify_run_auth(DOMAIN, DESCRIPTOR, RUN, stranger.sign_run(DOMAIN, RUN)),
        lambda: verify_close_auth(DOMAIN, DESCRIPTOR, stranger.sign_close(DOMAIN, SESSION_ID)),
        lambda: verify_reconnect_auth(
            DOMAIN, DESCRIPTOR, NONCE, stranger.sign_reconnect(DOMAIN, SESSION_ID, NONCE)
        ),
    ):
        with pytest.raises(ValidationError):
            action()


def test_same_wallet_and_fixed_signer_is_valid() -> None:
    same = replace(DESCRIPTOR, agent=DESCRIPTOR.session_signer)
    identifier = session_id(DOMAIN, same)
    signature = LocalSessionSigner(KEY).sign_voucher(DOMAIN, identifier, 1)
    verify_voucher(DOMAIN, same, 1, signature)


@pytest.mark.parametrize("amount", [True, -1, MAX_UINT64 + 1, 1.0, "1", None])
def test_voucher_rejects_coercion_and_out_of_range_before_signing(amount: object) -> None:
    with pytest.raises(ValidationError):
        LocalSessionSigner(KEY).sign_voucher(DOMAIN, SESSION_ID, amount)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [b"", bytes(31), bytes(33), bytearray(32), "0x" + "00" * 32])
def test_fixed_byte_fields_are_not_coerced(value: object) -> None:
    signer = LocalSessionSigner(KEY)
    with pytest.raises(ValidationError):
        signer.sign_close(DOMAIN, value)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        signer.sign_reconnect(DOMAIN, SESSION_ID, value)  # type: ignore[arg-type]


@pytest.mark.parametrize("recovery_id", [0, 1, 26, 29, 35, 255])
def test_signature_recovery_id_is_never_normalized(recovery_id: int) -> None:
    signature = raw(CASES["voucher"]["signature"])[:64] + bytes([recovery_id])
    with pytest.raises(ValidationError):
        verify_voucher(DOMAIN, DESCRIPTOR, 5510000, signature)


def test_signature_rejects_high_s_zero_scalars_out_of_range_and_compact_form() -> None:
    signature = raw(CASES["voucher"]["signature"])
    r = int.from_bytes(signature[:32], "big")
    s = int.from_bytes(signature[32:64], "big")
    v = signature[64]
    variants = [
        signature[:64],
        signature + b"\0",
        bytes(65),
        bytearray(signature),
        r.to_bytes(32, "big") + (ORDER - s).to_bytes(32, "big") + bytes([55 - v]),
        bytes(32) + signature[32:],
        ORDER.to_bytes(32, "big") + signature[32:],
        signature[:32] + bytes(32) + signature[64:],
        signature[:32] + ORDER.to_bytes(32, "big") + signature[64:],
    ]
    for malformed in variants:
        with pytest.raises(ValidationError):
            verify_voucher(DOMAIN, DESCRIPTOR, 5510000, malformed)  # type: ignore[arg-type]


def test_wrong_name_version_and_voucher_width_are_not_accepted() -> None:
    domain = FIXTURE["domain"] | {"chainId": DOMAIN.chain_id}
    message = {"sessionId": SESSION_ID, "cumulativeAmount": 5510000}
    fields = [
        {"name": "sessionId", "type": "bytes32"},
        {"name": "cumulativeAmount", "type": "uint64"},
    ]
    for changed_domain, changed_fields in (
        (domain | {"name": "Other"}, fields),
        (domain | {"version": "2"}, fields),
        (domain, [fields[0], {"name": "cumulativeAmount", "type": "uint256"}]),
    ):
        wrong = encode_typed_data(changed_domain, {"Voucher": changed_fields}, message)
        signature = bytes(Account.sign_message(wrong, KEY).signature)
        with pytest.raises(ValidationError):
            verify_voucher(DOMAIN, DESCRIPTOR, 5510000, signature)


@pytest.mark.parametrize(
    "key", [bytes(32), bytes(31), bytes(33), ORDER.to_bytes(32, "big"), True, 1, "secret-key"]
)
def test_invalid_private_keys_have_sanitized_errors(key: object) -> None:
    with pytest.raises(ValidationError) as failure:
        LocalSessionSigner(key)  # type: ignore[arg-type]
    assert failure.value.code is ValidationCode.INVALID_VALUE
    assert "secret-key" not in str(failure.value)


def test_signer_representation_and_backend_failures_reveal_no_secret() -> None:
    signer = LocalSessionSigner(KEY)
    assert repr(signer) == f"LocalSessionSigner(address=0x{DESCRIPTOR.session_signer.hex()})"
    assert KEY.hex() not in repr(signer)
    secret = "sentinel-credential-value"
    for method, action in (
        ("from_key", lambda: LocalSessionSigner(KEY)),
        ("sign_message", lambda: signer.sign_close(DOMAIN, SESSION_ID)),
        (
            "recover_message",
            lambda: verify_close_auth(DOMAIN, DESCRIPTOR, raw(CASES["close_auth"]["signature"])),
        ),
    ):
        with patch.object(Account, method, side_effect=ValueError(secret)):
            with pytest.raises(ValidationError) as failure:
                action()
            assert secret not in str(failure.value)
            assert failure.value.__suppress_context__


def test_structured_signing_uses_eip712_not_personal_sign() -> None:
    case = CASES["close_auth"]
    message = SignableMessage(b"\x01", domain_separator(DOMAIN), close_auth_struct_hash(SESSION_ID))
    expected = bytes(Account.sign_message(message, KEY).signature)
    assert (
        LocalSessionSigner(KEY).sign_close(DOMAIN, SESSION_ID) == expected == raw(case["signature"])
    )
