"""Call bytes checked against the published compiler ABI, not SDK encoders.

The independent oracle uses eth-abi 6.0 and the Solidity ABI declaration.
Canonical decoding also rejects surplus words, beyond eth-abi strict padding:
https://eth-abi.readthedocs.io/en/stable/decoding.html#strict-mode
"""

import json
from dataclasses import replace
from pathlib import Path

import pytest
from eth_abi import decode, encode
from eth_utils import keccak
from mppi_protocol.capabilities import FundingAuthorization
from mppi_protocol.contract import (
    ABI,
    ContractCalls,
    OperationKind,
    decode_event,
    decode_result,
    event_topic,
)
from mppi_protocol.errors import ValidationError
from mppi_protocol.types import Domain, SessionDescriptor

PUBLISHED_ABI = json.loads(
    (Path(__file__).resolve().parents[3] / "contracts/abi/MPPI.json").read_text()
)
DESCRIPTOR_TYPE = "(address,address,uint256,address,address,address,bytes32)"
AUTH_TYPE = "(uint256,uint256,bytes32,bytes)"
DOMAIN = Domain(5042002, bytes.fromhex("99" * 20))
TOKEN = bytes.fromhex("3600000000000000000000000000000000000000")
DESCRIPTOR = SessionDescriptor(
    bytes.fromhex("11" * 20),
    bytes.fromhex("12" * 20),
    (1 << 255) + 3,
    bytes.fromhex("22" * 20),
    bytes.fromhex("33" * 20),
    TOKEN,
    bytes.fromhex("44" * 32),
)
DESCRIPTOR_VALUES = (
    DESCRIPTOR.agent,
    DESCRIPTOR.session_signer,
    DESCRIPTOR.provider_id,
    DESCRIPTOR.recipient,
    DESCRIPTOR.operator,
    TOKEN,
    DESCRIPTOR.salt,
)
HASH = bytes.fromhex("66" * 32)
AUTH = FundingAuthorization(
    DOMAIN.chain_id,
    TOKEN,
    "USDC",
    "2",
    DESCRIPTOR.agent,
    DOMAIN.verifying_contract,
    5_510_000,
    1 << 80,
    (1 << 80) + 3600,
    bytes.fromhex("77" * 32),
)
# Opaque smart-account token authorization; deliberately not an MPPI ECDSA proof.
FUNDING_SIGNATURE = bytes(range(81))
VOUCHER_SIGNATURE = b"\x01" * 64 + b"\x1b"


def abi_type(field: dict) -> str:
    value = field["type"]
    if value.startswith("tuple"):
        return "(" + ",".join(abi_type(item) for item in field["components"]) + ")" + value[5:]
    return value


def function_entry(name: str) -> dict:
    return next(
        item for item in PUBLISHED_ABI if item.get("name") == name and item["type"] == "function"
    )


def expected_call(name: str, types: list[str], values: tuple) -> bytes:
    entry = function_entry(name)
    assert [abi_type(field) for field in entry["inputs"]] == types
    assert entry["stateMutability"] == "nonpayable"
    return keccak(text=name + "(" + ",".join(types) + ")")[:4] + encode(types, values)


@pytest.fixture
def calls() -> ContractCalls:
    return ContractCalls(DOMAIN, "USDC", "2")


def test_packaged_application_abi_matches_published_compiler_entries() -> None:
    for entry in ABI:
        assert entry in PUBLISHED_ABI
    expected = {
        "open",
        "topUp",
        "settle",
        "settleBatch",
        "close",
        "requestClose",
        "withdraw",
        "releaseIdle",
    }
    assert expected <= {entry.get("name") for entry in ABI if entry["type"] == "function"}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("kind", OperationKind.TOP_UP),
        ("kind", "open"),
        ("descriptors", (replace(DESCRIPTOR, recipient=b"\xfe" * 20),)),
        ("descriptors", ()),
        ("descriptors", [DESCRIPTOR]),
        ("amounts", (AUTH.value + 1,)),
        ("amounts", [AUTH.value]),
        ("expected_rate_card_hash", b"\xfe" * 32),
    ],
)
def test_prepared_operation_metadata_cannot_relabel_encoded_intent(
    calls: ContractCalls, field: str, value: object
) -> None:
    operation = calls.open(DESCRIPTOR, AUTH.value, HASH, AUTH, FUNDING_SIGNATURE)
    with pytest.raises(ValidationError):
        replace(operation, **{field: value})


def test_prepared_operation_rejects_changed_selector_and_noncanonical_call(
    calls: ContractCalls,
) -> None:
    operation = calls.settle(DESCRIPTOR, 100, 50, VOUCHER_SIGNATURE)
    wrong_selector = keccak(text="close(" + DESCRIPTOR_TYPE + ",uint64,uint64,bytes)")[:4]
    for data in (
        wrong_selector + operation.call.data[4:],
        operation.call.data + bytes(32),
        operation.call.data[:3],
    ):
        with pytest.raises(ValidationError):
            replace(operation, call=replace(operation.call, data=data))
    with pytest.raises(ValidationError):
        replace(operation, amounts=(51,))
    with pytest.raises(ValidationError):
        replace(operation, expected_rate_card_hash=HASH)


def test_prepared_batch_rejects_unequal_encoded_array_lengths(calls: ContractCalls) -> None:
    operation = calls.settle_batch(((DESCRIPTOR, 100, 50, VOUCHER_SIGNATURE),))
    bad = expected_call(
        "settleBatch",
        [DESCRIPTOR_TYPE + "[]", "uint64[]", "uint64[]", "bytes[]"],
        ((DESCRIPTOR_VALUES,), (), (50,), (VOUCHER_SIGNATURE,)),
    )
    with pytest.raises(ValidationError):
        replace(operation, call=replace(operation.call, data=bad))


def test_open_and_top_up_bind_complete_funding_intent(calls: ContractCalls) -> None:
    auth = (AUTH.valid_after, AUTH.valid_before, AUTH.nonce, FUNDING_SIGNATURE)
    opening = calls.open(DESCRIPTOR, AUTH.value, HASH, AUTH, FUNDING_SIGNATURE)
    assert opening.call.data == expected_call(
        "open",
        [DESCRIPTOR_TYPE, "uint64", "bytes32", AUTH_TYPE],
        (DESCRIPTOR_VALUES, AUTH.value, HASH, auth),
    )
    assert opening.kind is OperationKind.OPEN
    assert opening.call.chain_id == DOMAIN.chain_id
    assert opening.call.to == DOMAIN.verifying_contract
    assert opening.expected_rate_card_hash == HASH
    assert opening.descriptors == (DESCRIPTOR,)
    assert opening.amounts == (5_510_000,)
    expected_id = keccak(
        encode(
            [
                "address",
                "address",
                "uint256",
                "address",
                "address",
                "address",
                "bytes32",
                "address",
                "uint256",
            ],
            (*DESCRIPTOR_VALUES, DOMAIN.verifying_contract, DOMAIN.chain_id),
        )
    )
    assert opening.session_ids == (expected_id,)
    top_up = calls.top_up(DESCRIPTOR, AUTH.value, AUTH, FUNDING_SIGNATURE)
    assert top_up.call.data == expected_call(
        "topUp", [DESCRIPTOR_TYPE, "uint64", AUTH_TYPE], (DESCRIPTOR_VALUES, AUTH.value, auth)
    )
    assert top_up.expected_rate_card_hash is None


@pytest.mark.parametrize("name", ["settle", "close"])
def test_capture_preserves_authorization_ceiling_and_actual_capture(
    calls: ContractCalls, name: str
) -> None:
    operation = getattr(calls, name)(DESCRIPTOR, 5_510_000, 5_420_000, VOUCHER_SIGNATURE)
    assert operation.call.data == expected_call(
        name,
        [DESCRIPTOR_TYPE, "uint64", "uint64", "bytes"],
        (DESCRIPTOR_VALUES, 5_510_000, 5_420_000, VOUCHER_SIGNATURE),
    )
    assert operation.amounts == (5_420_000,)
    assert operation.kind.value == name


@pytest.mark.parametrize(
    ("method", "abi_name"),
    [("request_close", "requestClose"), ("withdraw", "withdraw"), ("release_idle", "releaseIdle")],
)
def test_exit_calls_use_complete_descriptor(
    calls: ContractCalls, method: str, abi_name: str
) -> None:
    operation = getattr(calls, method)(DESCRIPTOR)
    assert operation.call.data == expected_call(abi_name, [DESCRIPTOR_TYPE], (DESCRIPTOR_VALUES,))
    assert operation.amounts == ()
    assert operation.descriptors == (DESCRIPTOR,)


def test_batch_keeps_parallel_arrays_and_input_order(calls: ContractCalls) -> None:
    second = replace(DESCRIPTOR, salt=b"\x88" * 32)
    second_values = (*DESCRIPTOR_VALUES[:-1], second.salt)
    operation = calls.settle_batch(
        (
            (DESCRIPTOR, 5_510_000, 5_420_000, VOUCHER_SIGNATURE),
            (second, 100, 7, b"\x02" * 64 + b"\x1c"),
        )
    )
    types = [DESCRIPTOR_TYPE + "[]", "uint64[]", "uint64[]", "bytes[]"]
    values = (
        (DESCRIPTOR_VALUES, second_values),
        (5_510_000, 100),
        (5_420_000, 7),
        (VOUCHER_SIGNATURE, b"\x02" * 64 + b"\x1c"),
    )
    assert operation.call.data == expected_call("settleBatch", types, values)
    assert operation.descriptors == (DESCRIPTOR, second)
    assert operation.amounts == (5_420_000, 7)


def test_empty_and_repeated_session_batches_preserve_contract_semantics(
    calls: ContractCalls,
) -> None:
    types = [DESCRIPTOR_TYPE + "[]", "uint64[]", "uint64[]", "bytes[]"]
    empty = calls.settle_batch(())
    assert empty.call.data == expected_call("settleBatch", types, ((), (), (), ()))
    assert empty.session_ids == ()
    repeated = calls.settle_batch(
        ((DESCRIPTOR, 100, 7, VOUCHER_SIGNATURE), (DESCRIPTOR, 100, 9, VOUCHER_SIGNATURE))
    )
    assert repeated.call.data == expected_call(
        "settleBatch",
        types,
        (
            (DESCRIPTOR_VALUES, DESCRIPTOR_VALUES),
            (100, 100),
            (7, 9),
            (VOUCHER_SIGNATURE, VOUCHER_SIGNATURE),
        ),
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("chain_id", 5042),
        ("token", b"\xaa" * 20),
        ("domain_name", "Other"),
        ("domain_version", "3"),
        ("owner", b"\xbb" * 20),
        ("recipient", b"\xcc" * 20),
        ("value", 5_510_001),
    ],
)
@pytest.mark.parametrize("method", ["open", "top_up"])
def test_mismatched_token_authorization_is_rejected_before_preparation(
    calls: ContractCalls, field: str, value: object, method: str
) -> None:
    wrong = replace(AUTH, **{field: value})
    with pytest.raises(ValidationError):
        if method == "open":
            calls.open(DESCRIPTOR, AUTH.value, HASH, wrong, FUNDING_SIGNATURE)
        else:
            calls.top_up(DESCRIPTOR, AUTH.value, wrong, FUNDING_SIGNATURE)


@pytest.mark.parametrize("amount", [0, -1, True, 1 << 64])
def test_funding_requires_positive_uint64(calls: ContractCalls, amount: int) -> None:
    with pytest.raises(ValidationError):
        calls.open(DESCRIPTOR, amount, HASH, AUTH, FUNDING_SIGNATURE)


def test_uint64_max_amount_and_large_provider_id_stay_exact(calls: ContractCalls) -> None:
    maximum = (1 << 64) - 1
    operation = calls.open(
        DESCRIPTOR, maximum, HASH, replace(AUTH, value=maximum), FUNDING_SIGNATURE
    )
    values = decode([DESCRIPTOR_TYPE, "uint64", "bytes32", AUTH_TYPE], operation.call.data[4:])
    assert values[0][2] == (1 << 255) + 3
    assert values[1] == maximum
    assert values[3][0] == 1 << 80


@pytest.mark.parametrize("signature", [b"", b"\x01" * 65537, bytearray(b"a"), "0x11"])
def test_funding_signature_is_separate_but_nonempty_bounded_bytes(
    calls: ContractCalls, signature: object
) -> None:
    with pytest.raises(ValidationError):
        calls.top_up(DESCRIPTOR, AUTH.value, AUTH, signature)


@pytest.mark.parametrize(
    ("ceiling", "capture", "signature"),
    [
        (1, 2, VOUCHER_SIGNATURE),
        (-1, 0, VOUCHER_SIGNATURE),
        (1 << 64, 0, VOUCHER_SIGNATURE),
        (1, True, VOUCHER_SIGNATURE),
        (1, 1, b""),
        (1, 1, b"x" * 64),
        (1, 1, FUNDING_SIGNATURE),
    ],
)
def test_settle_requires_uint64_coverage_and_fixed_proof_even_for_noop(
    calls: ContractCalls, ceiling: int, capture: int, signature: bytes
) -> None:
    with pytest.raises(ValidationError):
        calls.settle(DESCRIPTOR, ceiling, capture, signature)


def test_refund_only_close_can_have_empty_signature(calls: ContractCalls) -> None:
    operation = calls.close(DESCRIPTOR, 0, 0, b"")
    assert operation.call.data == expected_call(
        "close", [DESCRIPTOR_TYPE, "uint64", "uint64", "bytes"], (DESCRIPTOR_VALUES, 0, 0, b"")
    )


def test_non_arc_token_and_wrong_catalog_width_reject(calls: ContractCalls) -> None:
    with pytest.raises(ValidationError):
        calls.withdraw(replace(DESCRIPTOR, token=b"\xee" * 20))
    with pytest.raises(ValidationError):
        calls.open(DESCRIPTOR, AUTH.value, HASH[:-1], AUTH, FUNDING_SIGNATURE)


def test_result_decode_accepts_exact_output_and_rejects_padding_or_surplus() -> None:
    session = (6_000_000, 5_420_000, 1, 100, 200, HASH)
    encoded = encode(["(uint64,uint64,uint8,uint64,uint64,bytes32)"], [session])
    assert decode_result("getSession", encoded) == (session,)
    for bad in (encoded[:-1], encoded + bytes(32), b"\x01" + encoded[1:]):
        with pytest.raises(ValidationError):
            decode_result("getSession", bad)


def test_event_decode_binds_topics_and_exact_data() -> None:
    declaration = "Settled(bytes32,address,address,uint64,uint64,uint64,uint64)"
    topic = keccak(text=declaration)
    assert event_topic("Settled") == topic
    sid = b"\xab" * 32
    topics = (
        topic,
        sid,
        encode(["address"], [DESCRIPTOR.agent]),
        encode(["address"], [DESCRIPTOR.recipient]),
    )
    data = encode(["uint64"] * 4, [5_510_000, 5_420_000, 20_000, 5_420_000])
    event = decode_event("Settled", topics, data)
    assert event["sessionId"] == sid
    assert event["agent"] == "0x" + DESCRIPTOR.agent.hex()
    assert event["deltaPaid"] == 20_000
    malformed = [
        (topics[:-1], data),
        ((bytes(32), *topics[1:]), data),
        (topics, data + bytes(32)),
        ((topic, sid, b"\x01" + topics[2][1:], topics[3]), data),
    ]
    for bad_topics, bad_data in malformed:
        with pytest.raises(ValidationError):
            decode_event("Settled", bad_topics, bad_data)
