"""Independent ABI/RPC evidence tests; no wallet submission or live RPC.

The stub rejects unplanned calls and requires every state read to use the same
EIP-1898 blockHash plus requireCanonical. Event bytes come from the published
compiler ABI, not the decoder under test. RPC honesty remains a trust assumption.
https://eips.ethereum.org/EIPS/eip-1898#specification
"""

import copy
import json
import time
from dataclasses import replace
from pathlib import Path

import pytest
from eth_abi import encode
from eth_account.messages import encode_typed_data
from eth_utils import keccak
from mppi_protocol.capabilities import FundingAuthorization
from mppi_protocol.catalog import parse_catalog
from mppi_protocol.chain import (
    ChainEvidenceError,
    ChainLagError,
    ChainReader,
    CodePin,
    DeploymentProfile,
    HttpJsonRpc,
)
from mppi_protocol.contract import ContractCalls
from mppi_protocol.errors import ValidationError
from mppi_protocol.http import TransportError
from mppi_protocol.types import (
    ConfirmedSettlementRef,
    Domain,
    OperationState,
    PauseLevel,
    SessionDescriptor,
    SessionStatus,
)

COMPILED_ABI = json.loads(
    (Path(__file__).resolve().parents[3] / "contracts/abi/MPPI.json").read_text()
)
CHAIN = 5042002
PROXY = b"\x99" * 20
IMPLEMENTATION = b"\x98" * 20
REGISTRY = b"\x97" * 20
REGISTRY_IMPLEMENTATION = b"\x96" * 20
TOKEN = bytes.fromhex("3600000000000000000000000000000000000000")
DOMAIN = Domain(CHAIN, PROXY)
DESCRIPTOR = SessionDescriptor(
    b"\x11" * 20, b"\x12" * 20, (1 << 200) + 7, b"\x22" * 20, b"\x33" * 20, TOKEN, b"\x44" * 32
)
RATE_HASH = b"\x66" * 32
TX = b"\xaa" * 32
BLOCK_HASH = b"\xbb" * 32
BLOCK_NUMBER = 100
TIMESTAMP = 1_790_331_000
SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
STATE_TYPE = "(uint64,uint64,uint8,uint64,uint64,bytes32)"
CATALOG = b'[{"input":"2","model":"example/model","output":"3"}]'
SIGNATURE = b"\x01" * 64 + b"\x1b"


def hx(value: bytes) -> str:
    return "0x" + value.hex()


def abi_call(signature: str, types: list[str], values: list) -> str:
    return hx(keccak(text=signature)[:4] + encode(types, values))


def descriptor_id(descriptor: SessionDescriptor) -> bytes:
    return keccak(
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
            [
                descriptor.agent,
                descriptor.session_signer,
                descriptor.provider_id,
                descriptor.recipient,
                descriptor.operator,
                descriptor.token,
                descriptor.salt,
                PROXY,
                CHAIN,
            ],
        )
    )


def event_log(name: str, values: dict, *, emitter: bytes = PROXY, index: int = 0) -> dict:
    event = next(item for item in COMPILED_ABI if item["type"] == "event" and item["name"] == name)
    fields = event["inputs"]
    signature = name + "(" + ",".join(item["type"] for item in fields) + ")"
    topics = [hx(keccak(text=signature))]
    topics.extend(
        hx(encode([item["type"]], [values[item["name"]]])) for item in fields if item["indexed"]
    )
    ordinary = [item for item in fields if not item["indexed"]]
    return {
        "address": hx(emitter),
        "topics": topics,
        "data": hx(
            encode([item["type"] for item in ordinary], [values[item["name"]] for item in ordinary])
        ),
        "logIndex": hex(index),
        "transactionIndex": "0x2",
        "transactionHash": hx(TX),
        "blockNumber": hex(BLOCK_NUMBER),
        "blockHash": hx(BLOCK_HASH),
        "removed": False,
    }


def opened(descriptor: SessionDescriptor = DESCRIPTOR, **changes: object) -> dict:
    return {
        "sessionId": descriptor_id(descriptor),
        "agent": descriptor.agent,
        "sessionSigner": descriptor.session_signer,
        "providerId": descriptor.provider_id,
        "recipient": descriptor.recipient,
        "operator": descriptor.operator,
        "token": TOKEN,
        "salt": descriptor.salt,
        "deposit": 5_510_000,
        "initialRateCardHash": RATE_HASH,
    } | changes


def settled(descriptor: SessionDescriptor = DESCRIPTOR, **changes: object) -> dict:
    return {
        "sessionId": descriptor_id(descriptor),
        "agent": descriptor.agent,
        "recipient": descriptor.recipient,
        "cumulativeAmount": 5_510_000,
        "captureAmount": 5_420_000,
        "deltaPaid": 5_420_000,
        "newSettled": 5_420_000,
    } | changes


class EvidenceRpc:
    """Finite expected state, strict method/target/calldata/block routing."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list]] = []
        self.chain = hex(CHAIN)
        self.block = {
            "number": hex(BLOCK_NUMBER),
            "hash": hx(BLOCK_HASH),
            "parentHash": hx(b"\xbc" * 32),
            "timestamp": hex(TIMESTAMP),
        }
        self.receipt: dict | None = {
            "transactionHash": hx(TX),
            "transactionIndex": "0x2",
            "status": "0x1",
            "blockNumber": hex(BLOCK_NUMBER),
            "blockHash": hx(BLOCK_HASH),
            "logs": [],
        }
        self.responses: dict[tuple[str, str], str] = {}
        self.code = {
            hx(address): hx(bytes([i]) + b"test-runtime")
            for i, address in enumerate(
                (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION), start=1
            )
        }
        self.slots = {
            hx(PROXY): hx(bytes(12) + IMPLEMENTATION),
            hx(REGISTRY): hx(bytes(12) + REGISTRY_IMPLEMENTATION),
        }
        self.block_reads = 0
        self.reorg_after: int | None = None
        self.failure: Exception | None = None

    def call(self, method: str, params: list[object]) -> object:
        self.calls.append((method, copy.deepcopy(params)))
        if self.failure is not None:
            raise self.failure
        if method == "eth_chainId":
            assert params == []
            return self.chain
        if method == "eth_getTransactionReceipt":
            assert params == [hx(TX)]
            return copy.deepcopy(self.receipt)
        if method == "eth_getBlockByNumber":
            assert params in (["latest", False], [hex(BLOCK_NUMBER), False])
            self.block_reads += 1
            result = copy.deepcopy(self.block)
            if self.reorg_after is not None and self.block_reads > self.reorg_after:
                result["hash"] = hx(b"\xdd" * 32)
            return result
        assert params[-1] == {"blockHash": hx(BLOCK_HASH), "requireCanonical": True}
        if method == "eth_call":
            assert len(params) == 2 and set(params[0]) == {"to", "data"}
            return self.responses[(params[0]["to"], params[0]["data"])]
        if method == "eth_getCode":
            assert len(params) == 2
            return self.code[params[0]]
        if method == "eth_getStorageAt":
            assert len(params) == 3 and params[1] == SLOT
            return self.slots[params[0]]
        raise AssertionError(f"unexpected RPC method: {method}")

    def response(
        self,
        address: bytes,
        signature: str,
        inputs: list[str],
        arguments: list,
        outputs: list[str],
        values: list,
    ) -> None:
        self.responses[(hx(address), abi_call(signature, inputs, arguments))] = hx(
            encode(outputs, values)
        )

    def session(
        self,
        descriptor: SessionDescriptor = DESCRIPTOR,
        *,
        deposit: int = 6_000_000,
        paid: int = 0,
        status: int = 0,
        closing: int = 0,
        activity: int = TIMESTAMP,
        rate_hash: bytes = RATE_HASH,
    ) -> None:
        self.response(
            PROXY,
            "getSession(bytes32)",
            ["bytes32"],
            [descriptor_id(descriptor)],
            [STATE_TYPE],
            [(deposit, paid, status, closing, activity, rate_hash)],
        )

    def logs(self, *logs: dict) -> None:
        assert self.receipt is not None
        self.receipt["logs"] = list(logs)


@pytest.fixture
def context() -> tuple[ChainReader, EvidenceRpc, ContractCalls]:
    rpc = EvidenceRpc()
    pins = [
        CodePin(address, keccak(bytes.fromhex(rpc.code[hx(address)][2:])))
        for address in (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION)
    ]
    profile = DeploymentProfile(DOMAIN, REGISTRY, *pins)
    rpc.session()
    return ChainReader(rpc, profile), rpc, ContractCalls(DOMAIN, "USDC", "2")


def opening(calls: ContractCalls):
    auth = FundingAuthorization(
        CHAIN,
        TOKEN,
        "USDC",
        "2",
        DESCRIPTOR.agent,
        PROXY,
        5_510_000,
        TIMESTAMP - 1,
        TIMESTAMP + 3600,
        b"\x77" * 32,
    )
    return calls.open(DESCRIPTOR, 5_510_000, RATE_HASH, auth, b"smart-account-token-proof")


def reconcile(reader: ChainReader, operation):
    return reader.reconcile(
        operation, operation_ref="intent-17", tx_hash=TX, wallet_operation_id="wallet-23"
    )


def test_open_requires_actual_proxy_event_and_matching_state(context) -> None:
    reader, rpc, calls = context
    rpc.logs(event_log("SessionOpened", opened()))
    result = reconcile(reader, opening(calls))
    assert result.state is OperationState.CONFIRMED
    assert result.operation_ref == "intent-17" and result.wallet_operation_id == "wallet-23"
    assert result.tx_hash == TX and result.observation.block_hash == BLOCK_HASH
    assert result.sessions[0].session_id == descriptor_id(DESCRIPTOR)
    assert result.sessions[0].settlement_reference is None
    assert rpc.block_reads == 2


def test_opening_retains_exact_catalog_despite_later_same_block_update(context) -> None:
    reader, rpc, _ = context
    accepted = parse_catalog(CATALOG)
    changed = parse_catalog(b'[{"input":"9","model":"example/model","output":"10"}]')
    rpc.session(rate_hash=accepted.rate_card_hash)
    rpc.logs(event_log("SessionOpened", opened(initialRateCardHash=accepted.rate_card_hash)))
    provider = DESCRIPTOR.provider_id
    for name, output, value in (
        ("ownerOf", "address", DESCRIPTOR.operator),
        ("getAgentWallet", "address", DESCRIPTOR.recipient),
        ("tokenURI", "string", "https://provider.example/discover"),
    ):
        rpc.response(REGISTRY, name + "(uint256)", ["uint256"], [provider], [output], [value])
    rpc.response(
        REGISTRY,
        "getMetadata(uint256,string)",
        ["uint256", "string"],
        [provider, "mppi.catalog"],
        ["bytes"],
        [changed.canonical_bytes],
    )
    # The end-of-block registry version differs; retained accepted bytes remain
    # sufficient because the actual opening event and state commit to their hash.
    evidence = reader.opening(TX, descriptor_id(DESCRIPTOR), accepted_catalog=accepted)
    assert evidence.initial_catalog == accepted and evidence.descriptor == DESCRIPTOR
    assert reader.identity(provider, reader.block()).catalog == changed
    with pytest.raises(ChainEvidenceError):
        reader.opening(TX, descriptor_id(DESCRIPTOR))
    with pytest.raises(ChainEvidenceError):
        reader.opening(TX, descriptor_id(DESCRIPTOR), accepted_catalog=changed)
    # Conversely a publication before opening binds the newly published version.
    rpc.session(rate_hash=changed.rate_card_hash)
    rpc.logs(event_log("SessionOpened", opened(initialRateCardHash=changed.rate_card_hash)))
    with pytest.raises(ChainEvidenceError):
        reader.opening(TX, descriptor_id(DESCRIPTOR), accepted_catalog=accepted)
    assert (
        reader.opening(TX, descriptor_id(DESCRIPTOR), accepted_catalog=changed).initial_catalog
        == changed
    )


@pytest.mark.parametrize(
    "mutation", ["duplicate", "reverted", "removed", "wrong-block", "no-event"]
)
def test_opening_notice_requires_unambiguous_canonical_success(context, mutation) -> None:
    reader, rpc, _ = context
    catalog = parse_catalog(CATALOG)
    rpc.session(rate_hash=catalog.rate_card_hash)
    log = event_log("SessionOpened", opened(initialRateCardHash=catalog.rate_card_hash))
    rpc.logs(log)
    if mutation == "duplicate":
        rpc.logs(
            log,
            event_log("SessionOpened", opened(initialRateCardHash=catalog.rate_card_hash), index=1),
        )
    elif mutation == "reverted":
        rpc.receipt["status"] = "0x0"
    elif mutation == "removed":
        log["removed"] = True
    elif mutation == "wrong-block":
        rpc.receipt["blockHash"] = hx(b"x" * 32)
    else:
        rpc.logs()
    with pytest.raises(ChainEvidenceError):
        reader.opening(TX, descriptor_id(DESCRIPTOR), accepted_catalog=catalog)


def test_close_result_and_settlement_reference_require_event_state_agreement(context) -> None:
    reader, rpc, _ = context
    sid = descriptor_id(DESCRIPTOR)
    rpc.session(paid=5_420_000)
    rpc.logs(event_log("Settled", settled()))
    reference = ConfirmedSettlementRef(reader.block().observation, TX, sid, 5_420_000, 5_420_000)
    reader.verify_settlement(reference, DESCRIPTOR)
    with pytest.raises(ChainEvidenceError):
        reader.verify_settlement(replace(reference, delta=1), DESCRIPTOR)
    rpc.session(paid=5_420_000, status=2)
    rpc.logs(
        event_log(
            "SessionClosed", {"sessionId": sid, "captureAmount": 5_420_000, "refunded": 580_000}
        )
    )
    evidence = reader.closing(TX, sid)
    assert evidence.capture_amount == 5_420_000 and evidence.refunded == 580_000
    rpc.session(paid=5_420_000, status=0)
    with pytest.raises(ChainEvidenceError):
        reader.closing(TX, sid)


@pytest.mark.parametrize("mode", ["inner-wallet-failed", "spoof-emitter", "different-session"])
def test_outer_wallet_success_does_not_prove_open(context, mode: str) -> None:
    reader, rpc, calls = context
    if mode == "spoof-emitter":
        rpc.logs(event_log("SessionOpened", opened(), emitter=b"\xef" * 20))
    elif mode == "different-session":
        rpc.logs(event_log("SessionOpened", opened(sessionId=b"\xef" * 32)))
    result = reconcile(reader, opening(calls))
    assert result.state is OperationState.UNKNOWN
    assert result.sessions[0].settlement_reference is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("agent", b"\xfe" * 20),
        ("sessionSigner", b"\xfe" * 20),
        ("providerId", 9),
        ("recipient", b"\xfe" * 20),
        ("operator", b"\xfe" * 20),
        ("token", b"\xfe" * 20),
        ("salt", b"\xfe" * 32),
        ("deposit", 5_510_001),
        ("initialRateCardHash", b"\xfe" * 32),
    ],
)
def test_open_event_binds_every_descriptor_and_funding_field(
    context, field: str, value: object
) -> None:
    reader, rpc, calls = context
    rpc.logs(event_log("SessionOpened", opened(**{field: value})))
    assert reconcile(reader, opening(calls)).state is OperationState.UNKNOWN


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("transactionHash", hx(b"\xee" * 32)),
        ("blockHash", hx(b"\xee" * 32)),
        ("status", "0x2"),
        ("status", "0x01"),
    ],
)
def test_receipt_transaction_block_and_status_binding(context, field: str, value: str) -> None:
    reader, rpc, calls = context
    rpc.receipt[field] = value
    rpc.logs(event_log("SessionOpened", opened()))
    assert reconcile(reader, opening(calls)).state is OperationState.UNKNOWN


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("transactionHash", hx(b"\xee" * 32)),
        ("blockHash", hx(b"\xee" * 32)),
        ("transactionIndex", "0x3"),
        ("blockNumber", "0x65"),
        ("removed", True),
        ("removed", None),
        ("logIndex", "0x00"),
    ],
)
def test_log_inclusion_is_bound_to_receipt(context, field: str, value: object) -> None:
    reader, rpc, calls = context
    log = event_log("SessionOpened", opened())
    log[field] = value
    rpc.logs(log)
    assert reconcile(reader, opening(calls)).state is OperationState.UNKNOWN


def test_duplicate_or_unordered_operation_events_remain_ambiguous(context) -> None:
    reader, rpc, calls = context
    first = event_log("SessionOpened", opened())
    rpc.logs(first, event_log("SessionOpened", opened(), index=1))
    assert reconcile(reader, opening(calls)).state is OperationState.UNKNOWN
    rpc.logs(first, first)
    assert reconcile(reader, opening(calls)).state is OperationState.UNKNOWN


def test_receipt_absence_ambiguity_and_revert_are_distinct(context) -> None:
    reader, rpc, calls = context
    operation = opening(calls)
    missing = reader.reconcile(operation, operation_ref="not-submitted", tx_hash=None)
    assert missing.state is OperationState.UNKNOWN and rpc.calls == []
    original = rpc.receipt
    rpc.receipt = None
    pending = reconcile(reader, operation)
    assert pending.state is OperationState.PENDING and pending.observation is None
    rpc.receipt = original
    rpc.receipt["status"] = "0x0"
    failed = reconcile(reader, operation)
    assert failed.state is OperationState.FAILED and failed.observation.block_hash == BLOCK_HASH


@pytest.mark.parametrize("status", ["0x0", "0x1"])
def test_reorg_during_observation_never_produces_definitive_result(context, status: str) -> None:
    reader, rpc, calls = context
    rpc.receipt["status"] = status
    rpc.logs(event_log("SessionOpened", opened()))
    rpc.reorg_after = 1
    assert reconcile(reader, opening(calls)).state is OperationState.UNKNOWN


def test_chain_mismatch_and_transport_failure_are_not_payment_failures(context) -> None:
    reader, rpc, calls = context
    rpc.chain = hex(5042)
    assert reconcile(reader, opening(calls)).state is OperationState.UNKNOWN
    with pytest.raises(ChainEvidenceError):
        reader.block()
    rpc.chain = hex(CHAIN)
    rpc.failure = TransportError("unavailable")
    assert reconcile(reader, opening(calls)).state is OperationState.UNKNOWN
    wrong = replace(opening(calls), call=replace(opening(calls).call, to=b"\xfa" * 20))
    with pytest.raises(ChainEvidenceError):
        reconcile(reader, wrong)


def test_same_block_later_session_activity_does_not_invalidate_open(context) -> None:
    reader, rpc, calls = context
    rpc.session(deposit=7_000_000, paid=5_500_000, status=2)
    rpc.logs(event_log("SessionOpened", opened()))
    assert reconcile(reader, opening(calls)).state is OperationState.CONFIRMED


def test_settlement_credits_only_receipt_delta_not_later_same_block_state(context) -> None:
    reader, rpc, calls = context
    rpc.session(paid=5_500_000)
    rpc.logs(event_log("Settled", settled(deltaPaid=420_000)))
    result = reconcile(reader, calls.settle(DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE))
    assert result.state is OperationState.CONFIRMED
    outcome = result.sessions[0]
    assert outcome.target_covered is True
    assert outcome.settlement_reference.delta == 420_000
    assert outcome.settlement_reference.settled_cumulative == 5_420_000
    assert outcome.settlement_reference.tx_hash == TX


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("covered", [False, True])
def test_silent_noop_skipped_batch_and_inner_failure_never_get_attribution(
    context, batch: bool, covered: bool
) -> None:
    reader, rpc, calls = context
    rpc.session(paid=5_420_000 if covered else 0)
    item = (DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE)
    operation = calls.settle_batch((item,)) if batch else calls.settle(*item)
    result = reconcile(reader, operation)
    assert result.state is OperationState.UNKNOWN
    assert result.sessions[0].target_covered is covered
    assert result.sessions[0].settlement_reference is None


def test_mixed_batch_confirms_only_attributable_item(context) -> None:
    reader, rpc, calls = context
    second = replace(DESCRIPTOR, salt=b"\x55" * 32)
    rpc.session(paid=5_420_000)
    rpc.session(second, paid=2)
    rpc.logs(event_log("Settled", settled()))
    operation = calls.settle_batch(
        ((DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE), (second, 10, 2, SIGNATURE))
    )
    result = reconcile(reader, operation)
    assert result.state is OperationState.CONFIRMED
    assert result.sessions[0].settlement_reference.delta == 5_420_000
    assert result.sessions[1].target_covered is True
    assert result.sessions[1].settlement_reference is None


def test_empty_batch_and_duplicate_input_evidence_remain_unattributed(context) -> None:
    reader, rpc, calls = context
    assert reconcile(reader, calls.settle_batch(())).state is OperationState.UNKNOWN
    rpc.session(paid=5_420_000)
    rpc.logs(event_log("Settled", settled()))
    item = (DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE)
    result = reconcile(reader, calls.settle_batch((item, item)))
    assert result.state is OperationState.UNKNOWN
    assert len(result.sessions) == 2
    assert all(outcome.target_covered for outcome in result.sessions)
    assert not any(outcome.settlement_reference for outcome in result.sessions)


def test_repeated_batch_session_distinct_targets_keep_order_and_unique_payments(context) -> None:
    reader, rpc, calls = context
    rpc.session(paid=5_420_000)
    rpc.logs(
        event_log(
            "Settled", settled(captureAmount=4_000_000, newSettled=4_000_000, deltaPaid=4_000_000)
        ),
        event_log("Settled", settled(deltaPaid=1_420_000), index=1),
    )
    operation = calls.settle_batch(
        (
            (DESCRIPTOR, 5_510_000, 4_000_000, SIGNATURE),
            (DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE),
        )
    )
    result = reconcile(reader, operation)
    assert result.state is OperationState.CONFIRMED
    assert [outcome.target for outcome in result.sessions] == [4_000_000, 5_420_000]
    refs = [outcome.settlement_reference for outcome in result.sessions]
    assert [ref.delta for ref in refs] == [4_000_000, 1_420_000]
    for ref in refs:
        reader.verify_settlement(ref, DESCRIPTOR)


def test_batch_ambiguous_duplicate_does_not_hide_an_independent_confirmed_item(context) -> None:
    reader, rpc, calls = context
    other = replace(DESCRIPTOR, salt=b"x" * 32)
    rpc.session(paid=5_420_000)
    rpc.session(other, paid=2)
    rpc.logs(
        event_log("Settled", settled()),
        event_log(
            "Settled",
            settled(other, cumulativeAmount=10, captureAmount=2, deltaPaid=2, newSettled=2),
            index=1,
        ),
    )
    item = (DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE)
    result = reconcile(reader, calls.settle_batch((item, (other, 10, 2, SIGNATURE), item)))
    assert result.state is OperationState.CONFIRMED
    assert [x.target_covered for x in result.sessions] == [True, True, True]
    assert result.sessions[0].settlement_reference is None
    assert result.sessions[1].settlement_reference.delta == 2
    assert result.sessions[2].settlement_reference is None


def test_paid_state_and_opening_catalog_cannot_contradict_events(context) -> None:
    reader, rpc, calls = context
    rpc.logs(event_log("Settled", settled()))
    rpc.session(paid=5_419_999)
    assert (
        reconcile(reader, calls.settle(DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE)).state
        is OperationState.UNKNOWN
    )
    rpc.logs(event_log("SessionOpened", opened()))
    rpc.session(rate_hash=b"\xfe" * 32)
    assert reconcile(reader, opening(calls)).state is OperationState.UNKNOWN


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("agent", b"\xee" * 20),
        ("recipient", b"\xee" * 20),
        ("captureAmount", 5_420_001),
        ("newSettled", 5_420_001),
        ("cumulativeAmount", 5_419_999),
        ("deltaPaid", 0),
        ("deltaPaid", 5_420_001),
    ],
)
def test_settlement_event_must_match_target_and_descriptor(
    context, field: str, value: object
) -> None:
    reader, rpc, calls = context
    rpc.session(paid=5_420_000)
    rpc.logs(event_log("Settled", settled(**{field: value})))
    assert (
        reconcile(reader, calls.settle(DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE)).state
        is OperationState.UNKNOWN
    )


@pytest.mark.parametrize("kind", ["top_up", "request_close", "close", "withdraw", "release_idle"])
def test_each_remaining_operation_requires_its_own_event_and_state(context, kind: str) -> None:
    reader, rpc, calls = context
    sid = descriptor_id(DESCRIPTOR)
    if kind == "top_up":
        auth = FundingAuthorization(
            CHAIN,
            TOKEN,
            "USDC",
            "2",
            DESCRIPTOR.agent,
            PROXY,
            490_000,
            TIMESTAMP - 1,
            TIMESTAMP + 100,
            b"\x77" * 32,
        )
        operation = calls.top_up(DESCRIPTOR, 490_000, auth, b"smart-account-proof")
        name, values = "ToppedUp", {"sessionId": sid, "amount": 490_000, "newDeposit": 6_000_000}
    elif kind == "request_close":
        operation = calls.request_close(DESCRIPTOR)
        rpc.session(status=1, closing=TIMESTAMP)
        name, values = "CloseRequested", {"sessionId": sid, "graceEnd": TIMESTAMP + 3600}
    else:
        rpc.session(status=2, paid=5_420_000)
        if kind == "close":
            operation = calls.close(DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE)
        else:
            operation = getattr(calls, kind)(DESCRIPTOR)
        if kind == "release_idle":
            name, values = "IdleReleased", {"sessionId": sid, "amount": 580_000}
        else:
            name, values = (
                "SessionClosed",
                {"sessionId": sid, "captureAmount": 5_420_000, "refunded": 580_000},
            )
    rpc.logs(event_log(name, values))
    assert reconcile(reader, operation).state is OperationState.CONFIRMED
    rpc.logs()
    assert reconcile(reader, operation).state is OperationState.UNKNOWN


def install_domain_response(rpc: EvidenceRpc) -> None:
    typed = encode_typed_data(
        {"name": "MPPI", "version": "1", "chainId": CHAIN, "verifyingContract": hx(PROXY)},
        {
            "Voucher": [
                {"name": "sessionId", "type": "bytes32"},
                {"name": "cumulativeAmount", "type": "uint64"},
            ]
        },
        {"sessionId": bytes(32), "cumulativeAmount": 0},
    )
    digest = keccak(b"\x19" + typed.version + typed.header + typed.body)
    rpc.response(
        PROXY,
        "voucherDigest(bytes32,uint64)",
        ["bytes32", "uint64"],
        [bytes(32), 0],
        ["bytes32"],
        [digest],
    )


def test_profile_checks_every_runtime_slot_and_domain_at_one_canonical_hash(context) -> None:
    reader, rpc, _ = context
    install_domain_response(rpc)
    reader.verify_deployment(reader.block())
    assert {params[0] for method, params in rpc.calls if method == "eth_getCode"} == set(rpc.code)
    assert {params[0] for method, params in rpc.calls if method == "eth_getStorageAt"} == set(
        rpc.slots
    )
    assert rpc.block_reads == 2


@pytest.mark.parametrize("address", [PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION])
def test_each_code_pin_is_enforced(context, address: bytes) -> None:
    reader, rpc, _ = context
    install_domain_response(rpc)
    rpc.code[hx(address)] = "0x60006000"
    with pytest.raises(ChainEvidenceError, match="code differs"):
        reader.verify_deployment(reader.block())


@pytest.mark.parametrize("address", [PROXY, REGISTRY])
def test_both_implementation_slots_are_enforced(context, address: bytes) -> None:
    reader, rpc, _ = context
    install_domain_response(rpc)
    rpc.slots[hx(address)] = hx(bytes(12) + b"\xee" * 20)
    with pytest.raises(ChainEvidenceError, match="implementation differs"):
        reader.verify_deployment(reader.block())


def test_domain_and_post_read_canonicality_are_enforced(context) -> None:
    reader, rpc, _ = context
    rpc.response(
        PROXY,
        "voucherDigest(bytes32,uint64)",
        ["bytes32", "uint64"],
        [bytes(32), 0],
        ["bytes32"],
        [bytes(32)],
    )
    with pytest.raises(ChainEvidenceError, match="signing domain"):
        reader.verify_deployment(reader.block())
    install_domain_response(rpc)
    rpc.block_reads = 0
    rpc.reorg_after = 1
    with pytest.raises(ChainEvidenceError, match="changed"):
        reader.verify_deployment(reader.block())


def test_registry_identity_uses_exact_provider_id_key_and_pinned_block(context) -> None:
    reader, rpc, _ = context
    provider = DESCRIPTOR.provider_id
    rpc.response(
        REGISTRY, "ownerOf(uint256)", ["uint256"], [provider], ["address"], [DESCRIPTOR.operator]
    )
    rpc.response(
        REGISTRY,
        "getAgentWallet(uint256)",
        ["uint256"],
        [provider],
        ["address"],
        [DESCRIPTOR.recipient],
    )
    rpc.response(
        REGISTRY,
        "tokenURI(uint256)",
        ["uint256"],
        [provider],
        ["string"],
        ["https://provider.example/discover"],
    )
    rpc.response(
        REGISTRY,
        "getMetadata(uint256,string)",
        ["uint256", "string"],
        [provider, "mppi.catalog"],
        ["bytes"],
        [CATALOG],
    )
    identity = reader.identity(provider, reader.block())
    assert identity.provider_id == provider
    assert identity.owner == DESCRIPTOR.operator and identity.recipient == DESCRIPTOR.recipient
    assert identity.agent_uri == "https://provider.example/discover"
    assert identity.catalog.canonical_bytes == CATALOG
    assert identity.catalog.rate_card_hash == keccak(CATALOG)
    assert identity.registry_address == REGISTRY
    assert identity.observation.block_hash == BLOCK_HASH
    assert len([call for call in rpc.calls if call[0] == "eth_call"]) == 4


@pytest.mark.parametrize(
    "state",
    [
        (1, 2, 0, 0, TIMESTAMP, RATE_HASH),
        (1, 0, 3, 0, TIMESTAMP, RATE_HASH),
        (0, 0, 0, 0, TIMESTAMP, bytes(32)),
        (0, 0, 0, 0, 0, RATE_HASH),
    ],
)
def test_inconsistent_session_state_is_not_usable(context, state: tuple) -> None:
    reader, rpc, _ = context
    rpc.response(
        PROXY,
        "getSession(bytes32)",
        ["bytes32"],
        [descriptor_id(DESCRIPTOR)],
        [STATE_TYPE],
        [state],
    )
    with pytest.raises(ChainEvidenceError):
        reader.session(descriptor_id(DESCRIPTOR), reader.block())


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(0, SessionStatus.OPEN), (1, SessionStatus.CLOSING), (2, SessionStatus.CLOSED)],
)
def test_solidity_session_enum_is_mapped_explicitly(
    context, raw: int, expected: SessionStatus
) -> None:
    reader, rpc, _ = context
    rpc.session(status=raw)
    assert reader.session(descriptor_id(DESCRIPTOR), reader.block()).status is expected


def test_zero_storage_is_missing_session_and_pause_enum_is_checked(context) -> None:
    reader, rpc, _ = context
    rpc.session(deposit=0, activity=0, rate_hash=bytes(32))
    assert not reader.session(descriptor_id(DESCRIPTOR), reader.block()).exists
    for value in range(3):
        rpc.response(PROXY, "pauseLevel()", [], [], ["uint8"], [value])
        assert reader.pause_level(reader.block()) is PauseLevel(value)
    rpc.response(PROXY, "pauseLevel()", [], [], ["uint8"], [3])
    with pytest.raises(ChainEvidenceError):
        reader.pause_level(reader.block())


def test_profile_and_block_cannot_silently_switch_network_or_contract(context) -> None:
    reader, _, _ = context
    with pytest.raises(ValidationError):
        replace(reader.profile, domain=Domain(1, PROXY))
    with pytest.raises(ValidationError):
        replace(reader.profile, registry_address=PROXY)
    block = reader.block()
    wrong = replace(block, observation=replace(block.observation, contract_address=REGISTRY))
    with pytest.raises(ChainEvidenceError):
        reader.session(descriptor_id(DESCRIPTOR), wrong)


class EchoTransport:
    def __init__(self, mode: str = "valid") -> None:
        self.mode = mode
        self.calls: list[dict] = []

    def request(self, endpoint: str, *, body: bytes) -> bytes:
        assert endpoint == "https://rpc.example/private-api-key"
        request = json.loads(body)
        self.calls.append(request)
        response = {"jsonrpc": "2.0", "id": request["id"], "result": hex(CHAIN)}
        if self.mode == "id":
            response["id"] = "wrong-request"
        elif self.mode == "version":
            response["jsonrpc"] = "1.0"
        elif self.mode == "error":
            response["error"] = {"message": "private-api-key"}
        elif self.mode == "missing":
            del response["result"]
        return json.dumps(response).encode()


def test_http_rpc_is_read_only_and_binds_response_identity() -> None:
    transport = EchoTransport()
    rpc = HttpJsonRpc("https://rpc.example/private-api-key", transport)
    assert rpc.call("eth_chainId", []) == hex(CHAIN)
    assert transport.calls[0]["method"] == "eth_chainId"
    with pytest.raises(ChainEvidenceError):
        rpc.call("eth_sendRawTransaction", ["0x00"])
    assert len(transport.calls) == 1
    assert "private-api-key" not in repr(rpc)


@pytest.mark.parametrize("mode", ["id", "version", "error", "missing"])
def test_rpc_malformed_response_never_echoes_remote_secrets(mode: str) -> None:
    rpc = HttpJsonRpc("https://rpc.example/private-api-key", EchoTransport(mode))
    with pytest.raises(ChainEvidenceError) as caught:
        rpc.call("eth_chainId", [])
    assert "private-api-key" not in str(caught.value)


@pytest.mark.parametrize("raw", [b"", b"[]", b"not-json", b'[{"input":2}]'])
def test_catalog_unavailable_can_preserve_verified_identity_but_not_active_offering(context, raw):
    reader, rpc, _ = context
    provider = DESCRIPTOR.provider_id
    for name, output, value in (
        ("ownerOf", "address", DESCRIPTOR.operator),
        ("getAgentWallet", "address", DESCRIPTOR.recipient),
        ("tokenURI", "string", "https://provider.example/discover"),
    ):
        rpc.response(REGISTRY, name + "(uint256)", ["uint256"], [provider], [output], [value])
    rpc.response(
        REGISTRY,
        "getMetadata(uint256,string)",
        ["uint256", "string"],
        [provider, "mppi.catalog"],
        ["bytes"],
        [raw],
    )
    block = reader.block()
    with pytest.raises(ValidationError):
        reader.identity(provider, block)
    identity = reader.identity(provider, block, allow_unavailable_catalog=True)
    assert identity.catalog is None
    assert identity.owner == DESCRIPTOR.operator
    assert identity.observation == block.observation
    rpc.failure = TransportError("offline")
    with pytest.raises(TransportError):
        reader.identity(provider, block, allow_unavailable_catalog=True)


def simulation_context(context, monkeypatch, *, estimate=80000, result="0x"):
    reader, rpc, calls = context
    operation = calls.settle_batch(((DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE),))
    install_domain_response(rpc)
    rpc.block["gasLimit"] = hex(30_000_000)
    original = rpc.call
    expected = {
        "from": hx(DESCRIPTOR.operator),
        "to": hx(PROXY),
        "data": hx(operation.call.data),
        "value": "0x0",
        "gas": hex(30_000_000),
    }

    def simulate(method, params):
        if method == "eth_estimateGas":
            rpc.calls.append((method, copy.deepcopy(params)))
            assert params == [expected, hex(BLOCK_NUMBER)]
            return hex(estimate)
        if method == "eth_call" and "from" in params[0]:
            rpc.calls.append((method, copy.deepcopy(params)))
            assert params == [expected, {"blockHash": hx(BLOCK_HASH), "requireCanonical": True}]
            return result
        return original(method, params)

    monkeypatch.setattr(rpc, "call", simulate)
    return reader, rpc, operation


def test_batch_simulation_binds_sender_calldata_canonical_block_and_cost(context, monkeypatch):
    reader, rpc, operation = simulation_context(context, monkeypatch)
    evidence = reader.simulate(operation, sender=DESCRIPTOR.operator)
    assert evidence.block.observation.block_hash == BLOCK_HASH
    assert evidence.gas_estimate == 80000 and evidence.gas_limit == 30_000_000
    assert sum(method == "eth_estimateGas" for method, _ in rpc.calls) == 1
    assert all(not method.startswith("eth_send") for method, _ in rpc.calls)


@pytest.mark.parametrize("estimate", [0, 30_000_001])
def test_batch_simulation_rejects_invalid_cost_before_submission(context, monkeypatch, estimate):
    reader, _, operation = simulation_context(context, monkeypatch, estimate=estimate)
    with pytest.raises(ChainEvidenceError, match="estimate"):
        reader.simulate(operation, sender=DESCRIPTOR.operator)


def test_batch_simulation_rejects_nonempty_return_and_changed_block(context, monkeypatch):
    reader, _, operation = simulation_context(context, monkeypatch, result="0x01")
    with pytest.raises(ChainEvidenceError, match="unexpected data"):
        reader.simulate(operation, sender=DESCRIPTOR.operator)
    monkeypatch.undo()
    reader, rpc, operation = simulation_context(context, monkeypatch)
    rpc.block_reads = 0
    rpc.reorg_after = 3
    with pytest.raises(ChainEvidenceError, match="changed"):
        reader.simulate(operation, sender=DESCRIPTOR.operator)


class RateLimitedTransport(EchoTransport):
    def __init__(self, codes: list[int]) -> None:
        super().__init__()
        self.codes = codes

    def request(self, endpoint: str, *, body: bytes) -> bytes:
        if not self.codes:
            return super().request(endpoint, body=body)
        request = json.loads(body)
        self.calls.append(request)
        error = {"code": self.codes.pop(0), "message": "private-api-key"}
        return json.dumps({"jsonrpc": "2.0", "id": request["id"], "error": error}).encode()


def test_rpc_retries_json_rpc_rate_limit_codes_only(monkeypatch) -> None:
    delays: list[float] = []
    monkeypatch.setattr(time, "sleep", delays.append)
    # EIP-1474 -32005 "Limit exceeded"; QuickNode's -32007 per-second limit.
    transport = RateLimitedTransport([-32005, -32007])
    rpc = HttpJsonRpc("https://rpc.example/private-api-key", transport)
    assert rpc.call("eth_chainId", []) == hex(CHAIN)
    assert len(transport.calls) == 3 and delays == [0.25, 1.0]
    for code in (3, -32000, -32602):  # Revert and invalid input are not retried.
        transport = RateLimitedTransport([code])
        with pytest.raises(ChainEvidenceError) as caught:
            HttpJsonRpc("https://rpc.example/private-api-key", transport).call("eth_chainId", [])
        assert len(transport.calls) == 1 and "private-api-key" not in str(caught.value)
    transport = RateLimitedTransport([-32005] * 3)
    with pytest.raises(ChainEvidenceError):
        HttpJsonRpc("https://rpc.example/private-api-key", transport).call("eth_chainId", [])
    assert len(transport.calls) == 3


def test_rpc_retries_stay_within_one_transport_timeout(monkeypatch) -> None:
    delays: list[float] = []
    monkeypatch.setattr(time, "sleep", delays.append)
    transport = RateLimitedTransport([-32005, -32005])
    transport.timeout_ms = 1_000  # Room for the 250 ms repeat, not the 1 s one.
    with pytest.raises(ChainEvidenceError):
        HttpJsonRpc("https://rpc.example/private-api-key", transport).call("eth_chainId", [])
    assert len(transport.calls) == 2 and delays == [0.25]


def test_rpc_retries_a_backend_that_has_not_imported_the_block(monkeypatch) -> None:
    # Arc: the load-balanced primary endpoint returns -32014 for a block one
    # backend has not imported yet; retry after a brief backoff.
    monkeypatch.setattr(time, "sleep", lambda _: None)
    transport = RateLimitedTransport([-32014])
    assert HttpJsonRpc("https://rpc.example/private-api-key", transport).call(
        "eth_chainId", []
    ) == hex(CHAIN)
    assert len(transport.calls) == 2


def test_an_unserved_numbered_block_is_lag_not_missing_evidence(context) -> None:
    reader, rpc, _ = context
    original = rpc.call

    def call(method, params):
        if method == "eth_getBlockByNumber" and params[0] != "latest":
            return None
        return original(method, params)

    rpc.call = call
    with pytest.raises(ChainLagError):
        reader.block(7)
