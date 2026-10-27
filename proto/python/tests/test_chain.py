"""Independent ABI/RPC evidence tests; no wallet submission or live RPC.

The stub rejects unplanned calls and requires every state read to use the same
EIP-1898 blockHash plus requireCanonical. Event bytes come from the published
compiler ABI, not the decoder under test. RPC honesty remains a trust assumption.
https://eips.ethereum.org/EIPS/eip-1898#specification
"""

import asyncio
import copy
import json
import time
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest
from eth_abi import decode, encode
from eth_account.messages import encode_typed_data
from eth_utils import keccak
from mppi_protocol import chain as chain_module
from mppi_protocol import follower as follower_module
from mppi_protocol.capabilities import FundingAuthorization
from mppi_protocol.catalog import parse_catalog
from mppi_protocol.chain import (
    ChainContradictionError,
    ChainEvidenceError,
    ChainLagError,
    ChainReader,
    CodePin,
    DeploymentProfile,
    FundingState,
    HttpJsonRpc,
)
from mppi_protocol.contract import ContractCalls
from mppi_protocol.discovery import MAX_DOCUMENT_BYTES
from mppi_protocol.errors import ValidationError
from mppi_protocol.follower import ChainFollower
from mppi_protocol.http import TransportError
from mppi_protocol.signing import voucher_digest
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
MULTICALL3 = bytes.fromhex("ca11bde05977b3631167028862be2a173976ca11")
MULTICALL3_RUNTIME = b"multicall3 test runtime"
WALLET, AGENT_ID = b"\x5a" * 20, 43  # A funding wallet and the agent id binding it.


@pytest.fixture
def probabilistic(monkeypatch):
    """A network without instant finality: proofs re-read state at the block."""
    monkeypatch.setattr(chain_module, "_INSTANT_FINALITY", frozenset())


@pytest.fixture(autouse=True)
def multicall3_code(monkeypatch):
    """Stand-in Multicall3 code; the real runtime's hash is checked elsewhere."""
    monkeypatch.setattr(chain_module, "_MULTICALL3_CODE_HASH", keccak(MULTICALL3_RUNTIME))


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
        self.code[hx(MULTICALL3)] = hx(MULTICALL3_RUNTIME)
        self.slots = {
            hx(PROXY): hx(bytes(12) + IMPLEMENTATION),
            hx(REGISTRY): hx(bytes(12) + REGISTRY_IMPLEMENTATION),
        }
        self.block_reads = 0
        self.reorg_after: int | None = None
        self.failure: Exception | None = None
        self.logs_found: list[dict] = []  # What eth_getLogs answers.

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
        if method == "eth_getLogs":
            return copy.deepcopy(self.logs_found)
        if method == "eth_getBlockByNumber":
            assert params in (
                ["latest", False],
                [hex(BLOCK_NUMBER), False],
                [self.block["number"], False],
            )
            self.block_reads += 1
            result = copy.deepcopy(self.block)
            if self.reorg_after is not None and self.block_reads > self.reorg_after:
                result["hash"] = hx(b"\xdd" * 32)
            return result
        if params[-1] == "latest":
            return self.latest(method, params)
        assert params[-1]["requireCanonical"] is True and set(params[-1]) == {
            "blockHash",
            "requireCanonical",
        }
        if params[-1]["blockHash"] != hx(BLOCK_HASH):
            # EIP-1898: an unknown block hash is "Resource not found" (-32001).
            raise ChainEvidenceError("RPC request returned no usable result")
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

    def latest(self, method: str, params: list) -> object:
        """Reads at the head, including one Multicall3 aggregate3 of views."""
        if method == "eth_getCode":
            return self.code[params[0].lower()]
        if method == "eth_getStorageAt":
            assert len(params) == 3 and params[1] == SLOT
            return self.slots[params[0]]
        assert method == "eth_call" and params[0]["to"] == hx(MULTICALL3)
        data = bytes.fromhex(params[0]["data"][2:])
        assert data[:4] == keccak(text="aggregate3((address,bool,bytes)[])")[:4]
        (calls,) = decode(["(address,bool,bytes)[]"], data[4:])
        head = {
            keccak(text="getBlockNumber()")[:4]: encode(
                ["uint256"], [int(self.block["number"], 16)]
            ),
            keccak(text="getCurrentBlockTimestamp()")[:4]: encode(
                ["uint256"], [int(self.block["timestamp"], 16)]
            ),
            keccak(text="getLastBlockHash()")[:4]: bytes.fromhex(self.block["parentHash"][2:]),
        }
        answers = []
        binding = abi_call("getAgentWallet(uint256)", ["uint256"], [AGENT_ID])
        for target, allow_failure, calldata in calls:
            key = (target.lower(), "0x" + calldata.hex())
            # One failing view fails the snapshot, except the funding binding.
            assert allow_failure is (key == (hx(REGISTRY), binding))
            if target.lower() == hx(MULTICALL3):
                answers.append((True, head[calldata]))
            elif key not in self.responses and allow_failure:
                answers.append((False, b""))  # The registry reverted.
            else:
                answers.append((True, bytes.fromhex(self.responses[key][2:])))
        return hx(encode(["(bool,bytes)[]"], [answers]))

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


def test_open_requires_actual_proxy_event_and_matching_state(context, probabilistic) -> None:
    reader, rpc, calls = context
    rpc.logs(event_log("SessionOpened", opened()))
    result = reconcile(reader, opening(calls))
    assert result.state is OperationState.CONFIRMED
    assert result.operation_ref == "intent-17" and result.wallet_operation_id == "wallet-23"
    assert result.tx_hash == TX and result.observation.block_hash == BLOCK_HASH
    assert result.sessions[0].session_id == descriptor_id(DESCRIPTOR)
    assert result.sessions[0].settlement_reference is None
    assert rpc.block_reads == 1  # Read once, in the batch with the session state.


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


def test_close_result_and_settlement_reference_require_event_state_agreement(
    context, probabilistic
) -> None:
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
def test_outer_wallet_success_does_not_prove_open(context, probabilistic, mode: str) -> None:
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
def test_reorg_during_observation_never_produces_definitive_result(
    context, probabilistic, status: str
) -> None:
    # The receipt names a block that is no longer canonical, and the endpoint
    # ignores requireCanonical: it still serves state at the old hash. The
    # block read in the same batch shows another hash at that number.
    reader, rpc, calls = context
    rpc.receipt["status"] = status
    rpc.logs(event_log("SessionOpened", opened()))
    rpc.reorg_after = 0
    assert reconcile(reader, opening(calls)).state is OperationState.UNKNOWN


def test_chain_mismatch_and_transport_failure_are_not_payment_failures(context) -> None:
    reader, rpc, calls = context
    rpc.chain = hex(5042)
    assert reconcile(reader, opening(calls)).state is OperationState.UNKNOWN
    with pytest.raises(ChainContradictionError, match="RPC chain differs"):
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


def test_paid_state_and_opening_catalog_cannot_contradict_events(context, probabilistic) -> None:
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
def test_each_remaining_operation_requires_its_own_event_and_state(
    context, probabilistic, kind: str
) -> None:
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
    pinned = set(rpc.code) - {hx(MULTICALL3)}  # Multicall3 is checked by snapshots only.
    assert {params[0] for method, params in rpc.calls if method == "eth_getCode"} == pinned
    assert {params[0] for method, params in rpc.calls if method == "eth_getStorageAt"} == set(
        rpc.slots
    )
    assert rpc.block_reads == 2


@pytest.mark.parametrize("address", [PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION])
def test_each_code_pin_is_enforced(context, address: bytes) -> None:
    reader, rpc, _ = context
    install_domain_response(rpc)
    rpc.code[hx(address)] = "0x60006000"
    with pytest.raises(ChainContradictionError, match="code differs"):
        reader.verify_deployment(reader.block())


@pytest.mark.parametrize("address", [PROXY, REGISTRY])
def test_both_implementation_slots_are_enforced(context, address: bytes) -> None:
    reader, rpc, _ = context
    install_domain_response(rpc)
    rpc.slots[hx(address)] = hx(bytes(12) + b"\xee" * 20)
    with pytest.raises(ChainContradictionError, match="implementation differs"):
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
    with pytest.raises(ChainContradictionError, match="signing domain"):
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
    monkeypatch.setattr(chain_module, "_jitter", lambda: 1.0)  # Each delay's ceiling.
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


def test_rpc_retries_start_within_one_transport_timeout_and_their_delays(monkeypatch) -> None:
    # Each attempt here takes the whole 1 s timeout and fails transiently. With
    # each delay at its ceiling, the first repeat starts at 1.25 s, inside
    # 1 s + 1.25 s; the next would start at 3.25 s, outside it.
    clock = [0.0]
    delays: list[float] = []
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(chain_module, "_jitter", lambda: 1.0)

    def sleep(delay: float) -> None:
        delays.append(delay)
        clock[0] += delay

    monkeypatch.setattr(time, "sleep", sleep)

    class Slow:
        timeout_ms = 1_000

        def __init__(self) -> None:
            self.calls = 0

        def request(self, url: str, *, body: bytes | None = None) -> bytes:
            self.calls += 1
            clock[0] += 1.0
            raise TransportError("HTTP request failed", transient=True)

    transport = Slow()
    with pytest.raises(TransportError):
        HttpJsonRpc("https://rpc.example/private-api-key", transport).call("eth_chainId", [])  # type: ignore[arg-type]
    assert transport.calls == 2 and delays == [0.25]


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


class Trips:
    """Counts round trips to the RPC: one per call, or one per batch."""

    def __init__(self, rpc: EvidenceRpc) -> None:
        self.rpc, self.trips = rpc, 0

    def call(self, method: str, params: list[object]) -> object:
        self.trips += 1
        return self.rpc.call(method, params)

    def call_many(self, calls: list[tuple[str, list[object]]]) -> list[object]:
        self.trips += 1
        return [self.rpc.call(method, params) for method, params in calls]


def registered(rpc: EvidenceRpc, catalog: bytes = CATALOG) -> None:
    """A verified deployment, unpaused, with one registered provider."""
    install_domain_response(rpc)
    rpc.response(PROXY, "pauseLevel()", [], [], ["uint8"], [0])
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
        [catalog],
    )


def test_each_read_step_is_one_round_trip_and_fixed_facts_are_read_once(context) -> None:
    reader, rpc, _ = context
    registered(rpc)
    trips = Trips(rpc)
    reader = ChainReader(trips, reader.profile)
    block = reader.block()
    reader.verify_deployment(block)
    reader.identity(DESCRIPTOR.provider_id, block)
    pause, identities, sessions = reader.observe(
        block, providers=(DESCRIPTOR.provider_id,), sessions=(descriptor_id(DESCRIPTOR),)
    )
    assert pause is PauseLevel.NONE and sessions[descriptor_id(DESCRIPTOR)].exists
    assert identities[DESCRIPTOR.provider_id].recipient == DESCRIPTOR.recipient
    assert trips.trips == 4
    # The chain id and the code at each pinned address never change for a reader.
    methods = Counter(method for method, _ in rpc.calls)
    assert (methods["eth_chainId"], methods["eth_getCode"], methods["eth_getStorageAt"]) == (
        1,
        4,
        4,
    )


def test_a_receipt_proof_is_two_round_trips(context, probabilistic) -> None:
    reader, rpc, calls = context
    rpc.logs(event_log("SessionOpened", opened()))
    trips = Trips(rpc)
    assert reconcile(ChainReader(trips, reader.profile), opening(calls)).state is (
        OperationState.CONFIRMED
    )
    assert trips.trips == 2  # The receipt; then its block with the session state.
    accepted = parse_catalog(CATALOG)
    rpc.session(rate_hash=accepted.rate_card_hash)
    rpc.logs(event_log("SessionOpened", opened(initialRateCardHash=accepted.rate_card_hash)))
    trips = Trips(rpc)
    ChainReader(trips, reader.profile).opening(
        TX, descriptor_id(DESCRIPTOR), accepted_catalog=accepted
    )
    assert trips.trips == 2


def test_a_follower_refresh_is_one_round_trip(context) -> None:
    reader, rpc, _ = context
    registered(rpc)
    trips = Trips(rpc)
    follower = ChainFollower(
        ChainReader(trips, reader.profile),
        max_age_ms=60_000,
        collection_margin_ms=1000,
        wall_clock=lambda: TIMESTAMP + 1,
    )

    async def scenario() -> None:
        await follower.start()
        await follower.watch_provider(DESCRIPTOR.provider_id)
        await follower.watch_session(descriptor_id(DESCRIPTOR))
        await follower.stop()

    asyncio.run(scenario())
    # Arc blocks are final: one Multicall3 snapshot per refresh, anchored to the
    # header in the same batch; fixed facts and the signing domain read once.
    assert trips.trips == 3
    methods = Counter(method for method, _ in rpc.calls)
    assert (methods["eth_chainId"], methods["eth_getCode"], methods["eth_getStorageAt"]) == (
        1,
        5,
        6,
    )


def test_an_unwatched_session_leaves_the_next_refresh(context) -> None:
    reader, rpc, _ = context
    registered(rpc)
    sid = descriptor_id(DESCRIPTOR)
    follower = ChainFollower(
        reader, max_age_ms=60_000, collection_margin_ms=1000, wall_clock=lambda: TIMESTAMP + 1
    )

    def session_reads() -> int:
        """getSession views in the last refresh's Multicall3 aggregate3."""
        data = [
            params[0]["data"]
            for method, params in rpc.calls
            if method == "eth_call" and params[0]["to"] == hx(MULTICALL3)
        ][-1]
        (calls,) = decode(["(address,bool,bytes)[]"], bytes.fromhex(data[10:]))
        selector = keccak(text="getSession(bytes32)")[:4]
        return sum(calldata[:4] == selector for _, _, calldata in calls)

    async def scenario() -> None:
        await follower.start()
        await follower.watch_session(sid)
        assert session_reads() == 1 and sid in follower.current.sessions
        follower.unwatch_session(sid)
        assert sid in follower.current.sessions  # Accepted evidence is kept.
        await follower.refresh()
        assert session_reads() == 0 and sid not in follower.current.sessions
        await follower.stop()

    asyncio.run(scenario())


class TimesOutOnce:
    """A JSON-RPC transport over the evidence fake whose first request times out:
    it spends the whole attempt timeout on ``clock`` before failing."""

    timeout_ms = 1_000

    def __init__(self, rpc: EvidenceRpc, clock: list[float]) -> None:
        self.rpc, self.clock, self.requests = rpc, clock, 0

    def request(self, url: str, *, body: bytes | None = None) -> bytes:
        self.requests += 1
        if self.requests == 1:
            self.clock[0] += self.timeout_ms / 1000
            raise TransportError("HTTP request failed", transient=True)  # A slow backend.
        assert body is not None
        payload = json.loads(body)

        def answer(call: dict) -> dict:
            try:
                return {
                    "jsonrpc": "2.0",
                    "id": call["id"],
                    "result": self.rpc.call(call["method"], call["params"]),
                }
            except ChainEvidenceError:
                return {"jsonrpc": "2.0", "id": call["id"], "error": {"code": -32001}}

        reply = [answer(call) for call in payload] if isinstance(payload, list) else answer(payload)
        return json.dumps(reply).encode()


@pytest.mark.parametrize("read", ["receipt", "closing", "composition"])
def test_a_proof_read_that_times_out_once_still_proves(context, read, monkeypatch) -> None:
    clock = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))
    reader, rpc, _ = context
    sid = descriptor_id(DESCRIPTOR)
    transport = TimesOutOnce(rpc, clock)
    chain = ChainReader(
        HttpJsonRpc("https://rpc.example/private-api-key", transport),  # type: ignore[arg-type]
        reader.profile,
    )
    if read == "receipt":
        rpc.session(paid=5_420_000)
        rpc.logs(event_log("Settled", settled()))
        reference = ConfirmedSettlementRef(
            reader.block().observation, TX, sid, 5_420_000, 5_420_000
        )
        chain.verify_settlement(reference, DESCRIPTOR)
    elif read == "closing":
        rpc.session(paid=5_420_000, status=2)
        rpc.logs(
            event_log(
                "SessionClosed",
                {"sessionId": sid, "captureAmount": 5_420_000, "refunded": 580_000},
            )
        )
        assert chain.closing(TX, sid).capture_amount == 5_420_000
    else:
        install_domain_response(rpc)
        chain.verify_deployment(chain.block())
    assert transport.requests >= 2  # The timed-out request was repeated.


def test_a_snapshot_is_one_round_trip_anchored_to_the_header_it_ran_at(context) -> None:
    reader, rpc, _ = context
    registered(rpc)
    trips = Trips(rpc)
    reader = ChainReader(trips, reader.profile)
    sid = descriptor_id(DESCRIPTOR)
    block, pause, identities, states = reader.latest(
        providers=(DESCRIPTOR.provider_id,), sessions=(sid,)
    )
    assert trips.trips == 1
    assert block.observation.block_number == BLOCK_NUMBER and pause is PauseLevel.NONE
    assert identities[DESCRIPTOR.provider_id].recipient == DESCRIPTOR.recipient
    assert states[sid].exists
    original = rpc.latest

    def another_block(method, params):
        # Views at the header's height, in a block with another parent.
        if method == "eth_call":
            rpc.block = dict(rpc.block, parentHash=hx(b"\xee" * 32))
        return original(method, params)

    rpc.latest = another_block
    with pytest.raises(ChainEvidenceError, match="different blocks"):
        reader.latest(sessions=(sid,))


def test_one_call_at_a_time_views_that_ran_after_the_header_are_read_again_at_it() -> None:
    node = Node()

    class OneCall:
        """An adapter without ``call_many``: each read is its own request.
        A block arrives between the header and the views."""

        def __init__(self) -> None:
            self.moved = False

        def call(self, method: str, params: list[object]) -> object:
            answer = node.answer(method, params)
            if params == ["latest", False] and not self.moved:
                self.moved = True
                node.head += 1
            return answer

    reader = ChainReader(OneCall(), node.profile())
    sid = descriptor_id(DESCRIPTOR)
    block, pause, _, states = reader.latest(sessions=(sid,))
    assert block.observation.block_number == BLOCK_NUMBER
    assert block.observation.block_hash == Node.hash(BLOCK_NUMBER) and pause is PauseLevel.NONE
    assert states[sid].last_activity == node.activity(BLOCK_NUMBER)
    assert [(at, pinned) for at, _, _, pinned in node.aggregates] == [
        (BLOCK_NUMBER + 1, False),
        (BLOCK_NUMBER, True),
    ]


@pytest.mark.parametrize("code", [b"", b"\x60\x80"], ids=["no_code", "code"])
@pytest.mark.parametrize("bound", [WALLET, bytes(20), None], ids=["bound", "cleared", "reverted"])
def test_a_snapshot_reads_the_funding_binding_and_code_in_its_one_round_trip(
    context, code, bound
) -> None:
    """An opening's funding check needs the registry's binding for its agent
    id and whether the funding wallet has code. Both ride on the snapshot:
    the binding in the multicall, allowed to fail since a registry reverting
    for an unknown id binds none, and the code beside the implementation
    slots."""
    reader, rpc, _ = context
    registered(rpc)
    rpc.code[hx(WALLET)] = hx(code)
    if bound is not None:
        rpc.response(
            REGISTRY, "getAgentWallet(uint256)", ["uint256"], [AGENT_ID], ["address"], [hx(bound)]
        )
    trips = Trips(rpc)
    reader = ChainReader(trips, reader.profile)
    sid = descriptor_id(DESCRIPTOR)
    block, pause, _, states, state = reader._latest((), (sid,), (AGENT_ID, WALLET))
    assert trips.trips == 1 and pause is PauseLevel.NONE and states[sid].exists
    assert state == FundingState(
        AGENT_ID, WALLET, bytes(20) if bound is None else bound, bool(code)
    )
    assert ("eth_getCode", [hx(WALLET), "latest"]) in rpc.calls
    # Without it, the snapshot is as before.
    assert reader.latest(sessions=(sid,))[0] == block and trips.trips == 2


def test_without_multicall3_the_pinned_batch_reads_the_funding_evidence(context) -> None:
    reader, rpc, _ = context
    registered(rpc)
    rpc.code[hx(MULTICALL3)] = hx(b"some other contract")
    rpc.code[hx(WALLET)] = hx(b"\x60\x80")
    rpc.response(
        REGISTRY, "getAgentWallet(uint256)", ["uint256"], [AGENT_ID], ["address"], [hx(WALLET)]
    )
    reader.latest()  # Learns the code is not Multicall3's.
    _, _, _, _, state = reader._latest((), (), (AGENT_ID, WALLET))
    assert state == FundingState(AGENT_ID, WALLET, WALLET, True)
    pinned = {"blockHash": hx(BLOCK_HASH), "requireCanonical": True}
    assert ("eth_getCode", [hx(WALLET), pinned]) in rpc.calls


class Answers:
    """An HTTP transport answering each JSON-RPC call from the evidence fake."""

    timeout_ms = 1_000

    def __init__(self, rpc: EvidenceRpc) -> None:
        self.rpc = rpc

    def request(self, url: str, *, body: bytes | None = None) -> bytes:
        assert body is not None
        payload = json.loads(body)
        calls = payload if isinstance(payload, list) else [payload]
        reply = [
            {
                "jsonrpc": "2.0",
                "id": call["id"],
                "result": self.rpc.call(call["method"], call["params"]),
            }
            for call in calls
        ]
        return json.dumps(reply if isinstance(payload, list) else reply[0]).encode()


def test_an_opening_s_transaction_is_found_by_its_session_log(context) -> None:
    """For an opening its wallet reports with no transaction: one read-only
    eth_getLogs, filtered by the proxy and the SessionOpened sessionId
    topic, over at most 9,999 blocks, Arc's documented chunk."""
    reader, rpc, _ = context
    sid = descriptor_id(DESCRIPTOR)
    log = event_log("SessionOpened", opened())
    rpc.logs_found = [log]
    http = ChainReader(HttpJsonRpc("https://rpc.example/private-key", Answers(rpc)), reader.profile)
    assert http.opening_transaction(sid, BLOCK_NUMBER - 50, BLOCK_NUMBER + 20_000) == TX
    ((query,),) = [params for method, params in rpc.calls if method == "eth_getLogs"]
    assert query == {
        "address": hx(PROXY),
        "topics": [log["topics"][0], hx(sid)],
        "fromBlock": hex(BLOCK_NUMBER - 50),
        "toBlock": hex(BLOCK_NUMBER - 50 + 9_998),
    }
    rpc.logs_found = []
    assert reader.opening_transaction(sid, BLOCK_NUMBER, BLOCK_NUMBER) is None
    rpc.calls.clear()
    assert reader.opening_transaction(sid, BLOCK_NUMBER, BLOCK_NUMBER - 1) is None
    assert rpc.calls == []  # An empty range reads nothing.
    rpc.logs_found = [log, dict(log, transactionHash=hx(b"\xcc" * 32), logIndex="0x1")]
    with pytest.raises(ChainEvidenceError, match="ambiguous"):
        reader.opening_transaction(sid, BLOCK_NUMBER, BLOCK_NUMBER)
    for outside in (
        dict(log, blockNumber=hex(BLOCK_NUMBER + 1)),
        dict(log, address=hx(REGISTRY)),
        dict(log, topics=[log["topics"][0], hx(b"\x01" * 32)]),
    ):
        rpc.logs_found = [outside]
        with pytest.raises(ChainEvidenceError, match="differs from its filter"):
            reader.opening_transaction(sid, BLOCK_NUMBER, BLOCK_NUMBER)
    rpc.logs_found = [dict(log, removed=True)]
    assert reader.opening_transaction(sid, BLOCK_NUMBER, BLOCK_NUMBER) is None


def test_unverified_multicall3_code_falls_back_to_batched_reads_for_good(context) -> None:
    reader, rpc, _ = context
    registered(rpc)
    rpc.code[hx(MULTICALL3)] = hx(b"some other contract")
    trips = Trips(rpc)
    reader = ChainReader(trips, reader.profile)
    block, pause, _, _ = reader.latest()
    assert block.observation.block_number == BLOCK_NUMBER and pause is PauseLevel.NONE
    # Sent once before its code was known, and its answer discarded.
    multicalls = [p for m, p in rpc.calls if m == "eth_call" and p[0]["to"] == hx(MULTICALL3)]
    assert len(multicalls) == 1 and trips.trips == 3
    first = trips.trips
    reader.latest()
    assert trips.trips - first == 2  # The head, then a batch pinned to it.
    assert (
        sum(params[0] == hx(MULTICALL3) for method, params in rpc.calls if method == "eth_getCode")
        == 1
    )


def test_a_snapshot_refuses_a_changed_implementation_slot(context) -> None:
    reader, rpc, _ = context
    registered(rpc)
    rpc.slots[hx(PROXY)] = hx(bytes(12) + b"\x55" * 20)
    with pytest.raises(ChainContradictionError, match="implementation differs"):
        reader.latest()


def remaining_operation(calls, rpc, kind):
    """An agent operation, and the event that proves it."""
    sid = descriptor_id(DESCRIPTOR)
    if kind == "open":
        return opening(calls), "SessionOpened", opened()
    if kind == "top_up":
        auth = FundingAuthorization(
            CHAIN, TOKEN, "USDC", "2", DESCRIPTOR.agent, PROXY, 490_000,
            TIMESTAMP - 1, TIMESTAMP + 100, b"\x77" * 32,
        )  # fmt: skip
        operation = calls.top_up(DESCRIPTOR, 490_000, auth, b"smart-account-proof")
        return operation, "ToppedUp", {"sessionId": sid, "amount": 490_000, "newDeposit": 6_000_000}
    if kind == "request_close":
        return (
            calls.request_close(DESCRIPTOR),
            "CloseRequested",
            {"sessionId": sid, "graceEnd": TIMESTAMP + 3600},
        )
    closed = {"sessionId": sid, "captureAmount": 5_420_000, "refunded": 580_000}
    if kind == "close":
        return calls.close(DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE), "SessionClosed", closed
    if kind == "withdraw":
        return calls.withdraw(DESCRIPTOR), "SessionClosed", closed
    return calls.release_idle(DESCRIPTOR), "IdleReleased", {"sessionId": sid, "amount": 580_000}


@pytest.mark.parametrize(
    "kind", ["open", "top_up", "request_close", "close", "withdraw", "release_idle"]
)
def test_on_a_final_network_the_event_alone_proves_an_operation(context, kind) -> None:
    """One receipt read: status 1 and our own decoded event from the pinned proxy."""
    reader, rpc, calls = context
    operation, name, values = remaining_operation(calls, rpc, kind)
    rpc.logs(event_log(name, values))
    trips = Trips(rpc)
    proven = reconcile(ChainReader(trips, reader.profile), operation)
    assert proven.state is OperationState.CONFIRMED and trips.trips == 1
    assert proven.observation.block_hash == BLOCK_HASH and rpc.block_reads == 0
    assert not any(method == "eth_call" for method, _ in rpc.calls)  # No state read.
    rpc.logs()  # Mined, but the MPPI call did not take effect in it.
    failed = reconcile(reader, operation)
    assert failed.state is OperationState.FAILED and failed.tx_hash == TX
    assert failed.cause == "the MPPI call did not take effect in its transaction"


@pytest.mark.parametrize("mode", ["spoof-emitter", "different-session"])
def test_on_a_final_network_another_contract_or_session_event_proves_nothing(context, mode):
    reader, rpc, calls = context
    if mode == "spoof-emitter":
        rpc.logs(event_log("SessionOpened", opened(), emitter=b"\xef" * 20))
    else:
        rpc.logs(event_log("SessionOpened", opened(sessionId=b"\xef" * 32)))
    assert reconcile(reader, opening(calls)).state is OperationState.FAILED


ENTRYPOINT = bytes.fromhex("0000000071727de22e5e9d8baf0edac6f37da032")


def revert_reason(reason: bytes, *, emitter: bytes = ENTRYPOINT, sender=None) -> dict:
    """EntryPoint v0.7 UserOperationRevertReason for the agent's UserOperation."""
    return {
        "address": hx(emitter),
        "topics": [
            hx(keccak(text="UserOperationRevertReason(bytes32,address,uint256,bytes)")),
            hx(b"\x44" * 32),
            hx(bytes(12) + (sender or DESCRIPTOR.agent)),
        ],
        "data": hx(encode(["uint256", "bytes"], [7, reason])),
        "logIndex": "0x0",
        "removed": False,
        "transactionHash": hx(TX),
        "transactionIndex": "0x2",
        "blockNumber": hex(BLOCK_NUMBER),
        "blockHash": hx(BLOCK_HASH),
    }


@pytest.mark.parametrize(
    "reason,expected",
    [
        (
            keccak(text="Error(string)")[:4]
            + encode(["string"], ["FiatTokenV2: invalid signature"]),
            ": FiatTokenV2: invalid signature",
        ),
        (keccak(text="NotAgent()")[:4], ": NotAgent"),
        (
            keccak(text="Error(string)")[:4] + encode(["string"], ["see https://scam.example"]),
            ": the wallet operation reverted (0x08c379a0)",
        ),
        (b"\x12\x34\x56\x78", ": the wallet operation reverted (0x12345678)"),
    ],
)
def test_an_erc4337_inner_revert_is_a_definite_failure_with_its_known_reason(
    context, reason, expected
) -> None:
    """A bundle succeeds while its UserOperation reverts: FAILED, never UNKNOWN,
    and the cause holds only a USDC reason or MPPI error the SDK knows."""
    reader, rpc, calls = context
    operation, _, _ = remaining_operation(calls, rpc, "top_up")
    rpc.logs(revert_reason(reason))
    failed = reconcile(reader, operation)
    assert failed.state is OperationState.FAILED
    assert failed.cause == "the MPPI call did not take effect in its transaction" + expected
    # The same event from another contract, or for another wallet, names nothing.
    for log in (
        revert_reason(reason, emitter=b"\xef" * 20),
        revert_reason(reason, sender=b"\xef" * 20),
    ):
        rpc.logs(log)
        plain = reconcile(reader, operation)
        assert plain.state is OperationState.FAILED
        assert plain.cause == "the MPPI call did not take effect in its transaction"


def test_on_a_final_network_opening_and_closing_proofs_are_one_round_trip(context) -> None:
    reader, rpc, _ = context
    sid = descriptor_id(DESCRIPTOR)
    accepted = parse_catalog(CATALOG)
    rpc.logs(event_log("SessionOpened", opened(initialRateCardHash=accepted.rate_card_hash)))
    trips = Trips(rpc)
    proof = ChainReader(trips, reader.profile).opening(TX, sid, accepted_catalog=accepted)
    assert trips.trips == 1 and proof.deposit == 5_510_000
    assert proof.observation.block_hash == BLOCK_HASH
    closed = {"sessionId": sid, "captureAmount": 5_420_000, "refunded": 580_000}
    rpc.logs(event_log("SessionClosed", closed))
    trips = Trips(rpc)
    evidence = ChainReader(trips, reader.profile).closing(TX, sid)
    assert trips.trips == 1
    assert (evidence.capture_amount, evidence.refunded) == (5_420_000, 580_000)
    # Settlements still read state: a no-op or skipped item emits no event.
    rpc.session(paid=5_420_000)
    rpc.logs(event_log("Settled", settled()))
    trips = Trips(rpc)
    settle = reconcile(ChainReader(trips, reader.profile), reader_calls_settle())
    assert settle.state is OperationState.CONFIRMED and trips.trips == 2


def reader_calls_settle():
    return ContractCalls(DOMAIN, "USDC", "2").settle(DESCRIPTOR, 5_510_000, 5_420_000, SIGNATURE)


class _Refused(Exception):
    def __init__(self, code: int) -> None:
        self.code = code


class Node:
    """A synthetic Arc node behind HttpJsonRpc, answering each read at the block
    it names. It records each request body's size and each aggregate3 it ran:
    the block, its view count, its registry views and whether it was pinned.
    A session's last activity names the block it was read at."""

    timeout_ms = 1_000

    def __init__(self, *, multicall: bool = True) -> None:
        self.head = BLOCK_NUMBER
        self.sizes: list[int] = []
        self.bodies: list[object] = []
        self.aggregates: list[tuple[int, int, int, bool]] = []
        # The head moves on once this many more aggregate3 calls at latest ran.
        self.advance_after: int | None = None
        # Arc's -32014 for every read pinned by block hash.
        self.unimported = False
        self.code = {
            address: bytes([i]) + b"test-runtime"
            for i, address in enumerate(
                (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION), start=1
            )
        }
        self.code[MULTICALL3] = MULTICALL3_RUNTIME if multicall else b"another contract"

    def profile(self) -> DeploymentProfile:
        pins = (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION)
        return DeploymentProfile(
            DOMAIN, REGISTRY, *(CodePin(pin, keccak(self.code[pin])) for pin in pins)
        )

    @staticmethod
    def hash(number: int) -> bytes:
        return keccak(number.to_bytes(8, "big"))

    @staticmethod
    def activity(number: int) -> int:
        return TIMESTAMP + number - BLOCK_NUMBER

    def block(self, number: int) -> dict | None:
        if number > self.head:
            return None
        return {
            "number": hex(number),
            "hash": hx(self.hash(number)),
            "parentHash": hx(self.hash(number - 1)),
            "timestamp": hex(self.activity(number)),
        }

    def request(self, url: str, *, body: bytes | None = None) -> bytes:
        assert body is not None
        self.sizes.append(len(body))
        payload = json.loads(body)
        self.bodies.append(payload)
        calls = payload if isinstance(payload, list) else [payload]
        replies = []
        for call in calls:
            try:
                reply = {"result": self.answer(call["method"], call["params"])}
            except _Refused as refused:
                reply = {"error": {"code": refused.code, "message": "not yet imported"}}
            replies.append({"jsonrpc": "2.0", "id": call["id"], **reply})
        return json.dumps(replies if isinstance(payload, list) else replies[0]).encode()

    def answer(self, method: str, params: list) -> object:
        if method == "eth_chainId":
            return hex(CHAIN)
        if method == "eth_getBlockByNumber":
            return self.block(self.head if params[0] == "latest" else int(params[0], 16))
        *arguments, position = params
        pinned = position != "latest"
        number = self.head
        if pinned:
            assert position["requireCanonical"] is True
            if self.unimported:
                raise _Refused(-32014)
            (number,) = [
                n for n in range(self.head + 1) if hx(self.hash(n)) == position["blockHash"]
            ]
        target = bytes.fromhex(arguments[0][2:] if method != "eth_call" else arguments[0]["to"][2:])
        if method == "eth_getCode":
            return hx(self.code.get(target, b""))
        if method == "eth_getStorageAt":
            return hx(
                bytes(12) + {PROXY: IMPLEMENTATION, REGISTRY: REGISTRY_IMPLEMENTATION}[target]
            )
        assert method == "eth_call"
        data = bytes.fromhex(arguments[0]["data"][2:])
        if target != MULTICALL3:
            return hx(self.view(target, data, number))
        assert data[:4] == keccak(text="aggregate3((address,bool,bytes)[])")[:4]
        (views,) = decode(["(address,bool,bytes)[]"], data[4:])
        answers = [(True, self.view(bytes.fromhex(to[2:]), view, number)) for to, _, view in views]
        registry = sum(to.lower() == hx(REGISTRY) for to, _, _ in views)
        self.aggregates.append((number, len(views), registry, pinned))
        if not pinned and self.advance_after is not None:
            self.advance_after -= 1
            if self.advance_after == 0:
                self.advance_after = None
                self.head += 1
        return hx(encode(["(bool,bytes)[]"], [answers]))

    def view(self, target: bytes, data: bytes, number: int) -> bytes:
        call, arguments = data[:4], data[4:]
        selector = lambda signature: keccak(text=signature)[:4]  # noqa: E731
        if target == MULTICALL3:
            return {
                selector("getBlockNumber()"): encode(["uint256"], [number]),
                selector("getCurrentBlockTimestamp()"): encode(
                    ["uint256"], [self.activity(number)]
                ),
                selector("getLastBlockHash()"): self.hash(number - 1),
            }[call]
        if target == PROXY:
            if call == selector("voucherDigest(bytes32,uint64)"):
                return voucher_digest(DOMAIN, bytes(32), 0)
            if call == selector("pauseLevel()"):
                return encode(["uint8"], [0])
            assert call == selector("getSession(bytes32)")
            state = (6_000_000, 0, 0, 0, self.activity(number), RATE_HASH)
            return encode([STATE_TYPE], [state])
        assert target == REGISTRY
        if call == selector("getMetadata(uint256,string)"):
            return encode(["bytes"], [CATALOG])
        (provider,) = decode(["uint256"], arguments)
        assert provider == DESCRIPTOR.provider_id
        return {
            selector("ownerOf(uint256)"): encode(["address"], [DESCRIPTOR.operator]),
            selector("getAgentWallet(uint256)"): encode(["address"], [DESCRIPTOR.recipient]),
            selector("tokenURI(uint256)"): encode(
                ["string"], ["https://provider.example/discover"]
            ),
        }[call]


def node_follower(node: Node, sessions: tuple[bytes, ...]) -> ChainFollower:
    """A follower of one provider and ``sessions`` over HttpJsonRpc to ``node``."""
    reader = ChainReader(
        HttpJsonRpc("https://rpc.example/private-api-key", node),  # type: ignore[arg-type]
        node.profile(),
    )
    follower = ChainFollower(
        reader, max_age_ms=60_000, collection_margin_ms=1000, wall_clock=lambda: TIMESTAMP + 30
    )
    follower.follow(providers=(DESCRIPTOR.provider_id,), sessions=sessions)
    return follower


def session_ids(count: int) -> tuple[bytes, ...]:
    return tuple(keccak(index.to_bytes(8, "big")) for index in range(count))


def test_a_snapshot_of_2500_sessions_is_read_at_one_block_in_bounded_requests() -> None:
    node = Node()
    ids = session_ids(2_500)
    follower = node_follower(node, ids)

    async def scenario() -> None:
        await follower.start()
        requests, answered = len(node.sizes), len(node.aggregates)
        node.head += 1
        snapshot = await follower.refresh()
        number = snapshot.block.observation.block_number
        assert number == node.head and len(snapshot.sessions) == 2_500
        # Each session's last activity names the block it was read at.
        assert all(snapshot.sessions[sid].last_activity == node.activity(number) for sid in ids)
        assert follower.readiness(ids[-1]).ready
        # Each aggregate3 ran at the snapshot's block, within its gas bound: 512
        # views besides its three head views, and one provider's identity.
        calls = node.aggregates[answered:]
        assert len(calls) == 5, calls
        assert all(
            at == number and views <= 3 + 512 and registry <= 4 and not pinned
            for at, views, registry, pinned in calls
        ), calls
        # About 1.1 MB of requests: two exchanges, each within the transport bound.
        assert len(node.sizes) - requests == 2
        assert max(node.sizes) <= MAX_DOCUMENT_BYTES, node.sizes
        await follower.stop()

    asyncio.run(scenario())


def test_snapshot_calls_that_ran_at_another_head_are_read_again_pinned_to_the_header() -> None:
    node = Node()
    follower = node_follower(node, session_ids(1_100))

    async def scenario() -> None:
        await follower.start()
        requests, answered = len(node.sizes), len(node.aggregates)
        number = node.head
        # The first call runs at the header's block; then the head moves on.
        node.advance_after = 1
        snapshot = await follower.refresh()
        assert snapshot.block.observation.block_number == number
        assert all(s.last_activity == node.activity(number) for s in snapshot.sessions.values())
        assert [(at, pinned) for at, _, _, pinned in node.aggregates[answered:]] == [
            (number, False),
            (number + 1, False),
            (number + 1, False),
            (number, True),
            (number, True),
        ]
        assert len(node.sizes) - requests == 2
        again = node.bodies[-1]
        pinned = {"blockHash": hx(Node.hash(number)), "requireCanonical": True}
        assert ["eth_getBlockByNumber", [hex(number), False]] in [
            [call["method"], call["params"]] for call in again
        ]
        assert sum(call["params"][-1] == pinned for call in again) == 2
        await follower.stop()

    asyncio.run(scenario())


def test_the_common_refresh_is_one_round_trip_even_over_several_calls() -> None:
    node = Node()
    follower = node_follower(node, session_ids(1_000))

    async def scenario() -> None:
        await follower.start()
        requests, answered = len(node.sizes), len(node.aggregates)
        node.head += 1
        assert len((await follower.refresh()).sessions) == 1_000
        assert len(node.sizes) - requests == 1
        assert len(node.aggregates) - answered == 2
        await follower.stop()

    asyncio.run(scenario())


def test_a_block_the_backend_has_not_imported_is_lag(monkeypatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda _: None)
    # Arc's -32014: a backend "hasn't yet imported that block"; repeated past
    # the reader's repeats, it is lag (docs.arc.io/arc/references/rpc-endpoints).
    transport = RateLimitedTransport([-32014] * 3)
    with pytest.raises(ChainLagError):
        HttpJsonRpc("https://rpc.example/private-api-key", transport).call("eth_chainId", [])
    assert len(transport.calls) == 3
    transport = RateLimitedTransport([-32005] * 3)
    with pytest.raises(ChainEvidenceError) as caught:
        HttpJsonRpc("https://rpc.example/private-api-key", transport).call("eth_chainId", [])
    assert not isinstance(caught.value, ChainLagError)

    async def no_wait(_: float) -> None:
        pass

    monkeypatch.setattr(follower_module, "_sleep", no_wait)
    # Without Multicall3 a refresh is the head, then reads pinned to its hash.
    node = Node(multicall=False)
    sid = descriptor_id(DESCRIPTOR)
    follower = node_follower(node, (sid,))

    async def scenario() -> None:
        await follower.start()
        accepted = follower.current
        node.head += 1
        node.unimported = True
        assert await follower.refresh() is accepted
        assert follower.readiness(sid).ready
        # A watch the accepted snapshot does not cover reports the lag.
        with pytest.raises(ChainLagError):
            await follower.watch_session(b"\x56" * 32)
        node.unimported = False
        assert (await follower.refresh()).block.observation.block_number == node.head
        await follower.stop()

    asyncio.run(scenario())
