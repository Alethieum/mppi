"""Independent ERC-8004 ABI, owner, proof-domain and canonical receipt tests.

Expected event layouts follow the pinned upstream contract, not this decoder.
No transaction wallet, credentials, network request or provider storage involved.
"""

import copy
from dataclasses import replace

import pytest
from eth_abi import encode
from eth_account.messages import encode_typed_data
from eth_utils import keccak
from mppi_protocol.capabilities import WalletCall
from mppi_protocol.catalog import Catalog, parse_catalog
from mppi_protocol.chain import ChainEvidenceError, ChainReader, CodePin, DeploymentProfile
from mppi_protocol.errors import ValidationError
from mppi_protocol.registry import RegistryCalls, RegistryKind, RegistryReader, agent_wallet_proof
from mppi_protocol.signing import voucher_digest
from mppi_protocol.types import Domain, OperationState

CHAIN = 5042002
PROXY = b"\x99" * 20
IMPLEMENTATION = b"\x98" * 20
REGISTRY = b"\x97" * 20
REGISTRY_IMPLEMENTATION = b"\x96" * 20
OWNER = b"\x11" * 20
RECIPIENT = b"\x22" * 20
TOKEN = bytes.fromhex("3600000000000000000000000000000000000000")
DOMAIN = Domain(CHAIN, PROXY)
TX = b"\xaa" * 32
HASH = b"\xbb" * 32
TIME = 1790331000
CATALOG = b'[{"input":"2","model":"example/model","output":"3"}]'
URI = "https://provider.example/discover"


def hx(value):
    return "0x" + value.hex()


def abi(signature, types=(), values=()):
    return keccak(text=signature)[:4] + encode(types, values)


def event(signature, topics, types=(), values=(), *, index=0):
    return {
        "address": hx(REGISTRY),
        "topics": [hx(keccak(text=signature)), *map(hx, topics)],
        "data": hx(encode(types, values)),
        "logIndex": hex(index),
        "transactionIndex": "0x2",
        "transactionHash": hx(TX),
        "blockNumber": "0x64",
        "blockHash": hx(HASH),
        "removed": False,
    }


def metadata(agent_id, key, value, *, index=2):
    return event(
        "MetadataSet(uint256,string,string,bytes)",
        [encode(["uint256"], [agent_id]), keccak(text=key)],
        ["string", "bytes"],
        [key, value],
        index=index,
    )


def registered(agent_id=0, *, owner=OWNER, uri=""):
    return [
        event(
            "Transfer(address,address,uint256)",
            [
                encode(["address"], [bytes(20)]),
                encode(["address"], [owner]),
                encode(["uint256"], [agent_id]),
            ],
        ),
        event(
            "Registered(uint256,string,address)",
            [encode(["uint256"], [agent_id]), encode(["address"], [owner])],
            ["string"],
            [uri],
            index=1,
        ),
        metadata(agent_id, "agentWallet", owner),
    ]


class Rpc:
    def __init__(self):
        self.calls = []
        self.responses = {}
        self.receipt = {
            "transactionHash": hx(TX),
            "transactionIndex": "0x2",
            "status": "0x1",
            "blockNumber": "0x64",
            "blockHash": hx(HASH),
            "logs": registered(),
        }
        self.block = {
            "number": "0x64",
            "hash": hx(HASH),
            "parentHash": hx(b"\xcc" * 32),
            "timestamp": hex(TIME),
        }
        self.code = {
            hx(address): hx(bytes([i]) + b"runtime")
            for i, address in enumerate(
                (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION), start=1
            )
        }
        self.domain = [b"\x0f", "ERC8004IdentityRegistry", "1", CHAIN, REGISTRY, bytes(32), []]
        self.reorg = False
        self.simulated_sender = None
        self.simulated_data = None
        self.simulated_result = b""
        self.identity()
        self.response(
            PROXY,
            "voucherDigest(bytes32,uint64)",
            ["bytes32", "uint64"],
            [bytes(32), 0],
            ["bytes32"],
            [voucher_digest(DOMAIN, bytes(32), 0)],
        )
        self.response(TOKEN, "decimals()", [], [], ["uint8"], [6])
        self.response(TOKEN, "balanceOf(address)", ["address"], [OWNER], ["uint256"], [5_510_000])

    def response(self, address, signature, inputs, values, outputs, result):
        self.responses[(hx(address), hx(abi(signature, inputs, values)))] = hx(
            encode(outputs, result)
        )

    def identity(self, agent_id=0, *, owner=OWNER, wallet=OWNER, uri="", catalog=b""):
        for name, result_type, value in (
            ("ownerOf", "address", owner),
            ("getAgentWallet", "address", wallet),
            ("tokenURI", "string", uri),
        ):
            self.response(
                REGISTRY, name + "(uint256)", ["uint256"], [agent_id], [result_type], [value]
            )
        self.response(
            REGISTRY,
            "getMetadata(uint256,string)",
            ["uint256", "string"],
            [agent_id, "mppi.catalog"],
            ["bytes"],
            [catalog],
        )

    def call(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        if method == "eth_chainId":
            assert params == []
            return hex(CHAIN)
        if method == "eth_getBlockByNumber":
            assert params in (["latest", False], ["0x64", False])
            return self.block | ({"hash": hx(b"\xdd" * 32)} if self.reorg else {})
        if method == "eth_getTransactionReceipt":
            assert params == [hx(TX)]
            return copy.deepcopy(self.receipt)
        assert params[-1] == {"blockHash": hx(HASH), "requireCanonical": True}
        if method == "eth_getCode":
            return self.code[params[0]]
        if method == "eth_getStorageAt":
            assert params[1] == "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
            return hx(
                bytes(12) + (IMPLEMENTATION if params[0] == hx(PROXY) else REGISTRY_IMPLEMENTATION)
            )
        if method == "eth_call":
            call = params[0]
            if "from" in call:
                assert call == {
                    "from": hx(self.simulated_sender),
                    "to": hx(REGISTRY),
                    "data": hx(self.simulated_data),
                    "value": "0x0",
                }
                return hx(self.simulated_result)
            assert set(call) == {"to", "data"}
            if call["data"] == hx(abi("eip712Domain()")):
                return hx(
                    encode(
                        [
                            "bytes1",
                            "string",
                            "string",
                            "uint256",
                            "address",
                            "bytes32",
                            "uint256[]",
                        ],
                        self.domain,
                    )
                )
            return self.responses[(call["to"], call["data"])]
        raise AssertionError(method)


@pytest.fixture
def context():
    rpc = Rpc()
    pins = [
        CodePin(address, keccak(bytes.fromhex(rpc.code[hx(address)][2:])))
        for address in (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION)
    ]
    reader = RegistryReader(ChainReader(rpc, DeploymentProfile(DOMAIN, REGISTRY, *pins)))
    return reader, rpc, RegistryCalls(CHAIN, REGISTRY)


def reconcile(reader, operation):
    return reader.reconcile(
        operation, operation_ref="stable-ref", tx_hash=TX, wallet_operation_id="wallet-17"
    )


def test_register_exact_overloads_no_catalog(context):
    _, _, calls = context
    assert calls.register(OWNER).call.data == abi("register()")
    assert calls.register(OWNER, agent_uri=URI).call.data == abi(
        "register(string)", ["string"], [URI]
    )
    assert calls.register(OWNER, agent_uri="").call.data != calls.register(OWNER).call.data
    assert calls.register(OWNER).catalog_bytes is None


def test_registration_zero_id_is_canonical_identity(context):
    reader, _, calls = context
    result = reconcile(reader, calls.register(OWNER))
    assert result.operation.state is OperationState.CONFIRMED
    assert result.identity.agent_id == 0
    assert result.identity.agent_wallet == OWNER
    assert result.operation.operation_ref == "stable-ref"
    assert result.operation.wallet_operation_id == "wallet-17"
    assert result.operation.observation.block_hash == HASH


def test_provider_uri_registration(context):
    reader, rpc, calls = context
    agent_id = (1 << 200) + 17
    rpc.identity(agent_id, uri=URI)
    rpc.receipt["logs"] = registered(agent_id, uri=URI)
    result = reconcile(reader, calls.register(OWNER, agent_uri=URI))
    assert result.operation.state is OperationState.CONFIRMED
    assert result.identity.agent_id == agent_id and result.identity.agent_uri == URI


@pytest.mark.parametrize(
    "mutation",
    [
        "silent",
        "missing_mint",
        "missing_wallet",
        "wrong_owner",
        "wrong_uri",
        "ambiguous",
        "wrong_emitter",
        "packed_32",
        "catalog_present",
        "wallet_cleared",
        "owner_changed",
        "removed",
        "wrong_hash",
        "wrong_tx",
        "out_of_order",
        "trailing_abi",
    ],
)
def test_registration_needs_exact_events_and_matching_state(context, mutation):
    reader, rpc, calls = context
    if mutation == "silent":
        rpc.receipt["logs"] = []
    elif mutation == "missing_mint":
        rpc.receipt["logs"].pop(0)
    elif mutation == "missing_wallet":
        rpc.receipt["logs"].pop()
    elif mutation == "wrong_owner":
        rpc.receipt["logs"] = registered(owner=RECIPIENT)
    elif mutation == "wrong_uri":
        rpc.receipt["logs"] = registered(uri=URI)
    elif mutation == "ambiguous":
        rpc.receipt["logs"] += [
            log | {"logIndex": hex(i + 3)} for i, log in enumerate(registered(1))
        ]
    elif mutation == "wrong_emitter":
        rpc.receipt["logs"] = [log | {"address": hx(PROXY)} for log in rpc.receipt["logs"]]
    elif mutation == "packed_32":
        rpc.receipt["logs"][2] = metadata(0, "agentWallet", encode(["address"], [OWNER]))
    elif mutation == "catalog_present":
        rpc.identity(catalog=CATALOG)
    elif mutation == "wallet_cleared":
        rpc.identity(wallet=bytes(20))
    elif mutation == "owner_changed":
        rpc.identity(owner=RECIPIENT)
    elif mutation == "removed":
        rpc.receipt["logs"][1]["removed"] = True
    elif mutation == "wrong_hash":
        rpc.receipt["blockHash"] = hx(b"\xdd" * 32)
    elif mutation == "wrong_tx":
        rpc.receipt["logs"][1]["transactionHash"] = hx(b"\xdd" * 32)
    elif mutation == "out_of_order":
        rpc.receipt["logs"].reverse()
    else:
        rpc.receipt["logs"][1]["data"] += "00" * 32
    result = reconcile(reader, calls.register(OWNER))
    assert result.operation.state is OperationState.UNKNOWN and result.identity is None
    assert result.operation.tx_hash == TX and result.operation.wallet_operation_id == "wallet-17"


def test_pending_failed_and_unknown_preserve_identifiers(context):
    reader, rpc, calls = context
    operation = calls.register(OWNER)
    rpc.receipt["status"] = "0x0"
    assert reconcile(reader, operation).operation.state is OperationState.FAILED
    rpc.receipt = None
    assert reconcile(reader, operation).operation.state is OperationState.PENDING
    result = reader.reconcile(
        operation, operation_ref="stable-ref", tx_hash=None, wallet_operation_id="wallet-17"
    )
    assert result.operation.state is OperationState.UNKNOWN
    assert result.operation.wallet_operation_id == "wallet-17"


def test_read_inactive_identity_and_six_decimal_balance(context):
    reader, rpc, _ = context
    rpc.identity(wallet=bytes(20), catalog=b"temporarily-invalid-offering")
    block = reader.verified_block()
    identity = reader.read(0, block=block)
    assert identity.agent_wallet == bytes(20) and identity.agent_uri == ""
    assert identity.catalog_bytes == b"temporarily-invalid-offering"
    assert reader.balance(OWNER, block=block) == 5_510_000
    assert all(method != "eth_getBalance" for method, _ in rpc.calls)


def test_balance_rejects_native_unit_confusion(context):
    reader, rpc, _ = context
    rpc.response(TOKEN, "decimals()", [], [], ["uint8"], [18])
    with pytest.raises(ChainEvidenceError, match="decimals"):
        reader.balance(OWNER)


@pytest.mark.parametrize(
    "kind", [RegistryKind.SET_URI, RegistryKind.SET_AGENT_WALLET, RegistryKind.SET_CATALOG]
)
def test_updates_require_exact_event_and_state(context, kind):
    reader, rpc, calls = context
    if kind is RegistryKind.SET_URI:
        operation = calls.set_uri(OWNER, 0, URI)
        rpc.identity(uri=URI)
        rpc.receipt["logs"] = [
            event(
                "URIUpdated(uint256,string,address)",
                [bytes(32), encode(["address"], [OWNER])],
                ["string"],
                [URI],
            )
        ]
        expected = abi("setAgentURI(uint256,string)", ["uint256", "string"], [0, URI])
    elif kind is RegistryKind.SET_AGENT_WALLET:
        operation = calls.set_agent_wallet(OWNER, 0, RECIPIENT, TIME + 300, b"contract-signature")
        rpc.identity(wallet=RECIPIENT)
        rpc.receipt["logs"] = [metadata(0, "agentWallet", RECIPIENT)]
        expected = abi(
            "setAgentWallet(uint256,address,uint256,bytes)",
            ["uint256", "address", "uint256", "bytes"],
            [0, RECIPIENT, TIME + 300, b"contract-signature"],
        )
    else:
        operation = calls.set_catalog(OWNER, 0, parse_catalog(CATALOG))
        rpc.identity(catalog=CATALOG)
        rpc.receipt["logs"] = [metadata(0, "mppi.catalog", CATALOG)]
        expected = abi(
            "setMetadata(uint256,string,bytes)",
            ["uint256", "string", "bytes"],
            [0, "mppi.catalog", CATALOG],
        )
    assert operation.call.data == expected
    assert reconcile(reader, operation).operation.state is OperationState.CONFIRMED
    rpc.receipt["logs"] = []
    assert reconcile(reader, operation).operation.state is OperationState.UNKNOWN


@pytest.mark.parametrize("deadline_offset", [-1, 301])
def test_proof_deadline_checked_against_canonical_block(context, deadline_offset):
    reader, _, _ = context
    with pytest.raises(ChainEvidenceError, match="five-minute"):
        reader.wallet_proof(0, RECIPIENT, owner=OWNER, deadline=TIME + deadline_offset)


@pytest.mark.parametrize("deadline_offset", [0, 300])
def test_recipient_proof_matches_independent_eip712(context, deadline_offset):
    reader, _, _ = context
    deadline = TIME + deadline_offset
    proof = reader.wallet_proof(0, RECIPIENT, owner=OWNER, deadline=deadline)
    signed = encode_typed_data(full_message=proof)
    expected_struct = keccak(
        encode(
            ["bytes32", "uint256", "address", "address", "uint256"],
            [
                keccak(
                    text="AgentWalletSet(uint256 agentId,address newWallet,address owner,uint256 deadline)"
                ),
                0,
                RECIPIENT,
                OWNER,
                deadline,
            ],
        )
    )
    expected_domain = keccak(
        encode(
            ["bytes32", "bytes32", "bytes32", "uint256", "address"],
            [
                keccak(
                    text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
                ),
                keccak(text="ERC8004IdentityRegistry"),
                keccak(text="1"),
                CHAIN,
                REGISTRY,
            ],
        )
    )
    assert signed.body == expected_struct and signed.header == expected_domain
    assert proof == agent_wallet_proof(CHAIN, REGISTRY, 0, RECIPIENT, OWNER, deadline)


@pytest.mark.parametrize(
    "index,value",
    [(0, b"\x1f"), (1, "Other"), (2, "2"), (3, 5042), (4, PROXY), (5, b"\x01" * 32), (6, [1])],
)
def test_proof_rejects_wrong_registry_domain(context, index, value):
    reader, rpc, _ = context
    rpc.domain[index] = value
    with pytest.raises(ChainEvidenceError, match="domain"):
        reader.wallet_proof(0, RECIPIENT, owner=OWNER, deadline=TIME + 300)


def test_approved_operator_is_not_owner_for_mppi(context):
    reader, _, calls = context
    with pytest.raises(ChainEvidenceError, match="owner"):
        reader.wallet_proof(0, RECIPIENT, owner=RECIPIENT, deadline=TIME + 300)
    with pytest.raises(ChainEvidenceError, match="owner"):
        reader.simulate(calls.set_uri(RECIPIENT, 0, URI))


@pytest.mark.parametrize("register", [True, False])
def test_simulate_exact_wallet_call_is_read_only(context, register):
    reader, rpc, calls = context
    operation = (
        calls.register(OWNER)
        if register
        else calls.set_agent_wallet(OWNER, 0, RECIPIENT, TIME + 300, b"contract-signature")
    )
    rpc.simulated_sender = OWNER
    rpc.simulated_data = operation.call.data
    rpc.simulated_result = encode(["uint256"], [1001]) if register else b""
    assert reader.simulate(operation).observation.block_hash == HASH
    assert all(not method.startswith("eth_send") for method, _ in rpc.calls)


def test_call_cannot_be_substituted_or_extended(context):
    reader, _, calls = context
    operation = calls.register(OWNER)
    with pytest.raises(ValidationError, match="calldata"):
        replace(operation, call=WalletCall(CHAIN, REGISTRY, b"bad"))
    with pytest.raises(ValidationError):
        replace(operation, catalog_bytes=CATALOG)
    with pytest.raises(ChainEvidenceError, match="profile"):
        reader.reconcile(
            replace(operation, call=WalletCall(CHAIN, PROXY, operation.call.data)),
            operation_ref="x",
            tx_hash=TX,
        )
    with pytest.raises(ValidationError):
        calls.set_catalog(OWNER, 0, Catalog((), CATALOG, bytes(32)))


def test_deployment_change_and_reorg_do_not_confirm(context):
    reader, rpc, calls = context
    rpc.code[hx(REGISTRY_IMPLEMENTATION)] = "0x6000"
    assert reconcile(reader, calls.register(OWNER)).operation.state is OperationState.UNKNOWN
    rpc.reorg = True
    assert reconcile(reader, calls.register(OWNER)).operation.state is OperationState.UNKNOWN


def test_bounded_proof_and_uri_before_abi_allocation(context):
    _, _, calls = context
    with pytest.raises(ValidationError):
        calls.register(OWNER, agent_uri="a" * ((1 << 20) + 1))
    with pytest.raises(ValidationError):
        calls.set_agent_wallet(OWNER, 0, RECIPIENT, TIME + 300, b"x" * ((64 << 10) + 1))
    with pytest.raises(ValidationError):
        calls.register(OWNER, agent_uri="\ud800")


def test_indexed_metadata_key_must_match_data(context):
    reader, rpc, calls = context
    rpc.receipt["logs"][2]["topics"][2] = hx(keccak(text="mppi.catalog"))
    assert reconcile(reader, calls.register(OWNER)).operation.state is OperationState.UNKNOWN


def test_other_users_non_utf8_registry_strings_do_not_poison_bundled_receipt(context):
    reader, rpc, calls = context
    # Solidity string has arbitrary bytes; another account can register these
    # successfully in the same ERC-4337 transaction as our valid registration.
    foreign = event(
        "Registered(uint256,string,address)",
        [encode(["uint256"], [99]), encode(["address"], [RECIPIENT])],
        ["bytes"],
        [b"\xff"],
        index=3,
    )
    foreign_metadata = event(
        "MetadataSet(uint256,string,string,bytes)",
        [encode(["uint256"], [99]), keccak(b"\xff")],
        ["bytes", "bytes"],
        [b"\xff", b"arbitrary"],
        index=4,
    )
    rpc.receipt["logs"].extend([foreign, foreign_metadata])
    assert reconcile(reader, calls.register(OWNER)).operation.state is OperationState.CONFIRMED
