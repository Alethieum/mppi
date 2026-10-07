"""Exact registration activation and recovery; synthetic RPC/CLI, no custody."""

import copy
import json
from dataclasses import replace

import pytest
from eth_abi import encode
from eth_utils import keccak
from mppi_protocol.capabilities import FundingAuthorization, WalletCall
from mppi_protocol.chain import ChainReader, CodePin, DeploymentProfile
from mppi_protocol.circle import CircleAgentWallet, CircleAgentWalletError
from mppi_protocol.errors import ValidationError
from mppi_protocol.registry import RegistryCalls
from mppi_protocol.signing import voucher_digest
from mppi_protocol.types import Domain, OperationState

CHAIN = 5042002
PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPL, WALLET, WALLET_IMPL = (
    bytes([i]) * 20 for i in range(1, 7)
)
BLOCK_HASH = b"\xab" * 32
TX = b"\xcd" * 32
ID = "11111111-2222-4333-8444-555555555555"
DOMAIN = Domain(CHAIN, PROXY)
REGISTER = RegistryCalls(CHAIN, REGISTRY).register(WALLET).call


def hx(value):
    return "0x" + value.hex()


class Rpc:
    def __init__(self):
        self.codes = {
            address: address + b"runtime"
            for address in (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPL, WALLET, WALLET_IMPL)
        }
        self.wallet_code = self.codes[WALLET]
        self.slots = {PROXY: IMPLEMENTATION, REGISTRY: REGISTRY_IMPL, WALLET: WALLET_IMPL}
        self.calls = []

    def call(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        if method == "eth_chainId":
            return hex(CHAIN)
        if method == "eth_getBlockByNumber":
            return {
                "number": "0x64",
                "hash": hx(BLOCK_HASH),
                "parentHash": hx(b"\xbc" * 32),
                "timestamp": "0x1234",
            }
        assert params[-1] == {"blockHash": hx(BLOCK_HASH), "requireCanonical": True}
        if method == "eth_getCode":
            return hx(self.codes[bytes.fromhex(params[0][2:])])
        if method == "eth_getStorageAt":
            return hx(bytes(12) + self.slots[bytes.fromhex(params[0][2:])])
        if method == "eth_call":
            assert params[0] == {
                "to": hx(PROXY),
                "data": hx(
                    keccak(text="voucherDigest(bytes32,uint64)")[:4]
                    + encode(["bytes32", "uint64"], [bytes(32), 0])
                ),
            }
            return hx(encode(["bytes32"], [voucher_digest(DOMAIN, bytes(32), 0)]))
        raise AssertionError(method)


class Cli:
    def __init__(self, path, rpc):
        self.path, self.rpc = path, rpc
        self.calls = []
        self.wallets = [{"type": "agent", "address": hx(WALLET), "blockchain": "ARC-TESTNET"}]
        self.version = b"1.1.4\n"
        self.changes = {}
        self.pages = []
        self.fail = False
        self.deployment = rpc.wallet_code

    def transaction(self, route="direct_register", **changes):
        direct = route == "direct_register"
        return {
            "id": ID,
            "state": "CONFIRMED",
            "blockchain": "ARC-TESTNET",
            "txHash": hx(TX),
            "sourceAddress": hx(WALLET),
            "destinationAddress": hx(REGISTRY if direct else WALLET),
            "contractAddress": hx(REGISTRY if direct else WALLET),
            "operation": "CONTRACT_EXECUTION",
            "transactionType": "OUTBOUND",
            "abiFunctionSignature": "register()" if direct else "execute(address,uint256,bytes)",
            "abiParameters": [] if direct else [hx(REGISTRY), "0", hx(REGISTER.data)],
            **changes,
        }

    def __call__(self, arguments):
        self.calls.append(copy.deepcopy(arguments))
        if arguments == ["--version"]:
            return self.version
        assert arguments[-2:] == ["--output", "json"]
        args = arguments[:-2]
        if args[:2] == ["wallet", "list"]:
            assert args == ["wallet", "list", "--type", "agent", "--chain", "ARC-TESTNET"]
            data = {"wallets": self.wallets}
        elif args[:2] == ["wallet", "execute"]:
            row = json.loads(self.path.read_text())["operations"]["register-ref"]
            assert row["state"] == "unknown" and row["wallet_operation_id"] is None
            assert row["call"] == {"chain_id": CHAIN, "to": hx(REGISTRY), "data": hx(REGISTER.data)}
            route = row["route"]
            assert route in ("self", "direct_register")
            direct = route == "direct_register"
            execution = (
                ["register()", "--amount", "0"]
                if direct
                else ["execute(address,uint256,bytes)", hx(REGISTRY), "0", hx(REGISTER.data)]
            )
            assert args == [
                "wallet",
                "execute",
                *execution,
                "--contract",
                hx(REGISTRY if direct else WALLET),
                "--address",
                hx(WALLET),
                "--chain",
                "ARC-TESTNET",
                "--idempotency-key",
                row["idempotency_key"],
            ]
            self.rpc.codes[WALLET] = self.deployment
            if self.fail:
                raise CircleAgentWalletError("lost response")
            data = self.transaction(route, idempotencyKey=row["idempotency_key"], **self.changes)
        elif args[:2] == ["transaction", "list"]:
            assert args == [
                "transaction",
                "list",
                "--address",
                hx(WALLET),
                "--chain",
                "ARC-TESTNET",
                "--operation",
                "execute",
                "--tx-type",
                "outbound",
                "--limit",
                "50",
            ]
            data = {"transactions": self.pages}
        else:
            raise AssertionError(args)
        return json.dumps({"data": data}).encode()


@pytest.fixture
def activation(tmp_path, monkeypatch):
    rpc = Rpc()
    pins = [
        CodePin(a, keccak(rpc.codes[a])) for a in (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPL)
    ]
    chain = ChainReader(rpc, DeploymentProfile(DOMAIN, REGISTRY, *pins))
    options = {
        "chain": chain,
        "wallet": CodePin(WALLET, keccak(rpc.wallet_code)),
        "implementation": CodePin(WALLET_IMPL, keccak(rpc.codes[WALLET_IMPL])),
        "checkpoint_path": tmp_path / "circle.json",
        "allow_registration_activation": True,
    }
    cli = Cli(options["checkpoint_path"], rpc)

    def wallet(**changes):
        result = CircleAgentWallet(**(options | changes))
        monkeypatch.setattr(result, "_run", cli)
        return result

    rpc.codes[WALLET] = b""
    return wallet, rpc, cli, options


@pytest.mark.parametrize("already_deployed", [False, True])
def test_registration_deploys_once_and_reconciles_persisted_direct_route(
    activation, already_deployed
):
    create, rpc, cli, _ = activation
    if already_deployed:
        rpc.codes[WALLET] = rpc.wallet_code
    wallet = create()
    wallet.verify_registration_wallet()
    result = wallet.execute(REGISTER, operation_ref="register-ref")
    assert result.state is OperationState.PENDING and result.tx_hash == TX
    wallet.verify_wallet()
    cli.pages = [cli.transaction()]
    cli.calls.clear()
    # The opt-in authorizes new activation, not recovery of an existing intent.
    reopened = create(allow_registration_activation=False)
    assert reopened.execute(REGISTER, operation_ref="register-ref") == result
    assert not any(args[:2] == ["wallet", "execute"] for args in cli.calls)
    assert rpc.codes[WALLET] == rpc.wallet_code


@pytest.mark.parametrize(
    "call",
    [
        replace(REGISTER, to=PROXY),
        replace(REGISTER, data=REGISTER.data + b"\x00"),
        RegistryCalls(CHAIN, REGISTRY).register(WALLET, agent_uri="https://example.com").call,
        WalletCall(CHAIN, PROXY, b"\x01\x23\x45\x67"),
    ],
)
def test_undeployed_registration_permission_cannot_authorize_other_calls(activation, call):
    create, _, cli, _ = activation
    with pytest.raises(CircleAgentWalletError):
        create().execute(call, operation_ref="register-ref")
    assert not cli.path.exists()
    assert not any(args[:2] == ["wallet", "execute"] for args in cli.calls)


def test_activation_requires_explicit_boolean_and_does_not_enable_signing(activation):
    create, _, cli, _ = activation
    with pytest.raises(ValidationError):
        create(allow_registration_activation=1)
    with pytest.raises(CircleAgentWalletError, match="not enabled"):
        create(allow_registration_activation=False).verify_registration_wallet()
    with pytest.raises(CircleAgentWalletError):
        create(allow_registration_activation=False).execute(REGISTER, operation_ref="register-ref")
    wallet = create()
    with pytest.raises(CircleAgentWalletError):
        wallet.verify_wallet()
    auth = FundingAuthorization(
        CHAIN, bytes.fromhex("36" + "00" * 19), "USDC", "2", WALLET, PROXY, 1, 10, 20, b"\x33" * 32
    )
    with pytest.raises(CircleAgentWalletError):
        wallet.sign_funding_authorization(auth)
    assert not cli.path.exists()
    assert not any(args[:2] in (["wallet", "execute"], ["wallet", "sign"]) for args in cli.calls)


@pytest.mark.parametrize("problem", ["version", "account", "chain", "implementation", "registry"])
def test_activation_still_verifies_custody_network_and_trusted_code(activation, problem):
    create, rpc, cli, _ = activation
    if problem == "version":
        cli.version = b"1.1.5"
    elif problem == "account":
        cli.wallets[0]["type"] = "local"
    elif problem == "chain":
        cli.wallets[0]["blockchain"] = "ARC"
    else:
        rpc.codes[WALLET_IMPL if problem == "implementation" else REGISTRY] = b"changed"
    with pytest.raises(RuntimeError):
        create().execute(REGISTER, operation_ref="register-ref")
    assert not cli.path.exists()


@pytest.mark.parametrize(
    "changes",
    [
        {"sourceAddress": hx(PROXY)},
        {"blockchain": "ARC"},
        {"contractAddress": hx(WALLET)},
        {"destinationAddress": hx(WALLET)},
        {"abiFunctionSignature": "register(string)"},
        {"abiParameters": [""]},
        {"abiParameters": {}},
        {"transactionType": "INBOUND"},
        {"operation": "TRANSFER"},
        {"idempotencyKey": "wrong"},
    ],
)
def test_direct_registration_response_binds_original_intent(activation, changes):
    create, _, cli, _ = activation
    cli.changes = changes
    result = create().execute(REGISTER, operation_ref="register-ref")
    assert result.state is OperationState.UNKNOWN
    assert result.tx_hash is None and result.wallet_operation_id is None


def test_lost_response_does_not_use_deployment_or_matching_transaction_as_identity(activation):
    create, _, cli, _ = activation
    cli.fail = True
    result = create().execute(REGISTER, operation_ref="register-ref")
    assert result.state is OperationState.UNKNOWN and result.wallet_operation_id is None
    before = cli.path.read_bytes()
    cli.fail = False
    cli.pages = [cli.transaction()]
    cli.calls.clear()
    assert create().execute(REGISTER, operation_ref="register-ref") == result
    assert cli.calls == [] and cli.path.read_bytes() == before


def test_pending_registration_recovers_only_original_id_and_route(activation):
    create, _, cli, _ = activation
    cli.changes = {"txHash": None, "state": "SENT", "abiParameters": None}
    result = create().execute(REGISTER, operation_ref="register-ref")
    assert result.state is OperationState.PENDING and result.wallet_operation_id == ID
    cli.pages = [cli.transaction(id="22222222-2222-4333-8444-555555555555")]
    assert create().reconcile(operation_ref="register-ref").state is OperationState.UNKNOWN
    cli.pages = [cli.transaction("self")]
    assert create().reconcile(operation_ref="register-ref").state is OperationState.UNKNOWN
    cli.pages = [cli.transaction()]
    assert create().reconcile(operation_ref="register-ref").tx_hash == TX
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 1


def test_unsupported_deployed_code_preserves_handles_and_blocks_funding(activation):
    create, _, cli, _ = activation
    cli.deployment = b"unsupported account"
    wallet = create()
    result = wallet.execute(REGISTER, operation_ref="register-ref")
    assert result.state is OperationState.UNKNOWN
    assert result.tx_hash == TX and result.wallet_operation_id == ID
    cli.pages = [cli.transaction()]
    assert create().reconcile(operation_ref="register-ref") == result
    with pytest.raises(CircleAgentWalletError):
        wallet.verify_registration_wallet()
    with pytest.raises(CircleAgentWalletError):
        wallet.verify_wallet()
    with pytest.raises(CircleAgentWalletError):
        wallet.execute(WalletCall(CHAIN, PROXY, b"\x01\x23\x45\x67"), operation_ref="fund")
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 1


def test_legacy_registration_journal_remains_self_execution_after_opt_in(activation):
    create, rpc, cli, _ = activation
    rpc.codes[WALLET] = rpc.wallet_code
    original = create(allow_registration_activation=False).execute(
        REGISTER, operation_ref="register-ref"
    )
    record = json.loads(cli.path.read_text())
    del record["operations"]["register-ref"]["route"]
    cli.path.write_text(json.dumps(record))
    cli.pages = [cli.transaction("self")]
    assert create().execute(REGISTER, operation_ref="register-ref") == original
    cli.pages = [cli.transaction()]
    assert create().reconcile(operation_ref="register-ref").state is OperationState.UNKNOWN


@pytest.mark.parametrize("change", ["unknown_route", "wrong_call"])
def test_route_schema_rejects_invalid_or_expanded_registration_authority(activation, change):
    create, _, cli, _ = activation
    create().execute(REGISTER, operation_ref="register-ref")
    record = json.loads(cli.path.read_text())
    row = record["operations"]["register-ref"]
    if change == "unknown_route":
        row["route"] = "arbitrary_direct"
    else:
        row["call"]["to"] = hx(PROXY)
    cli.path.write_text(json.dumps(record))
    with pytest.raises(CircleAgentWalletError):
        create()
