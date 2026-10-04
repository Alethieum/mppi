"""Public Python payment bindings against an owned ephemeral Anvil process.

Run inside the project's Apple Linux container with pinned Foundry 1.8.3. The
existing local USDC/registry fixtures model contract boundaries; this does not
claim Arc consensus, native USDC, a real wallet service, or inference runtime.
No endpoint, fork, secret, or network argument is accepted.

Command behavior: Foundry v1.8.3 crates/anvil/src/cmd.rs and
crates/forge/src/cmd/build.rs at https://github.com/foundry-rs/foundry/tree/v1.8.3.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from eth_abi import decode, encode
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak, to_checksum_address
from mppi_protocol.capabilities import FundingAuthorization
from mppi_protocol.chain import ChainReader, CodePin, DeploymentProfile, HttpJsonRpc
from mppi_protocol.contract import ContractCalls, PreparedOperation
from mppi_protocol.signing import LocalSessionSigner, session_id
from mppi_protocol.types import Domain, OperationState, SessionDescriptor, SessionStatus

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"
CHAIN_ID = 5042002
GENESIS = 1_790_331_000
USDC = bytes.fromhex("3600000000000000000000000000000000000000")
CATALOG = b'[{"input":"1","model":"test/model","output":"2"}]'


def hx(value: bytes) -> str:
    return "0x" + value.hex()


def raw(value: str) -> bytes:
    return bytes.fromhex(value.removeprefix("0x"))


def abi_type(field: dict) -> str:
    value = field["type"]
    if value.startswith("tuple"):
        return "(" + ",".join(abi_type(child) for child in field["components"]) + ")" + value[5:]
    return value


class Artifact:
    def __init__(self, name: str) -> None:
        self.data = json.loads((CONTRACTS / "out" / (name + ".sol") / (name + ".json")).read_text())
        self.abi = self.data["abi"]

    def call(self, name: str, values: tuple) -> bytes:
        entry = next(
            item for item in self.abi if item["type"] == "function" and item["name"] == name
        )
        types = [abi_type(item) for item in entry["inputs"]]
        return keccak(text=name + "(" + ",".join(types) + ")")[:4] + encode(types, values)

    def creation(self, values: tuple = ()) -> bytes:
        constructor = next(
            (item for item in self.abi if item["type"] == "constructor"), {"inputs": []}
        )
        return raw(self.data["bytecode"]["object"]) + encode(
            [abi_type(item) for item in constructor["inputs"]], values
        )


class OwnedAnvil:
    def __init__(self, process: subprocess.Popen, port: int) -> None:
        self.process, self.port = process, port
        self.endpoint = f"http://127.0.0.1:{port}"
        self.counter = 0
        # Deliberately public trivial keys; they exist only in this disposable node.
        self.deployer = Account.from_key((1).to_bytes(32, "big"))
        self.agent = Account.from_key((2).to_bytes(32, "big"))
        self.operator = Account.from_key((3).to_bytes(32, "big"))
        self.recipient = Account.from_key((4).to_bytes(32, "big"))

    def call(self, method: str, params: list) -> object:
        if self.process.poll() is not None:
            raise RuntimeError("owned Anvil process exited")
        self.counter += 1
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            connection.request(
                "POST",
                "/",
                json.dumps(
                    {"jsonrpc": "2.0", "id": self.counter, "method": method, "params": params}
                ),
                {"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            assert response.status == 200
            body = response.read(1_048_577)
            assert len(body) <= 1_048_576
            document = json.loads(body)
            assert document["jsonrpc"] == "2.0" and document["id"] == self.counter
            assert "error" not in document, document.get("error")
            return document["result"]
        finally:
            connection.close()

    def timestamp(self) -> int:
        return int(self.call("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)

    def send(self, account, destination: bytes | None, data: bytes) -> dict:
        self.call("evm_setNextBlockTimestamp", [self.timestamp() + 1])
        transaction = {
            "chainId": CHAIN_ID,
            "nonce": int(self.call("eth_getTransactionCount", [account.address, "pending"]), 16),
            "gas": 12_000_000,
            "gasPrice": int(self.call("eth_gasPrice", []), 16),
            "value": 0,
            "data": data,
        }
        if destination is not None:
            transaction["to"] = to_checksum_address(destination)
        signed = account.sign_transaction(transaction)
        transaction_hash = self.call("eth_sendRawTransaction", [hx(signed.raw_transaction)])
        assert transaction_hash == hx(keccak(signed.raw_transaction))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            receipt = self.call("eth_getTransactionReceipt", [transaction_hash])
            if receipt is not None:
                assert receipt["status"] == "0x1", receipt
                assert receipt["transactionHash"] == transaction_hash
                return receipt
            time.sleep(0.02)
        raise AssertionError("local transaction was not mined")

    def deploy(self, artifact: Artifact, values: tuple = ()) -> bytes:
        receipt = self.send(self.deployer, None, artifact.creation(values))
        address = raw(receipt["contractAddress"])
        assert self.call("eth_getCode", [hx(address), "latest"]) != "0x"
        return address

    def uint(self, address: bytes, artifact: Artifact, name: str, values: tuple) -> int:
        result = self.call(
            "eth_call", [{"to": hx(address), "data": hx(artifact.call(name, values))}, "latest"]
        )
        return decode(["uint256"], raw(result))[0]


@pytest.fixture
def node(tmp_path: Path) -> Iterator[OwnedAnvil]:
    assert sys.platform == "linux", (
        "execute this integration inside the project's Apple Linux container"
    )
    # Pytest precedes forge test in the public check script; build the current
    # sources here, allowing Forge's source-aware cache to avoid duplicate work.
    subprocess.run(["forge", "build"], cwd=CONTRACTS, check=True, capture_output=True, timeout=180)
    with socket.socket() as available:
        available.bind(("127.0.0.1", 0))
        port = available.getsockname()[1]
    command = [
        "anvil",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--chain-id",
        str(CHAIN_ID),
        "--hardfork",
        "paris",
        "--timestamp",
        str(GENESIS),
        "--accounts",
        "0",
        "--gas-limit",
        "30000000",
        "--silent",
    ]
    # No inherited RPC/fork/proxy environment and no public test keys in logs.
    with (tmp_path / "anvil.log").open("wb") as log:
        process = subprocess.Popen(
            command,
            cwd=tmp_path,
            env={"PATH": os.environ["PATH"]},
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            network = OwnedAnvil(process, port)
            deadline = time.monotonic() + 10
            while True:
                try:
                    client = network.call("web3_clientVersion", [])
                    break
                except (ConnectionError, OSError):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.05)
            assert "anvil" in client.lower()
            assert network.call("eth_chainId", []) == hex(CHAIN_ID)
            assert network.call("eth_blockNumber", []) == "0x0"
            assert network.timestamp() == GENESIS
            assert network.call("eth_accounts", []) == []
            for account in (network.deployer, network.agent, network.operator):
                network.call("anvil_setBalance", [account.address, hex(100 * 10**18)])
            yield network
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def funding_signature(node: OwnedAnvil, authorization: FundingAuthorization) -> bytes:
    typed = encode_typed_data(
        {
            "name": authorization.domain_name,
            "version": authorization.domain_version,
            "chainId": authorization.chain_id,
            "verifyingContract": hx(authorization.token),
        },
        {
            "ReceiveWithAuthorization": [
                {"name": "from", "type": "address"},
                {"name": "to", "type": "address"},
                {"name": "value", "type": "uint256"},
                {"name": "validAfter", "type": "uint256"},
                {"name": "validBefore", "type": "uint256"},
                {"name": "nonce", "type": "bytes32"},
            ]
        },
        {
            "from": hx(authorization.owner),
            "to": hx(authorization.recipient),
            "value": authorization.value,
            "validAfter": authorization.valid_after,
            "validBefore": authorization.valid_before,
            "nonce": authorization.nonce,
        },
    )
    return bytes(node.agent.sign_message(typed).signature)


def test_python_bindings_fund_capture_reconcile_and_refund_real_evm(node: OwnedAnvil) -> None:
    mppi, proxy_artifact = Artifact("MPPI"), Artifact("ERC1967Proxy")
    registry_artifact, token = Artifact("MockIdentityRegistry"), Artifact("MockUSDC")
    assert mppi.abi == json.loads((CONTRACTS / "abi/MPPI.json").read_text())
    registry_implementation = node.deploy(registry_artifact)
    # Reuse the existing fixture behind a real proxy, preserving the production
    # profile's two ERC-1967 implementation checks without inventing a new mock.
    registry_setup = registry_artifact.call(
        "setProvider", (42, node.operator.address, node.recipient.address, CATALOG)
    )
    registry = node.deploy(proxy_artifact, (registry_implementation, registry_setup))
    timelock = node.deploy(
        Artifact("TimelockController"), (60, [node.deployer.address], [bytes(20)], bytes(20))
    )
    implementation = node.deploy(mppi)
    initializer = mppi.call(
        "initialize", (registry, node.deployer.address, node.deployer.address, timelock)
    )
    proxy = node.deploy(proxy_artifact, (implementation, initializer))
    template = node.deploy(token)
    node.call("anvil_setCode", [hx(USDC), node.call("eth_getCode", [hx(template), "latest"])])
    node.send(node.deployer, USDC, token.call("mint", (node.agent.address, 6_000_000)))

    def pin(address: bytes) -> CodePin:
        return CodePin(address, keccak(raw(node.call("eth_getCode", [hx(address), "latest"]))))

    domain = Domain(CHAIN_ID, proxy)
    profile = DeploymentProfile(
        domain,
        registry,
        pin(proxy),
        pin(implementation),
        pin(registry),
        pin(registry_implementation),
    )
    reader = ChainReader(HttpJsonRpc(node.endpoint), profile)
    reader.verify_deployment(reader.block())
    signer = LocalSessionSigner((7).to_bytes(32, "big"))
    descriptor = SessionDescriptor(
        raw(node.agent.address),
        signer.address,
        42,
        raw(node.recipient.address),
        raw(node.operator.address),
        USDC,
        b"\x44" * 32,
    )
    identifier = session_id(domain, descriptor)
    assert descriptor.agent != descriptor.session_signer
    assert node.call("eth_getBalance", [hx(signer.address), "latest"]) == "0x0"
    calls = ContractCalls(domain, "USDC", "2")

    def authorization(amount: int, nonce: int) -> FundingAuthorization:
        return FundingAuthorization(
            CHAIN_ID,
            USDC,
            "USDC",
            "2",
            descriptor.agent,
            proxy,
            amount,
            node.timestamp() - 1,
            node.timestamp() + 3600,
            nonce.to_bytes(32, "big"),
        )

    def execute(account, operation: PreparedOperation, label: str):
        assert operation.call.chain_id == CHAIN_ID and operation.call.to == proxy
        receipt = node.send(account, operation.call.to, operation.call.data)
        result = reader.reconcile(
            operation, operation_ref=label, tx_hash=raw(receipt["transactionHash"])
        )
        assert result.observation.block_number == int(receipt["blockNumber"], 16)
        assert result.observation.block_hash == raw(receipt["blockHash"])
        return result

    first_auth = authorization(5_510_000, 1)
    opening = calls.open(
        descriptor,
        first_auth.value,
        keccak(CATALOG),
        first_auth,
        funding_signature(node, first_auth),
    )
    opened = execute(node.agent, opening, "local-open")
    assert opened.state is OperationState.CONFIRMED
    assert opening.session_ids == (identifier,)
    state = reader.session(identifier, reader.block())
    assert state.deposit == 5_510_000 and state.settled_cumulative == 0
    assert state.status is SessionStatus.OPEN and state.initial_rate_card_hash == keccak(CATALOG)
    assert node.uint(USDC, token, "balanceOf", (proxy,)) == 5_510_000
    assert node.uint(USDC, token, "balanceOf", (descriptor.agent,)) == 490_000
    assert node.uint(USDC, token, "authorizationState", (descriptor.agent, first_auth.nonce)) == 1

    top_up_auth = authorization(490_000, 2)
    top_up = calls.top_up(
        descriptor, top_up_auth.value, top_up_auth, funding_signature(node, top_up_auth)
    )
    assert execute(node.agent, top_up, "local-top-up").state is OperationState.CONFIRMED
    assert reader.session(identifier, reader.block()).deposit == 6_000_000
    assert node.uint(USDC, token, "balanceOf", (descriptor.agent,)) == 0

    voucher = signer.sign_voucher(domain, identifier, 5_510_000)
    capture = calls.settle(descriptor, 5_510_000, 5_420_000, voucher)
    collected = execute(node.operator, capture, "local-capture")
    assert collected.state is OperationState.CONFIRMED
    evidence = collected.sessions[0]
    assert evidence.target_covered is True
    assert evidence.settlement_reference.delta == 5_420_000
    assert evidence.settlement_reference.settled_cumulative == 5_420_000
    paid_state = reader.session(identifier, reader.block())
    assert paid_state.status is SessionStatus.OPEN and paid_state.settled_cumulative == 5_420_000
    assert node.uint(USDC, token, "balanceOf", (descriptor.recipient,)) == 5_420_000
    assert node.uint(USDC, token, "balanceOf", (proxy,)) == 580_000

    repeated = execute(node.operator, capture, "local-repeat")
    assert repeated.state is OperationState.UNKNOWN
    assert repeated.sessions[0].target_covered is True
    assert repeated.sessions[0].settlement_reference is None
    assert reader.session(identifier, reader.block()) == paid_state
    assert node.uint(USDC, token, "balanceOf", (descriptor.recipient,)) == 5_420_000

    close = calls.close(descriptor, 5_510_000, 5_420_000, voucher)
    assert execute(node.operator, close, "local-close").state is OperationState.CONFIRMED
    closed = reader.session(identifier, reader.block())
    assert closed.status is SessionStatus.CLOSED and closed.settled_cumulative == 5_420_000
    assert closed.deposit == 6_000_000 and closed.last_activity == paid_state.last_activity
    assert node.uint(USDC, token, "balanceOf", (proxy,)) == 0
    assert node.uint(USDC, token, "balanceOf", (descriptor.agent,)) == 580_000
    assert node.uint(USDC, token, "balanceOf", (descriptor.recipient,)) == 5_420_000
    assert node.uint(USDC, token, "fundingCalls", ()) == 2
    assert node.uint(USDC, token, "transferCalls", ()) == 2
