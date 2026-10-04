"""Disposable local-chain fixture helpers; never use this wallet in production.

Each role receives only its own generated key through a private local mount.
The RPC write adapter accepts only a private IP literal on the owned test chain.
"""

from __future__ import annotations

import importlib.util
import ipaddress
import json
import threading
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit

from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import to_checksum_address
from mppi_protocol.chain import ChainReader, CodePin, DeploymentProfile, HttpJsonRpc
from mppi_protocol.types import Domain, OperationResult, OperationState

CHAIN_ID = 5042002
USDC = bytes.fromhex("3600000000000000000000000000000000000000")
MODEL = "fixture/model"
CATALOG = b'[{"input":"1000000000","model":"fixture/model","output":"1000000000"}]'


def hx(value: bytes) -> str:
    return "0x" + value.hex()


def raw(value: str) -> bytes:
    return bytes.fromhex(value.removeprefix("0x"))


def write_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def role_evidence(role: str) -> dict:
    other = "provider" if role == "agent" else "agent"
    assert importlib.util.find_spec("mppi_" + other) is None
    own = importlib.util.find_spec("mppi_" + role)
    assert own is not None and own.origin is not None and "/site-packages/" in own.origin
    return {"role": role, "installed_module": own.origin, "opposite_role_absent": True}


def reader_for(config: dict) -> ChainReader:
    pins = [CodePin(raw(item["address"]), raw(item["code_hash"])) for item in config["pins"]]
    profile = DeploymentProfile(Domain(CHAIN_ID, pins[0].address), pins[2].address, *pins)
    return ChainReader(HttpJsonRpc(config["rpc"]), profile)


def onboarding_wallet(profile: DeploymentProfile) -> LocalWallet:
    """Trusted installed-CLI fixture factory; only this agent's key is mounted."""
    settings = json.loads(Path("/input/config.json").read_text())
    assert reader_for(settings).profile == profile
    assert settings.get("onboarding") is True
    return LocalWallet(settings, "wallet_key")


class LocalRpc:
    def __init__(self, endpoint: str) -> None:
        url = urlsplit(endpoint)
        assert url.scheme == "http" and url.hostname is not None and url.port is not None
        assert ipaddress.ip_address(url.hostname).is_private
        assert not url.username and not url.password and url.path in ("", "/")
        self.endpoint = endpoint
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        assert self.call("eth_chainId", []) == hex(CHAIN_ID)
        assert "anvil" in str(self.call("web3_clientVersion", [])).lower()

    def call(self, method: str, params: list) -> object:
        request = urllib.request.Request(
            self.endpoint,
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
            {"Content-Type": "application/json"},
        )
        with self.opener.open(request, timeout=5) as response:
            body = response.read(1_048_577)
        assert len(body) <= 1_048_576
        value = json.loads(body)
        assert value["jsonrpc"] == "2.0" and value["id"] == 1 and "error" not in value, value
        return value["result"]


class LocalWallet:
    """One operation reference maps to exactly one submitted signed transaction.

    This fixture has process-local state; production custody and durable
    idempotency belong to the caller's wallet adapter.
    """

    def __init__(self, config: dict, key_field: str) -> None:
        self.rpc = LocalRpc(config["rpc"])
        self.account = Account.from_key(raw(config[key_field]))
        self.results: dict[str, tuple[bytes, OperationResult]] = {}
        self.lock = threading.Lock()
        self.submissions = 0
        self.authorization_count = 0

    @property
    def address(self) -> bytes:
        return raw(self.account.address)

    def send(self, destination: bytes | None, data: bytes) -> dict:
        transaction = {
            "chainId": CHAIN_ID,
            "nonce": int(
                self.rpc.call("eth_getTransactionCount", [self.account.address, "pending"]), 16
            ),
            "gas": 12_000_000,
            "gasPrice": int(self.rpc.call("eth_gasPrice", []), 16),
            "value": 0,
            "data": data,
        }
        if destination is not None:
            transaction["to"] = to_checksum_address(destination)
        signed = self.account.sign_transaction(transaction)
        tx_hash = self.rpc.call("eth_sendRawTransaction", [hx(signed.raw_transaction)])
        for _ in range(250):
            receipt = self.rpc.call("eth_getTransactionReceipt", [tx_hash])
            if receipt is not None:
                assert receipt["status"] == "0x1", receipt
                return receipt
            time.sleep(0.02)
        raise TimeoutError("owned Anvil did not mine fixture transaction")

    def execute(self, call, *, operation_ref: str) -> OperationResult:
        assert call.chain_id == CHAIN_ID
        with self.lock:
            old = self.results.get(operation_ref)
            identity = call.to + call.data
            if old is not None:
                assert old[0] == identity
                return old[1]
            receipt = self.send(call.to, call.data)
            self.submissions += 1
            result = OperationResult(
                operation_ref,
                OperationState.PENDING,
                tx_hash=raw(receipt["transactionHash"]),
                wallet_operation_id=operation_ref,
            )
            self.results[operation_ref] = (identity, result)
            return result

    def reconcile(self, *, operation_ref: str) -> OperationResult:
        old = self.results.get(operation_ref)
        return old[1] if old else OperationResult(operation_ref, OperationState.UNKNOWN)

    def sign_funding_authorization(self, authorization) -> bytes:
        self.authorization_count += 1
        assert authorization.owner == self.address
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
        return bytes(self.account.sign_message(typed).signature)
