"""Create and independently reconcile the disposable paid-loop deployment."""

from __future__ import annotations

import argparse
import json
import secrets
from pathlib import Path

from common import CATALOG, CHAIN_ID, USDC, LocalRpc, LocalWallet, hx, raw, reader_for, write_json
from eth_abi import decode, encode
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak
from mppi_protocol.registry import RegistryReader


class Artifact:
    def __init__(self, contracts: Path, name: str) -> None:
        self.data = json.loads((contracts / "out" / (name + ".sol") / (name + ".json")).read_text())
        self.abi = self.data["abi"]

    @staticmethod
    def abi_type(field: dict) -> str:
        value = field["type"]
        if value.startswith("tuple"):
            return (
                "("
                + ",".join(Artifact.abi_type(child) for child in field["components"])
                + ")"
                + value[5:]
            )
        return value

    def call(self, name: str, values: tuple) -> bytes:
        entry = next(
            item for item in self.abi if item["type"] == "function" and item["name"] == name
        )
        types = [self.abi_type(item) for item in entry["inputs"]]
        return keccak(text=name + "(" + ",".join(types) + ")")[:4] + encode(types, values)

    def creation(self, values: tuple = ()) -> bytes:
        constructor = next(
            (item for item in self.abi if item["type"] == "constructor"), {"inputs": []}
        )
        return raw(self.data["bytecode"]["object"]) + encode(
            [self.abi_type(item) for item in constructor["inputs"]], values
        )


def bootstrap(args) -> None:
    rpc = LocalRpc(args.rpc)
    keys = {
        role: secrets.token_bytes(32)
        for role in ("deployer", "agent", "operator", "recipient", "signer")
    }
    addresses = {role: raw(Account.from_key(key).address) for role, key in keys.items()}
    assert len(set(addresses.values())) == 5
    for role in ("deployer", "agent", "operator"):
        rpc.call("anvil_setBalance", [hx(addresses[role]), hex(100 * 10**18)])
    wallet = LocalWallet({"rpc": args.rpc, "key": hx(keys["deployer"])}, "key")

    def deploy(name: str, values: tuple = ()) -> bytes:
        receipt = wallet.send(None, Artifact(args.contracts, name).creation(values))
        return raw(receipt["contractAddress"])

    registry_name = "OnboardingRegistry" if args.onboarding else "DiscoveryRegistry"
    registry_artifact = Artifact(args.contracts, registry_name)
    registry_impl = deploy(registry_name)
    registry = deploy(
        "ERC1967Proxy",
        (
            registry_impl,
            registry_artifact.call("initialize", ())
            if args.onboarding
            else registry_artifact.call(
                "setProvider", (42, addresses["operator"], addresses["recipient"], CATALOG)
            ),
        ),
    )
    if not args.onboarding:
        wallet.send(registry, registry_artifact.call("setURI", (42, args.origin + "/discover")))
        wallet.send(
            registry,
            registry_artifact.call(
                "setProvider", (43, addresses["agent"], addresses["agent"], CATALOG)
            ),
        )
    timelock = deploy("TimelockController", (60, [addresses["deployer"]], [bytes(20)], bytes(20)))
    implementation = deploy("MPPI")
    initializer = Artifact(args.contracts, "MPPI").call(
        "initialize", (registry, addresses["deployer"], addresses["deployer"], timelock)
    )
    proxy = deploy("ERC1967Proxy", (implementation, initializer))
    token_template = deploy("MockUSDC")
    rpc.call("anvil_setCode", [hx(USDC), rpc.call("eth_getCode", [hx(token_template), "latest"])])
    wallet.send(
        USDC, Artifact(args.contracts, "MockUSDC").call("mint", (addresses["agent"], 5_510_000))
    )
    pins = [
        {
            "address": hx(address),
            "code_hash": hx(keccak(raw(rpc.call("eth_getCode", [hx(address), "latest"])))),
        }
        for address in (proxy, implementation, registry, registry_impl)
    ]
    public = {
        "rpc": args.rpc,
        "origin": args.origin,
        "pins": pins,
        "provider_id": None if args.onboarding else 42,
        "agent_id": None if args.onboarding else 43,
        "onboarding": args.onboarding,
        "chain_id": CHAIN_ID,
        "agent": hx(addresses["agent"]),
        "recipient": hx(addresses["recipient"]),
        "operator": hx(addresses["operator"]),
        "signer": hx(addresses["signer"]),
        "bootstrap_block": rpc.call("eth_blockNumber", []),
    }
    for role in ("agent", "provider"):
        target = args.output / role
        target.mkdir(parents=True, exist_ok=True)
        value = dict(public)
        if role == "agent":
            value.update(wallet_key=hx(keys["agent"]), signer_key=hx(keys["signer"]))
        else:
            for field in ("agent", "signer", "agent_id", "bootstrap_block"):
                value.pop(field)
            value.update(wallet_key=hx(keys["operator"]))
        write_json(target / "config.json", value)
    write_json(args.output / "deployment.json", public)
    if args.onboarding:
        # Offline recipient fixture is confined to the chain container. Neither
        # SDK process receives its credential; only the exact proof is exported.
        write_json(args.output / "recipient-key.json", {"key": hx(keys["recipient"])})
    # Recipient/deployer keys are deliberately discarded after provisioning.
    assert rpc.call("eth_getBalance", [hx(addresses["signer"]), "latest"]) == "0x0"


def proof(args) -> None:
    settings = json.loads((args.output / "deployment.json").read_text())
    registered = json.loads((args.output / "provider-onboarding.json").read_text())
    registry = RegistryReader(reader_for(settings))
    block = registry.verified_block()
    typed = registry.wallet_proof(
        registered["provider_id"],
        raw(settings["recipient"]),
        owner=raw(settings["operator"]),
        deadline=block.timestamp + 240,
        block=block,
    )
    key_path = args.output / "recipient-key.json"
    account = Account.from_key(raw(json.loads(key_path.read_text())["key"]))
    assert raw(account.address) == raw(settings["recipient"])
    signature = account.sign_message(encode_typed_data(full_message=typed)).signature
    write_json(
        args.output / "recipient-proof.json", {"typed_data": typed, "signature": hx(signature)}
    )
    key_path.unlink()


def observe(args) -> None:
    config = json.loads((args.output / "deployment.json").read_text())
    rpc = LocalRpc(config["rpc"])
    token = Artifact(args.contracts, "MockUSDC")
    proxy = raw(config["pins"][0]["address"])

    def balance(address):
        return decode(
            ["uint256"],
            raw(
                rpc.call(
                    "eth_call",
                    [{"to": hx(USDC), "data": hx(token.call("balanceOf", (address,)))}, "latest"],
                )
            ),
        )[0]

    balances = {name: balance(raw(config[name])) for name in ("agent", "recipient")}
    balances["proxy"] = balance(proxy)
    assert balances == {"agent": 90_000, "recipient": 5_420_000, "proxy": 0}, balances
    logs = rpc.call(
        "eth_getLogs",
        [{"address": hx(proxy), "fromBlock": config["bootstrap_block"], "toBlock": "latest"}],
    )
    abi = Artifact(args.contracts, "MPPI").abi
    events = {}
    for entry in abi:
        if entry["type"] == "event":
            signature = (
                entry["name"]
                + "("
                + ",".join(Artifact.abi_type(item) for item in entry["inputs"])
                + ")"
            )
            events[hx(keccak(text=signature))] = entry
    decoded = []
    for log in logs:
        receipt = rpc.call("eth_getTransactionReceipt", [log["transactionHash"]])
        block = rpc.call("eth_getBlockByNumber", [log["blockNumber"], False])
        assert (
            receipt["status"] == "0x1" and receipt["blockHash"] == block["hash"] == log["blockHash"]
        )
        assert any(item == log for item in receipt["logs"])
        entry = events[log["topics"][0]]
        fields = [item for item in entry["inputs"] if not item["indexed"]]
        values = decode([Artifact.abi_type(item) for item in fields], raw(log["data"]))
        rendered = {
            field["name"]: hx(value) if isinstance(value, bytes) else value
            for field, value in zip(fields, values, strict=True)
        }
        decoded.append(
            {
                "event": entry["name"],
                "values": rendered,
                "tx_hash": log["transactionHash"],
                "block": log["blockNumber"],
                "log_index": log["logIndex"],
            }
        )
    assert [item["event"] for item in decoded] == ["SessionOpened", "Settled", "SessionClosed"], (
        decoded
    )
    assert decoded[0]["values"]["deposit"] == 5_510_000
    assert decoded[1]["values"] == {
        "cumulativeAmount": 5_510_000,
        "captureAmount": 5_420_000,
        "deltaPaid": 5_420_000,
        "newSettled": 5_420_000,
    }
    assert decoded[2]["values"] == {"captureAmount": 5_420_000, "refunded": 90_000}
    assert len({log["topics"][1] for log in logs}) == 1
    write_json(
        args.output / "observer.json",
        {"status": "PASS", "balances": balances, "canonical_events": decoded},
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("bootstrap", "observe", "proof"))
    parser.add_argument("--onboarding", action="store_true")
    parser.add_argument("--rpc")
    parser.add_argument("--origin")
    parser.add_argument("--contracts", type=Path, default=Path("/work/mppi/contracts"))
    parser.add_argument("--output", type=Path, default=Path("/provision"))
    args = parser.parse_args()
    {"bootstrap": bootstrap, "observe": observe, "proof": proof}[args.mode](args)


if __name__ == "__main__":
    main()
