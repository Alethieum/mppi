"""Rust production calls and receipt reads against the owned local Anvil fixture.

Python signs fixture proofs, submits Rust-produced calldata, and independently
checks EVM balances. Rust uses its production HTTP, ABI, deployment and receipt
readers. No supplied endpoint, live key, fork or public transaction is accepted.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
from pathlib import Path

from eth_abi import decode
from eth_utils import keccak
from mppi_protocol.capabilities import FundingAuthorization
from mppi_protocol.signing import LocalSessionSigner, session_id
from mppi_protocol.types import Domain, SessionDescriptor

SPEC = importlib.util.spec_from_file_location(
    "mppi_owned_anvil_helpers", Path(__file__).with_name("test_chain_anvil.py")
)
assert SPEC is not None and SPEC.loader is not None
HELPERS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HELPERS)
node = HELPERS.node
Artifact = HELPERS.Artifact
hx, raw = HELPERS.hx, HELPERS.raw
CHAIN_ID, USDC, CATALOG = HELPERS.CHAIN_ID, HELPERS.USDC, HELPERS.CATALOG
RUST = Path(__file__).resolve().parents[1] / "sdks/provider/rust"


def test_rust_calls_fund_capture_and_refund_on_real_evm(node, tmp_path: Path) -> None:
    mppi, proxy_artifact = Artifact("MPPI"), Artifact("ERC1967Proxy")
    registry_artifact, token = Artifact("MockIdentityRegistry"), Artifact("MockUSDC")
    registry_implementation = node.deploy(registry_artifact)
    registry = node.deploy(
        proxy_artifact,
        (
            registry_implementation,
            registry_artifact.call(
                "setProvider", (42, node.operator.address, node.recipient.address, CATALOG)
            ),
        ),
    )
    timelock = node.deploy(
        Artifact("TimelockController"), (60, [node.deployer.address], [bytes(20)], bytes(20))
    )
    implementation = node.deploy(mppi)
    proxy = node.deploy(
        proxy_artifact,
        (
            implementation,
            mppi.call(
                "initialize", (registry, node.deployer.address, node.deployer.address, timelock)
            ),
        ),
    )
    template = node.deploy(token)
    node.call("anvil_setCode", [hx(USDC), node.call("eth_getCode", [hx(template), "latest"])])
    node.send(node.deployer, USDC, token.call("mint", (node.agent.address, 6_000_000)))

    def pin(address: bytes) -> dict:
        return {
            "address": hx(address),
            "code_hash": hx(keccak(raw(node.call("eth_getCode", [hx(address), "latest"])))),
        }

    domain = Domain(CHAIN_ID, proxy)
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

    def authorization(amount: int, nonce: int) -> dict:
        auth = FundingAuthorization(
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
        return {
            "amount": amount,
            "valid_after": auth.valid_after,
            "valid_before": auth.valid_before,
            "nonce": hx(auth.nonce),
            "signature": hx(HELPERS.funding_signature(node, auth)),
        }

    settings = {
        "profile": {
            "version": 1,
            "chain_id": CHAIN_ID,
            "proxy": pin(proxy),
            "implementation": pin(implementation),
            "registry": pin(registry),
            "registry_implementation": pin(registry_implementation),
        },
        "rpc": node.endpoint,
        "descriptor": {
            "agent": hx(descriptor.agent),
            "session_signer": hx(descriptor.session_signer),
            "provider_id": 42,
            "recipient": hx(descriptor.recipient),
            "operator": hx(descriptor.operator),
            "salt": hx(descriptor.salt),
        },
        "session_id": hx(identifier),
        "catalog": hx(CATALOG),
        "open_auth": authorization(5_510_000, 1),
        "top_up_auth": authorization(490_000, 2),
        "voucher": hx(signer.sign_voucher(domain, identifier, 5_510_000)),
    }

    def rust(**phase) -> dict:
        request, response = tmp_path / "rust-input.json", tmp_path / "rust-output.json"
        request.write_text(json.dumps(settings | phase))
        response.unlink(missing_ok=True)
        env = dict(os.environ)
        env.update(MPPI_RUST_ANVIL_INPUT=str(request), MPPI_RUST_ANVIL_OUTPUT=str(response))
        completed = subprocess.run(
            [
                "cargo",
                "test",
                "--locked",
                "--test",
                "chain_anvil",
                "--",
                "--ignored",
                "--exact",
                "owned_anvil_interoperability",
                "--nocapture",
            ],
            cwd=RUST,
            env=env,
            capture_output=True,
            text=True,
            timeout=180,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
        return json.loads(response.read_text())

    prepared = rust(phase="prepare")

    def execute(name: str, account, *, repeated: bool = False) -> tuple[dict, dict]:
        call = prepared[name]
        assert call["chain_id"] == CHAIN_ID and raw(call["to"]) == proxy
        assert raw(call["session_id"]) == identifier
        receipt = node.send(account, proxy, raw(call["data"]))
        evidence = rust(
            phase="reconcile",
            operation=name,
            tx_hash=receipt["transactionHash"],
            block_number=int(receipt["blockNumber"], 16),
            block_hash=receipt["blockHash"],
            repeated=repeated,
        )
        return evidence, receipt

    opened, _ = execute("open", node.agent)
    assert opened["state"] == "confirmed" and opened["deposit"] == 5_510_000
    assert node.uint(USDC, token, "balanceOf", (proxy,)) == 5_510_000
    assert node.uint(USDC, token, "balanceOf", (descriptor.agent,)) == 490_000
    assert (
        node.uint(USDC, token, "authorizationState", (descriptor.agent, bytes(31) + b"\x01")) == 1
    )

    topped, _ = execute("top_up", node.agent)
    assert topped["state"] == "confirmed" and topped["deposit"] == 6_000_000
    assert node.uint(USDC, token, "balanceOf", (descriptor.agent,)) == 0

    paid, receipt = execute("settle", node.operator)
    assert paid["state"] == "confirmed" and paid["settlement_delta"] == 5_420_000
    assert paid["target_covered"] is True
    topic = hx(keccak(text="Settled(bytes32,address,address,uint64,uint64,uint64,uint64)"))
    logs = [
        item
        for item in receipt["logs"]
        if raw(item["address"]) == proxy and item["topics"][0] == topic
    ]
    assert len(logs) == 1
    assert raw(logs[0]["topics"][1]) == identifier
    assert decode(["uint64"] * 4, raw(logs[0]["data"])) == (
        5_510_000,
        5_420_000,
        5_420_000,
        5_420_000,
    )
    assert node.uint(USDC, token, "balanceOf", (descriptor.recipient,)) == 5_420_000
    assert node.uint(USDC, token, "balanceOf", (proxy,)) == 580_000

    repeat, receipt = execute("settle", node.operator, repeated=True)
    assert repeat["state"] == "unknown" and repeat["settlement_delta"] is None
    assert repeat["target_covered"] is True and repeat["last_activity"] == paid["last_activity"]
    assert receipt["logs"] == []
    assert node.uint(USDC, token, "balanceOf", (descriptor.recipient,)) == 5_420_000

    closed, _ = execute("close", node.operator)
    assert closed["state"] == "confirmed" and closed["settled_cumulative"] == 5_420_000
    assert closed["last_activity"] == paid["last_activity"]
    assert node.uint(USDC, token, "balanceOf", (proxy,)) == 0
    assert node.uint(USDC, token, "balanceOf", (descriptor.agent,)) == 580_000
    assert node.uint(USDC, token, "balanceOf", (descriptor.recipient,)) == 5_420_000
    assert node.uint(USDC, token, "fundingCalls", ()) == 2
    assert node.uint(USDC, token, "transferCalls", ()) == 2
