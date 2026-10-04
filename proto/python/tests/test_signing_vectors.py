"""Independent ABI/EIP-712 fixture oracle; this is not a wallet adapter."""

import json
from pathlib import Path

from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak

VECTORS = Path(__file__).resolve().parents[3] / "testvectors"


def test_shared_session_and_signing_vectors() -> None:
    fixture = json.loads((VECTORS / "signing.json").read_text())
    descriptor = fixture["descriptor"]
    domain = fixture["domain"] | {"chainId": int(fixture["domain"]["chainId"])}
    values = [
        descriptor["agent"],
        descriptor["sessionSigner"],
        int(descriptor["providerId"]),
        descriptor["recipient"],
        descriptor["operator"],
        descriptor["token"],
        bytes.fromhex(descriptor["salt"][2:]),
        domain["verifyingContract"],
        domain["chainId"],
    ]
    derived = keccak(
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
            values,
        )
    )
    assert "0x" + derived.hex() == fixture["sessionId"]
    assert descriptor["agent"].lower() != descriptor["sessionSigner"].lower()
    for case in fixture["cases"]:
        declaration = case["typeString"]
        kind, members = declaration[:-1].split("(", 1)
        fields = [
            {"name": name, "type": solidity_type}
            for solidity_type, name in (field.split(" ") for field in members.split(","))
        ]
        message = dict(case["message"])
        for field in fields:
            if field["type"].startswith("uint"):
                message[field["name"]] = int(message[field["name"]])
        signable = encode_typed_data(domain, {kind: fields}, message)
        assert "0x" + signable.body.hex() == case["structHash"], case["name"]
        digest = keccak(b"\x19" + signable.version + signable.header + signable.body)
        assert "0x" + digest.hex() == case["digest"], case["name"]
        signed = Account.sign_message(signable, fixture["privateKey"])
        assert "0x" + signed.signature.hex() == case["signature"], case["name"]
        assert (
            Account.recover_message(signable, signature=case["signature"]).lower()
            == case["expectedSigner"].lower()
        )
        signature = bytes.fromhex(case["signature"][2:])
        assert len(signature) == 65 and signature[-1] in (27, 28)
        assert (
            0
            < int.from_bytes(signature[32:64])
            <= 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF5D576E7357A4501DDFE92F46681B20A0
        )
        wrong_domain = domain | {"chainId": domain["chainId"] + 1}
        wrong_signable = encode_typed_data(wrong_domain, {kind: fields}, message)
        assert (
            Account.recover_message(wrong_signable, signature=case["signature"]).lower()
            != case["expectedSigner"].lower()
        )
        if kind == "Voucher":
            wrong_fields = [
                {
                    "name": f["name"],
                    "type": "uint256" if f["name"] == "cumulativeAmount" else f["type"],
                }
                for f in fields
            ]
            wrong_width = encode_typed_data(domain, {kind: wrong_fields}, message)
            assert wrong_width.body != signable.body
