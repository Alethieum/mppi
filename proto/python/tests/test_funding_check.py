"""The pre-submission funding check mirrors USDC's signature rules; synthetic RPC."""

import json

import pytest
from eth_abi import decode, encode
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak
from mppi_protocol.capabilities import FundingAuthorization
from mppi_protocol.chain import (
    Block,
    ChainEvidenceError,
    ChainReader,
    CodePin,
    DeploymentProfile,
    HttpJsonRpc,
)
from mppi_protocol.circle import funding_typed_data
from mppi_protocol.signing import funding_digest, funding_signer
from mppi_protocol.types import ChainObservation, Domain

CHAIN = 5042002
PROXY, REGISTRY = b"\x99" * 20, b"\x97" * 20
TOKEN = bytes.fromhex("3600000000000000000000000000000000000000")
HASH = b"\xbb" * 32
OWNER = Account.from_key(b"\x21" * 32)
SMART = b"\x5a" * 20
AGENT_ID = 43
ORDER = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
MAGIC = bytes.fromhex("1626ba7e") + bytes(28)
PROFILE = DeploymentProfile(
    Domain(CHAIN, PROXY),
    REGISTRY,
    *(CodePin(address, b"\x0c" * 32) for address in (PROXY, b"\x98" * 20, REGISTRY, b"\x96" * 20)),
)
BLOCK = Block(ChainObservation(CHAIN, PROXY, 100, HASH), 1_790_331_000, b"\xbc" * 32)


def hx(value: bytes) -> str:
    return "0x" + value.hex()


def authorization(owner: bytes) -> FundingAuthorization:
    return FundingAuthorization(
        CHAIN, TOKEN, "USDC", "2", owner, PROXY, 2_500, 1_790_330_999, 1_790_334_600, b"\x33" * 32
    )


def eoa_signature(auth: FundingAuthorization) -> bytes:
    typed = funding_typed_data(auth)
    return bytes(OWNER.sign_message(encode_typed_data(full_message=typed)).signature)


class Node:
    """One JSON-RPC endpoint answering a batch per request, as Arc's do."""

    timeout_ms = 1_000

    def __init__(self, *, code: bytes = b"", agent_wallet: bytes = b"", answer=None) -> None:
        self.code, self.answer = code, answer
        self.agent_wallet = agent_wallet
        self.requests: list[list[dict]] = []

    def result(self, method: str, params: list) -> dict:
        if method == "eth_chainId":
            return {"result": hex(CHAIN)}
        if method == "eth_getBlockByNumber":
            assert params == [hex(100), False]
            return {
                "result": {
                    "number": hex(100),
                    "hash": hx(HASH),
                    "parentHash": hx(b"\xbc" * 32),
                    "timestamp": hex(1_790_331_000),
                }
            }
        assert params[-1] == {"blockHash": hx(HASH), "requireCanonical": True}
        if method == "eth_getCode":
            return {"result": hx(self.code)}
        assert method == "eth_call" and set(params[0]) == {"to", "data"}  # Never a sender.
        target, data = bytes.fromhex(params[0]["to"][2:]), bytes.fromhex(params[0]["data"][2:])
        if target == REGISTRY:
            assert data == keccak(text="getAgentWallet(uint256)")[:4] + encode(
                ["uint256"], [AGENT_ID]
            )
            if self.agent_wallet is None:
                return {"error": {"code": 3, "message": "execution reverted"}}
            return {"result": hx(encode(["address"], [hx(self.agent_wallet)]))}
        assert data[:4] == bytes.fromhex("1626ba7e")
        self.checked = decode(["bytes32", "bytes"], data[4:])
        if not self.code:
            return {"result": "0x"}
        return self.answer

    def request(self, url: str, *, body: bytes | None = None) -> bytes:
        payload = json.loads(body)
        assert isinstance(payload, list)
        self.requests.append(payload)
        reply = [
            {"jsonrpc": "2.0", "id": call["id"]} | self.result(call["method"], call["params"])
            for call in payload
        ]
        return json.dumps(reply).encode()


def check(node: Node, auth: FundingAuthorization, signature: bytes):
    reader = ChainReader(HttpJsonRpc("https://rpc.example/private-key", node), PROFILE)
    evidence = reader.funding(auth, signature, AGENT_ID, BLOCK)
    assert len(node.requests) == 1  # One round trip, confirming its block.
    return evidence


def test_digest_is_the_typed_data_hash_a_wallet_signs() -> None:
    auth = authorization(bytes.fromhex(OWNER.address[2:]))
    signable = encode_typed_data(full_message=funding_typed_data(auth))
    assert funding_digest(auth) == keccak(
        b"\x19" + signable.version + signable.header + signable.body
    )
    assert funding_signer(auth, eoa_signature(auth)) == auth.owner


def test_an_account_without_code_must_recover_as_usdc_ecrecover_does() -> None:
    owner = bytes.fromhex(OWNER.address[2:])
    auth = authorization(owner)
    signature = eoa_signature(auth)
    node = Node(agent_wallet=owner)
    evidence = check(node, auth, signature)
    assert evidence.signature_valid and evidence.agent_wallet == owner
    assert node.checked == (funding_digest(auth), signature)
    r, s, v = signature[:32], int.from_bytes(signature[32:64], "big"), signature[64]
    stranger = Account.from_key(b"\x22" * 32).sign_message(
        encode_typed_data(full_message=funding_typed_data(auth))
    )
    for refused in (
        r + (ORDER - s).to_bytes(32, "big") + bytes([55 - v]),  # Same signer, upper-half s.
        signature[:64] + bytes([v - 27]),  # USDC accepts only v of 27 or 28.
        signature[:64],
        signature + b"\x00",
        bytes(stranger.signature),
    ):
        assert not check(Node(agent_wallet=owner), auth, refused).signature_valid


@pytest.mark.parametrize(
    "answer,valid",
    [
        ({"result": hx(MAGIC)}, True),
        ({"result": hx(MAGIC + bytes(32))}, True),  # SignatureChecker reads the first word.
        ({"result": hx(bytes.fromhex("ffffffff") + bytes(28))}, False),
        ({"result": hx(bytes.fromhex("1626ba7e"))}, False),  # Shorter than one word.
        ({"result": hx(bytes(28) + bytes.fromhex("1626ba7e"))}, False),  # Not left-aligned.
        ({"error": {"code": 3, "message": "execution reverted"}}, False),
    ],
)
def test_an_account_with_code_must_answer_erc1271_magic(answer, valid) -> None:
    auth = authorization(SMART)
    evidence = check(Node(code=b"\x60\x80", agent_wallet=SMART, answer=answer), auth, b"\x01" * 97)
    assert evidence.signature_valid is valid


def test_delegated_code_is_checked_through_erc1271_as_usdc_does() -> None:
    # USDC's isContract is extcodesize > 0, so a delegated account answers ERC-1271
    # even though its own key could sign.
    owner = bytes.fromhex(OWNER.address[2:])
    auth = authorization(owner)
    refused = {"result": hx(bytes.fromhex("ffffffff") + bytes(28))}
    node = Node(code=b"\xef\x01\x00" + b"\x44" * 20, agent_wallet=owner, answer=refused)
    assert not check(node, auth, eoa_signature(auth)).signature_valid


@pytest.mark.parametrize("bound", [bytes(20), None, b"\x77" * 20])
def test_agent_wallet_binding_is_read_at_the_same_block(bound) -> None:
    auth = authorization(SMART)
    node = Node(code=b"\x60\x80", agent_wallet=bound, answer={"result": hx(MAGIC)})
    evidence = check(node, auth, b"\x01" * 65)
    # Zero once ERC-8004 clears it; a reverting registry binds nothing either.
    assert evidence.agent_wallet == (bytes(20) if bound is None else bound)


def test_other_rpc_errors_are_missing_evidence_not_a_refusal() -> None:
    auth = authorization(SMART)
    node = Node(code=b"\x60\x80", agent_wallet=SMART, answer={"error": {"code": -32000}})
    with pytest.raises(ChainEvidenceError):
        check(node, auth, b"\x01" * 65)


def test_a_reverted_call_elsewhere_is_still_invalid_rpc_bytes() -> None:
    rpc = HttpJsonRpc("https://rpc.example/private-key", Node(agent_wallet=None))
    with pytest.raises(ChainEvidenceError):
        ChainReader(rpc, PROFILE).agent_wallet(AGENT_ID, BLOCK.observation)
