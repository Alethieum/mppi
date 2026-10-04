"""Circle adapter boundaries; no credentials, network or economic transactions."""

from __future__ import annotations

import base64
import copy
import json
from typing import Any
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

import mppi_protocol.circle as circle_module
import pytest
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from eth_abi import encode
from eth_account.messages import encode_typed_data
from eth_utils import keccak
from mppi_protocol.capabilities import FundingAuthorization, WalletCall
from mppi_protocol.circle import CircleDeveloperWallet, CircleWalletError, funding_typed_data
from mppi_protocol.errors import ValidationError
from mppi_protocol.types import OperationState

WALLET_ID = "11111111-1111-4111-8111-111111111111"
TX_ID = "22222222-2222-4222-8222-222222222222"
ADDRESS = bytes.fromhex("11" * 20)
TARGET = bytes.fromhex("22" * 20)
CALL = WalletCall(5042002, TARGET, bytes.fromhex("12345678") + bytes(128))
SECRET = bytes.fromhex("34" * 32)


@pytest.fixture(scope="module")
def rsa_key():
    return RSA.generate(2048)


class Service:
    def __init__(self, key):
        self.key = key
        self.records: list[dict[str, Any]] = []
        self.calls: list[tuple[str, str, dict[str, object] | None]] = []
        self.fail_after_post = False
        self.fail_before_post = False
        self.fail_detail = False
        self.wallet_state = "LIVE"

    def request(self, method: str, path: str, *, body=None):
        self.calls.append((method, path, copy.deepcopy(body)))
        if path == "/wallets/" + WALLET_ID:
            return {
                "wallet": {
                    "id": WALLET_ID,
                    "address": "0x" + ADDRESS.hex(),
                    "blockchain": "ARC-TESTNET",
                    "accountType": "SCA",
                    "custodyType": "DEVELOPER",
                    "state": self.wallet_state,
                }
            }
        if path == "/config/entity/publicKey":
            return {"publicKey": self.key.public_key().export_key().decode()}
        if path.startswith("/transactions?"):
            query = parse_qs(urlsplit(path).query)
            assert query["walletIds"] == [WALLET_ID]
            # Live Circle rejects these individually documented filters when
            # combined; this fixture models that request-validation boundary.
            if "walletIds" in query and "blockchain" in query:
                raise CircleWalletError("incompatible transaction filters")
            assert query["pageSize"] == ["2"]
            return {"transactions": copy.deepcopy(self.records)}
        if method == "POST" and path == "/developer/transactions/contractExecution":
            if self.fail_before_post:
                raise CircleWalletError("unavailable")
            self.records.append(
                {
                    "id": TX_ID,
                    "refId": body["refId"],
                    "walletId": WALLET_ID,
                    "blockchain": "ARC-TESTNET",
                    "custodyType": "DEVELOPER",
                    "sourceAddress": "0x" + ADDRESS.hex(),
                    "contractAddress": body["contractAddress"],
                    "operation": "CONTRACT_EXECUTION",
                    "transactionType": "OUTBOUND",
                    "state": "INITIATED",
                    "txHash": "",
                }
            )
            if self.fail_after_post:
                raise CircleWalletError("lost response")
            return {"id": TX_ID, "state": "INITIATED"}
        if path == "/transactions/" + TX_ID:
            if self.fail_detail:
                raise CircleWalletError("unavailable")
            return {"transaction": copy.deepcopy(self.records[0])}
        if path == "/developer/sign/typedData":
            return {"signature": "0x" + "aa" * 140}
        raise AssertionError((method, path))

    @property
    def submissions(self):
        return [
            b
            for m, p, b in self.calls
            if m == "POST" and p == "/developer/transactions/contractExecution"
        ]


def wallet(service: Service) -> CircleDeveloperWallet:
    result = CircleDeveloperWallet(
        api_key="TEST_API_KEY:hidden",
        entity_secret=SECRET,
        wallet_id=WALLET_ID,
        address=ADDRESS,
        chain_id=5042002,
    )
    result._request = service.request
    return result


def test_exact_original_call_and_service_reference(rsa_key):
    service = Service(rsa_key)
    adapter = wallet(service)
    result = adapter.execute(CALL, operation_ref="settleBatch:original")
    assert result.state is OperationState.PENDING
    assert result.wallet_operation_id == TX_ID
    body = service.submissions[0]
    assert body["callData"] == "0x" + CALL.data.hex()
    assert body["contractAddress"] == "0x" + TARGET.hex()
    assert body["amount"] == "0"
    assert body["walletId"] == WALLET_ID
    assert "blockchain" not in body and "abiParameters" not in body
    assert body["idempotencyKey"] == body["refId"]
    assert UUID(body["idempotencyKey"]).version == 4
    assert wallet(service)._reference("settleBatch:original") == body["refId"]
    assert adapter._reference("settleBatch:another") != body["refId"]
    ciphertext = base64.b64decode(body["entitySecretCiphertext"])
    assert PKCS1_OAEP.new(rsa_key, hashAlgo=SHA256).decrypt(ciphertext) == SECRET
    assert "hidden" not in repr(adapter) and SECRET.hex() not in repr(adapter)


def test_lookup_uses_wallet_filter_without_incompatible_blockchain_filter(rsa_key):
    service = Service(rsa_key)
    adapter = wallet(service)
    result = adapter.execute(CALL, operation_ref="filtered-operation")
    assert result.wallet_operation_id == TX_ID
    lookups = [path for method, path, _ in service.calls if path.startswith("/transactions?")]
    assert len(lookups) == 1
    assert parse_qs(urlsplit(lookups[0]).query) == {
        "refId": [adapter._reference("filtered-operation")],
        "walletIds": [WALLET_ID],
        "operation": ["CONTRACT_EXECUTION"],
        "custodyType": ["DEVELOPER"],
        "pageSize": ["2"],
    }
    assert len(service.submissions) == 1


@pytest.mark.parametrize("returned_chain", ["ARC", "BASE-SEPOLIA", None])
def test_lookup_rejects_wrong_chain_before_following_or_submitting(rsa_key, returned_chain):
    service = Service(rsa_key)
    adapter = wallet(service)
    service.records = [
        {
            "id": TX_ID,
            "refId": adapter._reference("original"),
            "walletId": WALLET_ID,
            "blockchain": returned_chain,
            "custodyType": "DEVELOPER",
            "operation": "CONTRACT_EXECUTION",
            "transactionType": "OUTBOUND",
            "sourceAddress": "0x" + ADDRESS.hex(),
            "contractAddress": "0x" + TARGET.hex(),
            "state": "INITIATED",
        }
    ]
    assert adapter.execute(CALL, operation_ref="original").state is OperationState.UNKNOWN
    assert wallet(service).reconcile(operation_ref="original").state is OperationState.UNKNOWN
    assert not service.submissions
    assert all(path.startswith("/transactions?") for _, path, _ in service.calls)


def test_lost_response_reconciles_original_without_resubmission(rsa_key):
    service = Service(rsa_key)
    service.fail_after_post = True
    adapter = wallet(service)
    assert adapter.execute(CALL, operation_ref="original").state is OperationState.UNKNOWN
    assert adapter.execute(CALL, operation_ref="original").wallet_operation_id == TX_ID
    restarted = wallet(service)
    service.records[0].update(state="COMPLETE", txHash="0x" + "ab" * 32)
    recovered = restarted.reconcile(operation_ref="original")
    assert recovered.state is OperationState.PENDING  # Canonical chain verification follows.
    assert recovered.tx_hash == bytes.fromhex("ab" * 32)
    assert len(service.submissions) == 1


def test_absent_unknown_operation_never_resubmits(rsa_key):
    service = Service(rsa_key)
    service.fail_before_post = True
    adapter = wallet(service)
    assert adapter.execute(CALL, operation_ref="unknown").state is OperationState.UNKNOWN
    assert adapter.execute(CALL, operation_ref="unknown").state is OperationState.UNKNOWN
    assert wallet(service).reconcile(operation_ref="unknown").state is OperationState.UNKNOWN
    assert len(service.submissions) == 1


def test_known_identifier_survives_detail_outage(rsa_key):
    service = Service(rsa_key)
    service.fail_detail = True
    adapter = wallet(service)
    result = adapter.execute(CALL, operation_ref="original")
    assert result.state is OperationState.UNKNOWN and result.wallet_operation_id == TX_ID
    assert adapter.reconcile(operation_ref="original").wallet_operation_id == TX_ID
    assert len(service.submissions) == 1


def test_changed_call_under_existing_reference_is_rejected(rsa_key):
    service = Service(rsa_key)
    adapter = wallet(service)
    adapter.execute(CALL, operation_ref="original")
    with pytest.raises(ValidationError, match="reused"):
        adapter.execute(WalletCall(CALL.chain_id, TARGET, b"changed"), operation_ref="original")
    assert len(service.submissions) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("walletId", "33333333-3333-4333-8333-333333333333"),
        ("blockchain", "ARC"),
        ("refId", "foreign"),
        ("custodyType", "ENDUSER"),
        ("operation", "TRANSFER"),
        ("transactionType", "INBOUND"),
        ("sourceAddress", "0x" + "33" * 20),
        ("contractAddress", "0x" + "33" * 20),
        ("callData", "0xdead"),
        ("txHash", "0xnothex"),
        ("state", []),
    ],
)
def test_inconsistent_service_record_stays_unknown(rsa_key, field, value):
    service = Service(rsa_key)
    adapter = wallet(service)
    adapter.execute(CALL, operation_ref="original")
    service.records[0][field] = value
    assert adapter.reconcile(operation_ref="original").state is OperationState.UNKNOWN
    assert len(service.submissions) == 1


def test_duplicate_lookup_fails_closed(rsa_key):
    service = Service(rsa_key)
    adapter = wallet(service)
    adapter.execute(CALL, operation_ref="original")
    service.records.append(copy.deepcopy(service.records[0]))
    assert wallet(service).reconcile(operation_ref="original").state is OperationState.UNKNOWN
    assert len(service.submissions) == 1


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("FAILED", OperationState.FAILED),
        ("DENIED", OperationState.FAILED),
        ("CANCELLED", OperationState.FAILED),
        ("QUEUED", OperationState.PENDING),
        ("STUCK", OperationState.PENDING),
        ("COMPLETE", OperationState.UNKNOWN),
        ("NEW_UNKNOWN_STATE", OperationState.UNKNOWN),
    ],
)
def test_service_state_is_not_chain_confirmation(rsa_key, raw, expected):
    service = Service(rsa_key)
    adapter = wallet(service)
    adapter.execute(CALL, operation_ref="original")
    service.records[0]["state"] = raw
    assert adapter.reconcile(operation_ref="original").state is expected


def test_disabled_wallet_and_wrong_network_cannot_submit(rsa_key):
    service = Service(rsa_key)
    service.wallet_state = "FROZEN"
    adapter = wallet(service)
    assert adapter.execute(CALL, operation_ref="original").state is OperationState.UNKNOWN
    with pytest.raises(ValidationError):
        adapter.execute(WalletCall(5042, TARGET, CALL.data), operation_ref="other")
    assert not service.submissions


def test_funding_signature_preserves_exact_token_scope_and_signature(rsa_key):
    service = Service(rsa_key)
    adapter = wallet(service)
    auth = FundingAuthorization(
        5042002,
        bytes.fromhex("36" + "00" * 19),
        "USD Coin",
        "2",
        ADDRESS,
        TARGET,
        2**64 - 1,
        0,
        2**128,
        bytes.fromhex("77" * 32),
    )
    signature = adapter.sign_funding_authorization(auth)
    adapter.sign_funding_authorization(auth)
    assert signature == bytes.fromhex("aa" * 140)
    requests = [b for _, p, b in service.calls if p == "/developer/sign/typedData"]
    assert requests[0]["entitySecretCiphertext"] != requests[1]["entitySecretCiphertext"]
    data = json.loads(requests[0]["data"])
    assert data["primaryType"] == "ReceiveWithAuthorization"
    assert data["domain"]["verifyingContract"] == "0x" + auth.token.hex()
    assert data["domain"]["chainId"] == 5042002
    assert data["message"]["from"] == "0x" + ADDRESS.hex()
    assert data["message"]["validBefore"] == str(2**128)
    assert data["message"]["value"] == str(2**64 - 1)
    assert not service.submissions


def test_shared_funding_json_matches_independent_eip3009_abi_hash():
    auth = FundingAuthorization(
        5042002,
        bytes.fromhex("36" + "00" * 19),
        "USDC",
        "2",
        ADDRESS,
        TARGET,
        2**96 + 1,
        1,
        2**128,
        bytes.fromhex("77" * 32),
    )
    signable = encode_typed_data(full_message=funding_typed_data(auth))
    expected = keccak(
        encode(
            ["bytes32", "address", "address", "uint256", "uint256", "uint256", "bytes32"],
            [
                keccak(
                    text="ReceiveWithAuthorization(address from,address to,uint256 value,uint256 validAfter,uint256 validBefore,bytes32 nonce)"
                ),
                auth.owner,
                auth.recipient,
                auth.value,
                auth.valid_after,
                auth.valid_before,
                auth.nonce,
            ],
        )
    )
    domain = keccak(
        encode(
            ["bytes32", "bytes32", "bytes32", "uint256", "address"],
            [
                keccak(
                    text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
                ),
                keccak(text="USDC"),
                keccak(text="2"),
                auth.chain_id,
                auth.token,
            ],
        )
    )
    assert signable.body == expected
    assert signable.header == domain


@pytest.mark.parametrize(
    "kwargs",
    [
        {"api_key": "bad\r\nheader"},
        {"entity_secret": b"short"},
        {"chain_id": True},
        {"chain_id": 1},
        {"timeout_ms": 0},
    ],
)
def test_invalid_configuration_rejected_without_request(kwargs):
    config = dict(
        api_key="TEST_API_KEY:hidden",
        entity_secret=SECRET,
        wallet_id=WALLET_ID,
        address=ADDRESS,
        chain_id=5042002,
    )
    config.update(kwargs)
    with pytest.raises(ValidationError):
        CircleDeveloperWallet(**config)


@pytest.mark.parametrize(
    "status,encoding,content_type,payload",
    [
        (302, "identity", "application/json", b"{}"),
        (503, "identity", "application/json", b"remote body with secrets"),
        (200, "gzip", "application/json", b"{}"),
        (200, "identity", "text/html", b"{}"),
        (200, "identity", "application/json", b"x" * 1_048_577),
    ],
)
def test_http_errors_are_bounded_redacted_and_never_retried(
    monkeypatch, status, encoding, content_type, payload
):
    requests = []
    closed = []

    class Response:
        def __init__(self):
            self.status = status

        def getheader(self, name, default):
            return {"Content-Encoding": encoding, "Content-Type": content_type}.get(name, default)

        def read(self, size):
            assert size == 1_048_577
            return payload[:size]

    class Connection:
        def __init__(self, host, **kwargs):
            assert host == "api.circle.com" and kwargs["timeout"] == 10.0

        def request(self, method, path, *, body, headers):
            assert headers["Authorization"] == "Bearer TEST_API_KEY:hidden"
            requests.append((method, path, body))

        def getresponse(self):
            return Response()

        def close(self):
            closed.append(True)

    monkeypatch.setattr(circle_module.http.client, "HTTPSConnection", Connection)
    adapter = CircleDeveloperWallet(
        api_key="TEST_API_KEY:hidden",
        entity_secret=SECRET,
        wallet_id=WALLET_ID,
        address=ADDRESS,
        chain_id=5042002,
    )
    with pytest.raises(CircleWalletError) as failure:
        adapter._request(
            "POST", "/developer/transactions/contractExecution", body={"test": "value"}
        )
    assert "hidden" not in str(failure.value) and "remote body" not in str(failure.value)
    assert len(requests) == 1 and len(closed) == 1
