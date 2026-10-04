"""Circle developer-controlled wallet adapter for exact, nonpayable Arc calls.

Credentials are supplied by the operator. No login, wallet creation, persistence
backend or transaction retry is implemented. Circle's documented ``refId`` lookup
recovers the original service operation; MPPI's ChainReader must independently
verify the retained PreparedOperation before accepting any payment result.

Sources checked 2026-09-27: developers.circle.com/api-reference/wallets/
developer-controlled-wallets/{create-developer-transaction-contract-execution,
list-transactions,get-transaction,get-wallet,sign-typed-data}; Circle Python SDK
9.6.0 api_client.py:823-835 (RSA-OAEP SHA-256 entity-secret encryption).
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import ssl
import threading
from typing import Any, cast
from urllib.parse import urlencode
from uuid import UUID

from Crypto.Cipher import PKCS1_OAEP
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

from .capabilities import FundingAuthorization, WalletCall
from .discovery import address_hex, parse_address, parse_document
from .errors import ValidationCode, ValidationError
from .types import OperationResult, OperationState, fixed_bytes

_BLOCKCHAINS = {5042: "ARC", 5042002: "ARC-TESTNET"}
_PENDING = {"INITIATED", "CLEARED", "QUEUED", "SENT", "STUCK"}
_FAILED = {"FAILED", "DENIED", "CANCELLED"}
_MAX_RESPONSE = 1_048_576


class CircleWalletError(RuntimeError):
    """Circle supplied no usable evidence. Messages exclude remote data/secrets."""


def _object(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CircleWalletError("Circle response object is missing")
    return cast(dict[str, Any], value)


def _identifier(value: object) -> str:
    if type(value) is not str:
        raise CircleWalletError("Circle identifier is missing")
    try:
        parsed = UUID(value)
    except ValueError:
        raise CircleWalletError("Circle identifier is invalid") from None
    if str(parsed) != value:
        raise CircleWalletError("Circle identifier is not canonical")
    return value


def _address(value: object) -> bytes:
    if type(value) is not str:
        raise CircleWalletError("Circle address is missing")
    return parse_address(value)


def _hex(value: object, *, length: int | None = None) -> bytes:
    if type(value) is not str or not value.startswith("0x"):
        raise CircleWalletError("Circle hex value is invalid")
    try:
        raw = bytes.fromhex(value[2:])
    except ValueError:
        raise CircleWalletError("Circle hex value is invalid") from None
    if value[2:].lower() != raw.hex() or (length is not None and len(raw) != length):
        raise CircleWalletError("Circle hex value has invalid encoding or width")
    return raw


def funding_typed_data(authorization: FundingAuthorization) -> dict[str, object]:
    """Build the token's uint256 EIP-3009 proof; this never signs an MPPI voucher."""
    if not isinstance(authorization, FundingAuthorization):
        raise ValidationError(ValidationCode.INVALID_VALUE, "invalid funding authorization")
    auth = authorization
    typed_data: dict[str, object] = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "ReceiveWithAuthorization": [
                {"name": "from", "type": "address"},
                {"name": "to", "type": "address"},
                {"name": "value", "type": "uint256"},
                {"name": "validAfter", "type": "uint256"},
                {"name": "validBefore", "type": "uint256"},
                {"name": "nonce", "type": "bytes32"},
            ],
        },
        "primaryType": "ReceiveWithAuthorization",
        "domain": {
            "name": auth.domain_name,
            "version": auth.domain_version,
            "chainId": auth.chain_id,
            "verifyingContract": address_hex(auth.token),
        },
        "message": {
            "from": address_hex(auth.owner),
            "to": address_hex(auth.recipient),
            "value": str(auth.value),
            "validAfter": str(auth.valid_after),
            "validBefore": str(auth.valid_before),
            "nonce": "0x" + auth.nonce.hex(),
        },
    }
    return typed_data


class CircleDeveloperWallet:
    """Configured Circle developer-controlled SCA on the selected Arc network.

    ``entity_secret`` is the operator's registered 32-byte secret, not a wallet
    private key. It is encrypted freshly for every authorized API request.
    Credentials never appear in repr or error messages. HTTP has a finite wait,
    bounded responses, verified TLS, no redirects and no automatic retries.

    Call ``execute`` explicitly once for a retained operation reference; after
    any uncertain outcome call ``reconcile``. Service records provide durable
    lookup without an SDK database. An in-process attempted-reference guard also
    prevents a second submit after a timeout. A recreated adapter must use the
    caller's retained reconciliation path, never infer permission to resubmit
    from an empty lookup. Returned PENDING/UNKNOWN handles are not chain proof.
    """

    def __init__(
        self,
        *,
        api_key: str,
        entity_secret: bytes,
        wallet_id: str,
        address: bytes,
        chain_id: int,
        timeout_ms: int = 10_000,
    ) -> None:
        if (
            type(api_key) is not str
            or not api_key
            or any(ord(c) < 33 or ord(c) > 126 for c in api_key)
            or type(entity_secret) is not bytes
            or len(entity_secret) != 32
            or type(chain_id) is not int
            or chain_id not in _BLOCKCHAINS
            or type(timeout_ms) is not int
            or not 0 < timeout_ms <= 86_400_000
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "invalid Circle wallet configuration"
            )
        self._wallet_id = _identifier(wallet_id)
        self._address = fixed_bytes(address, 20, field="wallet address", nonzero=True)
        self._chain_id = chain_id
        self._blockchain = _BLOCKCHAINS[chain_id]
        self._api_key = api_key
        self._entity_secret = entity_secret
        self._timeout_ms = timeout_ms
        self._lock = threading.RLock()
        self._calls: dict[str, WalletCall] = {}
        self._attempted: set[str] = set()
        self._known: dict[str, OperationResult] = {}

    @property
    def address(self) -> bytes:
        return self._address

    def _request(
        self, method: str, path: str, *, body: dict[str, object] | None = None
    ) -> dict[str, Any]:
        # Fixed service origin prevents credential forwarding or endpoint overrides.
        context = ssl.create_default_context()
        context.set_alpn_protocols(["http/1.1"])
        connection = http.client.HTTPSConnection(
            "api.circle.com", timeout=self._timeout_ms / 1000, context=context
        )
        try:
            connection.request(
                method,
                "/v1/w3s" + path,
                body=None if body is None else json.dumps(body, separators=(",", ":")).encode(),
                headers={
                    "Authorization": "Bearer " + self._api_key,
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                    "Content-Type": "application/json",
                },
            )
            response = connection.getresponse()
            if response.status not in (200, 201):
                raise CircleWalletError("Circle request did not return success")
            if (
                response.getheader("Content-Encoding", "identity").lower() != "identity"
                or response.getheader("Content-Type", "").split(";", 1)[0].strip().lower()
                != "application/json"
            ):
                raise CircleWalletError("Circle response format is invalid")
            raw = response.read(_MAX_RESPONSE + 1)
            if len(raw) > _MAX_RESPONSE:
                raise CircleWalletError("Circle response exceeds its byte limit")
            return _object(parse_document(raw).get("data"))
        except (OSError, http.client.HTTPException, ValueError, ValidationError):
            raise CircleWalletError("Circle request failed") from None
        finally:
            connection.close()

    def verify_wallet(self) -> None:
        """Read and verify the configured SCA; never provision or modify it."""
        wallet = _object(self._request("GET", "/wallets/" + self._wallet_id).get("wallet"))
        if (
            wallet.get("id") != self._wallet_id
            or wallet.get("blockchain") != self._blockchain
            or wallet.get("custodyType") != "DEVELOPER"
            or wallet.get("accountType") != "SCA"
            or wallet.get("state") != "LIVE"
            or _address(wallet.get("address")) != self.address
        ):
            raise CircleWalletError("Circle wallet does not match its configured identity")

    def _ciphertext(self) -> str:
        public_key = self._request("GET", "/config/entity/publicKey").get("publicKey")
        if type(public_key) is not str:
            raise CircleWalletError("Circle entity public key is missing")
        try:
            key = RSA.import_key(public_key)
            if key.has_private() or key.size_in_bits() < 2048:
                raise ValueError("invalid public key")
            encrypted = PKCS1_OAEP.new(key, hashAlgo=SHA256).encrypt(self._entity_secret)
        except (ValueError, TypeError, IndexError):
            raise CircleWalletError("Circle entity public key is invalid") from None
        return base64.b64encode(encrypted).decode("ascii")

    def _reference(self, operation_ref: str) -> str:
        if type(operation_ref) is not str or not operation_ref or len(operation_ref) > 4096:
            raise ValidationError(ValidationCode.INVALID_VALUE, "invalid operation reference")
        preimage = json.dumps(
            [
                "mppi-circle-operation-v1",
                self._wallet_id,
                address_hex(self.address),
                self._chain_id,
                operation_ref,
            ],
            separators=(",", ":"),
        ).encode()
        # Circle requires v4-shaped UUIDs. Derivation is stable, not fresh randomness;
        # the caller's retained unique reference separates independent operations.
        return str(UUID(bytes=hashlib.sha256(preimage).digest()[:16], version=4))

    def _unknown(self, operation_ref: str) -> OperationResult:
        previous = self._known.get(operation_ref)
        return OperationResult(
            operation_ref,
            OperationState.UNKNOWN,
            None if previous is None else previous.wallet_operation_id,
            None if previous is None else previous.tx_hash,
        )

    def _record(self, operation_ref: str, record: dict[str, Any]) -> OperationResult:
        identifier = _identifier(record.get("id"))
        if (
            record.get("refId") != self._reference(operation_ref)
            or record.get("walletId") != self._wallet_id
            or record.get("blockchain") != self._blockchain
            or record.get("custodyType") != "DEVELOPER"
            or record.get("operation") != "CONTRACT_EXECUTION"
            or record.get("transactionType") != "OUTBOUND"
            or _address(record.get("sourceAddress")) != self.address
        ):
            raise CircleWalletError("Circle operation identity mismatch")
        target = _address(record.get("contractAddress"))
        expected = self._calls.get(operation_ref)
        if expected is not None and (
            target != expected.to
            or (record.get("callData") is not None and _hex(record["callData"]) != expected.data)
        ):
            raise CircleWalletError("Circle operation call mismatch")
        previous = self._known.get(operation_ref)
        if previous is not None and previous.wallet_operation_id not in (None, identifier):
            raise CircleWalletError("Circle operation identifier changed")
        tx_hash = None
        if record.get("txHash") not in (None, ""):
            tx_hash = _hex(record["txHash"], length=32)
        if previous is not None and previous.tx_hash is not None:
            if tx_hash is not None and tx_hash != previous.tx_hash:
                raise CircleWalletError("Circle transaction hash changed")
            tx_hash = previous.tx_hash
        raw_state = record.get("state")
        if type(raw_state) is not str:
            raise CircleWalletError("Circle transaction state is invalid")
        state = OperationState.UNKNOWN
        if raw_state in _PENDING or (raw_state in ("CONFIRMED", "COMPLETE") and tx_hash):
            state = OperationState.PENDING
        elif raw_state in _FAILED:
            state = OperationState.FAILED
        result = OperationResult(operation_ref, state, identifier, tx_hash)
        self._known[operation_ref] = result
        return result

    def _lookup(self, operation_ref: str) -> OperationResult | None:
        reference = self._reference(operation_ref)
        query = urlencode(
            {
                "refId": reference,
                "walletIds": self._wallet_id,
                # Circle rejects walletIds together with blockchain (verified
                # against Arc testnet on 2026-09-27). Scope by exact wallet and
                # reference, then enforce the chain on every returned record.
                "operation": "CONTRACT_EXECUTION",
                "custodyType": "DEVELOPER",
                "pageSize": 2,
            }
        )
        rows = self._request("GET", "/transactions?" + query).get("transactions")
        if not isinstance(rows, list):
            raise CircleWalletError("Circle transaction list is missing")
        if not rows:
            return None
        if len(rows) != 1:
            raise CircleWalletError("Circle operation lookup is ambiguous")
        summary = _object(rows[0])
        # Validate list identity before following its ID; never select a foreign row.
        self._record(operation_ref, summary)
        identifier = _identifier(summary.get("id"))
        transaction = self._request("GET", "/transactions/" + identifier).get("transaction")
        return self._record(operation_ref, _object(transaction))

    def execute(self, call: WalletCall, *, operation_ref: str) -> OperationResult:
        """Submit an explicit original intent; uncertainty never triggers a retry."""
        if not isinstance(call, WalletCall) or call.chain_id != self._chain_id:
            raise ValidationError(ValidationCode.INVALID_VALUE, "Circle call has the wrong network")
        reference = self._reference(operation_ref)
        with self._lock:
            previous = self._calls.get(operation_ref)
            if previous is not None and previous != call:
                raise ValidationError(
                    ValidationCode.INVALID_VALUE, "operation reference was reused"
                )
            self._calls[operation_ref] = call
            if operation_ref in self._attempted:
                return self.reconcile(operation_ref=operation_ref)
            try:
                prior = self._lookup(operation_ref)
                if prior is not None:
                    self._attempted.add(operation_ref)
                    return prior
                self.verify_wallet()
                ciphertext = self._ciphertext()
                # Mark before the POST. A lost response leaves this reference query-only.
                self._attempted.add(operation_ref)
                data = self._request(
                    "POST",
                    "/developer/transactions/contractExecution",
                    body={
                        "idempotencyKey": reference,
                        "refId": reference,
                        "walletId": self._wallet_id,
                        "contractAddress": address_hex(call.to),
                        "callData": "0x" + call.data.hex(),
                        "amount": "0",
                        "feeLevel": "MEDIUM",
                        "entitySecretCiphertext": ciphertext,
                    },
                )
                identifier = _identifier(data.get("id"))
                self._known[operation_ref] = OperationResult(
                    operation_ref, OperationState.PENDING, identifier
                )
                detail = self._request("GET", "/transactions/" + identifier).get("transaction")
                return self._record(operation_ref, _object(detail))
            except (CircleWalletError, ValidationError):
                return self._unknown(operation_ref)

    def reconcile(self, *, operation_ref: str) -> OperationResult:
        """Query the original service operation. This method never submits."""
        self._reference(operation_ref)
        with self._lock:
            try:
                return self._lookup(operation_ref) or self._unknown(operation_ref)
            except (CircleWalletError, ValidationError):
                return self._unknown(operation_ref)

    def sign_funding_authorization(self, authorization: FundingAuthorization) -> bytes:
        """Ask the funding SCA for the exact token proof; never sign MPPI vouchers."""
        if (
            not isinstance(authorization, FundingAuthorization)
            or authorization.chain_id != self._chain_id
            or authorization.owner != self.address
        ):
            raise ValidationError(ValidationCode.INVALID_VALUE, "funding authorization mismatch")
        typed_data = funding_typed_data(authorization)
        with self._lock:
            self.verify_wallet()
            data = self._request(
                "POST",
                "/developer/sign/typedData",
                body={
                    "walletId": self._wallet_id,
                    "data": json.dumps(typed_data, separators=(",", ":")),
                    "entitySecretCiphertext": self._ciphertext(),
                },
            )
            signature = _hex(data.get("signature"))
            if not signature:
                raise CircleWalletError("Circle returned an empty signature")
            return signature
