"""Exact ERC-8004 identity calls and canonical onboarding evidence.

Targets the implementation selected by DeploymentProfile, never an inferred
registry deployment. Caller-owned persistence retains operations before wallet
submission. This module neither submits nor retries transactions.

ABI/proof source: erc-8004/erc-8004-contracts at
b9e466c250744a7e06b13dff9d3c2844ed64f825, IdentityRegistryUpgradeable.sol.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from eth_abi.abi import encode

from .amounts import uint64
from .capabilities import WalletCall
from .catalog import DEFAULT_CATALOG_BYTES, Catalog, parse_catalog
from .chain import Block, ChainEvidenceError, ChainReader, _data, _object, _quantity
from .contract import ARC_USDC, _decode_exact, _hash
from .discovery import MAX_DOCUMENT_BYTES, address_hex, parse_address
from .errors import ValidationCode, ValidationError
from .http import TransportError
from .types import ChainObservation, OperationResult, OperationState, fixed_bytes, uint256

_MAX_SIGNATURE_BYTES = 64 << 10
_MAX_RECEIPT_LOGS = 16_384
_NAME = "ERC8004IdentityRegistry"
_VERSION = "1"
_REGISTERED = _hash(b"Registered(uint256,string,address)")
_TRANSFER = _hash(b"Transfer(address,address,uint256)")
_METADATA = _hash(b"MetadataSet(uint256,string,string,bytes)")
_URI = _hash(b"URIUpdated(uint256,string,address)")


def _call(signature: str, inputs: list[str], values: list[object]) -> bytes:
    return _hash(signature.encode("ascii"))[:4] + encode(inputs, values)


def _nullable_address(value: str) -> bytes:
    # Values come from exact ABI decoding. ERC-8004 deliberately clears a wallet
    # on transfer; ERC-721 mint events deliberately contain a zero sender.
    return fixed_bytes(bytes.fromhex(value[2:]), 20, field="registry address")


def _uri(value: str) -> None:
    if type(value) is not str or len(value) > MAX_DOCUMENT_BYTES:
        raise ValidationError(ValidationCode.LIMIT_EXCEEDED, "registry URI limit exceeded")
    try:
        raw = value.encode("utf-8")
    except UnicodeError as exc:
        raise ValidationError(ValidationCode.INVALID_VALUE, "registry URI is not UTF-8") from exc
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise ValidationError(ValidationCode.LIMIT_EXCEEDED, "registry URI limit exceeded")


class RegistryKind(StrEnum):
    REGISTER = "REGISTER"
    SET_AGENT_WALLET = "SET_AGENT_WALLET"
    SET_URI = "SET_URI"
    SET_CATALOG = "SET_CATALOG"


@dataclass(frozen=True, slots=True)
class RegistryOperation:
    """Immutable intended registry write; exact calldata is checked on construction."""

    call: WalletCall
    kind: RegistryKind
    owner: bytes
    agent_id: int | None = None
    agent_uri: str | None = None
    new_wallet: bytes | None = None
    deadline: int | None = None
    signature: bytes | None = field(default=None, repr=False)
    catalog_bytes: bytes | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.call, WalletCall) or self.call.chain_id not in (5042, 5042002):
            raise ValidationError(ValidationCode.INVALID_VALUE, "registry requires Arc")
        if not isinstance(self.kind, RegistryKind):
            raise ValidationError(ValidationCode.INVALID_VALUE, "invalid registry operation")
        fixed_bytes(self.owner, 20, field="owner", nonzero=True)
        if self.agent_id is not None:
            uint256(self.agent_id, field="agent_id")
        if self.agent_uri is not None:
            _uri(self.agent_uri)
        if self.kind is RegistryKind.REGISTER:
            if self.agent_id is not None:
                raise ValidationError(ValidationCode.INVALID_VALUE, "registration has no ID yet")
            expected = (
                _call("register()", [], [])
                if self.agent_uri is None
                else _call("register(string)", ["string"], [self.agent_uri])
            )
        else:
            if self.agent_id is None:
                raise ValidationError(ValidationCode.INVALID_VALUE, "agent_id required")
            if self.kind is RegistryKind.SET_AGENT_WALLET:
                fixed_bytes(self.new_wallet, 20, field="new_wallet", nonzero=True)  # type: ignore[arg-type]
                uint256(self.deadline, field="deadline")  # type: ignore[arg-type]
                if (
                    type(self.signature) is not bytes
                    or not 0 < len(self.signature) <= _MAX_SIGNATURE_BYTES
                ):
                    raise ValidationError(ValidationCode.INVALID_VALUE, "invalid wallet proof")
                expected = _call(
                    "setAgentWallet(uint256,address,uint256,bytes)",
                    ["uint256", "address", "uint256", "bytes"],
                    [self.agent_id, self.new_wallet, self.deadline, self.signature],
                )
            elif self.kind is RegistryKind.SET_URI:
                if self.agent_uri is None:
                    raise ValidationError(ValidationCode.INVALID_VALUE, "agent_uri required")
                expected = _call(
                    "setAgentURI(uint256,string)",
                    ["uint256", "string"],
                    [self.agent_id, self.agent_uri],
                )
            else:
                if self.catalog_bytes is None:
                    raise ValidationError(ValidationCode.INVALID_VALUE, "catalog required")
                parse_catalog(self.catalog_bytes)
                expected = _call(
                    "setMetadata(uint256,string,bytes)",
                    ["uint256", "string", "bytes"],
                    [self.agent_id, "mppi.catalog", self.catalog_bytes],
                )
        if self.kind is not RegistryKind.SET_AGENT_WALLET and any(
            value is not None for value in (self.new_wallet, self.deadline, self.signature)
        ):
            raise ValidationError(ValidationCode.INVALID_VALUE, "unexpected wallet proof fields")
        if (
            self.kind not in (RegistryKind.REGISTER, RegistryKind.SET_URI)
            and self.agent_uri is not None
        ):
            raise ValidationError(ValidationCode.INVALID_VALUE, "unexpected URI")
        if self.kind is not RegistryKind.SET_CATALOG and self.catalog_bytes is not None:
            raise ValidationError(ValidationCode.INVALID_VALUE, "unexpected catalog")
        if self.call.data != expected:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "registry calldata differs from intent"
            )


@dataclass(frozen=True, slots=True)
class RegistryCalls:
    chain_id: int
    registry_address: bytes

    def __post_init__(self) -> None:
        uint64(self.chain_id, field="chain_id", positive=True)
        if self.chain_id not in (5042, 5042002):
            raise ValidationError(ValidationCode.INVALID_VALUE, "registry requires Arc")
        fixed_bytes(self.registry_address, 20, field="registry_address", nonzero=True)

    def _wallet_call(self, data: bytes) -> WalletCall:
        return WalletCall(self.chain_id, self.registry_address, data)

    def register(self, owner: bytes, *, agent_uri: str | None = None) -> RegistryOperation:
        if agent_uri is not None:
            _uri(agent_uri)
        data = (
            _call("register()", [], [])
            if agent_uri is None
            else _call("register(string)", ["string"], [agent_uri])
        )
        return RegistryOperation(
            self._wallet_call(data), RegistryKind.REGISTER, owner, agent_uri=agent_uri
        )

    def set_agent_wallet(
        self, owner: bytes, agent_id: int, new_wallet: bytes, deadline: int, signature: bytes
    ) -> RegistryOperation:
        uint256(agent_id, field="agent_id")
        uint256(deadline, field="deadline")
        fixed_bytes(new_wallet, 20, field="new_wallet", nonzero=True)
        if type(signature) is not bytes or not 0 < len(signature) <= _MAX_SIGNATURE_BYTES:
            raise ValidationError(ValidationCode.INVALID_VALUE, "invalid wallet proof")
        data = _call(
            "setAgentWallet(uint256,address,uint256,bytes)",
            ["uint256", "address", "uint256", "bytes"],
            [agent_id, new_wallet, deadline, signature],
        )
        return RegistryOperation(
            self._wallet_call(data),
            RegistryKind.SET_AGENT_WALLET,
            owner,
            agent_id,
            new_wallet=new_wallet,
            deadline=deadline,
            signature=signature,
        )

    def set_uri(self, owner: bytes, agent_id: int, uri: str) -> RegistryOperation:
        uint256(agent_id, field="agent_id")
        _uri(uri)
        return RegistryOperation(
            self._wallet_call(
                _call("setAgentURI(uint256,string)", ["uint256", "string"], [agent_id, uri])
            ),
            RegistryKind.SET_URI,
            owner,
            agent_id,
            agent_uri=uri,
        )

    def set_catalog(self, owner: bytes, agent_id: int, catalog: Catalog) -> RegistryOperation:
        uint256(agent_id, field="agent_id")
        if not isinstance(catalog, Catalog) or parse_catalog(catalog.canonical_bytes) != catalog:
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "catalog declarations differ from bytes"
            )
        return RegistryOperation(
            self._wallet_call(
                _call(
                    "setMetadata(uint256,string,bytes)",
                    ["uint256", "string", "bytes"],
                    [agent_id, "mppi.catalog", catalog.canonical_bytes],
                )
            ),
            RegistryKind.SET_CATALOG,
            owner,
            agent_id,
            catalog_bytes=catalog.canonical_bytes,
        )


def agent_wallet_proof(
    chain_id: int,
    registry_address: bytes,
    agent_id: int,
    new_wallet: bytes,
    owner: bytes,
    deadline: int,
) -> dict[str, Any]:
    """Construct recipient-signed typed data; wallet_proof verifies its live context.

    The recipient's wallet signs this message (EOA ECDSA or ERC-1271). MPPI's
    session signer is unrelated. Never treat structural construction as proof.
    """
    RegistryCalls(chain_id, registry_address)
    uint256(agent_id, field="agent_id")
    uint256(deadline, field="deadline")
    fixed_bytes(new_wallet, 20, field="new_wallet", nonzero=True)
    fixed_bytes(owner, 20, field="owner", nonzero=True)
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "AgentWalletSet": [
                {"name": "agentId", "type": "uint256"},
                {"name": "newWallet", "type": "address"},
                {"name": "owner", "type": "address"},
                {"name": "deadline", "type": "uint256"},
            ],
        },
        "primaryType": "AgentWalletSet",
        "domain": {
            "name": _NAME,
            "version": _VERSION,
            "chainId": chain_id,
            "verifyingContract": address_hex(registry_address),
        },
        "message": {
            "agentId": agent_id,
            "newWallet": address_hex(new_wallet),
            "owner": address_hex(owner),
            "deadline": deadline,
        },
    }


@dataclass(frozen=True, slots=True)
class RegistryIdentity:
    observation: ChainObservation
    agent_id: int
    owner: bytes
    agent_wallet: bytes
    agent_uri: str
    catalog_bytes: bytes


@dataclass(frozen=True, slots=True)
class RegistryResult:
    operation: OperationResult
    identity: RegistryIdentity | None = None


class RegistryReader:
    """Read-only registry adapter; use outside the payment event loop."""

    def __init__(self, chain: ChainReader) -> None:
        self.chain = chain

    def verified_block(self) -> Block:
        block = self.chain.block()
        self.chain.verify_deployment(block)
        return block

    def _block(self, block: Block | None) -> Block:
        if block is None:
            return self.verified_block()
        self.chain.verify_deployment(block)
        return block

    def _read(
        self,
        signature: str,
        inputs: list[str],
        values: list[object],
        outputs: list[str],
        block: Block,
        *,
        address: bytes | None = None,
    ) -> tuple[Any, ...]:
        data = self.chain._call(
            self.chain.profile.registry_address if address is None else address,
            _call(signature, inputs, values),
            block,
        )
        if len(data) > DEFAULT_CATALOG_BYTES + MAX_DOCUMENT_BYTES + 256:
            raise ChainEvidenceError("registry response exceeds resource profile")
        return _decode_exact(outputs, data)

    def read(self, agent_id: int, *, block: Block | None = None) -> RegistryIdentity:
        uint256(agent_id, field="agent_id")
        observed = self._block(block)
        # getAgentWallet/getMetadata alone return default values for nonexistent IDs.
        owner = parse_address(
            self._read("ownerOf(uint256)", ["uint256"], [agent_id], ["address"], observed)[0]
        )
        fixed_bytes(owner, 20, field="owner", nonzero=True)
        wallet = _nullable_address(
            self._read("getAgentWallet(uint256)", ["uint256"], [agent_id], ["address"], observed)[0]
        )
        uri = self._read("tokenURI(uint256)", ["uint256"], [agent_id], ["string"], observed)[0]
        _uri(uri)
        catalog = self._read(
            "getMetadata(uint256,string)",
            ["uint256", "string"],
            [agent_id, "mppi.catalog"],
            ["bytes"],
            observed,
        )[0]
        if len(catalog) > DEFAULT_CATALOG_BYTES:
            raise ChainEvidenceError("registry catalog exceeds resource profile")
        self.chain._check_canonical(observed)
        return RegistryIdentity(observed.observation, agent_id, owner, wallet, uri, catalog)

    def balance(self, wallet: bytes, *, block: Block | None = None) -> int:
        """Arc's USDC ERC-20 balance in six-decimal units; never native 18-decimal gas units."""
        fixed_bytes(wallet, 20, field="wallet", nonzero=True)
        observed = self._block(block)
        decimals = self._read("decimals()", [], [], ["uint8"], observed, address=ARC_USDC)[0]
        if decimals != 6:
            raise ChainEvidenceError("unexpected Arc USDC decimals")
        balance = self._read(
            "balanceOf(address)",
            ["address"],
            [wallet],
            ["uint256"],
            observed,
            address=ARC_USDC,
        )[0]
        self.chain._check_canonical(observed)
        return int(balance)

    def wallet_proof(
        self,
        agent_id: int,
        new_wallet: bytes,
        *,
        owner: bytes,
        deadline: int,
        block: Block | None = None,
    ) -> dict[str, Any]:
        proof = agent_wallet_proof(
            self.chain.profile.domain.chain_id,
            self.chain.profile.registry_address,
            agent_id,
            new_wallet,
            owner,
            deadline,
        )
        observed = self._block(block)
        domain = self._read(
            "eip712Domain()",
            [],
            [],
            ["bytes1", "string", "string", "uint256", "address", "bytes32", "uint256[]"],
            observed,
        )
        if domain != (
            b"\x0f",
            _NAME,
            _VERSION,
            self.chain.profile.domain.chain_id,
            address_hex(self.chain.profile.registry_address),
            bytes(32),
            (),
        ):
            raise ChainEvidenceError("registry signing domain differs from selected profile")
        if not observed.timestamp <= deadline <= observed.timestamp + 300:
            raise ChainEvidenceError("recipient proof deadline outside registry five-minute window")
        identity = self.read(agent_id, block=observed)
        if identity.owner != owner:
            raise ChainEvidenceError("registry owner differs from intended caller")
        self.chain._check_canonical(observed)
        return proof

    def _operation(self, operation: RegistryOperation) -> None:
        if (
            not isinstance(operation, RegistryOperation)
            or operation.call.chain_id != self.chain.profile.domain.chain_id
            or operation.call.to != self.chain.profile.registry_address
        ):
            raise ChainEvidenceError("registry operation differs from selected profile")

    def simulate(self, operation: RegistryOperation) -> Block:
        """Check exact caller/calldata and recipient proof without submitting anything."""
        self._operation(operation)
        block = self.verified_block()
        if operation.agent_id is not None:
            if self.read(operation.agent_id, block=block).owner != operation.owner:
                raise ChainEvidenceError("registry caller must be identity owner")
        if operation.kind is RegistryKind.SET_AGENT_WALLET:
            assert operation.agent_id is not None
            assert operation.new_wallet is not None and operation.deadline is not None
            self.wallet_proof(
                operation.agent_id,
                operation.new_wallet,
                owner=operation.owner,
                deadline=operation.deadline,
                block=block,
            )
        raw = _data(
            self.chain.rpc.call(
                "eth_call",
                [
                    {
                        "from": address_hex(operation.owner),
                        "to": address_hex(operation.call.to),
                        "data": "0x" + operation.call.data.hex(),
                        "value": "0x0",
                    },
                    self.chain._position(block),
                ],
            )
        )
        if operation.kind is RegistryKind.REGISTER:
            _decode_exact(["uint256"], raw)  # Predicted ID is not a mined registration ID.
        elif raw:
            raise ChainEvidenceError("unexpected registry simulation output")
        self.chain._check_canonical(block)
        return block

    def reconcile(
        self,
        operation: RegistryOperation,
        *,
        operation_ref: str,
        tx_hash: bytes | None,
        wallet_operation_id: str | None = None,
    ) -> RegistryResult:
        """Confirm only exact registry events plus state from the canonical receipt block.

        ERC-4337 outer success can conceal failed wallet execution. Missing,
        conflicting or ambiguous identity evidence stays UNKNOWN and must not
        trigger duplicate registration. This function performs no write.
        """
        self._operation(operation)
        if tx_hash is None:
            return RegistryResult(
                OperationResult(operation_ref, OperationState.UNKNOWN, wallet_operation_id)
            )
        fixed_bytes(tx_hash, 32, field="tx_hash")
        try:
            self.chain._network()
            raw = self.chain.rpc.call("eth_getTransactionReceipt", ["0x" + tx_hash.hex()])
            if raw is None:
                return RegistryResult(
                    OperationResult(
                        operation_ref, OperationState.PENDING, wallet_operation_id, tx_hash
                    )
                )
            receipt = _object(raw)
            block = self.chain.block(_quantity(receipt.get("blockNumber")))
            if (
                _data(receipt.get("transactionHash"), 32) != tx_hash
                or _data(receipt.get("blockHash"), 32) != block.observation.block_hash
            ):
                raise ChainEvidenceError("registry receipt differs from canonical transaction")
            status = _quantity(receipt.get("status"))
            if status not in (0, 1):
                raise ChainEvidenceError("invalid receipt status")
            self.chain.verify_deployment(block)
            if status == 0:
                return RegistryResult(
                    OperationResult(
                        operation_ref,
                        OperationState.FAILED,
                        wallet_operation_id,
                        tx_hash,
                        block.observation,
                    )
                )
            events = self._events(receipt, block, tx_hash)
            identity = self._confirmed_identity(operation, events, block)
            self.chain._check_canonical(block)
            return RegistryResult(
                OperationResult(
                    operation_ref,
                    OperationState.CONFIRMED,
                    wallet_operation_id,
                    tx_hash,
                    block.observation,
                ),
                identity,
            )
        except (
            TransportError,
            ChainEvidenceError,
            ValidationError,
            ValueError,
            KeyError,
            TypeError,
        ):
            return RegistryResult(
                OperationResult(operation_ref, OperationState.UNKNOWN, wallet_operation_id, tx_hash)
            )

    def _events(
        self,
        receipt: dict[str, Any],
        block: Block,
        tx_hash: bytes,
    ) -> list[tuple[bytes, tuple[bytes, ...], bytes]]:
        logs = receipt.get("logs")
        if not isinstance(logs, list) or len(logs) > _MAX_RECEIPT_LOGS:
            raise ChainEvidenceError("missing or oversized receipt logs")
        tx_index = _quantity(receipt.get("transactionIndex"))
        previous = -1
        events = []
        for raw in logs:
            log = _object(raw)
            index = _quantity(log.get("logIndex"))
            if (
                index <= previous
                or log.get("removed") is not False
                or _data(log.get("transactionHash"), 32) != tx_hash
                or _quantity(log.get("transactionIndex")) != tx_index
                or _quantity(log.get("blockNumber")) != block.observation.block_number
                or _data(log.get("blockHash"), 32) != block.observation.block_hash
            ):
                raise ChainEvidenceError("registry event inclusion mismatch")
            previous = index
            if _data(log.get("address"), 20) != self.chain.profile.registry_address:
                continue
            topics = log.get("topics")
            if not isinstance(topics, list) or not 1 <= len(topics) <= 4:
                raise ChainEvidenceError("invalid registry event topics")
            decoded = tuple(_data(value, 32) for value in topics)
            if decoded[0] not in (_REGISTERED, _TRANSFER, _METADATA, _URI):
                continue
            data = log.get("data")
            if (
                type(data) is not str
                or len(data) > 2 * (DEFAULT_CATALOG_BYTES + MAX_DOCUMENT_BYTES + 256) + 2
            ):
                raise ChainEvidenceError("registry event exceeds resource profile")
            events.append((decoded[0], decoded, _data(data)))
        return events

    def _confirmed_identity(
        self,
        operation: RegistryOperation,
        events: list[tuple[bytes, tuple[bytes, ...], bytes]],
        block: Block,
    ) -> RegistryIdentity:
        agent_id = operation.agent_id
        if operation.kind is RegistryKind.REGISTER:
            matches = []
            for topic, topics, data in events:
                if topic != _REGISTERED:
                    continue
                if len(topics) != 3:
                    raise ChainEvidenceError("registry topic count mismatch")
                owner = parse_address(_decode_exact(["address"], topics[2])[0])
                # Other users share bundled transactions. Their arbitrary
                # Solidity string bytes need not be valid UTF-8 for our receipt.
                if owner != operation.owner:
                    continue
                if _decode_exact(["string"], data)[0] == (operation.agent_uri or ""):
                    matches.append(int.from_bytes(topics[1]))
            if len(matches) != 1:
                raise ChainEvidenceError("registration event missing or ambiguous")
            agent_id = matches[0]
        assert agent_id is not None
        transfers: list[tuple[bytes, bytes, int]] = []
        metadata: list[tuple[int, str, bytes]] = []
        uris: list[tuple[int, str, bytes]] = []
        for topic, topics, data in events:
            if topic == _REGISTERED:
                continue
            if topic in (_REGISTERED, _METADATA, _URI) and len(topics) != 3:
                raise ChainEvidenceError("registry topic count mismatch")
            if topic == _TRANSFER:
                if len(topics) != 4 or data:
                    raise ChainEvidenceError("registry transfer encoding mismatch")
                if int.from_bytes(topics[3]) != agent_id:
                    continue
                transfers.append(
                    (
                        _nullable_address(_decode_exact(["address"], topics[1])[0]),
                        _nullable_address(_decode_exact(["address"], topics[2])[0]),
                        int.from_bytes(topics[3]),
                    )
                )
            elif int.from_bytes(topics[1]) != agent_id:
                continue
            elif topic == _URI and operation.kind is RegistryKind.SET_URI:
                uris.append(
                    (
                        int.from_bytes(topics[1]),
                        _decode_exact(["string"], data)[0],
                        parse_address(_decode_exact(["address"], topics[2])[0]),
                    )
                )
            elif topic == _METADATA:
                wanted_key = (
                    "mppi.catalog" if operation.kind is RegistryKind.SET_CATALOG else "agentWallet"
                )
                if topics[2] != _hash(wanted_key.encode("ascii")):
                    continue
                key, value = _decode_exact(["string", "bytes"], data)
                if _hash(key.encode("utf-8")) != topics[2]:
                    raise ChainEvidenceError("registry indexed metadata key mismatch")
                metadata.append((int.from_bytes(topics[1]), key, value))
        if operation.kind is RegistryKind.REGISTER:
            if (
                transfers.count((bytes(20), operation.owner, agent_id)) != 1
                or metadata.count((agent_id, "agentWallet", operation.owner)) != 1
            ):
                raise ChainEvidenceError("registration mint or initial wallet evidence missing")
        identity = self.read(agent_id, block=block)
        if identity.owner != operation.owner:
            raise ChainEvidenceError("confirmed identity owner differs from intended caller")
        if operation.kind is RegistryKind.REGISTER:
            matches_state = (
                identity.agent_wallet == operation.owner
                and identity.agent_uri == (operation.agent_uri or "")
                and identity.catalog_bytes == b""
            )
        elif operation.kind is RegistryKind.SET_URI:
            assert operation.agent_uri is not None
            matches_state = (
                uris.count((agent_id, operation.agent_uri, operation.owner)) == 1
                and identity.agent_uri == operation.agent_uri
            )
        else:
            key = (
                "agentWallet" if operation.kind is RegistryKind.SET_AGENT_WALLET else "mppi.catalog"
            )
            value = (
                operation.new_wallet
                if operation.kind is RegistryKind.SET_AGENT_WALLET
                else operation.catalog_bytes
            )
            assert value is not None
            matches_state = (
                metadata.count((agent_id, key, value)) == 1
                and (identity.agent_wallet if key == "agentWallet" else identity.catalog_bytes)
                == value
            )
        if not matches_state:
            raise ChainEvidenceError("registry event and intended state differ")
        return identity
