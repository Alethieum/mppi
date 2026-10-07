"""Circle Agent Wallet CLI 1.1.4 adapter with an operator-owned intent journal.

Uses the unmodified CLI and a caller-pinned SCA self-execution wrapper. Wallet
login/custody stay with Circle; MPPI's fixed SessionSigner remains separate.
Circle service status is never canonical payment confirmation. If execute loses
its returned transaction ID, the CLI has no reference lookup: retain UNKNOWN.

Sources: Circle CLI 1.1.4 dist/index.js, wallet/execute, wallet/sign and
transaction/list; developers.circle.com/agent-stack/agent-wallets/wallet-operations/.
"""

from __future__ import annotations

import json
import math
import os
import selectors
import signal
import subprocess
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

from mppi_protocol.capabilities import FundingAuthorization, WalletCall
from mppi_protocol.chain import ChainReader, CodePin, _data
from mppi_protocol.contract import ARC_USDC, _hash
from mppi_protocol.deployment import profile_to_mapping
from mppi_protocol.discovery import address_hex, parse_address, parse_document
from mppi_protocol.errors import ValidationCode, ValidationError
from mppi_protocol.registry import RegistryCalls
from mppi_protocol.types import OperationResult, OperationState

from .local_state import Checkpoint

_BLOCKCHAINS = {5042: "ARC", 5042002: "ARC-TESTNET"}
_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
_EXECUTE = "execute(address,uint256,bytes)"
_MAX_OUTPUT = 1 << 20
_MAX_STDERR = 64 << 10
_MAX_CALL_BYTES = 60 << 10  # One hex argument fits Linux's per-argument exec limit.
_MAX_OPERATIONS = 4096
_PENDING = {"INITIATED", "CLEARED", "QUEUED", "SENT", "STUCK", "CONFIRMED", "COMPLETE"}
_FAILED = {"FAILED", "CANCELLED", "DENIED"}


class CircleAgentWalletError(RuntimeError):
    """Local/remote wallet evidence unavailable; errors never include CLI output."""


_PROXY_HASH = bytes.fromhex("d09e34b2e51f09092e73e3ba1d520a42972496bf2a3bfdb22bb50b9729a9fd66")
_IMPLEMENTATION = CodePin(
    bytes.fromhex("895b96c92d1007872fe1f70606e6b7eeaebd620e"),
    bytes.fromhex("3692ce55057ce6c0da5f23b4fb8819fa6d5838758a5e88585b291fdccd36d0fa"),
)
# Network-specific Circle account versions. Mainnet v4 was bound to Circle's
# authenticated wallet metadata and creation receipt, then checked for ERC-1271
# and USDC authorization compatibility on both documented RPC endpoints.
# Public identities and verification references: docs/agent-quickstart.md.
_MAINNET_IMPLEMENTATION = CodePin(
    bytes.fromhex("9c6e09bc32d1e012dcaa2623e66d2cc9860c1aed"),
    bytes.fromhex("60ecaaadc845f14626e77541b390bd36a7f5c35efdee5c30471f0eb08626019e"),
)
_ACCOUNT_PINS = {
    5042002: (_PROXY_HASH, _IMPLEMENTATION),
    5042: (_PROXY_HASH, _MAINNET_IMPLEMENTATION),
}


def supported_account_pins(chain_id: int) -> tuple[bytes, CodePin]:
    """Return the reviewed Circle account implementation for the selected network."""
    try:
        return _ACCOUNT_PINS[chain_id]
    except KeyError:
        raise CircleAgentWalletError(
            "verified Circle account pins unavailable for this network"
        ) from None


def _object(value: object) -> dict[str, Any]:
    if type(value) is not dict:
        raise CircleAgentWalletError("Circle response object is missing")
    return cast(dict[str, Any], value)


def _address(value: object) -> bytes:
    if type(value) is not str:
        raise CircleAgentWalletError("Circle address is missing")
    return parse_address(value)


def _identifier(value: object) -> str:
    if type(value) is not str or len(value) != 36:
        raise CircleAgentWalletError("Circle transaction identifier is invalid")
    try:
        parsed = UUID(value)
    except ValueError:
        raise CircleAgentWalletError("Circle transaction identifier is invalid") from None
    if str(parsed) != value:
        raise CircleAgentWalletError("Circle transaction identifier is not canonical")
    return value


def _hex(value: object, *, size: int | None = None, maximum: int = _MAX_CALL_BYTES) -> bytes:
    if type(value) is not str or not value.startswith("0x") or len(value) > maximum * 2 + 2:
        raise CircleAgentWalletError("Circle bytes are invalid")
    try:
        decoded = bytes.fromhex(value[2:])
    except ValueError:
        raise CircleAgentWalletError("Circle bytes are invalid") from None
    if value[2:].lower() != decoded.hex() or size is not None and len(decoded) != size:
        raise CircleAgentWalletError("Circle bytes have invalid encoding or width")
    return decoded


def _reference(value: str) -> None:
    if type(value) is not str or not 0 < len(value) <= 4096:
        raise ValidationError(ValidationCode.INVALID_VALUE, "invalid operation reference")


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


class CircleAgentWallet:
    """FundingWallet using an authenticated official CLI and explicit account pins.

    ``command`` is a trusted executable prefix, not shell text. No secrets are
    accepted by this constructor or journal. Account runtime/implementation pins
    must have independently checked provenance. Explicit registration activation
    permits only the verified registry's exact register() call before deployment;
    funding and all other calls still require deployed, matching account code.
    Keep one journal for this wallet/profile. Its lock is owned only during each
    operation. Linux and macOS use the existing Checkpoint locking contract.
    """

    def __init__(
        self,
        *,
        chain: ChainReader,
        wallet: CodePin,
        implementation: CodePin,
        checkpoint_path: Path,
        command: tuple[str, ...] = ("circle",),
        timeout_seconds: float = 240,
        max_pages: int = 20,
        allow_registration_activation: bool = False,
        require_existing: bool = False,
    ) -> None:
        if (
            not isinstance(chain, ChainReader)
            or not isinstance(wallet, CodePin)
            or not isinstance(implementation, CodePin)
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "explicit chain and wallet pins required"
            )
        if (
            type(command) is not tuple
            or not 1 <= len(command) <= 16
            or any(
                type(arg) is not str or not 0 < len(arg) <= 4096 or "\0" in arg for arg in command
            )
            or type(timeout_seconds) not in (float, int)
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 300
            or type(max_pages) is not int
            or not 1 <= max_pages <= 100
            or type(allow_registration_activation) is not bool
            or type(require_existing) is not bool
        ):
            raise ValidationError(ValidationCode.INVALID_VALUE, "invalid Circle process limits")
        self._chain = chain
        self._wallet = wallet
        self._implementation = implementation
        self._chain_id = chain.profile.domain.chain_id
        self._blockchain = _BLOCKCHAINS[self._chain_id]
        self._command = command
        self._timeout = timeout_seconds
        self._max_pages = max_pages
        self._allow_registration_activation = allow_registration_activation
        self._require_existing = require_existing
        self._registration_call = (
            RegistryCalls(self._chain_id, chain.profile.registry_address)
            .register(wallet.address)
            .call
        )
        self._lock = threading.RLock()
        self._checkpoint_path = Path(checkpoint_path)
        self._checkpoint: Checkpoint | None = None
        self._operations: dict[str, Any] = {}
        self._scope = {
            "version": 1,
            "kind": "mppi-circle-agent-wallet",
            "profile": profile_to_mapping(chain.profile),
            "wallet": {
                "address": address_hex(wallet.address),
                "code_hash": "0x" + wallet.code_hash.hex(),
            },
            "implementation": {
                "address": address_hex(implementation.address),
                "code_hash": "0x" + implementation.code_hash.hex(),
            },
        }
        with self._journal():
            pass

    @contextmanager
    def _journal(self) -> Iterator[None]:
        """Reload/validate under one process and filesystem lock per invocation."""
        with self._lock:
            checkpoint = Checkpoint(self._checkpoint_path, require_existing=self._require_existing)
            try:
                self._load(checkpoint)
                self._checkpoint = checkpoint
                yield
            finally:
                self._checkpoint = None
                checkpoint.close()

    def _load(self, checkpoint: Checkpoint) -> None:
        try:
            record = checkpoint.load()
            if record is None:
                record = self._scope | {"operations": {}}
            if set(record) != set(self._scope) | {"operations"} or any(
                record[key] != value for key, value in self._scope.items()
            ):
                raise CircleAgentWalletError("Circle journal belongs to another wallet profile")
            operations = _object(record["operations"])
            if len(operations) > _MAX_OPERATIONS:
                raise CircleAgentWalletError("Circle journal operation limit exceeded")
            keys: set[str] = set()
            ids: set[str] = set()
            for reference, retained in operations.items():
                _reference(reference)
                row = self._validate_retained(retained)
                if row["idempotency_key"] in keys:
                    raise CircleAgentWalletError("Circle journal has reused idempotency keys")
                keys.add(row["idempotency_key"])
                if row["wallet_operation_id"] is not None:
                    if row["wallet_operation_id"] in ids:
                        raise CircleAgentWalletError("Circle journal has reused transaction IDs")
                    ids.add(row["wallet_operation_id"])
            self._operations = operations
        except Exception:
            raise CircleAgentWalletError("Circle journal could not be validated") from None

    @property
    def address(self) -> bytes:
        return self._wallet.address

    def _run(self, arguments: list[str]) -> bytes:
        """Bound output; let an original custody command finish its own polling.

        CLI 1.1.4 dist/index.js:20033-20107 owns challenge/transaction timeouts.
        Read commands have an SDK deadline. Killing an executing/signing command
        can discard its original result, so custody commands retain ownership
        until child exit, including when their bounded output is invalid.
        """
        custody = arguments[:2] in (["wallet", "execute"], ["wallet", "sign"])
        process: subprocess.Popen[bytes] | None = None
        try:
            process = subprocess.Popen(
                [*self._command, *arguments],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                start_new_session=True,
                env=dict(os.environ) | {"DO_NOT_TRACK": "1"},
            )
            assert process.stdout is not None and process.stderr is not None
            output = bytearray()
            error_bytes = 0
            output_error: str | None = None
            deadline = time.monotonic() + self._timeout
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ, "stdout")
                selector.register(process.stderr, selectors.EVENT_READ, "stderr")
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    if not custody and remaining <= 0:
                        raise CircleAgentWalletError("Circle command timed out")
                    for key, _ in selector.select(0.25 if custody else min(remaining, 0.25)):
                        chunk = os.read(key.fd, 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        if key.data == "stdout":
                            if len(output) + len(chunk) > _MAX_OUTPUT:
                                output_error = "Circle command output limit exceeded"
                                if not custody:
                                    raise CircleAgentWalletError(output_error)
                            elif output_error is None:
                                output.extend(chunk)
                        else:
                            error_bytes = min(error_bytes + len(chunk), _MAX_STDERR + 1)
                            if error_bytes > _MAX_STDERR:
                                output_error = "Circle command error-output limit exceeded"
                                if not custody:
                                    raise CircleAgentWalletError(output_error)
                remaining = deadline - time.monotonic()
                if not custody and remaining <= 0:
                    raise CircleAgentWalletError("Circle command timed out")
                status = process.wait(timeout=None if custody else remaining)
                if output_error is not None:
                    raise CircleAgentWalletError(output_error)
                if status != 0:
                    raise CircleAgentWalletError("Circle command did not return success")
            return bytes(output)
        except (OSError, ValueError, subprocess.SubprocessError):
            raise CircleAgentWalletError("Circle command is unavailable") from None
        finally:
            if process is not None:
                # The CLI may leave a descendant holding a pipe. This group is
                # created solely for this invocation, never the caller's group.
                cleanup_failed = False
                if custody and process.returncode is None:
                    # On a local read error, drain/discard the original process's
                    # remaining bounded chunks rather than replacing or killing it.
                    with selectors.DefaultSelector() as drain:
                        for stream in (process.stdout, process.stderr):
                            if stream is not None and not stream.closed:
                                drain.register(stream, selectors.EVENT_READ)
                        while drain.get_map():
                            for key, _ in drain.select(0.25):
                                try:
                                    chunk = os.read(key.fd, 65536)
                                except OSError:
                                    chunk = b""
                                if not chunk:
                                    drain.unregister(key.fileobj)
                    process.wait()
                elif process.returncode is None:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    except OSError:
                        cleanup_failed = True
                    try:
                        process.wait(timeout=5)
                    except (OSError, subprocess.SubprocessError):
                        cleanup_failed = True
                if process.stdout is not None:
                    process.stdout.close()
                if process.stderr is not None:
                    process.stderr.close()
                if cleanup_failed:
                    raise CircleAgentWalletError(
                        "Circle command cleanup did not complete"
                    ) from None

    def _json(self, arguments: list[str]) -> dict[str, Any]:
        try:
            return _object(parse_document(self._run([*arguments, "--output", "json"])).get("data"))
        except (ValueError, ValidationError):
            raise CircleAgentWalletError("Circle command returned invalid JSON") from None

    def verify_wallet(self) -> None:
        """Verify CLI selection and explicit canonical account code/implementation pins."""
        self._verify_wallet(allow_undeployed=False)

    def verify_registration_wallet(self) -> None:
        """Check explicitly opted-in registration custody; absence of code is permitted.

        This does not establish a deployed funding wallet. Call verify_wallet()
        before accepting completed setup or using the wallet for funding.
        """
        if not self._allow_registration_activation:
            raise CircleAgentWalletError("Circle registration activation was not enabled")
        self._verify_wallet(allow_undeployed=True)

    def _verify_wallet(self, *, allow_undeployed: bool) -> None:
        with self._lock:
            if self._run(["--version"]).strip() != b"1.1.4":
                raise CircleAgentWalletError("Circle CLI version must match verified 1.1.4")
            data = self._json(["wallet", "list", "--type", "agent", "--chain", self._blockchain])
            wallets = data.get("wallets")
            if type(wallets) is not list or len(wallets) > 50:
                raise CircleAgentWalletError("Circle wallet list is invalid")
            matches = [
                row
                for row in wallets
                if type(row) is dict
                and type(row.get("address")) is str
                and row["address"].lower() == address_hex(self.address)
            ]
            if (
                len(matches) != 1
                or matches[0].get("type") != "agent"
                or matches[0].get("blockchain") != self._blockchain
            ):
                raise CircleAgentWalletError(
                    "Circle wallet selection does not match configured identity"
                )
            block = self._chain.block()
            self._chain.verify_deployment(block)
            position = self._chain._position(block)
            for optional, pin in ((False, self._implementation), (allow_undeployed, self._wallet)):
                code = _data(
                    self._chain.rpc.call("eth_getCode", [address_hex(pin.address), position])
                )
                if optional and not code:
                    self._chain._check_canonical(block)
                    return
                if not code or _hash(code) != pin.code_hash:
                    raise CircleAgentWalletError("Circle SCA code differs from selected pins")
            slot = _data(
                self._chain.rpc.call(
                    "eth_getStorageAt", [address_hex(self.address), _SLOT, position]
                ),
                32,
            )
            if slot != bytes(12) + self._implementation.address:
                raise CircleAgentWalletError("Circle SCA implementation differs from selected pin")
            self._chain._check_canonical(block)

    def _call(self, value: object) -> WalletCall:
        row = _object(value)
        if set(row) != {"chain_id", "to", "data"}:
            raise CircleAgentWalletError("Circle journal call fields are invalid")
        call = WalletCall(row["chain_id"], parse_address(row["to"]), _hex(row["data"]))
        self._validate_call(call)
        return call

    def _validate_call(self, call: WalletCall) -> None:
        if (
            not isinstance(call, WalletCall)
            or call.chain_id != self._chain_id
            or len(call.data) > _MAX_CALL_BYTES
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "Circle call has wrong network or exceeds CLI limits"
            )

    def _validate_retained(self, value: object) -> dict[str, Any]:
        row = _object(value)
        fields = {"call", "idempotency_key", "wallet_operation_id", "tx_hash", "state"}
        if set(row) not in (fields, fields | {"route"}):
            raise CircleAgentWalletError("Circle journal operation fields are invalid")
        call = self._call(row["call"])
        # Version-1 journals predate route selection and always used self execution.
        route = row.get("route", "self")
        if route not in ("self", "direct_register") or (
            route == "direct_register" and call != self._registration_call
        ):
            raise CircleAgentWalletError("Circle journal execution route is invalid")
        if UUID(_identifier(row["idempotency_key"])).version != 4:
            raise CircleAgentWalletError("Circle idempotency key must be UUID v4")
        if row["wallet_operation_id"] is not None:
            _identifier(row["wallet_operation_id"])
        if row["tx_hash"] is not None:
            _hex(row["tx_hash"], size=32)
            if row["wallet_operation_id"] is None:
                raise CircleAgentWalletError("Circle transaction hash has no retained resource ID")
        if row["state"] not in ("unknown", "pending", "failed"):
            raise CircleAgentWalletError("Circle journal result state is invalid")
        return row

    def _save(self) -> None:
        if self._checkpoint is None:
            raise CircleAgentWalletError("Circle journal lock is required")
        self._checkpoint.save(self._scope | {"operations": self._operations})

    def _result(
        self, reference: str, row: dict[str, Any], *, unknown: bool = False
    ) -> OperationResult:
        state = OperationState.UNKNOWN if unknown else OperationState(row["state"])
        if state is OperationState.FAILED and row["tx_hash"] is None:
            # A service failure after submission is not proof that no write began.
            state = OperationState.UNKNOWN
        return OperationResult(
            reference,
            state,
            row["wallet_operation_id"],
            _hex(row["tx_hash"], size=32) if row["tx_hash"] is not None else None,
        )

    def _observe(
        self, reference: str, transaction: object, *, from_execute: bool = False
    ) -> OperationResult:
        retained = self._operations[reference]
        row = _object(transaction)
        identifier = _identifier(row.get("id"))
        known_id = retained["wallet_operation_id"]
        if known_id is not None and known_id != identifier:
            raise CircleAgentWalletError("Circle transaction identifier changed")
        if from_execute:
            if row.get("idempotencyKey") != retained["idempotency_key"]:
                raise CircleAgentWalletError(
                    "Circle execute reference differs from retained intent"
                )
        elif known_id is None:
            raise CircleAgentWalletError(
                "Circle list cannot prove the original submission reference"
            )
        call = self._call(retained["call"])
        parameters = row.get("abiParameters")
        direct = retained.get("route", "self") == "direct_register"
        destination = call.to if direct else self.address
        if direct:
            exact_call = (
                call == self._registration_call
                and row.get("abiFunctionSignature") == "register()"
                and (parameters is None or type(parameters) is list and not parameters)
            )
        else:
            exact_call = (
                row.get("abiFunctionSignature") == _EXECUTE
                and type(parameters) is list
                and len(parameters) == 3
                and parse_address(parameters[0]) == call.to
                and parameters[1] == "0"
                and _hex(parameters[2]) == call.data
            )
        if (
            row.get("blockchain") != self._blockchain
            or row.get("operation") != "CONTRACT_EXECUTION"
            or row.get("transactionType") != "OUTBOUND"
            or _address(row.get("sourceAddress")) != self.address
            or _address(row.get("contractAddress")) != destination
            or not exact_call
        ):
            raise CircleAgentWalletError("Circle transaction differs from retained exact call")
        if (
            row.get("destinationAddress") is not None
            and parse_address(row["destinationAddress"]) != destination
        ):
            raise CircleAgentWalletError("Circle transaction destination differs from selected SCA")
        for other_ref, other in self._operations.items():
            if other_ref != reference and other["wallet_operation_id"] == identifier:
                raise CircleAgentWalletError("Circle transaction already belongs to another intent")
        tx = None if row.get("txHash") in (None, "") else _hex(row["txHash"], size=32)
        if retained["tx_hash"] is not None:
            previous = _hex(retained["tx_hash"], size=32)
            if tx is not None and tx != previous:
                raise CircleAgentWalletError("Circle transaction hash changed")
            tx = previous
        state = row.get("state")
        if type(state) is not str or state not in _PENDING | _FAILED:
            raise CircleAgentWalletError("Circle transaction state is invalid")
        result_state = OperationState.FAILED if state in _FAILED else OperationState.PENDING
        retained.update(
            {
                "wallet_operation_id": identifier,
                "tx_hash": "0x" + tx.hex() if tx is not None else None,
                "state": result_state.value,
            }
        )
        self._save()
        if direct:
            # Preserve the original handles even if deployment produced unsupported
            # code. Setup must strictly verify deployment before reporting complete.
            self._verify_wallet(allow_undeployed=True)
        return self._result(reference, retained)

    def execute(self, call: WalletCall, *, operation_ref: str) -> OperationResult:
        """Persist one exact intent before invoking execute; repeat calls only reconcile."""
        _reference(operation_ref)
        self._validate_call(call)
        with self._journal():
            if operation_ref in self._operations:
                if self._call(self._operations[operation_ref]["call"]) != call:
                    raise ValidationError(
                        ValidationCode.INVALID_VALUE, "operation reference was reused"
                    )
                return self._reconcile(operation_ref)
            if len(self._operations) >= _MAX_OPERATIONS:
                raise CircleAgentWalletError("Circle journal operation limit exceeded")
            direct = self._allow_registration_activation and call == self._registration_call
            if direct:
                self.verify_registration_wallet()
            else:
                self.verify_wallet()
            row: dict[str, Any] = {
                "route": "direct_register" if direct else "self",
                "call": {
                    "chain_id": call.chain_id,
                    "to": address_hex(call.to),
                    "data": "0x" + call.data.hex(),
                },
                "idempotency_key": str(uuid4()),
                "wallet_operation_id": None,
                "tx_hash": None,
                "state": "unknown",
            }
            self._operations[operation_ref] = row
            self._save()  # Before any possible remote submission; never delete on uncertainty.
            try:
                # CLI 1.1.4 dist/index.js:96104-96255 forwards register() without
                # parameters and a zero native amount. Circle account-types, SCA:
                # https://developers.circle.com/wallets/account-types
                # First outbound transactions lazily deploy an SCA. Using the
                # intended registration avoids a separate activation transfer.
                execution = (
                    ["register()", "--amount", "0"]
                    if direct
                    else [_EXECUTE, address_hex(call.to), "0", "0x" + call.data.hex()]
                )
                result = self._json(
                    [
                        "wallet",
                        "execute",
                        *execution,
                        "--contract",
                        address_hex(call.to if direct else self.address),
                        "--address",
                        address_hex(self.address),
                        "--chain",
                        self._blockchain,
                        "--idempotency-key",
                        row["idempotency_key"],
                    ]
                )
                return self._observe(operation_ref, result, from_execute=True)
            except Exception:
                # A process failure, timeout or lost result is never proof of no write.
                return self._result(operation_ref, row, unknown=True)

    def reconcile(self, *, operation_ref: str) -> OperationResult:
        """Read the retained Circle transaction ID; never execute/sign/resubmit."""
        _reference(operation_ref)
        with self._journal():
            return self._reconcile(operation_ref)

    def _reconcile(self, operation_ref: str) -> OperationResult:
        retained = self._operations.get(operation_ref)
        if retained is None:
            return OperationResult(operation_ref, OperationState.UNKNOWN)
        identifier = retained["wallet_operation_id"]
        if identifier is None:
            return self._result(operation_ref, retained, unknown=True)
        try:
            cursor = None
            seen: set[str] = set()
            for _ in range(self._max_pages):
                args = [
                    "transaction",
                    "list",
                    "--address",
                    address_hex(self.address),
                    "--chain",
                    self._blockchain,
                    "--operation",
                    "execute",
                    "--tx-type",
                    "outbound",
                    "--limit",
                    "50",
                ]
                if cursor is not None:
                    args.extend(["--cursor", cursor])
                page = self._json(args).get("transactions")
                if type(page) is not list or len(page) > 50:
                    raise CircleAgentWalletError("Circle transaction page is invalid")
                found = None
                for transaction in page:
                    candidate = _object(transaction)
                    candidate_id = _identifier(candidate.get("id"))
                    if candidate_id in seen:
                        raise CircleAgentWalletError("Circle transaction listing is ambiguous")
                    seen.add(candidate_id)
                    cursor = candidate_id
                    if candidate_id == identifier:
                        found = candidate
                if found is not None:
                    return self._observe(operation_ref, found)
                if len(page) < 50:
                    break
        except Exception:
            pass
        return self._result(operation_ref, retained, unknown=True)

    def sign_funding_authorization(self, authorization: FundingAuthorization) -> bytes:
        """Sign the exact USDC token authorization; return unmodified SCA proof bytes."""
        if (
            not isinstance(authorization, FundingAuthorization)
            or authorization.chain_id != self._chain_id
            or authorization.owner != self.address
            or authorization.token != ARC_USDC
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "Circle funding authorization mismatch"
            )
        data = json.dumps(funding_typed_data(authorization), separators=(",", ":"))
        if len(data.encode()) > _MAX_CALL_BYTES:
            raise ValidationError(
                ValidationCode.LIMIT_EXCEEDED, "Circle signing payload exceeds CLI limits"
            )
        with self._journal():
            self.verify_wallet()
            response = self._json(
                [
                    "wallet",
                    "sign",
                    "typed-data",
                    data,
                    "--address",
                    address_hex(self.address),
                    "--chain",
                    self._blockchain,
                ]
            )
            signature = _hex(response.get("signature"), maximum=64 << 10)
            if not signature:
                raise CircleAgentWalletError("Circle returned an empty funding signature")
            return signature
