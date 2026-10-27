"""Circle Agent Wallet CLI 1.1.4 adapter with an operator-owned intent journal.

Uses the unmodified CLI and the account's own self-execution call. Wallet
login/custody stay with Circle; MPPI's fixed SessionSigner remains separate.
It accepts any Circle account generation: before submitting an opening, the
agent checks the signature as USDC will and that its agent id binds this wallet.
Circle service status is never canonical payment confirmation. If execute loses
its returned transaction ID, the CLI has no reference lookup: retain UNKNOWN.
An execute checks the CLI version and wallet selection first unless a check
began within the last 60 seconds; signing relies on the last passing check
until a command fails. The journal's lock is held only while it is read or
replaced, never while a CLI command runs; an execute or reconcile waits
for it up to its CLI timeout. A result Circle returned that the journal then
cannot take is returned UNKNOWN with Circle's transaction id and hash, and
written at the adapter's next journal step. A record keeps its call's digest,
not the call; retire marks it with a time from which it is forgotten, once it
holds a transaction hash, and a reconcile of it withdraws that mark.

Sources: Circle CLI 1.1.4 dist/index.js, wallet/execute, wallet/sign and
transaction/list; developers.circle.com/agent-stack/agent-wallets/wallet-operations/.
"""

from __future__ import annotations

import errno
import json
import math
import os
import selectors
import signal
import subprocess
import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any, NoReturn, cast
from uuid import UUID, uuid4

from Crypto.Hash import keccak

from mppi_protocol.capabilities import FundingAuthorization, NotSubmittedError, WalletCall
from mppi_protocol.chain import ChainReader
from mppi_protocol.contract import ARC_USDC
from mppi_protocol.deployment import profile_to_mapping
from mppi_protocol.discovery import address_hex, parse_address, parse_document
from mppi_protocol.errors import ValidationCode, ValidationError
from mppi_protocol.registry import RegistryCalls
from mppi_protocol.types import OperationResult, OperationState, fixed_bytes

from .local_state import _LOCK_WAIT_SECONDS, Checkpoint

_BLOCKCHAINS = {5042: "ARC", 5042002: "ARC-TESTNET"}
_EXECUTE = "execute(address,uint256,bytes)"
_MAX_OUTPUT = 1 << 20
_MAX_STDERR = 64 << 10
_MAX_CALL_BYTES = 60 << 10  # One hex argument fits Linux's per-argument exec limit.
# The journal's only bound, the same as the Rust adapter's: a new call it cannot
# record is refused before Circle is asked. From three quarters of it,
# near_capacity is true.
_MAX_JOURNAL_BYTES = 16 << 20
# A retirement mark is whole Unix seconds below this bound, which the Rust
# adapter's u64 shares, so both read every journal either writes.
_MAX_RETIRE_AT = 1 << 64
# An execute checks the CLI and wallet first unless a check that passed began
# less than this many seconds ago; signing and verify_wallet accept any.
_CHECK_SECONDS = 60
_PENDING = {"INITIATED", "CLEARED", "QUEUED", "SENT", "STUCK", "CONFIRMED", "COMPLETE"}
_FAILED = {"FAILED", "CANCELLED", "DENIED"}
# CLI 1.1.4 reports a failure as {"error": {"code", "message", ...}} on stdout and
# exits 1 (dist/index.js:732-750). These are all the codes its callers pass:
# literals, the HTTP status mappings (:97292-97300, :113126-113134), the remote
# version block (:974) and the one propagated exception code (:113304-113306).
# Only a listed code is retained, never the message or any other output.
_CLI_ERRORS = frozenset(
    {
        "AUTH_REQUIRED",
        "AUTH_EXPIRED",
        "PERMISSION_DENIED",
        "INVALID_ARGUMENT",
        "NOT_FOUND",
        "CONFLICT",
        "TIMEOUT",
        "VERSION_BLOCKED",
        "LOCAL_WALLET_UNAVAILABLE",
        "INTERNAL",
    }
)


class CircleAgentWalletError(RuntimeError):
    """Local/remote wallet evidence unavailable. Errors never include CLI output;
    a failed command names only the CLI's listed error code."""


class CircleNotSubmittedError(NotSubmittedError, CircleAgentWalletError):
    """Execute failed before its intent was journaled, so Circle was never asked.

    Still a CircleAgentWalletError for callers that catch that; its cause is
    the error that stopped it.
    """


def _failed(output: bytes) -> str:
    """A failed command's error, naming the CLI's code when it is a listed one."""
    message = "Circle command did not return success"
    try:
        error = parse_document(output).get("error")
    except ValueError:
        return message
    code = error.get("code") if type(error) is dict else None
    return f"{message} ({code})" if type(code) is str and code in _CLI_ERRORS else message


def _cause(error: Exception) -> str:
    """An unresolved result's cause: the error's kind and, for this adapter's
    own errors, their SDK-written reason. Never CLI output."""
    if isinstance(error, CircleAgentWalletError):
        return f"CircleAgentWalletError: {error}"
    return type(error).__name__


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


def _digest(call: WalletCall) -> bytes:
    """What a record binds: keccak256(uint256 chainId || to || data). Only data
    varies in length, so two calls never share these bytes."""
    raw = call.chain_id.to_bytes(32, "big") + call.to + call.data
    return keccak.new(digest_bits=256, data=raw).digest()


def _growth(row: dict[str, Any]) -> int:
    """Bytes a record may still grow by: the transaction id and hash Circle has
    yet to report replace null (a 36-character id and a 66-character hash), and
    so may a retirement mark (at most 20 digits). A later mark only lowers it,
    which never adds a digit; once a reconcile clears it, a new mark of as many
    digits (every second from 2001 to 2286 has ten) brings the record back to
    its size."""
    return (
        (34 if row["wallet_operation_id"] is None else 0)
        + (64 if row["tx_hash"] is None else 0)
        + (16 if row["retire_at"] is None else 0)
    )


def _due(row: dict[str, Any], now: float) -> bool:
    """Whether a save made at Unix time ``now`` forgets the record."""
    return row["tx_hash"] is not None and row["retire_at"] is not None and row["retire_at"] <= now


def _valid_reference(value: object) -> bool:
    return type(value) is str and 0 < len(value) <= 4096


def _reference(value: str) -> None:
    if not _valid_reference(value):
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
    """FundingWallet using the authenticated official CLI 1.1.4.

    ``command`` is a trusted executable prefix, not shell text. No secrets are
    accepted by this constructor or journal. The CLI must select ``address`` as
    an agent wallet on this network. Its account code is not pinned: an account
    that cannot execute or sign these calls fails them, and the agent checks
    every funding signature before submitting it. Explicit registration
    activation sends only the verified registry's exact register() call directly;
    every other call goes through the account's own execute with zero value.
    Keep one journal for this wallet/profile. Its file lock is held only to read
    and save it, never while a CLI command runs, so another process sharing it
    goes on meanwhile; ``timeout_seconds`` also bounds how long an execute or
    reconcile waits for it. Linux and macOS use the existing Checkpoint locking
    contract.
    """

    def __init__(
        self,
        *,
        chain: ChainReader,
        address: bytes,
        checkpoint_path: Path,
        command: tuple[str, ...] = ("circle",),
        timeout_seconds: float = 240,
        max_pages: int = 20,
        allow_registration_activation: bool = False,
        require_existing: bool = False,
    ) -> None:
        if not isinstance(chain, ChainReader):
            raise ValidationError(ValidationCode.INVALID_VALUE, "explicit chain reader required")
        fixed_bytes(address, 20, field="address", nonzero=True)
        if chain.profile.domain.chain_id not in _BLOCKCHAINS:
            raise CircleAgentWalletError("Circle Agent Wallets are unavailable for this network")
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
        self._address = address
        self._chain_id = chain.profile.domain.chain_id
        self._blockchain = _BLOCKCHAINS[self._chain_id]
        self._command = command
        self._timeout = timeout_seconds
        self._max_pages = max_pages
        self._allow_registration_activation = allow_registration_activation
        self._require_existing = require_existing
        self._registration_call = (
            RegistryCalls(self._chain_id, chain.profile.registry_address).register(address).call
        )
        # One operation at a time for this object, CLI commands included: it
        # guards the journal held open (_operations, _checkpoint) and lets no
        # command fail unseen between the start of a check and the record that
        # check sets. Another process sharing the journal is kept apart only by
        # the journal's file lock.
        self._lock = threading.RLock()
        # When the last passing check of the CLI's version and its selection of
        # this wallet began (time.monotonic()), written under the lock. Only the
        # environment can change them while the process runs. A login that
        # expires, ends or switches accounts makes the next command fail, as
        # does a missing CLI. Signing, which submits nothing, relies on this
        # whatever its age; an execute checks again once it is _CHECK_SECONDS
        # old. A check clears it first, and any failed command or refused
        # output clears it. Restart after installing another CLI version.
        self._checked_at: float | None = None
        # Whether the journal was at least three quarters of its bound when last
        # read or written (near_capacity).
        self._near_capacity = False
        self._checkpoint_path = Path(checkpoint_path)
        self._checkpoint: Checkpoint | None = None
        self._operations: dict[str, Any] = {}
        # What Circle reported for each reference whose record step failed,
        # written under the lock. The next journal step merges it into the
        # journal first.
        self._unrecorded: dict[str, dict[str, Any]] = {}
        self._scope = {
            "version": 2,
            "kind": "mppi-circle-agent-wallet",
            "profile": profile_to_mapping(chain.profile),
            "wallet": address_hex(address),
        }
        with self._journal():
            pass

    @contextmanager
    def _journal(self, lock_wait: float = _LOCK_WAIT_SECONDS) -> Iterator[None]:
        """The journal, reloaded and validated, held under the in-process lock
        and the journal's file lock until the block ends; opening waits up to
        ``lock_wait`` seconds for another holder. An execute or reconcile
        waits up to its CLI timeout; other steps a second, as other records.
        What an earlier record step could not save is merged into the journal
        first, so the block's own save writes it; a block that saves nothing
        has it saved alone after. A failed save of it keeps it in memory and
        is never the block's failure. That save writes the journal the block
        leaves, which holds only what the block meant to save: an intent that
        did not save is taken back out (_save_intent)."""
        with self._lock:
            checkpoint = Checkpoint(
                self._checkpoint_path,
                require_existing=self._require_existing,
                lock_wait=lock_wait,
            )
            try:
                self._load(checkpoint)
                self._checkpoint = checkpoint
                self._merge_unrecorded()
                try:
                    yield
                finally:
                    # A save that lands clears them (_save).
                    if self._unrecorded:
                        with suppress(Exception):
                            self._save()
            finally:
                self._checkpoint = None
                checkpoint.close()

    def _load(self, checkpoint: Checkpoint) -> None:
        try:
            record = checkpoint.load()
            if record is None:
                record = self._scope | {"operations": {}}
            else:
                with suppress(OSError):
                    self._measure(checkpoint.path.stat().st_size)
            if not self._owns(record):
                raise CircleAgentWalletError("Circle journal belongs to another wallet profile")
            operations: dict[str, Any] = {}
            keys: set[str] = set()
            ids: set[str] = set()
            for reference, retained in _object(record["operations"]).items():
                _reference(reference)
                row = operations[reference] = self._validate_retained(retained)
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

    def _owns(self, record: dict[str, Any]) -> bool:
        """This wallet and profile's journal. Version 1 also retained account
        code pins, which no longer select anything; the next save drops them."""
        scope = self._scope
        if record.get("version") == 1:
            pinned = record.get("wallet")
            return (
                set(record) == set(scope) | {"implementation", "operations"}
                and (record["kind"], record["profile"]) == (scope["kind"], scope["profile"])
                and type(pinned) is dict
                and pinned.get("address") == scope["wallet"]
            )
        return set(record) == set(scope) | {"operations"} and all(
            record[key] == value for key, value in scope.items()
        )

    @property
    def address(self) -> bytes:
        return self._address

    def _run(self, arguments: list[str]) -> bytes:
        """Bound output; let an original custody command finish its own polling.

        CLI 1.1.4 dist/index.js:20033-20107 owns challenge/transaction timeouts.
        Read commands have an SDK deadline. Killing an executing/signing command
        can discard its original result, so custody commands retain ownership
        until child exit, including when their bounded output is invalid.
        """
        custody = arguments[:2] in (["wallet", "execute"], ["wallet", "sign"])
        process: subprocess.Popen[bytes] | None = None
        # The group is stopped only on the way out of an error. That error
        # always wins; a stop that fails goes with it as a note.
        cleanup_failed = False
        try:
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
                        raise CircleAgentWalletError(_failed(bytes(output)))
                return bytes(output)
            except (OSError, ValueError, subprocess.SubprocessError):
                raise CircleAgentWalletError("Circle command is unavailable") from None
            finally:
                if process is not None:
                    # The CLI may leave a descendant holding a pipe. This group is
                    # created solely for this invocation, never the caller's group.
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
                        except OSError as error:
                            # macOS refuses the signal (EPERM) to a group whose
                            # members have all exited, the leader not yet reaped;
                            # with any member alive it signals them (checked on
                            # Darwin 27.0). Nothing is then left to stop.
                            if error.errno != errno.EPERM or process.poll() is None:
                                cleanup_failed = True
                        try:
                            process.wait(timeout=5)
                        except (OSError, subprocess.SubprocessError):
                            cleanup_failed = True
                    if process.stdout is not None:
                        process.stdout.close()
                    if process.stderr is not None:
                        process.stderr.close()
        except BaseException as failure:
            if cleanup_failed:
                failure.add_note("Circle command cleanup did not complete")
            raise

    def _json(self, arguments: list[str]) -> dict[str, Any]:
        """A command's JSON data object. A command that fails, or prints
        anything else, clears the check of the CLI and wallet."""
        try:
            return _object(parse_document(self._run([*arguments, "--output", "json"])).get("data"))
        except Exception as error:
            self._checked_at = None
            if isinstance(error, ValueError):
                raise CircleAgentWalletError("Circle command returned invalid JSON") from None
            raise

    def verify_wallet(self) -> None:
        """Verify the CLI version and its selection of this agent wallet unless
        a check passed since the last failed command, whatever its age: after a
        command fails, or prints output the adapter refuses, the next call
        verifies again. An execute checks again unless the last passing check
        began within 60 seconds."""
        with self._lock:
            if self._checked_at is None:
                self._check()

    def _check(self) -> None:
        """Check the CLI version and wallet selection now; under the lock. The
        record is cleared first and set, to when the check began, only once all
        of it passes."""
        self._checked_at = None
        began = time.monotonic()
        self._verify_selection()
        self._checked_at = began

    def verify_registration_wallet(self) -> None:
        """verify_wallet for a wallet explicitly opted in to registration activation."""
        if not self._allow_registration_activation:
            raise CircleAgentWalletError("Circle registration activation was not enabled")
        self.verify_wallet()

    def _verify_selection(self) -> None:
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
        """The record in its compact form, which binds its call by digest."""
        row = _object(value)
        kept = ("idempotency_key", "wallet_operation_id", "tx_hash", "state")
        if "call" in row:
            # An earlier release kept the whole call, and before that no route:
            # only self execution. Its digest binds it now, unmarked; the next
            # save writes the compact record, which an earlier release refuses.
            if set(row) not in ({"call", *kept}, {"call", "route", *kept}):
                raise CircleAgentWalletError("Circle journal operation fields are invalid")
            row = (
                {
                    "route": row.get("route", "self"),
                    "call_digest": "0x" + _digest(self._call(row["call"])).hex(),
                }
                | {key: row[key] for key in kept}
                | {"retire_at": None}
            )
        if set(row) != {"route", "call_digest", "retire_at", *kept}:
            raise CircleAgentWalletError("Circle journal operation fields are invalid")
        retire_at = row["retire_at"]
        if retire_at is not None and (
            type(retire_at) is not int or not 0 <= retire_at < _MAX_RETIRE_AT
        ):
            raise CircleAgentWalletError("Circle journal retirement time is invalid")
        bound = _hex(row["call_digest"], size=32)
        route = row["route"]
        if route not in ("self", "direct_register") or (
            route == "direct_register" and bound != _digest(self._registration_call)
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

    def _save(self, *, reserve: int = 0) -> None:
        """Replace the journal, first forgetting every record due to retire;
        refused if it would leave less than ``reserve`` bytes of its bound."""
        if self._checkpoint is None:
            raise CircleAgentWalletError("Circle journal lock is required")
        self._forget_due()
        record = self._scope | {"operations": self._operations}
        # Checkpoint.save writes exactly this encoding.
        size = len(json.dumps(record, ensure_ascii=True, separators=(",", ":"), allow_nan=False))
        if size + reserve > _MAX_JOURNAL_BYTES:
            raise CircleAgentWalletError("Circle journal operation capacity reached")
        self._checkpoint.save(record)
        self._measure(size)
        # Every journal block merges each kept answer into the journal it
        # saves, so this save wrote them all.
        self._unrecorded.clear()

    def _forget_due(self) -> bool:
        """Forget each record due to retire by the wall clock that holds a
        transaction hash; whether any was. A record without one keeps its mark,
        since its call may still reach Circle."""
        now = time.time()
        kept = {ref: row for ref, row in self._operations.items() if not _due(row, now)}
        forgot = len(kept) < len(self._operations)
        self._operations = kept
        return forgot

    def _measure(self, size: int) -> None:
        self._near_capacity = size >= _MAX_JOURNAL_BYTES // 4 * 3

    @property
    def near_capacity(self) -> bool:
        """Whether the journal was at least three quarters of its bound when last
        read or written. Past the bound, a new call is refused unsent."""
        return self._near_capacity

    def retire(self, *, operation_refs: Iterable[str], at: float) -> None:
        """Mark each named record to be forgotten from ``at``, Unix time in
        seconds as from time.time(), keeping an earlier mark, in one atomic
        replacement of the journal. The mark is kept in the journal, so a
        restart keeps it. That save, like every save, forgets each record that
        is due and holds a transaction hash; any other keeps its mark, since its
        call may have reached Circle. A reference with no record is skipped, as
        is one no record can have (not a string, empty or over 4,096
        characters). Name only references never presented again once ``at``
        has passed: the provider runtime names those of the sessions it
        retired, the agent those of its own retired session. A reconcile of a
        reference withdraws its mark: a recovery from an older checkpoint may
        present it again."""
        if type(at) not in (int, float) or not math.isfinite(at) or not 0 <= at < _MAX_RETIRE_AT:
            raise ValidationError(ValidationCode.INVALID_VALUE, "invalid retirement time")
        due = math.floor(at)
        references = tuple(operation_refs)
        with self._journal():
            marked = False
            for reference in references:
                row = self._operations.get(reference) if _valid_reference(reference) else None
                if row is not None and (row["retire_at"] is None or due < row["retire_at"]):
                    row["retire_at"] = due
                    marked = True
            if marked or self._forget_due():
                self._save()

    def _result(
        self,
        reference: str,
        row: dict[str, Any],
        *,
        unknown: bool = False,
        cause: str | None = None,
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
            cause=cause,
        )

    def _evidence(
        self, retained: dict[str, Any], transaction: object, *, from_execute: bool = False
    ) -> dict[str, Any]:
        """What Circle reported for the call ``retained`` binds, checked against
        that record without the journal: the record with Circle's transaction
        id, hash and state."""
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
        bound = _hex(retained["call_digest"], size=32)
        parameters = row.get("abiParameters")
        direct = retained["route"] == "direct_register"
        destination = self._registration_call.to if direct else self.address
        if direct:
            exact_call = (
                bound == _digest(self._registration_call)
                and row.get("abiFunctionSignature") == "register()"
                and (parameters is None or type(parameters) is list and not parameters)
            )
        else:
            exact_call = (
                row.get("abiFunctionSignature") == _EXECUTE
                and type(parameters) is list
                and len(parameters) == 3
                and parameters[1] == "0"
                and _digest(
                    WalletCall(self._chain_id, parse_address(parameters[0]), _hex(parameters[2]))
                )
                == bound
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
        return retained | {
            "wallet_operation_id": identifier,
            "tx_hash": "0x" + tx.hex() if tx is not None else None,
            "state": result_state.value,
        }

    def _merge(self, reference: str, evidence: dict[str, Any]) -> None:
        """Merge ``evidence`` into the reference's record in the journal just
        read under its lock: only that record changes. Raises KeyError if the
        journal no longer holds the record, and CircleAgentWalletError if it
        holds other evidence for it or gives its transaction id to another."""
        retained = self._operations[reference]
        identifier = evidence["wallet_operation_id"]
        for other_ref, other in self._operations.items():
            if other_ref != reference and other["wallet_operation_id"] == identifier:
                raise CircleAgentWalletError("Circle transaction already belongs to another intent")
        if retained["idempotency_key"] != evidence["idempotency_key"]:
            raise CircleAgentWalletError("Circle journal holds another intent for the reference")
        if retained["wallet_operation_id"] not in (None, identifier):
            raise CircleAgentWalletError("Circle transaction identifier changed")
        tx = evidence["tx_hash"]
        if retained["tx_hash"] is not None:
            if tx is not None and tx != retained["tx_hash"]:
                raise CircleAgentWalletError("Circle transaction hash changed")
            tx = retained["tx_hash"]
        retained.update(
            {"wallet_operation_id": identifier, "tx_hash": tx, "state": evidence["state"]}
        )

    def _merge_unrecorded(self) -> None:
        """Merge into the journal just read what Circle reported for each
        reference whose record step failed, before any other change. One the
        journal no longer holds the record for, or refuses, is dropped: the
        journal then holds its own evidence of it, and the record step's
        UNKNOWN result carried Circle's."""
        kept: dict[str, dict[str, Any]] = {}
        for reference, evidence in self._unrecorded.items():
            try:
                self._merge(reference, evidence)
            except (KeyError, CircleAgentWalletError):
                continue
            kept[reference] = evidence
        self._unrecorded = kept

    def _record(
        self,
        reference: str,
        retained: dict[str, Any],
        transaction: object,
        *,
        from_execute: bool = False,
    ) -> OperationResult:
        """Record what Circle reported for ``retained``, the reference's record
        as this operation read it: checked against it, then merged into the
        journal read again under its lock. If that step fails (the lock not
        taken within the timeout, a journal that cannot be read or validated,
        a record no longer there, a merge the journal refuses, a failed save),
        the result is UNKNOWN with Circle's transaction id and hash, kept to be
        written at the next journal step."""
        evidence = self._evidence(retained, transaction, from_execute=from_execute)
        try:
            with self._journal(self._timeout):
                recorded = self._operations[reference]
                self._merge(reference, evidence)
                self._save()
        except Exception as error:
            self._unrecorded[reference] = evidence
            return self._result(reference, evidence, unknown=True, cause=_cause(error))
        # The save forgets the record if it is due to retire; it is still reported.
        return self._result(reference, recorded)

    def _save_intent(self, call: WalletCall, operation_ref: str) -> dict[str, Any] | None:
        """This call's intent, saved under the journal's lock, or None when the
        reference already binds it; a reference bound to another call is
        refused as reused. The CLI and wallet are checked without that lock,
        and the journal is read again after."""
        checked = False
        while True:
            saving = False
            with self._journal(self._timeout):
                retained = self._operations.get(operation_ref)
                if retained is not None:
                    if not isinstance(call, WalletCall) or _hex(
                        retained["call_digest"], size=32
                    ) != _digest(call):
                        raise ValidationError(
                            ValidationCode.INVALID_VALUE, "operation reference was reused"
                        )
                    return None
                # The journal holds no record of this reference, and Circle is
                # asked only after one is saved: every refusal until then is a
                # definite NotSubmittedError.
                try:
                    self._validate_call(call)
                    # Execute moves money: unless a check began within
                    # _CHECK_SECONDS, check first, so a login lost since fails
                    # before this save, never sent.
                    if checked or (
                        self._checked_at is not None
                        and time.monotonic() - self._checked_at < _CHECK_SECONDS
                    ):
                        direct = (
                            self._allow_registration_activation and call == self._registration_call
                        )
                        row: dict[str, Any] = {
                            "route": "direct_register" if direct else "self",
                            "call_digest": "0x" + _digest(call).hex(),
                            "idempotency_key": str(uuid4()),
                            "wallet_operation_id": None,
                            "tx_hash": None,
                            "state": "unknown",
                            "retire_at": None,
                        }
                        self._operations[operation_ref] = row
                        # Before any possible remote submission. It leaves room
                        # for what every record may still gain, so recording
                        # what Circle reports, or a retirement mark, never fails
                        # for space.
                        saving = True
                        self._save(reserve=sum(map(_growth, self._operations.values())))
                        return row
                except Exception as error:
                    if not saving:
                        raise CircleNotSubmittedError("Circle was not asked to execute") from error
                    # Out of the journal this block leaves, so no later save
                    # of the block writes it.
                    self._operations.pop(operation_ref, None)
                    failure = error
            if saving:
                self._unsaved(operation_ref, row, failure)
            try:
                self._check()
            except Exception as error:
                raise CircleNotSubmittedError("Circle was not asked to execute") from error
            checked = True

    def _unsaved(self, operation_ref: str, row: dict[str, Any], failure: Exception) -> NoReturn:
        """After an intent save failed, perhaps once it replaced the journal
        (os.replace landed, then syncing the directory failed): the journal read
        again, without that intent, which was never sent. Only a journal shown
        free of the reference proves a definite NotSubmittedError."""
        try:
            with self._journal(self._timeout):
                kept = self._operations.get(operation_ref)
                if kept is not None and kept["idempotency_key"] == row["idempotency_key"]:
                    del self._operations[operation_ref]
                    self._save()
                cleared = operation_ref not in self._operations
        except Exception:
            cleared = False
        if cleared:
            raise CircleNotSubmittedError("Circle was not asked to execute") from failure
        raise CircleAgentWalletError(
            "Circle journal may still hold an intent that was never sent"
        ) from failure

    def execute(self, call: WalletCall, *, operation_ref: str) -> OperationResult:
        """Save the call's intent, its digest bound to the reference, before invoking
        execute; repeat calls only reconcile. The journal's lock is released while Circle
        executes, and only this reference's record changes when the result is recorded.
        An invalid reference, or a call refused once the journal shows no record of it,
        raises CircleNotSubmittedError; a journal not locked in time or read raises first."""
        try:
            # No journal can hold a reference that is empty or too long.
            _reference(operation_ref)
        except ValidationError as error:
            raise CircleNotSubmittedError("Circle was not asked to execute") from error
        with self._lock:
            row = self._save_intent(call, operation_ref)
            if row is None:
                return self._reconcile(operation_ref)
            direct = row["route"] == "direct_register"
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
                return self._record(operation_ref, row, result, from_execute=True)
            except Exception as error:
                # A process failure, timeout or lost result is never proof of no write.
                return self._result(operation_ref, row, unknown=True, cause=_cause(error))

    def reconcile(self, *, operation_ref: str) -> OperationResult:
        """Read the retained Circle transaction ID; never execute/sign/resubmit."""
        _reference(operation_ref)
        with self._lock:
            return self._reconcile(operation_ref)

    def _reconcile(self, operation_ref: str) -> OperationResult:
        """Under the in-process lock. The journal's lock is released while
        Circle's history is listed, and taken again to record what it shows.
        A reference looked up again may be presented again, so its retirement
        mark is withdrawn first: that only keeps the record longer."""
        with self._journal(self._timeout):
            retained = self._operations.get(operation_ref)
            if retained is not None and retained["retire_at"] is not None:
                retained["retire_at"] = None
                self._save()
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
                    return self._record(operation_ref, retained, found)
                if len(page) < 50:
                    break
            raise CircleAgentWalletError("Circle history does not list the original transaction")
        except Exception as error:
            return self._result(operation_ref, retained, unknown=True, cause=_cause(error))

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
        with self._lock:
            # The journal must still be this wallet's; its lock is not held
            # while Circle signs.
            with self._journal():
                pass
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
