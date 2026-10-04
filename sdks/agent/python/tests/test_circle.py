"""Circle CLI adapter boundaries; no credentials, actual CLI or network calls.

Subprocess limit checks run a tiny local Python executable in the Apple test
container. Independent CLI fixtures use the published 1.1.4 output schema.
"""

import copy
import json
import os
import subprocess
import sys
from dataclasses import replace
from uuid import uuid4

import pytest
from eth_abi import encode
from eth_account.messages import encode_typed_data
from eth_utils import keccak
from mppi_agent.checkpoint import Checkpoint
from mppi_agent.circle import CircleAgentWallet, CircleAgentWalletError
from mppi_protocol.capabilities import FundingAuthorization, WalletCall
from mppi_protocol.chain import ChainReader, CodePin, DeploymentProfile
from mppi_protocol.errors import ValidationError
from mppi_protocol.signing import voucher_digest
from mppi_protocol.types import Domain, OperationState

CHAIN = 5042002
PROXY = b"\x99" * 20
IMPLEMENTATION = b"\x98" * 20
REGISTRY = b"\x97" * 20
REGISTRY_IMPLEMENTATION = b"\x96" * 20
WALLET = b"\x11" * 20
WALLET_IMPLEMENTATION = b"\x12" * 20
TOKEN = bytes.fromhex("3600000000000000000000000000000000000000")
HASH = b"\xbb" * 32
TX = b"\xaa" * 32
ID = "11111111-2222-4333-8444-555555555555"
DOMAIN = Domain(CHAIN, PROXY)
CALL = WalletCall(CHAIN, PROXY, b"\x01\x23\x45\x67")


def hx(value):
    return "0x" + value.hex()


class Rpc:
    def __init__(self):
        self.calls = []
        self.code = {
            address: bytes([i]) + b"runtime"
            for i, address in enumerate(
                (
                    PROXY,
                    IMPLEMENTATION,
                    REGISTRY,
                    REGISTRY_IMPLEMENTATION,
                    WALLET,
                    WALLET_IMPLEMENTATION,
                ),
                start=1,
            )
        }
        self.wallet_implementation = WALLET_IMPLEMENTATION
        self.chain = CHAIN

    def call(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        if method == "eth_chainId":
            return hex(self.chain)
        if method == "eth_getBlockByNumber":
            return {
                "number": "0x64",
                "hash": hx(HASH),
                "parentHash": hx(b"\xbc" * 32),
                "timestamp": "0x1234",
            }
        assert params[-1] == {"blockHash": hx(HASH), "requireCanonical": True}
        if method == "eth_getCode":
            return hx(self.code[bytes.fromhex(params[0][2:])])
        if method == "eth_getStorageAt":
            address = bytes.fromhex(params[0][2:])
            target = {
                PROXY: IMPLEMENTATION,
                REGISTRY: REGISTRY_IMPLEMENTATION,
                WALLET: self.wallet_implementation,
            }[address]
            return hx(bytes(12) + target)
        if method == "eth_call":
            assert params[0] == {
                "to": hx(PROXY),
                "data": hx(
                    keccak(text="voucherDigest(bytes32,uint64)")[:4]
                    + encode(["bytes32", "uint64"], [bytes(32), 0])
                ),
            }
            return hx(encode(["bytes32"], [voucher_digest(DOMAIN, bytes(32), 0)]))
        raise AssertionError(method)


class Cli:
    def __init__(self, path):
        self.path = path
        self.calls = []
        self.version = b"1.1.4\n"
        self.wallets = [{"type": "agent", "address": hx(WALLET), "blockchain": "ARC-TESTNET"}]
        self.execute_error = False
        self.reply_changes = {}
        self.pages = [[]]
        self.signing_response = "0x" + "11" * 140
        self.signing_data = None
        self.before_submit = None

    def transaction(self, **changes):
        return {
            "id": ID,
            "state": "CONFIRMED",
            "blockchain": "ARC-TESTNET",
            "txHash": hx(TX),
            "sourceAddress": hx(WALLET),
            "destinationAddress": hx(WALLET),
            "operation": "CONTRACT_EXECUTION",
            "transactionType": "OUTBOUND",
            "abiFunctionSignature": "execute(address,uint256,bytes)",
            "abiParameters": [hx(PROXY), "0", hx(CALL.data)],
            "contractAddress": hx(WALLET),
            "createDate": "2026-09-27T00:00:00Z",
            **changes,
        }

    def __call__(self, args):
        self.calls.append(copy.deepcopy(args))
        if args == ["--version"]:
            return self.version
        assert args[-2:] == ["--output", "json"]
        args = args[:-2]
        if args[:2] == ["wallet", "list"]:
            assert args == ["wallet", "list", "--type", "agent", "--chain", "ARC-TESTNET"]
            data = {"wallets": self.wallets}
        elif args[:2] == ["wallet", "execute"]:
            # Real journal bytes must already exist before the command starts.
            record = json.loads(self.path.read_text())
            row = record["operations"]["original-ref"]
            expected = [
                "wallet",
                "execute",
                "execute(address,uint256,bytes)",
                hx(PROXY),
                "0",
                hx(CALL.data),
                "--contract",
                hx(WALLET),
                "--address",
                hx(WALLET),
                "--chain",
                "ARC-TESTNET",
                "--idempotency-key",
                row["idempotency_key"],
            ]
            assert args == expected
            assert row["wallet_operation_id"] is None and row["state"] == "unknown"
            if self.before_submit:
                self.before_submit(row)
            if self.execute_error:
                raise CircleAgentWalletError("mock timeout")
            data = self.transaction(idempotencyKey=row["idempotency_key"], **self.reply_changes)
        elif args[:2] == ["transaction", "list"]:
            expected = [
                "transaction",
                "list",
                "--address",
                hx(WALLET),
                "--chain",
                "ARC-TESTNET",
                "--operation",
                "execute",
                "--tx-type",
                "outbound",
                "--limit",
                "50",
            ]
            assert args[: len(expected)] == expected
            cursor = args[-1] if "--cursor" in args else None
            if cursor is None:
                data = {"transactions": self.pages[0]}
            else:
                prior = next(
                    i for i, page in enumerate(self.pages) if page and page[-1]["id"] == cursor
                )
                data = {"transactions": self.pages[prior + 1]}
        elif args[:3] == ["wallet", "sign", "typed-data"]:
            assert args[4:] == ["--address", hx(WALLET), "--chain", "ARC-TESTNET"]
            self.signing_data = json.loads(args[3])
            data = {"signature": self.signing_response}
        else:
            raise AssertionError(args)
        return json.dumps({"data": data}).encode()


@pytest.fixture
def context(tmp_path, monkeypatch):
    rpc = Rpc()
    pins = [
        CodePin(address, keccak(rpc.code[address]))
        for address in (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION)
    ]
    chain = ChainReader(rpc, DeploymentProfile(DOMAIN, REGISTRY, *pins))
    path = tmp_path / "wallet.json"
    cli = Cli(path)
    options = {
        "chain": chain,
        "wallet": CodePin(WALLET, keccak(rpc.code[WALLET])),
        "implementation": CodePin(WALLET_IMPLEMENTATION, keccak(rpc.code[WALLET_IMPLEMENTATION])),
        "checkpoint_path": path,
    }
    wallet = CircleAgentWallet(**options)
    monkeypatch.setattr(wallet, "_run", cli)
    return wallet, rpc, cli, options


def test_exact_call_is_journaled_before_single_submit(context):
    wallet, _, cli, _ = context
    result = wallet.execute(CALL, operation_ref="original-ref")
    assert result.state is OperationState.PENDING  # Canonical chain confirmation remains separate.
    assert result.tx_hash == TX and result.wallet_operation_id == ID
    cli.pages = [[cli.transaction()]]
    repeated = wallet.execute(CALL, operation_ref="original-ref")
    assert repeated == result
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 1
    with pytest.raises(ValidationError, match="reused"):
        wallet.execute(replace(CALL, data=b"different"), operation_ref="original-ref")


def test_restart_recovers_original_resource_with_reads_only(context, monkeypatch):
    wallet, _, cli, options = context
    original = wallet.execute(CALL, operation_ref="original-ref")
    cli.calls.clear()
    cli.pages = [[cli.transaction(state="COMPLETE")]]
    reopened = CircleAgentWallet(**options)
    monkeypatch.setattr(reopened, "_run", cli)
    assert reopened.execute(CALL, operation_ref="original-ref") == original
    assert all(args[:2] == ["transaction", "list"] for args in cli.calls)


def test_no_returned_handle_remains_unknown_even_with_matching_new_transactions(
    context, monkeypatch
):
    wallet, _, cli, options = context
    cli.execute_error = True
    result = wallet.execute(CALL, operation_ref="original-ref")
    assert result.state is OperationState.UNKNOWN and result.tx_hash is None
    key = json.loads(cli.path.read_text())["operations"]["original-ref"]["idempotency_key"]
    cli.calls.clear()
    cli.execute_error = False
    cli.pages = [[cli.transaction()]]
    reopened = CircleAgentWallet(**options)
    monkeypatch.setattr(reopened, "_run", cli)
    assert reopened.execute(CALL, operation_ref="original-ref") == result
    assert reopened.reconcile(operation_ref="original-ref") == result
    assert cli.calls == []
    assert json.loads(cli.path.read_text())["operations"]["original-ref"]["idempotency_key"] == key


@pytest.mark.parametrize(
    "changes",
    [
        {"sourceAddress": hx(PROXY)},
        {"blockchain": "ARC"},
        {"contractAddress": hx(PROXY)},
        {"destinationAddress": hx(PROXY)},
        {"operation": "TRANSFER"},
        {"transactionType": "INBOUND"},
        {"abiFunctionSignature": "other(address,uint256,bytes)"},
        {"abiParameters": [hx(PROXY), "1", hx(CALL.data)]},
        {"abiParameters": [hx(REGISTRY), "0", hx(CALL.data)]},
        {"abiParameters": [hx(PROXY), "0", "0x1234"]},
        {"abiParameters": [hx(PROXY), 0, hx(CALL.data)]},
        {"state": "INVENTED"},
        {"idempotencyKey": "wrong"},
    ],
)
def test_execute_response_must_bind_exact_intent(context, changes):
    wallet, _, cli, _ = context
    cli.reply_changes = changes
    result = wallet.execute(CALL, operation_ref="original-ref")
    assert result.state is OperationState.UNKNOWN
    assert result.wallet_operation_id is None and result.tx_hash is None


@pytest.mark.parametrize(
    "problem", ["missing", "foreign_hash", "wrong_call", "duplicate", "malformed"]
)
def test_lookup_outage_and_conflicts_preserve_handles(context, problem):
    wallet, _, cli, _ = context
    wallet.execute(CALL, operation_ref="original-ref")
    if problem == "missing":
        cli.pages = [[]]
    elif problem == "foreign_hash":
        cli.pages = [[cli.transaction(txHash=hx(b"\xdd" * 32))]]
    elif problem == "wrong_call":
        cli.pages = [[cli.transaction(abiParameters=[hx(PROXY), "0", "0x00"])]]
    elif problem == "duplicate":
        cli.pages = [[cli.transaction(), cli.transaction()]]
    else:
        cli.pages = [[{"id": ID}]]
    result = wallet.reconcile(operation_ref="original-ref")
    assert result.state is OperationState.UNKNOWN
    assert result.wallet_operation_id == ID and result.tx_hash == TX


def test_paginated_known_id_and_no_latest_match_heuristic(context):
    wallet, _, cli, _ = context
    wallet.execute(CALL, operation_ref="original-ref")
    first = [cli.transaction(id=str(uuid4())) for _ in range(50)]
    cli.pages = [first, [cli.transaction(state="FAILED")]]
    result = wallet.reconcile(operation_ref="original-ref")
    assert result.state is OperationState.FAILED and result.wallet_operation_id == ID
    assert cli.calls[-1][-4:] == ["--cursor", first[-1]["id"], "--output", "json"]


def test_page_limit_fails_closed(context):
    wallet, _, cli, _ = context
    wallet.execute(CALL, operation_ref="original-ref")
    wallet._max_pages = 1
    cli.pages = [[cli.transaction(id=str(uuid4())) for _ in range(50)], [cli.transaction()]]
    assert wallet.reconcile(operation_ref="original-ref").state is OperationState.UNKNOWN


@pytest.mark.parametrize(
    "problem",
    [
        "version",
        "account",
        "network",
        "duplicate",
        "code",
        "implementation",
        "chain",
        "malformed_address",
    ],
)
def test_selection_and_pins_rechecked_before_any_write(context, problem):
    wallet, rpc, cli, _ = context
    if problem == "version":
        cli.version = b"1.1.5\n"
    elif problem == "account":
        cli.wallets[0]["type"] = "local"
    elif problem == "network":
        cli.wallets[0]["blockchain"] = "ARC"
    elif problem == "duplicate":
        cli.wallets *= 2
    elif problem == "code":
        rpc.code[WALLET] = b"changed"
    elif problem == "implementation":
        rpc.wallet_implementation = PROXY
    elif problem == "chain":
        rpc.chain = 5042
    else:
        cli.wallets[0]["address"] = {}
    with pytest.raises((CircleAgentWalletError, RuntimeError)):
        wallet.execute(CALL, operation_ref="original-ref")
    assert not cli.path.exists()
    assert not any(args[:2] == ["wallet", "execute"] for args in cli.calls)


def test_journal_failure_prevents_submission(context, monkeypatch):
    wallet, _, cli, _ = context

    def fail(_self, _):
        raise OSError("storage failure")

    monkeypatch.setattr(Checkpoint, "save", fail)
    with pytest.raises(OSError):
        wallet.execute(CALL, operation_ref="original-ref")
    assert not any(args[:2] == ["wallet", "execute"] for args in cli.calls)


def test_funding_signature_bytes_preserved_and_typed_data_independent(context):
    wallet, _, cli, _ = context
    auth = FundingAuthorization(
        CHAIN, TOKEN, "USDC", "2", WALLET, PROXY, 5_510_000, 10, 20, b"\x33" * 32
    )
    signature = wallet.sign_funding_authorization(auth)
    assert signature == b"\x11" * 140  # Not an ECDSA 65-byte SessionSigner proof.
    message = encode_typed_data(full_message=cli.signing_data)
    expected = keccak(
        encode(
            ["bytes32", "address", "address", "uint256", "uint256", "uint256", "bytes32"],
            [
                keccak(
                    text="ReceiveWithAuthorization(address from,address to,uint256 value,uint256 validAfter,uint256 validBefore,bytes32 nonce)"
                ),
                WALLET,
                PROXY,
                5_510_000,
                10,
                20,
                b"\x33" * 32,
            ],
        )
    )
    assert message.body == expected
    assert not cli.path.exists()


@pytest.mark.parametrize("field,value", [("chain_id", 5042), ("owner", PROXY), ("token", PROXY)])
def test_signing_rejects_wrong_funding_authority_before_cli(context, field, value):
    wallet, _, cli, _ = context
    auth = FundingAuthorization(CHAIN, TOKEN, "USDC", "2", WALLET, PROXY, 1, 10, 20, b"\x33" * 32)
    with pytest.raises(ValidationError):
        wallet.sign_funding_authorization(replace(auth, **{field: value}))
    assert cli.calls == []


@pytest.mark.parametrize("signature", ["0x", "0xzz", "0x11 22", "secret", "0x" + "11" * 65537])
def test_bad_signing_responses_rejected(context, signature):
    wallet, _, cli, _ = context
    cli.signing_response = signature
    auth = FundingAuthorization(CHAIN, TOKEN, "USDC", "2", WALLET, PROXY, 1, 10, 20, b"\x33" * 32)
    with pytest.raises(CircleAgentWalletError):
        wallet.sign_funding_authorization(auth)


def test_restart_rejects_changed_profile_and_tampered_journal(context):
    wallet, _, cli, options = context
    wallet.execute(CALL, operation_ref="original-ref")
    with pytest.raises(CircleAgentWalletError):
        CircleAgentWallet(
            **(options | {"implementation": CodePin(WALLET_IMPLEMENTATION, b"\x01" * 32)})
        )
    record = json.loads(cli.path.read_text())
    record["operations"]["original-ref"]["call"]["chain_id"] = 5042
    cli.path.write_text(json.dumps(record))
    with pytest.raises(CircleAgentWalletError):
        CircleAgentWallet(**options)


@pytest.mark.parametrize(
    "script,expected",
    [
        ("import sys; sys.stdout.write('x' * 1048577)", "output limit"),
        ("import sys; sys.stderr.write('private-material-' * 6000)", "error-output limit"),
        ("import time; time.sleep(5)", "timed out"),
        ("import sys; sys.stderr.write('private-material'); sys.exit(7)", "success"),
    ],
)
def test_actual_subprocess_bounds_and_redacted_errors(context, script, expected):
    _, _, _, options = context
    bounded = CircleAgentWallet(
        **(options | {"command": (sys.executable, "-c", script), "timeout_seconds": 0.3})
    )
    with pytest.raises(CircleAgentWalletError, match=expected) as error:
        bounded._run([])
    assert "private-material" not in str(error.value)


def test_actual_subprocess_arguments_never_evaluated_by_shell(context):
    _, _, _, options = context
    direct = CircleAgentWallet(
        **(
            options
            | {
                "command": (
                    sys.executable,
                    "-c",
                    "import json,sys; print(json.dumps(sys.argv[1:]))",
                )
            }
        )
    )
    arguments = ["$(touch /tmp/never-run)", "`echo private`", "; echo bad"]
    assert json.loads(direct._run(arguments)) == arguments


def test_scoped_journal_lock_is_released_between_operations(context):
    wallet, _, cli, options = context
    held = Checkpoint(options["checkpoint_path"])
    try:
        with pytest.raises(ValidationError):
            wallet.execute(CALL, operation_ref="original-ref")
    finally:
        held.close()
    assert cli.calls == []
    assert wallet.execute(CALL, operation_ref="original-ref").wallet_operation_id == ID
    next_owner = Checkpoint(options["checkpoint_path"])
    next_owner.close()


@pytest.mark.parametrize(
    "script,needs_kill",
    [
        ("print('done')", False),
        ("import sys; sys.exit(7)", False),
        ("import time; time.sleep(5)", True),
        (
            "import sys,time; sys.stdout.write('x' * 1048577); sys.stdout.flush(); time.sleep(5)",
            True,
        ),
    ],
)
def test_process_cleanup_signals_before_reap_only_on_live_failure(
    context, monkeypatch, script, needs_kill
):
    _, _, _, options = context
    events = []
    killpg, wait = os.killpg, subprocess.Popen.wait

    def kill(pid, sig):
        events.append("kill")
        return killpg(pid, sig)

    def waited(process, *args, **kwargs):
        events.append("wait")
        return wait(process, *args, **kwargs)

    monkeypatch.setattr(os, "killpg", kill)
    monkeypatch.setattr(subprocess.Popen, "wait", waited)
    process = CircleAgentWallet(
        **(options | {"command": (sys.executable, "-c", script), "timeout_seconds": 0.3})
    )
    try:
        process._run([])
    except CircleAgentWalletError:
        pass
    if needs_kill:
        assert events[:2] == ["kill", "wait"]
    else:
        assert "kill" not in events and "wait" in events
