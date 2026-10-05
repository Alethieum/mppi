"""Composition checks use real SDK adapters with bounded read-only RPC/service fixtures."""

from __future__ import annotations

import asyncio
import json
import threading
import traceback
from dataclasses import replace
from importlib.resources import files
from pathlib import Path

import mppi_provider.runtime as runtime_module
import pytest
from eth_abi import encode
from eth_utils import keccak
from mppi_protocol.chain import ChainReader, CodePin, HttpJsonRpc
from mppi_protocol.circle import CircleDeveloperWallet
from mppi_protocol.contract import ARC_USDC
from mppi_protocol.deployment import profile_from_mapping
from mppi_protocol.follower import ChainFollower
from mppi_protocol.signing import voucher_digest
from mppi_provider import ConfigError, Hooks, Mppi, SetupError

WALLET_ID = "11111111-1111-4111-8111-111111111111"
ADDRESS = bytes.fromhex("11" * 20)
API_KEY = "TEST_API_KEY:never-display-key"
ENTITY_SECRET = "ab" * 32
RPC_URL = "https://rpc.example/private-credential"
TOML = """
[provider]
name = "Test provider"
description = "Provider-owned execution"
public_origin = "https://provider.example"
recipient = "0x2222222222222222222222222222222222222222"
[identity]
provider_id = "42"
wallet_id = "11111111-1111-4111-8111-111111111111"
wallet_address = "0x1111111111111111111111111111111111111111"
[chain]
network = "testnet"
max_age_ms = 60000
rpc = "https://config-rpc.example"
[upstream]
url = "http://127.0.0.1:8080/v1"
api_key_env = "UPSTREAM_KEY"
[[models]]
model = "example/model"
description = "Test model"
context_window = 128000
max_output = 8192
modalities = ["text"]
embeddings = false
streaming = true
tools = true
structured_output = true
rates = {input = "300000", output = "1200000"}
[payment]
collection_margin_ms = 30000
authorization_wait_ms = 10000
[settlement]
interval_ms = 30000
[listen]
bind = "127.0.0.1:8443"
tls_mode = "ingress"
"""


def forbidden(*args, **kwargs):
    pytest.fail("setup must not sign, submit, execute, retain state or start a lifecycle")


class Boundary:
    def __init__(self, monkeypatch, tmp_path):
        self.config_path = tmp_path / "mppi.toml"
        self.config_path.write_text(TOML)
        self.original = self.config_path.read_bytes()
        self.block_hash = b"b" * 32
        self.chain_id = 5042002
        self.calls = []
        self.wallet_calls = []
        self.threads = []
        self.endpoints = []
        self.wallet = {
            "id": WALLET_ID,
            "address": "0x" + ADDRESS.hex(),
            "blockchain": "ARC-TESTNET",
            "custodyType": "DEVELOPER",
            "accountType": "SCA",
            "state": "LIVE",
        }
        self.usdc = {
            "name()": ("string", "USDC"),
            "version()": ("string", "2"),
            "decimals()": ("uint8", 6),
        }
        self.separator = None
        self.malformed = None
        self.reorg = False
        published = profile_from_mapping(
            json.loads(files("mppi_provider").joinpath("arc-testnet.json").read_bytes())
        )
        self.code = {
            pin.address: bytes([index + 1])
            for index, pin in enumerate(
                (
                    published.proxy,
                    published.implementation,
                    published.registry,
                    published.registry_implementation,
                )
            )
        }
        self.profile = replace(
            published,
            **{
                name: CodePin(
                    getattr(published, name).address,
                    keccak(self.code[getattr(published, name).address]),
                )
                for name in ("proxy", "implementation", "registry", "registry_implementation")
            },
        )
        owner = self

        def circle_request(adapter, method, path, *, body=None):
            owner.threads.append(threading.get_ident())
            owner.wallet_calls.append((method, path, body))
            assert method == "GET" and path == "/wallets/" + WALLET_ID and body is None
            assert adapter._api_key == API_KEY
            assert adapter._entity_secret == bytes.fromhex(ENTITY_SECRET)
            assert adapter._timeout_ms == 30_000
            return {"wallet": dict(owner.wallet)}

        def rpc_call(adapter, method, params):
            owner.endpoints.append(adapter.endpoint)
            owner.threads.append(threading.get_ident())
            return owner.call(method, params)

        monkeypatch.setattr(CircleDeveloperWallet, "_request", circle_request)
        monkeypatch.setattr(HttpJsonRpc, "call", rpc_call)
        monkeypatch.setattr(CircleDeveloperWallet, "execute", forbidden)
        monkeypatch.setattr(CircleDeveloperWallet, "reconcile", forbidden)
        monkeypatch.setattr(CircleDeveloperWallet, "sign_funding_authorization", forbidden)
        monkeypatch.setattr(ChainFollower, "start", forbidden)
        monkeypatch.setenv("CIRCLE_API_KEY", API_KEY)
        monkeypatch.setenv("CIRCLE_ENTITY_SECRET", ENTITY_SECRET)
        monkeypatch.setenv("CUSTOM_RPC", RPC_URL)
        monkeypatch.delenv("UPSTREAM_KEY", raising=False)

    def call(self, method, params):
        self.calls.append((method, params))
        if method == "eth_chainId":
            return hex(self.chain_id)
        if method == "eth_getBlockByNumber":
            assert params[1] is False
            return {
                "number": "0x64",
                "hash": "0x"
                + ((b"x" * 32) if self.reorg and params[0] != "latest" else self.block_hash).hex(),
                "timestamp": "0x6aaaaaaa",
                "parentHash": "0x" + (b"p" * 32).hex(),
            }
        assert params[-1] == {"blockHash": "0x" + self.block_hash.hex(), "requireCanonical": True}
        if method == "eth_getCode":
            return "0x" + self.code[bytes.fromhex(params[0][2:])].hex()
        if method == "eth_getStorageAt":
            assert params[1] == "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
            implementation = (
                self.profile.implementation
                if params[0].lower() == "0x" + self.profile.proxy.address.hex()
                else self.profile.registry_implementation
            )
            return "0x" + (bytes(12) + implementation.address).hex()
        assert method == "eth_call"
        call = params[0]
        selector = bytes.fromhex(call["data"][2:10])
        if call["to"].lower() == "0x" + self.profile.proxy.address.hex():
            assert selector == keccak(text="voucherDigest(bytes32,uint64)")[:4]
            return "0x" + voucher_digest(self.profile.domain, bytes(32), 0).hex()
        assert call["to"].lower() == "0x" + ARC_USDC.hex()
        for function, (kind, value) in self.usdc.items():
            if selector == keccak(text=function)[:4]:
                raw = encode([kind], [value])
                return "0x" + (raw + bytes(32) if self.malformed == function else raw).hex()
        assert selector == keccak(text="DOMAIN_SEPARATOR()")[:4]
        separator = keccak(
            encode(
                ["bytes32", "bytes32", "bytes32", "uint256", "address"],
                [
                    keccak(
                        text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
                    ),
                    keccak(text="USDC"),
                    keccak(text="2"),
                    self.chain_id,
                    ARC_USDC,
                ],
            )
        )
        return "0x" + (separator if self.separator is None else self.separator).hex()

    async def create(self, **options):
        arguments = dict(
            testnet=True,
            profile=self.profile,
            rpc_env="CUSTOM_RPC",
            coverage=forbidden,
            execution=forbidden,
        )
        arguments.update(options)
        return await Mppi.from_config(self.config_path, **arguments)

    def unchanged(self):
        assert self.config_path.read_bytes() == self.original
        assert list(self.config_path.parent.iterdir()) == [self.config_path]


@pytest.fixture
def boundary(monkeypatch, tmp_path):
    return Boundary(monkeypatch, tmp_path)


def test_composition_verifies_adapters_preserves_callbacks_and_starts_no_lifecycle(
    boundary, monkeypatch
):
    hooks = Hooks(on_meter=forbidden, on_error=forbidden)
    original_init = Mppi.__init__
    original_follower = runtime_module.ChainFollower

    async def scenario():
        loop = asyncio.get_running_loop()
        owner = threading.get_ident()
        tasks = asyncio.all_tasks()

        def init(self, *args, **kwargs):
            assert threading.get_ident() == owner and asyncio.get_running_loop() is loop
            assert kwargs["hooks"] is hooks
            return original_init(self, *args, **kwargs)

        def follower(*args, **kwargs):
            assert threading.get_ident() == owner and asyncio.get_running_loop() is loop
            return original_follower(*args, **kwargs)

        monkeypatch.setattr(Mppi, "__init__", init)
        monkeypatch.setattr(runtime_module, "ChainFollower", follower)
        result = await boundary.create(hooks=hooks, control_auth_timeout=19)
        assert result.coverage is forbidden and result.execution is forbidden
        assert result._control_auth_timeout == 19
        assert isinstance(result.wallet, CircleDeveloperWallet)
        assert result.wallet.address == ADDRESS
        assert result.contract.domain == boundary.profile.domain
        assert result.chain.reader.profile == boundary.profile
        assert result.chain.max_age_ms == 60000 and result.chain.collection_margin_ms == 30000
        assert result.config.identity.provider_id == 42
        assert result.config.listen is not None
        assert result._maintenance is None and not result._tasks and not result._started
        assert result.chain._task is None and not result.chain._running
        assert not result._observers._started and result.hook_stats.delivered == 0
        assert result._sessions == {} and result.chain.current is None
        assert asyncio.all_tasks() == tasks
        assert all(thread != owner for thread in boundary.threads)
        assert boundary.wallet_calls == [("GET", "/wallets/" + WALLET_ID, None)]
        assert set(boundary.endpoints) == {RPC_URL}
        assert {call[0] for call in boundary.calls} <= {
            "eth_chainId",
            "eth_getBlockByNumber",
            "eth_getCode",
            "eth_getStorageAt",
            "eth_call",
        }
        for secret in (API_KEY, ENTITY_SECRET, RPC_URL):
            assert secret not in repr(result) + repr(result.wallet) + repr(result.config) + repr(
                result.chain.reader.rpc
            )

    asyncio.run(scenario())
    boundary.unchanged()


def test_packaged_profile_matches_published_and_is_selected_only_explicitly(boundary, monkeypatch):
    published = Path(__file__).resolve().parents[4] / "deployments/arc-testnet.json"
    assert (
        files("mppi_provider").joinpath("arc-testnet.json").read_bytes() == published.read_bytes()
    )
    expected = profile_from_mapping(json.loads(published.read_bytes()))
    checked = []
    monkeypatch.setattr(
        ChainReader, "verify_deployment", lambda reader, block: checked.append(reader.profile)
    )
    result = asyncio.run(boundary.create(profile=None))
    assert result.chain.reader.profile == expected and checked == [expected]
    with pytest.raises(SetupError, match="selected network"):
        asyncio.run(boundary.create(profile=None, testnet=False))
    boundary.unchanged()


@pytest.mark.parametrize(
    "change",
    [
        "implicit-testnet",
        "mainnet-profile",
        "untyped-profile",
        "untyped-selection",
        "missing-mainnet-profile",
    ],
)
def test_invalid_profile_selection_fails_before_remote_reads(boundary, change):
    options = {}
    if change == "implicit-testnet":
        options["testnet"] = False
    elif change == "mainnet-profile":
        options["profile"] = replace(
            boundary.profile, domain=replace(boundary.profile.domain, chain_id=5042)
        )
    elif change == "untyped-profile":
        options["profile"] = {"chain_id": 5042002}
    elif change == "untyped-selection":
        options["testnet"] = 1
    else:
        boundary.config_path.write_text(TOML.replace('network = "testnet"', 'network = "mainnet"'))
        options.update(testnet=False, profile=None)
    with pytest.raises(SetupError):
        asyncio.run(boundary.create(**options))
    assert not boundary.calls and not boundary.wallet_calls


@pytest.mark.parametrize(
    "rpc_env,endpoint", [(None, "https://config-rpc.example"), ("CUSTOM_RPC", RPC_URL)]
)
def test_rpc_selection_preserves_config_with_explicit_environment_override(
    boundary, rpc_env, endpoint
):
    asyncio.run(boundary.create(rpc_env=rpc_env))
    assert set(boundary.endpoints) == {endpoint}
    boundary.unchanged()


def test_explicit_trusted_mainnet_profile_retains_advanced_composition(boundary):
    boundary.config_path.write_text(TOML.replace('network = "testnet"', 'network = "mainnet"'))
    boundary.profile = replace(
        boundary.profile, domain=replace(boundary.profile.domain, chain_id=5042)
    )
    boundary.chain_id = 5042
    boundary.wallet["blockchain"] = "ARC"
    result = asyncio.run(boundary.create(testnet=False))
    assert result.contract.domain == boundary.profile.domain
    assert result.config.chain.network == "mainnet"
    assert not result.chain._running and not result._started


@pytest.mark.parametrize(
    "variable,value",
    [
        ("CIRCLE_API_KEY", None),
        ("CIRCLE_API_KEY", ""),
        ("CIRCLE_API_KEY", "secret\nkey"),
        ("CIRCLE_ENTITY_SECRET", None),
        ("CIRCLE_ENTITY_SECRET", "ab" * 31),
        ("CIRCLE_ENTITY_SECRET", "0x" + ENTITY_SECRET),
        ("CIRCLE_ENTITY_SECRET", "zz" * 32),
        ("CIRCLE_ENTITY_SECRET", " " + ENTITY_SECRET),
        ("CUSTOM_RPC", None),
        ("CUSTOM_RPC", ""),
    ],
)
def test_environment_errors_are_sanitized_and_precede_remote_reads(
    boundary, monkeypatch, variable, value
):
    if value is None:
        monkeypatch.delenv(variable)
    else:
        monkeypatch.setenv(variable, value)
    with pytest.raises(SetupError) as error:
        asyncio.run(boundary.create())
    rendered = "".join(traceback.format_exception(error.value))
    assert API_KEY not in rendered and ENTITY_SECRET not in rendered and RPC_URL not in rendered
    assert not boundary.calls and not boundary.wallet_calls
    boundary.unchanged()


@pytest.mark.parametrize("rpc_env", ["", "NOT-AN-ENV", 1, "RPC\nSECRET"])
def test_invalid_environment_reference_never_becomes_an_endpoint(boundary, rpc_env):
    with pytest.raises(SetupError, match="environment variable"):
        asyncio.run(boundary.create(rpc_env=rpc_env))
    assert not boundary.calls and not boundary.wallet_calls


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", "different"),
        ("address", "0x" + "22" * 20),
        ("address", "secret-invalid-address"),
        ("blockchain", "ARC"),
        ("accountType", "EOA"),
        ("custodyType", "ENDUSER"),
        ("state", "FROZEN"),
    ],
)
def test_circle_wallet_mismatch_stops_setup(boundary, field, value):
    boundary.wallet[field] = value
    with pytest.raises(SetupError, match="Circle wallet verification failed") as error:
        asyncio.run(boundary.create())
    assert value not in str(error.value)
    assert not boundary.calls
    boundary.unchanged()


def test_missing_confirmed_wallet_id_cannot_create_a_replacement(boundary):
    boundary.config_path.write_text(TOML.replace(f'wallet_id = "{WALLET_ID}"\n', ""))
    with pytest.raises(SetupError, match="wallet id"):
        asyncio.run(boundary.create())
    assert not boundary.calls and not boundary.wallet_calls


@pytest.mark.parametrize(
    "failure",
    [
        "name",
        "version",
        "decimals",
        "separator",
        "short-separator",
        "surplus-abi",
        "chain",
        "code",
        "canonical",
    ],
)
def test_chain_or_token_domain_failure_returns_no_runtime(boundary, failure, monkeypatch):
    if failure in ("name", "version", "decimals"):
        boundary.usdc[failure + "()"] = {
            "name": ("string", "Other Token"),
            "version": ("string", "3"),
            "decimals": ("uint8", 18),
        }[failure]
    elif failure in ("separator", "short-separator"):
        boundary.separator = b"x" * (32 if failure == "separator" else 31)
    elif failure == "surplus-abi":
        boundary.malformed = "name()"
    elif failure == "chain":
        boundary.chain_id = 5042
    elif failure == "code":
        boundary.code[boundary.profile.proxy.address] = b"other code"
    else:
        boundary.reorg = True
    monkeypatch.setattr(Mppi, "__init__", forbidden)
    with pytest.raises(SetupError):
        asyncio.run(boundary.create())
    boundary.unchanged()


@pytest.mark.parametrize("remote", ["circle", "rpc"])
def test_remote_errors_never_expose_credentials_or_endpoint(boundary, monkeypatch, remote):
    secret_message = API_KEY + ENTITY_SECRET + RPC_URL

    def fail(*args, **kwargs):
        raise RuntimeError(secret_message)

    monkeypatch.setattr(
        CircleDeveloperWallet if remote == "circle" else HttpJsonRpc,
        "_request" if remote == "circle" else "call",
        fail,
    )
    with pytest.raises(SetupError) as error:
        asyncio.run(boundary.create())
    rendered = "".join(traceback.format_exception(error.value))
    assert API_KEY not in rendered and ENTITY_SECRET not in rendered and RPC_URL not in rendered
    boundary.unchanged()


def test_malformed_config_is_sanitized_before_remote_reads(boundary):
    boundary.config_path.write_text('name = "' + API_KEY)
    with pytest.raises(ConfigError) as error:
        asyncio.run(boundary.create())
    assert API_KEY not in str(error.value)
    assert not boundary.calls and not boundary.wallet_calls


def test_cancelled_setup_never_constructs_or_starts_runtime(boundary, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    verify = CircleDeveloperWallet.verify_wallet

    def delayed(wallet):
        entered.set()
        assert release.wait(5)
        return verify(wallet)

    monkeypatch.setattr(CircleDeveloperWallet, "verify_wallet", delayed)
    monkeypatch.setattr(Mppi, "__init__", forbidden)

    async def scenario():
        task = asyncio.create_task(boundary.create())
        try:
            assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()

    asyncio.run(scenario())
    boundary.unchanged()
