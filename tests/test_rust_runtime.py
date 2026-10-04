"""Independent Python agent -> native Rust provider -> local streamed upstream.

The caller-owned fixture supplies token counts and definitive job reports. It
uses public SDK interfaces and an owned Anvil process; no provider state is
injected into the agent and no Python provider SDK is imported by this harness.
All certificates, checkpoints and outputs live in pytest's temporary directory.

Primary APIs checked for this fixture:
https://docs.python.org/3.13/library/asyncio-stream.html#asyncio.start_server
https://docs.python.org/3.13/library/ssl.html#ssl.SSLContext.set_alpn_protocols
https://docs.openssl.org/3.0/man1/openssl-req/
https://grpc.github.io/grpc/python/grpc.html#grpc.secure_channel
https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
Foundry v1.8.3 crates/anvil/src/eth/api.rs:304 and core/src/eth/mod.rs:543
document interval mining used to keep the independently observed head fresh.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import socket
import ssl
import subprocess
import time
from pathlib import Path

import grpc
import pytest
from eth_abi import decode
from eth_utils import keccak
from mppi.v1 import mppi_pb2 as pb
from mppi.v1 import mppi_pb2_grpc as services
from mppi_agent import Client, PaymentError
from mppi_protocol.chain import ChainReader, HttpJsonRpc
from mppi_protocol.contract import ContractCalls
from mppi_protocol.deployment import profile_from_mapping
from mppi_protocol.follower import ChainFollower
from mppi_protocol.signing import LocalSessionSigner
from mppi_protocol.types import OperationState, PaymentCode, RunContext, SessionStatus
from mppi_protocol.wire import GRPC_OPTIONS

SPEC = importlib.util.spec_from_file_location(
    "mppi_runtime_owned_anvil", Path(__file__).with_name("test_chain_anvil.py")
)
assert SPEC is not None and SPEC.loader is not None
OWNED = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OWNED)
node = OWNED.node
Artifact, hx, raw = OWNED.Artifact, OWNED.hx, OWNED.raw
CHAIN_ID, USDC = OWNED.CHAIN_ID, OWNED.USDC

COMMON_SPEC = importlib.util.spec_from_file_location(
    "mppi_runtime_wallet", Path(__file__).parent / "paid_loop/common.py"
)
assert COMMON_SPEC is not None and COMMON_SPEC.loader is not None
COMMON = importlib.util.module_from_spec(COMMON_SPEC)
COMMON_SPEC.loader.exec_module(COMMON)
RUST = Path(__file__).resolve().parents[1] / "sdks/provider/rust"
MODEL, CATALOG = COMMON.MODEL, COMMON.CATALOG

# Whitespace and Unicode make reserialization observable in the upstream log.
REQUEST = (
    b'{ "model": "fixture/model", "stream": true, '
    b'"messages": [{"role":"user", "content":"fixture \\u03bb"}], '
    b'"stream_options": {"include_usage":true} }'
)
CHUNKS = (
    b'{"id":"fixture","choices":[{"delta":{"content":"paid "}}]}',
    b'{"id":"fixture","choices":[{"delta":{"content":"response"}}]}',
    b'{"choices":[],"usage":{"prompt_tokens":4510,"completion_tokens":910,'
    b'"total_tokens":5420,"prompt_tokens_details":{"cached_tokens":0}}}',
)


def tls_context(directory: Path) -> ssl.SSLContext:
    """Temporary CA plus separate server leaf; clients still verify hostname."""

    def openssl(*arguments: str) -> None:
        subprocess.run(
            ["openssl", *arguments], cwd=directory, check=True, capture_output=True, timeout=15
        )

    openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        "ca.key",
        "-out",
        "ca.pem",
        "-days",
        "1",
        "-subj",
        "/CN=MPPI runtime fixture CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
    )
    openssl(
        "req",
        "-new",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        "leaf.key",
        "-out",
        "leaf.csr",
        "-subj",
        "/CN=localhost",
    )
    (directory / "leaf.ext").write_text(
        "basicConstraints=critical,CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost\n"
    )
    openssl(
        "x509",
        "-req",
        "-in",
        "leaf.csr",
        "-CA",
        "ca.pem",
        "-CAkey",
        "ca.key",
        "-CAcreateserial",
        "-out",
        "leaf.pem",
        "-days",
        "1",
        "-extfile",
        "leaf.ext",
    )
    for path in directory.glob("*.key"):
        path.chmod(0o600)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(directory / "leaf.pem", directory / "leaf.key")
    context.set_alpn_protocols(["h2", "http/1.1"])
    return context


class Peers:
    """Small bounded test peers; this is not a production HTTP ingress."""

    def __init__(self) -> None:
        self.grpc_port = 0
        self.discovery = {}
        self.requests = []
        self.discovery_paths = []
        self.h2_connections = 0
        self.tasks = set()
        self.errors = []

    @staticmethod
    async def close_writer(writer) -> None:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, ssl.SSLError):
                pass

    async def upstream(self, reader, writer) -> None:
        task = asyncio.current_task()
        self.tasks.add(task)
        try:
            async with asyncio.timeout(5):
                header = await reader.readuntil(b"\r\n\r\n")
                request, *fields = header[:-4].split(b"\r\n")
                assert request == b"POST /v1/chat/completions HTTP/1.1"
                headers = {}
                for field in fields:
                    key, value = field.split(b":", 1)
                    key = key.lower()
                    assert key not in headers
                    headers[key] = value.strip()
                assert headers[b"content-type"].startswith(b"application/json")
                assert b"transfer-encoding" not in headers
                length = int(headers[b"content-length"])
                assert 0 < length <= 1 << 20
                payload = await reader.readexactly(length)
                self.requests.append(payload)
                assert payload == REQUEST
                assert json.loads(payload)["stream_options"] == {"include_usage": True}
                events = [b"data: " + chunk + b"\n\n" for chunk in CHUNKS]
                events.append(b"data: [DONE]\n\n")
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                    + f"Content-Length: {sum(map(len, events))}\r\n".encode()
                    + b"Connection: close\r\n\r\n"
                )
                for event in events:
                    writer.write(event)
                    await writer.drain()
                    await asyncio.sleep(0.01)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            self.errors.append(error)
        finally:
            await self.close_writer(writer)
            self.tasks.discard(task)

    async def ingress(self, reader, writer) -> None:
        task = asyncio.current_task()
        self.tasks.add(task)
        upstream = None
        pumps = []
        try:
            tls = writer.get_extra_info("ssl_object")
            if tls.selected_alpn_protocol() == "h2":
                self.h2_connections += 1
                incoming, upstream = await asyncio.open_connection("127.0.0.1", self.grpc_port)

                async def pump(source, destination):
                    while chunk := await source.read(65536):
                        destination.write(chunk)
                        await destination.drain()

                pumps = [
                    asyncio.create_task(pump(reader, upstream)),
                    asyncio.create_task(pump(incoming, writer)),
                ]
                done, _ = await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
                for completed in done:
                    completed.result()
            else:
                async with asyncio.timeout(5):
                    header = await reader.readuntil(b"\r\n\r\n")
                method, path, protocol = header.split(b"\r\n", 1)[0].decode("ascii").split(" ")
                assert method == "GET" and protocol == "HTTP/1.1"
                response = self.discovery[path]
                body = response["body"].encode("utf-8")
                self.discovery_paths.append(path)
                headers = [
                    (key, value)
                    for key, value in response["headers"]
                    if key.lower() not in ("content-length", "connection")
                ]
                headers.extend([("Content-Length", str(len(body))), ("Connection", "close")])
                writer.write(
                    (
                        f"HTTP/1.1 {response['status']} OK\r\n"
                        + "".join(f"{key}: {value}\r\n" for key, value in headers)
                        + "\r\n"
                    ).encode("ascii")
                    + body
                )
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            self.errors.append(error)
        finally:
            for pending in pumps:
                pending.cancel()
            if pumps:
                await asyncio.gather(*pumps, return_exceptions=True)
            await self.close_writer(upstream)
            await self.close_writer(writer)
            self.tasks.discard(task)

    async def close(self) -> None:
        pending = tuple(self.tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


def deploy(node, origin: str) -> tuple[dict, bytes, str]:
    proxy_artifact = Artifact("ERC1967Proxy")
    registry_artifact, token, mppi = (
        Artifact("DiscoveryRegistry"),
        Artifact("MockUSDC"),
        Artifact("MPPI"),
    )
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
    node.send(node.deployer, registry, registry_artifact.call("setURI", (42, origin + "/discover")))
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
    node.send(node.deployer, USDC, token.call("mint", (node.agent.address, 5_510_000)))

    def pin(address: bytes) -> dict:
        return {
            "address": hx(address),
            "code_hash": hx(keccak(raw(node.call("eth_getCode", [hx(address), "latest"])))),
        }

    profile = {
        "version": 1,
        "chain_id": CHAIN_ID,
        "proxy": pin(proxy),
        "implementation": pin(implementation),
        "registry": pin(registry),
        "registry_implementation": pin(registry_implementation),
    }
    # The reusable node has a deterministic old genesis. Align it to the real
    # clock before asking either SDK's normal follower to grant fresh execution.
    now = int(time.time())
    assert now > node.timestamp()
    node.call("evm_setNextBlockTimestamp", [now])
    node.call("evm_mine", [])
    node.call("evm_setIntervalMining", [1])
    return profile, proxy, node.call("eth_blockNumber", [])


def provider_config(node, origin: str, upstream: str) -> dict:
    return {
        "provider": {
            "name": "Native Rust fixture",
            "description": "Controlled upstream and provider-owned counts",
            "public_origin": origin,
            "recipient": node.recipient.address,
        },
        "identity": {"provider_id": "42", "wallet_address": node.operator.address},
        "chain": {"network": "testnet", "max_age_ms": 10000},
        "upstream": {"url": upstream},
        "models": [
            {
                "model": MODEL,
                "description": "Fixture",
                "context_window": 32768,
                "max_output": 8192,
                "modalities": ["text"],
                "embeddings": False,
                "streaming": True,
                "tools": False,
                "structured_output": False,
                "rates": {"input": "1000000000", "output": "1000000000"},
            }
        ],
        "payment": {
            "output_buffer_tokens": 1000,
            "suggest_session_tokens": 5510,
            "collection_margin_ms": 60000,
            "authorization_wait_ms": 10000,
        },
        "settlement": {"interval_ms": 600000, "amount": "5420000"},
    }


def unpaid_request(origin: str, profile: dict, certificate: Path) -> None:
    signer = LocalSessionSigner((7).to_bytes(32, "big"))
    domain = profile_from_mapping(profile).domain
    context = RunContext(b"\x88" * 32, b"\x99" * 32, 1, MODEL, keccak(CATALOG))
    request = pb.CompleteRequest(
        session_id=context.session_id,
        run_id=context.run_id,
        control_generation=1,
        model=MODEL,
        rate_card_hash=context.rate_card_hash,
        auth_signature=signer.sign_run(domain, context),
        payload=REQUEST,
        stream=True,
    )
    with grpc.secure_channel(
        origin.removeprefix("https://"),
        grpc.ssl_channel_credentials(certificate.read_bytes()),
        options=GRPC_OPTIONS,
    ) as channel:
        with pytest.raises(grpc.RpcError) as rejected:
            list(services.ChatStub(channel).Completions(request, timeout=5))
        assert rejected.value.code() == grpc.StatusCode.UNAUTHENTICATED


def paid_agent(node, origin: str, profile: dict, directory: Path, recovery: bool = False) -> dict:
    wallet = COMMON.LocalWallet(
        {"rpc": node.endpoint, "wallet_key": hx((2).to_bytes(32, "big"))}, "wallet_key"
    )
    signer = LocalSessionSigner((7).to_bytes(32, "big"))
    assert wallet.address != signer.address
    reader = ChainReader(HttpJsonRpc(node.endpoint), profile_from_mapping(profile))
    follower = ChainFollower(
        reader, max_age_ms=10000, collection_margin_ms=60000, poll_interval_ms=100
    )
    with Client(
        origin,
        wallet=wallet,
        signer=signer,
        chain=follower,
        contract=ContractCalls(reader.profile.domain, "USDC", "2"),
        checkpoint_path=directory / "agent-checkpoint.json",
        signer_reference="fixture:distinct-public-session-key",
        initial_session_tokens=5510,
        ca_file=str(directory / "ca.pem"),
        timeout_ms=15000,
        request_timeout_ms=30000,
    ) as client:
        opened = client.sessions.open(origin, model=MODEL)
        assert opened.operation.state is OperationState.CONFIRMED
        assert opened.session is not None and opened.session.readiness.ready
        identifier = opened.session.session_id
        assert opened.session.deposit == 5_510_000
        with client.chat.completions.create_raw(REQUEST, model=MODEL, stream=True) as stream:
            assert list(stream) == list(CHUNKS)
        terminal = stream.terminal
        assert terminal is not None and terminal.amount == 5_420_000
        assert (terminal.input_tokens, terminal.cache_tokens, terminal.output_tokens) == (
            4510,
            0,
            910,
        )
        deadline = time.monotonic() + 10
        while (receipt := stream.receipt) is None:
            assert time.monotonic() < deadline, "Control receipt was not delivered"
            time.sleep(0.02)
        for field in (
            "session_id",
            "run_id",
            "model",
            "rate_card_hash",
            "status",
            "final_meter_seq",
            "input_tokens",
            "cache_tokens",
            "output_tokens",
            "amount",
        ):
            assert getattr(terminal, field) == getattr(receipt, field)
        # The same healthy Control attachment remains usable after a request's
        # short-deposit failure. Client never implicitly tops up or retries it.
        with pytest.raises(PaymentError) as rejected:
            with client.chat.completions.create_raw(REQUEST, model=MODEL, stream=True) as denied:
                list(denied)
        assert rejected.value.failure.code is PaymentCode.INSUFFICIENT_DEPOSIT
        if recovery:
            # Request replacement of the provider's runtime through the fixture,
            # then reconnect using only the agent's own retained payment history.
            assert reader.session(identifier, reader.block()).settled_cumulative == 0
            (directory / "recover-request").touch()
            deadline = time.monotonic() + 30
            recovered_path = directory / "recovered.json"
            while not recovered_path.exists():
                assert time.monotonic() < deadline, "Rust supplied-history recovery failed"
                time.sleep(0.02)
            restored = json.loads(recovered_path.read_text())
            assert restored["restored_usage"] == restored["captured"] == 5_420_000
            assert restored["restored_ready"] is False
            recovered = client.sessions.recover(identifier)
            assert recovered.usage_amount == 5_420_000
            assert recovered.accepted_authorization == 5_510_000
            assert recovered.readiness.ready
        deadline = time.monotonic() + 20
        while True:
            state = reader.session(identifier, reader.block())
            if state.settled_cumulative == 5_420_000:
                break
            assert time.monotonic() < deadline, "Rust provider did not capture final usage"
            time.sleep(0.05)
        assert state.status is SessionStatus.OPEN
        view = client.sessions.get(identifier)
        assert view.usage_amount == 5_420_000 and view.accepted_authorization == 5_510_000
        closed = client.sessions.close(identifier)
        assert closed.state is OperationState.CONFIRMED
        state = reader.session(identifier, reader.block())
        assert state.status is SessionStatus.CLOSED and state.settled_cumulative == 5_420_000
    assert wallet.submissions == wallet.authorization_count == 1
    return {
        "session_id": hx(identifier),
        "opening_tx": hx(opened.operation.tx_hash),
        "close_tx": hx(closed.tx_hash),
        "authorization": view.accepted_authorization,
        "usage": view.usage_amount,
        "wallet_submissions": wallet.submissions,
        "funding_authorizations": wallet.authorization_count,
    }


def observe(node, proxy: bytes, start: str, agent: dict, provider: dict) -> None:
    token = Artifact("MockUSDC")
    assert node.uint(USDC, token, "balanceOf", (node.agent.address,)) == 90_000
    assert node.uint(USDC, token, "balanceOf", (node.recipient.address,)) == 5_420_000
    assert node.uint(USDC, token, "balanceOf", (proxy,)) == 0
    logs = node.call(
        "eth_getLogs", [{"address": hx(proxy), "fromBlock": start, "toBlock": "latest"}]
    )
    expected = [None, (5_510_000, 5_420_000, 5_420_000, 5_420_000), (5_420_000, 90_000)]
    # Resolve the opening event from the current compiled ABI: the observer
    # compares actual canonical logs rather than trusting either SDK's totals.
    events = {entry["name"]: entry for entry in Artifact("MPPI").abi if entry["type"] == "event"}
    assert len(logs) == 3
    for log, name, amounts in zip(
        logs, ("SessionOpened", "Settled", "SessionClosed"), expected, strict=True
    ):
        entry = events[name]
        signature = name + "(" + ",".join(OWNED.abi_type(field) for field in entry["inputs"]) + ")"
        assert log["topics"][0] == hx(keccak(text=signature))
        assert log["topics"][1] == agent["session_id"]
        receipt = node.call("eth_getTransactionReceipt", [log["transactionHash"]])
        block = node.call("eth_getBlockByNumber", [log["blockNumber"], False])
        assert receipt["status"] == "0x1"
        assert receipt["blockHash"] == log["blockHash"] == block["hash"]
        assert log in receipt["logs"]
        fields = [field for field in entry["inputs"] if not field["indexed"]]
        values = decode([OWNED.abi_type(field) for field in fields], raw(log["data"]))
        if amounts is None:
            assert (
                dict(zip((field["name"] for field in fields), values, strict=True))["deposit"]
                == 5_510_000
            )
        else:
            assert values == amounts
    assert logs[0]["transactionHash"] == agent["opening_tx"]
    assert logs[-1]["transactionHash"] == agent["close_tx"]
    assert {item["tx_hash"] for item in provider["operations"]} == {
        logs[1]["transactionHash"],
        logs[2]["transactionHash"],
    }
    assert node.uint(USDC, token, "fundingCalls", ()) == 1
    assert node.uint(USDC, token, "transferCalls", ()) == 2


@pytest.mark.parametrize("recovery", [False, True], ids=["ordinary", "recovery-batch"])
def test_python_agent_pays_native_rust_provider_through_controlled_upstream(
    node, tmp_path: Path, recovery: bool
) -> None:
    context = tls_context(tmp_path)

    async def scenario() -> None:
        peers = Peers()
        upstream = await asyncio.start_server(peers.upstream, "127.0.0.1", 0, limit=65536)
        ingress = await asyncio.start_server(
            peers.ingress,
            "127.0.0.1",
            0,
            ssl=context,
            limit=65536,
            ssl_handshake_timeout=5,
            ssl_shutdown_timeout=2,
        )
        process = None
        stop = tmp_path / "rust-stop"
        result = tmp_path / "rust-result.json"
        logfile = tmp_path / "rust-provider.log"
        log = logfile.open("wb")
        try:
            origin = f"https://localhost:{ingress.sockets[0].getsockname()[1]}"
            endpoint = f"http://127.0.0.1:{upstream.sockets[0].getsockname()[1]}/v1"
            profile, proxy, start = deploy(node, origin)
            with socket.socket() as available:
                available.bind(("127.0.0.1", 0))
                peers.grpc_port = available.getsockname()[1]
            ready = tmp_path / "rust-ready.json"
            settings = {
                "rpc": node.endpoint,
                "profile": profile,
                "config": provider_config(node, origin, endpoint),
                "grpc_addr": f"127.0.0.1:{peers.grpc_port}",
                "ready_path": str(ready),
                "stop_path": str(stop),
                "result_path": str(result),
            }
            if recovery:
                settings["config"]["settlement"]["amount"] = "1000000000"
                settings["recover_path"] = str(tmp_path / "recover-request")
                settings["recovered_path"] = str(tmp_path / "recovered.json")
            inputs = tmp_path / "rust-input.json"
            inputs.write_text(json.dumps(settings))
            process = await asyncio.create_subprocess_exec(
                "cargo",
                "test",
                "--locked",
                "--test",
                "runtime_gateway",
                "--",
                "--ignored",
                "--exact",
                "owned_runtime_gateway",
                "--nocapture",
                cwd=RUST,
                env={**os.environ, "MPPI_RUST_RUNTIME_INPUT": str(inputs)},
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            async with asyncio.timeout(180):
                while True:
                    assert process.returncode is None, logfile.read_text()
                    if ready.exists():
                        try:
                            announced = json.loads(ready.read_text())
                            _, writer = await asyncio.open_connection("127.0.0.1", peers.grpc_port)
                            await peers.close_writer(writer)
                            peers.discovery = announced["discovery"]
                            assert announced["grpc_addr"] == settings["grpc_addr"]
                            break
                        except (ConnectionError, json.JSONDecodeError):
                            pass
                    await asyncio.sleep(0.05)
            await asyncio.to_thread(unpaid_request, origin, profile, tmp_path / "ca.pem")
            assert peers.requests == [], peers.errors
            connections_before_paid = peers.h2_connections
            try:
                agent = await asyncio.to_thread(
                    paid_agent, node, origin, profile, tmp_path, recovery
                )
            except Exception as error:
                raise AssertionError(
                    f"Paid agent failed; peer errors={peers.errors!r}; "
                    f"discovery={peers.discovery_paths!r}; provider={logfile.read_text()}"
                ) from error
            assert peers.requests == [REQUEST], peers.errors
            assert peers.h2_connections == connections_before_paid + (2 if recovery else 1)
            assert "/discover" in peers.discovery_paths
            assert "/models" in peers.discovery_paths
            stop.touch()
            await asyncio.wait_for(process.wait(), 20)
            assert process.returncode == 0, logfile.read_text()
            provider = json.loads(result.read_text())
            assert provider["preparations"] == 2
            assert provider["executions"] == provider["final_reports"] == 1
            assert provider["usage_reports"] == 1
            assert provider["coverage_grants"] == 1 and provider["coverage_denials"] == 0
            assert provider["actual_usage"] == 5_420_000
            assert provider["session_id"] == agent["session_id"]
            assert provider["wallet_submissions"] == 2
            assert provider["shutdown_unresolved_runs"] == 0
            assert provider["shutdown_pending_operations"] == 0
            assert provider["execution_errors"] == provider["stop_requests"] == 0
            if recovery:
                assert provider["recovery"]["captured"] == 5_420_000
                assert provider["recovery"]["batch_tx"] is not None
            else:
                assert provider["recovery"] is None
            assert provider["sessions"] == [
                {
                    "session_id": agent["session_id"],
                    "deposit": 5_510_000,
                    "settled_cumulative": 5_420_000,
                    "authorization": 5_510_000,
                    "usage": 5_420_000,
                    "status": "CLOSED",
                }
            ]
            observe(node, proxy, start, agent, provider)
            assert peers.errors == []
            (tmp_path / "interop-evidence.json").write_text(
                json.dumps(
                    {
                        "agent": agent,
                        "provider": provider,
                        "upstream_requests": len(peers.requests),
                        "paid_h2_connections": peers.h2_connections - connections_before_paid,
                        "discovery_paths": peers.discovery_paths,
                    },
                    indent=2,
                )
            )
        finally:
            if process is not None and process.returncode is None:
                stop.touch()
                try:
                    await asyncio.wait_for(process.wait(), 10)
                except TimeoutError:
                    process.kill()
                    await process.wait()
            ingress.close()
            upstream.close()
            await ingress.wait_closed()
            await upstream.wait_closed()
            await peers.close()
            log.close()

    asyncio.run(scenario())
