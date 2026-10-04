"""Provider-only synthetic execution and test TLS ingress; no inference gateway.

TLS ALPN chooses native gRPC h2 versus tiny fixture-only HTTP/1 discovery.
Python 3.13 ssl.SSLContext.set_alpn_protocols and asyncio.start_server document
this composition: https://docs.python.org/3.13/library/ssl.html and
https://docs.python.org/3.13/library/asyncio-stream.html.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import signal
import ssl
from pathlib import Path

import grpc
from common import CATALOG, MODEL, LocalWallet, hx, raw, reader_for, role_evidence, write_json
from mppi_protocol import RunStatus, TokenCounts
from mppi_protocol.catalog import parse_catalog
from mppi_protocol.contract import ContractCalls
from mppi_protocol.follower import ChainFollower
from mppi_protocol.registry import RegistryCalls, RegistryReader
from mppi_protocol.types import OperationState
from mppi_protocol.wire import GRPC_OPTIONS, MAX_MESSAGE_BYTES
from mppi_provider import ProviderConfig
from mppi_provider.runtime import Mppi
from recovery import decode as decode_recovery
from recovery import encode as encode_recovery

STATE = Path("/state")
INPUT = Path("/input")


def onboard_fixture(mode, settings):
    """Caller-owned disposable fixture state; not a provider SDK persistence API."""
    wallet = LocalWallet(settings, "wallet_key")
    registry = RegistryReader(reader_for(settings))
    calls = RegistryCalls(
        registry.chain.profile.domain.chain_id, registry.chain.profile.registry_address
    )
    path = STATE / "provider-onboarding.json"
    retained = json.loads(path.read_text()) if path.exists() else {"operations": []}

    def submit(operation):
        registry.simulate(operation)
        reference = "fixture:" + secrets.token_hex(32)
        intent = {
            "reference": reference,
            "calldata": hx(operation.call.data),
            "kind": operation.kind.value,
        }
        retained["operations"].append(intent)
        write_json(path, retained)
        result = wallet.execute(operation.call, operation_ref=reference)
        intent["tx_hash"] = hx(result.tx_hash)
        write_json(path, retained)
        evidence = registry.reconcile(operation, operation_ref=reference, tx_hash=result.tx_hash)
        assert evidence.operation.state is OperationState.CONFIRMED
        return evidence.identity

    if mode == "onboard-register":
        if "provider_id" not in retained:
            assert retained["operations"] == [], "unresolved registration cannot be resubmitted"
            identity = submit(
                calls.register(wallet.address, agent_uri=settings["origin"] + "/discover")
            )
            assert identity.owner == identity.agent_wallet == wallet.address
            assert identity.catalog_bytes == b""
            retained.update(
                provider_id=identity.agent_id, registration_tx=retained["operations"][0]["tx_hash"]
            )
            write_json(path, retained)
        else:
            assert registry.read(retained["provider_id"]).owner == wallet.address
        return
    identifier = retained["provider_id"]
    identity = registry.read(identifier)
    assert identity.owner == wallet.address
    recipient = raw(settings["recipient"])
    assert recipient != wallet.address
    if identity.agent_wallet != recipient:
        assert identity.agent_wallet == wallet.address and identity.catalog_bytes == b""
        imported = json.loads((STATE / "recipient-proof.json").read_text())
        deadline = imported["typed_data"]["message"]["deadline"]
        expected = registry.wallet_proof(
            identifier, recipient, owner=wallet.address, deadline=deadline
        )
        assert imported["typed_data"] == expected
        identity = submit(
            calls.set_agent_wallet(
                wallet.address, identifier, recipient, deadline, raw(imported["signature"])
            )
        )
        assert identity.agent_wallet == recipient
    assert identity.agent_uri == settings["origin"] + "/discover"
    if identity.catalog_bytes != CATALOG:
        assert identity.catalog_bytes == b""
        # The recipient is confirmed before the offering becomes payable.
        assert registry.read(identifier).agent_wallet == recipient
        identity = submit(calls.set_catalog(wallet.address, identifier, parse_catalog(CATALOG)))
    assert identity.catalog_bytes == CATALOG and identity.agent_wallet == recipient
    if retained.get("complete"):
        assert wallet.submissions == 0
        write_json(
            STATE / "provider-onboarding-rerun.json",
            {"status": "PASS", "wallet_submissions": 0, "recipient_signatures": 0},
        )
    else:
        retained.update(
            complete=True,
            catalog_published_last=retained["operations"][-1]["kind"] == "SET_CATALOG",
        )
        write_json(path, retained)


class Fixture:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.evidence = role_evidence("provider")
        self.evidence.update(
            preparations=0,
            executions=0,
            final_reports=0,
            h2_connections=0,
            discovery_requests=[],
            process_id=os.getpid(),
        )
        self.session_id = None
        self.reported = asyncio.Event()

    def save(self) -> None:
        write_json(STATE / "provider.json", self.evidence)

    async def coverage(self, request, context):
        self.evidence["preparations"] += 1
        assert request.stream is True
        assert json.loads(request.payload)["model"] == MODEL
        return TokenCounts(4510, 0, 1000)

    async def execute(self, request, context, runs, stop_requested):
        self.evidence["executions"] += 1
        self.session_id = context.session_id
        assert not stop_requested.is_set()
        yield b'{"id":"fixture","choices":[{"delta":{"content":"paid "}}]}'
        if self.mode == "recovery-initial":
            async with asyncio.timeout(20):
                await stop_requested.wait()
            final = await runs.report_terminal(
                context, TokenCounts(4510, 0, 910), RunStatus.CANCELLED
            )
            assert final.accounting.session_usage_amount == 5_420_000
            self.evidence["stop_observed"] = True
            self.evidence["terminal_status"] = RunStatus.CANCELLED.value
            self.evidence["final_reports"] += 1
            self.evidence["actual_usage"] = final.accounting.session_usage_amount
            self.reported.set()
            self.save()
            return
        yield b'{"id":"fixture","choices":[{"delta":{"content":"response"}}]}'
        final = await runs.report_terminal(context, TokenCounts(4510, 0, 910), RunStatus.COMPLETED)
        assert final.accounting.session_usage_amount == 5_420_000
        self.evidence["final_reports"] += 1
        self.evidence["actual_usage"] = final.accounting.session_usage_amount
        self.reported.set()
        self.save()

    async def collect(self, provider, wallet):
        await self.reported.wait()
        view = await provider.sessions.get(self.session_id)
        assert view.accepted_authorization == 5_510_000
        self.evidence["authorization"] = view.accepted_authorization
        if self.mode == "recovery-initial":
            checkpoint = await provider.sessions.checkpoint(self.session_id)
            write_json(STATE / "recovery.json", encode_recovery(checkpoint))
            self.evidence["checkpoint_written"] = True
            self.evidence["wallet_submissions_at_checkpoint"] = wallet.submissions
            self.save()
            return
        result = await provider.sessions.settle(self.session_id)
        assert result.state is OperationState.CONFIRMED and result.sessions[0].target_covered
        reference = result.sessions[0].settlement_reference
        assert reference is not None and reference.delta == 5_420_000
        self.evidence["capture_tx"] = hx(result.tx_hash)
        self.evidence["capture_delta"] = reference.delta
        self.evidence["wallet_submissions_at_capture"] = wallet.submissions
        self.save()


class TlsIngress:
    """Bounded fixture frontend; h2 traffic is relayed byte for byte."""

    def __init__(self, provider, fixture, grpc_port):
        self.provider, self.fixture, self.grpc_port = provider, fixture, grpc_port
        self.tasks = set()

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        upstream = None
        pumps = []
        try:
            tls = writer.get_extra_info("ssl_object")
            if tls.selected_alpn_protocol() == "h2":
                self.fixture.evidence["h2_connections"] += 1
                self.fixture.save()
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
                response = self.provider.discovery(path)
                assert response is not None
                self.fixture.evidence["discovery_requests"].append(path)
                self.fixture.save()
                headers = [
                    ("Connection", "close"),
                    *response.headers,
                ]
                writer.write(
                    (
                        f"HTTP/1.1 {response.status} OK\r\n"
                        + "".join(f"{key}: {value}\r\n" for key, value in headers)
                        + "\r\n"
                    ).encode()
                    + response.body
                )
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            for pending in pumps:
                pending.cancel()
            if pumps:
                await asyncio.gather(*pumps, return_exceptions=True)
            for connection in (upstream, writer):
                if connection is not None:
                    connection.close()
                    try:
                        await connection.wait_closed()
                    except (ConnectionError, ssl.SSLError):
                        pass
            self.tasks.discard(task)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=(
            "standard",
            "recovery-initial",
            "recovery-restored",
            "onboarding",
            "onboard-register",
            "onboard-complete",
        ),
        nargs="?",
        default="standard",
    )
    mode = parser.parse_args().mode
    settings = json.loads((INPUT / "config.json").read_text())
    if mode in ("onboard-register", "onboard-complete"):
        onboard_fixture(mode, settings)
        return
    if mode == "onboarding":
        settings["provider_id"] = json.loads((STATE / "provider-onboarding.json").read_text())[
            "provider_id"
        ]
    fixture = Fixture(mode)
    wallet = LocalWallet(settings, "wallet_key")
    assert hx(wallet.address).lower() == settings["operator"].lower()
    assert wallet.address != bytes.fromhex(settings["recipient"][2:])
    reader = reader_for(settings)
    config = ProviderConfig.from_mapping(
        {
            "provider": {
                "name": "Isolated local provider",
                "description": "Synthetic count fixture",
                "public_origin": settings["origin"],
                "recipient": settings["recipient"],
            },
            "identity": {
                "provider_id": settings["provider_id"],
                "wallet_address": settings["operator"],
            },
            "chain": {"network": "testnet", "max_age_ms": 10000},
            "upstream": {"url": "http://127.0.0.1:9999/v1"},
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
            "settlement": {"interval_ms": 600000},
        }
    )
    chain = ChainFollower(
        reader, max_age_ms=10000, collection_margin_ms=60000, poll_interval_ms=100
    )
    provider = Mppi(
        config,
        chain=chain,
        contract=ContractCalls(reader.profile.domain, "USDC", "2"),
        wallet=wallet,
        coverage=fixture.coverage,
        execution=fixture.execute,
    )
    server = grpc.aio.server(options=GRPC_OPTIONS)
    provider.register_grpc(server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    assert (await provider.start()).ready
    if mode == "recovery-restored":
        serialized = (STATE / "recovery.json").read_bytes()
        assert len(serialized) <= 2 * MAX_MESSAGE_BYTES
        checkpoint = decode_recovery(json.loads(serialized))
        restored = await provider.sessions.recover(checkpoint)
        fixture.session_id = restored.session_id
        fixture.evidence.update(
            recovered=True,
            recovered_generation=checkpoint.snapshot.control_generation,
            recovery_process=True,
            preparations=0,
            executions=0,
            final_reports=0,
        )
        result = await provider.sessions.settleBatch((restored.session_id,))
        assert result.state is OperationState.CONFIRMED and result.sessions[0].target_covered
        reference = result.sessions[0].settlement_reference
        assert reference is not None and reference.delta == 5_420_000
        fixture.evidence.update(
            capture_tx=hx(result.tx_hash),
            capture_delta=reference.delta,
            wallet_submissions_at_capture=wallet.submissions,
        )
        fixture.save()
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.minimum_version = ssl.TLSVersion.TLSv1_2
    tls.load_cert_chain(INPUT / "server.crt", INPUT / "server.key")
    tls.set_alpn_protocols(["h2", "http/1.1"])
    ingress = TlsIngress(provider, fixture, port)
    listener = await asyncio.start_server(
        ingress.handle,
        "0.0.0.0",
        8443,
        ssl=tls,
        limit=8192,
        ssl_handshake_timeout=5,
        ssl_shutdown_timeout=2,
    )
    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    collect = (
        asyncio.create_task(fixture.collect(provider, wallet))
        if mode != "recovery-restored"
        else None
    )
    fixture.evidence["ready"] = True
    fixture.save()
    try:
        await stop.wait()
        if collect is not None:
            collect.result()
    finally:
        listener.close()
        await listener.wait_closed()
        if collect is not None:
            collect.cancel()
            await asyncio.gather(collect, return_exceptions=True)
        for task in tuple(ingress.tasks):
            task.cancel()
        await asyncio.gather(*tuple(ingress.tasks), return_exceptions=True)
        result = await provider.shutdown(asyncio.get_running_loop().time() + 5)
        await server.stop(0)
        fixture.evidence["shutdown_unresolved_runs"] = len(result.unresolved_runs)
        fixture.evidence["wallet_submissions"] = wallet.submissions
        fixture.save()


if __name__ == "__main__":
    asyncio.run(main())
