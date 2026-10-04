"""Agent-only installed SDK consumer; authoritative evidence comes from the wire/chain."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path

from common import MODEL, LocalWallet, hx, raw, reader_for, role_evidence, write_json
from mppi.v1 import mppi_pb2 as pb
from mppi_agent.client import Client
from mppi_protocol.contract import ContractCalls, session_id
from mppi_protocol.deployment import profile_to_mapping
from mppi_protocol.follower import ChainFollower
from mppi_protocol.registry import RegistryReader
from mppi_protocol.signing import LocalSessionSigner
from mppi_protocol.types import OperationState, SessionStatus
from mppi_protocol.wire import descriptor_from_wire

INPUT = Path("/input")
STATE = Path("/state")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=("standard", "recovery-initial", "recovery-restored", "onboarding"),
        nargs="?",
        default="standard",
    )
    mode = parser.parse_args().mode
    settings = json.loads((INPUT / "config.json").read_text())
    evidence = role_evidence("agent")
    evidence["process_id"] = os.getpid()
    wallet = LocalWallet(settings, "wallet_key")
    reader = reader_for(settings)
    signer = LocalSessionSigner(raw(settings["signer_key"]))
    assert signer.address != wallet.address
    agent_id = 43
    if mode == "onboarding":
        assert settings.get("onboarding") is True
        profile_path = STATE / "deployment.json"
        config_path = STATE / "agent-onboarding.json"
        write_json(profile_path, profile_to_mapping(reader.profile))
        command = [
            "/opt/role/bin/mppi-agent",
            "onboard",
            "--profile",
            str(profile_path),
            "--config",
            str(config_path),
            "--wallet-factory",
            "common:onboarding_wallet",
        ]
        environment = dict(os.environ, MPPI_RPC_URL=settings["rpc"], PYTHONPATH="/fixture")
        nonce_before = int(
            wallet.rpc.call("eth_getTransactionCount", [hx(wallet.address), "pending"]), 16
        )
        first = subprocess.run(command, env=environment, capture_output=True, text=True, check=True)
        first_result = json.loads(first.stdout)
        assert first_result["complete"] and first_result["submitted"]
        assert first_result["operation_state"] == "confirmed"
        assert first_result["wallet"].lower() == hx(wallet.address)
        retained = config_path.read_bytes()
        config = json.loads(retained)
        agent_id = int(first_result["agent_id"])
        assert config["agent_id"] == str(agent_id)
        nonce_after = int(
            wallet.rpc.call("eth_getTransactionCount", [hx(wallet.address), "pending"]), 16
        )
        assert nonce_after == nonce_before + 1
        repeated = subprocess.run(
            command, env=environment, capture_output=True, text=True, check=True
        )
        repeated_result = json.loads(repeated.stdout)
        assert repeated_result["complete"] and not repeated_result["submitted"]
        assert repeated_result["agent_id"] == str(agent_id)
        assert repeated_result["tx_hash"] is None
        assert config_path.read_bytes() == retained
        assert (
            int(wallet.rpc.call("eth_getTransactionCount", [hx(wallet.address), "pending"]), 16)
            == nonce_after
        )
        identity = RegistryReader(reader).read(agent_id)
        assert identity.owner == identity.agent_wallet == wallet.address
        assert identity.agent_uri == "" and identity.catalog_bytes == b""
        evidence["onboarding"] = {
            "fresh_agent_id": str(agent_id),
            "first": first_result,
            "repeated": repeated_result,
            "registration_transactions": nonce_after - nonce_before,
            "rerun_transactions": 0,
            "retained_config_unchanged": True,
            "identity_verified_from_chain": True,
            "installed_cli": command[0],
        }
    follower = ChainFollower(
        reader, max_age_ms=10000, collection_margin_ms=60000, poll_interval_ms=100
    )
    with Client(
        settings["origin"],
        wallet=wallet,
        signer=signer,
        chain=follower,
        contract=ContractCalls(reader.profile.domain, "USDC", "2"),
        checkpoint_path=STATE / "checkpoint.json",
        signer_reference="fixture:/input/signer_key",
        initial_session_tokens=5510,
        agent_id=agent_id,
        ca_file=str(INPUT / "server.crt"),
        timeout_ms=15000,
        request_timeout_ms=30000,
    ) as client:
        if mode == "recovery-restored":
            before = json.loads((STATE / "checkpoint.json").read_text())
            descriptor = descriptor_from_wire(
                pb.SessionDescriptor.FromString(bytes.fromhex(before["ledger"]["descriptor"]))
            )
            old_generation = before["ledger"]["generation"]
            identifier = session_id(reader.profile.domain, descriptor)
            recovered = client.sessions.recover(identifier)
            after = json.loads((STATE / "checkpoint.json").read_text())
            assert recovered.session_id == identifier
            assert recovered.usage_amount == 5_420_000
            assert after["ledger"]["generation"] > old_generation
            evidence.update(
                session_id=hx(identifier),
                recovered=True,
                prior_generation=old_generation,
                recovered_generation=after["ledger"]["generation"],
                actual_usage=recovered.usage_amount,
                accepted_authorization=recovered.accepted_authorization,
            )
            closed = client.sessions.close(identifier)
            assert closed.state is OperationState.CONFIRMED
            evidence["close_tx"] = hx(closed.tx_hash)
            state = reader.session(identifier, reader.block())
            assert state.status is SessionStatus.CLOSED
            assert state.settled_cumulative == 5_420_000
            evidence.update(
                status="PASS",
                wallet_submissions=wallet.submissions,
                funding_authorizations=wallet.authorization_count,
            )
            assert wallet.submissions == wallet.authorization_count == 0
            write_json(STATE / "agent-recovered.json", evidence)
            return
        opening = client.sessions.open(settings["origin"], model=MODEL)
        assert opening.operation.state is OperationState.CONFIRMED
        assert opening.session is not None and opening.session.readiness.ready
        identifier = opening.session.session_id
        evidence.update(
            session_id=hx(identifier),
            opening_tx=hx(opening.operation.tx_hash),
            deposit=opening.session.deposit,
        )
        assert opening.session.deposit == 5_510_000
        if mode == "recovery-initial":
            stream = client.chat.completions.create(
                model=MODEL, messages=[{"role": "user", "content": "fixture"}], stream=True
            )
            first = next(stream)
            assert first == b'{"id":"fixture","choices":[{"delta":{"content":"paid "}}]}'
            stream.close()
            pending = json.loads((STATE / "checkpoint.json").read_text())
            assert len(pending["ledger"]["runs"]) == 1
            assert pending["ledger"]["runs"][0]["final"] is None
            write_json(STATE / "checkpoint-before-disconnect.json", pending)
            evidence.update(
                status="INTERRUPTED",
                first_chunk_received=True,
                pending_final_checkpoint=True,
                wallet_submissions=wallet.submissions,
                funding_authorizations=wallet.authorization_count,
            )
            assert wallet.submissions == wallet.authorization_count == 1
            write_json(STATE / "agent-initial.json", evidence)
            return
        with client.chat.completions.create(
            model=MODEL, messages=[{"role": "user", "content": "fixture"}], stream=True
        ) as stream:
            chunks = list(stream)
        assert chunks == [
            b'{"id":"fixture","choices":[{"delta":{"content":"paid "}}]}',
            b'{"id":"fixture","choices":[{"delta":{"content":"response"}}]}',
        ]
        evidence["unchanged_chunks"] = len(chunks)
        terminal = stream.terminal
        assert terminal is not None and terminal.amount == 5_420_000
        receipt_deadline = time.monotonic() + 5
        while (receipt := stream.receipt) is None:
            assert time.monotonic() < receipt_deadline, "final Control receipt was not delivered"
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
        evidence["matching_final_records"] = True
        # This is an independent chain observation, never a fixture notification.
        deadline = time.monotonic() + 20
        while True:
            state = reader.session(identifier, reader.block())
            if state.settled_cumulative == 5_420_000:
                break
            assert time.monotonic() < deadline, "provider did not capture actual usage"
            time.sleep(0.05)
        assert state.status is SessionStatus.OPEN
        view = client.sessions.get(identifier)
        assert view.usage_amount == 5_420_000 and view.accepted_authorization == 5_510_000
        evidence.update(
            actual_usage=view.usage_amount,
            authorization=view.accepted_authorization,
            capture_kept_open=True,
        )
        closed = client.sessions.close(identifier)
        assert closed.state is OperationState.CONFIRMED
        evidence["close_tx"] = hx(closed.tx_hash)
        state = reader.session(identifier, reader.block())
        assert state.status is SessionStatus.CLOSED and state.settled_cumulative == 5_420_000
    evidence.update(
        status="PASS",
        wallet_submissions=wallet.submissions,
        funding_authorizations=wallet.authorization_count,
    )
    assert wallet.submissions == wallet.authorization_count == 1
    write_json(STATE / "agent.json", evidence)


if __name__ == "__main__":
    main()
