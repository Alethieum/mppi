"""Strict JSON transport for the provider-owned recovery fixture checkpoint."""

from __future__ import annotations

import base64

from mppi.v1 import mppi_pb2 as pb
from mppi_protocol import RunAccounting, RunStatus, TokenCounts
from mppi_protocol.types import ChainObservation, RunContext
from mppi_provider import RecoveryEvidence, RecoveryRun


def _b(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _unb(value: object) -> bytes:
    if type(value) is not str:
        raise ValueError("encoded bytes must be a string")
    return base64.b64decode(value, validate=True)


def _observation(value: ChainObservation) -> dict[str, object]:
    return {
        "chain_id": value.chain_id,
        "contract_address": _b(value.contract_address),
        "block_number": value.block_number,
        "block_hash": _b(value.block_hash),
    }


def _read_observation(value: object) -> ChainObservation:
    if type(value) is not dict or set(value) != {
        "chain_id",
        "contract_address",
        "block_number",
        "block_hash",
    }:
        raise ValueError("invalid recovery observation")
    return ChainObservation(
        value["chain_id"],
        _unb(value["contract_address"]),
        value["block_number"],
        _unb(value["block_hash"]),
    )


def _context(value: RunContext) -> dict[str, object]:
    return {
        "session_id": _b(value.session_id),
        "run_id": _b(value.run_id),
        "control_generation": value.control_generation,
        "model": value.model,
        "rate_card_hash": _b(value.rate_card_hash),
    }


def _read_context(value: object) -> RunContext:
    if type(value) is not dict or set(value) != {
        "session_id",
        "run_id",
        "control_generation",
        "model",
        "rate_card_hash",
    }:
        raise ValueError("invalid recovery run context")
    return RunContext(
        _unb(value["session_id"]),
        _unb(value["run_id"]),
        value["control_generation"],
        value["model"],
        _unb(value["rate_card_hash"]),
    )


def _counts(value: TokenCounts) -> list[int]:
    return [value.input_tokens, value.cache_tokens, value.output_tokens]


def _read_counts(value: object) -> TokenCounts:
    if type(value) is not list or len(value) != 3:
        raise ValueError("invalid recovery token counts")
    return TokenCounts(*value)


def encode(evidence: RecoveryEvidence) -> dict[str, object]:
    """Encode the exact pre-collection subset used by this acceptance case."""
    if evidence.operation is not None or evidence.baseline is not None:
        raise ValueError("fixture checkpoint must precede every collection operation")
    return {
        "version": 1,
        "snapshot": _b(evidence.snapshot.SerializeToString()),
        "reservations": [
            {
                "context": _context(context),
                "counts": _counts(accounting.counts),
                "remaining_numerator": accounting.remaining_numerator,
                "final_status": accounting.final_status.value
                if accounting.final_status is not None
                else None,
            }
            for context, accounting in evidence.reservations
        ],
        "observation": _observation(evidence.observation),
        "opening_tx": _b(evidence.opening_tx),
        "runs": [
            {
                "context": _context(run.context),
                "signature": _b(run.signature),
                "counts": _counts(run.accounting.counts),
                "remaining_numerator": run.accounting.remaining_numerator,
                "final_status": run.accounting.final_status.value
                if run.accounting.final_status is not None
                else None,
                "covered_counts": _counts(run.covered_counts),
                "meter_seq": run.meter_seq,
                "admitted": run.admitted,
                "not_admitted": run.not_admitted,
            }
            for run in evidence.runs
        ],
        "publications": [
            [_b(catalog_hash), _observation(observation)]
            for catalog_hash, observation in evidence.publications
        ],
        "confirmed_units": evidence.confirmed_units,
        "close_reference": evidence.close_reference,
        "inconsistent": evidence.inconsistent,
    }


def decode(value: object) -> RecoveryEvidence:
    """Reject unknown shapes before constructing validated public value records."""
    fields = {
        "version",
        "snapshot",
        "reservations",
        "observation",
        "opening_tx",
        "runs",
        "publications",
        "confirmed_units",
        "close_reference",
        "inconsistent",
    }
    if type(value) is not dict or set(value) != fields or value["version"] != 1:
        raise ValueError("invalid recovery checkpoint envelope")
    if type(value["reservations"]) is not list or type(value["runs"]) is not list:
        raise ValueError("invalid recovery checkpoint collections")
    reservations = []
    for item in value["reservations"]:
        if type(item) is not dict or set(item) != {
            "context",
            "counts",
            "remaining_numerator",
            "final_status",
        }:
            raise ValueError("invalid recovery reservation")
        status = item["final_status"]
        reservations.append(
            (
                _read_context(item["context"]),
                RunAccounting(
                    _read_counts(item["counts"]),
                    item["remaining_numerator"],
                    RunStatus(status) if status is not None else None,
                ),
            )
        )
    runs = []
    run_fields = {
        "context",
        "signature",
        "counts",
        "remaining_numerator",
        "final_status",
        "covered_counts",
        "meter_seq",
        "admitted",
        "not_admitted",
    }
    for item in value["runs"]:
        if type(item) is not dict or set(item) != run_fields:
            raise ValueError("invalid recovery run")
        status = item["final_status"]
        runs.append(
            RecoveryRun(
                _read_context(item["context"]),
                _unb(item["signature"]),
                RunAccounting(
                    _read_counts(item["counts"]),
                    item["remaining_numerator"],
                    RunStatus(status) if status is not None else None,
                ),
                _read_counts(item["covered_counts"]),
                item["meter_seq"],
                item["admitted"],
                item["not_admitted"],
            )
        )
    publications = value["publications"]
    if type(publications) is not list or any(
        type(item) is not list or len(item) != 2 for item in publications
    ):
        raise ValueError("invalid recovery publications")
    return RecoveryEvidence(
        pb.SessionState.FromString(_unb(value["snapshot"])),
        tuple(reservations),
        _read_observation(value["observation"]),
        _unb(value["opening_tx"]),
        tuple(runs),
        tuple((_unb(item[0]), _read_observation(item[1])) for item in publications),
        None,
        None,
        value["confirmed_units"],
        value["close_reference"],
        value["inconsistent"],
    )
