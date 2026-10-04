"""Read provider documents and correlate guidance with observed ERC-8004 terms."""

from dataclasses import dataclass

from mppi.v1.mppi_pb2 import Challenge
from mppi_protocol.amounts import uint64
from mppi_protocol.catalog import validate_model
from mppi_protocol.discovery import (
    ProviderIdentity,
    normalize_origin,
    origin_from_discover,
    verify_models,
    verify_registration,
)
from mppi_protocol.errors import ValidationCode, ValidationError
from mppi_protocol.http import HttpTransport
from mppi_protocol.types import fixed_bytes


@dataclass(frozen=True, slots=True)
class ProviderDiscovery:
    identity: ProviderIdentity
    origin: str
    registration: bytes
    models: bytes
    model_ids: tuple[str, ...]


def discover(
    identity: ProviderIdentity,
    *,
    expected_origin: str | None = None,
    transport: HttpTransport | None = None,
) -> ProviderDiscovery:
    """Fetch direct HTTPS documents; the caller supplies verified chain identity.

    No registration, funding, key creation or price acceptance occurs here.
    The network Client's chain follower supplies the identity observation.
    """
    origin = origin_from_discover(identity.agent_uri)
    if expected_origin is not None and normalize_origin(expected_origin) != origin:
        raise ValidationError(ValidationCode.INVALID_VALUE, "provider origin differs from registry")
    http = transport if transport is not None else HttpTransport()
    registration = http.request(origin + "/discover")
    verify_registration(registration, identity, expected_origin=origin)
    # A removed/malformed current offering blocks new work, not recovery of
    # retained session terms. Registration still binds the serving endpoint.
    models = b"" if identity.catalog is None else http.request(origin + "/models")
    model_ids = () if identity.catalog is None else verify_models(models, identity.catalog)
    return ProviderDiscovery(identity, origin, registration, models, model_ids)


def verify_challenge(
    challenge: Challenge, identity: ProviderIdentity, *, request_id: bytes, model: str
) -> None:
    """Validate one same-Control-stream reply before any funding decision."""
    fixed_bytes(request_id, 32, field="request_id")
    validate_model(model)
    if identity.catalog is None:
        raise ValidationError(ValidationCode.INVALID_VALUE, "provider has no valid current catalog")
    expected = {
        "request_id": request_id,
        "model": model,
        "rate_card_hash": identity.catalog.rate_card_hash,
        "provider_id": identity.provider_id.to_bytes(32, "big"),
        "contract": identity.observation.contract_address,
        "chain_id": identity.observation.chain_id,
        "operator": identity.owner,
    }
    for field, value in expected.items():
        if not challenge.HasField(field) or getattr(challenge, field) != value:
            raise ValidationError(ValidationCode.INVALID_VALUE, f"Challenge {field} mismatch")
    identity.catalog.rates_for(model)
    for field in ("cover_output_tokens", "suggest_session_tokens"):
        if not challenge.HasField(field):
            raise ValidationError(ValidationCode.INVALID_VALUE, f"Challenge {field} absent")
        uint64(getattr(challenge, field), field=field, positive=True)
