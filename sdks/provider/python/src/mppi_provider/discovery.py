"""Framework-neutral discovery documents from applied provider declarations.

The caller supplies block-correlated identity evidence. This comparison does not
establish RPC honesty, freshness, wallet access, deployed-code identity or paid
readiness. It neither publishes registry metadata nor opens a listener.
"""

from dataclasses import dataclass, field

from mppi_protocol.discovery import (
    REGISTRATION_TYPE,
    ProviderIdentity,
    address_hex,
    origin_from_discover,
    verify_models,
    verify_registration,
)

from .config import ConfigError, ProviderConfig, _json_bytes
from .interfaces import DiscoveryResponse


@dataclass(frozen=True, slots=True)
class DiscoveryDocuments:
    config: ProviderConfig
    identity: ProviderIdentity
    registration: bytes = field(init=False, repr=False)
    models: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.config, ProviderConfig) or not isinstance(
            self.identity, ProviderIdentity
        ):
            raise ConfigError("discovery", "validated config and identity evidence are required")
        config, identity = self.config, self.identity
        if (
            config.chain.chain_id != identity.observation.chain_id
            or config.identity.provider_id != identity.provider_id
            or config.identity.wallet_address != identity.owner
            or config.provider.recipient != identity.recipient
            or config.provider.public_origin != origin_from_discover(identity.agent_uri)
        ):
            raise ConfigError(
                "discovery", "configuration differs from the supplied registry identity"
            )
        if identity.catalog is not None and config.catalog != identity.catalog:
            raise ConfigError("models.rates", "applied catalog differs from confirmed publication")
        origin = config.provider.public_origin
        registration: dict[str, object] = {
            "type": REGISTRATION_TYPE,
            "name": config.provider.name,
            "description": config.provider.description,
            "services": [
                {"name": "mppi", "endpoint": origin, "version": "1", "models": origin + "/models"},
                *(service.as_object() for service in config.provider.services),
            ],
            "active": True,
            "x402Support": False,
            "registrations": [
                {
                    "agentId": identity.provider_id,
                    "agentRegistry": f"eip155:{identity.observation.chain_id}:{address_hex(identity.registry_address)}",
                }
            ],
        }
        if config.provider.image is not None:
            registration["image"] = config.provider.image
        raw_registration = _json_bytes(registration, "discovery.registration")
        raw_models = _json_bytes(
            {
                "models": [model.card() for model in config.models]
                if identity.catalog is not None
                else []
            },
            "discovery.models",
        )
        verify_registration(raw_registration, identity, expected_origin=origin)
        if identity.catalog is not None:
            verify_models(raw_models, identity.catalog)
        object.__setattr__(self, "registration", raw_registration)
        object.__setattr__(self, "models", raw_models)

    def get(self, path: str) -> DiscoveryResponse | None:
        """Handle an already parsed HTTP GET path; other paths remain host-owned."""
        if path in ("/discover", "/.well-known/agent-registration.json"):
            body = self.registration
        elif path == "/models":
            body = self.models
        else:
            return None
        return DiscoveryResponse(
            status=200,
            headers=(
                ("content-type", "application/json; charset=utf-8"),
                ("content-length", str(len(body))),
                ("cache-control", "no-store"),
            ),
            body=body,
        )
