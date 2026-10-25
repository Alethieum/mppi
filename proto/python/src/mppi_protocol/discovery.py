"""Bounded discovery validation against a separately observed registry identity.

Records do not attest to RPC honesty, freshness or deployed code identity. The
chain reader must establish those properties before these comparisons are used.
"""

from __future__ import annotations

import ipaddress
import json
import re
from dataclasses import dataclass
from typing import cast
from urllib.parse import urlsplit

from .amounts import uint64
from .catalog import Catalog, _check_depth, _reject_constant, _unique_object, parse_catalog
from .errors import ValidationCode, ValidationError
from .types import ChainObservation, fixed_bytes, uint256

REGISTRATION_TYPE = "https://eips.ethereum.org/EIPS/eip-8004#registration-v1"
MAX_DOCUMENT_BYTES = 1 << 20
_ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z", re.ASCII)
_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z", re.ASCII)


def _invalid(message: str) -> ValidationError:
    return ValidationError(ValidationCode.INVALID_VALUE, message)


def parse_address(value: str) -> bytes:
    if type(value) is not str or _ADDRESS.fullmatch(value) is None:
        raise _invalid("address must be 0x followed by 40 hexadecimal characters")
    return fixed_bytes(bytes.fromhex(value[2:]), 20, field="address", nonzero=True)


def address_hex(value: bytes) -> str:
    return "0x" + fixed_bytes(value, 20, field="address", nonzero=True).hex()


def normalize_origin(value: str) -> str:
    """MPPI HTTPS origin: ASCII DNS/IP host, optional port, optional final slash.

    Unicode DNS names must be provided in their explicit ASCII A-label form.
    Validate before urlsplit: it otherwise silently removes some control bytes.
    """
    if (
        type(value) is not str
        or not value
        or len(value) > 2048
        or any(ord(c) <= 32 or ord(c) >= 127 for c in value)
        or any(c in value for c in "\\%@?#")
    ):
        raise _invalid("public origin is not a valid HTTPS origin")
    try:
        url = urlsplit(value)
        host, port = url.hostname, url.port
    except ValueError:
        raise _invalid("public origin has an invalid host or port") from None
    if url.scheme != "https" or not host or url.path not in ("", "/"):
        raise _invalid("public origin requires HTTPS, a host and no path prefix")
    if url.netloc.endswith(":") or port == 0:
        raise _invalid("public origin has an invalid port")
    if ":" in host:
        try:
            host = "[" + ipaddress.IPv6Address(host).compressed + "]"
        except ValueError:
            raise _invalid("public origin has an invalid IPv6 host") from None
    elif len(host) > 253 or any(not _LABEL.fullmatch(part) for part in host.split(".")):
        raise _invalid("public origin has an invalid DNS host")
    return "https://" + host + (f":{port}" if port not in (None, 443) else "")


def origin_from_discover(uri: str) -> str:
    if type(uri) is not str or not uri.endswith("/discover"):
        raise _invalid("registry agentURI must identify /discover")
    origin = normalize_origin(uri[: -len("/discover")])
    if urlsplit(uri).path != "/discover":
        raise _invalid("registry agentURI must identify exactly /discover")
    return origin


def parse_document(raw: bytes, *, max_bytes: int = MAX_DOCUMENT_BYTES) -> dict[str, object]:
    if type(max_bytes) is not int or not 0 < max_bytes <= MAX_DOCUMENT_BYTES:
        raise _invalid("document byte limit must be positive and at most 1 MiB")
    return _parse_document(raw, max_bytes=max_bytes)


def _parse_document(raw: bytes, *, max_bytes: int) -> dict[str, object]:
    """Strict object decoding with a bound selected by the calling boundary."""
    result = _parse_json(raw, max_bytes=max_bytes)
    if not isinstance(result, dict):
        raise _invalid("document must be a JSON object")
    return cast(dict[str, object], result)


def _parse_json(raw: bytes, *, max_bytes: int) -> object:
    """Strict JSON decoding: unique keys, no constants, bounded size and depth."""
    if type(raw) is not bytes:
        raise _invalid("document must be bytes")
    if len(raw) > max_bytes:
        raise ValidationError(ValidationCode.LIMIT_EXCEEDED, "document exceeds byte limit")
    _check_depth(raw)
    try:
        result = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
    except (UnicodeError, ValueError, RecursionError) as error:
        if isinstance(error, ValidationError):
            raise
        raise _invalid("document must contain valid UTF-8 JSON") from None
    return cast(object, result)


@dataclass(frozen=True, slots=True)
class ProviderIdentity:
    observation: ChainObservation
    registry_address: bytes
    provider_id: int
    owner: bytes
    recipient: bytes
    agent_uri: str
    catalog: Catalog | None

    def __post_init__(self) -> None:
        if not isinstance(self.observation, ChainObservation):
            raise _invalid("identity requires a chain observation")
        for field in ("registry_address", "owner", "recipient"):
            fixed_bytes(getattr(self, field), 20, field=field, nonzero=True)
        uint256(self.provider_id, field="provider_id")
        origin_from_discover(self.agent_uri)
        if self.catalog is not None and (
            not isinstance(self.catalog, Catalog)
            or parse_catalog(self.catalog.canonical_bytes) != self.catalog
        ):
            raise _invalid("identity catalog must match its exact canonical bytes")


def verify_registration(
    raw: bytes, identity: ProviderIdentity, *, expected_origin: str | None = None
) -> str:
    """Return the bound origin; discovery data can never replace chain identity."""
    origin = origin_from_discover(identity.agent_uri)
    if expected_origin is not None and normalize_origin(expected_origin) != origin:
        raise _invalid("selected origin differs from the current registry URI")
    document = parse_document(raw)
    if (
        document.get("type") != REGISTRATION_TYPE
        or type(document.get("name")) is not str
        or not document["name"]
        or type(document.get("description")) is not str
        or document.get("active") is not True
        or document.get("x402Support") is not False
    ):
        raise _invalid("registration fields do not match the MPPI profile")
    registrations = document.get("registrations")
    services = document.get("services")
    if not isinstance(registrations, list) or not isinstance(services, list):
        raise _invalid("registration requires registrations and services arrays")
    matched = 0
    for entry in registrations:
        if not isinstance(entry, dict) or type(entry.get("agentId")) is not int:
            raise _invalid("registration identity must use an exact integer agentId")
        uint256(entry["agentId"], field="agentId")
        registry = entry.get("agentRegistry")
        if type(registry) is not str:
            raise _invalid("registration requires an agentRegistry")
        parts = registry.split(":")
        if len(parts) != 3 or parts[0] != "eip155" or not re.fullmatch(r"[1-9][0-9]*", parts[1]):
            raise _invalid("registration must identify an EVM registry")
        address = parse_address(parts[2])
        if int(parts[1]) == identity.observation.chain_id and address == identity.registry_address:
            if entry["agentId"] != identity.provider_id:
                raise _invalid("registration conflicts with the selected registry identity")
            matched += 1
    if matched != 1:
        raise _invalid("registration must contain exactly one selected registry identity")
    matching_services = []
    for service in services:
        if (
            not isinstance(service, dict)
            or type(service.get("name")) is not str
            or not service["name"]
        ):
            raise _invalid("service must have a nonempty name")
        if type(service.get("endpoint")) is not str or not service["endpoint"]:
            raise _invalid("service must have an endpoint")
        if service["name"] == "mppi":
            matching_services.append(service)
    if len(matching_services) != 1:
        raise _invalid("registration must contain exactly one MPPI service")
    service = matching_services[0]
    if (
        service.get("version") != "1"
        or normalize_origin(service["endpoint"]) != origin
        or service.get("models") != origin + "/models"
    ):
        raise _invalid("MPPI service endpoints or version do not match the selected origin")
    return origin


def verify_models(raw: bytes, catalog: Catalog) -> tuple[str, ...]:
    """Validate advisory cards; return ids, never prices taken from HTTP."""
    document = parse_document(raw)
    cards = document.get("models")
    if not isinstance(cards, list):
        raise _invalid("models document requires a models array")
    accepted = {row.model for row in catalog.rows}
    found: list[str] = []
    for card in cards:
        if not isinstance(card, dict):
            raise _invalid("model card must be an object")
        model = card.get("model")
        if type(model) is not str or model not in accepted or model in found:
            raise _invalid("model cards must uniquely identify current catalog entries")
        if type(card.get("description")) is not str:
            raise _invalid("model description must be a string")
        context = uint64(
            cast(int, card.get("context_window")), field="context_window", positive=True
        )
        output = uint64(cast(int, card.get("max_output")), field="max_output", positive=True)
        modalities = card.get("modalities")
        if (
            output > context
            or not isinstance(modalities, list)
            or not modalities
            or any(
                type(item) is not str or item not in ("text", "image", "audio", "video")
                for item in modalities
            )
            or len(set(modalities)) != len(modalities)
            or any(
                type(card.get(flag)) is not bool
                for flag in ("embeddings", "streaming", "tools", "structured_output")
            )
        ):
            raise _invalid("model capabilities are invalid")
        found.append(model)
    return tuple(found)
