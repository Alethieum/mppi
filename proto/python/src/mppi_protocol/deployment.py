"""Explicit non-secret deployment profiles; no audit address is a public default.

Parsing checks the shape of caller-supplied pins. ChainReader verifies their
observed code/slots/domain; the caller establishes the manifest's provenance.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .chain import CodePin, DeploymentProfile
from .discovery import MAX_DOCUMENT_BYTES, address_hex, parse_address, parse_document
from .errors import ValidationCode, ValidationError
from .types import Domain, fixed_bytes


def _invalid() -> ValidationError:
    return ValidationError(ValidationCode.INVALID_VALUE, "invalid deployment profile")


def _pin(value: object) -> CodePin:
    if type(value) is not dict or set(value) != {"address", "code_hash"}:
        raise _invalid()
    raw = value["code_hash"]
    if type(raw) is not str or len(raw) != 66 or not raw.startswith("0x"):
        raise _invalid()
    try:
        code_hash = fixed_bytes(bytes.fromhex(raw[2:]), 32, field="code_hash", nonzero=True)
        address = parse_address(value["address"])
    except (ValueError, TypeError, ValidationError):
        raise _invalid() from None
    return CodePin(address, code_hash)


def profile_from_mapping(value: object) -> DeploymentProfile:
    if type(value) is not dict or set(value) != {
        "version",
        "chain_id",
        "proxy",
        "implementation",
        "registry",
        "registry_implementation",
    }:
        raise _invalid()
    if type(value["version"]) is not int or value["version"] != 1:
        raise _invalid()
    proxy, implementation = _pin(value["proxy"]), _pin(value["implementation"])
    registry, registry_implementation = (
        _pin(value["registry"]),
        _pin(value["registry_implementation"]),
    )
    return DeploymentProfile(
        Domain(value["chain_id"], proxy.address),
        registry.address,
        proxy,
        implementation,
        registry,
        registry_implementation,
    )


def profile_to_mapping(profile: DeploymentProfile) -> dict[str, Any]:
    def pin(value: CodePin) -> dict[str, str]:
        return {"address": address_hex(value.address), "code_hash": "0x" + value.code_hash.hex()}

    return {
        "version": 1,
        "chain_id": profile.domain.chain_id,
        "proxy": pin(profile.proxy),
        "implementation": pin(profile.implementation),
        "registry": pin(profile.registry),
        "registry_implementation": pin(profile.registry_implementation),
    }


def load_profile(path: Path) -> DeploymentProfile:
    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(MAX_DOCUMENT_BYTES + 1)
    except OSError:
        raise _invalid() from None
    return profile_from_mapping(parse_document(raw))
