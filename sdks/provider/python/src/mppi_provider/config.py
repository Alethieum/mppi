"""Validated provider declarations; loading configuration performs no publication.

TOML, explicit overrides and direct construction converge on these immutable
records. They contain references to credentials, never resolved secret values.
"""

from __future__ import annotations

import json
import math
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import NoReturn, cast
from urllib.parse import urlsplit
from uuid import UUID

from mppi_protocol import Catalog, CatalogRow, Rates, publish_catalog
from mppi_protocol.amounts import decimal_uint64, uint64
from mppi_protocol.catalog import validate_model
from mppi_protocol.discovery import (
    MAX_DOCUMENT_BYTES,
    normalize_origin,
    parse_address,
    parse_document,
)
from mppi_protocol.errors import ValidationCode, ValidationError
from mppi_protocol.types import fixed_bytes, uint256

MAX_CONFIG_BYTES = 1 << 20
MAX_DURATION_MS = ((1 << 63) - 1) // 1_000_000
_ENV = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z", re.ASCII)
_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)\Z", re.ASCII)
_SECTIONS = {
    "provider",
    "identity",
    "chain",
    "upstream",
    "models",
    "payment",
    "settlement",
    "listen",
}


class ConfigError(ValidationError):
    """A field-local configuration error which does not include rejected values."""

    def __init__(self, field: str, message: str, *, source: Path | None = None) -> None:
        self.field = field
        self.source = source
        self.reason = message
        super().__init__(
            ValidationCode.INVALID_VALUE, f"{source or 'configuration'}:{field}: {message}"
        )


def _fail(name: str, message: str = "invalid value") -> NoReturn:
    raise ConfigError(name, message)


def _text(value: object, name: str, *, nonempty: bool = False) -> str:
    if type(value) is not str or (nonempty and not value) or len(value) > MAX_DOCUMENT_BYTES:
        _fail(name, "expected a bounded string" + (" with content" if nonempty else ""))
    try:
        if len(value.encode("utf-8")) > MAX_DOCUMENT_BYTES:
            _fail(name, "string exceeds document limit")
    except UnicodeError:
        _fail(name, "invalid Unicode")
    return value


def _positive(value: object, name: str, *, duration: bool = False) -> int:
    try:
        result = uint64(cast(int, value), field=name, positive=True)
    except ValidationError:
        _fail(name, "expected a positive integer")
    if duration and result > MAX_DURATION_MS:
        _fail(name, "duration exceeds signed-64-bit nanosecond range")
    return result


def _amount(value: object, name: str, *, positive: bool = False) -> int:
    try:
        result = (
            decimal_uint64(value, field=name)
            if type(value) is str
            else uint64(cast(int, value), field=name)
        )
    except ValidationError:
        _fail(name, "expected an exact uint64 amount")
    if positive and result == 0:
        _fail(name, "amount must be positive")
    return result


def _address(value: object, name: str) -> bytes:
    try:
        return (
            parse_address(value)
            if type(value) is str
            else fixed_bytes(cast(bytes, value), 20, field=name, nonzero=True)
        )
    except ValidationError:
        _fail(name, "expected a nonzero EVM address")


def _object(value: object, name: str, allowed: set[str], required: set[str]) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(type(key) is not str for key in value):
        _fail(name, "expected a table")
    if set(value) - allowed:
        _fail(name, "unexpected field")
    for key in sorted(required - set(value)):
        _fail(f"{name}.{key}", "missing field")
    return dict(value)


def _http_url(value: object, name: str) -> str:
    value = _text(value, name, nonempty=True)
    if any(ord(c) <= 32 or ord(c) == 127 for c in value) or any(c in value for c in "\\?#"):
        _fail(name, "invalid HTTP URL")
    try:
        parts = urlsplit(value)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            _fail(name, "expected an absolute HTTP/HTTPS URL")
        normalize_origin("https://" + parts.netloc)
    except (ValueError, ValidationError):
        _fail(name, "invalid HTTP URL authority")
    return value.rstrip("/")


def _json_bytes(value: dict[str, object], name: str) -> bytes:
    """Bound declarations before encoding; preserve JSON extension fields."""
    remaining = MAX_DOCUMENT_BYTES

    def visit(item: object, depth: int) -> None:
        nonlocal remaining
        remaining -= 1
        if remaining < 0 or depth > 128:
            _fail(name, "JSON document limit exceeded")
        if type(item) is str:
            remaining -= len(_text(item, name))
        elif type(item) is dict:
            for key, child in cast(dict[object, object], item).items():
                visit(_text(key, name), depth)
                visit(child, depth + 1 if isinstance(child, (dict, list)) else depth)
        elif type(item) is list:
            for child in cast(list[object], item):
                visit(child, depth + 1 if isinstance(child, (dict, list)) else depth)
        elif type(item) is int:
            if abs(item).bit_length() > MAX_DOCUMENT_BYTES:
                _fail(name, "JSON number limit exceeded")
        elif type(item) is float:
            if not math.isfinite(item):
                _fail(name, "non-finite JSON number")
        elif item is not None and type(item) is not bool:
            _fail(name, "expected a JSON value")
        if remaining < 0:
            _fail(name, "JSON document limit exceeded")

    visit(value, 1)
    chunks: list[bytes] = []
    size = 0
    try:
        encoder = json.JSONEncoder(
            ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
        )
        for chunk in encoder.iterencode(value):
            if len(chunk) > MAX_DOCUMENT_BYTES:
                _fail(name, "JSON document limit exceeded")
            encoded = chunk.encode("utf-8")
            size += len(encoded)
            if size > MAX_DOCUMENT_BYTES:
                _fail(name, "JSON document limit exceeded")
            chunks.append(encoded)
        raw = b"".join(chunks)
        parse_document(raw)
    except (ValueError, TypeError, RecursionError):
        _fail(name, "invalid or oversized JSON document")
    return raw


@dataclass(frozen=True, slots=True)
class AdditionalService:
    """Immutable JSON snapshot; arbitrary valid service extensions are retained."""

    document: bytes = field(repr=False)

    def __post_init__(self) -> None:
        try:
            value = parse_document(self.document)
            name = _text(value.get("name"), "provider.services.name", nonempty=True)
            _text(value.get("endpoint"), "provider.services.endpoint", nonempty=True)
            if name == "mppi":
                _fail("provider.services", "MPPI service is derived by the SDK")
            _json_bytes(value, "provider.services")
        except ValidationError:
            _fail("provider.services", "invalid additional service")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> AdditionalService:
        if not isinstance(value, Mapping) or any(type(key) is not str for key in value):
            _fail("provider.services", "expected service object")
        return cls(_json_bytes(dict(value), "provider.services"))

    def as_object(self) -> dict[str, object]:
        return parse_document(self.document)


@dataclass(frozen=True, slots=True)
class ProviderDetails:
    name: str
    description: str
    public_origin: str
    recipient: bytes
    image: str | None = None
    services: tuple[AdditionalService, ...] = ()

    def __post_init__(self) -> None:
        _text(self.name, "provider.name", nonempty=True)
        _text(self.description, "provider.description")
        try:
            object.__setattr__(self, "public_origin", normalize_origin(self.public_origin))
        except ValidationError:
            _fail("provider.public_origin", "expected an HTTPS origin")
        object.__setattr__(self, "recipient", _address(self.recipient, "provider.recipient"))
        if self.image is not None:
            image = _text(self.image, "provider.image", nonempty=True)
            try:
                if not urlsplit(image).scheme or any(ord(c) <= 32 for c in image):
                    _fail("provider.image", "expected a URI")
            except ValueError:
                _fail("provider.image", "expected a URI")
        if type(self.services) is not tuple or any(
            not isinstance(s, AdditionalService) for s in self.services
        ):
            _fail("provider.services", "expected additional service records")


@dataclass(frozen=True, slots=True)
class ProviderIdentityConfig:
    provider_id: int
    wallet_address: bytes
    wallet_id: str | None = None

    def __post_init__(self) -> None:
        try:
            uint256(self.provider_id, field="provider_id")
        except ValidationError:
            _fail("identity.provider_id", "expected uint256")
        object.__setattr__(
            self, "wallet_address", _address(self.wallet_address, "identity.wallet_address")
        )
        if self.wallet_id is not None:
            try:
                UUID(_text(self.wallet_id, "identity.wallet_id", nonempty=True))
            except ValueError:
                _fail("identity.wallet_id", "expected a wallet UUID")


@dataclass(frozen=True, slots=True)
class ChainConfig:
    max_age_ms: int
    network: str = "mainnet"
    rpc: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.network not in ("mainnet", "testnet"):
            _fail("chain.network", "expected mainnet or testnet")
        _positive(self.max_age_ms, "chain.max_age_ms", duration=True)
        if self.rpc is not None:
            _http_url(self.rpc, "chain.rpc")

    @property
    def chain_id(self) -> int:
        return 5042 if self.network == "mainnet" else 5042002

    @property
    def rpc_url(self) -> str:
        return self.rpc if self.rpc is not None else f"https://rpc.{self.network}.arc.io"


@dataclass(frozen=True, slots=True)
class UpstreamConfig:
    url: str = field(repr=False)
    api_key_env: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "url", _http_url(self.url, "upstream.url"))
        if self.api_key_env is not None and (
            type(self.api_key_env) is not str or not _ENV.fullmatch(self.api_key_env)
        ):
            _fail("upstream.api_key_env", "expected an environment variable name")

    @property
    def completions_url(self) -> str:
        return self.url + "/chat/completions"


@dataclass(frozen=True, slots=True)
class ModelConfig:
    model: str
    description: str
    context_window: int
    max_output: int
    modalities: tuple[str, ...]
    embeddings: bool
    streaming: bool
    tools: bool
    structured_output: bool
    rates: Rates

    def __post_init__(self) -> None:
        try:
            validate_model(self.model)
        except ValidationError:
            _fail("models.model", "invalid model identifier")
        _text(self.description, "models.description")
        _positive(self.context_window, "models.context_window")
        _positive(self.max_output, "models.max_output")
        if self.max_output > self.context_window:
            _fail("models.max_output", "exceeds context window")
        if (
            type(self.modalities) is not tuple
            or not self.modalities
            or any(
                type(m) is not str or m not in ("text", "image", "audio", "video")
                for m in self.modalities
            )
            or len(set(self.modalities)) != len(self.modalities)
        ):
            _fail("models.modalities", "expected unique supported modalities")
        if any(
            type(getattr(self, name)) is not bool
            for name in ("embeddings", "streaming", "tools", "structured_output")
        ):
            _fail("models", "capability flags must be booleans")
        if not isinstance(self.rates, Rates):
            _fail("models.rates", "expected checked Rates")

    def card(self) -> dict[str, object]:
        return {
            "model": self.model,
            "description": self.description,
            "context_window": self.context_window,
            "max_output": self.max_output,
            "modalities": list(self.modalities),
            "embeddings": self.embeddings,
            "streaming": self.streaming,
            "tools": self.tools,
            "structured_output": self.structured_output,
        }


@dataclass(frozen=True, slots=True)
class PaymentConfig:
    collection_margin_ms: int
    authorization_wait_ms: int
    output_buffer_tokens: int = 1000
    suggest_session_tokens: int = 1_000_000

    def __post_init__(self) -> None:
        for name in ("collection_margin_ms", "authorization_wait_ms"):
            _positive(getattr(self, name), "payment." + name, duration=True)
        for name in ("output_buffer_tokens", "suggest_session_tokens"):
            _positive(getattr(self, name), "payment." + name)


@dataclass(frozen=True, slots=True)
class SettlementConfig:
    interval_ms: int
    amount: int | None = None
    units: int | None = None

    def __post_init__(self) -> None:
        _positive(self.interval_ms, "settlement.interval_ms", duration=True)
        if self.amount is not None:
            _positive(self.amount, "settlement.amount")
        if self.units is not None:
            _positive(self.units, "settlement.units")


@dataclass(frozen=True, slots=True)
class ListenerConfig:
    bind: str
    tls_mode: str
    tls_cert: Path | None = None
    tls_key: Path | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        bind = _text(self.bind, "listen.bind", nonempty=True)
        try:
            normalize_origin("https://" + bind)
            if urlsplit("https://" + bind).port is None:
                _fail("listen.bind", "expected host and port")
        except ValueError:
            _fail("listen.bind", "invalid listener address")
        if self.tls_mode == "direct":
            if not isinstance(self.tls_cert, Path) or not isinstance(self.tls_key, Path):
                _fail("listen", "direct TLS requires certificate and key paths")
        elif self.tls_mode == "ingress":
            if self.tls_cert is not None or self.tls_key is not None:
                _fail("listen", "ingress TLS cannot specify certificate paths")
        else:
            _fail("listen.tls_mode", "expected direct or ingress")


@dataclass(frozen=True, slots=True)
class ProviderConfig:
    provider: ProviderDetails
    identity: ProviderIdentityConfig
    chain: ChainConfig
    upstream: UpstreamConfig
    models: tuple[ModelConfig, ...]
    payment: PaymentConfig
    settlement: SettlementConfig
    listen: ListenerConfig | None = None
    _catalog: Catalog = field(init=False, repr=False)

    def __post_init__(self) -> None:
        for name, expected in (
            ("provider", ProviderDetails),
            ("identity", ProviderIdentityConfig),
            ("chain", ChainConfig),
            ("upstream", UpstreamConfig),
            ("payment", PaymentConfig),
            ("settlement", SettlementConfig),
        ):
            if not isinstance(getattr(self, name), expected):
                _fail(name, "expected validated configuration record")
        if self.provider.recipient == self.identity.wallet_address:
            _fail("provider.recipient", "must differ from the operator wallet")
        if type(self.models) is not tuple or any(
            not isinstance(m, ModelConfig) for m in self.models
        ):
            _fail("models", "expected model records")
        try:
            object.__setattr__(
                self, "_catalog", publish_catalog(CatalogRow(m.model, m.rates) for m in self.models)
            )
        except ValidationError:
            _fail("models", "invalid, duplicate or oversized catalog")
        if self.listen is not None and not isinstance(self.listen, ListenerConfig):
            _fail("listen", "expected listener configuration")

    @property
    def catalog(self) -> Catalog:
        """Desired publication bytes; this property performs no registry write."""
        return self._catalog

    @classmethod
    def from_mapping(
        cls, values: Mapping[str, object], *, overrides: Mapping[str, object] | None = None
    ) -> ProviderConfig:
        source = _object(values, "configuration", _SECTIONS, set())
        if overrides is not None:
            changes = _object(overrides, "overrides", _SECTIONS, set())
            for name, value in changes.items():
                if (
                    name != "models"
                    and isinstance(value, Mapping)
                    and isinstance(source.get(name), Mapping)
                ):
                    source[name] = {**cast(Mapping[str, object], source[name]), **value}
                else:
                    source[name] = value
        return _parse_config(source)

    @classmethod
    def from_file(
        cls, path: str | Path = "mppi.toml", *, overrides: Mapping[str, object] | None = None
    ) -> ProviderConfig:
        filename = Path(path).absolute()
        try:
            with filename.open("rb") as stream:
                raw = stream.read(MAX_CONFIG_BYTES + 1)
            if len(raw) > MAX_CONFIG_BYTES:
                _fail("configuration", "TOML byte limit exceeded")
            try:
                values = cast(dict[str, object], tomllib.loads(raw.decode("utf-8")))
            except (ValueError, RecursionError):
                _fail("configuration", "invalid TOML")
            _require_quoted_money(values)
            # File-relative paths are resolved before explicit programmatic
            # overrides, whose relative paths belong to the caller's context.
            listener = values.get("listen")
            if isinstance(listener, dict):
                for name in ("tls_cert", "tls_key"):
                    if name in listener:
                        listener[name] = str(
                            _path(listener[name], "listen." + name, filename.parent)
                        )
            return cls.from_mapping(values, overrides=overrides)
        except ConfigError as error:
            raise ConfigError(error.field, error.reason, source=filename) from None
        except OSError:
            raise ConfigError(
                "configuration", "cannot read configuration file", source=filename
            ) from None


def _path(value: object, name: str, base: Path | None = None) -> Path:
    text = _text(value, name, nonempty=True)
    if "\x00" in text:
        _fail(name, "invalid path")
    path = Path(text)
    return path if path.is_absolute() else (base or Path.cwd()) / path


def _require_quoted_money(values: dict[str, object]) -> None:
    identity = values.get("identity")
    if (
        isinstance(identity, dict)
        and "provider_id" in identity
        and type(identity["provider_id"]) is not str
    ):
        _fail("identity.provider_id", "TOML provider id must be a decimal string")
    settlement = values.get("settlement")
    if (
        isinstance(settlement, dict)
        and "amount" in settlement
        and type(settlement["amount"]) is not str
    ):
        _fail("settlement.amount", "TOML amount must be a decimal string")
    models = values.get("models")
    if isinstance(models, list):
        for model in models:
            if isinstance(model, dict) and isinstance(model.get("rates"), dict):
                for value in model["rates"].values():
                    if type(value) is not str:
                        _fail("models.rates", "TOML rates must be decimal strings")


def _parse_config(values: dict[str, object]) -> ProviderConfig:
    values = _object(values, "configuration", _SECTIONS, _SECTIONS - {"listen"})
    provider = _object(
        values["provider"],
        "provider",
        {"name", "description", "public_origin", "recipient", "image", "services"},
        {"name", "description", "public_origin", "recipient"},
    )
    identity = _object(
        values["identity"],
        "identity",
        {"provider_id", "wallet_address", "wallet_id"},
        {"provider_id", "wallet_address"},
    )
    chain = _object(values["chain"], "chain", {"network", "rpc", "max_age_ms"}, {"max_age_ms"})
    upstream = _object(values["upstream"], "upstream", {"url", "api_key_env"}, {"url"})
    payment = _object(
        values["payment"],
        "payment",
        {
            "collection_margin_ms",
            "authorization_wait_ms",
            "output_buffer_tokens",
            "suggest_session_tokens",
        },
        {"collection_margin_ms", "authorization_wait_ms"},
    )
    settlement = _object(
        values["settlement"], "settlement", {"interval_ms", "amount", "units"}, {"interval_ms"}
    )
    identifier = identity["provider_id"]
    if type(identifier) is str:
        if len(identifier) > 78 or not _DECIMAL.fullmatch(identifier):
            _fail("identity.provider_id", "expected canonical decimal uint256")
        identifier = int(identifier)
    services = provider.get("services", [])
    if type(services) is not list:
        _fail("provider.services", "expected an array")
    models = values["models"]
    if type(models) is not list:
        _fail("models", "expected an array")
    parsed_models = []
    model_fields = {
        "model",
        "description",
        "context_window",
        "max_output",
        "modalities",
        "embeddings",
        "streaming",
        "tools",
        "structured_output",
        "rates",
    }
    for item in models:
        model = _object(item, "models", model_fields, model_fields)
        rates = _object(
            model.pop("rates"), "models.rates", {"input", "output", "cache"}, {"input", "output"}
        )
        modalities = model["modalities"]
        if type(modalities) is not list:
            _fail("models.modalities", "expected an array")
        model["modalities"] = tuple(modalities)
        parsed_models.append(
            ModelConfig(
                model=cast(str, model["model"]),
                description=cast(str, model["description"]),
                context_window=cast(int, model["context_window"]),
                max_output=cast(int, model["max_output"]),
                modalities=cast(tuple[str, ...], model["modalities"]),
                embeddings=cast(bool, model["embeddings"]),
                streaming=cast(bool, model["streaming"]),
                tools=cast(bool, model["tools"]),
                structured_output=cast(bool, model["structured_output"]),
                rates=Rates(
                    input=_amount(rates["input"], "models.rates.input"),
                    output=_amount(rates["output"], "models.rates.output"),
                    cache=_amount(rates["cache"], "models.rates.cache")
                    if "cache" in rates
                    else None,
                ),
            )
        )
    listener = None
    if "listen" in values:
        fields = _object(
            values["listen"],
            "listen",
            {"bind", "tls_mode", "tls_cert", "tls_key"},
            {"bind", "tls_mode"},
        )
        for name in ("tls_cert", "tls_key"):
            if name in fields:
                fields[name] = _path(fields[name], "listen." + name)
        listener = ListenerConfig(**fields)  # type: ignore[arg-type]
    if "amount" in settlement:
        settlement["amount"] = _amount(settlement["amount"], "settlement.amount", positive=True)
    return ProviderConfig(
        provider=ProviderDetails(
            name=cast(str, provider["name"]),
            description=cast(str, provider["description"]),
            public_origin=cast(str, provider["public_origin"]),
            recipient=_address(provider["recipient"], "provider.recipient"),
            image=cast(str | None, provider.get("image")),
            services=tuple(AdditionalService.from_mapping(item) for item in services),
        ),
        identity=ProviderIdentityConfig(
            provider_id=cast(int, identifier),
            wallet_address=_address(identity["wallet_address"], "identity.wallet_address"),
            wallet_id=cast(str | None, identity.get("wallet_id")),
        ),
        chain=ChainConfig(**chain),  # type: ignore[arg-type]
        upstream=UpstreamConfig(**upstream),  # type: ignore[arg-type]
        models=tuple(parsed_models),
        payment=PaymentConfig(**payment),  # type: ignore[arg-type]
        settlement=SettlementConfig(**settlement),  # type: ignore[arg-type]
        listen=listener,
    )
