"""MPPI's restricted ASCII catalog profile, not a general JCS serializer."""

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import cast

from Crypto.Hash import keccak

from .amounts import Rates, decimal_uint64
from .errors import ValidationCode, ValidationError

DEFAULT_CATALOG_BYTES = 1 << 20
DEFAULT_JSON_DEPTH = 128
_COMPONENT = re.compile(r"[A-Za-z0-9_](?:[A-Za-z0-9_.-]*[A-Za-z0-9_])?\Z", re.ASCII)


def validate_model(model: str) -> str:
    if type(model) is not str or model.count("/") != 1:
        raise ValidationError(ValidationCode.INVALID_VALUE, "model must be namespace/repository")
    namespace, repository = model.split("/")
    if (
        not _COMPONENT.fullmatch(namespace)
        or not _COMPONENT.fullmatch(repository)
        or not 1 <= len(repository) <= 96
        or "--" in model
        or ".." in model
        or model.endswith(".git")
    ):
        raise ValidationError(
            ValidationCode.INVALID_VALUE, "model does not match the MPPI identifier profile"
        )
    return model


def _check_depth(raw: bytes, maximum: int = DEFAULT_JSON_DEPTH) -> None:
    depth = 0
    quoted = escaped = False
    for byte in raw:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            depth += 1
            if depth > maximum:
                raise ValidationError(ValidationCode.LIMIT_EXCEEDED, "JSON nesting limit exceeded")
        elif byte in (93, 125):
            depth -= 1


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(ValidationCode.DUPLICATE_KEY, "duplicate decoded JSON property")
        result[key] = value
    return result


def _reject_constant(_: str) -> object:
    raise ValidationError(ValidationCode.INVALID_VALUE, "non-JSON numeric constant")


@dataclass(frozen=True, slots=True)
class CatalogRow:
    model: str
    rates: Rates

    def __post_init__(self) -> None:
        validate_model(self.model)
        if not isinstance(self.rates, Rates):
            raise ValidationError(ValidationCode.INVALID_VALUE, "rates must be Rates")


@dataclass(frozen=True, slots=True)
class Catalog:
    rows: tuple[CatalogRow, ...]
    canonical_bytes: bytes
    rate_card_hash: bytes

    def rates_for(self, model: str) -> Rates:
        for row in self.rows:
            if row.model == model:
                return row.rates
        raise KeyError(model)


def _encode(rows: tuple[CatalogRow, ...]) -> bytes:
    records: list[dict[str, str]] = []
    for row in rows:
        record = {
            "input": str(row.rates.input),
            "model": row.model,
            "output": str(row.rates.output),
        }
        if row.rates.cache is not None:
            record["cache"] = str(row.rates.cache)
        records.append(record)
    # Every accepted key/value is ASCII and every price is a string. Thus this
    # schema has neither JCS binary64 serialization nor UTF-16 sorting ambiguity.
    return json.dumps(records, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )


def _limit(max_bytes: int) -> None:
    if type(max_bytes) is not int or not 0 < max_bytes <= DEFAULT_CATALOG_BYTES:
        raise ValidationError(
            ValidationCode.INVALID_VALUE, "catalog limit must be in 1..1048576 bytes"
        )


def publish_catalog(
    rows: Iterable[CatalogRow], *, max_bytes: int = DEFAULT_CATALOG_BYTES
) -> Catalog:
    """Canonicalize provider declarations; receivers use parse_catalog instead."""
    _limit(max_bytes)
    bounded: list[CatalogRow] = []
    encoded_size = 2  # Opening and closing array delimiters.
    for row in rows:
        if not isinstance(row, CatalogRow):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "catalog declarations must be CatalogRow values"
            )
        # Validated models and decimal rates are ASCII and require no JSON
        # escapes. Count their exact bytes before allocating any encoded row.
        row_size = (
            len('{"input":"","model":"","output":""}')
            + len(row.model)
            + len(str(row.rates.input))
            + len(str(row.rates.output))
        )
        if row.rates.cache is not None:
            row_size += len('"cache":"",') + len(str(row.rates.cache))
        encoded_size += row_size + bool(bounded)
        if encoded_size > max_bytes:
            raise ValidationError(ValidationCode.LIMIT_EXCEEDED, "catalog byte limit exceeded")
        bounded.append(row)
    ordered = tuple(sorted(bounded, key=lambda row: row.model.encode("ascii")))
    if not ordered or len({row.model for row in ordered}) != len(ordered):
        raise ValidationError(
            ValidationCode.INVALID_VALUE, "catalog must contain unique nonempty model rows"
        )
    raw = _encode(ordered)
    if len(raw) > max_bytes:
        raise ValidationError(ValidationCode.LIMIT_EXCEEDED, "catalog byte limit exceeded")
    return Catalog(ordered, raw, keccak.new(digest_bits=256, data=raw).digest())


def parse_catalog(raw: bytes, *, max_bytes: int = DEFAULT_CATALOG_BYTES) -> Catalog:
    """Validate exact received bytes; never repair order, spelling or encoding."""
    _limit(max_bytes)
    if type(raw) is not bytes:
        raise ValidationError(ValidationCode.INVALID_VALUE, "catalog must be bytes")
    if len(raw) > max_bytes:
        raise ValidationError(ValidationCode.LIMIT_EXCEEDED, "catalog byte limit exceeded")
    _check_depth(raw)
    try:
        decoded = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
    except ValidationError:
        raise
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValidationError(
            ValidationCode.INVALID_VALUE, "catalog is not valid UTF-8 JSON"
        ) from exc
    if type(decoded) is not list or not decoded:
        raise ValidationError(ValidationCode.INVALID_VALUE, "catalog must be a nonempty array")
    rows: list[CatalogRow] = []
    previous: bytes | None = None
    for item in decoded:
        if type(item) is not dict or set(item) not in (
            {"model", "input", "output"},
            {"model", "input", "output", "cache"},
        ):
            raise ValidationError(ValidationCode.INVALID_VALUE, "invalid catalog row fields")
        row = cast(dict[str, object], item)
        model = validate_model(cast(str, row["model"]))
        model_bytes = model.encode("ascii")
        if previous is not None and model_bytes <= previous:
            raise ValidationError(
                ValidationCode.NON_CANONICAL, "catalog model order must be strictly increasing"
            )
        previous = model_bytes
        rates = Rates(
            input=decimal_uint64(cast(str, row["input"]), field="input"),
            output=decimal_uint64(cast(str, row["output"]), field="output"),
            cache=decimal_uint64(cast(str, row["cache"]), field="cache")
            if "cache" in row
            else None,
        )
        rows.append(CatalogRow(model, rates))
    checked_rows = tuple(rows)
    if raw != _encode(checked_rows):
        raise ValidationError(
            ValidationCode.NON_CANONICAL, "catalog bytes differ from canonical encoding"
        )
    return Catalog(checked_rows, raw, keccak.new(digest_bits=256, data=raw).digest())
