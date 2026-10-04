import json
from pathlib import Path
from unittest.mock import patch

import pytest
from mppi_protocol import CatalogRow, Rates, ValidationError, parse_catalog, publish_catalog

VECTORS = Path(__file__).resolve().parents[3] / "testvectors"


def test_shared_catalog_vectors() -> None:
    fixture = json.loads((VECTORS / "catalog.json").read_text())
    assert fixture["schemaVersion"] == 1
    for case in fixture["accept"]:
        raw = case["utf8"].encode("utf-8")
        catalog = parse_catalog(raw)
        assert catalog.canonical_bytes == raw
        assert "0x" + catalog.rate_card_hash.hex() == case["keccak256"], case["name"]
        assert publish_catalog(reversed(catalog.rows)) == catalog
    for case in fixture["reject"]:
        with pytest.raises(ValidationError) as caught:
            parse_catalog(bytes.fromhex(case["rawHex"]))
        assert caught.value.code.value == case["error"], case["name"]


def test_publication_sorts_but_receipt_never_repairs() -> None:
    rows = [CatalogRow("z/model", Rates(1, 2)), CatalogRow("a/model", Rates(1, 2))]
    published = publish_catalog(rows)
    assert [r.model for r in published.rows] == ["a/model", "z/model"]
    reversed_raw = b"[" + b",".join(publish_catalog([r]).canonical_bytes[1:-1] for r in rows) + b"]"
    with pytest.raises(ValidationError):
        parse_catalog(reversed_raw)


def test_limits_precede_decode_and_depth_allocation() -> None:
    with pytest.raises(ValidationError) as caught:
        parse_catalog(b" " * 9, max_bytes=8)
    assert caught.value.code.value == "LIMIT_EXCEEDED"
    with pytest.raises(ValidationError) as caught:
        parse_catalog(b"[" * 129 + b"]" * 129)
    assert caught.value.code.value == "LIMIT_EXCEEDED"
    with pytest.raises(ValidationError):
        parse_catalog(b"[]", max_bytes=True)


def test_absent_and_free_cache_have_distinct_commitments() -> None:
    absent = publish_catalog([CatalogRow("x/y", Rates(1, 2))])
    explicit = publish_catalog([CatalogRow("x/y", Rates(1, 2, 0))])
    assert absent.rate_card_hash != explicit.rate_card_hash
    assert absent.rows[0].rates.cache is None
    assert explicit.rows[0].rates.cache == 0


def test_oversized_publication_row_rejects_before_encoding() -> None:
    row = CatalogRow("a" * (1 << 20) + "/b", Rates(1, 1))
    with patch("mppi_protocol.catalog._encode") as encoder:
        with pytest.raises(ValidationError) as caught:
            publish_catalog((row,))
        assert caught.value.code.value == "LIMIT_EXCEEDED"
        encoder.assert_not_called()


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        (
            (CatalogRow("x/y", Rates(0, (1 << 64) - 1)),),
            b'[{"input":"0","model":"x/y","output":"18446744073709551615"}]',
        ),
        (
            (CatalogRow("x/y", Rates((1 << 64) - 1, 1, 0)),),
            b'[{"cache":"0","input":"18446744073709551615","model":"x/y","output":"1"}]',
        ),
        (
            (CatalogRow("x/y", Rates(1, 0, (1 << 64) - 1)),),
            b'[{"cache":"18446744073709551615","input":"1","model":"x/y","output":"0"}]',
        ),
        (
            (
                CatalogRow("z/model", Rates(3, 4, 2)),
                CatalogRow("a/model", Rates(0, 1)),
                CatalogRow("m/model", Rates(2, 3, 0)),
            ),
            b'[{"input":"0","model":"a/model","output":"1"},'
            b'{"cache":"0","input":"2","model":"m/model","output":"3"},'
            b'{"cache":"2","input":"3","model":"z/model","output":"4"}]',
        ),
    ],
)
def test_publication_exact_byte_boundary(rows: tuple[CatalogRow, ...], expected: bytes) -> None:
    published = publish_catalog(rows, max_bytes=len(expected))
    assert published.canonical_bytes == expected
    assert parse_catalog(expected, max_bytes=len(expected)) == published
    with patch("mppi_protocol.catalog._encode") as encoder:
        with pytest.raises(ValidationError) as caught:
            publish_catalog(rows, max_bytes=len(expected) - 1)
        assert caught.value.code.value == "LIMIT_EXCEEDED"
        encoder.assert_not_called()
