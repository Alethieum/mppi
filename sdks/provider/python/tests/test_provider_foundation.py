import pytest
from mppi_provider import CatalogRow, ProviderRuns, Rates, publish_catalog


def test_role_package_exposes_canonical_catalog() -> None:
    catalog = publish_catalog([CatalogRow("x/y", Rates(0, 1))])
    assert catalog.canonical_bytes == b'[{"input":"0","model":"x/y","output":"1"}]'
    with pytest.raises(TypeError):
        ProviderRuns()  # type: ignore[misc]
