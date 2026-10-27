import pytest
from mppi_provider import CatalogRow, Hooks, ProviderRuns, Rates, publish_catalog


def test_role_package_exposes_canonical_catalog() -> None:
    catalog = publish_catalog([CatalogRow("x/y", Rates(0, 1))])
    assert catalog.canonical_bytes == b'[{"input":"0","model":"x/y","output":"1"}]'
    with pytest.raises(TypeError):
        ProviderRuns()  # type: ignore[misc]


def test_hooks_keep_the_positional_order_of_0_1_2() -> None:
    """A host that passes callbacks by position, as 0.1.2 declared them, keeps
    each one bound to its own hook; later hooks only follow them."""
    released = (
        "on_challenge",
        "on_session_open",
        "on_voucher",
        "on_meter",
        "on_low_credit",
        "on_drain",
        "on_settle",
        "on_close",
        "on_error",
    )
    callbacks = [lambda record, name=name: name for name in released]
    hooks = Hooks(*callbacks)
    for name, callback in zip(released, callbacks, strict=True):
        assert getattr(hooks, name) is callback, name
    assert hooks.on_session_retired is None and hooks.on_timing is None
    assert Hooks.__dataclass_fields__.keys() - set(released) == {"on_session_retired", "on_timing"}
