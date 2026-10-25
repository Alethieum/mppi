# MPPI Python protocol support

Internal shared distribution for the agent and provider SDKs. It contains the
generated `mppi.v1` wire bindings and `mppi_protocol` exact payment primitives.
It is not another role SDK.

The implementation validates catalogs and report transitions, computes exact
amounts, signs and verifies fixed-session proofs, prepares contract calls and
checks canonical receipt evidence. Version `0.1.1` also supplies shared discovery
validation and the Circle Agent Wallet adapter used by both Python roles.
Version `0.1.2`, used by `mppi-agent` 0.1.3, adds bounded repeats of transient
read-only JSON-RPC calls, lag-tolerant chain following
(`ChainLagError`, `last_refresh_failure`), `HookError.detail` and
`TransportError.transient`, and accepts checkpoint format 2; every change is
additive.
These building blocks do not authorize inference or store provider state.

See [the SDK interfaces](../../docs/sdk-interfaces.md) for the implemented surface
and runtime integration boundaries.

The [runtime reference](../../docs/python-runtime.md) describes the implemented
trust boundaries, dependency adapters and evidence limitations.

## License

Copyright 2026 MPPI contributors. Licensed under the [Apache License, Version 2.0](LICENSE).
