# Shared conformance fixtures

These deterministic files are consumed by the package tests. Integer strings
keep test values exact across JSON implementations. Signing addresses and keys
are synthetic test material and must never hold funds.

| File | Checks |
| --- | --- |
| `catalog.json` | Accepted canonical bytes and Keccak-256; rejected shape, spelling, ordering and range cases |
| `amounts.json` | Funding ceiling, cumulative usage floor, aggregate coverage, top-up bounds and remaining-token guidance |
| `lifecycle.json` | Provider-report consistency, reservation consumption, definitive final accounting and duplicates |
| `signing.json` | Descriptor-derived session id, EIP-712 domain, all four MPPI proofs and replay boundaries |
| `schema.json` | Fixture format for catalog, amount and lifecycle suites |

The [protocol profile](../docs/protocol.md) defines the rules. Python and Rust
consume the amount, catalog and report-transition fixtures. Solidity, Rust and an independent
Python EIP-712 implementation check signing fixtures. The
[local checks](../docs/build.md) also verify the generated protobuf schema.

These fixtures establish encoding and pure calculation conformance. The Python
runtime has separate session opening, voucher-driven admission, capture,
reconnect, close and wallet integration tests. Rust runtime acceptance follows
its service and chain implementation.
