# MPPI — Machine Payments Protocol for Inference

An Alethieum research project.

MPPI lets agents authorize inference payments and providers capture reported
usage through funded USDC sessions on Arc. ERC-8004 supplies identity and
discovery. Each admitted run retains its accepted prices.

## Public components

| Component | Implementations | Purpose |
| --- | --- | --- |
| [Agent SDK](sdks/agent/python/) | Python | Inference client, signed payment authorization and agent checkpoints |
| [Provider SDK](sdks/provider/) | Python and Rust | Provider payment types, protocol services and integration interfaces |
| [Smart contracts](contracts/) | Solidity | MPPI's contract interface and payment authority |

Start with the [agent quickstart](docs/agent-quickstart.md) to onboard a wallet
and make paid inference requests. Providers start with the
[provider quickstart](docs/provider-quickstart.md) to initialize declarations,
onboard and attach MPPI to their existing gateway. Agent and provider SDKs are
version `0.1.1`; the shared protocol package remains `0.1.0`. Both quickstarts
select the packaged official Arc testnet profile. Explicit
[trusted deployment profiles](docs/deployment.md#trusted-profile-file) remain
supported. The [API map](docs/sdk-interfaces.md#api-reference-map)
links the implemented interfaces, lifecycle hooks and public conformance tests.

The provider supplies required tokens, actual counts and final outcomes. The
SDK prices and validates those reports. Providers own execution, counting,
cancellation, hosting, TLS and their storage/recovery systems.

A future public testnet service is built and hosted by its provider. Agent
developers obtain the provider's URL or ERC-8004 ID and use the SDK with the
official testnet profile. MPPI supplies the protocol, contracts, SDKs, onboarding
and integration documentation; provider endpoint availability is independent of
the SDK release.

## Arc testnet release

Version `0.1.0` targets only the official Arc testnet deployment, chain ID `5042002`.
Use the [v0.1.0 release record](https://github.com/Alethieum/mppi/releases/tag/v0.1.0)
for the exact source revision, artifact digests, published-install verification status
and security review scope and results.

This source tree contains the v1 schema and generated bindings, exact payment
calculations, fixed session signing, configuration/discovery checks, contract
calls and canonical evidence. The Python SDKs implement native paid request
services, chain freshness, provider reporting, agent streaming and actual-usage
capture/close, supplied-history recovery and ordered batch collection. The MPPI
contract and its Foundry suites are implemented. Python onboarding, registry
bindings and wallet adapters have passed live Arc testnet onboarding and payment
checks with Circle Agent Wallet and developer-controlled wallets. Rust provides
the same exact amount, catalog and provider-report calculations, validated
configuration/discovery, chain evidence and explicit contract operations. Its
native paid runtime supports authenticated admission, provider reports and
capture/close, supplied-history recovery and ordered batch collection.
Rust CLI onboarding, native Circle integration and endpoint serving/verification
have passed live Arc testnet acceptance with an independent Python agent,
including usage capture, refund, explicit updates and unchanged reruns.
The [official Arc testnet contracts](docs/deployment.md) are deployed. Configuration
and Python/Rust verification of the explicit [testnet profile](deployments/arc-testnet.json)
passed, including the scoped configuration/profile security review. The official
deployment also passed an internal acceptance run using frozen release artifacts:
a fresh Rust provider and Python agent onboarded in separate Apple containers,
completed two real inference requests, and settled and refunded the session with
independently verified exact accounting and zero remaining liability. This
functional evidence covers the recorded testnet integration and its private
endpoint.

There are two SDK roles. The Python packages share a small `mppi-protocol`
support distribution containing generated wire types and common validation;
it is a dependency of both SDKs.

## Layout

```text
sdks/
  agent/python/       Python agent SDK
  provider/python/    Python provider SDK
  provider/rust/      Rust provider SDK
contracts/            MPPI implementation, ABI, deployment script and Foundry tests
proto/                Canonical mppi.v1 schema and shared Python support
testvectors/          Public, deterministic conformance fixtures
tests/                Cross-component conformance checks
scripts/              Reproducible generation and local checks
docs/                 Public protocol and integration documentation
examples/             Public configuration and integration examples
```

See the [protocol](docs/protocol.md), [SDK interfaces](docs/sdk-interfaces.md),
[runtime building blocks](docs/python-runtime.md),
[Rust provider runtime](docs/rust-runtime.md),
[Python onboarding and wallets](docs/python-onboarding.md),
[contract interface](docs/contract-interface.md) and [local build](docs/build.md).
Everything needed to build these public sources is contained in this tree.

## Versioning

Package versions live in their manifests. The wire version is the protobuf
package `mppi.v1`. Generated bindings come from the one checked-in schema;
their source is checked for drift during local validation.

Use the release record to establish artifact and deployment provenance. A
matching package version or contract address alone is insufficient. Signing
fixtures use explicitly identified test keys and synthetic addresses.

## License

Copyright 2026 MPPI contributors. Licensed under the [Apache License, Version 2.0](LICENSE).
Vendored dependencies retain their existing licenses and notices.
