# MPPI provider — Rust

Version `0.1.1` provides exact payment arithmetic, canonical price catalogs,
immutable validation of provider usage reports, generated `mppi.v1` tonic types
and typed session/proof hashing and verification. It also provides validated
configuration and integration traits, discovery documents, bounded HTTP/RPC,
canonical chain evidence, freshness checks and explicit contract operations.
`runtime::Mppi` adds native paid services, authenticated attachments, run
admission, provider reports, supplied-history recovery, ordered batch capture
and cooperative close. The native CLI adds provider onboarding, explicit updates,
provider-host launch and endpoint verification. `provider init` generates fresh
declarations; `--testnet` selects the packaged official profile.
`setup::runtime_from_config` composes the runtime with the existing Circle
operator. Earlier `0.1.0` live Arc testnet acceptance exercised an independent
Python agent, including capture/refund and explicit updates. The official deployment remains Arc testnet. See the
[v0.1.0 release record](https://github.com/Alethieum/mppi/releases/tag/v0.1.0) for
the exact artifact digests, published-install verification status and security
review scope and results.

## Install from crates.io

Start with the [provider quickstart](../../../docs/provider-quickstart.md) for
the complete setup and Linux ARM64 binary installation. To build the native CLI
from crates.io, use Rust 1.95.0:

```sh
cargo install mppi-provider --version 0.1.1 --locked --bin mppi
mppi --help
```

Ensure Cargo's executable directory is on your `PATH`. Add the SDK to your
provider application's `Cargo.toml`:

```toml
[dependencies]
mppi-provider = "=0.1.1"
```

Build that application using its own committed lockfile. The CLI uses the
package's included lockfile. See [Cargo installation](https://doc.rust-lang.org/cargo/commands/cargo-install.html)
and [exact version requirements](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#comparison-requirements).

## Use locally

Use Rust 1.95.0. Add this directory as a path dependency in your provider host:

```toml
[dependencies]
mppi-provider = { path = "/path/to/mppi/sdks/provider/rust" }
```

Build that host using its own committed lockfile. To install the native setup
CLI, run from the public `mppi/` source root:

```sh
cargo install --path sdks/provider/rust --locked --bin mppi
mppi --help
```

The [provider quickstart](../../../docs/provider-quickstart.md) covers the trusted
deployment profile, private configuration/artifacts, operator and recipient
wallets, registration, host startup and verification. The
[composition helper](../../../docs/rust-runtime.md#compose-a-provider-host)
connects the saved configuration to the runtime. Supply your inference gateway,
coverage/count callbacks, definitive job outcomes and retained history; the
crate does not provide an inference engine or provider storage.

## Payment calculations

Rates are raw USDC units per million tokens. Amounts and token counts use `u64`;
exact cost numerators use checked `U256`. `decimal_uint64` validates externally
supplied decimal strings. It rejects signs, leading zeroes, exponents and values
outside the protocol range.

```rust
use mppi_provider::{
    Rates, RunAccounting, RunStatus, TokenCounts, cumulative_usage,
    priced_numerator, report_transition, required_coverage,
};

let rates = Rates::new(1_000_000, 2_000_000, None);
// The provider supplies the counts and the accepted rates for this run.
let bound = priced_numerator(TokenCounts::new(100, 0, 1_000)?, rates)?;
let authorized = required_coverage([bound])?; // 2,100 raw USDC units
let previous = RunAccounting::new(TokenCounts::new(0, 0, 0)?, bound, None)?;
let report = report_transition(
    &previous,
    TokenCounts::new(100, 0, 10)?,
    rates,
    Some(RunStatus::Completed),
)?;
let used = cumulative_usage([report.delta_numerator()])?; // 120 raw units
assert!(used < authorized);
assert_eq!(report.state().remaining_numerator(), mppi_provider::U256::ZERO);
```

For a session, sum every run's exact numerator before taking the usage floor.
Coverage uses one ceiling over cumulative usage and remaining/new reservations.
An accepted report consumes reservation by the same exact cost delta; a final
report releases unused coverage. `report_transition` returns a value for the
caller to apply consistently to its payment state. It does not count tokens,
commit state, verify termination or authorize execution. Preserve each run's
accepted rates when a newer catalog becomes available.

`publish_catalog(rows, max_bytes)` sorts validated `CatalogRow` declarations and
returns canonical bytes and their Keccak hash. `parse_catalog(raw, max_bytes)`
validates received bytes exactly: model order, field order, decimal strings,
duplicate decoded keys and encoding. It never repairs received price evidence.
`DEFAULT_CATALOG_BYTES` is 1 MiB; callers can set a smaller positive limit.
The parser checks the 128-container depth bound before decoding. Cache pricing
is optional; an absent price and an explicit zero price have different hashes.

The complete executable example is [`examples/payment.rs`](examples/payment.rs):

```sh
cargo run --manifest-path sdks/provider/rust/Cargo.toml --locked --example payment
```

## Configuration and integration

For installation and Circle setup, see the
[provider quickstart](../../../docs/provider-quickstart.md). The
[native CLI reference](../../../docs/rust-cli.md) covers recipient proofs and
all five commands.

For native service composition and callback responsibilities, see
[the Rust runtime guide](../../../docs/rust-runtime.md).

`config::ProviderConfig::from_file(path, overrides)` loads the public
[provider example](../../../examples/provider/mppi.toml). `from_mapping` accepts
`serde_json::Value` declarations. Both reject unknown SDK fields and preserve
exact amounts and provider IDs. Overrides replace fields within a section;
the models array is replaced as a whole. TOML money must be quoted even when
overridden. File-relative TLS paths resolve from the configuration directory.
The SDK does not read secrets while validating configuration.

Configuration and discovery documents are bounded at 1 MiB. JSON permits at
most 128 nested containers; the TOML 1.0 parser applies its tighter 80-level
nesting limit. Validated records expose getters. RPC URLs, upstream credentials
and private-key paths are omitted from debug output.

`discovery::DiscoveryDocuments::new(&config, &identity)` compares the configured
provider with separately observed registry identity and catalog, then supplies
`/discover`, `/.well-known/agent-registration.json` and `/models` response bytes
for the provider to publish. The
provider owns its endpoint and TLS. HTTP model cards never replace on-chain
catalog pricing.

`interfaces` defines asynchronous coverage, usage-report, execution, session
and lifecycle contracts. Providers supply counts, termination and storage.
`PreparedRequest` keeps validated inference JSON bytes unchanged; a cancellation
signal does not prove final usage. Recovery records carry supplied evidence;
`ProviderSessions::recover` validates it before installing payment state.

`wire::decode_message` enforces received byte limits and structural protobuf
validation. `wire::validate_message` checks already decoded values. Use the
same configured limits at the gRPC transport before decoding. Widths, required
presence, enum mapping, duplicate logical IDs, JSON and rich payment errors are
checked here; signature, nonce, catalog-history and lifecycle checks remain
separate. The `queue` module supplies a count/byte-bounded asynchronous queue
with a fixed producer wait deadline.

## Chain evidence and wallet calls

Use `deployment::official_testnet_profile()` for the packaged verified official
Arc testnet pins, or `deployment::load_profile(path)` for an explicit trusted
manifest. The CLI selects the packaged profile with `--testnet`; without that
selection an explicit profile is required. A supplied manifest needs independent
provenance. The chain reader verifies the observed deployment against either
profile, using the selected trusted RPC.

`http::HttpTransport::new(timeout_ms, max_bytes, ca_pem)` uses verified TLS,
no redirects, no implicit proxy, no retries and no decompression. A supplied
PEM trust bundle replaces system roots. A total request deadline and streamed
body limit apply. For JSON-RPC use `http::MAX_RPC_RESPONSE_BYTES` (4 MiB): ABI
framing and hexadecimal encoding expand a valid 1 MiB catalog beyond 2 MiB.
Discovery documents, request bodies and CA bundles retain their 1 MiB limits.
Endpoints whose request paths would be silently normalized
are rejected. `HttpJsonRpc` exposes only read methods and omits endpoint values
and remote response bodies from errors.

`chain::ChainReader` checks the selected chain, code hashes, proxy implementation
slots, voucher domain, canonical block inclusion and exact ABI/event bytes.
`identity` binds the ERC-8004 owner, recipient, discovery URI and catalog to one
observation. These checks establish consistency with the caller's trusted inputs.

`follower::ChainFollower` runs on Tokio. Call `start`, watch the provider and
session IDs in use, then read `snapshot` and `readiness` without per-token RPC.
Observation age, head progress, block time, pause state and collection margin
all affect readiness. Failures and cancelled refreshes invalidate readiness;
`current` retains historical evidence. Keep the local clock synchronized.
`stop` aborts and joins the background task; dropping the follower aborts it.

`contract::ContractCalls` prepares the eight MPPI operations against the exact
packaged Solidity interface. `settle` captures the supplied actual cumulative
usage within the voucher's authorization. A higher voucher alone is not usage.
`operations::submit_operation` invokes the supplied wallet once;
`reconcile_operation` queries the original operation without resubmission.
Wallet adapters must durably bind each operation reference to its exact intent
before sending it. Keep unknown outcomes and their handles for reconciliation.
Only verified canonical receipts and state establish confirmation; a repeated
no-op cannot invent a new collection event. `circle::CircleDeveloperWallet`
supplies the native Circle SCA capability with an explicit bounded operation
record. Registry and CLI onboarding use the same original-operation checks.

## Proof verification

Construct `signing::SigningContext` with the verified chain ID and MPPI proxy
address, then call `session_id`, `digest` and `verify` with the typed descriptor
and proof.

`verify` checks the proof's session id, 65-byte signature, v=27/28, low-s and fixed
`sessionSigner`. The caller must supply a verified descriptor/domain and enforce
the session's lifecycle, deposit, attachment, nonce and run state. Cryptographic
verification alone never admits generation. Provider infrastructure and token
measurement remain provider responsibilities.

The crate builds independently from its package archive: generated wire source
is included, and normal builds require neither `protoc` nor sibling project
directories. Generated parsing alone does not enforce the wire validation profile;
use the checked `wire` entry points before interpreting external messages.
See the public [protocol](../../../docs/protocol.md) and [contract interface](../../../docs/contract-interface.md).

## Development

Use Rust 1.95.0 and the committed lockfile. From the public `mppi` root:

```sh
cargo test --manifest-path sdks/provider/rust/Cargo.toml --locked
cargo package --manifest-path sdks/provider/rust/Cargo.toml --locked --allow-dirty
```

Regenerate wire source with the pinned tonic/prost generator and `protoc` 35.1
(provided by grpcio-tools 1.84.0 in the project test environment):

```sh
PROTOC=/path/to/protoc cargo run --manifest-path sdks/provider/rust/Cargo.toml \
  --locked --features codegen --example generate -- proto sdks/provider/rust/src/generated
```

To check generation without modifying source, generate into a temporary
directory and compare its `mppi.v1.rs` with `src/generated/mppi.v1.rs`. Do not edit
generated code by hand. The authoritative schema is [mppi.proto](../../../proto/mppi/v1/mppi.proto).

Regenerate the explicitly public, insecure signing fixture:

```sh
cargo run --manifest-path sdks/provider/rust/Cargo.toml --locked --quiet \
  --example signing_vectors > testvectors/signing.json
```

The fixture's private key is public test data. Never fund it. Tests verify these
bytes independently in Solidity and Python as well as Rust. Catalog, amount and
report-transition tests consume the same public fixtures as Python. Provider
storage and token counting remain provider responsibilities. The native Circle
adapter and CLI are described in the [command guide](../../../docs/rust-cli.md).

## Lifecycle observers

Use `runtime::Hooks` with `Mppi::with_limits_and_hooks(...)`.
See the [hook payloads, ordering and bounds](../../../docs/lifecycle-hooks.md).

## License

Copyright 2026 MPPI contributors. Licensed under the [Apache License, Version 2.0](LICENSE).
