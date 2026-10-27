# MPPI provider — Rust

Version `0.2.0` provides exact payment arithmetic, canonical price catalogs,
immutable validation of provider usage reports, generated `mppi.v1` tonic types
and typed session/proof hashing and verification. It also provides validated
configuration and integration traits, discovery documents, bounded HTTP/RPC,
canonical chain evidence, freshness checks and explicit contract operations.
`runtime::Mppi` adds native paid services, authenticated attachments, run
admission, provider reports, supplied-history recovery, ordered batch capture
and cooperative close. The native CLI adds provider onboarding, explicit updates,
provider-host launch and endpoint verification. `provider init` generates fresh
declarations for an explicit network; saved configuration selects the packaged
official mainnet or testnet profile, with optional `--mainnet` / `--testnet` overrides.
`setup::runtime_from_config` composes the runtime with the existing Circle
Agent Wallet operator, and `setup::runtime_from_config_with_wallet` with another
`TransactionWallet`. Earlier `0.1.0` live Arc testnet acceptance exercised an independent
Python agent, including capture/refund and explicit updates. See the
[deployment reference](../../../docs/deployment.md) for both networks and the
[v0.1.0 release record](https://github.com/Alethieum/mppi/releases/tag/v0.1.0) for
the exact artifact digests, published-install verification status and security
review scope and results.

## Upgrading from 0.1.2

- The `mppi` CLI and the Python package read and write the same
  `<config>.wallet-operations.json`. Upgrade both to `0.2.0` together: `0.1.2`
  of either refuses a journal `0.2.0` has saved. The Circle adapter's journal
  keeps each call's digest instead of the call, has no count limit, and holds
  its lock only while it reads or replaces the journal, never across a Circle
  call, so `mppi provider update` can run while a host serves. An execute or
  reconcile waits for that lock up to `CircleOptions::read_timeout_ms`.
  Within one process an adapter runs one operation at a time, CLI commands
  included, so an operation may also wait behind another's command.
- `interfaces::ProviderSessions` has a new method, `recover_current`, whose
  default refuses with `UNIMPLEMENTED`. A host type that implements the trait,
  such as a wrapper around the runtime's `sessions()`, still compiles unless
  another trait in scope has a method of the same name, but must forward
  `recover_current` itself: otherwise a call through it is refused.
- `setup::runtime_from_config_with_wallet(host, wallet, coverage, execution,
  options)` composes the runtime with any `TransactionWallet`, such as one whose
  key an HSM or KMS holds. Setup checks its address against the saved operator
  and verifies the deployment and USDC domain; the wallet's own account,
  network and access are its own to check. A `CircleAgentWallet` passed this way
  skips the CLI check and the existing-record requirement that
  `runtime_from_config` applies: use that for Circle.
- `capabilities::TransactionWallet` has three defaulted methods, so an
  implementation compiles unchanged unless another trait in scope has a method
  of the same name: `retire(operation_refs, at)` receives references the
  runtime no longer needs, with the time from which they may be forgotten,
  which a later `reconcile` of the reference, or its record gaining a
  transaction hash, withdraws (the default keeps every record);
  `near_capacity`, each rise of which the runtime reports as an
  `on_error` `HookError`; and `max_call_bytes` (default 61,440, the Circle
  adapter's bound), which sizes each batch the runtime builds. The runtime may
  now call `execute` and `reconcile` for different operations at once: an
  implementation serializes its own records, as `circle::CircleAgentWallet`
  does, one operation at a time. The
  [wallet requirements](../../../docs/sdk-interfaces.md#wallet-requirements)
  state everything the runtime assumes of a wallet.
- `runtime::Hooks` has new fields, `on_session_retired` and `on_timing`: build
  it with `..Hooks::default()`. `on_timing` receives a `runtime::OperationTiming`
  for each request's admission and each settlement or close, in milliseconds:
  the host's coverage callback, chain reads, the wallet's call and waiting.
- `runtime::RuntimeLimits::new(max_control_streams, wire)` no longer takes a
  session count, and `max_sessions()` is gone. A session closed on chain leaves
  the runtime after `closed_retention()` (24 hours by default,
  `with_closed_retention`). A `SessionOpen` for a closed session it no longer
  holds, and a recovery from a record that `RecoveryEvidence::is_final` reports
  final once the chain shows the session closed, are refused
  `SESSION_NOT_READY` / `SESSION_CLOSED`. Pass such final records to `recover`
  at startup rather than skipping them: their refusal hands the wallet each
  reference only they still name, if the wallet's `reconcile` of it agrees with
  the record: the same reference, the same transaction hash or none on both,
  and the same wallet operation id when the record names one.
  Maintenance makes the same check before it hands over a reference no wallet
  answer in this process has matched (one restored without a transaction hash,
  or kept against an answer that disagreed); every other hand-over makes no
  wallet call first.
- Collections: a batch fits the wallet's `max_call_bytes`, at most 136 sessions
  for Circle, and maintenance splits the rest; a host's own `settle_batch` over
  it is refused before any intent is recorded. A close mined and reverted is
  prepared again under a successor of its reference (`<reference>.1`, `.2`, …),
  at most once per settlement interval, and sent only once the successor
  simulates: a failed simulation sends nothing, and a `close` call then fails
  with its error. A settlement or close that never began
  (a `Failed` result with neither a transaction hash nor a wallet operation id)
  is restored as `Failed`, so the live rules prepare it again.
- A session's waiting runs are admitted in the order they first waited, while
  each voucher arrives within the runtime's turn; `0.1.2` could admit a later
  run first.
- `chain::ChainError` has new variants, `Transport`, `Rpc`, `Reverted` and
  `Lagging`, and `http::TransportError` is an enum of causes: `Invalid`,
  `Connection`, `Timeout`, `Status` and `Response`. Both, and
  `circle::CircleError`, are `#[non_exhaustive]`, so a `match` on any of them
  outside this crate needs a wildcard arm. `chain::Rpc::call_many` is a
  defaulted method that sends one call at a time; implement it to batch a
  step's reads. An `Rpc` that sees a block its backend has not imported
  reports `Lagging`, as `HttpJsonRpc` does for Arc's -32014.
- `circle::supported_account_pins` is removed: the adapter pins no account code.
- Behaviour a caller can observe: `ChainFollower::refresh` returns the accepted
  snapshot as `Ok` when the RPC lags and that snapshot covers every watch, and
  `Err(ChainError::Lagging)` otherwise; a Circle `execute` refused for an
  invalid reference, or once the journal shows no record of its reference,
  returns a `Failed` result with a `NotSubmittedError: <kind>` cause instead of
  an error; `OnboardingError` and `CliError` include a failed chain read's typed
  cause in their `Display` and equality; and recovery refusals name the check
  that failed (`SESSION_NOT_READY: <check>`).
- `types::OperationResult` can carry a wallet's cause (`with_cause`, `cause`);
  the Circle adapter names the error kind and the CLI's listed error code.
  `local_state::LocalState`, the locked, crash-safe record the Circle adapter
  keeps, is public for other wallet adapters.
- The package's lockfile, which `cargo install --locked` uses, moves rustls to
  0.23.46 and rustls-webpki to 0.103.16 (RUSTSEC-2026-0346 to RUSTSEC-2026-0349
  and RUSTSEC-2026-0382), webpki-root-certs to 1.0.10 (RUSTSEC-2026-0383) and
  the yanked yoke-derive 0.8.3 to 0.8.4, and tokio is pinned to `=1.53.3`
  (RUSTSEC-2026-0386). An application built with its own lockfile should
  update them too:
  `cargo update -p rustls -p rustls-webpki -p webpki-root-certs -p yoke-derive`.
  Its `cargo audit` also reports RUSTSEC-2026-0387 for crypto-bigint 0.5.5,
  which alloy-primitives' `k256` feature brings in. This crate accepts that
  advisory in `.cargo/audit.toml`: it only recovers the signer of a received
  signature, whose inputs are public, and holds no secp256k1 key.

## Install from crates.io

Start with the [provider quickstart](../../../docs/provider-quickstart.md) for
the complete setup and Linux ARM64 binary installation. To build the native CLI
from crates.io, use Rust 1.95.0:

```sh
cargo install mppi-provider --version 0.2.0 --locked --bin mppi
mppi --help
```

Ensure Cargo's executable directory is on your `PATH`. Add the SDK to your
provider application's `Cargo.toml`:

```toml
[dependencies]
mppi-provider = "=0.2.0"
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
`RecoveryEvidence::is_final` marks a closed session's record with nothing left
to recover: recovery refuses it, but pass it at startup all the same, since the
refusal hands to retirement each reference it names that the wallet's own
record agrees with. The runtime retires a closed session after
`RuntimeLimits::closed_retention` and limits no session count.

`wire::decode_message` enforces received byte limits and structural protobuf
validation. `wire::validate_message` checks already decoded values. Use the
same configured limits at the gRPC transport before decoding. Widths, required
presence, enum mapping, duplicate logical IDs, JSON and rich payment errors are
checked here; signature, nonce, catalog-history and lifecycle checks remain
separate. The `queue` module supplies a count/byte-bounded asynchronous queue
with a fixed producer wait deadline.

## Chain evidence and wallet calls

Use `deployment::official_mainnet_profile()` or
`deployment::official_testnet_profile()` for the packaged official Arc pins, or
`deployment::load_profile(path)` for an explicit trusted manifest. The CLI selects
the packaged profile from the resolved configuration network. `--mainnet` and
`--testnet` override that network; `--profile` supplies independent pins which
must still match it. A supplied manifest needs independent provenance. The
chain reader verifies the observed deployment against the selected profile
using the selected trusted RPC.

`http::HttpTransport::new(timeout_ms, max_bytes, ca_pem)` uses verified TLS,
no redirects, no implicit proxy and no decompression, and sends each request
once over kept-alive connections. A supplied PEM trust bundle replaces system
roots. A total request deadline and streamed body limit apply. For JSON-RPC use
`http::MAX_RPC_RESPONSE_BYTES` (4 MiB): ABI framing and hexadecimal encoding
expand a valid 1 MiB catalog beyond 2 MiB. Discovery documents, request bodies
and CA bundles retain their 1 MiB limits. Endpoints whose request paths would be
silently normalized are rejected. `HttpJsonRpc` exposes only read methods, sends
a step's reads as JSON-RPC batches and repeats a transient failure at most twice.
`chain::ChainError` and `http::TransportError` name a failure's cause (status,
JSON-RPC code, revert selector, lag) without endpoint values or remote response
bodies. A wallet that submits its own transactions posts them through
`HttpTransport::request`, which never repeats them.

`chain::ChainReader` checks the selected chain, code hashes, proxy implementation
slots, voucher domain, canonical block inclusion and exact ABI/event bytes,
reading each step at one block, in one exchange per JSON-RPC batch of 32 calls and
1 MiB. `identity` binds the ERC-8004 owner, recipient, discovery URI and catalog
to one observation. These checks establish consistency with the caller's trusted
inputs.

`follower::ChainFollower` runs on Tokio. Call `start`, watch the provider and
session IDs in use, then read `snapshot` and `readiness` without per-token RPC. On
Arc each refresh reads every watch at one block through Multicall3, in one round
trip up to about 2,300 watched sessions. Session watches that wait behind a
refresh are read together by the next one. Observation age, head progress, block
time, pause state and collection margin all affect readiness. Failures and
cancelled refreshes invalidate readiness; a lagging RPC keeps the accepted
evidence, which ages as usual. `current` retains historical evidence. Keep the
local clock synchronized. `stop` aborts and joins the background task; dropping
the follower aborts it.

`contract::ContractCalls` prepares the eight MPPI operations against the exact
packaged Solidity interface. `settle` captures the supplied actual cumulative
usage within the voucher's authorization. A higher voucher alone is not usage.
`operations::submit_operation` invokes the supplied wallet once;
`reconcile_operation` queries the original operation without resubmission.
Wallet adapters must durably bind each operation reference to its exact intent
before sending it. Keep unknown outcomes and their handles for reconciliation.
`TransactionWallet::max_call_bytes`, 61,440 by default (the Circle adapter's
bound), caps the calldata of each batch the runtime builds: 260 bytes plus 448
per session, at most 136 sessions by default. A wallet whose real limit is gas
rather than bytes states a smaller size.
Only verified canonical receipts and state establish confirmation; a repeated
no-op cannot invent a new collection event. `circle::CircleAgentWallet`
uses official Circle CLI `1.1.4` with an explicit bounded operation record.
`CircleAgentWallet::new(chain, address, path, CircleOptions)` creates a journal;
`open_existing` requires the original journal. `setup::runtime_from_config(host,
operation_path, coverage, execution)` composes the authenticated wallet and runtime;
`setup::runtime_from_config_with_wallet(host, wallet, coverage, execution, options)`
composes another `TransactionWallet`, checked by its address.
Registry and CLI onboarding use the same original-operation checks. Select the
operator with `mppi provider onboard --wallet ADDRESS`; login and renewal remain
with Circle and the operator.

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

Use `runtime::Hooks` with `Mppi::with_limits_and_hooks(...)` or
`setup::RuntimeOptions`. `on_timing` reports where each admission and
collection spent its time. See the
[hook payloads, ordering and bounds](../../../docs/lifecycle-hooks.md).

## License

Copyright 2026 MPPI contributors. Licensed under the [Apache License, Version 2.0](LICENSE).
