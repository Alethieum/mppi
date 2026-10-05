# Python SDK interfaces

The two role packages are **`mppi-agent`** and **`mppi-provider`**, version
`0.1.1`, requiring Python 3.12 or newer. They share one support distribution,
`mppi-protocol`, containing generated wire bindings and exact payment primitives.
The support library is not an additional role SDK.

The SDK implements the pure functions below, typed integration contracts and
[Python payment runtime](python-runtime.md): native agent `Client`, provider
`Mppi`, chain freshness, fixed session signing, configuration/discovery validation,
contract call preparation and canonical receipt reconciliation. The Python
[onboarding and wallet bindings](python-onboarding.md) add resumable agent
registration, deployment profiles, Circle adapters and shared provider setup
helpers. These integrations have passed live Arc testnet onboarding and payment
checks with the wallet products and prerequisites documented there.
The interface classes below are Python `Protocol` declarations;
`Client` and `Mppi` provide the concrete implemented entry points.

Start with the [provider quickstart](provider-quickstart.md) or
[agent quickstart](agent-quickstart.md). Built-in setup uses the packaged official
Arc testnet [deployment profile](deployment.md#trusted-profile-file). The
[Python composition reference](python-quickstart.md) covers explicit capabilities;
the [native CLI reference](rust-cli.md) covers provider commands for both languages.

## API reference map

The role package exports are explicit in the [agent module](../sdks/agent/python/src/mppi_agent/__init__.py)
and [provider module](../sdks/provider/python/src/mppi_provider/__init__.py).
Generated wire records and shared validation belong to the `mppi-protocol`
dependency. Rust's [crate root](../sdks/provider/rust/src/lib.rs) declares its
public modules and re-exports. These source references identify the callable
surface; constructing a type does not establish a successful payment.

| API family | Integration reference | Public checks |
| --- | --- | --- |
| Agent `Client`, completions, streams, session methods and errors | [Agent composition and session operations](python-runtime.md#agent-composition) | [Agent component tests](../sdks/agent/python/tests/), [independent roles](../tests/paid_loop/) |
| Provider `Mppi`, `Mppi.from_config`, coverage/execution interfaces, reports, sessions and recovery | [Provider composition](python-runtime.md#provider-composition), report/session contracts below | [Provider component tests](../sdks/provider/python/tests/), [independent roles](../tests/paid_loop/) |
| `Hooks`, `HookError`, `ObserverStats` and payloads | [Lifecycle callbacks, bounds and repeat semantics](lifecycle-hooks.md) | Both role component suites; Rust runtime tests |
| Agent discovery/onboarding and provider configuration/registration | [Python wallet/onboarding reference](python-onboarding.md), [provider CLI](rust-cli.md) | Both role component suites, [CLI package checks](../tests/test_rust_cli.py) |
| Catalogs, rates, counts, exact arithmetic and report transitions | Payment primitives below, [wire/accounting profile](protocol.md) | [Shared Python tests](../proto/python/tests/), [cross-language vectors](../testvectors/) |
| Fixed signers, funding-wallet capabilities, contract calls and operation evidence | Wallet/evidence types below, [runtime reference](python-runtime.md), [contract interface](contract-interface.md) | Shared/role component suites and independent role checks |
| Deployment, HTTP/RPC, chain reader/follower, registry and wallet adapters | [Trusted pins](deployment.md#trusted-profile-file), [onboarding](python-onboarding.md), [freshness](python-runtime.md#fresh-chain-state) | Shared/role component suites; synthetic service checks do not establish external wallet support |
| Native provider traits, runtime, CLI, listener and verification | [Rust runtime](rust-runtime.md), [native commands](rust-cli.md), [crate API](../sdks/provider/rust/README.md) | [Rust source tests](../sdks/provider/rust/src/), [Rust integration tests](../sdks/provider/rust/tests/) |
| `mppi_agent.testing` | [Loopback mock](python-onboarding.md#local-testing) | [Mock-provider checks](../sdks/agent/python/tests/test_testing.py); no chain/payment claim |

The [build instructions](build.md) execute these local checks. Source tests and
synthetic fixtures establish their stated assertions; they are not evidence of
official deployment, package publication or every external wallet product.
Applications own provider execution, token counts, storage, TLS and operating
policy. SDK callbacks supply observations and never authorize payment by themselves.

## Implemented payment primitives

| Function/type | Rule |
| --- | --- |
| `Rates(input, output, cache=None)` | Exact uint64 prices in six-decimal USDC units per million tokens; absence of cache differs from zero. |
| `TokenCounts(input_tokens, cache_tokens, output_tokens)` | Exact uint64 provider counts; cache is a subset of input. No tokenization is performed. |
| `uint64`, `decimal_uint64` | Reject coercion, booleans, negative/fractional values, noncanonical decimal strings and overflow. |
| `parse_catalog(raw)` | Accept only exact canonical received bytes. Reject duplicate properties, malformed encoding, model/rate/order errors and oversized/deep input. |
| `publish_catalog(rows)` | Validate provider declarations, sort exact model bytes, preserve optional cache and produce canonical bytes plus Keccak-256. No registry write. |
| `priced_numerator(counts, rates)` | Exact `(input-cache)*inputRate + cache*cacheRate + output*outputRate`; positive cache requires its accepted price. |
| `cumulative_usage(numerators)` | Sum every run/version numerator, then floor once by 1,000,000. |
| `required_coverage(numerators)` | Sum actual usage and all remaining/new bounds, then ceil once by 1,000,000. |
| `funding_amount(tokens, rates)` | Caller-selected positive tokens times the greatest applicable rate, rounded up once. A zero result rejects. |
| `top_up_total(deposit, amount)` | Positive raw USDC base-unit top-up; cumulative uint64 capacity is checked without subtracting prior settlement. |
| `remaining_funding_tokens(...)` | Conservative deposit guidance; `None` for all-zero prices. Only this advisory count saturates at uint64 maximum. |
| `report_transition(previous, counts, rates, final_status=None)` | Pure immutable calculation preserving exact usage/reservation conservation and final-report idempotence. |

All intermediates use Python arbitrary-precision integers. Final monetary fields
are checked uint64 values; no floating-point money is accepted. The catalog
serializer is restricted to MPPI's ASCII/string-only schema. It is not a general
JSON canonicalizer and is never used to rewrite inference payloads.

Catalog inputs default to a 1 MiB limit, configurable downward. A byte limit is
checked before decoding and a depth limit before JSON allocation. Publication
also stops consuming declarations once their serialized size exceeds the bound.

The [Rust provider crate](../sdks/provider/rust/README.md) implements these payment,
catalog and report calculations with validated Rust types and checked `U256`
intermediates. Its constructors/getters and explicit catalog limits are described
in the crate README. Rust also supplies native async integration traits,
configuration/discovery, wire validation, chain evidence and contract operations.
Its native lifecycle implements live admission, reporting, capture/close,
supplied-history recovery and ordered batch collection.

## Provider report contract

`RunContext` carries session id, run id, original Control generation, model and
accepted catalog hash. It correlates provider facts; it does not grant execution.

| Async interface | Input and result |
| --- | --- |
| `CoverageProvider(request, context)` | Unchanged validated JSON bytes and context → provider-required token counts, before paid execution. |
| `ProviderRuns.require_coverage(context, remaining_tokens)` | Provider's next required remaining bounds → a payment grant/denial and required amount. |
| `ProviderRuns.report_usage(context, counts)` | Cumulative supplied counts → validated accounting result and session meter cursor. |
| `ProviderRuns.report_terminal(context, counts, status)` | Definitive final counts/status together → final accounting result. |
| `ProviderSessions.checkpoint(session_id)` | Detached payment evidence for caller-owned retention; no storage is added. |
| `ProviderSessions.recover(evidence)` | Supplied snapshot, reservation and chain evidence → validated session view. It does not load or save provider data, authenticate Control or establish readiness by itself. |

The pure `report_transition` helper already rejects decreasing counters,
decreasing exact recognized cost, unpriceable cache counts, usage exceeding its
remaining reservation and conflicting definitive finals. A valid report moves
the same exact cost from reservation into usage. A valid final report releases
only unused coverage. Exact repeated final reports are no-ops. Invalid inputs
leave the caller's immutable previous value unchanged.

Its result is a calculation, not a commit operation, signature check, meter
allocator, chain verification, termination proof or execution grant. The provider
runtime applies it using accepted immutable run prices and its consistent live
session view. It enforces generation fencing, readiness, concurrent admission
and meter sequencing. Supplied-history recovery revalidates retained run proofs,
prices, reservations, accounting and operation evidence before installing state.
Old-generation reports may finalize their original admitted runs; they cannot
authorize new work. Provider storage, coordination, counting, engine control and
recovery infrastructure remain provider-owned.

## Resource lifetime and sessions

`ProviderLifecycle` defines native gRPC registration, HTTP discovery responses,
`start() -> Readiness` and `shutdown(deadline) -> ShutdownResult`. Registration
precedes caller-owned server startup. The shutdown deadline is an absolute
`time.monotonic()` value in seconds for Python and `tokio::time::Instant` for
Rust, each on the owning process. Shutdown stops grants,
waits boundedly for definitive reports and returns unresolved session/run ids and
pending operations. It does not close escrow, stop a caller-owned server or infer
that a job terminated. `Mppi` implements this resource lifetime.

`AgentSessions` defines synchronous `open`, `topUp`, `close`, `requestClose`,
`withdraw`, `releaseIdle`, `recover`, `reconcile`, `get` and `list`. Opening carries the explicit funding
choice. `topUp` accepts integer six-decimal USDC base units. `list` covers known
sessions, uses an opaque cursor and a checked limit from 1 through 1,000. The
network `Client` owns its I/O loop and exposes this interface for its selected
provider and retained session. No automatic
funding, replacement session or inference replay follows an error.

`CompletionStream` defines deterministic cleanup for one Complete. Closing it
does not close the session. The Client's local close also preserves
unresolved operation/run evidence rather than implicitly closing escrow.

The provider's asynchronous session interface defines `settle`, `settleBatch`,
`close`, `get`, `list` and evidence validation through `recover`. All economic
operations return an `OperationResult` with stable operation reference and one of
`pending`, `confirmed`, `failed`, `unknown`. Confirmed results require available
transaction and block evidence. An unknown reply is never a fresh economic
operation. Batch results retain independent per-session target coverage and
attributable settlement references. `settleBatch` preserves ordered targets,
simulates the exact call and retains unresolved submission identity. Repeated
session items can establish coverage without identifying which identical item
caused an event. Entirely silent batches remain unresolved.

`checkpoint` returns `RecoveryEvidence`, including `RecoveryRun` and
`RecoveryOperation` records. `recover` requires complete provider-supplied
history and rechecks canonical evidence; it never establishes readiness by
trusting a saved ready flag. A new authenticated Control attachment and resolved
prior execution are required. The provider chooses how to retain this evidence.
Restored transaction hashes must match read-only wallet reconciliation of their
original operation references before their chain outcomes are accepted. An
unavailable or conflicting lookup rejects restoration without replacing live
state or submitting another transaction.

## Wallet and evidence types

`SessionSigner` declares the four fixed-key MPPI signing capabilities: Voucher,
RunAuth, CloseAuth and ReconnectAuth. `FundingWallet` separately declares USDC
authorization signing and wallet execution/reconciliation. `FundingAuthorization`
uses the token's separate domain and uint256 token-interface fields; MPPI amount
validation still precedes execution. `WalletCall` represents a nonpayable
MPPI/registry call. No provider credential or secret is stored in these records.

`SessionDescriptor`, `RunContext`, `ChainObservation`, settlement references,
readiness and operation records check represented widths and basic consistency.
`PaymentFailure` retains the same compact diagnostic evidence as the wire:
SESSION_NOT_READY requires a nonempty reason; PRICE_CHANGED includes both catalog
hashes; CONTRACT_PAUSED includes a typed observed active pause level. A run
diagnostic also requires its session id. Unknown not-ready reasons remain
not-ready and cannot imply permission to run.
Constructing a record does not verify signatures, finality, deployed identity,
current ownership, freshness, transaction success or payment eligibility. Those
checks belong to the runtime and must be demonstrated before paid operation.

## Conformance and remaining work

The [catalog](../testvectors/catalog.json), [amount](../testvectors/amounts.json)
and [report lifecycle](../testvectors/lifecycle.json) suites use the common
[JSON schema](../testvectors/schema.json). Large numeric values are decimal
strings in fixtures. Python tests also verify the independent ABI/EIP-712
[signing fixtures](../testvectors/signing.json); their disclosed key is test-only.

Native services, bounded queues, rich payment errors, agent checkpoints,
continuous chain freshness, Circle wallet adapters and onboarding are
implemented. Separate live Arc tests verified onboarding, payment, recovery and
a bounded authenticated Control reconnect soak. Provider-reported synthetic
usage exercises the SDK boundary; it does not certify an inference engine.
The [Rust provider runtime](rust-runtime.md) implements live admission, reports
and capture/close, supplied-history recovery and batch collection. Its
[native CLI](rust-cli.md) has passed live Arc onboarding, payment, update and
endpoint-verification checks. [Typed lifecycle observers](lifecycle-hooks.md)
are available for both roles, with callback payload and runtime-trigger tests.
Official deployment, publication and integrated release security acceptance
remain separate release gates.

See [the protocol profile](protocol.md) for wire definitions, limits and
accounting rules. Local build commands and validation are described in the
project's root documentation.
