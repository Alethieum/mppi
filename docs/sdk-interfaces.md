# Python SDK interfaces

The two role packages are **`mppi-agent`** version `0.1.6` and
**`mppi-provider`** version `0.2.0`, requiring Python 3.12 or newer. They share
support distribution `mppi-protocol` `0.1.4`,
containing generated wire bindings, exact payment primitives
and the Circle Agent Wallet adapter.
The support library is not an additional role SDK.

The SDK implements the pure functions below, typed integration contracts and
[Python payment runtime](python-runtime.md): native agent `Client`, provider
`Mppi`, chain freshness, fixed session signing, configuration/discovery validation,
contract call preparation and canonical receipt reconciliation. The Python
[onboarding and wallet bindings](python-onboarding.md) add resumable agent
registration, deployment profiles, Circle adapters and shared provider setup
helpers. Earlier live Arc testnet onboarding and payment evidence belongs to
the exact integrations in the [versioned release record](https://github.com/Alethieum/mppi/releases/tag/v0.1.0).
The interface classes below are Python `Protocol` declarations;
`Client` and `Mppi` provide the concrete implemented entry points.

Start with the [provider quickstart](provider-quickstart.md) or
[agent quickstart](agent-quickstart.md). Built-in setup uses the packaged official
[deployment profiles](deployment.md#trusted-profile-file) for explicit Arc testnet
and mainnet selection. The
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
| `ProviderSessions.recover_current(evidence)` | As `recover`, for evidence the host asserts is current: checkpointed after every admission and before dispatch, never older than its newest. A pending run absent from it is then answered `NOT_ADMITTED`; after `recover` it refuses the attachment. |

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
`withdraw`, `releaseIdle`, `recover`, `reconcile`, `get`, `list`, `terms` and
`run`. Opening carries the explicit funding
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
trusting a saved ready flag. `RecoveryEvidence.is_final()` (Rust `is_final()`)
reports evidence of a closed session with no reservation and no unresolved
operation that is not inconsistent; `recover` refuses it `SESSION_CLOSED` once
the chain shows the session closed, and hands to retirement each operation
reference it names that the wallet's own record agrees with, so a host passes
such records at startup rather than skipping them. A runtime retires such a
session after its retention window and never
reinstalls it. A new authenticated Control attachment and resolved
prior execution are required. The provider chooses how to retain this evidence.
Restored transaction hashes must match read-only wallet reconciliation of their
original operation references before their chain outcomes are accepted. An
unavailable or conflicting lookup rejects restoration without replacing live
state or submitting another transaction.

## Wallet and evidence types

`SessionSigner` declares the four fixed-key MPPI signing capabilities: Voucher,
RunAuth, CloseAuth and ReconnectAuth. `FundingWallet` separately declares USDC
authorization signing and wallet execution/reconciliation; any wallet provider
fits behind it ([wallet requirements](#wallet-requirements)).
Funding requires the agent's ERC-8004 id. Before submitting an opening, the
agent checks the registry binds that id to the wallet and checks the signature
as USDC will. `FundingAuthorization`
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

## Wallet requirements

Circle Agent Wallet is the recommended wallet, and both SDKs include an adapter
for it. Any other wallet, such as MetaMask, Coinbase or Privy, works behind a
thin adapter that meets the requirements below: the agent's `FundingWallet` and
the provider's `TransactionWallet`, in Python
[`mppi_protocol.capabilities`](../proto/python/src/mppi_protocol/capabilities.py)
and in Rust [`capabilities`](../sdks/provider/rust/src/capabilities.rs). The
agent and both provider runtimes assume nothing else of a wallet. What the
Circle adapter does beyond them, such as its CLI check and its journal, is
documented as the Circle adapter's
([Python](python-onboarding.md#circle-agent-wallet-adapter),
[Rust](rust-cli.md#local-records-and-results)). MPPI accepts any wallet that
meets them, whatever its implementation or version; the Circle adapter alone
drives the Circle CLI's exact command line, so it accepts only the verified
`@circle-fin/cli` 1.1.4 and refuses any other version before anything is sent.

**Address.** `address` is the account that sends every call: the agent's
funding wallet, or the provider's configured operator. It does not change,
and reading it must not block or fail: the runtimes and the agent read it
synchronously, in Python on the event loop.

**Operation references.**

- `execute(call, operation_ref)` binds the reference durably to exactly that
  call (from `address`, with its chain, destination and calldata, and zero
  native value) before anything can reach the wallet's service, then sends it
  once. The binding lasts until `retire` lets it go. A wallet whose records are
  full refuses new calls as not submitted; it never forgets a binding to make
  room.
- The same reference with the same call is the same submission: answer as
  `reconcile` does and send nothing. With another call, refuse it as an error,
  which leaves the result `UNKNOWN`, never as not submitted: the first call may
  have been sent.
- A new reference is a new submission, even of a call identical to an earlier
  one. The agent sends a failed exit again under `<reference>.1`, `.2`, and a
  failed opening or top-up under a new reference with the same authorization;
  a provider sends a reverted close again under `close:<id>.1`, `.2`, and each
  settlement under a new reference. A wallet that matched calls across
  references would hand back the earlier transaction.
- After a not-submitted result (below), the reference binds nothing: a later
  `execute` of it, with that call or another, is a new submission. A provider
  continues such a close under its own reference.

**Results.** `execute` and `reconcile` return an `OperationResult` for the
reference they were given; one naming another reference is never its answer
(it counts as `UNKNOWN` with no identifiers, and a recovery's lookup refuses
it). A wallet's state is never chain evidence:

- With a transaction hash, the runtimes and the agent read that transaction's
  receipt and MPPI events, whatever state the wallet reported. The hash is the Arc
  transaction that carries the call: for a smart account or a bundler, the
  outer transaction.
- Without one, `FAILED` asserts that the wallet's own durable record proves
  nothing under the reference reached its service, so the call may be sent
  again. Nothing else is `FAILED`: not a failure after the call may have
  reached the service, and not a reference forgotten after `retire`.
  `PENDING` with a wallet operation id stays pending; anything else is
  `UNKNOWN`.
- From `execute`, a Python adapter may instead raise `NotSubmittedError`, or a
  subclass, with that same proof; any other exception is `UNKNOWN`. Rust has no
  such error: return the `Failed` result, whose cause by convention begins
  `NotSubmittedError`; an `Err(WalletError)` is `Unknown`. Return identifiers
  the wallet knows in an `UNKNOWN` result rather than an error, so they are
  kept.
- `wallet_operation_id`, when present, is a nonempty string of at most 256
  characters (onboarding refuses a longer one). `cause`, when present, is the
  adapter's own short reason, never text from its service: the agent passes an
  `execute`'s to `on_error`, and both roles keep it with the result.

**`reconcile`** reports the wallet's record of the reference and never sends;
for a reference it holds no binding for, it answers `UNKNOWN`.

- It never contradicts an earlier answer: a transaction hash and a wallet
  operation id, once reported, are reported again unchanged, after a restart
  too, until `retire` lets the binding go, and a reference reported not
  submitted shows no transaction later unless a later `execute` of it sent one.
  The agent refuses an answer that changes them; a provider keeps the first and
  asks the wallet again before handing the reference to `retire`. The Circle
  adapter falls short of this in two cases: an answer its journal could not
  take lives only in its memory until its next journal step, so a restart
  before then loses it ([Python runtime](python-runtime.md),
  [Rust runtime](rust-runtime.md)); and one a damaged journal contradicts is
  dropped at that step, so a later `reconcile` reports the journal's record.
- A provider refuses to recover a checkpoint holding a transaction hash unless
  the wallet's answer for its reference names that hash, and the checkpoint's
  wallet operation id when it names one: keep both in the durable record.
- It withdraws any time `retire` gave the reference. A provider recovering an
  older checkpoint looks up each saved transaction again, and hands the
  reference to `retire` again once no session needs it.

**Without a transaction hash.** A result has none before the service names a
transaction, when its answer was lost, or when nothing was sent. Unless it is
`FAILED`, the runtimes and the agent keep the operation unresolved, never send
it again, and reconcile it:

- a provider reconciles it before preparing another collection for its
  sessions, and asks the wallet about one it restored without a hash, or kept
  against an answer that disagreed, before handing it to `retire`;
- the agent finds an uncertain opening by its `SessionOpened` log, treats an
  opening or top-up whose authorization expired unused as `FAILED`, and asks
  the wallet about its own exit before deciding whose close closed its session.

**Funding (agent).**

- `sign_funding_authorization(authorization)` returns 1 to 65,536 bytes that
  USDC accepts for exactly that authorization from `address`: an ECDSA
  signature for an account without code, or bytes its ERC-1271
  `isValidSignature` approves. The agent checks them before submitting an
  opening. A smart account must have code when it funds.
- A wallet whose service creates the signature as it executes declares
  `signs_at_execute = True` (exactly `True`): `sign_funding_authorization`
  returns a placeholder, and `execute` replaces it with the real signature
  before sending. The agent then checks only that the registry binds its id to
  `address`; USDC checks the real signature on chain. The placeholder is still
  1 to 65,536 bytes, and the binding is to the call as given, placeholder
  included: `execute` sends it with only the signature replaced.

**`retire` (optional).** Python `retire(*, operation_refs, at)`, with `at` in
Unix seconds; Rust `retire(&self, operation_refs: &[String], at: SystemTime)`.

- From `at` on, the wallet may forget the bindings of `operation_refs`; none is
  presented again. A wallet that forgets keeps the earliest `at` given for each
  as durably as the binding, so a restart before `at` loses nothing, and a
  `reconcile` withdraws it.
- A wallet clears a reference's retirement time whenever its record of that
  reference gains a transaction hash, as a `reconcile` does; the binding then
  stays until `retire` names it again. A provider may hand over a reference
  with no hash, and asks about one it restores without a hash from an older
  checkpoint only at its next reconcile: a binding forgotten as soon as its
  hash arrived would leave that reconcile `UNKNOWN`, with no hash to read on
  chain. The Circle adapter meets this by construction: only an `execute` or
  `reconcile` of a reference records its hash, an `execute` on the record it
  has just created and a `reconcile` after withdrawing the retirement time, or
  an answer of theirs it kept in memory, written at the start of a later
  journal step before that step marks anything.
- It keeps any binding whose call may have reached its service without a
  transaction to show for it.
- The agent passes the references of a session it retired, once its
  checkpoint no longer names them, and only those its own record holds a
  transaction hash for; that hash may come from the chain, as for an opening
  found by its log, rather than from the wallet; it may pass none at all. A
  provider may pass one with no hash: one whose call never began, or whose
  record and the wallet's latest answer both name none.
- An error refuses the call: a provider tries again at least one settlement
  interval later; the agent reports `HookError("retire", ...)` to `on_error`
  and goes on, leaving the records.
- A provider makes one call at a time, outside its locks, for each whole
  second its references fall due in, with the latest `at` of that second:
  never before a reference is due.
- Without it, the default, the wallet keeps every binding, which is always
  safe; its records then grow with every operation.

**`near_capacity` and `max_call_bytes` (optional, provider).**

- `near_capacity` is true while the wallet's records are near the bound past
  which it refuses new calls as not submitted. A provider reads it on every
  maintenance pass, so it must not block, and reports each rise to `on_error`
  as a `HookError` with operation `"wallet"`. In Python it is a property, read
  on the event loop, and only `True` itself counts. The default is never.
- `max_call_bytes` is the largest calldata the wallet accepts in one call, in
  Python an `int` attribute: at least 452, one settlement call, read once when
  the runtime is built. Each batch the runtime builds fits it, and a larger `settleBatch` is
  refused before any intent is recorded. The default, 61,440, is the Circle
  adapter's bound; a wallet with a smaller limit, or a gas limit, states its
  own.

**Concurrency and time.**

- The runtimes and the agent may call `execute` and `reconcile` at once, from
  several threads in Python and several tasks in Rust, for different
  references or the same one: a provider's recovery reads before taking the
  runtime's locks, maintenance asks about references it hands over, and an
  agent's `execute` that outlived its wait goes on while a later `reconcile`
  runs. A wallet serializes access to its own records; a `reconcile` of a
  reference whose `execute` has not returned answers from the binding that
  `execute` saved, never `FAILED`.
- A Rust runtime may drop any wallet future before it completes, as at
  shutdown: `execute`, `reconcile` or `retire`, each of which must leave the
  wallet's records consistent. The binding saved before anything was sent
  keeps an `execute`'s reference reconcilable; the Circle adapter finishes the
  call on a task of its own, so its answer is still recorded.
- The agent waits at most its `timeout_ms` for each wallet call: 10 seconds by
  default, 300 with the built-in Circle setup. An `execute` still running then
  leaves its operation `UNKNOWN`, to be reconciled; a signature that has not
  returned refuses that funding, with nothing sent; a `reconcile` reads
  `UNKNOWN`; and a `retire` is reported to `on_error`. Choose a timeout longer than the wallet's
  usual execute.
- `execute`, `reconcile` and `sign_funding_authorization` must return in
  bounded time. The provider runtimes do not bound them: each collection makes
  its wallet call under the runtime's operations lock, so a call that never
  returns stalls every collection after it. The agent stops waiting at its
  `timeout_ms`, as above. The Circle adapter stops each read command, such as
  a page of the transaction list a `reconcile` reads, at its CLI timeout (240
  seconds by default). It deliberately sets no limit on `wallet execute` or
  `wallet sign`: stopping one could lose the answer to a call Circle may
  already have accepted, so it waits for the CLI to finish. MPPI does not rely
  on the CLI's own limits; with Circle CLI 1.1.4 (verified) each request to
  Circle times out after 30 seconds, and `wallet execute` stops waiting for its
  challenge after about 60 seconds and for its transaction after about 120;
  `wallet sign` does not poll. Other CLI versions may differ, and the adapter
  accepts only the verified 1.1.4 today.

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
