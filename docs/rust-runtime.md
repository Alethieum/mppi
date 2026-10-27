# Rust provider runtime

`mppi_provider::runtime::Mppi` implements the native `mppi.v1` provider payment
services. The host supplies its verified deployment profile, selected RPC,
wallet capability, coverage callback and execution callback. It also owns its
listener, TLS, inference gateway, token counts and retained payment history.

The runtime implements live-session payment handling, supplied-history recovery
and ordered batch collection. Start with the ordered
[provider quickstart](provider-quickstart.md). Version `0.2.0` selects a packaged
official mainnet or testnet profile from the resolved configuration network; an
explicit trusted profile is also supported.

## Compose a provider host

After onboarding, your executable loads the saved configuration using
`cli::host_configuration(std::env::args_os().skip(1))`. It accepts the arguments
forwarded by `mppi provider serve`, including optional `--mainnet` / `--testnet`
network overrides, an optional explicit `--profile`, and the RPC environment-variable
name. Without a network flag it uses the saved configuration network.
Keep the validated listener settings before moving the host configuration.

Use the SDK's composition helper in your provider host:

```rust
use std::{path::Path, sync::Arc};
use mppi_provider::{
    cli::HostConfiguration,
    interfaces::{CoverageProvider, ExecutionProvider},
    runtime::Mppi,
    setup::{self, SetupError},
};

pub async fn compose(
    host: HostConfiguration,
    operation_record: &Path,
    coverage: Arc<dyn CoverageProvider>,
    execution: Arc<dyn ExecutionProvider>,
) -> Result<Arc<Mppi>, SetupError> {
    Ok(Arc::new(setup::runtime_from_config(
        host, operation_record, coverage, execution,
    ).await?))
}
```

Use the authenticated Circle Agent Wallet CLI `1.1.4` and the retained onboarding
operation record: for `mppi.toml`, this is `mppi.toml.wallet-operations.json`.
Do not select a new record to replace an unresolved operation. Circle login and
renewal stay with the operator; keep its authentication state outside MPPI config.

The helper checks the existing Circle operator against the saved identity,
verifies deployment and USDC domain evidence, constructs the chain follower with
the configuration's freshness and collection margin, and supplies contract calls.
It creates no wallet and starts neither the runtime nor a listener. Advanced
integrations can still construct `Mppi::new` with their own wallet and chain capabilities.
`setup::runtime_from_config_with_options(..., RuntimeOptions)` accepts explicit
`CircleOptions`, follower polling, runtime limits and lifecycle hooks.
`CircleOptions` selects a trusted executable prefix, read timeout and history-page
bound. Runtime composition requires an existing wallet-operation record.

For another `TransactionWallet`, such as one whose operator key an HSM or KMS
holds, call `setup::runtime_from_config_with_wallet(host, wallet, coverage,
execution, RuntimeOptions)`. It refuses a wallet whose address is not the saved
operator before any chain read, then verifies the deployment and USDC domain
and composes the runtime as above; `RuntimeOptions::circle` is not used. Setup
cannot check another wallet's account, network or access: the wallet must
submit from the saved operator on the selected network and meet the
[wallet requirements](sdk-interfaces.md#wallet-requirements), binding each
operation reference durably before submitting it (see `local_state::LocalState`
below).
`mppi provider serve` launches such a host as before. The `onboard`, `update`
and `verify` commands use the Circle wallet; with another wallet, call the
library functions behind them: `onboarding::onboard` and
`provider_update::update` take any `TransactionWallet`, and
`verify::verify_endpoint` checks the endpoint without a wallet.

Your coverage and execution implementations supply upstream calls, token
measurements, job termination and final reports. Declaring `upstream.url` alone
does not install an inference adapter.

For the SDK-owned listener, pass the composed runtime, validated `ListenerConfig`,
your shutdown `CancellationToken` and an explicit `ServePolicy` to
`standalone::serve`. It starts the runtime and serves discovery plus both native
gRPC services. The host's Tokio runtime must enable I/O and timers. Cancel the
token and await `serve` to receive `ShutdownResult`; a `ServeError` can also
contain that report through `shutdown()`. Keep the report and any session
checkpoints through your existing retention system. Provider infrastructure is
supplied by your application; the SDK has no database or job supervisor.

The composition uses the packaged [setup helper](../sdks/provider/rust/src/setup.rs),
[host configuration](../sdks/provider/rust/src/cli.rs),
[Circle adapter](../sdks/provider/rust/src/circle.rs),
[runtime constructor](../sdks/provider/rust/src/runtime/mod.rs) and
[listener lifecycle](../sdks/provider/rust/src/standalone.rs).

## Embed the SDK

The setup helper can also compose an embedded runtime. To supply your own wallet
and chain capabilities, use the explicit construction sequence:

1. Validate `config::ProviderConfig` and an explicit deployment manifest. Build
   `chain::ChainReader` with the selected RPC and deployment pins.
2. Construct `follower::ChainFollower` with the configuration's freshness and
   collection-margin values. Supply a `TransactionWallet` whose address matches
   the configured operator.
3. Implement `CoverageProvider` and `ExecutionProvider`, then construct
   `runtime::Mppi::new`. Construction starts no network I/O.
4. Call `ProviderLifecycle::grpc_services` and register **both returned services**
   with the host's tonic server. The returned wrappers enforce declared gRPC
   message lengths before decoding; keep them around the generated services.
   Your own services, such as an estimate endpoint, can be added to the same
   server, and your HTTP routes served beside `discovery(path)`.
   `standalone::serve` serves only the MPPI services and the three discovery
   documents, so a host with routes of its own serves the runtime this way.
5. Start the host server and call `start`. Publish the bytes and headers from
   `discovery(path)` at the host's HTTPS origin. Check returned readiness before
   advertising paid availability; the runtime continues checking freshness.
6. On shutdown, pass an absolute `tokio::time::Instant` deadline to `shutdown`.
   Inspect unresolved runs and pending operations and retain their evidence
   through the provider's own integration. Stop the host server separately.

`ProviderLifecycle`, `ProviderRuns` and
`ProviderSessions` are traits: import the applicable trait to call its methods.
The [independent gateway test](../sdks/provider/rust/tests/runtime_gateway.rs)
shows the complete composition against disposable local infrastructure. Its
HTTP adapter, wallet and synthetic counts are test fixtures.

## Chain reads

`http::HttpJsonRpc` sends only reads: `eth_chainId`, `eth_getBlockByNumber`,
`eth_getTransactionReceipt`, `eth_getCode`, `eth_getStorageAt`, `eth_call` and
`eth_estimateGas`. A read that fails for a transient reason is repeated at most
twice, after a random delay of up to 250 ms and then up to 1 s: a failed or
dropped connection, a timeout, HTTP 408, 425, 429, 500, 502, 503 or 504, or
JSON-RPC error -32005, -32007 or -32014. A refusal's `Retry-After`, in seconds or
as an HTTP date, is waited before that delay. Repeats start within one attempt's
timeout plus 1.25 s: a wait that would end later is not begun, and the read
fails at once. A repeat of a batch sends only the calls not yet answered. A
-32014 still refused once the repeats are spent is `ChainError::Lagging`.
Connections are kept alive between reads.

`chain::ChainReader` sends each step's reads through `Rpc::call_many`, which
`HttpJsonRpc` sends as JSON-RPC batches of at most 32 calls and 1 MiB, one
exchange after another; a call longer than 1 MiB is refused before anything is
sent. State reads are pinned to one block hash, and the same `call_many` reads
that block back by number, last. The RPC's chain id and the pinned contracts'
code are read once per reader. A follower refresh reads the header, the
implementation slots and Multicall3 `aggregate3` calls at the head, each of at
most 512 views and one provider's registry entry. Each call reports the block it
ran at; one that ran at another block than the header is read again pinned to the
header's block, so a snapshot never combines two blocks. A refresh is one round
trip while its calls fit one batch, up to about 2,300 watched sessions; each
further batch adds one, and calls read again add one per batch of them. Without
Multicall3's verified code at its canonical address, a refresh is the head and
then the reads pinned to it, one exchange per batch.

`chain::ChainError` keeps a failure's cause without endpoint, credential or remote
text: `Transport` (an `http::TransportError`, including the HTTP status), `Rpc`
(the JSON-RPC error code), `Reverted` (an executed `eth_call` or `eth_estimateGas`
reverted, with the revert data's 4-byte selector when it has one), `Lagging`,
`InvalidEvidence` and `Unavailable`. A lagging RPC serves a head behind accepted
evidence, or has not served or imported a block yet: the follower keeps its
accepted snapshot, which ages as usual. Contradicting evidence still fails
readiness closed.

A wallet that signs and submits its own transactions reads its nonce, fees and
balance and sends `eth_sendRawTransaction` through `http::HttpTransport::request`,
which applies the same TLS and size policy and sends each request once. Clone the
transport given to `HttpJsonRpc` to share its connections. Treat a failed
submission as one that may have reached the node, unless the error is
`TransportError::Invalid`, and reconcile it by transaction hash.

## Provider callbacks

`CoverageProvider::coverage` receives validated original inference JSON and the
signed `RunContext`. Return the required input, cached-input and output counts
for initial admission. Preparation must not start paid execution. The SDK binds
the current confirmed catalog, checks the signature and calculates aggregate
session coverage before calling `ExecutionProvider::execute`.

`coverage` is also each request's preflight. It runs once the signed request
has passed its checks, before any authorization wait, voucher or reservation,
so it can refuse work your gateway would not serve. Return
`runtime::UpstreamFailure` with your JSON error body to end the request with
that body and `FAILED_PRECONDITION`; any other error ends it
`STATE_UNAVAILABLE`. Either way the run is not admitted.

Execution returns a stream of the original JSON response objects. The runtime
keeps payload bytes intact and adds protocol outcomes separately. A nonstreaming
request must produce exactly one response object. The provider reports
cumulative counts through `ProviderRuns::report_usage` and obtains additional
reservation with `require_coverage`. Accepted prices stay fixed for each run.

When coverage renewal fails, stop the provider's job. The SDK also exposes a
`CancellationToken` for Control loss, invalid chain readiness, deadlines and
other termination requests. A signal does not establish that execution stopped.
Submit definitive counts and status together through `report_terminal`; only
that accepted final report releases the unused reservation. Missing final
reports retain unresolved work and prevent new admission and cooperative close.
Late vouchers do not restart terminated requests.

Callbacks execute outside the payment-state lock. Provider implementations must
avoid blocking the async executor and must own the lifetime of their jobs.
For a validated upstream JSON error, return `runtime::UpstreamFailure`; arbitrary
integration errors are sanitized. An admitted upstream failure still requires
a definitive failed report before the SDK can emit its final accounting.
When `require_coverage` refuses a run, report its terminal and end the stream.
An execution error other than `UpstreamFailure` after that refusal ends the
request with the refusal itself, such as `INSUFFICIENT_DEPOSIT`, rather than
`STATE_UNAVAILABLE`; an `UpstreamFailure` still delivers its JSON error.

## Payment state and lifetime

The runtime accepts a confirmed funded opening and authenticates Control with
the fixed session signer. New requests need the current attachment generation,
a signed model/catalog binding and enough cumulative monetary authorization.
Admission reserves shared session coverage atomically. Streaming reports consume
that reservation; they do not change the admitted run's prices.

Only unadmitted requests wait for authorization, within one fixed bounded wait.
They are admitted in the order they first waited, each for a turn of at most two
seconds. An insufficient deposit fails with payment guidance; the SDK does not
fund the session. `TopUpNotice` is a hint, and independently confirmed chain
evidence establishes the deposit. Readiness uses cached coherent observations
rather than per-token RPC calls.

A valid voucher from the current authenticated attachment that exceeds the
cached deposit triggers one canonical refresh, bounded by `max_age_ms` and
outside the accounting lock. The SDK rechecks ownership, readiness and the
deposit bound afterward. Covered vouchers use the cache. Execution continues
under its existing funded limit; a denied grant still terminates the run.

`sessions().settle` captures validated actual cumulative usage and leaves the
session open. An agent's Control `CloseRequest` requires signed close authority.
The trusted provider integration can also explicitly call `sessions().close`;
the runtime then applies the contract's voucher and deadline rules. Close
captures usage and refunds the remainder. The highest voucher is an
authorization ceiling, not the amount owed.
Unknown transaction outcomes retain their original operation identity; they do
not cause a second submission. Wallet adapters must durably bind operation
references before submission. `OperationResult::cause` says why a wallet call
ended unresolved or failed, in the SDK's or the adapter's words, never remote
text; it is not compared. A hashless `Failed` result means the adapter proved
the call never reached its service; a settlement then prepares a new reference
at its next collection. Recovery keeps such a result `Failed` when it has no
wallet operation id either, so the same holds after a restart; any result with
a wallet handle is read again. `local_state::LocalState` is the record the Circle
adapter binds its references with, and other adapters can use it: one JSON object per file,
locked while a handle is open (`open` waits up to one second for another
holder, then refuses), replaced atomically and synced on every `write`, and
refused if the file is a link, has group or other permissions, or is not strict
JSON. After a failed write the handle refuses further use; reopen it to read
what is durable. Keep one record per wallet, back it up, and never replace it
while an operation is unresolved.
A failed transaction does not end close intent:
when the wallet attests that submission never began (`Failed` without a
transaction hash), the runtime prepares the close again from current chain state
under the same operation reference, on the next close call and in the background
at most once per settlement interval. A close mined and reverted (`Failed` with a
transaction hash) changed no state, but its reference stays bound to that
transaction: the runtime prepares it again under a successor reference, the
close reference with its retry count after a dot (`close:<id>.1`, then `.2`),
at most once per settlement interval, on a `close` call or in the background.
The session's close reference then names the new one, so its checkpoint
recovers; a runtime restored from an older checkpoint derives the same
successor, and the wallet's binding of it continues that retry. The runtime
simulates each successor from the provider wallet first, as it does a batch: if
the simulation fails, nothing is recorded or sent, the `close` call returns the
simulation's error, and the next attempt waits a settlement interval. A close
waits for the session's active runs to end. Accepting close
intent stops them and sends the owner one `Drain`; a later `close` call, the
agent's `CloseRequest` or a maintenance pass only continues the close, and
maintenance reconciles a submitted close on every pass until it resolves.

## Recovery and batch collection

`sessions().checkpoint(id)` returns detached `RecoveryEvidence`. The provider
retains this evidence through its own infrastructure and supplies it to
`sessions().recover(evidence)` after starting the replacement runtime. No SDK
database or persistence adapter is required.

A known unused opening before first attachment can be checkpointed at generation
zero in this private evidence. It has no accepted voucher, usage, runs or
reservations and is never ready. The provider supplies complete authoritative
history; chain records alone do not prove it unused. Recovery preserves close
intent and operation identity, installs no owner, and leaves generation zero
unchanged until fresh authentication creates generation one. Ordinary Control
snapshots continue to require a positive generation.

Recovery verifies the canonical opening and historical blocks, exact published
catalogs, original signed run bindings, voucher, complete run history, usage and
reservations. It also reconciles retained payment operations against their
original transaction evidence. Invalid or incomplete history cannot partially
replace live state. A live attachment or unfinished local work prevents
replacement; accepted generations, usage, close intent and known operation
identities cannot regress.

A retained transaction hash must also match the wallet's read-only lookup of
the original operation reference before recovery trusts its chain outcome.
Unavailable or conflicting wallet evidence rejects restoration; it cannot turn
an unrelated reverted transaction into permission to submit again.

With the Circle adapter, an answer from Circle that the adapter's journal could
not take is returned `Unknown` with its transaction id and hash, and kept only
in that adapter's memory until its next journal step writes it
([local records](rust-cli.md#local-records-and-results)). If the process stops
first, the journal row holds no transaction id or hash, the adapter's lookup of
that reference is `Unknown` without a hash, and recovery of a checkpoint holding
the hash is refused with `SESSION_NOT_READY: the wallet's record of a saved
transaction differs from the evidence`, since recovery needs the wallet's own
record of the transaction to show it. The restarted adapter never writes it.
Circle's documentation describes no lookup of a transaction by the idempotency
key the adapter sends, `circle transaction list` prints a wallet's transactions
without their idempotency keys, and MPPI documents no procedure for writing a
transaction id and hash into the adapter's journal: there is no documented
remedy.

A refusal for missing or unavailable evidence is `SESSION_NOT_READY` with reason
`STATE_UNAVAILABLE`, and its `Status` message names the check that failed in the
SDK's own words, such as `SESSION_NOT_READY: a catalog has no publication
evidence` or `SESSION_NOT_READY: the wallet could not look up a saved
transaction`; `checkpoint` names its refusals the same way. A refusal for
evidence that contradicts itself, the chain, the wallet or a session the
runtime already holds has reason `INCONSISTENT_STATE` and names its check the
same way, such as `SESSION_NOT_READY: the checkpoint block contradicts the chain
or the opening`. A check of several conditions has one label, and the Python
runtime gives each check both runtimes make the same label. Its wallet-lookup
label also names the error type in parentheses, and these refusals keep the
classification each runtime had in 0.1.2:
- an invalid run or voucher signature is `INCONSISTENT_STATE` here, where
  Python raises `ValidationError` (`INVALID_VALUE`) whose message names the
  same check;
- evidence lacking a run the held session has is `STATE_UNAVAILABLE` here,
  `the evidence lacks a run the held session has`, where Python refuses
  `INCONSISTENT_STATE`, `the evidence is older than the held session or
  contradicts it`;
- a checkpoint block no longer canonical returns a chain error here, and an
  unattached checkpoint holding accounting history gRPC `INVALID_ARGUMENT`,
  where Python refuses both `INCONSISTENT_STATE`;
- evidence over the resource limits returns `RESOURCE_EXHAUSTED` here, and run
  history whose counts do not add up `INVALID_ARGUMENT`, where Python raises
  `ValidationError` for the limits and refuses the run concerned under its own
  label.

Log the message; the payment failure is what to act on. Each `recover` restores one
session, so a host can start without a refused session and retry it later.

Recovery reads all its evidence first, including the wallet's lookup of each
saved transaction, and only then takes the runtime's locks to check the
evidence against what the runtime holds and install it. A host can therefore
recover many sessions at once, and collections and openings continue
meanwhile; a change they make while a recovery reads is checked like one made
before it. The wallet's `reconcile` may then run for several operations at the
same time. Each recovery watches its session through the chain follower, and
watches that wait behind a refresh are read together by the next one, so many
recoveries take a few refreshes rather than one each.

Recovered history supplies no Control authority and resumes no inference. The
agent must authenticate a fresh attachment. Unfinished recovered runs remain
stopped and retain their reservations until the provider supplies definitive
reports using their original `RunContext`. Repeated reports do not add usage.
Accepted prices remain fixed per run across catalog updates.

An admitted run that your host never dispatched before a crash has no meter and
no final, so it stays unresolved and blocks its session. If your own records
prove nothing reached the inference backend, report it `CANCELLED` at zero
through `report_terminal`; the session then reattaches with that outcome. A run
that may have done billable work stays unresolved for the operator. A reconnect
may also name a pending run this provider never held: the agent records a run
before its Complete arrives. When every admission is known, the snapshot answers
it `NOT_ADMITTED` and records nothing, and the new generation refuses a late
Complete for it. Every admission is known for a session this runtime opened, or
one restored with `sessions().recover_current(evidence)`. After
`sessions().recover(evidence)` such a run refuses the attachment instead: an
older checkpoint may lack a run that was admitted and served, and answering it
`NOT_ADMITTED` would leave that work uncollected. Use `recover_current` only if
your host checkpoints durably after every admission and before dispatch, and
never restores a checkpoint older than its newest.

**When to checkpoint.** The runtime has no persistence callback and never waits
for your storage. Meters, Receipts, snapshots and close results reach the agent
before your next checkpoint can hold them. If a crash restores a checkpoint
older than what the agent last saw, its reattachment is refused
`STATE_UNAVAILABLE`: the agent names a later Meter sequence than the checkpoint
holds. Nothing is paid wrongly, and the agent keeps its exits (`requestClose`
then `withdraw`, or the idle release), but you can collect only up to the
voucher that checkpoint holds and lose cooperative close for that session.
Checkpoint inside your execution provider after admission and before dispatch,
again when you record each final report, and restore only your newest
checkpoint.

A closed session is terminal on chain: the contract refuses every payment on
it. Once the runtime holds nothing more to do for one (no run active or
preparing, every retained operation resolved, a confirmed close of its own
with its `CloseResult`, and not inconsistent), it retires the session after
`RuntimeLimits::closed_retention`, unless a live attachment owns it. The window
is 24 hours by default; `with_closed_retention(Duration)` selects one hour to
seven days. A close another exit completed, `withdraw` or `releaseIdle`, needs
no `CloseResult`. Within the window an agent can reattach to read the final
state or retry a lost `CloseResult`, and the host can take a final checkpoint.
Afterwards `get` and `checkpoint` return `STATE_UNAVAILABLE`; the optional
`on_session_retired` hook observes the retirement. No session count is limited.

Retirement also bounds the wallet's records. The runtime remembers each
operation reference it prepared or restored, and when each session it covers
stops needing it: when the session retires here, or `closed_retention` after a
later confirmed collection of the session, a capture or close under another
reference, leaves the reference named in neither the session's operation nor
its baseline. Once no covered session needs a reference, maintenance passes it
at once to `TransactionWallet::retire` with the latest of those times as `at`,
from which the wallet may forget it, even a day ahead. A wallet that forgets
keeps `at` durably, so a restart before then loses nothing, and withdraws it on
a `reconcile` of the reference, or when its record of the reference gains a
transaction hash, since a recovery from an older checkpoint may present the
reference again ([wallet requirements](sdk-interfaces.md#wallet-requirements)).
Calls go one at a time, outside the runtime's lock, one for each whole second
of `at` since the epoch, with the latest `at` of that second, so none is
forgotten before it is due (the Circle adapter keeps whole seconds); after a
refusal the rest wait at least one settlement interval. A checkpoint
taken more than `closed_retention` before a later confirmed collection may name
a record the wallet has forgotten, and its recovery is refused. A reference
this process never learned, such as one superseded and not yet passed when the
process stopped, is never passed, and its record stays. No wallet call precedes
a hand-over, except for a reference whose record no wallet answer in this
process has matched: one restored without a transaction hash, or kept against
a wallet answer that disagreed with it. Maintenance asks the wallet about such
a reference first and passes it only if the wallet's record of it agrees;
otherwise its record stays. A recovery that installs a session takes the
references it names off the retirement queue, and a `retire` call skips each
one a session needs again when it checks, just before calling the wallet. One a
recovery names while that wallet call runs can still be marked, and a later
recovery from a checkpoint naming it is refused once the wallet has forgotten
it. `retire` and `near_capacity` are
defaulted trait methods: a wallet that keeps every record needs neither. When
`near_capacity` turns true, maintenance reports one `on_error` `HookError` with
operation `"wallet"` and code `ResourceExhausted`. Close long-lived sessions
before the wallet refuses new operations.

The runtime never reinstalls a closed session. A `SessionOpen` for one it does
not hold is refused `SESSION_CLOSED`. `recover` and `recover_current` refuse
evidence that `RecoveryEvidence::is_final()` reports final once the chain shows
the session closed: it records no reservation and no unresolved operation, and
is not inconsistent. Pass such records to `recover` at startup all the same; do
not skip them. The refusal hands to retirement, as if the session retired
then, each operation reference they name whose own record in the wallet names
the same transaction hash as the saved record, or, like it, none, and the same
wallet operation id when the saved record names one. The runtime asks the wallet
first and keeps any other reference, which stays for the wallet's own
reconciliation, so a mixed-up record cannot have another session's record
forgotten. Closed evidence with unresolved work still recovers, and retires
once that work resolves.

`sessions().settle_batch(&ids)` preserves the requested order, including repeated
session IDs and an empty batch. The SDK sizes and simulates the prepared call
from the provider wallet before submission. Automatic maintenance groups due
sessions and divides rejected groups into smaller independently checked calls.
Simulation is advisory and establishes neither submission nor payment.
Every batch fits `TransactionWallet::max_call_bytes`, the largest calldata the
wallet accepts in one call: 61,440 bytes by default, the Circle adapter's bound.
A batch's calldata is 260 bytes plus 448 per session, so by default at most 136
sessions; maintenance collects the rest of the due sessions in further batches
of the same pass, and a `settle_batch` call over the bound is refused
`RESOURCE_EXHAUSTED` before any intent is recorded. A wallet whose real limit is
gas rather than bytes states a smaller size. `Mppi::new` refuses a bound below
one settlement call, 452 bytes.

Canonical evidence determines each target's coverage. A successful transaction
does not prove that every item paid; silent no-ops and skipped items receive no
invented settlement reference. Usage reported while collection is pending does
not replace the original target's token-count baseline. Intersecting collection
requests reconcile the retained call before preparing another payment.

Shutdown reports each pending batch reference once, including empty batches.
Simulation failure or cancellation removes the known unsubmitted intent before
checkpoint export. Once submission might have started, its original identity
remains unresolved until independently confirmed.

The runtime retains a bounded number of Control attachments; sessions are
bounded by retirement, not by a count.
An unauthenticated Control stream has a server-owned 60-second setup deadline.
`RuntimeLimits::with_control_auth_timeout(Duration)` selects a positive limit
of at most one hour for the provider's deployment. This is separate from
Complete's `payment.authorization_wait_ms`. Challenges and opening notices do
not extend it; a successful signed attachment removes the setup deadline.
An earlier client RPC deadline still applies. Expiration releases SDK attachment
capacity even if the client stops reading, without closing funds or changing
usage. A client whose funding preparation outlasts setup must reopen Control
and authenticate its existing session. This bounds idle unauthenticated
retention; it does not replace ingress rate limits against repeated connections.

`RuntimeLimits` can reduce these limits and the protocol's wire limits. Request
history, catalog evidence and output queues are bounded. A stream ending without
a final provider report preserves its reservation and unresolved status. The
host must retain the supplied checkpoint through its own storage integration;
there is no SDK database or automatic recovery service.

Shutdown stops new grants, requests termination and waits within its deadline
for final reports. It returns unresolved work and transaction handles. It does
not infer job termination, close funded sessions or stop the host's server.

## Inspect and stop the runtime

Import `ProviderLifecycle` to obtain `runtime.sessions()`.
`sessions().get(session_id)` returns one known session; `list(cursor, limit)`
returns a page and an optional next cursor. Preserve the returned cursor exactly.
Check each `SessionView`'s `fresh()` and `readiness()` before interpreting its
cached deposit, settlement and status as current. A missing usage value means
the runtime has no accepted usage history for that session, not zero usage.

For an explicit canonical-state observation, keep the configured
`Arc<ChainReader>` when composing the host. Read `block(None)`, call
`verify_deployment(&block)`, then read `session(session_id, &block)` against that
same block. This can confirm deposit, `settled_cumulative` and session status at
the observed block; chain state alone cannot reconstruct delivered usage or
prove that a provider job terminated.

After `settle`, `settle_batch` or `close`, inspect the returned `OperationResult`
and its per-session outcomes. A submitted transaction hash is not a confirmed
capture. `OperationState::Confirmed` requires canonical transaction/block
evidence, and an individual outcome's `target_covered()` and
`settlement_reference()` describe its proven coverage. Repeated targets and
contract no-ops need not produce a new settlement event. Retain unknown
operations and reconcile their original references; do not replace an unknown
payment with a fresh submission.

For the owned listener, cancel the supplied token and await `standalone::serve`.
For an embedded server, call `runtime.shutdown(absolute_deadline)` and stop your
server through its own lifecycle. Both paths return unresolved session/run and
operation information. Preserve that information with `sessions().checkpoint`
evidence through your own retention boundary. Neither shutdown path settles or
closes every funded session. Use the separate payment methods when closure is
intended and their prerequisites are satisfied.

`Hooks::on_timing` receives a `runtime::OperationTiming` for each request's
admission and each `settle`, `settle_batch` or `close` call, including
maintenance's: its time split into the coverage callback, chain reads, the
wallet's call and waiting. See [lifecycle hooks](lifecycle-hooks.md).

These return values and boundaries are defined by the public
[session and lifecycle traits](../sdks/provider/rust/src/interfaces.rs),
[result types](../sdks/provider/rust/src/types.rs) and
[canonical chain reader](../sdks/provider/rust/src/chain.rs).

## Local acceptance fixture

The [Python harness](../tests/test_rust_runtime.py) uses the public agent SDK
against a separate Rust provider process, TLS ingress and a local
OpenAI-compatible streaming endpoint. An owned Anvil node runs the real MPPI
contract. The simulated provider supplies its counts through public SDK methods;
the agent checks payment messages and signatures independently. Tests also
check canonical collection events and recipient/refund balances.

A recovery case replaces the SDK runtime and chain follower inside the provider
process, restores caller-supplied history, collects through a batch containing
the session twice, and requires fresh agent authentication before closure.
The observer verifies actual usage capture and the unused deposit refund.
Duplicate batch targets can have confirmed coverage while the aggregate result
remains unknown: the SDK does not invent an attributable result for a silent
duplicate item. This fixture tests the public recovery boundary, not durable
storage or recovery from a process crash.

This exercises MPPI's payment and integration boundary. It does not certify a
provider's tokenizer, inference engine, storage or recovery infrastructure.
