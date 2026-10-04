# Rust provider runtime

`mppi_provider::runtime::Mppi` implements the native `mppi.v1` provider payment
services. The host supplies its verified deployment profile, selected RPC,
wallet capability, coverage callback and execution callback. It also owns its
listener, TLS, inference gateway, token counts and retained payment history.

The runtime implements live-session payment handling, supplied-history recovery
and ordered batch collection. Start with the ordered
[installation and onboarding walkthrough](rust-cli.md). The crate is unpublished;
this is not a production release or a bundled inference gateway.

## Compose a provider host

After onboarding, your executable loads the saved configuration using
`cli::host_configuration(std::env::args_os().skip(1))`. That accepts the arguments
forwarded by `mppi provider serve`, including the explicit trusted profile and
RPC environment-variable name. Keep a clone of the validated listener settings
before moving the configuration into the runtime.

The following composition function uses only the SDK and Rust's standard
library. Add `mppi-provider` as a [path dependency](../sdks/provider/rust/README.md#use-locally)
in your host crate. The host supplies the arguments, secret loading, callbacks,
Tokio application lifecycle and the private path for its wallet operation record.
The function checks the established Circle wallet; it provisions no new wallet.

```rust
use std::{error::Error, io, path::Path, sync::Arc};
use mppi_provider::{
    circle::{CircleClient, CircleCredentials, CircleDeveloperWallet},
    cli::HostConfiguration,
    contract::ContractCalls,
    follower::ChainFollower,
    interfaces::{CoverageProvider, ExecutionProvider},
    runtime::Mppi,
};

pub async fn compose(
    host: HostConfiguration,
    credentials: CircleCredentials,
    operation_record: &Path,
    usdc_domain_name: &str,
    usdc_domain_version: &str,
    coverage: Arc<dyn CoverageProvider>,
    execution: Arc<dyn ExecutionProvider>,
) -> Result<Arc<Mppi>, Box<dyn Error + Send + Sync>> {
    let client = Arc::new(CircleClient::new(credentials, 30_000)?);
    let wallet_id = host.config.identity().wallet_id().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "onboarding wallet ID required")
    })?;
    let identity = client
        .wallet(wallet_id, host.config.chain().chain_id())
        .await?;
    if identity.address() != host.config.identity().wallet_address() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput, "operator wallet mismatch"
        ).into());
    }
    client.verify_credentials(&identity).await?;
    let wallet = Arc::new(CircleDeveloperWallet::new(
        client, identity, operation_record,
    )?);
    let follower = Arc::new(ChainFollower::new(
        host.chain.as_ref().clone(),
        host.config.chain().max_age_ms(),
        host.config.payment().collection_margin_ms(),
        None,
    )?);
    let calls = ContractCalls::new(
        *host.profile.domain(), usdc_domain_name.into(), usdc_domain_version.into(),
    )?;
    Ok(Arc::new(Mppi::new(
        host.config, follower, calls, wallet, coverage, execution,
    )?))
}
```

Construct `CircleCredentials` using the secret manager's API-key value and
decoded 32-byte registered entity secret. Supply the USDC signing-domain name
and version verified for the selected deployment; they are explicit inputs to
`ContractCalls`, separate from the MPPI domain in the profile.
Do not place credential values in the
configuration, command arguments or application logs. The composition function
performs Circle access checks and opens the supplied operation record;
`Mppi::new` itself starts no network work. The SDK owns payment validation. Your
coverage and execution implementations own upstream calls, token measurements,
job termination and final outcome reports; declaring `upstream.url` alone does
not install an inference adapter.

For the SDK-owned listener, pass the composed runtime, validated `ListenerConfig`,
your shutdown `CancellationToken` and an explicit `ServePolicy` to
`standalone::serve`. It starts the runtime and serves discovery plus both native
gRPC services. The host's Tokio runtime must enable I/O and timers. Cancel the
token and await `serve` to receive `ShutdownResult`; a `ServeError` can also
contain that report through `shutdown()`. Keep the report and any session
checkpoints through your existing retention system. Provider infrastructure is
supplied by your application; the SDK has no database or job supervisor.

The composition uses the packaged [host configuration](../sdks/provider/rust/src/cli.rs),
[Circle adapter](../sdks/provider/rust/src/circle.rs),
[runtime constructor](../sdks/provider/rust/src/runtime/mod.rs) and
[listener lifecycle](../sdks/provider/rust/src/standalone.rs).

## Embed the SDK

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

## Provider callbacks

`CoverageProvider::coverage` receives validated original inference JSON and the
signed `RunContext`. Return the required input, cached-input and output counts
for initial admission. Preparation must not start paid execution. The SDK binds
the current confirmed catalog, checks the signature and calculates aggregate
session coverage before calling `ExecutionProvider::execute`.

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

## Payment state and lifetime

The runtime accepts a confirmed funded opening and authenticates Control with
the fixed session signer. New requests need the current attachment generation,
a signed model/catalog binding and enough cumulative monetary authorization.
Admission reserves shared session coverage atomically. Streaming reports consume
that reservation; they do not change the admitted run's prices.

Only unadmitted requests wait for authorization, within one fixed bounded wait.
An insufficient deposit fails with payment guidance; the SDK does not fund the
session. `TopUpNotice` is a hint, and independently confirmed chain evidence
establishes the deposit. Readiness uses cached coherent observations rather than
per-token RPC calls.

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
references before submission.

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

Recovered history supplies no Control authority and resumes no inference. The
agent must authenticate a fresh attachment. Unfinished recovered runs remain
stopped and retain their reservations until the provider supplies definitive
reports using their original `RunContext`. Repeated reports do not add usage.
Accepted prices remain fixed per run across catalog updates.

`sessions().settle_batch(&ids)` preserves the requested order, including repeated
session IDs and an empty batch. The SDK sizes and simulates the prepared call
from the provider wallet before submission. Automatic maintenance groups due
sessions and divides rejected groups into smaller independently checked calls.
Simulation is advisory and establishes neither submission nor payment.

Canonical evidence determines each target's coverage. A successful transaction
does not prove that every item paid; silent no-ops and skipped items receive no
invented settlement reference. Usage reported while collection is pending does
not replace the original target's token-count baseline. Intersecting collection
requests reconcile the retained call before preparing another payment.

Shutdown reports each pending batch reference once, including empty batches.
Simulation failure or cancellation removes the known unsubmitted intent before
checkpoint export. Once submission might have started, its original identity
remains unresolved until independently confirmed.

The runtime retains a bounded number of sessions and Control attachments.
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
