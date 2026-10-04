# MPPI provider SDK for Python

Version `0.1.0` supplies validated configuration, discovery, exact payment
arithmetic, canonical chain evidence and native Complete/Control services.
The Python runtime supports the paid request loop described below. Production
release remains pending. Shared registry
helpers and the Circle developer-controlled wallet adapter support provider
setup and submission, verified through live Arc testnet registration, recipient
binding, catalog publication, capture and close. The shared native
[provider CLI](../../../docs/rust-cli.md) ships in the Rust package and supports
Python integrations.

Start with the [Python installation and paid-exchange walkthrough](../../../docs/python-quickstart.md).
See [onboarding and wallets](../../../docs/python-onboarding.md) for the operator
wallet, recipient proof, publication sequence and completed-rerun checks.

```python
from mppi_provider import CatalogRow, Rates, publish_catalog

catalog = publish_catalog([CatalogRow("example/model", Rates(input=1_000_000, output=3_000_000))])
```

The provider supplies counts, requirements and definitive outcomes. MPPI computes
and validates their payment meaning. Catalog publication here produces bytes;
it does not make a registry transaction. Pure calculation helpers return immutable
results. The runtime retains live payment state and signals the provider's execution
hook when further work is no longer authorized.

See [SDK interface definitions](../../../docs/sdk-interfaces.md) and the
[protocol profile](../../../docs/protocol.md).

The [runtime reference](../../../docs/python-runtime.md) includes configuration,
HTTP host integration and evidence requirements.

## Embedded provider runtime

Construct `mppi_provider.runtime.Mppi(config, chain=..., contract=..., wallet=...,
coverage=..., execution=...)` on the serving event-loop thread. Supply:

- `ProviderConfig`: validated applied declarations and provider-selected timings.
- `ChainFollower`: the shared follower of the selected verified deployment.
  Its freshness and collection-margin timings must equal the validated config.
- `ContractCalls`: the same MPPI domain and verified USDC funding domain.
- `TransactionWallet`: the configured provider operator's execution capability.
- `coverage`: an async callable `(PreparedRequest, RunContext) -> TokenCounts`.
  Return the prepared prefill bound and at least the configured actual output
  buffer; preparation performs no paid inference.
- `execution`: an async iterator callable `(PreparedRequest, RunContext,
  ProviderRuns, asyncio.Event) -> AsyncIterator[bytes]`. It yields original JSON
  objects after the provider removes any upstream framing.

Unauthenticated Control setup expires after 60 seconds. The optional constructor
keyword `control_auth_timeout` sets a positive timeout in seconds, up to 3,600.
Only successful signed session attachment removes this deadline; challenges and
opening notices do not extend it. Authenticated Control streams may remain open.
This server policy is independent of `payment.authorization_wait_ms` and applies
even when a client omits its own RPC deadline.

The execution integration uses `report_usage(context, cumulative_counts)` and
`report_terminal(context, final_counts, RunStatus)` to supply facts. MPPI measures
no tokens. A definitive final must follow actual provider-reported termination;
iterator EOF, a disconnect and a cancellation signal do not substitute for it.
For an upstream application error, report definitive FAILED usage when known and
raise `UpstreamFailure(original_json_bytes, http_status=...)`.

`require_coverage(context, remaining_tokens)` requests a complete desired remaining
bound at the admitted run prices. It grants only when the combined session target
fits the accepted voucher and confirmed deposit. An already larger reservation
is retained. Only definitive final accounting releases unused coverage. During
ordinary decode, report usage early enough for asynchronous voucher renewal,
then request the next rolling output bound before consuming unreserved work.
A rejected grant sets the event passed to execution. The provider terminates
its actual job and reports final usage; a later voucher does not restart it.

Pass `mppi_protocol.wire.GRPC_OPTIONS` when creating the host's `grpc.aio.server`.
Call `register_grpc(server)` before starting that server, then await `start()` and
check its `Readiness`. Publish `discovery(path)` responses through the host's
HTTP stack on the same HTTPS origin. The host supplies TLS credentials, ingress
and listener lifecycle. MPPI starts no listener and does not serve keyed `/v1`.
An unavailable chain at startup returns not-ready while the follower retries.
Discovery and the first-use baseline become available only after fresh identity
evidence arrives; an outage does not establish empty session history.

`sessions.settle(id)` captures cumulative actual usage and keeps the session open.
`sessions.close(id)` preserves close intent, requests termination and verifies
canonical closure before returning a confirmed result. Pending/unknown calls
retain their original transaction identity; repeated calls reconcile it. The
close intent has its own stable reference; confirming an earlier capture does
not report that close as confirmed. The
background schedule groups collection requests. `settleBatch(session_ids)`
simulates the exact ordered call before wallet submission and reconciles each
target independently. Unknown submission outcomes preserve their original
identity; they never trigger a replacement capture.

Call `shutdown(deadline)` with an absolute serving-loop monotonic deadline.
It stops new grants, signals execution, processes available final reports and
returns unresolved runs/operations. The caller stops its own gRPC server.
Shutdown does not initiate escrow closure or invent final reports.

## Payment-state ownership and recovery

One `Mppi` instance holds the authoritative live payment state for its sessions.
Its Complete and Control handlers may receive different backend connections;
socket equality grants no authority. The provider must ensure every operation
uses that same authoritative state. Independent processes do not share payment
state through this SDK. Routing, worker coordination, durability and recovery
remain provider responsibilities.

A newly observed opening after startup can create first-use state under that
ownership requirement. A historical opening after restart cannot establish zero
usage. `await sessions.checkpoint(session_id)` exports detached
`RecoveryEvidence` for provider-owned retention. `await sessions.recover(evidence)`
revalidates the supplied run proofs, exact prices, accounting, reservations and
canonical chain evidence before installing state. Missing private history returns
`STATE_UNAVAILABLE`; invalid recovery leaves existing state unchanged. Recovery
requires fresh Control authentication and never resumes old execution.
Unresolved old execution prevents a ready replacement attachment.

A known opening before its first authenticated attachment can be checkpointed
at generation zero. This private evidence has no accepted voucher, usage, runs
or reservations and remains unready. The provider must retain complete history;
zero on-chain settlement alone cannot establish that it was unused. Recovery
preserves any refund-only close intent and assigns no owner. Fresh authentication
creates generation one; generation-zero snapshots are never valid on Control.

Wallet adapters must durably bind an operation reference to its exact intended
call and retain available wallet/transaction identifiers. These requirements do
not add a provider database. The agent SDK supplies its own local checkpoint.
The shared Circle developer-controlled wallet adapter and registry helpers
provide the implemented onboarding and submission integration described above.

## Lifecycle observers

Pass `hooks=Hooks(...)` to the runtime/client for typed lifecycle observations.
See the [hook payloads, ordering and bounds](../../../docs/lifecycle-hooks.md).

## License

Copyright 2026 MPPI contributors. Licensed under the [Apache License, Version 2.0](LICENSE).
