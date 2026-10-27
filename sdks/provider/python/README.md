# MPPI provider SDK for Python

Version `0.2.0` supports explicit Arc mainnet and testnet selection with validated configuration, discovery, exact payment
arithmetic, canonical chain evidence and native Complete/Control services.
`await Mppi.from_config(...)` composes these services from the native CLI's saved
configuration, the packaged official Arc profile for the selected network and
the existing Circle operator. The provider owns its inference integration and hosting.

```sh
uv pip install 'mppi-provider==0.2.0'
```

Start with the [provider quickstart](../../../docs/provider-quickstart.md) to
install the native CLI, generate declarations, onboard and attach the SDK.
Linux ARM64 has a native CLI release binary; Python providers use that same CLI
for wallet selection, recipient binding and catalog publication. The quickstart also
covers a locked Cargo source build. The shared protocol dependency installs
automatically with this Python package.

The [wallet reference](../../../docs/python-onboarding.md) covers custom adapters
and registry operations. Earlier `0.1.0` integrations passed live Arc testnet
registration, recipient binding, catalog publication, capture and close. See the
[official release record](https://github.com/Alethieum/mppi/releases/tag/v0.1.0)
for that evidence's exact artifact and security scope.

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

## Upgrading from 0.1.2

- `0.2.0` requires `mppi-protocol==0.1.4`, which installs with it. A `0.1.2`
  integration ran on protocol `0.1.1`, so protocol `0.1.3`'s non-additive
  changes apply too: `CircleAgentWallet(chain=..., address=...,
  checkpoint_path=...)` replaces the `wallet` and `implementation` pins, and
  `supported_account_pins` is gone; `OpeningEvidence` and `ClosingEvidence`
  carry `observation` instead of `block`, and `ChainReader.agent_wallet` takes
  that observation. See the [protocol package notes](../../../proto/python/README.md).
- The `mppi` CLI and this package read and write the same
  `<config>.wallet-operations.json`. Upgrade both to `0.2.0` together: `0.1.2`
  of either refuses a journal `0.2.0` has saved. The journal's lock is now held
  only while the journal is read or replaced, never across a Circle call, so
  `mppi provider update` can run while the host serves. An execute or
  reconcile waits for that lock up to the adapter's `timeout_seconds` (240 by
  default). Within one process an adapter runs one operation at a time, CLI
  commands included, so an operation may also wait behind another's command.
- The `ProviderSessions` Protocol has a new member, `recover_current(evidence)`.
  A host class written against it without that member no longer type-checks:
  a type checker reports the missing member where the class is used as
  `ProviderSessions`, and refuses to instantiate a class that subclasses the
  Protocol without implementing it. At run time such a subclass inherits the
  Protocol's stub, which returns `None`. Implement the method, or forward it to
  the SDK's `sessions.recover_current`.
- `await Mppi.from_config(..., wallet=operator_wallet)` composes the runtime
  with any `TransactionWallet`. Setup checks its address against the saved
  operator and verifies the deployment and USDC domain; the wallet's own
  account, network and access are its own to check, so call a
  `CircleAgentWallet`'s `verify_wallet()` before passing it. Without `wallet=`,
  the Circle path is unchanged.
- A wallet may have three optional members the runtime uses when present, so
  the `TransactionWallet` Protocol is unchanged: `retire(*, operation_refs, at)`
  receives references the runtime no longer needs, with the Unix time from
  which they may be forgotten, which a later `reconcile` of the reference, or
  its record gaining a transaction hash, withdraws; a `near_capacity` property
  has each rise
  reported as an `on_error` `HookError`; and `max_call_bytes` (default 61,440,
  the Circle adapter's bound) sizes each batch the runtime builds. The runtime
  may now call `execute` and `reconcile` for different operations at once, from
  several threads: a custom wallet serializes its own records, as both Circle
  adapters do, one operation at a time. The
  [wallet requirements](../../../docs/sdk-interfaces.md#wallet-requirements)
  state everything the runtime assumes of a wallet.
- A session closed on chain leaves the runtime after `closed_retention` seconds
  (24 hours by default, set on `Mppi` or `Mppi.from_config`), and
  `Hooks(on_session_retired=...)` receives its id. No session count limits
  openings. A `SessionOpen` for a closed session the runtime no longer holds,
  and a recovery from a record that `RecoveryEvidence.is_final()` reports final
  once the chain shows the session closed, are refused `SESSION_NOT_READY` /
  `SESSION_CLOSED`. Pass such final records to `recover` at startup rather than
  skipping them: their refusal hands the wallet each reference only they still
  name, if the wallet's `reconcile` of it agrees with the record: the same
  reference, the same transaction hash or none on both, and the same wallet
  operation id when the record names one. Maintenance makes the same
  check before it hands over a reference no wallet answer in this process has
  matched (one restored without a transaction hash, or kept against an answer
  that disagreed); every other hand-over makes no wallet call first.
- Collections: a batch fits the wallet's `max_call_bytes`, at most 136 sessions
  for Circle, and maintenance splits the rest; a host's own `sessions.settleBatch` over
  it is refused before any intent is recorded. A close mined and reverted is
  prepared again under a successor of its reference (`<reference>.1`, `.2`, …),
  at most once per settlement interval, and sent only once the successor
  simulates: a failed simulation sends nothing, and a `close` call then fails
  with its error. A settlement or close that never began
  (a `FAILED` result with neither a transaction hash nor a wallet operation id)
  is restored from a checkpoint as `FAILED`, so the live rules prepare it again.
- A session's waiting runs are admitted in the order they first waited, while
  each voucher arrives within the runtime's turn; `0.1.2` could admit a later
  run first.
- `Hooks` gains `on_session_retired` and `on_timing` after its `0.1.2` fields,
  so positional construction is unchanged. `on_timing` receives an
  `OperationTiming` for each request's admission and each settlement or close,
  in milliseconds: the host's coverage callback, chain reads, the wallet's call
  and waiting.
- An exception other than `UpstreamFailure` from the host's coverage callback
  reaches the agent as `STATE_UNAVAILABLE`, without its text. Recovery refusals
  name the check that failed: `SESSION_NOT_READY: <check>`. An invalid run or
  voucher signature in the evidence still raises `ValidationError`
  (`INVALID_VALUE`), now with that message; a run whose model has no rates in
  its catalog is refused `STATE_UNAVAILABLE`, where `0.1.2` raised `KeyError`.

## Embedded provider runtime

The usual Circle composition is:

```python
from mppi_provider import Mppi

sdk = await Mppi.from_config(
    "mppi.toml",
    testnet=True,
    rpc_env="ARC_RPC_URL_TESTNET",
    coverage=prepare_required_tokens,
    execution=execute_and_report,
)
```

Run this on the serving event loop. The helper verifies the saved operator and
selected deployment, uses the authenticated Circle Agent Wallet CLI and the
original onboarding wallet-operation record, and constructs the wallet, chain
follower and contract calls.
`rpc_env="ARC_RPC_URL_TESTNET"` selects a trusted RPC environment variable;
`profile=load_profile(Path("trusted-profile.json"))` selects an explicit
trusted profile using `mppi_protocol.deployment.load_profile`. With
`testnet=True`, the default profile is packaged official testnet; the RPC comes
from the validated configuration unless `rpc_env` is supplied. It starts no
listener or inference job. The host owns runtime startup, its server and shutdown.
`wallet=operator_wallet` composes another `TransactionWallet` instead of the
Circle one, such as one whose key an HSM or KMS holds; setup checks its address
against the saved operator but cannot verify its account, network or access.

For custom integrations, the explicit constructor remains available.
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
close intent has its own reference, which a retry of a close mined and
reverted replaces with its successor; confirming an earlier capture does not
report that close as confirmed. The
background schedule groups collection requests into batches that fit the
wallet's optional `max_call_bytes` (61,440 bytes by default, at most 136
sessions; see the runtime reference). `settleBatch(session_ids)`
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
`RecoveryEvidence.is_final()` marks a closed session's record with nothing left
to recover: recovery refuses it, but pass it at startup all the same, since the
refusal hands to retirement each reference it names that the wallet's own
record agrees with. The runtime retires a closed session after
`closed_retention` seconds (24 hours by default) and limits no session count.

A known opening before its first authenticated attachment can be checkpointed
at generation zero. This private evidence has no accepted voucher, usage, runs
or reservations and remains unready. The provider must retain complete history;
zero on-chain settlement alone cannot establish that it was unused. Recovery
preserves any refund-only close intent and assigns no owner. Fresh authentication
creates generation one; generation-zero snapshots are never valid on Control.

Wallet adapters must durably bind an operation reference to its exact intended
call and retain available wallet/transaction identifiers. These requirements do
not add a provider database. The shared Circle Agent Wallet adapter retains its
wallet-operation record, while each application owns its payment-state retention.
The native CLI and registry helpers provide onboarding and submission.

## Lifecycle observers

Pass `hooks=Hooks(...)` to the runtime/client for typed lifecycle observations.
`on_timing` reports where each admission and collection spent its time. See the
[hook payloads, ordering and bounds](../../../docs/lifecycle-hooks.md).

## License

Copyright 2026 MPPI contributors. Licensed under the [Apache License, Version 2.0](LICENSE).
