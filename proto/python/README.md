# MPPI Python protocol support

Internal shared distribution for the agent and provider SDKs. It contains the
generated `mppi.v1` wire bindings and `mppi_protocol` exact payment primitives.
It is not another role SDK.

The implementation validates catalogs and report transitions, computes exact
amounts, signs and verifies fixed-session proofs, prepares contract calls and
checks canonical receipt evidence. Version `0.1.1` also supplies shared discovery
validation and the Circle Agent Wallet adapter used by both Python roles.
Version `0.1.4`, used by `mppi-agent` 0.1.6 and `mppi-provider` 0.2.0, adds:

- `ChainFollower.unwatch_session`, which stops reading a session and drops the
  block that proved it;
- `ChainFollower.follow(funding=(agent_id, wallet))` and `unwatch_funding`: each
  refresh also reads the registry's funding wallet for that agent id and the
  wallet's code (`ChainSnapshot.funding`, a `FundingState`) at no extra round
  trip, and `ChainReader.funding_signature` checks a funding signature at that
  block in one;
- `ChainReader.opening_transaction`, one `eth_getLogs` naming the transaction
  of a session's `SessionOpened`, a lead to prove by its receipt;
- `ChainFollower.confirm(..., observation)`: a refresh behind the block that
  proved a confirmed state is lag (`ChainLagError`), not a contradiction. Once
  refreshes have stayed behind it for longer than `max_age_ms`,
  `confirmed_readiness` reports that session `STATE_UNAVAILABLE` until one
  reaches it;
- `TransportError.retry_after`. A transient read is repeated after a random
  share of its wait and after any `Retry-After` that fits the existing bound,
  and a batch repeats only its refused entries;
- `CircleAgentWallet.retire(operation_refs=..., at=...)` and `near_capacity`.
  A journal record keeps its call's digest instead of the call, and only the
  16 MiB bound limits the journal; `retire` marks records, in the journal,
  with the time from which they are forgotten once they hold a transaction
  hash. A failed command's error names the CLI's listed error code and an
  unknown result carries its cause. An execute checks the CLI and wallet first
  unless a check began within 60 seconds, and after any failed command the next
  execute or signature checks again. The journal's lock is held only while the
  journal is read or replaced, never across a Circle command; within one
  process the adapter runs one operation at a time, CLI commands included. An
  execute or reconcile waits for the lock up to `timeout_seconds`, and one that
  cannot then
  record Circle's answer returns `UNKNOWN` with its transaction id and hash,
  written into the journal at the next journal step; a failed save keeps them
  in memory and does not fail that step. A reconcile withdraws a
  record's retirement mark. An execute refused for an invalid reference, or
  once the journal shows no record of its reference, raises
  `CircleNotSubmittedError`;
- local records wait up to one second for a lock that a spawned child process
  still holds, or as long as `Checkpoint(..., lock_wait=...)` gives;
- `ChainFollower.watch_session`: watches that wait behind a refresh are read
  together by the next one, so many sessions watched at once, as a provider's
  recoveries at startup watch theirs, take a few refreshes rather than one each;
- a snapshot splits its views across Multicall3 calls of at most 512 views, each
  anchored to the snapshot's block, and `HttpJsonRpc` keeps each request within
  1 MiB, so a large watch set takes more round trips instead of failing every
  refresh; a backend that still has not imported a block (-32014) once the
  repeats are spent is `ChainLagError`.

Every interface change is additive. One behaviour narrows: `HttpTransport.request`
refuses a request body over 1 MiB before sending it, as the Rust transport does.
One exception changes: for an invalid reference, or a call for another network
or over 60 KiB, `CircleAgentWallet.execute` raises `CircleNotSubmittedError`,
which is not a `ValidationError`, where `0.1.3` raised `ValidationError`.
Earlier releases refuse a Circle journal this one has saved, so do not
downgrade over it.
Version `0.1.3`, used by `mppi-agent` 0.1.4, 0.1.4.1 and 0.1.5, keeps HTTP connections
alive and sends a step's reads as one JSON-RPC batch (`HttpJsonRpc.call_many`),
and treats a read timeout as transient. It adds:

- `ChainReader.latest`, a one-request snapshot through Multicall3, and the
  batched steps `observe` and `session_at`; on final networks
  (`DeploymentProfile.instant_finality`) receipts alone prove operations;
- `ChainReader.funding`, which checks a funding signature as USDC does;
- `ChainFollower.follow`, `refresh_at`, `confirm`, `confirmed_readiness` and
  `on_refresh_failure`, for requests that read no chain;
- `NotSubmittedError` and `OperationResult.cause`.

Three changes are not additive. `OpeningEvidence` and `ClosingEvidence` carry the
proving `observation` instead of a `Block`, and `ChainReader.agent_wallet` takes
that observation. The Circle adapter pins no account code:
`CircleAgentWallet(chain=..., address=..., checkpoint_path=...)` replaces the
`wallet` and `implementation` pins, and its journal is format 2 (format 1 still
loads).
Version `0.1.2`, used by `mppi-agent` 0.1.3, adds bounded repeats of transient
read-only JSON-RPC calls, lag-tolerant chain following
(`ChainLagError`, `last_refresh_failure`), `HookError.detail` and
`TransportError.transient`, and accepts checkpoint format 2; every change is
additive.
These building blocks do not authorize inference or store provider state.

See [the SDK interfaces](../../docs/sdk-interfaces.md) for the implemented surface
and runtime integration boundaries.

The [runtime reference](../../docs/python-runtime.md) describes the implemented
trust boundaries, dependency adapters and evidence limitations.

## License

Copyright 2026 MPPI contributors. Licensed under the [Apache License, Version 2.0](LICENSE).
