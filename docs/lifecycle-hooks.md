# Lifecycle hooks

Both Python SDKs accept optional `hooks=Hooks(...)`. Import `Hooks` from
`mppi_provider` for `Mppi`, and from `mppi_agent` for `Client`. Rust providers
use `runtime::Hooks` with `Mppi::with_limits_and_hooks(...)`. The existing
constructors remain available.

Python callbacks are synchronous functions returning `None`. Rust callbacks
are `Arc<dyn Fn(T) + Send + Sync>`. Each callback receives an owned snapshot
of the typed record below; mutating that snapshot cannot change SDK state.

| Callback | Payload | Emission boundary |
| --- | --- | --- |
| `on_challenge` | `Challenge` | Provider enqueues guidance; agent verifies correlated guidance. A valid repeated challenge can emit again. |
| `on_session_open` | `SessionOpen` | Provider installs verified opening evidence for a new retained session; agent confirms its opening and completes attachment. This is separate from a reconnect. |
| `on_voucher` | `Voucher` | Provider commits an increased, verified ceiling, including a successful reconnect; agent queues a signed voucher for delivery. Agent delivery attempts can repeat. It does not mean settlement. |
| `on_meter` | `Meter` | Provider commits a new meter; agent applies a changed valid meter. Duplicated accounting does not create another meter event. Provider delivery failure does not erase this local accounting observation. |
| `on_low_credit` | `LowCredit` | Provider queues guidance; agent validates it. It grants no funds and does not automatically top up. |
| `on_drain` | `Drain` | Provider queues the instruction; agent applies it. Repeated instructions can emit again. |
| `on_settle` (provider) | `OperationResult` | Confirmed capture/batch result, including reconciliation. Read its per-session outcomes: a successful outer transaction is not success for every batch item. Repeated reconciliation can emit again. |
| `on_close` (provider) | `CloseResult` | Canonical closing evidence retained, independently of Control delivery. Reconciliation can emit again. |
| `on_receipt` (agent) | `Receipt` | Receipt passes accounting checks against its run's accepted terms, with no chain read. Its settled total is the provider's report; the agent proves the settled total at close. Repeated valid receipts can emit again. |
| `on_timing` (agent) | `OperationTiming` | Once per opening, top-up, close or exit, when it returns or raises: its total time split into this SDK's chain reads (`chain_ms`), the wallet adapter's signing (`wallet_sign_ms`) and its execute call until it returned (`wallet_submit_ms`, including a wallet service's own submission and any wait for mining), and the rest (`provider_ms`: the provider exchange and local work). Requests within a session emit nothing. |
| `on_error` | `HookError` | Runtime Control/Complete failures; agent also reports errors crossing its synchronous API boundary. One failure can cross multiple boundaries. Error records omit prompts, secrets, upstream bodies and raw exception text. |

Session/run/request identifiers are in the corresponding protocol payload.
Python `HookError` additionally permits session/run identifiers when known,
and `detail`: the rejected message kind and the SDK's own reason, such as
`"meter: meter sequence has a gap"`, never remote text;
Rust `HookError` carries the boundary name and gRPC code. These are observations,
not a durable event stream or a payment authority. Correlate by protocol IDs,
meter sequence and operation/transaction reference; do not count callback
invocations as payments.

Log a Python `HookError`'s `operation`, `code` and `detail` together;
`detail`, when present, carries the SDK's own reason. In the agent, a failed
background chain refresh arrives as operation `"chain"`, code
`"TransportError"` and detail `"background refresh: HTTP request failed"`, and
a lost Control attachment as operation `"control"`.

## Bounds and failure isolation

Each instance uses one serial observer worker, started lazily when a configured
hook first emits. Enqueue order is callback order. Concurrent RPCs and deferred
chain verification can complete in different orders; callbacks make no stronger
cross-stream ordering promise.

The queue holds at most 64 records and 1 MiB of payload budget, plus one record
being processed. Payload sizing includes protobuf encoded length and bounded
accounting-record field sizes; it is not a claim about allocator overhead.
The newest event is dropped when a limit is reached. `hook_stats` in Python and
`hook_stats()` in Rust return delivered, dropped, failed and queued counters.
These counters are local to the SDK instance and are never payment evidence.

Callback exceptions and Rust unwinding panics increment `failed`; subsequent
callbacks continue. SDK payment work does not wait for callback completion.
Shutdown drops pending observations and does not wait for an application
callback. A permanently blocked callback can retain its single worker thread;
callbacks should hand off quickly to the application's own logs/metrics.
Callbacks are trusted application code in the same process, not a sandbox:
process termination, Rust `panic=abort`, and code holding Python's GIL are outside
this isolation guarantee. See Python's [thread model](https://docs.python.org/3/library/threading.html#thread-objects)
and Rust's [unwinding boundary](https://doc.rust-lang.org/std/panic/fn.catch_unwind.html).

The agent's original `on_low_credit=` constructor callback still receives a
detached `pb.LowCredit`, but since 0.1.3 only for a requirement the confirmed
deposit cannot cover, or for a plain low-credit notice; coverage the deposit
covers is signed by the SDK and not forwarded. The new `on_funding_notice=`
callback receives those notices as a `FundingNotice` with `deposit`,
`required` and `shortfall`, worked out at delivery. Both share one delivery
worker in which a newer notice replaces an undelivered one. `hooks.on_low_credit` is the
best-effort observer described here and still sees every validated notice. Configuring both invokes both independently; use the
new hook for passive telemetry. MPPI supplies no monitoring service, provider
storage, token counter, inference engine or automatic payment action here.
