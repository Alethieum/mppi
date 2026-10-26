# Python payment runtime

The native Python agent `Client`, provider `Mppi` payment services and shared
chain observer implement the paid request loop below. Start with the
[provider quickstart](provider-quickstart.md) or [agent quickstart](agent-quickstart.md)
for built-in setup with packaged official Arc mainnet and testnet profiles.
The [Python composition reference](python-quickstart.md) covers custom wallet
and explicit chain capabilities.

## Native paid request loop

The public [wire profile](protocol.md) defines Complete and Control. One agent
channel carries both RPCs; application queues and payment processing are
independent. HTTP discovery and TLS ingress are supplied by the provider.

```mermaid
sequenceDiagram
    participant A as Agent Client
    participant P as Provider Mppi
    participant E as Provider execution integration
    participant C as Local or selected MPPI contract
    A->>P: ChallengeRequest on Control
    P-->>A: Correlated Challenge and attachment nonce
    A->>C: Explicit positive funding
    C-->>A: Confirmed opening
    A->>P: SessionOpen and signed ReconnectRequest
    P-->>A: Ready SessionState
    A->>P: Complete with RunAuth
    P->>E: Prepare required counts
    E-->>P: Prefill and output bound
    P-->>A: LowCredit with complete coverage target
    A->>P: Signed cumulative Voucher
    P->>E: Funded dispatch
    E-->>P: JSON output and supplied usage
    P-->>A: Output on Complete; Meter on Control
    A->>P: Renewed Voucher when needed
    E-->>P: Definitive final report
    P-->>A: Terminal and Receipt
    P->>C: Capture actual cumulative usage
    A->>P: Signed cooperative close
    P->>C: Close and refund remainder
    P-->>A: Canonically verified CloseResult
```

An existing covering voucher skips the request-start coverage exchange. Funding
shortage never triggers an automatic deposit. The provider supplies counts and
terminates its jobs when further coverage is denied. Missing final usage leaves
the reservation unresolved.

### Provider composition

The built-in Circle path uses `await Mppi.from_config("mppi.toml", testnet=True,
coverage=..., execution=...)` on the serving event loop. It verifies the saved
configuration, deployment and existing operator, then constructs the wallet,
chain follower and contract calls. It starts no listener or inference job.
See the [provider quickstart](provider-quickstart.md#4-attach-the-sdk-to-your-application).

For custom capabilities, construct the runtime explicitly:

```python
import grpc
from mppi_provider import Mppi
from mppi_protocol.wire import GRPC_OPTIONS

server = grpc.aio.server(options=GRPC_OPTIONS)
sdk = Mppi(
    config,
    chain=chain_follower,
    contract=contract_calls,
    wallet=operator_wallet,
    coverage=prepare_required_tokens,
    execution=execute_and_report,
)
sdk.register_grpc(server)  # before host-owned server startup
readiness = await sdk.start()
```

These are integration objects supplied by the host. `coverage(request, context)`
returns `TokenCounts` before any paid execution. The execution async iterator
receives `(request, context, runs, stop_requested)`, yields original JSON bytes,
and explicitly calls `runs.report_usage` and `runs.report_terminal`. For another
execution interval it calls `runs.require_coverage(context, remaining_tokens)`
and proceeds only after a granted result. `remaining_tokens` is a complete
desired remaining bound; already reserved cost is retained until consumed or
released by a definitive final report. The stop event requests termination; it
does not assert the job has stopped.

The host configures its server with `GRPC_OPTIONS` to enforce decompressed
message limits, publishes `sdk.discovery(path)` through its HTTP stack, and owns
listener startup/TLS. `sdk.shutdown(deadline)` uses an absolute monotonic deadline,
stops grants and returns unresolved run/session/operation evidence. It does not
stop the host's gRPC server or close escrow.

One `Mppi` instance owns live payment state for its sessions. A provider must
coordinate its workers around that authoritative state. Separate frontend and
backend sockets are supported; socket identity is never payment authority.
This SDK adds no database or distributed worker coordinator. Loading an old
opening after losing private usage history cannot manufacture a zero-usage
session. `await sdk.sessions.checkpoint(session_id)` returns detached
`RecoveryEvidence` for the host to retain. After restart,
`await sdk.sessions.recover(evidence)` verifies complete supplied history and
canonical chain evidence before installing it. The SDK supplies no persistence
backend. Missing or contradictory evidence fails without replacing live state.
Recovery never restarts execution or restores an old Control stream's authority.
Unresolved runs retain their reservations until definitive provider reports.

A reconnect may name a pending run this provider never held: the agent records
a run before its Complete arrives. When every admission is known, the snapshot
answers it `NOT_ADMITTED` and records nothing. Every admission is known for a
session this runtime opened, or one restored with
`await sdk.sessions.recover_current(evidence)`. After `recover` such a run
refuses the attachment instead: an older checkpoint may lack a run that was
admitted and served. Use `recover_current` only if your host checkpoints durably
after every admission and before dispatch, and never restores a checkpoint
older than its newest.

Before the first authenticated attachment, a known unused opening can be
exported and recovered at generation zero in private `RecoveryEvidence`.
It has no accepted voucher, usage, runs or reservations and is never ready.
The provider supplies complete authoritative history; chain records alone do
not prove it unused. Recovery preserves close intent and operation identity.
Only fresh authentication creates generation one, and ordinary Control snapshots
continue to require a positive generation.

`settleBatch(session_ids)` captures cumulative actual usage for the ordered
session list. It simulates the exact sender and calldata before submission,
preserves operation references through uncertainty and verifies outcomes per
item. The background collector groups due sessions and can split a group after
a pre-submission simulation failure. No split retries an unknown submission.

### Agent composition

```python
from pathlib import Path
from mppi_agent import Client

with Client(
    "https://mppi.provider.example",
    wallet=funding_wallet,
    signer=fixed_session_signer,
    chain=chain_follower,
    contract=contract_calls,
    checkpoint_path=Path("agent-state/provider.json"),
    signer_reference="wallet-service/session-key-reference",
    initial_session_tokens=1_000_000,
    agent_id=agent_id,  # Required to fund; the registry must bind it to the wallet.
) as client:
    with client.chat.completions.create(
        model="example/model",
        messages=[{"role": "user", "content": "Hello"}],
        stream=True,
    ) as stream:
        for payload in stream:
            consume_original_json(payload)
```

The wallet, signer, contract binding and chain follower are explicit capabilities,
not generated by this snippet. Use a separate follower for each runtime's owning
event loop. The agent's synchronous API owns its background loop/thread, and
wallet/HTTP/chain calls run outside that loop. `create_raw` accepts validated JSON
bytes when the caller must retain exact numeric text. Nonstreaming create returns
one original JSON response after a valid terminal and gRPC OK trailers.

`Client(..., voucher_headroom=amount)`, also accepted by `Client.from_config`, is
off by default (0). With a positive
amount, each voucher the client signs is that many USDC base units above what
the provider requires, within the confirmed deposit; a signed voucher already
that large is reused. A provider admits a request at once when its accepted
voucher already covers usage, every running request's reservation and that
request's prefill and output buffer, and no earlier waiting run is still within
its turn ([admission](protocol.md#admission-and-voucher-renewal), step 2), so a
request that fits in the headroom left
starts without the `LowCredit` and `Voucher` round trip. One that does not fit
takes it, and the voucher it gets restores the headroom; a session's first
request always does. Size the amount to the prompt and the provider's output
buffer (`cover_output_tokens`, 1,000 tokens by default) of the requests it
should admit, at their prices. The contract lets a provider capture up to the
signed voucher and the deposit whatever it delivered, while the provider SDK
captures actual usage: the headroom adds at most `amount` to that trust
exposure, and close refunds what was not captured.

Each run's Complete stream ends on its own terms. A lost Control attachment or a
reattach never cancels it, so a run the provider already completed still
delivers its answer and final. One Control report that fails validation (a
challenge, `LowCredit`, `Receipt` or `Drain`) is set aside with an `on_error`
record naming its kind and reason; the attachment and every run continue. A
Receipt is checked against its retained run and accepted prices, as its
Terminal is, with no chain read. Its settled total and observation are the
provider's report: the agent proves the settled total at a cooperative close
and reads it at every attachment. A rejected `Meter` stops the session
cursor: Control stays open so active runs finish (closing it would make the
provider cancel them), no new run starts, and the next request re-syncs once
no stream is live, or after 60 s, when a stream still live is ended. A run registered but never sent is recorded `NOT_ADMITTED`
locally and is never named pending at a reattach.

Reuse the checkpoint path and recoverable signer credential across restarts.
The agent uses an exclusive local file lock and atomic, mode-0600 checkpoints
on macOS/Linux. It stores signed payment intents and accounting, not private
keys or prompts. Its size stays bounded in a long session: a run is forgotten
once its final and final Meter are applied, its stream has ended and its
Receipt was verified (or a snapshot resolved it, the session closed, or 1,024
settled runs already wait for Receipts). Usage stays exact in per-price totals,
and one run per price is kept as signed price acceptance. Since 0.1.3,
`mppi-agent` writes this checkpoint as format 2 and still reads format 1; 0.1.2
refuses format 2, so do not downgrade over a checkpoint 0.1.3 or later has
written. A 0.1.4 checkpoint can also hold a reported close not yet proven,
which 0.1.3 would not know: do not downgrade over it either. Failure to retain required intent stops submission/signing.
Wallet-service credentials and durable transaction idempotency remain in the
wallet adapter. A changed signer never silently opens a second funded session.

`client.sessions.recover(session_id)` restores retained payment state and
authenticates a new Control generation without funding or replaying a request.
Observed catalog changes do not imply acceptance: call `accept_catalog(hash)`
explicitly for subsequent runs. Historical runs retain their original prices.
An explicit open can continue an original, never-submitted opening intent using
its retained authorization. Once submission is uncertain, reconcile its original
reference; recovery never creates a replacement deposit.

`client.sessions.open` supports explicit positive pre-warming. `topUp` uses raw
positive USDC base units. `sessions.close` requests cooperative close; transaction
exits retain `requestClose`, `withdraw` and `releaseIdle`. Inspect operation
states and reconcile original references after uncertainty. An
`OperationResult` keeps the kind of error a wallet call raised as `cause`
(SDK-written, never remote text), and the same goes to `on_error`. A wallet
adapter raises `NotSubmittedError` only when its own durable record proves the
call never reached its service; the result is then `FAILED` and the call may be
sent again. Any other wallet error leaves the result `UNKNOWN`. A confirmed
top-up whose event shows less than the deposit confirmed before it was sent,
plus its amount, raises `ClientError`: nothing is recorded from that event, the
next refresh reads the deposit, and a retry sends the same authorization again,
since that attempt's own transaction may still land. A request the
provider refuses before admission (`INSUFFICIENT_DEPOSIT`, or
`SESSION_NOT_READY` for `AUTHORIZATION_REQUIRED` or `PRICE_CHANGED`) raises
`PaymentError` and is recorded never admitted, so it does not stay unresolved
after the session closes. `stream.close()`
cancels that request, and `client.close()` releases local resources while
preserving unresolved evidence. Neither implicitly closes escrow.

Restored session inspection, reconciliation and wallet-only exits use retained
local evidence and the chain without requiring the provider endpoint. Cooperative
close first establishes authenticated Control. A canonical close above validated
usage raises `CloseAccountingError` with its confirmed operation result; the
client retains that evidence and never treats the discrepancy as a failed
transaction to resubmit.

The client retains the provider's close report before proving it on chain. If
that proof cannot be read yet, `sessions.close` returns `PENDING` with the close
transaction instead of failing. Calling `sessions.close(session_id)` again, or
`sessions.reconcile("close-" + session_id.hex())`, proves the same close; it never
sends a second close request. Only the canonical event and state make it
`CONFIRMED`.

A checkpoint holds one session at a time. Once the agent's own close is
`CONFIRMED` (a cooperative close within validated usage, or its own `withdraw`
or `releaseIdle`), the next request or `sessions.open` retires that session and
funds a new one with the same signer over a new Control stream. The checkpoint's
last record for the closed session is kept beside it as
`<checkpoint name>.<session id>.closed<suffix>`, and the checkpoint then drops
that session's ledger and operations, so `sessions.get`, `list`, `terms`, `run`
and `reconcile` no longer know it. Retirement waits until a new session can be
funded: without `agent_id` or an `initial_session_tokens` choice, opening raises
before anything is dropped. Retirement waits for no stream: while a
request on the closed session is still ending, opening raises `ClientError`.
A close above validated usage is never retired; opening raises its
`CloseAccountingError`. For the agent's own `withdraw` or `releaseIdle`, the
session's state at or after the exit's block, where its settled total is final,
is compared with validated usage the same way. That is current state, which a
closed session never changes, so no node needs to keep the exit's block. A close
still `PENDING`, a `requestClose` and a session closed by its provider or anyone
else are not retired.

### Many requests in one session

One `Client` can run many requests at once in its session. They share the
session's deposit, its cumulative voucher ceiling and the prices it has
accepted. Each request is its own run: a random 32-byte run ID, the model and
price version it signed, its own Meters, Terminal and Receipt, and its own
Complete stream beside the session's one Control stream.

Every call runs on the client's own event loop thread, so calls made from
several threads proceed together; concurrent first requests share one opening.
In asyncio code, run each call with `asyncio.to_thread`.

```python
from concurrent.futures import ThreadPoolExecutor
from mppi_agent import Client

tasks = ["Summarize the report", "List the open risks", "Draft a reply"]

with Client.from_config(
    "https://YOUR_PROVIDER",
    initial_session_tokens=100_000,
) as client:

    def ask(task: str) -> bytes:
        return client.chat.completions.create(
            model="YOUR_PROVIDER/MODEL",
            messages=[{"role": "user", "content": task}],
        )

    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        for response in pool.map(ask, tasks):
            print(response.decode("utf-8"))
```

**Size the deposit for every run in flight.** The provider admits a run only
when the confirmed deposit covers the session's usage so far, the remaining
reservation of every admitted run and the new run's own: its prefill and an
output bound of at least the provider's output buffer (a Challenge's
`cover_output_tokens`, 1,000 output tokens by default). N runs in flight need
about N times one run's reservation, beyond the usage already recorded
([catalogs and pricing](protocol.md#catalogs-and-pricing)).
`initial_session_tokens` converts token equivalents at the highest applicable
rate, so N × (input tokens per request + output bound) covers N such runs at
once in a new session. A request the deposit cannot cover fails with
`INSUFFICIENT_DEPOSIT`, and `on_funding_notice` reports the shortfall; only an
explicit `sessions.topUp` adds funds. A session keeps at most 1,024 unfinished
runs.

**Admission order.** When the voucher ceiling does not yet cover a new run, the
provider sends a coverage request on Control and the run waits for the higher
voucher, so each such admission may cost one voucher round trip. The client
signs those vouchers within the confirmed deposit, one at a time. Provider SDK
releases after `0.1.2` admit a session's waiting runs in the order they first
waited, while each voucher arrives within the provider's turn
([admission and voucher renewal](protocol.md#admission-and-voucher-renewal),
step 4); with `0.1.2`, a later run can be admitted first.

**Share one client between workers.** A session has one Control attachment:
a successful reattachment replaces the previous one and starts a new
generation, and requests signed for an earlier generation cannot start
([attachment](protocol.md#control-directions-and-attachment)). The checkpoint
lock also refuses a second `Client` on the same configuration ("already in
use"). To fan work out to sub-agents or workers, give them the one `Client`, or
route their requests through the process that holds it; do not attach a
separate process per worker to the same session.

For comparison, x402 sends a signed payment authorization with each paid
request and leaves session handling out of its
[v2 specification](https://github.com/coinbase/x402/blob/main/specs/x402-specification-v2.md),
and MPP's `session` intent is a
[streaming payment over a payment channel](https://mpp.dev/protocol). In MPPI,
one escrowed deposit and one cumulative voucher ceiling serve every run in
flight, and each run streams on its own Complete RPC.

### After Control loss

If the provider restarts or the Control stream otherwise ends, the client
reports it through `on_error` (operation `"control"`) and marks the session
not ready; running Complete streams still end on their own terms. Nothing
reattaches in the background. The next request, or
`client.sessions.open(...)`, reattaches first: it reconnects Control, proves
the retained opening again and authenticates a new generation. If the provider
is still unavailable, that call fails and a later one tries again. Until then
`client.sessions.get(session_id).readiness` reports `SESSION_NOT_READY` with
reason `CONTROL_NOT_READY`. It only reports: send the next request instead of
waiting for it to become ready.

## Fresh chain state

`ChainFollower(reader, max_age_ms=..., collection_margin_ms=...,
poll_interval_ms=...)` maintains a complete canonical cached position. The
provider follower's limits must equal its declared configuration. Admission and
renewal make no per-token chain reads. Freshness bounds both observation age and
time since head progress, checks block time against the synchronized local clock,
and rejects changes to previously accepted canonical blocks or regressed state.

A valid voucher from the current authenticated attachment can arrive after a
confirmed top-up but before the provider's next poll. If it exceeds the cached
deposit, the provider makes one canonical refresh bounded by `max_age_ms`,
outside the accounting lock, then rechecks ownership, readiness and the deposit
bound. Covered vouchers use the cache. Execution continues under its existing
funded limit while refresh is pending; a denied grant still terminates the run.

**Read cost.** Arc commits each block with final, irreversible settlement
([deterministic finality](https://docs.arc.io/arc/concepts/deterministic-finality)),
so no earlier block is re-read. A refresh is one round trip: one JSON-RPC batch
with the latest header, one Multicall3 `aggregate3` call reading the pause level,
each watched provider's registry entry and each watched session, and the two
implementation slots. The multicall's own block number, time and parent hash
must match the header; views that ran at another block, as through an adapter
that sends one call at a time, must match that block's header, read by number
in one more round trip. The first refresh also checks the chain id, the pinned
contracts' code and Multicall3's code; without verified Multicall3 code a
refresh is two round trips. Opening, top-up and close are each proved by one
transaction receipt holding the contract's own event; a mined transaction
without that event is a definite failure, never unknown. A settlement also
reads the session at its block. JSON-RPC batches do not reduce what most paid
plans count.

A failed refresh is repeated once, 250 ms later, before it fails closed, and
`last_refresh_failure` keeps the last failure's error class, SDK reason and
monotonic time. A load-balanced RPC may serve backends at slightly different
heights ([Arc RPC endpoints](https://docs.arc.io/arc/references/rpc-endpoints)):
a head or block behind accepted evidence is `ChainLagError`, not a regression.
If it persists, the accepted snapshot stays and ages as usual, so freshness is
never extended. A different block at an accepted height still fails.

`snapshot()` returns fresh evidence or fails; `current` exposes the last evidence
for accounting even when stale. `readiness(session_id)` checks positive existence,
status, pause and the collection margin on fresh evidence; a provider admits on
it. `confirmed_readiness(session_id)` applies the same rules to the last accepted
evidence whatever its age: an agent's requests within an attached session use it
and read no chain. Deposits and activity only grow, so older evidence only
understates the deposit and the collection deadline. A refresh that finds
evidence contradicting the pinned deployment or accepted evidence
(`ChainContradictionError`: another chain id, deployed code, implementation or
signing domain, or a changed or regressed block or session) holds it at
`INCONSISTENT_STATE` until a later refresh is accepted; unavailable, rate-limited,
lagging or timed-out reads never do. The agent refreshes before
opening, attachment, top-up and close when its evidence is older than
`max_age_ms`, and `on_refresh_failure` reports a failed background refresh. A pending transaction extends no
deposit or deadline. The observer requires an RPC supporting EIP-1898 canonical
block-hash reads, and retains the existing RPC-honesty trust assumption.

`ChainReader.opening(tx_hash, session_id, accepted_catalog=...)` binds retained
exact catalog bytes to the canonical opening event and state. This matters when
a later transaction updates prices in the same block. `closing` verifies the
actual close/refund event and state; `verify_settlement` verifies a supplied
canonical settlement reference. Neither manufactures payment confirmation from
an outer wallet transaction's success alone.

A missing or malformed current catalog does not discard fresh session and pause
observations. The follower retains verified identity with `catalog=None`.
Historical accounting and eligible exits remain available; new work requires a
valid confirmed catalog. Direct `identity` reads remain strict unless the caller
explicitly selects `allow_unavailable_catalog=True`.

## Provider configuration and discovery

```python
from mppi_provider import ProviderConfig

config = ProviderConfig.from_file("mppi.toml")
catalog_bytes = config.catalog.canonical_bytes
```

Start from the [example configuration](../examples/provider/mppi.toml). Its
addresses, model and timings are illustrative. Loading validates declarations;
it makes no wallet call, registry write or inference request.

`from_file(path, overrides=...)` applies explicit section values over file values.
`from_mapping(values, overrides=...)` accepts the same sections; direct typed
construction uses the exported immutable section records. Unknown configuration
keys fail. TOML money and provider IDs are canonical quoted decimal strings;
programmatic amounts may also be exact integers. Floats and booleans cannot
become money. Errors identify the field without echoing rejected secret values.

| Section | Required fields and optional values |
| --- | --- |
| `provider` | `name`, `description`, HTTPS `public_origin`, nonzero `recipient`; optional `image`, extra `services` |
| `identity` | uint256 `provider_id`, owner `wallet_address` |
| `chain` | positive `max_age_ms`; `network` defaults to `mainnet`, optional explicit `rpc` |
| `upstream` | full internal `url`, optional `api_key_env` credential reference |
| `models` | unique model cards with explicit capabilities and input/output rates; cache rate optional |
| `payment` | positive `collection_margin_ms`, `authorization_wait_ms`; output buffer defaults to 1,000, funding suggestion to 1,000,000 tokens |
| `settlement` | positive `interval_ms`; optional positive `amount` and `units` triggers |
| `listen` | optional standalone declarations: `bind`, explicit `tls_mode`; direct mode requires cert/key paths, ingress rejects them |

Durations fit signed 64-bit nanoseconds after conversion. Model limits are
positive uint64 values; maximum output cannot exceed context size. File-relative
TLS paths resolve against that file. The internal URL's complete path prefix is
retained. Keys and resolved credentials never enter the configuration model.

`DiscoveryDocuments(config, identity)` checks applied declarations against a
`ProviderIdentity` obtained from the selected registry observation. Owner,
recipient, provider ID, network, origin and exact catalog must agree. Its
`get(path)` returns a framework-neutral `DiscoveryResponse` for `/discover`,
`/.well-known/agent-registration.json` and `/models`; other paths return `None`.
The host application owns GET routing and TLS. Discovery never publishes local
pending prices or declares payment readiness.

TOML and each discovery document are bounded to 1 MiB. JSON rejects duplicate
keys and nesting beyond 128. HTTPS origins normalize host/scheme case, the
default port and a final slash. DNS names use ASCII, including explicit A-labels
for international names; ambiguous controls, credentials and URL path prefixes
fail. Registration URI and model URL are bound to the same origin.

## Agent discovery and fixed signing

`mppi_agent.discover(identity, expected_origin=..., transport=...)` fetches the
direct HTTPS documents, checks their registry/service bindings and validates
model cards against the confirmed catalog. Prices always come from that catalog.
Redirects fail. `verify_challenge(challenge, identity, request_id=..., model=...)`
checks correlation, network, proxy, operator, provider ID and catalog before a
caller makes its explicit funding decision. With no current catalog, discovery
still verifies the provider's registry binding but does not fetch advisory model
cards or enable opening and new work.

`LocalSessionSigner(private_key: bytes)` implements the existing `SessionSigner`
capability using exactly one supplied 32-byte ECDSA credential. It neither
generates nor persists keys. Supply it through the agent's secret integration;
keep its recoverable reference for the session lifetime. Its public address is
distinct from funding-wallet authority and ERC-8004 identity.

The signer provides `sign_voucher`, `sign_run`, `sign_reconnect` and `sign_close`.
The corresponding `mppi_protocol.signing.verify_*` functions require the selected
domain and confirmed complete descriptor. They derive its session ID and reject
wrong authorities, domains and bindings, malformed proofs, high-s values and
recovery bytes outside 27/28. Digests and byte-for-byte test vectors are shared
with the contract. Signature validity alone is never a work grant.

## Calls and canonical payment evidence

`ContractCalls(domain, token_domain_name, token_domain_version)` prepares typed
nonpayable `PreparedOperation` records for `open`, `top_up`, `settle`,
`settle_batch`, `close`, `request_close`, `withdraw` and `release_idle`. The ABI
resource is copied from the compiled contract and checked for drift. Encoded
calldata must exactly match the operation's descriptors, targets and initial
catalog hash. Funding authorization must match chain, token/domain, wallet,
recipient and positive amount. Token authorization signature bytes retain their
separate smart-wallet format; they are not forced into MPPI's ECDSA format.

`mppi_protocol.operations.submit_operation(wallet, operation, operation_ref=...,
chain=...)` validates caller authority and makes exactly one explicit wallet
handoff. `reconcile_operation` only queries the original wallet operation and
checks available chain evidence. It never resubmits. Keep the original prepared
call/reference. Adapters must durably bind that reference to the exact call and
return retained identifiers through service outages. If an adapter throws without
structured evidence, only the supplied reference can be preserved. The session
runtime must retain prior confirmed results and reject observation regressions.

`ChainReader(rpc, profile)` performs synchronous, block-pinned reads. Invoke it
outside the gRPC I/O loop. `HttpJsonRpc` is a bounded read-only adapter: no wallet
submissions or redirects. Its methods only read, so it repeats a transient
failure up to twice, 250 ms and then 1 s later: HTTP 408, 425, 429, 500, 502,
503 or 504, a refused or dropped connection, a timeout, or JSON-RPC error -32005
or -32007 (rate limits) or -32014 (a backend that has not imported the block
yet), including one entry of a batch. Any other failure is not repeated. Repeats
start within one transport timeout plus those delays of the first attempt, so a
timed-out attempt is repeated once; by default each attempt is bounded at 5 s.
It keeps connections alive and sends the reads one step needs as one JSON-RPC
batch of at most 32 calls; an endpoint that answers a batch with a single object,
or refuses a batch request and then serves a single call, is read one call at a
time. TLS verifies certificate and host;
`HttpTransport(ca_file=...)` can trust a provider's private CA. Its timeout bounds
individual blocking I/O operations, not an entire SDK operation's lifetime.

`DeploymentProfile` requires explicit Arc chain/proxy/registry identity and
runtime-code pins for both proxies and implementations. Built-in setup loads
the packaged official testnet pins; explicit composition supplies its own profile.
`verify_deployment(block)` checks code hashes, ERC-1967 slots and
MPPI signing domain. Manifest provenance, initialization and governance checks
remain deployment release responsibilities. A caller-supplied profile is not
automatically a trusted release manifest.

`block`, `identity`, `session` and `pause_level` expose observed state. Reads use
EIP-1898 block hashes with `requireCanonical`; unsupported RPC behavior fails
without a fallback to unpinned latest state. A positive retained deposit
establishes session existence, including fully consumed and closed records.
Block records alone do not establish freshness or permission to execute.

`simulate(operation, sender=...)` checks deployment identity, calls the exact
nonpayable operation at a canonical block hash and estimates gas at that block's
number, then rechecks its hash. `SimulationEvidence` reports the observed block,
estimate and block gas limit. Simulation is advisory; it never submits a wallet
transaction or replaces canonical confirmation.

`reconcile(operation, operation_ref=..., tx_hash=..., wallet_operation_id=...)`
never submits a transaction. It returns the shared `OperationResult`:

- Missing transaction identity is `unknown`; an absent receipt is `pending`.
- A canonical reverted receipt is `failed`.
- Successful outer receipts require matching canonical MPPI events/state before
  the intended effect is `confirmed`. Missing or contradictory evidence is
  `unknown` and cannot authorize replacement funding.
- A settlement reference identifies only an actual `Settled` event in that
  transaction. Later state can show target coverage without attribution. Silent
  no-ops keep that distinction. A partly effective batch retains independent
  session outcomes; an entirely silent batch remains `unknown`.
- Empty and repeated-session batches are valid contract calls. Distinct targets
  retain independently attributable events. Identical repeated targets retain
  ordered coverage results without assigning one event to multiple items.

An ERC-4337 transaction may contain other users' calls. Unrelated events are not
attributed to the intended operation. Confirmation here establishes its expected
economic effect in the named transaction, not a particular UserOperation's
execution success. The wallet adapter must bind the original operation reference,
exact calldata, wallet operation and transaction hash. Ambiguous repeated events
for an intended session remain unresolved.

RPC consistency checks do not establish RPC honesty. The chain follower also
enforces freshness, canonical progress, pause and collection deadlines before
admission. The public reporting interfaces keep provider counting,
execution control, storage and recovery infrastructure with the provider.
