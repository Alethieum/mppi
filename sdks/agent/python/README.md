# MPPI agent SDK for Python

Version `0.1.4` keeps the wire protocol, contract, signatures and payment
semantics and works with the providers running `0.1.2`. It reads the chain in
a few batched round trips over kept-alive connections, and resolves outcomes
that `0.1.3` left uncertain:

- each chain lookup is one request: a refresh is one batch around one
  Multicall3 call anchored to its block, and opening, top-up and close are each
  proved by their transaction's receipt and event. Arc blocks are final, so
  none is re-read. A mined transaction whose call did not take effect is
  `FAILED` with its cause, and a retried top-up resends the same authorization
  while it is valid;
- requests within an attached session read no chain: a slow or unreachable
  RPC no longer refuses them with `CHAIN_STATE_STALE`. They use the confirmed
  deposit, accepted prices and attachment; opening, attachment, top-up, close
  and accepting new prices refresh first when evidence is older than
  `max_age_ms`. The built-in client polls once a minute, and a failed
  background refresh reaches the error hook. A refresh that finds evidence
  contradicting the deployment (another chain, code, implementation or
  signing domain, or state going backwards) stops requests with
  `INCONSISTENT_STATE` until a later refresh is accepted;

- each step reads everything it needs at one block in one JSON-RPC batch.
  Top-up and close repeat no whole refresh; an attachment reads only its
  session once the follower has reached the provider's block, and everything
  at that block otherwise, as a new session's first attachment does. Fixed
  facts (the chain id, deployed code, the Circle CLI and wallet selection) are
  read once, implementation slots every time;
- any Circle Agent Wallet account generation works: no account code is pinned.
  Before submitting an opening, the agent checks in one read that the registry
  binds its agent id to the wallet and that USDC would accept the signature,
  and otherwise raises `FundingAuthorizationError` (`reason`
  `AGENT_ID_REQUIRED`, `AGENT_WALLET_MISMATCH` or `INVALID_SIGNATURE`) with
  nothing sent; a top-up relies on its opening's proof. A wallet whose service
  signs as it executes declares `signs_at_execute = True`: the opening then
  checks only the binding and hands the placeholder to `execute`, and an
  undeclared all-zero placeholder is `SIGNATURE_DEFERRED_UNDECLARED`. Funding
  now requires `Client(..., agent_id=...)`; exits do not;
- a timed-out read is repeated once; a reported cooperative close whose proof
  cannot be read yet is `PENDING` with its transaction, and calling
  `sessions.close` again proves it without a second close request;
- a wallet call the adapter proves never reached its service is `FAILED`, with
  `cause` beginning `NotSubmittedError`; the next `sessions.open` sends the same
  intent again while its authorization is valid. An uncertain opening is
  `FAILED` once its authorization has expired with no session funded;
- a request the provider refuses before admission is recorded never admitted,
  so a cleanly closed session shuts down with nothing unresolved;
- a plain low-credit notice never replaces an undelivered shortfall, and a
  top-up confirmed while Control reattaches renews vouchers once attached;
- integrations read a session's accepted prices (`sessions.terms`) and a run's
  outcome (`sessions.run`) through public types instead of client internals.
  `Hooks(on_timing=...)` reports where each opening, top-up and close spent
  its time: chain reads, the wallet's signing and submission, and the rest.

Composed integrations keep the `ChainReader`, `ChainFollower` and `Client`
constructors and `FundingNotice`; new fields and methods are additive. Three
interfaces changed: `OpeningEvidence` and `ClosingEvidence` carry a
`ChainObservation` as `observation` instead of `block`;
`ChainReader.agent_wallet(agent_id, observation)` takes one too; and
`CircleAgentWallet(chain=..., address=..., checkpoint_path=...)` takes the
wallet address instead of account pins, so `supported_account_pins` is gone.
A follower passed to `Client` keeps its own `on_refresh_failure`; give it
`poll_interval_ms=max_age_ms` for the built-in cadence.

Do not downgrade to `0.1.3` over a checkpoint that holds a close not yet proven,
or over a Circle wallet journal `0.1.4` has rewritten (format 2).

Version `0.1.3` keeps the wire protocol, contract, signatures and payment
semantics of `0.1.2` and works with the providers running `0.1.2`. It changes
how the client survives concurrency, rate limits and lagging RPCs:

- one invalid or unprovable Control report is rejected alone; Control loss
  or a reattach never cancels a Complete stream, so completed answers arrive;
- a coverage request overtaken by another stream's final is ignored, not
  fatal, and judged by price;
- chain reads repeat rate-limited, unavailable and not-yet-imported reads;
  a lagging RPC head keeps accepted evidence; a failed refresh is repeated;
- a top-up no longer blocks requests; `from_config` polls every 15 s and
  takes `max_age_ms` and `poll_interval_ms`; a Receipt observation is
  proven once;
- `on_low_credit` receives a `LowCredit` only for what the deposit cannot
  cover; the new `on_funding_notice` gets a `FundingNotice` with its
  `shortfall`, worked out at delivery;
- settled runs are forgotten, so the payment checkpoint stays bounded. It is
  written as format 2; do not downgrade to `0.1.2` over it.

Since `0.1.3`, `on_low_credit` receives fewer notices than in `0.1.2`: only
shortfalls the deposit cannot cover and plain low-credit notices, coalesced to
the latest undelivered one.

Version `0.1.2` supports explicit Arc mainnet and testnet selection with built-in Circle CLI onboarding and
`Client.from_config(...)`. See the
[agent quickstart](../../../docs/agent-quickstart.md).

Version `0.1.0` includes a synchronous `Client` with an owning background
asyncio thread for native TLS gRPC Complete and Control. It verifies discovery,
positive funding, fixed-signer attachment, signed run prices, cumulative vouchers,
provider usage reports and canonical settlement/close evidence. Version `0.1.0`
is published for official Arc testnet. The package includes resumable ERC-8004 onboarding and a
Circle Agent Wallet adapter tested through live Arc testnet onboarding, funding,
payment, authenticated reconnect and cooperative close.

Start with the [Python installation and paid-exchange walkthrough](../../../docs/python-quickstart.md).
See [onboarding and wallet setup](../../../docs/python-onboarding.md) for
`mppi-agent onboard`, deployment profiles, wallet integration and rerun behavior.

```python
from mppi_agent import Rates, funding_amount

assert funding_amount(1_000_000, Rates(input=1_000_000, output=3_000_000)) == 3_000_000
```

The result is six-decimal USDC base units. This calculation transfers no funds.
The caller selects funding; actual usage retains its individual accepted rates.

Construct `Client(provider, wallet=..., signer=..., chain=..., contract=...,
checkpoint_path=..., signer_reference=..., initial_session_tokens=..., agent_id=...)` with
the shared `ChainFollower` and `ContractCalls` configured for the verified
deployment. The funding wallet and fixed ECDSA signer are distinct capabilities.
The caller supplies wallet integration; no login or key creation is implicit.
Funding requires `agent_id`, the ERC-8004 identity the registry binds to the
funding wallet.

- `sessions.open(provider, model=..., initial_session_tokens=...)` explicitly
  prewarms a positively funded session. `chat.completions.create(...)` can open
  lazily using the caller's configured positive funding choice.
- `chat.completions.create(model=..., messages=..., stream=True)` returns an
  iterator of original JSON bytes with `terminal` and `receipt` records.
  `create_raw(payload, model=..., stream=...)` preserves original request bytes.
- `accept_catalog(rate_card_hash)` explicitly accepts the exact fresh published
  catalog for subsequent runs. Active and historical runs keep their prices.
- `sessions.topUp(session_id, amount)` takes positive USDC base units. Funding
  is explicit; low-credit guidance never triggers a wallet transfer.
- `sessions.terms(session_id)` returns the session's accepted catalogs by
  rate-card hash, its initial one and the provider's current one
  (`SessionTerms.rates(model)`). `sessions.run(session_id, run_id)` returns a
  retained run's `RunOutcome`: its signed prices, accepted counts, charge and
  `RunOutcomeStatus` (`PENDING`, `COMPLETED`, `FAILED`, `CANCELLED` or
  `NOT_ADMITTED`, which the provider never held). Neither reads the chain.
- `Client(..., on_low_credit=callback)` delivers funding notices to caller
  code outside the Control event loop, as a detached `pb.LowCredit`. A
  provider's `LowCredit` both asks for a voucher and signals low funding: the
  SDK signs every coverage the confirmed deposit covers itself, so only a
  requirement the deposit cannot cover, or a plain low-credit notice, reaches
  the callback. `Client(..., on_funding_notice=callback)` receives the same
  notices as a `FundingNotice(notice, deposit, required, shortfall)`, in USDC
  base units (`required` and `shortfall` are `None` for a plain notice), for
  example `client.sessions.topUp(session_id, funding.shortfall)`. The deposit
  and shortfall are worked out at delivery, a notice that a confirmed top-up
  has since covered is dropped, and an undelivered notice is replaced by a
  newer one, so a slow callback never sees a stale shortfall. The SDK adds no
  funding policy. Keep callbacks bounded: an exception fails the attachment.
  An undelivered notice is discarded when Control is lost;
  an already running callback cannot be interrupted. Confirmed top-up rechecks
  the retained voucher target against fresh chain funding.
- `sessions.recover(session_id)` restores retained state and authenticates a new
  Control generation. It neither funds a session nor replays inference. Current
  observed prices still require explicit acceptance for later runs.
- `sessions.close(session_id)` requests signed cooperative close. The explicit
  `requestClose`, `withdraw`, `releaseIdle` and `reconcile` methods retain their
  respective chain eligibility and evidence checks.
- `Client.close()` and context-manager exit shut down local streams and return
  unresolved work. They do not implicitly close a funded session.

The checkpoint path is required and exclusively locked on macOS/Linux. Reuse
it for the same payment scope across restarts. It contains descriptors, signed
payment intent and operation references, with mode 0600; it contains no prompts
or private signing key. Keep the recoverable signer credential in the supplied
secret service. The client owns the supplied follower's lifecycle.

An explicit open can continue the same retained, never-submitted opening intent
while its authorization remains valid. A pending or unknown submission is
reconciled using its original reference; it cannot create replacement funding.

Retained session inspection and wallet-only exits restore from the checkpoint
and verified chain state without requiring the provider's HTTP endpoint. A
`CloseAccountingError` reports capture above validated usage while retaining
the confirmed transaction in its `operation`; it does not imply a retry is safe.

Queue bounds and wait timeouts are explicit constructor settings. Slow output
consumption cannot grow its queue without bound; failed or cancelled inference
is never automatically replayed. Transport errors can leave final accounting
unresolved. Fresh cached chain evidence is required for new authorization.

See [SDK interface definitions](../../../docs/sdk-interfaces.md) and the
[protocol profile](../../../docs/protocol.md). The shared protocol dependency is
an implementation support library, not a third role SDK.

See [runtime building blocks](../../../docs/python-runtime.md) for the new APIs
and their verification boundaries.

## Application unit tests

`mppi_agent.testing.MockProvider` runs the real generated Chat and Control gRPC
services in-process on an ephemeral loopback port. Supply asynchronous handlers
for the exact protobuf messages and errors your application should encounter:

```python
import asyncio
from mppi.v1 import mppi_pb2 as pb
from mppi_agent.testing import MockProvider


async def complete(request, context):
    yield pb.ChatChunk(payload=b'{"answer":"Inference successful"}')


async def control(requests, context):
    async for message in requests:
        # A test script can inspect the request and yield its selected reply.
        yield pb.ControlMessage(reconnect_challenge=pb.ReconnectChallenge(nonce=b"n" * 32))


async def test_application():
    async with MockProvider(completions=complete, control=control) as provider:
        call = provider.chat.Completions(
            pb.CompleteRequest(model="test/model", payload=b"{}"), timeout=2
        )
        assert (await call.read()).payload == b'{"answer":"Inference successful"}'


asyncio.run(test_application())
```

The `chat` and `control` properties expose asynchronous generated stubs. Their
RPCs accept normal gRPC deadlines; context exit closes the channel and cancels
unfinished calls. Each instance is used once. Both handlers run on the test's
event loop, so keep them asynchronous.

Scripts control every reply, including omitted final reports and rejected
requests. The helper implements no discovery, TLS, wallet, chain state or
payment validation. It does not change `Client` verification or turn scripted
messages into proof of settlement. Use deployed testnet integration for those
boundaries. No provider package is needed to import this helper.

## Lifecycle observers

Pass `hooks=Hooks(...)` to the runtime/client for typed lifecycle observations.
See the [hook payloads, ordering and bounds](../../../docs/lifecycle-hooks.md).

## License

Copyright 2026 MPPI contributors. Licensed under the [Apache License, Version 2.0](LICENSE).
