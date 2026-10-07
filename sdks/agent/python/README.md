# MPPI agent SDK for Python

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
checkpoint_path=..., signer_reference=..., initial_session_tokens=...)` with
the shared `ChainFollower` and `ContractCalls` configured for the verified
deployment. The funding wallet and fixed ECDSA signer are distinct capabilities.
The caller supplies wallet integration; no login or key creation is implicit.

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
- `Client(..., on_low_credit=callback)` delivers validated `LowCredit` messages
  to caller code in order, outside the Control event loop. Each message is a
  detached copy. The caller may explicitly call `sessions.topUp`; the SDK adds
  no funding policy. Keep callbacks bounded: an exception or full callback queue
  fails the attachment. Queued callbacks are discarded when Control is lost;
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
