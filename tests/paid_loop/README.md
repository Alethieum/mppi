# Independent SDK paid-loop fixture

These are reusable **local test fixtures**, separate from the production SDKs.
A launcher provisions three isolated Linux environments: an owned Anvil node,
an installed provider SDK, and an installed agent SDK. The application roles
receive different generated credentials and cannot import the opposite SDK.

Build the three Python wheels from this public tree. Install `mppi-protocol`
with exactly one role wheel in each role environment. Run `provider.py` and
`agent.py` from directories containing their corresponding script and
`common.py`; the provider directory also needs `recovery.py`. Run `chain.py`
separately with compiled public contract artifacts.
Each role reads its own `/input/config.json` and writes only `/state`. Generated
keys and TLS material must remain outside the public source tree.

The chain fixture uses chain ID 5042002, current MPPI contracts, a mock token at
Arc's USDC address and `DiscoveryRegistry`, a minimal local fixture that is
**not an ERC-8004 implementation**. Its deployment profile pins actual deployed
code. This establishes local contract and SDK interoperability, not live Arc,
wallet-service, registry, finality or provider-engine compatibility.

The provider fixture owns synthetic required counts and final reports. At rates
of 1,000,000,000 USDC base units per million tokens, admission of 4,510 input and
1,000 output tokens requires 5.51 USDC. Definitive usage of 4,510 input and 910
output tokens costs 5.42 USDC. The provider captures 5.42 and keeps the session
open; cooperative close refunds 0.09. The observer independently decodes the
canonical contract events and checks balances. The launcher does not inject
accepted sessions, vouchers, usage or ready attachments into either SDK.

With the launcher's `--recovery` mode, the first agent consumes one response
chunk and then closes its stream and client. The provider observes its stop
signal, records definitive `CANCELLED` usage of the same 4,510 input and 910
output tokens, and exports `sessions.checkpoint` before any capture. The
fixture's strict JSON codec accepts only the public checkpoint records needed
by this pre-collection case; it rejects unknown fields and deliberately does
not serialize pending operation objects. A fresh provider process validates
and imports that checkpoint without dispatching inference, then performs a
one-item `settleBatch`. A fresh agent process restores its independently
persisted checkpoint, authenticates `sessions.recover`, and requests the same
cooperative close. Evidence requires one opening, one execution, one funding
authorization, two provider transactions, a generation advance and the same
5.42 capture plus 0.09 refund as the ordinary paired flow. This tests SDK
protocol recovery over caller-owned fixture persistence; it does not provide a
provider database or prescribe a production checkpoint format.

The `--onboarding` mode starts with an empty `OnboardingRegistry` fixture. It
models the required EOA registration, ownership, event and recipient-proof ABI;
it is explicitly not ERC-8004's deployed contract. The independently installed
provider registers, exports its identity for a recipient proof signed only in
the separate chain environment, confirms that recipient and publishes the
catalog last. The agent uses the installed `mppi-agent onboard` entry point.
Both completed reruns must preserve their configuration bytes and issue no new
wallet writes; the provider needs no new recipient signature. The same paid
request then uses those fresh identities. These tests use disposable local EOA
wallets; Circle service and smart-account acceptance require real chain testing.

A tiny test-only TLS frontend advertises `h2` and `http/1.1` via ALPN. It relays
native gRPC bytes to a loopback gRPC server and serves the SDK's discovery
responses over HTTP/1.1 at the same origin. Its HTTP parser is deliberately
limited to the fixture's bounded GET requests. It is not a production ingress
implementation. The fixture records negotiated HTTP/2 connections, discovery
paths, public API execution counts and wallet submissions.

Primary APIs used: Python 3.13
[`SSLContext.set_alpn_protocols`](https://docs.python.org/3.13/library/ssl.html#ssl.SSLContext.set_alpn_protocols),
[`asyncio.start_server`](https://docs.python.org/3.13/library/asyncio-stream.html#asyncio.start_server),
OpenSSL 3.0 [`req`](https://docs.openssl.org/3.0/man1/openssl-req/), and
Foundry [v1.8.3 Anvil](https://github.com/foundry-rs/foundry/blob/v1.8.3/crates/anvil/src/cmd.rs).
Provider runtime public interfaces are documented in
[Python runtime](../../docs/python-runtime.md).
