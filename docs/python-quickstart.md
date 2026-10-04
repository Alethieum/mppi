# Connect a Python provider and agent

This walkthrough installs version `0.1.0` from PyPI and composes two independent
applications against the official Arc testnet deployment. Start in your own
application directory. Python 3.12+ is required; the agent's checkpoint locking
supports macOS and Linux.

The [official Arc testnet contracts](deployment.md) are deployed; configuration
and Python/Rust SDK profile checks passed, including the scoped
configuration/profile security review. Internal acceptance with frozen release
artifacts passed fresh Rust provider and Python agent onboarding in separate
Apple containers, two real inference completions, and independently verified
exact settlement and refund accounting with zero remaining liability. The
[v0.1.0 release record](https://github.com/Alethieum/mppi/releases/tag/v0.1.0)
identifies the exact artifacts, published-install verification status and security
review scope and results. Supply the deployment profile explicitly.
The provider builds and hosts its own testnet service and supplies its URL or
ERC-8004 ID to agent developers. This walkthrough uses that provider's URL and
the official profile. Provider endpoint availability is independent of the SDK
release.

## 1. Install independent roles

With [uv installed](https://docs.astral.sh/uv/getting-started/installation/), create
an environment for each role you operate. On the provider host:

```sh
uv venv --python 3.12 .venv-provider
uv pip install --python .venv-provider/bin/python 'mppi-provider==0.1.0'
```

On the agent host:

```sh
uv venv --python 3.12 .venv-agent
uv pip install --python .venv-agent/bin/python 'mppi-agent==0.1.0'
```

Each role package installs its required `mppi-protocol==0.1.0` dependency.
The provider process uses `.venv-provider/bin/python`; the agent process uses
`.venv-agent/bin/python`. They communicate over the provider's HTTPS origin.
The commands follow uv's [package installation](https://docs.astral.sh/uv/pip/packages/#installing-a-package)
and [environment selection](https://docs.astral.sh/uv/pip/environments/#using-arbitrary-python-environments)
interfaces. Source development uses the separate [local build guide](build.md).

Record the installed dependency versions and import locations on each host.
For the provider:

```sh
uv pip freeze --python .venv-provider/bin/python > provider-packages.txt
.venv-provider/bin/python -I -c 'import mppi_provider, mppi_protocol; print(mppi_provider.__file__, mppi_protocol.__file__)'
```

For the agent:

```sh
uv pip freeze --python .venv-agent/bin/python > agent-packages.txt
.venv-agent/bin/python -I -c 'import mppi_agent, mppi_protocol; print(mppi_agent.__file__, mppi_protocol.__file__)'
```

The printed import paths must point into their respective environments. Retain
these records with the source revision and artifact digests from the release
record.

## 2. Supply the integration inputs

| Input | Provider application | Agent application |
| --- | --- | --- |
| Deployment | Trusted [profile JSON](deployment.md#trusted-profile-file), selected network and RPC access | The same trusted deployment and its own RPC access |
| Wallet | `TransactionWallet` for the registered operator; separate payout recipient | Funded `FundingWallet` and its ERC-8004 identity |
| Signing | Wallet submission capability; recipient proof during onboarding | A recoverable fixed `SessionSigner`, separate from the funding wallet |
| Token domain | Verified USDC authorization domain name/version for the selected network | The same verified token domain |
| Configuration | Applied `ProviderConfig`, matching confirmed identity/catalog | Provider HTTPS origin, published model ID, explicit positive funding choice |
| Hosting | Existing HTTPS discovery and native HTTP/2 gRPC routing, certificates and server lifecycle | System-trusted provider TLS; explicit CA file for private PKI |
| Execution | `CoverageProvider` and `ExecutionProvider` integrations | Original request messages and response consumer |
| Retained state | Existing provider evidence retention and wallet operation references | Stable onboarding file, payment checkpoint, wallet journal and signer reference |

Keep secret values in your secret integration. Pass capabilities to Python;
never put keys or credential-bearing RPC URLs in shell arguments. The deployment
profile contains public pins, not secrets. Loading that profile validates its
shape; establishing its provenance is a separate requirement.

Download the [v0.1.0 Arc testnet profile](https://github.com/Alethieum/mppi/releases/download/v0.1.0/arc-testnet.json)
as `arc-profile.json` and verify its SHA-256 using the
[trusted-profile instructions](deployment.md#trusted-profile-file). Pass that
verified file's path to `chain_bindings` below for each role. Install a copy with
each application. The SDK does not automatically select or download a profile.

Create the common chain bindings once per application. Supply verified token
domain strings explicitly; the MPPI signing domain is distinct from USDC's.
Call this blocking setup outside the serving event loop:

```python
from pathlib import Path
from mppi_protocol.chain import ChainReader, HttpJsonRpc
from mppi_protocol.contract import ContractCalls
from mppi_protocol.deployment import load_profile
from mppi_protocol.follower import ChainFollower


def chain_bindings(
    profile_path: Path,
    rpc_url: str,
    *,
    max_age_ms: int,
    collection_margin_ms: int,
    usdc_domain_name: str,
    usdc_domain_version: str,
):
    profile = load_profile(profile_path)
    reader = ChainReader(HttpJsonRpc(rpc_url), profile)
    reader.verify_deployment(reader.block())
    follower = ChainFollower(
        reader,
        max_age_ms=max_age_ms,
        collection_margin_ms=collection_margin_ms,
    )
    contract = ContractCalls(profile.domain, usdc_domain_name, usdc_domain_version)
    return follower, contract
```

The provider's follower timings must equal its validated configuration. The
agent owns a separate follower and its background event loop. Do not share one
follower between roles. Canonical RPC support and freshness requirements are
documented in [Fresh chain state](python-runtime.md#fresh-chain-state).

## 3. Onboard and attach the provider SDK

Use the shared native [provider onboarding CLI](rust-cli.md#3-register-and-retain-the-result)
to create/check the operator wallet, register ERC-8004, bind the separate payout
recipient and publish the catalog last. The CLI supports Python integrations;
its output is applied `mppi.toml`, setup records and the confirmed provider ID.
Keep its sidecars. Rerunning checks completed steps and reconciles pending ones.
Python-only applications can implement the same sequence with the public
[registry helpers](python-onboarding.md#provider-registration-and-publication).

Load `ProviderConfig.from_file(Path("mppi.toml"))`. The
[example configuration](../examples/provider/mppi.toml) contains illustrative
addresses and declarations; replace them with your confirmed setup. Supply the
operator wallet described in [Provider wallet integration](python-onboarding.md#provider-wallet-integration).

Your host creates its `grpc.aio.server(options=GRPC_OPTIONS)` on the serving
event loop, supplies coverage/execution implementations, and calls:

```python
from mppi_provider import Mppi
from mppi_protocol.wire import GRPC_OPTIONS


async def attach_mppi(server, config, chain, contract, operator_wallet, coverage, execution):
    sdk = Mppi(
        config,
        chain=chain,
        contract=contract,
        wallet=operator_wallet,
        coverage=coverage,
        execution=execution,
    )
    sdk.register_grpc(server)  # Before the host starts its gRPC server.
    readiness = await sdk.start()
    return sdk, readiness
```

The host starts its own listener and maps `sdk.discovery(path)` responses to
`GET /discover`, `GET /.well-known/agent-registration.json` and `GET /models`
on the configured HTTPS origin. Return the supplied status, headers and body
unchanged. A `None` response provides no document. Check `readiness.ready`;
unavailable chain evidence does not make the endpoint payment-ready. See the
[runtime reference](python-runtime.md#provider-composition) for retrying chain
observations and host lifecycle.

The integration signatures are:

| Integration | Obligation |
| --- | --- |
| `await coverage(request, context) -> TokenCounts` | Prepare required input/cache/output counts before paid execution; include at least the configured output buffer. |
| `execution(request, context, runs, stop_requested) -> AsyncIterator[bytes]` | Execute admitted work and yield original JSON objects after removing upstream framing. |
| `await runs.require_coverage(context, remaining_tokens)` | Obtain a granted result before consuming the next complete remaining bound. |
| `await runs.report_usage(context, cumulative_counts)` | Supply cumulative counts early enough for voucher renewal. |
| `await runs.report_terminal(context, final_counts, status)` | Report definitive actual termination and final counts. |

The provider owns counting, cache policy, engine calls and stopping its job.
MPPI validates payment coverage and supplied accounting. Stop work when coverage
is denied; later vouchers cannot restart that run. Missing final usage stays
unresolved. Network EOF and a cancellation signal are not definitive finals.
The SDK supplies no database, worker coordinator or inference gateway.

## 4. Onboard the agent and make one paid request

Supply the selected [agent wallet and fixed signer](python-onboarding.md#agent-setup).
The funding wallet's ERC-8004 ID is required; the session signer has no separate
registration. Use a private state directory with stable paths. `onboard` keeps
its registration record; `Client` uses a different payment checkpoint. A Circle
Agent Wallet adapter also needs its own operation journal.

This function takes application-owned capabilities as arguments. `consume`
processes original response JSON bytes; `retain` records the named operation
and shutdown results in the application's existing evidence handling. Keep both
callbacks bounded and ensure retention failures reach your application.

```python
from pathlib import Path
from mppi_agent import Client, onboard
from mppi_protocol.registry import RegistryReader
from mppi_protocol.types import OperationState


def exchange(
    *,
    provider_origin,
    model,
    chain,
    contract,
    funding_wallet,
    signer,
    signer_reference,
    state_dir: Path,
    initial_session_tokens,
    consume,
    retain,
    ca_file=None,
):
    identity = onboard(
        wallet=funding_wallet,
        registry=RegistryReader(chain.reader),
        config_path=state_dir / "onboarding.json",
    )
    retain("onboarding", identity)
    if not identity.complete:
        return  # Rerun with the same file to reconcile the original registration.
    assert identity.identity is not None

    client = Client(
        provider_origin,
        wallet=funding_wallet,
        signer=signer,
        chain=chain,
        contract=contract,
        checkpoint_path=state_dir / "payments.json",
        signer_reference=signer_reference,
        initial_session_tokens=initial_session_tokens,
        agent_id=identity.identity.agent_id,
        ca_file=ca_file,
    )
    try:
        opening = client.sessions.open(provider_origin, model=model)
        retain("opening", opening)
        if opening.operation.state is not OperationState.CONFIRMED:
            return  # Preserve the operation reference; do not replace funding.
        if opening.session is None:
            raise RuntimeError("No ready session was returned")
        with client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "Hello"}],
            stream=True,
        ) as stream:
            for payload in stream:
                consume(payload)
            retain("terminal", stream.terminal)
            retain("receipt", stream.receipt)
        close = client.sessions.close(opening.session.session_id)
        retain("close", close)
        # Only CONFIRMED establishes the canonical cooperative close.
    finally:
        retain("shutdown", client.close())
```

The `Client` performs registry-bound discovery, checks the challenge and accepted
prices, submits the caller's positive opening choice and authenticates Control.
It sends inference to the provider's MPPI Complete service; it does not contact
an upstream inference API directly. Signed vouchers authorize cumulative USDC
coverage calculated from provider-supplied counts and accepted prices.

`initial_session_tokens` is your funding choice. Low credit does not trigger an
automatic transfer. For longer use, integrate `Client(on_low_credit=callback)`
with your approved funding policy; explicitly call
`client.sessions.topUp(session_id, amount)` with positive USDC base units and
inspect its operation result. Ordinary voucher renewal remains automatic within
confirmed funding. See [agent composition](python-runtime.md#agent-composition)
and [lifecycle observers](lifecycle-hooks.md) for callback behavior and bounds.

## 5. Confirm outcomes and retain uncertain work

| Result | Next action |
| --- | --- |
| `CONFIRMED` operation | Matching canonical chain evidence is available; retain its original reference. |
| `PENDING` / `UNKNOWN` | Reconcile the original operation; do not replace its funding or submission. Agent method: `sessions.reconcile(operation_ref)`. |
| `FAILED` | Inspect failure evidence before authorizing a distinct attempt; never infer failure from a timeout. |
| Missing final inference accounting | Retain the original run and reservation; do not invent token usage or replay inference. |
| Provider unavailable | Agent `requestClose`, then eligible `withdraw`, provide the contract exit path. `releaseIdle` has its own eligibility. |

The three resource closures are distinct:

- `stream.close()` cancels one Complete; it does not close escrow.
- `client.close()` releases agent I/O and returns unresolved evidence; it does
  not close escrow. Keep its checkpoint and fixed signer across restarts.
- `await sdk.shutdown(deadline)` stops provider grants and returns unresolved
  work. The deadline is absolute `time.monotonic()` time. The host stops its own
  server. Settlement and escrow close remain explicit economic operations.

The provider can call `await sdk.sessions.settle(session_id)` to capture actual
usage while the session remains open; `await sdk.sessions.close(session_id)`
requests closure and refunds unused deposit after canonical confirmation.
Its application retains `await sdk.sessions.checkpoint(session_id)` using its
existing storage and supplies complete evidence to `sessions.recover` after a
restart. Agent recovery uses its existing checkpoint and `sessions.recover`.
Neither recovery API resumes inference automatically.

## Source references

The snippets follow the current public implementations:
[profile parsing](../proto/python/src/mppi_protocol/deployment.py),
[chain reads](../proto/python/src/mppi_protocol/chain.py),
[follower ownership](../proto/python/src/mppi_protocol/follower.py),
[provider interfaces](../sdks/provider/python/src/mppi_provider/interfaces.py),
[provider runtime](../sdks/provider/python/src/mppi_provider/runtime.py),
[agent onboarding](../sdks/agent/python/src/mppi_agent/onboarding.py),
[agent runtime](../sdks/agent/python/src/mppi_agent/client.py) and
[operation records](../proto/python/src/mppi_protocol/types.py).
See the [complete API reference](sdk-interfaces.md) for the remaining operations.
