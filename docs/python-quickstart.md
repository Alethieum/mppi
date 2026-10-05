# Python application composition reference

For the usual setup, start with the [provider quickstart](provider-quickstart.md)
or [agent quickstart](agent-quickstart.md). This reference documents explicit
application composition and a complete agent exchange using supplied wallet and
signer capabilities. Python 3.12+ is required.

Install the role your application uses:

```sh
# Provider environment
uv pip install 'mppi-provider==0.1.1'

# Separate agent environment
uv pip install 'mppi-agent==0.1.1'
```

Each role installs its required `mppi-protocol==0.1.0` dependency. The applications
communicate through the provider's HTTPS origin; neither needs the other role's
package. Source development uses the [local build guide](build.md).

## Explicit integration inputs

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
each application. The explicit composition below uses this file. The built-in
provider and agent setup paths instead use their packaged official profile.

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

## Compose the provider

Use the [provider quickstart](provider-quickstart.md) to generate declarations,
onboard the operator and recipient, and attach `await Mppi.from_config(...)` to
your host. That helper replaces the manual provider chain/wallet wiring above.
It creates no inference adapter, listener or provider storage.

For a custom wallet or chain integration, the explicit constructor remains:

```python
from mppi_provider import Mppi

sdk = Mppi(
    config,
    chain=chain_follower,
    contract=contract_calls,
    wallet=operator_wallet,
    coverage=coverage,
    execution=execution,
)
```

Construct this runtime on the serving event loop. Its follower freshness and
collection-margin values must equal the validated configuration. Follow the
[provider runtime reference](python-runtime.md#provider-composition) for gRPC
registration, discovery hosting, callbacks, readiness and shutdown.

## Compose the agent and make one paid request

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

## Confirm outcomes and retain uncertain work

| Result | Next action |
| --- | --- |
| `CONFIRMED` operation | Matching canonical chain evidence is available; retain its original reference. |
| `PENDING` / `UNKNOWN` | Reconcile the original wallet operation with `sessions.reconcile(operation_ref)`; for cooperative close, repeat `sessions.close(session_id)` to inspect that same close. Do not replace funding or replay inference. |
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
