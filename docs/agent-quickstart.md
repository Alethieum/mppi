# Agent: wallet to paid inference

The installed CLI prepares your identity and signing configuration. The SDK
then handles discovery, payment messages and inference against the provider's
MPPI endpoint. This guide uses Circle Agent Wallet on official Arc testnet,
with Python 3.12+ on macOS or Linux.

The built-in setup requires `mppi-agent` version `0.1.5`. Existing applications
can continue using the [explicit composition interface](python-quickstart.md).

## 1. Install

```sh
uv venv --python 3.12
uv pip install 'mppi-agent==0.1.5'
```

The shared protocol dependency installs automatically. Run the remaining
commands from the same application directory.

## 2. Supply your wallet

Use your authenticated, funded Circle Agent Wallet on Arc testnet. For a new
wallet, follow [Circle's setup](https://developers.circle.com/agent-stack/agent-wallets/quickstart):

```sh
npm install -g @circle-fin/cli@1.1.4
circle wallet login you@example.com --testnet
circle wallet list --type agent --chain ARC-TESTNET --output json
circle wallet fund --address YOUR_WALLET_ADDRESS --chain ARC-TESTNET
```

Complete Circle authentication yourself; its CLI retains its credentials. MPPI
does not require your funding wallet's private key. If Circle authentication
expires, log in again to restore access to the same wallet.

The SDK runs `circle` from your `PATH` for onboarding and the client. To use
another installation, such as a project-local npm install, set `MPPI_CIRCLE_CLI`
to its executable path, for example
`export MPPI_CIRCLE_CLI="$PWD/node_modules/.bin/circle"`.

## 3. Onboard

Add `.mppi/` to your application's `.gitignore` before onboarding. If you choose
a custom configuration directory, exclude that directory from version control.

```sh
.venv/bin/mppi-agent onboard --testnet --wallet YOUR_WALLET_ADDRESS
```

The CLI loads its packaged official deployment profile, verifies the selected
wallet and USDC balance, registers ERC-8004 and prepares a fixed session signer.
The intended registration also activates a supported undeployed Circle account;
deployed code is verified before completion. Onboarding deposits no inference funds.

JSON `complete: true` and exit code `0` mean setup is ready. Exit code `2` means
the original operation is pending: rerun the same command to reconcile it.
Exit code `1` reports a failure. Its message names the cause where the SDK can
establish it: the Circle CLI is missing or not version `1.1.4`; Circle could not
verify the wallet on the selected network, so the message gives the
`circle wallet list` command that shows Circle's reason and the login that
network needs; that login does not list the address; or the wallet holds no
USDC on that network. Circle's own output is never printed. Completed reruns
retain the same identity and signer. `--agent-id ID` adopts an existing
identity after verifying its wallet.

Keep the whole private `.mppi` directory across restarts and back it up securely.
`.mppi/agent.json` is non-secret configuration. Its separate mode-0600 `.signer`
file contains a **plaintext local session credential protected by OS permissions**.
It has no wallet execution authority. Missing or changed retained credentials
stop setup; the SDK does not silently replace them. `--config PATH` selects
another configuration in a private directory.

The default RPC is Arc's documented `https://rpc.blockdaemon.testnet.arc.io`.
Set `ARC_RPC_URL_TESTNET` through your secret environment for your own trusted
RPC; `--rpc-env NAME` selects a different variable. URLs with credentials are
never stored in configuration.

Requests within an attached session read no chain: they use the session's
confirmed deposit, accepted prices and attachment. Opening, attachment, top-up
and close use chain evidence less than 60 seconds old, refreshing first when it
is older, and the client also refreshes once a minute in the background; pass
`Client.from_config(..., max_age_ms=..., poll_interval_ms=...)` to choose
another cadence. A failed background refresh reaches the error hook. An
unavailable RPC never blocks requests; evidence contradicting the deployment
stops them until a later refresh is accepted. See
[read cost](python-runtime.md#fresh-chain-state) to size a shared RPC plan.
Wallet operations have a 300-second wait budget, including
Circle's signing and submission steps. Waiting never extends a session's
collection deadline. A Complete stream has no deadline unless you set one: an explicitly
composed `Client(..., request_timeout_ms=...)` bounds each inference RPC.

## 4. Make a request

Your provider supplies its HTTPS origin and model ID. Replace those values and
choose the positive token budget you authorize for the initial session deposit.
The SDK calculates its USDC amount from accepted provider prices.

```python
from mppi_agent import Client

with Client.from_config(
    "https://YOUR_PROVIDER",
    initial_session_tokens=10_000,
) as client:
    response = client.chat.completions.create(
        model="YOUR_PROVIDER/MODEL",
        messages=[{"role": "user", "content": "Hello"}],
    )
    print(response.decode("utf-8"))
```

Run your script with `.venv/bin/python`. Inference goes to the provider's MPPI
endpoint; the agent needs no upstream inference API credential. `stream=True`
returns a context-managed iterator of original response JSON bytes, with terminal
and receipt records. Voucher renewal is automatic within confirmed funding.
Extra deposits remain explicit through `sessions.topUp(session_id, amount)`, in
six-decimal USDC base units.

One setup retains one provider's payment checkpoint. Reuse that configuration
and provider across restarts. A conflicting provider or signer is rejected.
Client shutdown releases connections; the session remains funded for reuse.
Inspect `client.sessions.list()` and call `client.sessions.close(session_id)`
when finished to settle and refund unused funds. Inspect the returned operation.
Once this agent's own close is confirmed, the next request or `sessions.open`
funds a new session with the same signer. The closed session's last record
stays beside the checkpoint as
`.mppi/agent.json.payments.<session id>.closed.json`.
For pending or unknown wallet operations, use `sessions.reconcile(operation_ref)`.
For cooperative close, repeat `sessions.close(session_id)` to inspect the same
close operation. Never repeat inference to resolve an uncertain payment.
See [runtime lifecycle](python-runtime.md).

## Select mainnet explicitly

Circle Agent Wallet supports both [Arc networks](https://developers.circle.com/agent-stack/agent-wallets/supported-blockchains).
It is the same wallet product, with separate mainnet and testnet login sessions,
chain balances and MPPI configuration. Use the address returned for `ARC`;
do not assume a testnet wallet or identity is ready on mainnet.

Authenticate and select your mainnet wallet:

```sh
circle wallet login you@example.com
circle wallet list --type agent --chain ARC --output json
circle wallet balance --address YOUR_MAINNET_WALLET --chain ARC
```

Circle login without `--testnet` selects mainnet. Follow
[Circle's funding instructions](https://developers.circle.com/agent-stack/agent-wallets/quickstart)
to fund that wallet with your chosen amount of real USDC. Then use a separate
private configuration directory:

```sh
.venv/bin/mppi-agent onboard --mainnet --wallet YOUR_MAINNET_WALLET \
  --config .mppi-mainnet/agent.json
```

Exclude `.mppi-mainnet/` from version control and retain the entire directory.
The network flag selects the packaged deployment and `ARC_RPC_URL_MAINNET`.
If that variable is unset, the agent uses Arc's documented
`https://rpc.blockdaemon.mainnet.arc.io`; an explicit `--rpc-env NAME` selects
your own environment reference. See [Arc connection details](https://docs.arc.io/arc/references/connect-to-arc).

Use the same network explicitly when constructing the client:

```python
from pathlib import Path
from mppi_agent import Client

with Client.from_config(
    "https://YOUR_MAINNET_PROVIDER",
    config_path=Path(".mppi-mainnet/agent.json"),
    testnet=False,
    initial_session_tokens=10_000,
) as client:
    # Use the request and session methods shown above.
    print(client.agent_id)
```

The Python agent API defaults to testnet. `onboard_circle(..., testnet=False)`
also selects mainnet and its RPC environment when `rpc_env` is omitted.
Both setup and client construction reject a retained configuration from another
deployment. An explicit trusted `profile=` override remains supported.

## Wallet requirements

Built-in setup accepts any Circle Agent Wallet account generation; it pins no
account code or implementation. Before submitting a session opening, the agent
checks at one block what makes the authorization valid for it:

- the registry binds its ERC-8004 agent id to this wallet, as the provider
  checks when the session opens;
- the RPC serves the selected network;
- USDC would accept the wallet's signature: an ECDSA signature for an account
  without code, or ERC-1271 `isValidSignature` approval from a smart account.

A refusal raises `FundingAuthorizationError` with `reason` `AGENT_ID_REQUIRED`,
`AGENT_WALLET_MISMATCH` or `INVALID_SIGNATURE`. Nothing is submitted, and the
same funding can be retried once the wallet can sign it. A top-up relies on the
binding and signature its session's opening proved; USDC still checks it, and a
refusal is a definite failed top-up. The MPPI contract and USDC enforce
everything else on chain. A smart account must be deployed before
it funds; onboarding's registration deploys it. Exits such as `requestClose`
and `withdraw` need no agent id or signature check.

Existing compatible wallet integrations retain `FundingWallet`, `SessionSigner`,
explicit `Client(..., agent_id=...)` and `--wallet-factory MODULE:FUNCTION --profile FILE`.
Other wallets fit behind the same interface with a thin adapter, including
wallets whose service signs as it executes; see
[funding wallet adapters](python-onboarding.md#funding-wallet-adapters).
