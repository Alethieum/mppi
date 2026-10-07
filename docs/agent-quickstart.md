# Agent: wallet to paid inference

The installed CLI prepares your identity and signing configuration. The SDK
then handles discovery, payment messages and inference against the provider's
MPPI endpoint. This guide uses Circle Agent Wallet on official Arc testnet,
with Python 3.12+ on macOS or Linux.

The built-in setup requires `mppi-agent` version `0.1.2`. Existing applications
can continue using the [explicit composition interface](python-quickstart.md).

## 1. Install

```sh
uv venv --python 3.12
uv pip install 'mppi-agent==0.1.2'
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
Exit code `1` reports a failure. Completed reruns retain the same identity and
signer. `--agent-id ID` adopts an existing identity after verifying its wallet.

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

The built-in client polls Arc every 2 seconds and requires chain evidence less
than 60 seconds old. Wallet operations have a 300-second wait budget, including
Circle's signing and submission steps. Expired chain evidence still stops new
authorization; waiting longer never extends a session's collection deadline.

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

## Supported wallet versions

The built-in Circle profile verifies these Arc testnet values:

| Component | Trusted value |
| --- | --- |
| Proxy runtime Keccak-256 | `0xd09e34b2e51f09092e73e3ba1d520a42972496bf2a3bfdb22bb50b9729a9fd66` |
| SingleOwnerMSCA implementation | `0x895b96c92d1007872fe1f70606e6b7eeaebd620e` |
| Implementation runtime Keccak-256 | `0x3692ce55057ce6c0da5f23b4fb8819fa6d5838758a5e88585b291fdccd36d0fa` |

The implementation has [verified source](https://explorer.testnet.arc.io/address/0x895b96c92d1007872fe1f70606e6b7eeaebd620e?tab=contract).
Mainnet source support pins the actual Circle Agent Wallet
`circle_6900_singleowner_v4` account generation:

| Component | Trusted mainnet value |
| --- | --- |
| Proxy runtime Keccak-256 | `0xd09e34b2e51f09092e73e3ba1d520a42972496bf2a3bfdb22bb50b9729a9fd66` |
| Implementation | `0x9c6e09bc32d1e012dcaa2623e66d2cc9860c1aed` |
| Implementation runtime Keccak-256 | `0x60ecaaadc845f14626e77541b390bd36a7f5c35efdee5c30471f0eb08626019e` |

The mainnet pins were bound to Circle's authenticated account metadata and its
[activation receipt](https://explorer.arc.io/tx/0x39cc91d351345c62163c6cd45ad6815f34c5cb063eb2b90cd4cd5fae53984e4c),
then checked for the adapter's self-execution call, ERC-1271 signatures and
zero-value USDC authorization simulation. That account interoperability check
does not establish a funded MPPI exchange or an audit of Circle's implementation.
Unsupported code fails verification. RPC-returned code is never automatically
trusted. Circle's [contract versions](https://developers.circle.com/wallets/wallet-upgrades#contract-versions)
differ between products and account generations; these pins do not cover every
Circle SCA or infer an Agent Wallet version from Developer Wallet documentation.

Existing compatible wallet integrations retain `FundingWallet`, `SessionSigner`,
explicit `Client(...)` and `--wallet-factory MODULE:FUNCTION --profile FILE`.
See [advanced wallet integration](python-onboarding.md).
