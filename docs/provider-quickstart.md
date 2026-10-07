# Provider: onboard and accept paid inference

This guide connects your Python or Rust inference application to MPPI on official
Arc testnet. MPPI supplies registration, payment authorization, usage accounting
and settlement. Your application supplies inference, token counts, final job
outcomes, HTTPS hosting and retained session history.

This walkthrough uses provider `0.1.2` and shared Python protocol `0.1.1`.
Both networks use the same Circle Agent Wallet integration. Select testnet or
mainnet explicitly and retain separate configuration and operation records.

## 1. Install the native CLI and your SDK

Python providers use the same native onboarding CLI as Rust providers. On
**Linux ARM64**, download the release binary and verify its release SHA-256 before
installing it:

```sh
set -eu
curl -fL -o mppi-linux-arm64 \
  https://github.com/Alethieum/mppi/releases/download/provider-v0.1.2/mppi-linux-arm64
curl -fL -o SHA256SUMS \
  https://github.com/Alethieum/mppi/releases/download/provider-v0.1.2/SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
mkdir -p "$HOME/.local/bin"
install -m 755 mppi-linux-arm64 "$HOME/.local/bin/mppi"
export PATH="$HOME/.local/bin:$PATH"
mppi --version
```

The [provider release](https://github.com/Alethieum/mppi/releases/tag/provider-v0.1.2)
includes the binary and `SHA256SUMS`. Linux ARM64 is the published native binary
target. For a source build, use Rust 1.95.0 and a working native build toolchain:

```sh
cargo install mppi-provider --version 0.1.2 --locked --bin mppi
mppi --version
```

Ensure Cargo's executable directory is on `PATH`. `--version` selects exactly
`0.1.2`; `--locked` uses the package's dependency lockfile. Source builds on other
platforms are not covered by the Linux ARM64 binary verification.
[Cargo installation and lockfiles](https://doc.rust-lang.org/cargo/commands/cargo-install.html).

For a Python application, use Python 3.12+:

```sh
uv venv --python 3.12
uv pip install 'mppi-provider==0.1.2'
```

The shared protocol dependency installs automatically. For a Rust application,
add `mppi-provider = "=0.1.2"` to `[dependencies]` in your own `Cargo.toml` and
build with your application's committed lockfile.

## 2. Generate your declarations

Work in a private application directory. Exclude the configuration and its
sidecars from version control, and keep them across restarts.

```sh
mppi provider init --testnet
```

This writes `provider-declarations.toml` without contacting a wallet or network.
It refuses to replace an existing file. `--declarations PATH` selects another
output path. Edit every `REPLACE` value and review the illustrative settings:

| Declaration | Your value |
| --- | --- |
| Provider name and HTTPS origin | Your service's name and public origin |
| Payout recipient | A separate receive-only wallet whose signing custody stays outside serving |
| Model declarations and rates | Your model IDs, capabilities and quoted USDC base units per million tokens |
| Upstream API base | Your inference API's complete base URL, including its path prefix |
| Payment and freshness policy | Bounds appropriate to your token reporting and chain access |
| Listener settings | Add your Rust listener settings, or use your Python host's own server configuration |

The template has no `[identity]` section or preselected listener. Onboarding
writes confirmed identity fields to the separate `mppi.toml` result. Declaring an upstream URL does not
create an inference adapter.

Select a Circle Agent Wallet as the provider operator. Install the supported
CLI and complete Circle login yourself:

```sh
npm install -g @circle-fin/cli@1.1.4
circle wallet login you@example.com --testnet
circle wallet list --type agent --chain ARC-TESTNET --output json
```

Use the selected address as `YOUR_OPERATOR_WALLET` below. The provider operator
must differ from the payout recipient. Keep the recipient's signing authority
outside the serving process. If multiple wallets share a Circle account, their
addresses are separate but their account custody is shared.

Circle retains its login session; mainnet and testnet sessions are separate and
expire after 28 days. Renew the same session when needed and retain the original
MPPI operation records. Circle Agent Wallet currently sponsors transaction gas,
subject to caps and fair use; operator funding is not a prerequisite for these
sponsored operations. See Circle's [authentication](https://developers.circle.com/agent-stack/agent-wallets/wallet-operations/authenticate)
and [fees](https://developers.circle.com/agent-stack/agent-wallets/fees).

Select a trusted RPC. This public endpoint is listed in
[Arc's testnet connection details](https://docs.arc.io/arc/references/connect-to-arc):

```sh
export ARC_RPC_URL_TESTNET=https://rpc.blockdaemon.testnet.arc.io
```

Use your own endpoint through the secret environment when needed. It must
support the SDK's [canonical reads](python-runtime.md#fresh-chain-state).

## 3. Onboard your provider

```sh
mppi provider onboard --testnet --wallet YOUR_OPERATOR_WALLET \
  --declarations provider-declarations.toml --rpc-env ARC_RPC_URL_TESTNET
```

The CLI uses the packaged [official Arc testnet profile](deployment.md#trusted-profile-file)
and verifies the observed deployment against its pinned addresses and code.
`--rpc-env` reads the URL from your selected variable. Use `--profile FILE` for
an independently trusted explicit deployment.

Without `--rpc-env`, the CLI uses the configuration's RPC, whose testnet default
is `https://rpc.testnet.arc.io`. Environment overrides are explicit; setting a
variable alone does not select it.

`--wallet ADDRESS` selects the existing authenticated Agent Wallet. Onboarding
registers ERC-8004, binds the separate recipient and publishes your model catalog
last. A saved rerun may omit `--wallet`; a different wallet is rejected. Circle
login and wallet creation remain outside MPPI onboarding.

When a recipient signature is needed, the JSON result includes `request`.
Sign that exact EIP-712 request with the recipient wallet and save the original
request and returned signature in `signed-recipient-proof.json`:

```json
{"request": {"...": "the complete unchanged exported request"}, "signature": "0x..."}
```

Then rerun:

```sh
mppi provider onboard --testnet --declarations provider-declarations.toml \
  --rpc-env ARC_RPC_URL_TESTNET --recipient-proof signed-recipient-proof.json
```

Exit code `0` means onboarding completed; `2` means a recipient proof or original
operation is pending; `1` reports a failure. Rerun the same command to reconcile
pending work. A completed rerun verifies the existing setup without another
registration. If an unsubmitted proof expires, rerun without `--recipient-proof`
to obtain a fresh request. Keep `mppi.toml` and all its sidecars in the same private
directory. See the [CLI reference](rust-cli.md#local-records-and-results) for
their names and retained-operation behavior.

## 4. Attach the SDK to your application

For Python, compose the runtime on your serving event loop:

```python
import grpc
from mppi_provider import Mppi
from mppi_protocol.wire import GRPC_OPTIONS


async def attach_mppi(coverage, execution):
    sdk = await Mppi.from_config(
        "mppi.toml",
        testnet=True,
        rpc_env="ARC_RPC_URL_TESTNET",
        coverage=coverage,
        execution=execution,
    )
    server = grpc.aio.server(options=GRPC_OPTIONS)
    sdk.register_grpc(server)
    readiness = await sdk.start()
    return sdk, server, readiness
```

`from_config` checks the saved configuration, trusted deployment and existing
Circle operator, then constructs the wallet, chain follower and contract calls.
It uses the authenticated Circle CLI and the retained
`mppi.toml.wallet-operations.json` from onboarding. It starts no listener or inference job. The host supplies both callbacks:

| Callback | Required behavior |
| --- | --- |
| `await coverage(request, context)` | Return measured `TokenCounts` for prefill and at least the configured output buffer before paid execution. |
| `execution(request, context, runs, stop_requested)` | Yield original inference JSON bytes; report cumulative usage and definitive terminal counts through `runs`; obtain coverage before more work. |

The provider terminates its actual job when coverage is denied. Network EOF
and a cancellation signal do not establish final usage. See the
[callback and runtime reference](python-runtime.md#provider-composition).

Your Python host configures TLS and starts its gRPC server. Publish
`sdk.discovery(path)` responses unchanged at `/discover`,
`/.well-known/agent-registration.json` and `/models` on the same HTTPS origin.
Route native gRPC over HTTP/2. Check `readiness.ready` before advertising paid
availability. Retain session checkpoints and unresolved shutdown results using
your existing storage.

For Rust, use `setup::runtime_from_config` and either `standalone::serve` or your
existing tonic host. The [Rust composition example](rust-runtime.md#compose-a-provider-host)
shows the exact inputs and listener lifecycle. The native CLI can launch your
compiled host with `mppi provider serve --testnet --rpc-env ARC_RPC_URL_TESTNET
--host /absolute/path/to/host`.

## 5. Verify your endpoint

After your application is serving its configured public origin:

```sh
mppi provider verify --testnet --rpc-env ARC_RPC_URL_TESTNET
```

This checks operator access, deployment, identity, discovery, TLS and unpaid
native gRPC behavior. It does not fund a session or run paid inference. A
successful result applies to the endpoint observed at that time.

Give agent developers your HTTPS origin or ERC-8004 ID and model IDs. The
[agent quickstart](agent-quickstart.md) covers a funded agent's first request.
Capture reported usage with the SDK's session methods and inspect canonical
operation results. Keep uncertain operations for reconciliation. For price and
configuration changes, use an [explicit update](rust-cli.md#apply-explicit-changes).

The [Python runtime](../sdks/provider/python/src/mppi_provider/runtime.py) and
[setup source](../sdks/provider/python/src/mppi_provider/setup.py),
[native setup source](../sdks/provider/rust/src/setup.rs) and
[CLI implementation](../sdks/provider/rust/src/cli.rs) define these entry points.

## Select mainnet explicitly

Use a separate private application directory for mainnet declarations,
`mppi.toml` and all sidecars. Authenticate the same Circle Agent Wallet product
for `ARC` and select its mainnet operator address. Testnet wallet resources and
recipient proofs do not become mainnet state. Circle's sponsorship conditions
apply to provider operations on either network.

The SDK packages the mainnet deployment profile. Select mainnet when logging in,
generating declarations and onboarding:

```sh
circle wallet login you@example.com
circle wallet list --type agent --chain ARC --output json
export ARC_RPC_URL_MAINNET=https://rpc.blockdaemon.mainnet.arc.io
mppi provider init --mainnet
# Edit all declarations and use your mainnet payout recipient.
mppi provider onboard --mainnet --wallet YOUR_MAINNET_OPERATOR_WALLET \
  --declarations provider-declarations.toml --rpc-env ARC_RPC_URL_MAINNET
```

The CLI selects the packaged profile from the resolved configuration network.
Once `mppi.toml` records mainnet, subsequent commands can omit `--mainnet`; keep
the same RPC selection when importing the recipient proof, serving or verifying.
`--profile FILE` remains an optional independently trusted override and must
match the selected network. The
[Circle Agent Wallet networks](https://developers.circle.com/agent-stack/agent-wallets/supported-blockchains)
and [Arc connection details](https://docs.arc.io/arc/references/connect-to-arc)
describe the network prerequisites.

The Python source helper selects its packaged mainnet profile with
`testnet=False`:

```python
sdk = await Mppi.from_config(
    "mppi.toml",
    testnet=False,
    rpc_env="ARC_RPC_URL_MAINNET",
    coverage=coverage,
    execution=execution,
)
```

The configuration, profile, RPC chain and Circle wallet must agree on mainnet.
The provider helper uses the saved configuration's RPC when `rpc_env` is omitted;
environment overrides remain explicit. An independently trusted `profile=`
override is also supported. Runtime attachment, hosting and readiness checks are
the same as above. Verify your own endpoint and inspect canonical outcomes for
each onboarding and payment operation.
