# Provider: onboard and accept paid inference

This guide connects your Python or Rust inference application to MPPI on official
Arc testnet. MPPI supplies registration, payment authorization, usage accounting
and settlement. Your application supplies inference, token counts, final job
outcomes, HTTPS hosting and retained session history.

## 1. Install the native CLI and your SDK

Python providers use the same native onboarding CLI as Rust providers. On
**Linux ARM64**, download the release binary and verify its pinned SHA-256 before
installing it:

```sh
set -eu
curl -fL -o mppi-linux-arm64 \
  https://github.com/Alethieum/mppi/releases/download/provider-v0.1.1/mppi-linux-arm64
printf '%s  %s\n' 'f8a989e5d4d590552e44249ec6791650a46a8f6ffedaad7e82b980edeaa1aca3' mppi-linux-arm64 | sha256sum --check -
mkdir -p "$HOME/.local/bin"
install -m 755 mppi-linux-arm64 "$HOME/.local/bin/mppi"
export PATH="$HOME/.local/bin:$PATH"
mppi --version
```

The [provider release](https://github.com/Alethieum/mppi/releases/tag/provider-v0.1.1)
includes the binary and `SHA256SUMS`. Linux ARM64 is the published native binary
target. For a source build, use Rust 1.95.0 and a working native build toolchain:

```sh
cargo install mppi-provider --version 0.1.1 --locked --bin mppi
mppi --version
```

Ensure Cargo's executable directory is on `PATH`. `--version` selects exactly
`0.1.1`; `--locked` uses the package's dependency lockfile. Source builds on other
platforms are not covered by the Linux ARM64 binary verification.
[Cargo installation and lockfiles](https://doc.rust-lang.org/cargo/commands/cargo-install.html).

For a Python application, use Python 3.12+:

```sh
uv venv --python 3.12
uv pip install 'mppi-provider==0.1.1'
```

The shared protocol dependency installs automatically. For a Rust application,
add `mppi-provider = "=0.1.1"` to `[dependencies]` in your own `Cargo.toml` and
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

Configure `CIRCLE_API_KEY` and the registered `CIRCLE_ENTITY_SECRET` through
your secret environment. The native CLI uses a Circle developer-controlled SCA
as the operator; configure Circle Gas Station for Arc testnet. This operator is
separate from the payout recipient. See
[Circle developer wallet setup](https://developers.circle.com/wallets/dev-controlled/create-your-first-wallet).

Select a trusted RPC. This public endpoint is listed in
[Arc's testnet connection details](https://docs.arc.io/arc/references/connect-to-arc):

```sh
export ARC_RPC_URL_TESTNET=https://rpc.blockdaemon.testnet.arc.io
```

Use your own endpoint through the secret environment when needed. It must
support the SDK's [canonical reads](python-runtime.md#fresh-chain-state).

## 3. Onboard your provider

```sh
mppi provider onboard --testnet --declarations provider-declarations.toml \
  --rpc-env ARC_RPC_URL_TESTNET
```

The CLI uses the packaged [official Arc testnet profile](deployment.md#trusted-profile-file)
and verifies the observed deployment against its pinned addresses and code.
`--rpc-env` reads the URL from your selected variable. Use `--profile FILE` for
an independently trusted explicit deployment.

Without `--rpc-env`, the CLI uses the configuration's RPC, whose testnet default
is `https://rpc.testnet.arc.io`. Environment overrides are explicit; setting a
variable alone does not select it.

`--wallet-id UUID` selects an existing accessible Circle SCA. Otherwise the CLI
creates the missing operator wallet resources. It registers ERC-8004, binds the
separate recipient and publishes your model catalog last.

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
It reads the same Circle environment variables used for onboarding. It starts
no listener or inference job. The host supplies both callbacks:

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
