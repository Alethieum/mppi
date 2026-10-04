# Rust provider onboarding and operation

The `mppi-provider` crate contains the native `mppi` executable and the provider
SDK. This walkthrough installs version `0.1.0` from crates.io, registers a
provider against the official Arc testnet deployment, then runs the provider's
own SDK integration. The [v0.1.0 release record](https://github.com/Alethieum/mppi/releases/tag/v0.1.0)
identifies the exact artifacts, published-install verification status and security
review scope and results.

## 1. Install the CLI and SDK

Use Rust 1.95.0. The package includes generated wire source; ordinary
installation does not need `protoc`.

```sh
cargo install mppi-provider --version 0.1.0 --locked --bin mppi
mppi --version
mppi --help
```

Ensure Cargo's executable directory is on your `PATH`. This installs the setup
and host-launch CLI. `--version` selects exactly `0.1.0`; `--locked` uses the
package's included dependency lockfile. See [Cargo installation](https://doc.rust-lang.org/cargo/commands/cargo-install.html).

Add the SDK to your provider application's `Cargo.toml`:

```toml
[dependencies]
mppi-provider = "=0.1.0"
```

Build that application using its own committed lockfile. It supplies the gateway
callbacks described in the [runtime composition guide](rust-runtime.md#compose-a-provider-host).
The exact dependency follows [Cargo version requirements](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#comparison-requirements).
For source development, use the [local build guide](build.md).

## 2. Prepare explicit inputs

Keep the following deployment inputs in your provider application's private
working directory. The filenames in the remaining commands
refer to that directory; `mppi.toml` is the output of onboarding.

| Input | Supplied by |
| --- | --- |
| `arc-profile.json` | Download the [v0.1.0 Arc testnet profile](https://github.com/Alethieum/mppi/releases/download/v0.1.0/arc-testnet.json) and verify its SHA-256 using the [trusted-profile instructions](deployment.md#trusted-profile-file). |
| `provider-declarations.toml` | You: provider origin, separate payout recipient, models, rates, upstream API base and payment policy. Start from the example below. |
| Operator credentials | Your secret environment: `CIRCLE_API_KEY` and the registered `CIRCLE_ENTITY_SECRET` for the same Circle account. |
| `ARC_RPC_URL_TESTNET` | Your secret environment: the trusted Arc testnet RPC endpoint. Pass its variable name to the CLI. |
| Recipient signing access | Your recipient wallet, separately controlled from the serving operator; used to sign the exported binding request. |
| TLS and routing | Your public hostname, certificate/key and HTTP/2 ingress routing to the MPPI services. |
| Provider host executable | Your built application using the SDK, connected to your inference gateway, counts, final outcomes and retained history. |

Use `--testnet` for Arc testnet; the configuration and profile must agree on the
network. The SDK verifies the selected deployment before registry operations.
`--config` selects the saved configuration path; its default is `mppi.toml`.
The [official Arc testnet contracts](deployment.md) are deployed; configuration
and Python/Rust SDK profile checks passed. Save the verified profile at the path
selected by `--profile`. The CLI does not select or download a profile
automatically. The scoped configuration/profile security review passed.
Internal acceptance with frozen release artifacts passed fresh Rust provider and
Python agent onboarding in separate Apple containers, two real inference
completions, and independently verified exact settlement and refund accounting
with zero remaining liability. This evidence covers the recorded private testnet
integration.

A future public testnet provider builds and hosts its own service using this
SDK and the official profile. It supplies the service URL or ERC-8004 ID to agent
developers, who use the agent SDK with the same profile. Provider endpoint
availability is independent of the MPPI SDK release.

The operator is a Circle developer-controlled SCA. Configure `CIRCLE_API_KEY`
and the registered `CIRCLE_ENTITY_SECRET` through your secret environment.
Configure Circle Gas Station for that wallet/network. The receive-only payout
recipient is a different address, with signing custody separate from serving.
Use `--rpc-env ARC_RPC_URL_TESTNET` to reference an RPC endpoint held in your
secret environment. Secret values are never command-line options.

Create initial declarations using the [configuration example](../examples/provider/mppi.toml)
with its `[identity]` section removed. Set your actual provider, model, payment,
upstream and endpoint declarations. Keep monetary values quoted. The upstream
URL is its complete API base, including its path prefix.

For the owned listener used below, also add explicit listener settings to the
declarations. This direct-TLS example uses certificate paths relative to the
declarations file:

```toml
[listen]
bind = "0.0.0.0:8443"
tls_mode = "direct"
tls_cert = "tls/fullchain.pem"
tls_key = "tls/key.pem"
```

The hostname in `provider.public_origin` must match your certificate and public
route. For existing provider ingress, set `tls_mode = "ingress"`, use your
private bind address and omit both TLS file fields. Ingress must terminate
HTTPS and carry native gRPC over HTTP/2 to that listener. The SDK does not
configure your gateway, DNS, certificates or upstream authentication.

## 3. Register and retain the result

```sh
mppi provider onboard --testnet --config mppi.toml --profile arc-profile.json \
  --declarations provider-declarations.toml --rpc-env ARC_RPC_URL_TESTNET
```

`--wallet-id` selects an existing accessible Circle SCA. Without an existing
wallet selection, onboarding creates the missing wallet resources. The command
checks registration, binds the separate recipient, prepares discovery and
publishes the initial catalog last. A provider identity is saved only after
confirmed registry evidence; ID zero remains a valid identity.

When recipient proof is needed, the JSON result contains `request`. Sign that
exact EIP-712 request with the recipient's wallet. Import its original request
and signature in a JSON file:

```json
{"request": {"...": "the complete unchanged exported request"}, "signature": "0x..."}
```

```sh
mppi provider onboard --testnet --config mppi.toml --profile arc-profile.json \
  --declarations provider-declarations.toml --rpc-env ARC_RPC_URL_TESTNET \
  --recipient-proof signed-recipient-proof.json
```

The proof binds the registry, network, provider ID, current owner, recipient
and deadline. Preserve a contract wallet's returned signature bytes. If it
expires before submission, rerun without `--recipient-proof` for a fresh request.
A confirmed recipient requires no new signature.

Rerun the same command to check a pending operation. An API response alone is
not confirmation. The command checks the original wallet operation and canonical
registry evidence. An unresolved registration never becomes another mint.
Completed onboarding checks current state without another registry write.
Keep the same configuration path and its sidecars across invocations. After
completion, `mppi.toml` contains the confirmed provider ID, operator address and
Circle wallet ID alongside the declarations. The result explicitly reports
`endpoint_verified: false` and `runtime_application_required: true`: registration
does not start your application or prove its public route.

For a deployed operator SCA, the credential check signs a fresh nonpayment
message and discards its signature. Circle rejects typed-data signing before an
SCA's first onchain transaction. Initial onboarding therefore checks the selected
wallet's deployment at a canonical block. When it has no code, the intended
registration transaction validates the entity secret and deploys the wallet;
no separate activation transaction is added. This also supports an accessible,
undeployed SCA selected with `--wallet-id`. Existing provider configurations and
identity adoption require a deployed operator. See Circle's
[SCA wallets and lazy deployment](https://developers.circle.com/wallets/signing-apis#sca-wallets-and-lazy-deployment).

If wallet creation loses its response, the next invocation searches the retained
wallet reference. An unresolved wallet-set creation requires checking Circle
Console and supplying that existing `--wallet-set-id`. The command does not
repeatedly create resources while an earlier result is unknown.

## 4. Build and start your integration

Providers supply measured coverage, cumulative usage, final job outcomes and
available session history through the existing
[Rust integration interfaces](rust-runtime.md). Their application constructs
`runtime::Mppi` with those integrations and the selected wallet.

For an owned listener, the application calls
`standalone::serve(runtime, listener_config, shutdown, policy)`. The helper starts the
SDK lifecycle and exposes the native services plus `/discover`,
`/.well-known/agent-registration.json` and `/models`. Direct TLS uses your
certificate chain and key, supporting HTTP/1 discovery and native HTTP/2 gRPC
on the same listener. Explicit `listen.tls_mode = "ingress"` selects a private
plaintext listener behind your TLS termination.

The installed CLI launches that provider application directly:

```sh
mppi provider serve --config /etc/mppi/mppi.toml \
  --testnet --profile /etc/mppi/arc-profile.json --rpc-env ARC_RPC_URL_TESTNET \
  --host /opt/provider/bin/provider-mppi-host
```

The host receives `--config`, `--profile` and any explicit `--rpc-env`,
`--timeout-ms`, `--testnet` and `--upstream` arguments. Use
`cli::host_configuration(std::env::args_os().skip(1))` to load the same validated
configuration, profile and chain reader. It inherits the provider's environment;
select a trusted absolute executable path. On Unix, process replacement keeps
signals and exit status with that application. The application supplies its
shutdown cancellation token and finite `ServePolicy` limits.

The `/etc/mppi/` and `/opt/provider/` paths above are deployment examples. Install
your completed configuration, verified profile and built host at your actual
private paths before running this command. Keep the wallet-operation record at
the durable path selected by your host. The CLI does not compile that host or
implement its inference callbacks.

`--upstream` changes this process's API base without modifying the file.
Listener startup does not establish public readiness. Providers own DNS,
certificates, ingress routing, token measurement and job termination. Embedded
applications can continue to register the native services in their own lifecycle.

## 5. Verify the public endpoint

```sh
mppi provider verify --testnet --config mppi.toml --profile arc-profile.json \
  --rpc-env ARC_RPC_URL_TESTNET
```

The command checks Circle operator access, the selected chain/deployment,
confirmed identity, public discovery, TLS and native gRPC. It keeps Control
request input open while checking responses, exchanges correlated challenges
and checks concurrent unpaid Complete rejection. `--ca` supplies an explicit
trust bundle; it replaces system roots without disabling hostname checks.

The result lists checked behavior and remaining limits. It does not fund a
session. Paid streaming, collection/refund and provider job behavior require
separate funded acceptance. A successful check describes the observed endpoint
at that time.

## 6. Apply explicit changes

An update file selects only intended changes. For example:

```json
{
  "models": {"upsert": [{"model": "example-org/example-model", "rates": {"output": "1500000"}}]},
  "upstream": {"url": "http://127.0.0.1:8080/custom/v1"}
}
```

```sh
mppi provider update --testnet --config mppi.toml --profile arc-profile.json \
  --changes changes.json --rpc-env ARC_RPC_URL_TESTNET
```

`models.upsert` accepts partial edits to existing model declarations; new models
need complete declarations. `models.remove` removes named models.
`replace_models` explicitly replaces the entire model list and cannot accompany
`models`. Rate edits merge with the latest confirmed catalog so stale unrelated
local prices are not republished. An omitted cache price differs from zero;
`"cache": null` explicitly removes it in a rate edit.

Other selectable sections are `provider`, `upstream`, `payment`, `settlement`
and `listen`. Identity, operator and network replacement are not implicit updates.
Recipient changes use the same separate proof exchange. A changed recipient
confirms before a changed catalog is published. Description, capabilities and
local execution/listener settings do not cause a price publication.

Rerun identical changes to check pending work. Conflicting concurrent changes
are reported. Completed configuration is saved as a complete replacement; your
application must load it through its own deployment lifecycle. Existing sessions
retain their recipient/operator, and admitted runs retain their accepted prices.

## Local records and results

The CLI keeps setup and wallet-operation sidecars beside the selected config.
For `--config mppi.toml`, these are `mppi.toml.wallet-setup.json`,
`mppi.toml.onboarding.json`, `mppi.toml.wallet-operations.json` and, after an
explicit update, `mppi.toml.update.json`, with their locking records.
Keep that directory private, durable and backed up. Cooperating local commands
use file locks. Providers coordinate independent hosts using the same operator.
The records contain exact setup/payment transaction intent and identifiers,
not credentials, inference history, usage storage or a provider recovery service.

`CircleDeveloperWallet` requires an explicit local operation-record path. It
binds each reference to the exact wallet, network, destination and calldata
before submission. Reconciliation is read-only. The adapter trusts Circle's
authenticated original reference/transaction-ID lookup to associate a chain hash
with that call; the SDK separately verifies canonical receipts and effects. Its bounded record accepts
16 MiB and 16,384 operation references; it refuses new operations at capacity
and never silently deletes original evidence. Retain old records for unresolved
and recoverable operations when managing that wallet's retained history.

Exit code `0` means the requested check/setup completed; `2` means pending work
or recipient proof is required; `1` means invalid input, conflicting state or
unavailable evidence. JSON output contains public result fields. A saved local
configuration does not assert that running provider replicas applied it.

Inspect live payment state through the runtime's `sessions().get/list` methods
and inspect chain results through the selected `ChainReader`; the CLI has no
separate session-inspection command. Follow the
[confirmation and shutdown guidance](rust-runtime.md#inspect-and-stop-the-runtime).
Stopping the host releases resources and reports unresolved work. Closing a
funded session is a separate explicit payment operation.

The commands and result fields above follow the packaged
[CLI parser and implementation](../sdks/provider/rust/src/cli.rs),
[configuration types](../sdks/provider/rust/src/config.rs) and
[owned listener](../sdks/provider/rust/src/standalone.rs).
