# Native provider CLI reference

The `mppi-provider` crate contains the native `mppi` executable used by Python
and Rust providers. Start with the [provider quickstart](provider-quickstart.md)
for installation, generated declarations and application composition. This
reference covers onboarding, host launch, verification and explicit updates.

Provider `0.1.1` packages the verified official Arc testnet profile. Use
`--testnet` to select it; the observed chain, code and proxy implementation must
still match its pins. `--rpc-env NAME` reads your trusted RPC URL from the named
environment variable. The commands below use `ARC_RPC_URL_TESTNET`; set it through
your secret environment before running them. Without that flag, the configuration
selects the RPC, defaulting to `https://rpc.testnet.arc.io` on testnet.
`--profile FILE` accepts an independently trusted explicit profile.
`--testnet` selects the testnet configuration override. The resulting network
and selected profile must agree.
[Arc connection details](https://docs.arc.io/arc/references/connect-to-arc).

## Generate declarations

```sh
mppi provider init --testnet
```

This creates `provider-declarations.toml` and refuses to overwrite an existing
path. It makes no wallet or network call. Use `--declarations PATH` to choose
another output. Replace every `REPLACE` placeholder and review the illustrative
model, payment and freshness settings; add listener settings for standalone use.
The template intentionally has no confirmed identity. Onboarding writes that identity to `mppi.toml`.

## Configuration and credentials

`--config` selects the applied configuration path; its default is `mppi.toml`.
Keep the configuration and its sidecars in a private durable directory.
`--timeout-ms` sets the bounded CLI request timeout; the default is 30,000 ms.

The operator is a Circle developer-controlled SCA. Configure `CIRCLE_API_KEY`
and the registered `CIRCLE_ENTITY_SECRET` through your secret environment.
Configure Circle Gas Station for that wallet/network. The receive-only payout
recipient is a different address, with signing custody separate from serving.
Secret values are never command-line options. The upstream URL is its complete
API base, including its path prefix; your application supplies its authentication.

For a Rust owned listener, add explicit listener settings before onboarding:

```toml
[listen]
bind = "0.0.0.0:8443"
tls_mode = "direct"
tls_cert = "tls/fullchain.pem"
tls_key = "tls/key.pem"
```

Certificate chain and key paths are relative to the declarations file. Your
provider hostname must match the certificate and public route. With
`tls_mode = "ingress"`, use a private bind address and omit TLS file fields; your ingress terminates HTTPS and carries
native gRPC over HTTP/2. Python applications configure their own listeners.

## Register and retain the result

```sh
mppi provider onboard --testnet --config mppi.toml \
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
mppi provider onboard --testnet --config mppi.toml \
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

## Build and start your integration

Providers supply measured coverage, cumulative usage, final job outcomes and
available session history through the existing
[Rust integration interfaces](rust-runtime.md). Use `setup::runtime_from_config`
to compose `runtime::Mppi` from the saved configuration, selected wallet and those
integrations.

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
  --testnet --rpc-env ARC_RPC_URL_TESTNET \
  --host /opt/provider/bin/provider-mppi-host
```

The host receives `--config` and any explicit `--profile`, `--rpc-env`,
`--timeout-ms`, `--testnet` and `--upstream` arguments. Use
`cli::host_configuration(std::env::args_os().skip(1))` to load the same validated
configuration, profile and chain reader. It inherits the provider's environment;
select a trusted absolute executable path. On Unix, process replacement keeps
signals and exit status with that application. The application supplies its
shutdown cancellation token and finite `ServePolicy` limits.

The `/etc/mppi/` and `/opt/provider/` paths above are deployment examples. Install
your completed configuration and built host at your actual private paths before
running this command. If you select `--profile`, also install that verified file.
Keep the wallet-operation record at the durable path selected by your host. The
CLI does not compile that host or implement its inference callbacks.

`--upstream` changes this process's API base without modifying the file.
Listener startup does not establish public readiness. Providers own DNS,
certificates, ingress routing, token measurement and job termination. Embedded
applications can continue to register the native services in their own lifecycle.

## Verify the public endpoint

```sh
mppi provider verify --testnet --config mppi.toml \
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

## Apply explicit changes

An update file selects only intended changes. For example:

```json
{
  "models": {"upsert": [{"model": "example-org/example-model", "rates": {"output": "1500000"}}]},
  "upstream": {"url": "http://127.0.0.1:8080/custom/v1"}
}
```

```sh
mppi provider update --testnet --config mppi.toml \
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
