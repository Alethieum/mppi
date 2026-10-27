# Native provider CLI reference

The `mppi-provider` crate contains the native `mppi` executable used by Python
and Rust providers. Start with the [provider quickstart](provider-quickstart.md)
for installation, generated declarations and application composition. This
reference covers onboarding, host launch, verification and explicit updates.

Provider `0.2.0` packages official Arc mainnet and testnet profiles and selects
the profile from the resolved `chain.network`. `--mainnet` or `--testnet` overrides that
configuration selection; `init` requires one of these flags. An explicit
`--profile FILE` overrides packaged pins and must match the selected network.
Observed chain, code and proxy implementation must still match those pins.

`--rpc-env NAME` reads your trusted RPC URL from the named environment variable.
The testnet commands below use `ARC_RPC_URL_TESTNET`; set it through your secret
environment before running them. Without that flag, the configuration selects
the RPC, defaulting to `https://rpc.testnet.arc.io` on testnet and
`https://rpc.mainnet.arc.io` on mainnet. Use `ARC_RPC_URL_MAINNET` explicitly for a
mainnet environment override. See [Arc connection details](https://docs.arc.io/arc/references/connect-to-arc).
`--timeout-ms` bounds each chain read attempt. A read that fails for a transient
reason is repeated at most twice; no transaction is repeated. A failed read
names its cause after the message, for example
`provider registry identity is unavailable: chain RPC unavailable: JSON HTTP endpoint returned status 503`,
never the RPC URL or its credentials.

## Generate declarations

```sh
mppi provider init --testnet
```

This creates `provider-declarations.toml` and refuses to overwrite an existing
path. It makes no wallet or network call. Use `--declarations PATH` to choose
another output. Replace every `REPLACE` placeholder and review the illustrative
model, payment and freshness settings; add listener settings for standalone use.
The template intentionally has no confirmed identity. Onboarding writes that identity to `mppi.toml`.

## Configuration and wallet access

`--config` selects the applied configuration path; its default is `mppi.toml`.
Keep the configuration and its sidecars in a private durable directory.
`--timeout-ms` sets the bounded CLI request timeout; the default is 30,000 ms.

The operator is an existing Circle Agent Wallet selected by address. Install
`@circle-fin/cli@1.1.4`; the Circle adapter accepts only this verified CLI version
and refuses any other before anything is sent, and other wallets are not affected.
Complete `circle wallet login EMAIL --testnet`, or omit
`--testnet` for mainnet. Use `circle wallet list --type agent --chain ARC-TESTNET`
(`ARC` on mainnet) to select it. Keep the Circle CLI available on `PATH` to both
the native CLI and provider host. Circle's [sessions](https://developers.circle.com/agent-stack/agent-wallets/wallet-operations/authenticate)
expire after 28 days; renew access to the same wallet when needed.

The payout recipient is a different address whose signing authority stays outside
serving. Circle Agent Wallet gas is [sponsored subject to caps and fair use](https://developers.circle.com/agent-stack/agent-wallets/fees).
The upstream URL is its complete API base, including its path prefix; your
application supplies its authentication.

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
mppi provider onboard --testnet --config mppi.toml --wallet YOUR_OPERATOR_WALLET \
  --declarations provider-declarations.toml --rpc-env ARC_RPC_URL_TESTNET
```

`--wallet ADDRESS` selects an existing authenticated Circle Agent Wallet on the
selected network. Saved reruns may omit the address; a different selection is
rejected. The command checks registration, binds the separate recipient, prepares
discovery and publishes the initial catalog last. A provider identity is saved only after
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
completion, `mppi.toml` contains the confirmed provider ID and operator address
alongside the declarations. The result explicitly reports
`endpoint_verified: false` and `runtime_application_required: true`: registration
does not start your application or prove its public route.

The adapter checks the CLI version and the authenticated account's selection of
the wallet before an execute, unless a check that passed began less than 60
seconds earlier. For signing, which submits nothing, it relies on the last
passing check until any CLI command fails or prints output it refuses. It pins
no account code, so any Circle account generation works. Initial onboarding
sends the intended registration call directly, so no separate activation
transaction is added. See
[the adapter source](../sdks/provider/rust/src/circle.rs) and Circle's
[contract execution interface](https://developers.circle.com/agent-stack/agent-wallets/wallet-operations/execute-contract).

## Build and start your integration

Providers supply measured coverage, cumulative usage, final job outcomes and
available session history through the existing
[Rust integration interfaces](rust-runtime.md). Use `setup::runtime_from_config`
to compose `runtime::Mppi` from the saved configuration, selected wallet and those
integrations, or `setup::runtime_from_config_with_wallet` with
[another wallet](rust-runtime.md#compose-a-provider-host).

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
`--timeout-ms`, `--mainnet` / `--testnet` and `--upstream` arguments. Use
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

The CLI keeps onboarding and wallet-operation sidecars beside the selected config.
For `--config mppi.toml`, these are `mppi.toml.onboarding.json`,
`mppi.toml.wallet-operations.json` and, after an
explicit update, `mppi.toml.update.json`, with their locking records.
Keep that directory private, durable and backed up. Cooperating local commands
use file locks. A second onboarding or update command for the same
configuration is refused as already in use. A record another process holds is
waited for up to one second and then refused, except that the Circle adapter's
execute and reconcile wait for the wallet-operation journal up to the CLI read
timeout (`CircleOptions::read_timeout_ms`, 240 seconds by default); a journal
still held is reported as the Circle Agent Wallet being unavailable or
inconsistent. A serving provider shares that journal, but the Circle adapter
holds its lock only while it reads or replaces the journal, never while a
Circle CLI command runs. An execute that cannot take the lock in time sends
nothing, yet is `UNKNOWN`: the journal was not read to show it unsent. If the
adapter cannot record Circle's answer (the journal is held past that time,
cannot be read or saved, or already holds other evidence for it), the result is
`UNKNOWN` with Circle's transaction id and hash, and the adapter writes them
into the journal at its next journal step, unless the journal still holds other
evidence; a failed save keeps them in memory and does not fail that step. They live
only in that memory until then: if the process stops first, the journal row
holds no transaction id or hash, a reconcile of the reference is `UNKNOWN`
without a hash, and the runtime refuses to recover a checkpoint holding that
hash ([Rust runtime](rust-runtime.md)). Circle's documentation describes no
lookup of a transaction by the idempotency key the adapter sends,
`circle transaction list` prints a wallet's transactions without their
idempotency keys, and MPPI documents no procedure for writing a transaction id
and hash into a journal row: there is no documented remedy. Providers
coordinate independent hosts using the same operator.
The records contain exact setup/payment transaction intent and identifiers,
not credentials, inference history, usage storage or a provider recovery service.

`CircleAgentWallet` requires an explicit local operation-record path. It binds
each reference to the exact wallet, network, destination and calldata before
submission. Python and Rust use the same journal format. Reconciliation is
read-only and requires the original retained Circle transaction ID and matching
call fields. If submission loses its result before that ID is saved, the operation
remains `UNKNOWN`; similar history entries do not establish its identity. Keep
the original journal and resolve the original operation with Circle before any
replacement is considered. Such a result's `cause` names the adapter's error and,
when Circle CLI `1.1.4` reported one, its error code, for example
`Unavailable (TIMEOUT)` or `Authentication (AUTH_EXPIRED)`; Circle's message and
other output are never retained. A failure after the journal shows no record of
the reference and before the intent is saved (a call over 60 KiB or for another
network, capacity, CLI version, wallet selection or login, or the save itself)
never reached Circle: the result is `FAILED` without a hash and its cause begins
`NotSubmittedError`. A save that fails after it may have replaced the journal is
read back and its unsent intent removed first; if that fails too, the result is
`UNKNOWN`. Onboarding and update then clear a `FAILED` step, so a rerun prepares
a fresh intent. An execute checks the CLI version and wallet selection first
unless a check that passed began less than 60 seconds earlier; signing accepts
the last passing check whatever its age, and any failed command clears it. When
that check finds the login gone, the execute is such a failure:
`NotSubmittedError: Authentication (AUTH_REQUIRED)`. A login lost less than 60
seconds after a passing check began is found only by the CLI's refusal of the
execute itself, after the intent is saved: the result is `UNKNOWN` with no
transaction id, which can only be reconciled, so the sessions of a capture that
ends this way cannot be collected again. Circle CLI `1.1.4` reports
`AUTH_REQUIRED` when its saved login is missing or past its expiry, and
`AUTH_EXPIRED` when Circle's service refuses a login the CLI still holds.

The adapter permits 60 KiB calldata and a 16 MiB journal. Each record keeps a
32-byte digest of its call rather than the call, so the journal holds tens of
thousands of them. The runtime retires a session's records with the session:
`retire` marks each with the time from which it may be forgotten, kept in the
journal so a restart keeps it, and every save from then on forgets each marked
record that holds a transaction hash; one without a hash keeps its mark. A
reconcile of a marked record withdraws its mark, since a recovery from an older
checkpoint may present that reference again. A new operation that would leave
too little room for its record, and for the transaction id, hash and
retirement mark every record may still gain, is refused before Circle is asked; original evidence is retained. Earlier releases
refuse a journal this release has saved, so do not downgrade over it.
Read commands have a default 240-second deadline and reconciliation examines at
most 20 pages. Execute and sign commands drain to completion; a timeout kill does
not turn an uncertain submission into a safe retry. Trusted embedded hosts can
set `CircleOptions`; these are adapter bounds, not protocol limits.

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
