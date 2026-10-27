# Python onboarding and wallets

Start with the [agent quickstart](agent-quickstart.md) or
[provider quickstart](provider-quickstart.md) for built-in Circle setup. This page
documents custom wallet and application composition.

Python agent `0.1.6` and provider `0.2.0` share the Circle Agent Wallet adapter
from `mppi-protocol` `0.1.4`. Both built-in setup paths select the packaged official
profile for the requested network. Explicit integrations below supply their own
trusted profile. Python and Rust providers use the same native onboarding CLI.
The [versioned testnet release record](https://github.com/Alethieum/mppi/releases/tag/v0.1.0)
preserves the earlier wallet integrations and their exact acceptance scope.

MPPI handles payment messages and evidence. The provider supplies token counts,
execution, final reports, hosting, TLS and its own state management.

The [Python composition reference](python-quickstart.md) shows an exchange using
explicit capabilities.

## Install and select a deployment

Install your role into a Python 3.12+ environment:

```sh
uv pip install 'mppi-agent==0.1.6'
```

The provider environment installs `mppi-provider==0.2.0`. The shared protocol
dependency installs automatically. Neither role needs the other role's package.
For source development, see the [build guide](build.md).

Obtain a trusted deployment profile for the selected Arc network. MPPI ships no
temporary audit address as a default. The JSON profile has exactly these fields:

| Field | Value |
| --- | --- |
| `version` | Integer `1` |
| `chain_id` | `5042002` for Arc testnet; `5042` for Arc mainnet |
| `proxy` | MPPI `address` and `code_hash` |
| `implementation` | MPPI implementation `address` and `code_hash` |
| `registry` | ERC-8004 proxy `address` and `code_hash` |
| `registry_implementation` | ERC-8004 implementation `address` and `code_hash` |

Each pin is an object with exactly `address` (20-byte `0x` hex) and `code_hash`
(32-byte `0x` hex, Keccak-256 of runtime code). `load_profile(path)` checks this
shape. `ChainReader` checks chain ID, canonical code, proxy implementation slots
and the MPPI domain. Establish the profile's provenance separately; copying code
hashes from an untrusted RPC does not make a deployment trusted. The registry
binding targets the [ERC-8004 implementation at b9e466c](https://github.com/erc-8004/erc-8004-contracts/blob/b9e466c250744a7e06b13dff9d3c2844ed64f825/contracts/IdentityRegistryUpgradeable.sol).

## Agent setup

Use a funded Arc wallet implementing `FundingWallet`: `address`,
`sign_funding_authorization`, `execute` and read-only `reconcile`. An existing
compatible wallet requires no Circle account. For a new wallet, follow
[Circle's Agent Wallet quickstart](https://developers.circle.com/agent-stack/agent-wallets/quickstart).
Wallet login, custody and renewal stay with Circle and the operator.

The human completes wallet login and funding. Testnet setup with the reviewed
Circle CLI version is:

```sh
npm install -g @circle-fin/cli@1.1.4
circle wallet login you@example.com --testnet
circle wallet list --type agent --chain ARC-TESTNET --output json
circle wallet fund --address YOUR_WALLET --chain ARC-TESTNET
circle wallet balance --address YOUR_WALLET --chain ARC-TESTNET
```

MPPI's Circle adapter accepts only this verified CLI version and refuses any other before anything is sent; other wallets are not affected.

Provide a trusted local integration function `wallet(profile)` in an importable
module. It returns the selected wallet adapter, with secrets obtained from your
wallet service or secret environment. Configure `MPPI_RPC_URL` through that
environment too. Then run:

```sh
mppi-agent onboard --profile deployment.json --config agent.json \
  --wallet-factory my_wallet:wallet
```

For Arc testnet, add `--testnet --rpc-env ARC_RPC_URL_TESTNET` and supply the
trusted testnet deployment profile. `--testnet` rejects a profile whose chain ID
is not `5042002` before loading the wallet integration. The profile still selects
the pinned contract and registry; the explicit RPC is checked against that
deployment during onboarding. The flag supplies no audit contract address, RPC
credential or wallet funding.

The factory flag imports executable Python code; use your own trusted module.
`--rpc-env NAME` selects a different RPC environment variable. `--agent-id ID`
adopts an existing identity after checking its current owner and `agentWallet`.
Identity zero is valid. The RPC URL and credentials are never saved in
`agent.json`.

Onboarding checks a positive six-decimal USDC balance, verifies the selected
deployment, and registers through the funding wallet without a public URI.
It saves the canonical identity and retained operation in an exclusively locked,
mode-0600 local file. A completed rerun checks current ownership, wallet binding
and balance without registering again. Keep the same configuration file.

Exit status `0` means complete; `2` means the retained operation is unresolved;
`1` means setup could not complete. Standard output is a small JSON result.
Rerunning after a timeout queries the original operation. It never interprets a
missing response as permission to mint another identity. A canonically reverted
registration can be attempted again by a later explicit invocation.

The same operation is available to Python applications:

```python
from pathlib import Path
from mppi_agent import onboard
from mppi_protocol.registry import RegistryReader

result = onboard(
    wallet=wallet, registry=RegistryReader(chain_reader), config_path=Path("agent.json")
)
```

Configure the fixed ECDSA session signer separately. A Circle funding account
does not become the voucher signer automatically. Its identity and refund
address remain the funding wallet. The signer needs neither funding nor its own
ERC-8004 registration. Retain the signer credential in your secret service and
pass its recoverable reference to `Client`.

### Funding wallet adapters

Any wallet provider fits behind `FundingWallet` with a thin adapter; the SDK
pins no wallet implementation or version. The
[wallet requirements](sdk-interfaces.md#wallet-requirements) state everything
the agent and the providers assume of a wallet. An adapter provides:

| Member | Requirement |
| --- | --- |
| `address` | The funding wallet: the session's agent, its refund destination and the wallet the agent's ERC-8004 id binds |
| `execute(call, *, operation_ref)` | Sends the nonpayable `call` once from `address` with zero native value, after durably binding `operation_ref` to it. Raise `NotSubmittedError` only when its own record proves nothing reached the wallet service |
| `reconcile(*, operation_ref)` | Reports that same submission without sending anything |
| `sign_funding_authorization(authorization)` | Returns bytes USDC accepts for exactly that authorization from `address`: an ECDSA signature for an account without code, or bytes its ERC-1271 `isValidSignature` approves |
| `signs_at_execute` (optional) | `True` when the wallet service creates that signature as it executes: `sign_funding_authorization` returns a placeholder, and `execute` replaces it with the real signature before sending |

Funding requires the agent id: pass `agent_id` to `Client`. Before submitting
an opening, the agent checks at one block that the registry's
`getAgentWallet(agent_id)` is `address` and that the signature passes the same
check USDC applies. The binding, and whether `address` has code, come with the
chain snapshot the opening reads anyway; the signature of an account without
code is then recovered by ECDSA with no read, and one with code is asked
through `isValidSignature`, in one read. Otherwise it raises `FundingAuthorizationError`
with `reason` `AGENT_ID_REQUIRED`, `AGENT_WALLET_MISMATCH` or
`INVALID_SIGNATURE`, and nothing is submitted; an all-zero placeholder from a
wallet that has not declared `signs_at_execute` is
`SIGNATURE_DEFERRED_UNDECLARED`. For a wallet that declares it, the agent
checks only the binding and never simulates the placeholder; USDC verifies the
real signature on chain. A top-up relies on what its session's opening proved,
with USDC still checking its signature. A smart
account must have code when it funds; registering through it deploys it. Exits
are never gated this way.

### Circle Agent Wallet adapter

`mppi_protocol.circle.CircleAgentWallet` uses the unmodified Circle CLI `1.1.4`.
It accepts a `ChainReader`, the funding wallet's address and its own local
operation journal:

```python
from pathlib import Path
from mppi_protocol.circle import CircleAgentWallet

wallet = CircleAgentWallet(
    chain=chain_reader,
    address=funding_wallet_address,
    checkpoint_path=Path("circle-operations.json"),
)
wallet.verify_wallet()
```

The adapter checks the CLI version and that the CLI selects `address` as an
agent wallet on the selected network. An execute runs that check first unless
one that passed began less than 60 seconds earlier; a signature and
`verify_wallet` accept the last passing check whatever its age. Any CLI command
that fails, or prints output the adapter refuses, clears it, so the next
operation checks again. When an execute's check finds the login gone, it raises
`CircleNotSubmittedError` before saving its intent: nothing was sent. A login
lost less than 60 seconds after a passing check began is found only by the
CLI's own refusal of the execute, before it sends anything but after the
adapter saved the intent: the result is `UNKNOWN` with no transaction id and
cause
`CircleAgentWalletError: Circle command did not return success (AUTH_REQUIRED)`.
It can only ever be reconciled, so the sessions of a provider's capture that
ends this way cannot be collected again. A signature with a lost login raises.
Circle CLI `1.1.4` reports `AUTH_REQUIRED` when its saved login is missing or
past its expiry, and `AUTH_EXPIRED` when Circle's service refuses a login the
CLI still holds. Restart after installing another CLI version.
The adapter pins no account code, so any Circle account generation that supports
the self-execution call works. Circle documents deployment on the
[first outbound transaction](https://developers.circle.com/wallets/account-types#smart-contract-accounts-sca-and-msca);
receiving funds alone does not establish deployment. Circle wallet upgrades are
started only by the wallet holder ([upgrades](https://developers.circle.com/wallets/wallet-upgrades));
an upgraded account keeps working while USDC accepts its signatures.

The adapter passes the exact target and calldata through the account's
`execute(address,uint256,bytes)` function with zero native value. It saves a
stable key and its call's digest before invoking the CLI. The journal's file
lock is held only while the journal is read or replaced, never while a CLI
command runs, so another process sharing the journal, such as the native
`mppi provider update` command, goes on meanwhile; each save keeps that
process's records. An execute or reconcile waits for that lock up to
`timeout_seconds` (240 by default); other operations wait a second. Within one
process the adapter runs one operation at a time, CLI commands included, so an
operation may also wait behind another's command. An execute that cannot save
its intent in that time raises before Circle is asked, and `submit_operation`
records it `UNKNOWN`: nothing was sent, but the journal was not read to show
it. If the adapter cannot record Circle's answer (the journal is held past that
time, cannot be read or saved, or already holds other evidence for it), the
result is `UNKNOWN` with Circle's transaction id and hash, and the adapter
writes them into the journal at its next journal step, unless the journal still
holds other evidence; a failed save keeps them in memory and does not fail that
step. They live only in that memory until then: if the process stops first,
the journal row holds no transaction id or hash, a reconcile of the reference
is `UNKNOWN` without a hash, and a provider runtime refuses to recover a
checkpoint holding that hash ([Python runtime](python-runtime.md)). Circle's
documentation describes no lookup of a transaction by the idempotency key the
adapter sends, `circle transaction list` prints a wallet's transactions without
their idempotency keys, and MPPI documents no procedure for writing a
transaction id and hash into a journal row: there is no documented remedy.
Funding signatures remain the original Circle signature bytes; MPPI's fixed
ECDSA signer stays separate.
The earlier agent integration passed the versioned live Arc testnet registration,
funding and payment checks with a deployed Circle SCA.

CLI reconciliation uses only the original retained transaction ID and matching
call fields. If the execute response is lost before an ID is saved, the public
CLI exposes no idempotency-key lookup: the result remains `UNKNOWN`, even when a
similar transaction appears in recent history. Do not delete the journal to
retry. Resolve the original operation with the wallet service first.

A failed Circle command names only Circle CLI `1.1.4`'s error code, never its
message or other output: `Circle command did not return success (AUTH_EXPIRED)`.
When `execute` or `reconcile` leaves an operation `UNKNOWN`, the result's `cause`
carries that reason, for example
`CircleAgentWalletError: Circle command did not return success (TIMEOUT)`.

Each record keeps a 32-byte digest of its call rather than the call, so the
16 MiB journal, its only bound, holds tens of thousands of them. A record from
an earlier release, which kept the whole call, still loads and is written in
the compact form on the next save; earlier releases refuse that form, so do not
downgrade over it. Format 2 no longer retains account code pins, and a format 1
journal for the same wallet and deployment still loads. A new operation that
would leave too little room for its record, and for the transaction id, hash
and retirement mark every record may still gain, is refused before Circle is
asked. `near_capacity` is true from three quarters of the bound.
`retire(operation_refs=..., at=...)` marks each named record with `at`, Unix
time in seconds as from `time.time()`, keeping an earlier mark; it skips a
reference without a record and one no record can have. The mark is saved in
the journal, so a restart keeps it, and every save from `at` on forgets each
marked record that holds a transaction hash. A record without one keeps its
mark, since its call may still reach Circle. A `reconcile` of a marked record
withdraws its mark, since a recovery from an older checkpoint may present that
reference again. The provider runtime passes the references of the sessions
it retired, which may include one without a transaction hash. The agent passes
those of a session it retired, once its checkpoint no longer names them, and
never one without a transaction hash. Pass only references that are never used
again once `at` has passed. Calls are limited to 60 KiB of
calldata to fit the reviewed CLI transport; a larger call, or one for another
network, is refused before its intent is saved, as `CircleNotSubmittedError`.
A save that fails after it may have replaced the journal is read back and its
unsent intent removed; only then is the failure reported as not submitted.
Each command has bounded output. Read-only commands have a default
240-second timeout, and reconciliation examines at most 20 pages. Execute and
sign commands drain to completion without a timeout kill. These are adapter limits,
not new protocol limits. The `command`, `timeout_seconds` and `max_pages`
constructor parameters configure its trusted executable and bounded processing.

## Provider wallet integration

The built-in `await Mppi.from_config(...)` path loads the confirmed operator from
`mppi.toml`, verifies its selected deployment and account, and restores the native
onboarding journal at `mppi.toml.wallet-operations.json`. It uses the authenticated
Circle CLI. Start with the [provider quickstart](provider-quickstart.md).

Explicit provider integrations use the same `mppi_protocol.circle.CircleAgentWallet`
constructor shown above, with the provider operator's address and journal. Set
`require_existing=True` when restoring an onboarded wallet. Its operator must
match the configuration and differ from the receive-only payout recipient.
Recipient proof signing stays outside the serving process. Separate wallet
addresses under one Circle account still share account custody. Another
`TransactionWallet` composes through `Mppi.from_config(..., wallet=...)`; see
[provider composition](python-runtime.md#provider-composition).

Circle owns login and its 28-day session renewal. Agent Wallet transaction gas
is sponsored subject to caps and fair use; see Circle's
[authentication](https://developers.circle.com/agent-stack/agent-wallets/wallet-operations/authenticate)
and [fees](https://developers.circle.com/agent-stack/agent-wallets/fees).

`execute` sends exact calldata through the supported SCA using the official
[contract execution command](https://developers.circle.com/agent-stack/agent-wallets/wallet-operations/execute-contract).
The shared journal binds each reference to its call's digest before submission
and uses only the original returned transaction ID for read-only
reconciliation. Circle service
state alone does not confirm MPPI payment; the runtime independently verifies
canonical contract events and state. An unknown outcome remains attached to its
original operation. The adapter provides no provider storage service or automatic
transaction retry.

## Provider registration and publication

The native `mppi provider onboard` command performs this sequence for both
Python and Rust providers. The following helpers are the lower-level interface
for custom onboarding integrations.

`RegistryCalls` prepares exact calls; `RegistryReader` verifies deployment,
identity, recipient proofs and canonical outcomes. These shared helpers are
available when only the provider package is installed.

```python
from mppi_protocol.registry import RegistryCalls, RegistryReader

registry = RegistryReader(chain_reader)
calls = RegistryCalls(chain_reader.profile.domain.chain_id, chain_reader.profile.registry_address)
registration = calls.register(wallet.address, agent_uri="https://provider.example/discover")
registry.simulate(registration)  # Exact caller and calldata; no transaction.
```

Complete the following steps through the operator wallet, checking confirmed
state before each missing write:

1. Register the intended discovery URI **without a catalog**. Retain the exact
   operation and stable reference before `wallet.execute`. Derive the identity
   from `registry.reconcile(...)`, never from a predicted next ID.
2. Read the identity and require the configured operator to own it. Generate
   `registry.wallet_proof(id, recipient, owner=wallet.address, deadline=...)`
   when the recipient is ready to sign. The selected registry permits a deadline
   at most five minutes ahead. Export this exact typed message to the recipient.
3. Import the recipient's signature, compare every typed-data field with the
   intended context, prepare `calls.set_agent_wallet(...)`, and simulate before
   submission. Confirm the recipient from canonical evidence. Its private key
   never enters the serving process.
4. Confirm the intended URI and registration document. Use `calls.set_uri(...)`
   only when the initial URI step remains incomplete.
5. Publish the canonical catalog **last**, using `calls.set_catalog(...)`, and
   confirm the exact bytes. An empty catalog keeps the offering inactive while
   ERC-8004's initial wallet still points to the operator.

A completed rerun verifies existing owner, recipient, URI and catalog. It needs
no new write and no new recipient signature. A conflicting completed setup is
an error; deliberate later changes are an explicit update. An expired imported
proof requires a fresh signature only if its binding has not completed.

For an operation with a returned transaction hash, use:

```python
verified = registry.reconcile(
    registration,
    operation_ref=retained_reference,
    tx_hash=submitted.tx_hash,
    wallet_operation_id=submitted.wallet_operation_id,
)
```

Only `verified.operation.state == OperationState.CONFIRMED` establishes the
matching registry effect; `verified.identity` then supplies its observed state.
An outer smart-account transaction success alone is insufficient. Pending and
unknown outcomes preserve their original identifiers.

The provider exposes the generated registration document and the SDK's native
HTTP/2 gRPC services through its existing TLS origin. Listener, DNS, certificates,
execution and persistence remain provider integration work. See the
[runtime reference](python-runtime.md) for service composition and payment flows.

## Local testing

`from mppi_agent.testing import MockProvider` supplies an asynchronous loopback
wire peer. Construct it with `completions=` and `control=` async-generator
handlers, then use `async with MockProvider(...) as peer`. Within that context,
`peer.chat` and `peer.control` are the generated gRPC stubs. Give each call an
explicit timeout and use a new peer instance for each context.

The application supplies the exact protobuf messages and errors. This helper
does not open escrow, verify signatures, count tokens or manufacture final
usage. It has no wallet, chain or TLS and cannot bind a public interface.
See the [complete executable examples](../sdks/agent/python/tests/test_testing.py)
and [helper API](../sdks/agent/python/src/mppi_agent/testing.py).

## Onboarding and first payment

```mermaid
sequenceDiagram
    participant H as Operator
    participant A as Agent SDK
    participant W as Funding wallet
    participant R as ERC-8004
    participant P as Provider SDK
    participant C as MPPI contract
    H->>W: Create/login and fund through chosen wallet service
    H->>A: onboard with wallet and trusted deployment
    A->>R: Check deployment, balance and saved identity
    A->>W: Register once with retained operation reference
    W->>R: register()
    A->>R: Confirm exact receipt, owner and agentWallet
    A->>A: Save identity; configure fixed signer separately
    A->>P: Discover and accept catalog; request Challenge
    A->>W: Authorize explicit positive funding and descriptor
    W->>C: open with fixed sessionSigner
    A->>C: Verify canonical opening
    A->>P: Authenticate and authorize provider-required tokens
    P->>C: Capture provider-reported actual usage
```
