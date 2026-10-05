# Python onboarding and wallets

The [agent quickstart](agent-quickstart.md) covers built-in Circle setup and its
release status. This page documents custom wallet and application composition.

The Python packages include agent onboarding, ERC-8004 registration helpers and
wallet adapters. Live Arc testnet checks have verified Circle Agent Wallet
CLI 1.1.4 and Circle developer-controlled SCA integration with separately
installed agent and provider packages. These packages are not yet a production
release; an official deployment profile must be supplied separately. The full
provider command line belongs to the Rust package.

MPPI handles payment messages and evidence. The provider supplies token counts,
execution, final reports, hosting, TLS and its own state management.

For the complete path from independent package installation to one paid
exchange, start with the [Python walkthrough](python-quickstart.md).

## Install and select a deployment

From this exported public source directory, install the shared package and the
role you will use into your Python 3.12+ environment:

```sh
uv pip install ./proto/python ./sdks/agent/python
```

The provider environment installs `./proto/python ./sdks/provider/python`.
Neither role needs the other role's package.

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

### Circle Agent Wallet adapter

`mppi_agent.circle.CircleAgentWallet` uses the unmodified Circle CLI `1.1.4`.
It accepts a `ChainReader`, an explicitly trusted `CodePin` for the funding SCA,
a second pin for its implementation, and its own local operation journal:

```python
from pathlib import Path
from mppi_agent.circle import CircleAgentWallet

wallet = CircleAgentWallet(
    chain=chain_reader,
    wallet=funding_sca_pin,
    implementation=sca_implementation_pin,
    checkpoint_path=Path("circle-operations.json"),
)
wallet.verify_wallet()
```

Use a deployed SCA whose implementation supports the self-execution call. The
adapter verifies its runtime code, implementation slot and CLI account selection
before signing or submission. Circle documents deployment on the
[first outbound transaction](https://developers.circle.com/wallets/account-types#smart-contract-accounts-sca-and-msca);
receiving funds alone does not establish deployment. Wallet activation and
provenance of the selected account pins remain wallet setup prerequisites.

The adapter passes the exact target and calldata through the account's
`execute(address,uint256,bytes)` function with zero native value. It retains a
stable key and exact intent before invoking the CLI. Funding signatures remain
the original Circle signature bytes; MPPI's fixed ECDSA signer stays separate.
This route has passed live Arc testnet registration, funding and payment checks
through the real Circle service with a deployed, pinned SCA.

CLI reconciliation uses only the original retained transaction ID and matching
call fields. If the execute response is lost before an ID is saved, the public
CLI exposes no idempotency-key lookup: the result remains `UNKNOWN`, even when a
similar transaction appears in recent history. Do not delete the journal to
retry. Resolve the original operation with the wallet service first.

The journal permits 4,096 retained operations; exceeding that bound stops new
submissions. Calls are limited to 60 KiB of calldata to fit the reviewed CLI
transport. Each command has bounded output and a default 240-second timeout;
read-only reconciliation examines at most 20 pages. These are adapter limits,
not new protocol limits. The `command`, `timeout_seconds` and `max_pages`
constructor parameters configure its trusted executable and bounded processing.

## Provider wallet integration

The provider operator uses a Circle **developer-controlled SCA**, with a separate
receive-only payout recipient. Circle Agent Wallet login is a different product.
Follow the [developer-wallet setup](https://developers.circle.com/wallets/dev-controlled/create-your-first-wallet)
with `accountType: "SCA"`, retain its wallet ID/address, and configure Circle Gas
Station for the selected network. Supply the API key and registered entity
secret through environment or secret storage.

```python
import os
from mppi_protocol.circle import CircleDeveloperWallet
from mppi_protocol.discovery import parse_address

wallet = CircleDeveloperWallet(
    api_key=os.environ["CIRCLE_API_KEY"],
    entity_secret=bytes.fromhex(os.environ["CIRCLE_ENTITY_SECRET"]),
    wallet_id=os.environ["CIRCLE_WALLET_ID"],
    address=parse_address(os.environ["CIRCLE_WALLET_ADDRESS"]),
    chain_id=5042002,
)
wallet.verify_wallet()  # Read-only SCA, network, custody and address checks.
```

Pass this wallet to `Mppi`. It sends exact raw calldata through Circle's
[contract-execution API](https://developers.circle.com/api-reference/wallets/developer-controlled-wallets/create-developer-transaction-contract-execution),
using a stable idempotency key and reference derived from the retained operation
reference. Each authorized request uses fresh entity-secret ciphertext.
Credentials are excluded from its representation and error messages.

Service acceptance is pending evidence, including Circle `COMPLETE`. The MPPI
runtime independently reconciles the intended contract events and state.
`reconcile(operation_ref=...)` only reads Circle records. Preserve operation
references and returned handles through your existing integration. After an
unknown outcome, reconcile; do not create a replacement operation. The adapter
provides no storage backend, wallet provisioning or automatic transaction retry.

## Provider registration and publication

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
