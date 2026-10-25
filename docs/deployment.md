# MPPI Arc deployment

## Arc mainnet

The MPPI contracts are deployed on Arc mainnet, chain ID **5042**. SDK `0.1.2`
packages this deployment profile for explicit mainnet selection.

| Contract | Deployed address |
| --- | --- |
| MPPI proxy | `0xbDF7c950390316B6091b729F271371984B37deD7` |
| MPPI implementation | `0x6dE8b8cd1200ffD27C460929c8BF8437E129243b` |
| Governance timelock | `0x228966CAA1f2b14b6f8d0a86baAB8493EC419D06` |

The [mainnet profile](../deployments/arc-mainnet.json) has SHA-256
`94a9ea5c146d8a0fd51818a7066df613f1629d1e679b48f32f63b9ac99e8693c`.
Configuration and independent Python/Rust SDK reads passed at block `0x1760f10`,
hash `0x7342b32cef7346ad8237c833014906793424925cf43704c81b1a5d3cf8bbf38e`.
These checks verified the original successful deployment transactions, exact
runtime bytecode, initialization, custody, signing domains and the
**86,400-second** governance delay. These deployment checks do not by themselves
establish an application's onboarding, endpoint or paid exchange.

| Deployment | Transaction hash |
| --- | --- |
| Timelock | `0x2df6501a0a4f17d7a663298f4debd11db79f66cb8674dd28c013a558b4dd62b4` |
| Implementation | `0xe33c98d35d881ce98ca8beb62caef2ac80d4c4a87eb5fd0b38763b4480de9be2` |
| Proxy | `0x3c7bf2c8b0023178a985ecda73965a618b2f0b9b9803d7994786c9d835258486` |

## Arc testnet

The official MPPI testnet deployment uses Arc testnet, chain ID **5042002**. It was
published with contract release `0.1.0`; SDK onboarding updates use the same
verified deployment:

| Contract | Deployed address |
| --- | --- |
| MPPI proxy | `0x1C687e8df34F6B35b162AB42733D98478eDedFED` |
| MPPI implementation | `0xD5C3aD686Ea1550204411C0eE3da9Dd72261eDf8` |
| Governance timelock | `0xF20fd0D61655De46c5B5965b1B29C80A532f5233` |

Creation receipts: [proxy](https://explorer.testnet.arc.io/tx/0x560d1359f07a6993d4d8133c9fe0ba8e45fb8f4f60be1595a25d2886b2c04e6e),
[implementation](https://explorer.testnet.arc.io/tx/0xff1b42ad104cca4b3d5074afc94616f3975b894b29d6514c18bdf5f8ba9173b6),
[timelock](https://explorer.testnet.arc.io/tx/0xd387f8bcd52f34fb1f7195c4da9536c8f1e05c7a8d40c0d701c050f9a9115f6f).
Deployed configuration and Python/Rust SDK profile checks passed at block `0x3e41629`, hash
`0x6537d97b8261210f225ac096c09abc97a00c98017689a995f200a455f1fc685c`.
The scoped configuration/profile security review passed. An end-to-end
acceptance run using frozen release artifacts on this deployment passed fresh
Rust provider and Python agent onboarding, each in its own isolated environment,
followed by two real inference completions, capture while the session was open,
a top-up and cooperative close. Independent chain verification confirmed exact payment and
refund accounting with zero remaining liability. See the
[v0.1.0 release record](https://github.com/Alethieum/mppi/releases/tag/v0.1.0) for
the exact source revision, artifact digests, published-install verification status and
security review scope and results.

Providers and agents connect to the MPPI proxy. The protocol team's
[DeployMPPI.s.sol](../contracts/script/DeployMPPI.s.sol) preparation procedure is
documented below; providers and agents do not deploy these contracts.

A future public testnet provider builds and hosts its own service using the
provider SDK and this deployment profile. Agents obtain the service URL or
ERC-8004 ID from that provider and use the agent SDK with the official profile.
Provider endpoint availability is independent of the MPPI SDK release.

## Trusted profile file

Agent `0.1.4` and provider `0.1.2` built-in setup package the verified official
profiles for both Arc networks. `--testnet` and `--mainnet` select the respective
profile without a separate download. Version `0.1.1` packages testnet only. The
provider composition helpers use those same pins; they still verify chain ID,
runtime code, proxy implementation slots and the signing domain before use.
The packaged profile's trust comes from the installed official artifact,
independently of provider discovery and RPC responses.

Explicit integrations and older `0.1.0` CLI installations can use a separate
trusted file. Download the
[v0.1.0 Arc testnet profile](https://github.com/Alethieum/mppi/releases/download/v0.1.0/arc-testnet.json)
from the official release and save it as `arc-profile.json`. Its SHA-256 is:

```text
6993c7525b99bf1dd4b4efd007f118d0f758b2f0aee2a2ca0dfe58ab9c94c458
```

Check the downloaded bytes before use:

```sh
shasum -a 256 arc-profile.json
```

The digest must match exactly. Establish trust in the release record itself;
a matching digest supplied by a provider does not establish that trust. The same
profile is included at [deployments/arc-testnet.json](../deployments/arc-testnet.json)
in the release source. `load_profile(path)` and the CLI's `--profile` argument
select your verified local copy explicitly. No profile is downloaded at runtime.
An address supplied by a provider, successful RPC call or matching chain ID does
not establish the profile's provenance.

The Python and Rust parsers accept exactly this JSON shape. The bracketed strings
below describe values to obtain from that record; this template is deliberately
not a usable deployment profile:

```json
{
  "version": 1,
  "chain_id": 5042002,
  "proxy": {
    "address": "<MPPI proxy: 0x plus 40 hex digits>",
    "code_hash": "<runtime code keccak256: 0x plus 64 hex digits>"
  },
  "implementation": {
    "address": "<MPPI implementation: 0x plus 40 hex digits>",
    "code_hash": "<runtime code keccak256: 0x plus 64 hex digits>"
  },
  "registry": {
    "address": "<ERC-8004 registry proxy: 0x plus 40 hex digits>",
    "code_hash": "<runtime code keccak256: 0x plus 64 hex digits>"
  },
  "registry_implementation": {
    "address": "<ERC-8004 registry implementation: 0x plus 40 hex digits>",
    "code_hash": "<runtime code keccak256: 0x plus 64 hex digits>"
  }
}
```

Addresses and hashes must be nonzero. Code hashes cover deployed runtime bytecode,
not creation bytecode or a source-file digest. `proxy.address` is MPPI's signing
domain verifying contract. The profile contains public pins, not RPC credentials,
wallet secrets or provider configuration. Keep release revision, package digests,
deployment receipts, observed blocks and governance/custody verification in the
accompanying release record; the strict profile parser rejects extra fields.

`load_profile(path)` parses the supplied file. The chain reader subsequently
checks observed code, implementation slots and domain against those pins. These
are distinct checks: parsing does not establish trust, and reading a chain does
not choose trusted pins for the caller. For built-in setup, `--testnet` selects
the packaged official profile when no override is given, and requires the
testnet chain ID when an explicit profile is supplied.

Source contracts: [Python profile parser](../proto/python/src/mppi_protocol/deployment.py),
[Python chain checks](../proto/python/src/mppi_protocol/chain.py),
[Rust profile parser](../sdks/provider/rust/src/deployment.rs) and
[Rust chain checks](../sdks/provider/rust/src/chain.rs).

## SDK installation inputs

The provider quickstart uses `mppi-provider==0.1.2` from PyPI and native
`mppi-provider` version `0.1.2`. The agent quickstart uses `mppi-agent==0.1.4`.
The agent depends on `mppi-protocol==0.1.3` and the provider on
`mppi-protocol==0.1.1`. Use the release record to
identify the reviewed source revision and exact artifact digests. For development, the
[public source tree can be built independently](build.md).

| Input | How the application uses it |
| --- | --- |
| Agent Python wheel and shared protocol wheel | Installed in the agent environment with their declared dependencies |
| Provider Python wheel and shared protocol wheel | Installed in the Python provider environment with their declared dependencies |
| Rust provider crate and `mppi` binary | The provider's Rust integration and optional native onboarding/launch CLI |
| Trusted profile and independently selected RPC | Both roles verify the chosen deployment before payment |
| Provider configuration, endpoint/TLS and gateway integration | The provider publishes discovery and supplies execution, counts and final reports |
| Wallet access, identity and operation checkpoints | Each role controls its own authority and retains original operations for reconciliation |

Record source revision, package versions and package SHA-256 digests before
distribution. A filename or matching version is not an artifact identity.
Private test deployments and their profiles are not official release inputs.
See the [provider quickstart](provider-quickstart.md),
[agent quickstart](agent-quickstart.md) and [build instructions](build.md) for
exact entry points. Release verification and security results belong to the exact artifacts identified in the release
record.

## Verified inputs before any broadcast

The script accepts public release parameters through these environment variables. It reads no
private key and writes no credentials; the release operator supplies the wallet
to Foundry separately when an actual deployment is authorized.

| Variable | Required release evidence |
| --- | --- |
| `MPPI_REGISTRY` | Verified canonical ERC-8004 identity registry on the selected Arc network, implementation/proxy identity and observed block, matching the trusted deployment profile. |
| `MPPI_GUARDIAN` | Intended nonzero guardian with reviewed key custody; the protocol permits an EOA. |
| `MPPI_SAFE` | Verified, initialized governance Safe, including singleton/proxy identity, effective authorization configuration and control of its selected owner. |
| `MPPI_SAFE_OWNER` | The intended owner of this script's selected 1-of-1 Safe. |
| `MPPI_DEPLOYER` | Nonzero broadcaster address, distinct from `MPPI_SAFE_OWNER`; the selected wallet must control this address. This is public configuration, not a private key. |
| `MPPI_RELEASE_SALT` | Explicit 32-byte release salt, recorded with the exact build and initializer parameters before simulation and broadcast. |

Record chain identity, source revision, compiler/settings, dependency hashes,
contract identities, Safe owner/threshold, custody review and observed block in
the release record. Code presence and successful `getOwners()`/`getThreshold()`
calls do not establish Safe identity or custody. Inspect the Safe's effective
configuration, including modules and guards, before approving it. Do not place
keys, credentials or private custody information in the public manifest.
[Safe owner query](https://docs.safe.global/reference-smart-account/owners/getOwners),
[Safe threshold query](https://docs.safe.global/reference-smart-account/owners/getThreshold)

The script's `run()` entry point accepts only Arc testnet **5042002** and its
`runMainnet()` entry point accepts only Arc mainnet **5042**. The network is
never selected by guessing from the connected RPC. A matching chain ID alone
does not authenticate an RPC server. Reverify the endpoint and chain before
release. [Arc connection details](https://docs.arc.io/arc/references/connect-to-arc)

## Creation and initialization

With Foundry **1.8.3**, Solidity **0.8.37** and the checked-in OpenZeppelin **5.7.0**
dependencies, the script checks nonzero addresses, registry/Safe code and the
selected Safe's single expected owner and threshold of one before preparing.
It rejects the Safe owner as deployer and calls `vm.startBroadcast(deployer)`
with that checked address, binding all three factory transactions to the separate deployment
wallet. Foundry's selected wallet must supply the corresponding signer. These
distinct role addresses can share a controlling owner; that arrangement does
not provide independent signing custody.
[Pinned broadcaster API](../contracts/lib/forge-std/src/Vm.sol)

It prepares:

1. `TimelockController(60, [safe], [address(0)], address(0))`.
2. The MPPI implementation, whose constructor disables initialization.
3. `ERC1967Proxy(implementation, abi.encodeCall(initialize,
   (registry, guardian, safe, timelock)))`.

Proxy initialization happens in its creation transaction. The three creations
are separate transactions; a later deployment failure does not erase an already
mined implementation or timelock. Retain receipts and reconcile any partial
deployment before retrying. The proxy starts at `PauseLevel.None`, so it can
accept eligible funding as soon as creation succeeds.

The Safe receives proposer/canceller roles, the timelock administers itself,
and execution is open after the delay. Creation grants the broadcaster and factory
no implicit MPPI authority and no timelock admin role. The guardian can pause payments; the Safe can clear
that level. Only the timelock can control the all-operations pause or authorize
UUPS upgrades. [Authority profile](contract-interface.md#initialization-and-authority-definition),
[pinned timelock constructor](../contracts/lib/openzeppelin-contracts/contracts/governance/TimelockController.sol),
[pinned proxy constructor](../contracts/lib/openzeppelin-contracts/contracts/proxy/ERC1967/ERC1967Proxy.sol)

## Deterministic factory and addresses

Arc documents the Arachnid CREATE2 factory at
`0x4e59b44847b379578588920cA78FbF26c0B4956C` on both networks. Before recording any
transaction, this script requires its runtime code hash to equal:

```text
0x2fa86add0aed31f33a762c9d88e807c475bd51d0f52bd0955754b2608f7e4989
```

The factory runtime comes from Arachnid commit
`be3c5974db5028d502537209329ff2e730ed336c` and is independently pinned by Foundry
1.8.3. The script sends the factory the 32-byte salt followed directly by creation
bytecode and ABI-encoded constructor arguments; it checks the returned 20-byte
address and deployed code. [Arc common contracts](https://docs.arc.io/arc/references/contract-addresses#common-ethereum-contracts),
[Arachnid source](https://github.com/Arachnid/deterministic-deployment-proxy/blob/be3c5974db5028d502537209329ff2e730ed336c/source/deterministic-deployment-proxy.yul),
[published deployment bytecode](https://github.com/Arachnid/deterministic-deployment-proxy/blob/be3c5974db5028d502537209329ff2e730ed336c/README.md#deployment-transaction),
[Foundry factory constants](https://github.com/foundry-rs/foundry/blob/cae51ad458f6abb64852b7709eb784352429825d/crates/evm/core/src/constants.rs#L57)

For each role, the script computes `salt = keccak256(abi.encode(releaseSalt, role))`:

| Contract | Exact role string |
| --- | --- |
| Timelock | `mppi.timelock` |
| Implementation | `mppi.implementation` |
| Proxy | `mppi.proxy` |

Each address is the last 20 bytes of
`keccak256(0xff || factory || salt || keccak256(initCode))`. The proxy's init code
includes the predicted implementation and the complete initializer with the
predicted timelock. This binds its authority configuration to its address. The
script checks all three predicted addresses for existing code or a nonzero nonce
before recording the first factory transaction. Existing deployment or collision
causes failure; reconcile receipts and release parameters before retrying.
[EIP-1014 address and collision rules](https://eips.ethereum.org/EIPS/eip-1014)

Record all three derived salts, init-code hashes and predicted addresses in the
release record. Inputs, compiler settings, source metadata and dependency changes
can change these addresses. A predicted address becomes a published canonical
MPPI address only after confirmed deployment and release verification. No public
deployment is performed by the local verification suite.

## Local rehearsal and release verification

The local suite tests this script on an isolated EVM configured with the selected
Arc chain ID; it does not bypass the chain guard. Negative tests cover
using either entry point on the wrong network, missing code, zero configuration and invalid Safe owner or
threshold, zero deployer and reuse of the Safe owner as deployer. The tests install
the canonical factory runtime and cover missing/wrong factory code, deterministic
address calculation, repeated deployment, existing code and nonce-only collisions.
The successful case verifies all three factory calls consume the selected deployer's
nonce and that it receives no timelock admin, proposer or canceller role. The read-interface
Safe fixture is not a Safe transaction test.

Foundry's script command simulates by default; its explicit `--broadcast` flag
sends recorded transactions. `vm.startBroadcast(address)` records calls at the script's
call depth. The script makes explicit calls to the verified factory rather than
depending on an implicit CREATE2-deployer setting. Keep generated simulation, broadcast and verification results outside
the public source tree. Actual testnet broadcast is a separate release action
after prerequisites and wallet access are ready.
[Foundry broadcast semantics](https://getfoundry.sh/reference/cheatcodes/start-broadcast),
[Foundry script workflow](https://getfoundry.sh/guides/scripting-with-config/)

After confirmed creation, verify all three addresses and runtime code identities,
the ERC-1967 implementation slot, exact initializer parameters, initialized proxy,
disabled implementation initializer, initial pause level, roles/delay and absence
of deployer authority. Verify the MPPI EIP-712 domain against the actual chain and
proxy. Record receipts and observed blocks. Complete Arc-native USDC, registry,
wallet and SDK execution checks before publishing a canonical deployment manifest.
Local Anvil fixtures do not prove those integrations.

For mainnet preparation, select `runMainnet()` explicitly with Foundry's
`--sig 'runMainnet()'`. The existing `run()` entry point continues to enforce
testnet and its **60-second** timelock. Mainnet uses the selected initialized
**1-of-1 Safe** and an **86,400-second** timelock. It requires the designated
Daybreak audit of the exact artifacts and deployment inputs, resolved required
findings and its own verified release evidence. Preparation is not deployment
acceptance. The one-hour session dispute window and 24-hour idle timeout are
separate from governance delay.
