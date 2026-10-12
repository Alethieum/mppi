# MPPI v1 contract interface

**Version: 0.1.0.** [IMPPI.sol](../contracts/src/IMPPI.sol) and its
[generated ABI](../contracts/abi/IMPPI.json) define the application interface.
[MPPISigning.sol](../contracts/src/MPPISigning.sol) supplies pure hashing.
[MPPI.sol](../contracts/src/MPPI.sol) implements payment execution and namespaced
upgradeable storage; its [full ABI](../contracts/abi/MPPI.json) includes inherited
OpenZeppelin entries. The official Arc mainnet and testnet deployments are
[published](deployment.md), with explicit network selection in SDK `0.1.2`.
Deployment records and security-review evidence describe separate acceptance scopes.
No address in the signing fixtures is a deployment announcement.

## Identity and signatures

The descriptor has this exact ABI order:

```text
(address agent, address sessionSigner, uint256 providerId,
 address recipient, address operator, address token, bytes32 salt)
```

`agent` is the funded wallet and refund destination. `sessionSigner` is the fixed,
nonzero ECDSA authority for the session proofs. `providerId` is the provider's
ERC-8004 identity; recipient and operator are the opening-bound provider addresses.
The token is Arc USDC, `0x3600000000000000000000000000000000000000`; all MPPI
amounts are unsigned 64-bit values in six-decimal ERC-20 base units. Native gas
precision must not be substituted. [Arc contract addresses, USDC](https://docs.arc.io/arc/references/contract-addresses#usdc).
The agent's ERC-8004 onboarding and USDC authorization remain attached to its
funding wallet.

```text
sessionId = keccak256(abi.encode(
  agent, sessionSigner, providerId, recipient, operator, token, salt,
  canonicalProxy, uint256(chainId)
))
```

The four EIP-712 types are fixed:

```text
Voucher(bytes32 sessionId,uint64 cumulativeAmount)
RunAuth(bytes32 sessionId,uint64 controlGeneration,bytes32 runId,string model,bytes32 rateCardHash)
ReconnectAuth(bytes32 sessionId,bytes32 nonce)
CloseAuth(bytes32 sessionId)
```

The domain is `name=MPPI`, `version=1`, the verified chain id and canonical proxy
as `verifyingContract`. No implementation address or additional domain field is
substituted. Strings are hashed as their UTF-8 bytes. MPPI proofs use 65-byte
`r || s || v`, low-s, with v equal to 27 or 28, recovering to `sessionSigner`.
This is MPPI's explicit encoding profile; a generic library's broader input
acceptance must not broaden it. Signature validity alone proves no live session,
fresh attachment, nonce eligibility, price acceptance or funded run. USDC receive
authorization uses the token's own domain and funding-wallet authority.

These encodings follow [EIP-712, hashStruct/encodeData/domainSeparator](https://eips.ethereum.org/EIPS/eip-712)
and [Solidity 0.8.37 ABI tuple encoding](https://docs.soliditylang.org/en/v0.8.37/abi-spec.html).
The [shared vectors](../testvectors/signing.json) include all four proof types,
zero and maximum vouchers, a provider id above JavaScript's exact integer range,
and an additional UTF-8 hashing case. That extra model is not an admissible MPPI
catalog identifier; it tests EIP-712 encoding only. Hashing proves no eligibility.

## Interface surface

All application calls are nonpayable. Only the four specified getters/helpers are
`view`; `computeSessionId` reads the executing chain/proxy context in the
contract. The pure library takes that context explicitly.

| Function | Authority and result |
| --- | --- |
| `initialize(registry, guardian, safe, timelock)` | One-time proxy initialization; definition below |
| `open(d, deposit, expectedRateCardHash, auth)` | Funding wallet `d.agent`; positive deposit; record descriptor/catalog and pull exact authorized USDC |
| `topUp(d, amount, auth)` | Funding wallet; positive amount; increase cumulative deposit without changing descriptor or opening catalog hash |
| `settle(d, cumulativeAmount, captureAmount, voucherSig)` | Frozen operator or recipient; pay only an eligible increasing captured delta |
| `settleBatch(d[], cumulativeAmounts[], captureAmounts[], voucherSigs[])` | Same per-item authority; equal array lengths; individual item failures are isolated |
| `requestClose(d)` | Funding wallet; start the unilateral close interval |
| `close(d, cumulativeAmount, captureAmount, voucherSig)` | Operator or recipient; final eligible capture and refund to funding wallet |
| `withdraw(d)` | Funding wallet after the applicable closing deadline |
| `releaseIdle(d)` | Permissionless after idle eligibility; refund destination remains the funding wallet |
| `computeSessionId(d)` | View; identity only, no existence assertion |
| `getSession(sessionId)` | View; retained record, including Closed; positive deposit distinguishes existence |
| `voucherDigest(sessionId, cumulativeAmount)` | View; MPPI voucher digest in the executing proxy domain |
| `pauseLevel()` | View; authoritative pause state at the observed block |
| `pause()` | Guardian; apply Level 1 |
| `unpause()` | Safe; clear Level 1 |
| `pauseAll()` | Timelock; apply Level 2 |
| `unpauseAll()` | Timelock; clear either paused level |
| `_settleBatchItem(caller, d, cumulativeAmount, captureAmount, voucherSig)` | Restricted self-call while the batch guard is entered; preserves original caller; not an SDK operation |

The interface also declares all seven application events and 27 application
errors. `SessionOpened` indexes exactly `sessionId`, `agent` and `providerId`;
`sessionSigner` is non-indexed immediately after `agent`. Structs, labels and
indexed positions are available in the generated ABI; consumers must generate
bindings from it rather than reconstruct the old six-field descriptor.

`SessionStatus` encodes Open=0, Closing=1, Closed=2. Protobuf uses a separate
unspecified value and encodes those statuses as 1, 2, 3: map explicitly.
`PauseLevel` is None=0, Payments=1, All=2 on both interfaces; protobuf presence
distinguishes explicit None from an absent field.

`captureAmount` is cumulative actual collection, bounded by the voucher ceiling
and deposit. An increasing capture needs the fixed signer's voucher. An eligible
equal capture is a no-op settlement, with no event or activity refresh. Closing
still refunds the balance and ends the session. The contract does not count tokens
or prove the provider's usage. Run pricing and streaming authorization are defined
by the [protocol profile](protocol.md).

## Initialization and authority definition

The initializer's exact selector is generated from:

```solidity
initialize(address registry, address guardian, address safe, address timelock)
```

The implementation constructor must disable its initializers. Create the
ERC-1967 proxy with nonempty encoded initializer calldata in the creation
transaction. All four arguments must be nonzero; reject a zero value using
`InvalidInitializationAddress(address account)`. Store the registry and three
authority addresses and initialize the single global pause level to None. A
failed initializer must revert proxy creation. Reinitializing the same proxy
must fail using the pinned Initializable implementation's guard.

Use direct stored-address caller checks. An unauthorized administrative call
raises `UnauthorizedCaller(caller, requiredAuthority)` before transition checks.
This applies to the four pause controls and timelock-only upgrade authorization.
No MPPI owner layer, role grant API, authority setter or emergency bypass is added.
The existing `NotAgent` and `NotOperatorOrRecipient` errors retain their payment
roles. `InvalidSessionSigner` rejects a zero session signer at opening, after the
funding caller check and before token interaction.

| Stored authority | Permitted control |
| --- | --- |
| Guardian | Apply Payments from None immediately |
| Governance Safe | Clear Payments to None |
| Timelock | Apply All from None/Payments; clear either paused level; authorize a UUPS upgrade |

Level-1 calls cannot weaken All. Repeated pause or blocked weakening raises
`EnforcedPause`; unpause at None raises `ExpectedPause`. A successful transition
emits exactly one `PauseLevelChanged`. Pausing changes no session accounting,
status or timestamps. Refunds and exact deadline boundaries are enforced by the
payment implementation and exercised by its lifecycle tests.

Deploy the selected OpenZeppelin **5.7.0** TimelockController with:

```text
minDelay = 60 seconds on Arc testnet; 86,400 seconds on Arc mainnet
proposers = [governance Safe]
executors = [address(0)]
admin = address(0)
```

The controller administers itself; the Safe receives proposer/canceller authority,
and execution is permissionless after a scheduled operation becomes eligible.
There is no deployer bootstrap admin. The stock delay can subsequently change
through a timelocked self-call; MPPI does not add an immutable delay floor.

Before creating the mainnet proxy, verify the actual governance Safe is initialized
with the selected **single owner and threshold one**, matching the testnet
custody arrangement. Shared signing custody provides no independent quorum. Verify the
registry, Safe, controller, chain id, deployed code identities, roles and delay;
code presence alone is not identity proof. Publish a deployment manifest only
after implementation/proxy initialization, domain, initial pause state and absence
of deployer privileges are verified. No registry or proxy address is invented here.

This wiring follows the pinned sources:

- [ERC1967Proxy 5.7.0 constructor](https://github.com/OpenZeppelin/openzeppelin-contracts/blob/v5.7.0/contracts/proxy/ERC1967/ERC1967Proxy.sol): initializer delegatecall during construction.
- [Initializable 5.7.0](https://github.com/OpenZeppelin/openzeppelin-contracts/blob/v5.7.0/contracts/proxy/utils/Initializable.sol): one-time initialization and disabling implementation initialization.
- [TimelockController 5.7.0](https://github.com/OpenZeppelin/openzeppelin-contracts/blob/v5.7.0/contracts/governance/TimelockController.sol): constructor roles, open executor semantics and self-administered delay changes.
- [UUPSUpgradeable 5.7.0](https://github.com/OpenZeppelin/openzeppelin-contracts/blob/v5.7.0/contracts/proxy/utils/UUPSUpgradeable.sol): implementation-side upgrade authorization.

The application ABI is a subset of the full inherited implementation ABI.
OpenZeppelin upgrade/initialization functions, events and errors, SafeERC20,
SafeCast and reentrancy errors must be emitted by the pinned implementation build.
`IMPPI` does not pretend to implement that inheritance. The timelock can replace
logic governing escrowed funds; storage-layout compatibility alone does not limit
a new implementation's authority. Deployment and audit remain required.

## Verification boundary

Foundation checks exercise EVM hashing/recovery, Rust/Python agreement and
generated artifacts. The [contract suites](../contracts/test/) additionally
exercise funding, capture, refunds, role/pause matrices, rollback, reentrancy,
deadlines, invariant accounting, storage preservation and delayed upgrades through
the actual proxy. Token, identity and Safe read fixtures are local test boundaries.
They do not certify live Arc USDC, wallet products, deployed registry/Safe identity
or governance custody. Follow the [deployment requirements](deployment.md) before
any broadcast; independent audit and chain acceptance remain release gates.
