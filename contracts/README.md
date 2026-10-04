# MPPI smart contract

[MPPI.sol](src/MPPI.sol) implements funded USDC sessions: authorized partial
captures, top-ups, isolated batch settlement, cooperative close, unilateral
refunds, exact deadlines, pause controls and timelock-authorized UUPS upgrades.
It enforces payment authority and bounds; it does not measure inference usage.

| Artifact | Purpose |
| --- | --- |
| [IMPPI.sol](src/IMPPI.sol), [application ABI](abi/IMPPI.json) | Stable protocol functions, descriptors, errors and events |
| [MPPI.sol](src/MPPI.sol), [implementation ABI](abi/MPPI.json) | Payment implementation and inherited OpenZeppelin surface |
| [MPPISigning.sol](src/MPPISigning.sol) | Session identity and EIP-712 hashes shared with both SDKs |
| [DeployMPPI.s.sol](script/DeployMPPI.s.sol) | Guarded Arc testnet deployment preparation |
| [test/](test/) | Unit, adversarial, fuzz, invariant and deployment tests |

Read the [contract profile](../docs/contract-interface.md) and
[deployment requirements](../docs/deployment.md). No live MPPI address is
announced here. SDK runtime integration and Arc acceptance remain pending.

## Verify locally

With Foundry **1.8.3** and Solidity **0.8.37**, from this directory:

```sh
forge build --sizes
forge test --fuzz-seed 0x6d70706920260925
```

The release compiler targets Paris, optimizer enabled with 200 runs and no
via-IR. Tests execute the real MPPI implementation through ERC-1967 proxies,
with local USDC/identity fixtures and synthetic keys. The stateful campaign
checks independent accounting across generated operations and refunds every
remaining eligible session at the end of each run. These fixtures do not
reproduce Arc's native USDC runtime or establish real wallet compatibility.

OpenZeppelin **5.7.0** and forge-std **1.16.2** sources are vendored unchanged;
their `UPSTREAM.json` manifests record exact commits, file hashes and licenses.
No dependency update is implicit in a build. The standard namespaced
ReentrancyGuard is the upstream-supported proxy variant; tests explicitly check
its initial zero state, first-call protection and rollback. See the pinned
[guard source](lib/openzeppelin-contracts/contracts/utils/ReentrancyGuard.sol) and
[upstream changelog](https://github.com/OpenZeppelin/openzeppelin-contracts/blob/v5.7.0/CHANGELOG.md).

Project Solidity sources are licensed under the [Apache License, Version 2.0](../LICENSE).
Vendored dependencies retain their own licenses. Generated build,
coverage and broadcast outputs must stay outside the published source tree.
