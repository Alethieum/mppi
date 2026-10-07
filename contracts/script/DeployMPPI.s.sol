// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {Script} from "forge-std/Script.sol";
import {TimelockController} from "@openzeppelin/contracts/governance/TimelockController.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {MPPI} from "../src/MPPI.sol";
import {IMPPI} from "../src/IMPPI.sol";

interface IMPPIDeploymentSafe {
    function getOwners() external view returns (address[] memory);
    function getThreshold() external view returns (uint256);
}

/// @notice Arc deployment preparation with the selected 1-of-1 Safe profile.
/// @dev Read docs/deployment.md before use. Code/configuration checks do not prove
///      registry/Safe identity, custody, or approval of the release parameters.
///      No private key is read by the script. Foundry supplies the broadcaster.
contract DeployMPPI is Script {
    uint256 public constant ARC_TESTNET_CHAIN_ID = 5042002;
    uint256 public constant ARC_MAINNET_CHAIN_ID = 5042;
    uint256 public constant TESTNET_TIMELOCK_DELAY = 60;
    uint256 public constant MAINNET_TIMELOCK_DELAY = 86_400;
    address public constant MPPI_CREATE2_FACTORY = 0x4e59b44847b379578588920cA78FbF26c0B4956C;
    // Arachnid be3c5974 runtime; independently pinned by Foundry 1.8.3 constants.rs.
    bytes32 public constant CREATE2_FACTORY_CODEHASH =
        0x2fa86add0aed31f33a762c9d88e807c475bd51d0f52bd0955754b2608f7e4989;

    struct Configuration {
        address registry;
        address guardian;
        address safe;
        address safeOwner;
        address deployer;
        bytes32 releaseSalt;
    }

    struct Creation {
        bytes32 salt;
        bytes initCode;
        address predicted;
    }

    error WrongChain(uint256 actual, uint256 expected);
    error ZeroDeploymentAddress();
    error MissingContractCode(address account);
    error InvalidGovernanceSafe();
    error DeployerIsSafeOwner();
    error UnexpectedFactoryCode(bytes32 actual);
    error DeploymentAddressOccupied(address account);
    error FactoryDeploymentFailed(bytes32 salt);
    error UnexpectedFactoryResult(address expected);

    function run() external returns (TimelockController timelock, MPPI implementation, ERC1967Proxy proxy) {
        return _run(ARC_TESTNET_CHAIN_ID, TESTNET_TIMELOCK_DELAY);
    }

    /// @notice Explicit mainnet entry point; `run()` remains testnet-only.
    function runMainnet() external returns (TimelockController timelock, MPPI implementation, ERC1967Proxy proxy) {
        return _run(ARC_MAINNET_CHAIN_ID, MAINNET_TIMELOCK_DELAY);
    }

    function _run(uint256 expectedChainId, uint256 delay)
        private
        returns (TimelockController timelock, MPPI implementation, ERC1967Proxy proxy)
    {
        if (block.chainid != expectedChainId) revert WrongChain(block.chainid, expectedChainId);
        Configuration memory config = Configuration({
            registry: vm.envAddress("MPPI_REGISTRY"),
            guardian: vm.envAddress("MPPI_GUARDIAN"),
            safe: vm.envAddress("MPPI_SAFE"),
            safeOwner: vm.envAddress("MPPI_SAFE_OWNER"),
            deployer: vm.envAddress("MPPI_DEPLOYER"),
            releaseSalt: vm.envBytes32("MPPI_RELEASE_SALT")
        });
        _validate(config);
        Creation[3] memory plan = _prepare(config, delay);
        for (uint256 i; i < plan.length; ++i) {
            if (plan[i].predicted.code.length != 0 || vm.getNonce(plan[i].predicted) != 0) {
                revert DeploymentAddressOccupied(plan[i].predicted);
            }
        }
        vm.startBroadcast(config.deployer);
        for (uint256 i; i < plan.length; ++i) {
            _deploy(plan[i]);
        }
        vm.stopBroadcast();
        return (
            TimelockController(payable(plan[0].predicted)),
            MPPI(plan[1].predicted),
            ERC1967Proxy(payable(plan[2].predicted))
        );
    }

    function _validate(Configuration memory config) private view {
        if (
            config.registry == address(0) || config.guardian == address(0) || config.safe == address(0)
                || config.safeOwner == address(0) || config.deployer == address(0)
        ) {
            revert ZeroDeploymentAddress();
        }
        if (config.deployer == config.safeOwner) revert DeployerIsSafeOwner();
        if (config.registry.code.length == 0) revert MissingContractCode(config.registry);
        if (config.safe.code.length == 0) revert MissingContractCode(config.safe);
        address[] memory owners = IMPPIDeploymentSafe(config.safe).getOwners();
        if (
            owners.length != 1 || owners[0] != config.safeOwner || owners[0] == address(1) || owners[0] == config.safe
                || IMPPIDeploymentSafe(config.safe).getThreshold() != 1
        ) revert InvalidGovernanceSafe();
        if (MPPI_CREATE2_FACTORY.codehash != CREATE2_FACTORY_CODEHASH) {
            revert UnexpectedFactoryCode(MPPI_CREATE2_FACTORY.codehash);
        }
    }

    function _prepare(Configuration memory config, uint256 delay) private pure returns (Creation[3] memory plan) {
        address[] memory proposers = new address[](1);
        proposers[0] = config.safe;
        address[] memory executors = new address[](1);
        executors[0] = address(0);
        plan[0] = _creation(
            config.releaseSalt,
            "mppi.timelock",
            abi.encodePacked(type(TimelockController).creationCode, abi.encode(delay, proposers, executors, address(0)))
        );
        plan[1] = _creation(config.releaseSalt, "mppi.implementation", type(MPPI).creationCode);
        bytes memory initializer =
            abi.encodeCall(IMPPI.initialize, (config.registry, config.guardian, config.safe, plan[0].predicted));
        plan[2] = _creation(
            config.releaseSalt,
            "mppi.proxy",
            abi.encodePacked(type(ERC1967Proxy).creationCode, abi.encode(plan[1].predicted, initializer))
        );
    }

    function _creation(bytes32 releaseSalt, string memory role, bytes memory initCode)
        private
        pure
        returns (Creation memory)
    {
        bytes32 salt = keccak256(abi.encode(releaseSalt, role));
        address predicted = address(
            uint160(uint256(keccak256(abi.encodePacked(hex"ff", MPPI_CREATE2_FACTORY, salt, keccak256(initCode)))))
        );
        return Creation(salt, initCode, predicted);
    }

    function _deploy(Creation memory creation) private {
        (bool success, bytes memory result) =
            MPPI_CREATE2_FACTORY.call(abi.encodePacked(creation.salt, creation.initCode));
        if (!success) revert FactoryDeploymentFailed(creation.salt);
        if (
            result.length != 20 || address(bytes20(result)) != creation.predicted || creation.predicted.code.length == 0
        ) {
            revert UnexpectedFactoryResult(creation.predicted);
        }
    }
}
