// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {Test} from "forge-std/Test.sol";
import {TimelockController} from "@openzeppelin/contracts/governance/TimelockController.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {DeployMPPI} from "../script/DeployMPPI.s.sol";
import {MPPI} from "../src/MPPI.sol";
import {IMPPI} from "../src/IMPPI.sol";
import {MockIdentityRegistry} from "./MockIdentityRegistry.sol";

/// @dev Safe read-interface fixture only; this does not exercise Safe signatures.
contract DeploymentSafeFixture {
    address[] private owners;
    uint256 private threshold;

    constructor(address owner) {
        owners.push(owner);
        threshold = 1;
    }

    function configure(address[] memory nextOwners, uint256 nextThreshold) external {
        owners = nextOwners;
        threshold = nextThreshold;
    }

    function getOwners() external view returns (address[] memory) {
        return owners;
    }

    function getThreshold() external view returns (uint256) {
        return threshold;
    }
}

contract MPPIDeploymentTest is Test {
    // Exact runtime from Arachnid be3c5974 README deployment transaction and
    // Foundry 1.8.3 crates/evm/core/src/constants.rs; no replacement Solidity factory.
    bytes private constant FACTORY_RUNTIME =
        hex"7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe03601600081602082378035828234f58015156039578182fd5b8082525050506014600cf3";
    bytes32 private constant RELEASE_SALT = keccak256("local deployment verification");
    DeployMPPI private script;
    MockIdentityRegistry private registry;
    DeploymentSafeFixture private safe;
    address private guardian;
    address private owner;
    address private deployer;

    function setUp() public {
        vm.chainId(5042002);
        guardian = makeAddr("deployment guardian");
        owner = makeAddr("testnet safe owner");
        deployer = makeAddr("separate testnet deployer");
        script = new DeployMPPI();
        registry = new MockIdentityRegistry();
        safe = new DeploymentSafeFixture(owner);
        vm.etch(script.MPPI_CREATE2_FACTORY(), FACTORY_RUNTIME);
    }

    function _environment() private {
        vm.setEnv("MPPI_REGISTRY", vm.toString(address(registry)));
        vm.setEnv("MPPI_GUARDIAN", vm.toString(guardian));
        vm.setEnv("MPPI_SAFE", vm.toString(address(safe)));
        vm.setEnv("MPPI_SAFE_OWNER", vm.toString(owner));
        vm.setEnv("MPPI_DEPLOYER", vm.toString(deployer));
        vm.setEnv("MPPI_RELEASE_SALT", vm.toString(RELEASE_SALT));
    }

    // setEnv changes the process environment in Foundry 1.8.3. Keep these cases
    // in one test so parallel test functions cannot race on the six variables.
    function testDeploymentWiringAndRejections() public {
        _environment();
        _rejectMissingAndWrongFactory();
        _atomicDeploymentAndFinalRoles();
        _rejectOccupiedAddressBeforeBroadcast();
        _environment();
        _rejectWrongChainIncludingMainnet();
        vm.chainId(5042002);
        _rejectZeroConfiguration();
        _environment();
        _rejectMissingRegistryCode();
        _environment();
        _rejectEOASafe();
        _environment();
        _rejectSafeOwnerAsDeployer();
        _environment();
        _rejectZeroDeployer();
        _environment();
        _rejectUninitializedSafe();
        _rejectWrongOwnerAndThreshold();
        _rejectMultipleOwnersForSelectedTestnetProfile();
    }

    function _atomicDeploymentAndFinalRoles() private {
        uint64 beforeNonce = vm.getNonce(deployer);
        address expectedTimelock = _predictTimelock(RELEASE_SALT);
        address expectedImplementation = _predict(RELEASE_SALT, "mppi.implementation", type(MPPI).creationCode);
        bytes memory initializer =
            abi.encodeCall(IMPPI.initialize, (address(registry), guardian, address(safe), expectedTimelock));
        address expectedProxy = _predict(
            RELEASE_SALT,
            "mppi.proxy",
            abi.encodePacked(type(ERC1967Proxy).creationCode, abi.encode(expectedImplementation, initializer))
        );
        (TimelockController timelock, MPPI implementation, ERC1967Proxy proxy) = script.run();
        assertEq(vm.getNonce(deployer), beforeNonce + 3, "all three factory calls use the selected broadcaster");
        assertEq(address(timelock), expectedTimelock);
        assertEq(address(implementation), expectedImplementation);
        assertEq(address(proxy), expectedProxy);
        MPPI mppi = MPPI(address(proxy));
        assertEq(timelock.getMinDelay(), 60);
        assertTrue(timelock.hasRole(timelock.PROPOSER_ROLE(), address(safe)));
        assertTrue(timelock.hasRole(timelock.CANCELLER_ROLE(), address(safe)));
        assertTrue(timelock.hasRole(timelock.EXECUTOR_ROLE(), address(0)));
        assertTrue(timelock.hasRole(timelock.DEFAULT_ADMIN_ROLE(), address(timelock)));
        assertFalse(timelock.hasRole(timelock.DEFAULT_ADMIN_ROLE(), address(safe)));
        assertFalse(timelock.hasRole(timelock.DEFAULT_ADMIN_ROLE(), address(this)));
        assertFalse(timelock.hasRole(timelock.DEFAULT_ADMIN_ROLE(), address(script)));
        assertFalse(timelock.hasRole(timelock.DEFAULT_ADMIN_ROLE(), tx.origin));
        assertFalse(timelock.hasRole(timelock.DEFAULT_ADMIN_ROLE(), deployer));
        assertFalse(timelock.hasRole(timelock.PROPOSER_ROLE(), deployer));
        assertFalse(timelock.hasRole(timelock.CANCELLER_ROLE(), deployer));
        bytes32 slot = bytes32(uint256(keccak256("eip1967.proxy.implementation")) - 1);
        assertEq(address(uint160(uint256(vm.load(address(proxy), slot)))), address(implementation));
        assertEq(uint8(mppi.pauseLevel()), uint8(IMPPI.PauseLevel.None));
        vm.expectRevert(bytes4(keccak256("InvalidInitialization()")));
        mppi.initialize(address(registry), guardian, address(safe), address(timelock));
        vm.expectRevert(bytes4(keccak256("InvalidInitialization()")));
        implementation.initialize(address(registry), guardian, address(safe), address(timelock));
        vm.prank(guardian);
        mppi.pause();
        vm.prank(address(safe));
        mppi.unpause();
        vm.prank(address(timelock));
        mppi.pauseAll();
        assertEq(uint8(mppi.pauseLevel()), uint8(IMPPI.PauseLevel.All));
        beforeNonce = vm.getNonce(deployer);
        vm.expectRevert(abi.encodeWithSelector(DeployMPPI.DeploymentAddressOccupied.selector, address(timelock)));
        script.run();
        assertEq(vm.getNonce(deployer), beforeNonce, "repeat rejected before broadcast");
    }

    function _predict(bytes32 releaseSalt, string memory role, bytes memory initCode) private view returns (address) {
        bytes32 salt = keccak256(abi.encode(releaseSalt, role));
        bytes32 digest =
            keccak256(abi.encodePacked(bytes1(0xff), script.MPPI_CREATE2_FACTORY(), salt, keccak256(initCode)));
        return address(uint160(uint256(digest)));
    }

    function _predictTimelock(bytes32 releaseSalt) private view returns (address) {
        address[] memory proposers = new address[](1);
        proposers[0] = address(safe);
        address[] memory executors = new address[](1);
        return _predict(
            releaseSalt,
            "mppi.timelock",
            abi.encodePacked(
                type(TimelockController).creationCode, abi.encode(uint256(60), proposers, executors, address(0))
            )
        );
    }

    function _rejectMissingAndWrongFactory() private {
        address factory = script.MPPI_CREATE2_FACTORY();
        assertEq(keccak256(FACTORY_RUNTIME), script.CREATE2_FACTORY_CODEHASH(), "primary artifact hash");
        uint64 beforeNonce = vm.getNonce(deployer);
        vm.etch(factory, "");
        vm.expectRevert(abi.encodeWithSelector(DeployMPPI.UnexpectedFactoryCode.selector, factory.codehash));
        script.run();
        vm.etch(factory, hex"00");
        vm.expectRevert(abi.encodeWithSelector(DeployMPPI.UnexpectedFactoryCode.selector, factory.codehash));
        script.run();
        assertEq(vm.getNonce(deployer), beforeNonce, "factory identity failure precedes broadcast");
        vm.etch(factory, FACTORY_RUNTIME);
    }

    function _rejectOccupiedAddressBeforeBroadcast() private {
        bytes32 salt = keccak256("local occupied address verification");
        vm.setEnv("MPPI_RELEASE_SALT", vm.toString(salt));
        address implementation = _predict(salt, "mppi.implementation", type(MPPI).creationCode);
        address timelock = _predictTimelock(salt);
        uint64 beforeNonce = vm.getNonce(deployer);
        vm.etch(implementation, hex"00");
        vm.expectRevert(abi.encodeWithSelector(DeployMPPI.DeploymentAddressOccupied.selector, implementation));
        script.run();
        assertEq(timelock.code.length, 0, "later collision detected before first creation");
        vm.etch(implementation, "");
        vm.setNonce(implementation, 1);
        vm.expectRevert(abi.encodeWithSelector(DeployMPPI.DeploymentAddressOccupied.selector, implementation));
        script.run();
        assertEq(timelock.code.length, 0, "nonzero nonce also prevents CREATE2 collision");
        assertEq(vm.getNonce(deployer), beforeNonce, "occupied address rejected before broadcast");
    }

    function _rejectWrongChainIncludingMainnet() private {
        vm.chainId(31337);
        vm.expectRevert(abi.encodeWithSelector(DeployMPPI.WrongChain.selector, 31337, 5042002));
        script.run();
        vm.chainId(5042);
        vm.expectRevert(abi.encodeWithSelector(DeployMPPI.WrongChain.selector, 5042, 5042002));
        script.run();
    }

    function _rejectZeroConfiguration() private {
        vm.setEnv("MPPI_GUARDIAN", vm.toString(address(0)));
        vm.expectRevert(DeployMPPI.ZeroDeploymentAddress.selector);
        script.run();
    }

    function _rejectMissingRegistryCode() private {
        address missing = makeAddr("missing registry");
        vm.setEnv("MPPI_REGISTRY", vm.toString(missing));
        vm.expectRevert(abi.encodeWithSelector(DeployMPPI.MissingContractCode.selector, missing));
        script.run();
    }

    function _rejectEOASafe() private {
        vm.setEnv("MPPI_SAFE", vm.toString(owner));
        vm.expectRevert(abi.encodeWithSelector(DeployMPPI.MissingContractCode.selector, owner));
        script.run();
    }

    function _rejectUninitializedSafe() private {
        safe.configure(new address[](0), 0);
        vm.expectRevert(DeployMPPI.InvalidTestnetSafe.selector);
        script.run();
    }

    function _rejectSafeOwnerAsDeployer() private {
        vm.setEnv("MPPI_DEPLOYER", vm.toString(owner));
        uint64 beforeNonce = vm.getNonce(owner);
        vm.expectRevert(DeployMPPI.DeployerIsSafeOwner.selector);
        script.run();
        assertEq(vm.getNonce(owner), beforeNonce, "rejected before contract creation");
    }

    function _rejectZeroDeployer() private {
        vm.setEnv("MPPI_DEPLOYER", vm.toString(address(0)));
        vm.expectRevert(DeployMPPI.ZeroDeploymentAddress.selector);
        script.run();
    }

    function _rejectWrongOwnerAndThreshold() private {
        address[] memory owners = new address[](1);
        owners[0] = guardian;
        safe.configure(owners, 1);
        vm.expectRevert(DeployMPPI.InvalidTestnetSafe.selector);
        script.run();
        owners[0] = owner;
        safe.configure(owners, 0);
        vm.expectRevert(DeployMPPI.InvalidTestnetSafe.selector);
        script.run();
    }

    function _rejectMultipleOwnersForSelectedTestnetProfile() private {
        address[] memory owners = new address[](2);
        owners[0] = owner;
        owners[1] = guardian;
        safe.configure(owners, 1);
        vm.expectRevert(DeployMPPI.InvalidTestnetSafe.selector);
        script.run();
    }
}
