// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {Test} from "forge-std/Test.sol";
import {Vm} from "forge-std/Vm.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {TimelockController} from "@openzeppelin/contracts/governance/TimelockController.sol";
import {MPPI} from "../src/MPPI.sol";
import {IMPPI} from "../src/IMPPI.sol";
import {MockUSDC} from "./MockUSDC.sol";
import {MockIdentityRegistry} from "./MockIdentityRegistry.sol";

abstract contract MPPIBase is Test {
    // Publicly known test keys; never funded outside the local ephemeral chain.
    uint256 internal constant AGENT_KEY = 0xA11CE;
    uint256 internal constant SIGNER_KEY = 0xB0B;
    uint256 internal constant PROVIDER_ID = 42;
    address internal constant USDC = 0x3600000000000000000000000000000000000000;
    bytes internal constant CATALOG = '[{"model":"test-model","input":"1","output":"2"}]';
    MPPI internal mppi;
    MPPI internal implementation;
    MockUSDC internal token;
    MockIdentityRegistry internal registry;
    TimelockController internal timelock;
    address internal agent;
    address internal sessionSigner;
    address internal operator;
    address internal recipient;
    address internal guardian;
    address internal safe;
    address internal stranger;

    function setUp() public virtual {
        vm.warp(1_000_000);
        agent = vm.addr(AGENT_KEY);
        sessionSigner = vm.addr(SIGNER_KEY);
        operator = makeAddr("provider operator");
        recipient = makeAddr("provider recipient");
        guardian = makeAddr("guardian");
        safe = makeAddr("safe");
        stranger = makeAddr("stranger");
        registry = new MockIdentityRegistry();
        registry.setProvider(PROVIDER_ID, operator, recipient, CATALOG);
        MockUSDC template = new MockUSDC();
        vm.etch(USDC, address(template).code);
        token = MockUSDC(USDC);
        token.mint(agent, uint256(type(uint64).max) * 100);
        address[] memory proposers = new address[](1);
        proposers[0] = safe;
        address[] memory executors = new address[](1);
        executors[0] = address(0);
        timelock = new TimelockController(60, proposers, executors, address(0));
        implementation = new MPPI();
        mppi = MPPI(address(new ERC1967Proxy(address(implementation), _initializer())));
    }

    function _initializer() internal view returns (bytes memory) {
        return abi.encodeCall(IMPPI.initialize, (address(registry), guardian, safe, address(timelock)));
    }

    function _descriptor(bytes32 salt) internal view returns (IMPPI.SessionDescriptor memory) {
        return IMPPI.SessionDescriptor(agent, sessionSigner, PROVIDER_ID, recipient, operator, USDC, salt);
    }

    function _id(IMPPI.SessionDescriptor memory d) internal view returns (bytes32) {
        return mppi.computeSessionId(d);
    }

    function _auth(IMPPI.SessionDescriptor memory d, uint64 amount, bytes32 nonce)
        internal
        view
        returns (IMPPI.EIP3009Auth memory)
    {
        return _authAt(d, amount, nonce, block.timestamp - 1, block.timestamp + 7 days);
    }

    function _authAt(IMPPI.SessionDescriptor memory d, uint64 amount, bytes32 nonce, uint256 after_, uint256 before_)
        internal
        view
        returns (IMPPI.EIP3009Auth memory auth)
    {
        auth = IMPPI.EIP3009Auth(after_, before_, nonce, "");
        auth.signature =
            _sign(AGENT_KEY, token.authorizationDigest(d.agent, address(mppi), amount, after_, before_, nonce));
    }

    function _sign(uint256 key, bytes32 digest) internal pure returns (bytes memory) {
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(key, digest);
        return abi.encodePacked(r, s, v);
    }

    function _voucher(IMPPI.SessionDescriptor memory d, uint64 ceiling) internal view returns (bytes memory) {
        return _sign(SIGNER_KEY, mppi.voucherDigest(_id(d), ceiling));
    }

    function _open(uint64 deposit, bytes32 salt) internal returns (IMPPI.SessionDescriptor memory d) {
        d = _descriptor(salt);
        IMPPI.EIP3009Auth memory auth = _auth(d, deposit, keccak256(abi.encode("open", salt)));
        vm.prank(agent);
        mppi.open(d, deposit, keccak256(CATALOG), auth);
    }

    function _settle(IMPPI.SessionDescriptor memory d, uint64 ceiling, uint64 capture) internal {
        bytes memory signature = _voucher(d, ceiling);
        vm.prank(operator);
        mppi.settle(d, ceiling, capture, signature);
    }

    function _close(IMPPI.SessionDescriptor memory d, uint64 ceiling, uint64 capture) internal {
        bytes memory signature = _voucher(d, ceiling);
        vm.prank(operator);
        mppi.close(d, ceiling, capture, signature);
    }

    function _assertSession(IMPPI.Session memory actual, IMPPI.Session memory expected) internal pure {
        assertEq(abi.encode(actual), abi.encode(expected), "all session fields");
    }

    function _assertStatus(IMPPI.SessionDescriptor memory d, IMPPI.SessionStatus status) internal view {
        assertEq(uint8(mppi.getSession(_id(d)).status), uint8(status));
    }

    function _mpLogs(Vm.Log[] memory logs) internal view returns (uint256 count) {
        for (uint256 i; i < logs.length; ++i) {
            if (logs[i].emitter == address(mppi)) ++count;
        }
    }

    function _fails(address caller, bytes memory data, bytes4 selector) internal {
        vm.prank(caller);
        (bool success, bytes memory reason) = address(mppi).call(data);
        assertFalse(success, "expected rejection");
        assertEq(reason, abi.encodeWithSelector(selector));
    }

    function _timelockCall(bytes memory data, bytes32 salt) internal {
        vm.prank(safe);
        timelock.schedule(address(mppi), 0, data, bytes32(0), salt, 60);
        vm.warp(block.timestamp + 60);
        vm.prank(stranger);
        timelock.execute(address(mppi), 0, data, bytes32(0), salt);
    }
}
