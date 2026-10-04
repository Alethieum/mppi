// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {MPPIBase, MPPI, IMPPI, MockUSDC, ERC1967Proxy, TimelockController, Vm} from "./MPPIBase.t.sol";
import {Initializable} from "@openzeppelin/contracts/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts/proxy/utils/UUPSUpgradeable.sol";
import {ERC1967Utils} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Utils.sol";
import {IAccessControl} from "@openzeppelin/contracts/access/IAccessControl.sol";
import {IERC1967} from "@openzeppelin/contracts/interfaces/IERC1967.sol";

/// @dev Compatible test successor; adds no storage or production authority.
contract MPPIUpgradeFixture is MPPI {
    function version() external pure returns (uint256) {
        return 2;
    }
}

contract InvalidUUIDFixture {
    function proxiableUUID() external pure returns (bytes32) {
        return bytes32(0);
    }
}

contract RejectingUUIDFixture {
    function proxiableUUID() external pure returns (bytes32) {
        revert("not a UUPS implementation");
    }
}

contract MPPIAdminTest is MPPIBase {
    bytes32 private constant IMPLEMENTATION_SLOT = 0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc;

    function testAtomicInitializationAndDisabledImplementation() public {
        assertEq(uint8(mppi.pauseLevel()), 0);
        vm.expectRevert(Initializable.InvalidInitialization.selector);
        implementation.initialize(address(registry), guardian, safe, address(timelock));
        vm.expectRevert(Initializable.InvalidInitialization.selector);
        mppi.initialize(address(registry), guardian, safe, address(timelock));
        assertEq(address(uint160(uint256(vm.load(address(mppi), IMPLEMENTATION_SLOT)))), address(implementation));
        for (uint256 i; i < 4; ++i) {
            address[4] memory arguments = [address(registry), guardian, safe, address(timelock)];
            arguments[i] = address(0);
            bytes memory data =
                abi.encodeCall(IMPPI.initialize, (arguments[0], arguments[1], arguments[2], arguments[3]));
            vm.expectRevert(abi.encodeWithSelector(IMPPI.InvalidInitializationAddress.selector, address(0)));
            new ERC1967Proxy(address(implementation), data);
        }
    }

    function testEveryAdminAuthorityCheckPrecedesTransitionCheck() public {
        bytes4[4] memory selectors =
            [IMPPI.pause.selector, IMPPI.unpause.selector, IMPPI.pauseAll.selector, IMPPI.unpauseAll.selector];
        address[4] memory authorities = [guardian, safe, address(timelock), address(timelock)];
        for (uint256 level; level < 3; ++level) {
            _freshProxyAtLevel(level);
            for (uint256 i; i < selectors.length; ++i) {
                vm.prank(stranger);
                (bool success, bytes memory reason) = address(mppi).call(abi.encodeWithSelector(selectors[i]));
                assertFalse(success);
                assertEq(reason, abi.encodeWithSelector(IMPPI.UnauthorizedCaller.selector, stranger, authorities[i]));
                assertEq(uint8(mppi.pauseLevel()), level);
            }
        }
    }

    function testAllTwelveAuthorizedPauseTransitions() public {
        bytes4[4] memory selectors =
            [IMPPI.pause.selector, IMPPI.unpause.selector, IMPPI.pauseAll.selector, IMPPI.unpauseAll.selector];
        address[4] memory authorities = [guardian, safe, address(timelock), address(timelock)];
        for (uint256 level; level < 3; ++level) {
            for (uint256 call_; call_ < 4; ++call_) {
                _freshProxyAtLevel(level);
                bool permitted = (call_ == 0 && level == 0) || (call_ == 1 && level == 1) || (call_ == 2 && level < 2)
                    || (call_ == 3 && level > 0);
                uint256 next = call_ == 0 ? 1 : (call_ == 2 ? 2 : 0);
                vm.recordLogs();
                if (permitted) {
                    vm.expectEmit(address(mppi));
                    emit IMPPI.PauseLevelChanged(authorities[call_], IMPPI.PauseLevel(level), IMPPI.PauseLevel(next));
                    vm.prank(authorities[call_]);
                    (bool success,) = address(mppi).call(abi.encodeWithSelector(selectors[call_]));
                    assertTrue(success);
                    assertEq(uint8(mppi.pauseLevel()), next);
                    assertEq(_mpLogs(vm.getRecordedLogs()), 1);
                } else {
                    bytes4 expected = (call_ == 1 || call_ == 3) && level == 0
                        ? IMPPI.ExpectedPause.selector
                        : IMPPI.EnforcedPause.selector;
                    _fails(authorities[call_], abi.encodeWithSelector(selectors[call_]), expected);
                    assertEq(uint8(mppi.pauseLevel()), level);
                    assertEq(vm.getRecordedLogs().length, 0);
                }
            }
        }
    }

    function testPaymentEntriesAndEmptyBatchRejectBothPauseLevels() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        _settle(d, 100, 10);
        vm.expectCall(USDC, abi.encodeWithSelector(MockUSDC.receiveWithAuthorization.selector), uint64(0));
        IMPPI.Session memory before_ = mppi.getSession(_id(d));
        IMPPI.EIP3009Auth memory auth = _auth(d, 1, bytes32("topup"));
        for (uint256 level = 1; level <= 2; ++level) {
            if (level == 1) {
                vm.prank(guardian);
                mppi.pause();
            } else {
                vm.prank(address(timelock));
                mppi.pauseAll();
            }
            _fails(agent, abi.encodeCall(IMPPI.open, (d, 1, keccak256(CATALOG), auth)), IMPPI.EnforcedPause.selector);
            _fails(agent, abi.encodeCall(IMPPI.topUp, (d, 1, auth)), IMPPI.EnforcedPause.selector);
            _fails(operator, abi.encodeCall(IMPPI.settle, (d, 100, 10, _voucher(d, 100))), IMPPI.EnforcedPause.selector);
            _fails(operator, abi.encodeCall(IMPPI.close, (d, 10, 10, bytes(""))), IMPPI.EnforcedPause.selector);
            _fails(
                operator,
                abi.encodeCall(
                    IMPPI.settleBatch,
                    (new IMPPI.SessionDescriptor[](0), new uint64[](0), new uint64[](0), new bytes[](0))
                ),
                IMPPI.EnforcedPause.selector
            );
            IMPPI.SessionDescriptor[] memory ds = new IMPPI.SessionDescriptor[](1);
            ds[0] = d;
            uint64[] memory amounts = new uint64[](1);
            bytes[] memory signatures = new bytes[](1);
            _fails(
                operator,
                abi.encodeCall(IMPPI.settleBatch, (ds, amounts, amounts, signatures)),
                IMPPI.EnforcedPause.selector
            );
            _assertSession(mppi.getSession(_id(d)), before_);
            assertNotEq(mppi.voucherDigest(_id(d), 100), bytes32(0));
        }
    }

    function testExitOperationsAllowedUnderPaymentsAndBlockedUnderAll() public {
        IMPPI.SessionDescriptor memory closing = _open(100, bytes32("closing"));
        IMPPI.SessionDescriptor memory idle = _open(100, bytes32("idle"));
        IMPPI.SessionDescriptor memory zeroRefund = _open(100, bytes32("zero"));
        _settle(zeroRefund, 100, 100);
        vm.prank(guardian);
        mppi.pause();
        vm.prank(agent);
        mppi.requestClose(closing);
        vm.prank(agent);
        mppi.requestClose(zeroRefund);
        vm.prank(address(timelock));
        mppi.pauseAll();
        vm.warp(block.timestamp + 24 hours);
        _fails(agent, abi.encodeCall(IMPPI.requestClose, (idle)), IMPPI.EnforcedPause.selector);
        _fails(agent, abi.encodeCall(IMPPI.withdraw, (closing)), IMPPI.EnforcedPause.selector);
        _fails(agent, abi.encodeCall(IMPPI.withdraw, (zeroRefund)), IMPPI.EnforcedPause.selector);
        _fails(stranger, abi.encodeCall(IMPPI.releaseIdle, (idle)), IMPPI.EnforcedPause.selector);
        vm.prank(address(timelock));
        mppi.unpauseAll();
        vm.prank(guardian);
        mppi.pause();
        vm.prank(agent);
        mppi.withdraw(closing);
        vm.prank(agent);
        mppi.withdraw(zeroRefund);
        vm.prank(stranger);
        mppi.releaseIdle(idle);
        assertEq(token.balanceOf(address(mppi)), 0);
    }

    function testPauseDoesNotExtendDeadlineOrReviveClaims() public {
        for (uint256 level = 1; level <= 2; ++level) {
            IMPPI.SessionDescriptor memory d = _open(100, bytes32(level));
            IMPPI.Session memory before_ = mppi.getSession(_id(d));
            if (level == 1) {
                vm.prank(guardian);
                mppi.pause();
            } else {
                vm.prank(address(timelock));
                mppi.pauseAll();
            }
            vm.warp(block.timestamp + 24 hours);
            if (level == 1) {
                vm.prank(safe);
                mppi.unpause();
            } else {
                vm.prank(address(timelock));
                mppi.unpauseAll();
            }
            _assertSession(mppi.getSession(_id(d)), before_);
            _fails(
                operator,
                abi.encodeCall(IMPPI.settle, (d, 100, 10, _voucher(d, 100))),
                IMPPI.ClaimDeadlinePassed.selector
            );
            mppi.releaseIdle(d);
        }
    }

    function testTimelockRolesDelayCancellationAndPermissionlessExecution() public {
        assertEq(timelock.getMinDelay(), 60);
        assertTrue(timelock.hasRole(timelock.PROPOSER_ROLE(), safe));
        assertTrue(timelock.hasRole(timelock.CANCELLER_ROLE(), safe));
        assertTrue(timelock.hasRole(timelock.EXECUTOR_ROLE(), address(0)));
        assertTrue(timelock.hasRole(timelock.DEFAULT_ADMIN_ROLE(), address(timelock)));
        assertFalse(timelock.hasRole(timelock.DEFAULT_ADMIN_ROLE(), address(this)));
        assertFalse(timelock.hasRole(timelock.DEFAULT_ADMIN_ROLE(), safe));
        bytes memory data = abi.encodeCall(IMPPI.pauseAll, ());
        bytes32 salt = bytes32("pause");
        bytes32 proposerRole = timelock.PROPOSER_ROLE();
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, proposerRole)
        );
        vm.prank(stranger);
        timelock.schedule(address(mppi), 0, data, 0, salt, 60);
        vm.prank(safe);
        timelock.schedule(address(mppi), 0, data, 0, salt, 60);
        bytes32 operation = timelock.hashOperation(address(mppi), 0, data, 0, salt);
        assertEq(uint8(mppi.pauseLevel()), 0);
        vm.expectRevert(
            abi.encodeWithSelector(
                TimelockController.TimelockUnexpectedOperationState.selector, operation, bytes32(uint256(1 << 2))
            )
        );
        timelock.execute(address(mppi), 0, data, 0, salt);
        vm.prank(safe);
        timelock.cancel(operation);
        assertFalse(timelock.isOperation(operation));
        vm.prank(safe);
        timelock.schedule(address(mppi), 0, data, 0, salt, 60);
        vm.warp(block.timestamp + 60);
        vm.prank(stranger);
        timelock.execute(address(mppi), 0, data, 0, salt);
        assertTrue(timelock.isOperationDone(operation));
        assertEq(uint8(mppi.pauseLevel()), 2);
    }

    function testMainnetProfileAndQueuedSelfDelayUpdate() public {
        address[] memory proposers = new address[](1);
        proposers[0] = safe;
        address[] memory executors = new address[](1);
        executors[0] = address(0);
        TimelockController mainnetProfile = new TimelockController(86400, proposers, executors, address(0));
        assertEq(mainnetProfile.getMinDelay(), 86400);
        bytes memory data = abi.encodeCall(TimelockController.updateDelay, (uint256(1)));
        vm.expectRevert(abi.encodeWithSelector(TimelockController.TimelockUnauthorizedCaller.selector, safe));
        vm.prank(safe);
        timelock.updateDelay(1);
        vm.prank(safe);
        timelock.schedule(address(timelock), 0, data, 0, 0, 60);
        assertEq(timelock.getMinDelay(), 60);
        vm.warp(block.timestamp + 60);
        timelock.execute(address(timelock), 0, data, 0, 0);
        assertEq(timelock.getMinDelay(), 1);
    }

    function testUpgradeAuthorityProxyContextAndInvalidImplementation() public {
        MPPIUpgradeFixture successor = new MPPIUpgradeFixture();
        for (uint256 i; i < 4; ++i) {
            address caller = i == 0 ? safe : (i == 1 ? guardian : (i == 2 ? operator : stranger));
            vm.expectRevert(abi.encodeWithSelector(IMPPI.UnauthorizedCaller.selector, caller, address(timelock)));
            vm.prank(caller);
            mppi.upgradeToAndCall(address(successor), "");
        }
        vm.expectRevert(UUPSUpgradeable.UUPSUnauthorizedCallContext.selector);
        implementation.upgradeToAndCall(address(successor), "");
        vm.expectRevert(UUPSUpgradeable.UUPSUnauthorizedCallContext.selector);
        mppi.proxiableUUID();
        assertEq(implementation.proxiableUUID(), IMPLEMENTATION_SLOT);
        // A successful STATICCALL to an EOA has empty returndata. Solidity's
        // caller-side bytes32 decoder reverts outside the UUPS catch clause.
        vm.prank(address(timelock));
        (bool success, bytes memory reason) =
            address(mppi).call(abi.encodeCall(UUPSUpgradeable.upgradeToAndCall, (stranger, bytes(""))));
        assertFalse(success);
        assertEq(reason.length, 0);
        RejectingUUIDFixture rejecting = new RejectingUUIDFixture();
        vm.expectRevert(abi.encodeWithSelector(ERC1967Utils.ERC1967InvalidImplementation.selector, address(rejecting)));
        vm.prank(address(timelock));
        mppi.upgradeToAndCall(address(rejecting), "");
        InvalidUUIDFixture bad = new InvalidUUIDFixture();
        vm.expectRevert(abi.encodeWithSelector(UUPSUpgradeable.UUPSUnsupportedProxiableUUID.selector, bytes32(0)));
        vm.prank(address(timelock));
        mppi.upgradeToAndCall(address(bad), "");
        assertEq(address(uint160(uint256(vm.load(address(mppi), IMPLEMENTATION_SLOT)))), address(implementation));
    }

    function testRealTimelockUpgradeUnderAllPreservesStorageAndOldVoucher() public {
        IMPPI.SessionDescriptor memory open = _open(100, bytes32("open"));
        IMPPI.SessionDescriptor memory closing = _open(200, bytes32("closing"));
        IMPPI.SessionDescriptor memory closed = _open(300, bytes32("closed"));
        _settle(open, 100, 10);
        vm.prank(agent);
        mppi.requestClose(closing);
        _close(closed, 50, 50);
        bytes32 id = _id(open);
        bytes32 digest = mppi.voucherDigest(id, 100);
        bytes memory oldSignature = _voucher(open, 100);
        IMPPI.Session memory openBefore = mppi.getSession(id);
        IMPPI.Session memory closingBefore = mppi.getSession(_id(closing));
        IMPPI.Session memory closedBefore = mppi.getSession(_id(closed));
        _timelockCall(abi.encodeCall(IMPPI.pauseAll, ()), bytes32("pause"));
        MPPIUpgradeFixture successor = new MPPIUpgradeFixture();
        bytes memory data = abi.encodeCall(UUPSUpgradeable.upgradeToAndCall, (address(successor), bytes("")));
        vm.prank(safe);
        timelock.schedule(address(mppi), 0, data, 0, bytes32("upgrade"), 60);
        vm.warp(block.timestamp + 60);
        vm.expectEmit(address(mppi));
        emit IERC1967.Upgraded(address(successor));
        vm.prank(stranger);
        timelock.execute(address(mppi), 0, data, 0, bytes32("upgrade"));
        assertEq(MPPIUpgradeFixture(address(mppi)).version(), 2);
        assertEq(uint8(mppi.pauseLevel()), 2);
        assertEq(_id(open), id);
        assertEq(mppi.voucherDigest(id, 100), digest);
        _assertSession(mppi.getSession(id), openBefore);
        _assertSession(mppi.getSession(_id(closing)), closingBefore);
        _assertSession(mppi.getSession(_id(closed)), closedBefore);
        _timelockCall(abi.encodeCall(IMPPI.unpauseAll, ()), bytes32("recover"));
        vm.prank(operator);
        mppi.settle(open, 100, 20, oldSignature);
        vm.prank(guardian);
        mppi.pause();
        vm.prank(safe);
        mppi.unpause();
        IMPPI.SessionDescriptor memory fresh = _open(1, bytes32("post-upgrade"));
        assertEq(mppi.getSession(_id(fresh)).deposit, 1, "registry address remains usable");
        _fails(
            agent,
            abi.encodeCall(IMPPI.initialize, (address(registry), guardian, safe, address(timelock))),
            Initializable.InvalidInitialization.selector
        );
    }

    function _freshProxyAtLevel(uint256 level) private {
        mppi = MPPI(address(new ERC1967Proxy(address(implementation), _initializer())));
        if (level == 1) {
            vm.prank(guardian);
            mppi.pause();
        }
        if (level == 2) {
            vm.prank(address(timelock));
            mppi.pauseAll();
        }
    }
}
