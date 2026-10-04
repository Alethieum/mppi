// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {MPPIBase, IMPPI, Vm} from "./MPPIBase.t.sol";

contract MPPIBatchTest is MPPIBase {
    function testMixedBatchPreservesOriginalCallerAndSuccessfulNeighbors() public {
        (IMPPI.SessionDescriptor[] memory ds, uint64[] memory ceilings, uint64[] memory captures, bytes[] memory sigs) =
            _batch(3);
        sigs[1] = hex"badbad";
        vm.recordLogs();
        vm.prank(operator);
        mppi.settleBatch(ds, ceilings, captures, sigs);
        assertEq(_mpLogs(vm.getRecordedLogs()), 2);
        assertEq(mppi.getSession(_id(ds[0])).settledCumulative, 10);
        assertEq(mppi.getSession(_id(ds[1])).settledCumulative, 0);
        assertEq(mppi.getSession(_id(ds[2])).settledCumulative, 10);
        assertEq(token.balanceOf(recipient), 20);
        sigs[1] = _voucher(ds[1], 100);
        captures[0] = 20;
        captures[2] = 20;
        vm.prank(stranger, operator);
        mppi.settleBatch(ds, ceilings, captures, sigs);
        assertEq(token.balanceOf(recipient), 20, "tx.origin is not authority");
        vm.prank(recipient);
        mppi.settleBatch(ds, ceilings, captures, sigs);
        assertEq(token.balanceOf(recipient), 50);
    }

    function testBatchHelperRequiresSelfAndEnteredGuard() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        bytes memory payload = abi.encodeCall(IMPPI._settleBatchItem, (operator, d, 100, 10, _voucher(d, 100)));
        _fails(stranger, payload, IMPPI.InvalidBatchContext.selector);
        _fails(operator, payload, IMPPI.InvalidBatchContext.selector);
        _fails(address(mppi), payload, IMPPI.InvalidBatchContext.selector);
        assertEq(mppi.getSession(_id(d)).settledCumulative, 0);
    }

    function testBatchTransferFailureRollsBackOnlyItsItem() public {
        (IMPPI.SessionDescriptor[] memory ds, uint64[] memory ceilings, uint64[] memory captures, bytes[] memory sigs) =
            _batch(3);
        // Give the middle session a distinct frozen destination so only it fails.
        address badRecipient = makeAddr("blocked batch recipient");
        registry.setProvider(PROVIDER_ID + 1, operator, badRecipient, CATALOG);
        IMPPI.SessionDescriptor memory bad = _descriptor(bytes32("bad"));
        bad.providerId = PROVIDER_ID + 1;
        bad.recipient = badRecipient;
        IMPPI.EIP3009Auth memory auth = _auth(bad, 100, bytes32("bad"));
        vm.prank(agent);
        mppi.open(bad, 100, keccak256(CATALOG), auth);
        ds[1] = bad;
        sigs[1] = _voucher(bad, 100);
        IMPPI.Session memory before_ = mppi.getSession(_id(bad));
        token.setFailures(false, badRecipient, true, 0);
        vm.warp(block.timestamp + 1);
        vm.recordLogs();
        vm.prank(operator);
        mppi.settleBatch(ds, ceilings, captures, sigs);
        assertEq(_mpLogs(vm.getRecordedLogs()), 2);
        _assertSession(mppi.getSession(_id(bad)), before_);
        assertEq(token.balanceOf(badRecipient), 0);
        assertEq(token.balanceOf(recipient), 20);
        assertEq(token.transferCalls(), 2);
        token.setFailures(false, address(0), false, 0);
        _settle(bad, 100, 10);
        assertEq(token.balanceOf(badRecipient), 10);
    }

    function testDuplicateBatchTargetsUseInputOrderAndNoopSilence() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        IMPPI.SessionDescriptor[] memory ds = new IMPPI.SessionDescriptor[](5);
        uint64[] memory ceilings = new uint64[](5);
        uint64[] memory captures = new uint64[](5);
        bytes[] memory sigs = new bytes[](5);
        uint64[5] memory sequence = [uint64(10), 10, 20, 5, 30];
        bytes memory signature = _voucher(d, 100);
        for (uint256 i; i < 5; ++i) {
            ds[i] = d;
            ceilings[i] = 100;
            captures[i] = sequence[i];
            sigs[i] = signature;
        }
        vm.recordLogs();
        vm.prank(operator);
        mppi.settleBatch(ds, ceilings, captures, sigs);
        assertEq(_mpLogs(vm.getRecordedLogs()), 3);
        assertEq(token.transferCalls(), 3);
        assertEq(token.balanceOf(recipient), 30);
        assertEq(mppi.getSession(_id(d)).settledCumulative, 30);
        IMPPI.Session memory before_ = mppi.getSession(_id(d));
        vm.warp(block.timestamp + 1);
        vm.recordLogs();
        vm.prank(operator);
        mppi.settleBatch(ds, ceilings, captures, sigs);
        assertEq(vm.getRecordedLogs().length, 0);
        _assertSession(mppi.getSession(_id(d)), before_);
    }

    function testBatchEmptyAndEveryArrayShapeMismatch() public {
        (IMPPI.SessionDescriptor[] memory ds, uint64[] memory ceilings, uint64[] memory captures, bytes[] memory sigs) =
            _batch(1);
        _fails(
            operator,
            abi.encodeCall(IMPPI.settleBatch, (new IMPPI.SessionDescriptor[](0), ceilings, captures, sigs)),
            IMPPI.ArrayLengthMismatch.selector
        );
        _fails(
            operator,
            abi.encodeCall(IMPPI.settleBatch, (ds, new uint64[](0), captures, sigs)),
            IMPPI.ArrayLengthMismatch.selector
        );
        _fails(
            operator,
            abi.encodeCall(IMPPI.settleBatch, (ds, ceilings, new uint64[](0), sigs)),
            IMPPI.ArrayLengthMismatch.selector
        );
        _fails(
            operator,
            abi.encodeCall(IMPPI.settleBatch, (ds, ceilings, captures, new bytes[](0))),
            IMPPI.ArrayLengthMismatch.selector
        );
        vm.recordLogs();
        mppi.settleBatch(new IMPPI.SessionDescriptor[](0), new uint64[](0), new uint64[](0), new bytes[](0));
        assertEq(vm.getRecordedLogs().length, 0);
        vm.prank(operator);
        mppi.settleBatch(ds, ceilings, captures, sigs);
        assertEq(token.balanceOf(recipient), 10, "outer failure restored guard");
    }

    function testMissingClosedClosingExpiredAndMalformedBatchItemsAreSkipped() public {
        (IMPPI.SessionDescriptor[] memory ds, uint64[] memory ceilings, uint64[] memory captures, bytes[] memory sigs) =
            _batch(6);
        // Leave the first item absent by substituting a never-opened salt.
        ds[0].salt = bytes32("missing");
        sigs[0] = _voucher(ds[0], 100);
        _close(ds[1], 0, 0);
        vm.prank(agent);
        mppi.requestClose(ds[2]);
        vm.warp(block.timestamp + 24 hours);
        // All old open items have expired; replace the last two with fresh ones.
        ds[4] = _open(100, bytes32("malformed"));
        ds[5] = _open(100, bytes32("valid"));
        sigs[4] = hex"badbad";
        sigs[5] = _voucher(ds[5], 100);
        vm.recordLogs();
        vm.prank(operator);
        mppi.settleBatch(ds, ceilings, captures, sigs);
        assertEq(_mpLogs(vm.getRecordedLogs()), 1);
        for (uint256 i; i < 5; ++i) {
            assertEq(mppi.getSession(_id(ds[i])).settledCumulative, 0);
        }
        assertEq(mppi.getSession(_id(ds[5])).settledCumulative, 10);
        assertEq(mppi.getSession(_id(ds[0])).deposit, 0);
    }

    function testLargeHostileRevertPayloadIsIsolated() public {
        (IMPPI.SessionDescriptor[] memory ds, uint64[] memory ceilings, uint64[] memory captures, bytes[] memory sigs) =
            _batch(2);
        address badRecipient = makeAddr("large revert recipient");
        registry.setProvider(PROVIDER_ID + 1, operator, badRecipient, CATALOG);
        IMPPI.SessionDescriptor memory bad = _descriptor(bytes32("large revert"));
        bad.providerId = PROVIDER_ID + 1;
        bad.recipient = badRecipient;
        IMPPI.EIP3009Auth memory auth = _auth(bad, 100, bytes32("large revert"));
        vm.prank(agent);
        mppi.open(bad, 100, keccak256(CATALOG), auth);
        ds[0] = bad;
        sigs[0] = _voucher(bad, 100);
        token.setFailures(false, badRecipient, false, 131_072);
        vm.prank(operator);
        mppi.settleBatch(ds, ceilings, captures, sigs);
        assertEq(mppi.getSession(_id(bad)).settledCumulative, 0);
        assertEq(mppi.getSession(_id(ds[1])).settledCumulative, 10);
    }

    function testOuterMalformedAbiAndGasExhaustionRollback() public {
        (IMPPI.SessionDescriptor[] memory ds, uint64[] memory ceilings, uint64[] memory captures, bytes[] memory sigs) =
            _batch(40);
        bytes memory payload = abi.encodeCall(IMPPI.settleBatch, (ds, ceilings, captures, sigs));
        vm.prank(operator);
        (bool success,) = address(mppi).call{gas: 150_000}(payload);
        assertFalse(success, "bounded outer budget must exhaust on this fixture");
        for (uint256 i; i < ds.length; ++i) {
            assertEq(mppi.getSession(_id(ds[i])).settledCumulative, 0);
        }
        assertEq(token.balanceOf(recipient), 0);
        vm.prank(operator);
        (success,) = address(mppi).call(abi.encodePacked(IMPPI.settleBatch.selector, bytes32(uint256(1))));
        assertFalse(success);
        _settle(ds[0], 100, 10);
        assertEq(token.balanceOf(recipient), 10);
    }

    function testBatchAbiRejectsWideArrayAmounts() public {
        (IMPPI.SessionDescriptor[] memory ds,,, bytes[] memory sigs) = _batch(1);
        uint256[] memory wide = new uint256[](1);
        wide[0] = uint256(type(uint64).max) + 1;
        uint64[] memory normal = new uint64[](1);
        normal[0] = 10;
        bytes[2] memory calls = [
            abi.encodeWithSelector(IMPPI.settleBatch.selector, ds, wide, normal, sigs),
            abi.encodeWithSelector(IMPPI.settleBatch.selector, ds, normal, wide, sigs)
        ];
        for (uint256 i; i < calls.length; ++i) {
            vm.prank(operator);
            (bool success,) = address(mppi).call(calls[i]);
            assertFalse(success);
            assertEq(mppi.getSession(_id(ds[0])).settledCumulative, 0);
        }
    }

    function _batch(uint256 count)
        private
        returns (
            IMPPI.SessionDescriptor[] memory ds,
            uint64[] memory ceilings,
            uint64[] memory captures,
            bytes[] memory sigs
        )
    {
        ds = new IMPPI.SessionDescriptor[](count);
        ceilings = new uint64[](count);
        captures = new uint64[](count);
        sigs = new bytes[](count);
        for (uint256 i; i < count; ++i) {
            ds[i] = _open(100, bytes32(i));
            ceilings[i] = 100;
            captures[i] = 10;
            sigs[i] = _voucher(ds[i], 100);
        }
    }
}
