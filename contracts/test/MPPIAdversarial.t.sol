// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {MPPIBase, MPPI, IMPPI, MockUSDC, Vm, ERC1967Proxy} from "./MPPIBase.t.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {SafeCast} from "@openzeppelin/contracts/utils/math/SafeCast.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";

contract WalletForwarderFixture {
    function execute(address target, bytes calldata data) external returns (bool, bytes memory) {
        return target.call(data);
    }
}

contract NonpayableRecipientFixture {
    fallback() external {
        revert("wallet fallback must not run");
    }
}

contract MPPIAdversarialTest is MPPIBase {
    // Pinned OpenZeppelin ReentrancyGuard.sol:35-37, not MPPI application storage.
    bytes32 private constant GUARD_SLOT = 0x9b779b17422d0df92223018b32b4d1fa46e071723d6817e2486d003becc55f00;
    IMPPI.Session private observed;
    uint256 private observations;

    function observe(bytes32 id) external {
        require(msg.sender == USDC, "token callback only");
        observed = mppi.getSession(id);
        ++observations;
    }

    function assertFirstOpenGuard(bytes calldata reentry) external {
        require(msg.sender == USDC, "token callback only");
        assertEq(uint256(vm.load(address(mppi), GUARD_SLOT)), 2, "guard entered before first external funding call");
        (bool success, bytes memory reason) = address(mppi).call(reentry);
        assertFalse(success);
        assertEq(reason, abi.encodeWithSelector(ReentrancyGuard.ReentrancyGuardReentrantCall.selector));
    }

    function testFreshProxyGuardFirstOpenAndFailedOpenRollback() public {
        bytes32 derivedSlot = keccak256(abi.encode(uint256(keccak256("openzeppelin.storage.ReentrancyGuard")) - 1))
            & ~bytes32(uint256(0xff));
        assertEq(derivedSlot, GUARD_SLOT);
        assertEq(
            uint256(vm.load(address(implementation), GUARD_SLOT)), 1, "constructor initialized implementation only"
        );
        for (uint256 i; i < 2; ++i) {
            if (i == 1) mppi = MPPI(address(new ERC1967Proxy(address(implementation), _initializer())));
            assertEq(uint256(vm.load(address(mppi), GUARD_SLOT)), 0, "fresh initialized proxy guard starts at zero");
            IMPPI.SessionDescriptor memory d = _descriptor(bytes32(i + 1));
            IMPPI.EIP3009Auth memory auth = _auth(d, 100, bytes32(i + 1));
            token.setHook(
                address(this),
                abi.encodeCall(this.assertFirstOpenGuard, (abi.encodeCall(IMPPI.releaseIdle, (d)))),
                true,
                false,
                true
            );
            if (i == 1) {
                token.setFailures(true, address(0), false, 0);
                // This exact token error also proves the earlier callback checks
                // passed; a failed callback assertion would instead be HookFailed.
                _fails(
                    agent,
                    abi.encodeCall(IMPPI.open, (d, 100, keccak256(CATALOG), auth)),
                    MockUSDC.ForcedFailure.selector
                );
                assertEq(uint256(vm.load(address(mppi), GUARD_SLOT)), 0, "revert restores the original zero guard");
                assertEq(mppi.getSession(_id(d)).deposit, 0);
                assertFalse(token.authorizationState(agent, auth.nonce));
                token.setFailures(false, address(0), false, 0);
            }
            vm.prank(agent);
            mppi.open(d, 100, keccak256(CATALOG), auth);
            assertTrue(token.hookSucceeded());
            assertEq(uint256(vm.load(address(mppi), GUARD_SLOT)), 1, "successful completion leaves NOT_ENTERED");
            assertEq(
                uint256(vm.load(address(implementation), GUARD_SLOT)), 1, "proxy never changes implementation guard"
            );
            assertEq(mppi.getSession(_id(d)).deposit, 100);
        }
    }

    function testFundingArgumentsAreExactForOpenAndTopUp() public {
        IMPPI.SessionDescriptor memory d = _descriptor(0);
        IMPPI.EIP3009Auth memory auth = _auth(d, 123_456, bytes32("open"));
        vm.expectCall(
            USDC,
            abi.encodeCall(
                MockUSDC.receiveWithAuthorization,
                (agent, address(mppi), uint256(123_456), auth.validAfter, auth.validBefore, auth.nonce, auth.signature)
            ),
            1
        );
        vm.prank(agent);
        mppi.open(d, 123_456, keccak256(CATALOG), auth);
        auth = _auth(d, 9, bytes32("topup"));
        vm.expectCall(
            USDC,
            abi.encodeCall(
                MockUSDC.receiveWithAuthorization,
                (agent, address(mppi), uint256(9), auth.validAfter, auth.validBefore, auth.nonce, auth.signature)
            ),
            1
        );
        vm.prank(agent);
        mppi.topUp(d, 9, auth);
        assertEq(mppi.getSession(_id(d)).deposit, 123_465);
    }

    function testFundingInvalidSignaturesRejectWithoutCredit() public {
        IMPPI.SessionDescriptor memory d = _descriptor(0);
        for (uint256 i; i < 8; ++i) {
            IMPPI.EIP3009Auth memory auth = _auth(d, 100, bytes32(i));
            bytes32 digest =
                token.authorizationDigest(agent, address(mppi), 100, auth.validAfter, auth.validBefore, auth.nonce);
            if (i == 0) auth.signature = _sign(SIGNER_KEY, digest);
            if (i == 1) auth.signature = hex"1234";
            if (i == 2) {
                auth.signature = _sign(
                    AGENT_KEY,
                    token.authorizationDigest(agent, stranger, 100, auth.validAfter, auth.validBefore, auth.nonce)
                );
            }
            if (i == 3) {
                auth.signature = _sign(
                    AGENT_KEY,
                    token.authorizationDigest(agent, address(mppi), 101, auth.validAfter, auth.validBefore, auth.nonce)
                );
            }
            if (i == 4) {
                auth.signature = _sign(
                    AGENT_KEY,
                    token.authorizationDigest(
                        stranger, address(mppi), 100, auth.validAfter, auth.validBefore, auth.nonce
                    )
                );
            }
            if (i == 5) vm.chainId(block.chainid + 1);
            if (i == 6) auth.signature = _sign(AGENT_KEY, keccak256(abi.encodePacked("wrong type", digest)));
            if (i == 7) auth.nonce = bytes32("substituted");
            uint256 before_ = token.balanceOf(agent);
            _fails(
                agent,
                abi.encodeCall(IMPPI.open, (d, 100, keccak256(CATALOG), auth)),
                MockUSDC.InvalidAuthorization.selector
            );
            assertEq(mppi.getSession(_id(d)).deposit, 0);
            assertFalse(token.authorizationState(agent, auth.nonce));
            assertEq(token.balanceOf(agent), before_);
        }
    }

    function testFundingAuthorizationStrictTimeBoundaries() public {
        IMPPI.SessionDescriptor memory d = _descriptor(0);
        uint256 now_ = vm.getBlockTimestamp();
        IMPPI.EIP3009Auth memory auth = _authAt(d, 1, 0, now_, now_ + 2);
        _fails(
            agent,
            abi.encodeCall(IMPPI.open, (d, 1, keccak256(CATALOG), auth)),
            MockUSDC.AuthorizationNotYetValid.selector
        );
        vm.warp(now_ + 2);
        _fails(
            agent, abi.encodeCall(IMPPI.open, (d, 1, keccak256(CATALOG), auth)), MockUSDC.AuthorizationExpired.selector
        );
        vm.warp(now_ + 1);
        vm.prank(agent);
        mppi.open(d, 1, keccak256(CATALOG), auth);
        assertTrue(token.authorizationState(agent, 0));
    }

    function testUsedAndCancelledTokenNoncesReject() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        bytes32 used = keccak256(abi.encode("open", bytes32(0)));
        IMPPI.EIP3009Auth memory auth = _auth(d, 1, used);
        _fails(agent, abi.encodeCall(IMPPI.topUp, (d, 1, auth)), MockUSDC.AuthorizationUsed.selector);
        token.cancelAuthorization(agent, bytes32("cancelled"));
        auth = _auth(d, 1, bytes32("cancelled"));
        _fails(agent, abi.encodeCall(IMPPI.topUp, (d, 1, auth)), MockUSDC.AuthorizationUsed.selector);
        assertEq(mppi.getSession(_id(d)).deposit, 100);
    }

    function testFundingPullPrecedesCreditAndFailureRollsBack() public {
        IMPPI.SessionDescriptor memory d = _descriptor(0);
        bytes32 id = _id(d);
        token.setHook(address(this), abi.encodeCall(this.observe, (id)), true, false, true);
        d = _open(100, 0);
        assertEq(observed.deposit, 0);
        IMPPI.EIP3009Auth memory auth = _auth(d, 10, bytes32("topup"));
        vm.prank(agent);
        mppi.topUp(d, 10, auth);
        assertEq(observed.deposit, 100);
        assertEq(observations, 2);
        token.setFailures(true, address(0), false, 0);
        auth = _auth(d, 10, bytes32("failed"));
        IMPPI.Session memory before_ = mppi.getSession(id);
        uint256 agentBalance = token.balanceOf(agent);
        uint256 escrowBalance = token.balanceOf(address(mppi));
        vm.recordLogs();
        _fails(agent, abi.encodeCall(IMPPI.topUp, (d, 10, auth)), MockUSDC.ForcedFailure.selector);
        _assertSession(mppi.getSession(id), before_);
        assertEq(token.balanceOf(agent), agentBalance);
        assertEq(token.balanceOf(address(mppi)), escrowBalance);
        assertFalse(token.authorizationState(agent, auth.nonce));
        assertEq(observations, 2);
        // Recorded traces may include reverted LOG opcodes; committed receipt logs
        // are checked independently by the Anvil transaction harness.
        assertEq(_mpLogs(vm.getRecordedLogs()), 0);
        IMPPI.SessionDescriptor memory missing = _descriptor(bytes32("failed open"));
        auth = _auth(missing, 10, bytes32("failed open"));
        _fails(
            agent, abi.encodeCall(IMPPI.open, (missing, 10, keccak256(CATALOG), auth)), MockUSDC.ForcedFailure.selector
        );
        assertEq(mppi.getSession(_id(missing)).deposit, 0);
        assertFalse(token.authorizationState(agent, auth.nonce));
    }

    function testVoucherDomainTypeSignerAndMalformedRejection() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        bytes32 id = _id(d);
        bytes32 domainType =
            keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
        bytes32 correctType = keccak256("Voucher(bytes32 sessionId,uint64 cumulativeAmount)");
        for (uint256 i; i < 11; ++i) {
            bytes memory signature;
            bytes32 domain = keccak256(
                abi.encode(
                    domainType,
                    keccak256("MPPI"),
                    keccak256("1"),
                    i == 0 ? block.chainid + 1 : block.chainid,
                    i == 1 ? address(implementation) : address(mppi)
                )
            );
            bytes32 hash = keccak256(
                abi.encode(
                    i == 2 ? keccak256("Voucher(bytes32 sessionId,uint256 cumulativeAmount)") : correctType,
                    i == 3 ? bytes32(uint256(id) ^ 1) : id,
                    uint64(100)
                )
            );
            signature = _sign(i == 4 ? AGENT_KEY : SIGNER_KEY, keccak256(abi.encodePacked(hex"1901", domain, hash)));
            if (i == 5) signature = new bytes(0);
            if (i == 6) signature = new bytes(64);
            if (i == 7) signature = new bytes(66);
            if (i == 8) signature[64] = bytes1(uint8(29));
            if (i == 9) {
                (uint8 v, bytes32 r, bytes32 s) = vm.sign(SIGNER_KEY, mppi.voucherDigest(id, 100));
                uint256 order = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141;
                signature = abi.encodePacked(r, bytes32(order - uint256(s)), uint8(v == 27 ? 28 : 27));
            }
            if (i == 10) signature = new bytes(65);
            _fails(operator, abi.encodeCall(IMPPI.settle, (d, 100, 1, signature)), IMPPI.InvalidVoucher.selector);
            _fails(operator, abi.encodeCall(IMPPI.close, (d, 100, 1, signature)), IMPPI.InvalidVoucher.selector);
        }
        assertEq(mppi.getSession(id).settledCumulative, 0);
    }

    function testEveryPaymentEntryRejectsFundingAndPayoutReentry() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        for (uint256 i; i < 9; ++i) {
            bytes memory data = _entryData(d, i);
            token.setHook(address(mppi), data, true, true, false);
            IMPPI.EIP3009Auth memory auth = _auth(d, 1, bytes32(i + 1));
            vm.prank(agent);
            mppi.topUp(d, 1, auth);
            assertFalse(token.hookSucceeded());
            bytes4 expected =
                i == 8 ? IMPPI.InvalidBatchContext.selector : ReentrancyGuard.ReentrancyGuardReentrantCall.selector;
            assertEq(token.hookResult(), abi.encodeWithSelector(expected));
            _settle(d, 100, uint64(i + 1));
            assertFalse(token.hookSucceeded());
            assertEq(token.hookResult(), abi.encodeWithSelector(expected));
        }
        token.setHook(address(0), "", false, false, false);
        _close(d, 100, 10);
        _assertStatus(d, IMPPI.SessionStatus.Closed);
    }

    function testOutgoingEffectsAreVisibleBeforeTransfers() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        token.setHook(address(this), abi.encodeCall(this.observe, (_id(d))), false, true, true);
        _settle(d, 100, 40);
        assertEq(observed.settledCumulative, 40);
        assertEq(uint8(observed.status), uint8(IMPPI.SessionStatus.Open));
        _close(d, 100, 60);
        assertEq(observed.settledCumulative, 60);
        assertEq(uint8(observed.status), uint8(IMPPI.SessionStatus.Closed));
        assertEq(observations, 3);
        for (uint256 i; i < 2; ++i) {
            d = _open(100, bytes32(i + 1));
            token.setHook(address(this), abi.encodeCall(this.observe, (_id(d))), false, true, true);
            if (i == 0) {
                vm.prank(agent);
                mppi.requestClose(d);
                vm.warp(block.timestamp + 1 hours);
                vm.prank(agent);
                mppi.withdraw(d);
            } else {
                vm.warp(block.timestamp + 24 hours);
                mppi.releaseIdle(d);
            }
            assertEq(uint8(observed.status), uint8(IMPPI.SessionStatus.Closed));
        }
    }

    function testFalseAndRevertingTransfersRollbackCapture() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        IMPPI.Session memory before_ = mppi.getSession(_id(d));
        bytes memory signature = _voucher(d, 100);
        vm.warp(block.timestamp + 10);
        for (uint256 i; i < 2; ++i) {
            token.setFailures(false, recipient, i == 0, 0);
            if (i == 0) vm.expectRevert(abi.encodeWithSelector(SafeERC20.SafeERC20FailedOperation.selector, USDC));
            else vm.expectRevert(MockUSDC.ForcedFailure.selector);
            vm.prank(operator);
            mppi.settle(d, 100, 40, signature);
            _assertSession(mppi.getSession(_id(d)), before_);
            assertEq(token.balanceOf(recipient), 0);
            assertEq(token.balanceOf(address(mppi)), 100);
            assertEq(token.transferCalls(), 0);
        }
    }

    function testCloseEachFailedLegRollsBackEverything() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        _settle(d, 100, 20);
        IMPPI.Session memory before_ = mppi.getSession(_id(d));
        uint256 agentBalance = token.balanceOf(agent);
        for (uint256 i; i < 2; ++i) {
            token.setFailures(false, i == 0 ? recipient : agent, false, 0);
            _fails(
                operator, abi.encodeCall(IMPPI.close, (d, 100, 40, _voucher(d, 100))), MockUSDC.ForcedFailure.selector
            );
            _assertSession(mppi.getSession(_id(d)), before_);
            assertEq(token.balanceOf(recipient), 20);
            assertEq(token.balanceOf(agent), agentBalance);
            assertEq(token.balanceOf(address(mppi)), 80);
            assertEq(token.transferCalls(), 1);
        }
        token.setFailures(false, address(0), false, 0);
        _close(d, 100, 40);
        assertEq(token.balanceOf(recipient), 40);
        assertEq(token.balanceOf(agent), agentBalance + 60);
    }

    function testRevertedWithdrawAndIdleReleasePreserveRefund() public {
        for (uint256 i; i < 2; ++i) {
            token.setFailures(false, address(0), false, 0);
            IMPPI.SessionDescriptor memory d = _open(100, bytes32(i));
            if (i == 0) {
                vm.prank(agent);
                mppi.requestClose(d);
            }
            vm.warp(block.timestamp + 24 hours);
            IMPPI.Session memory before_ = mppi.getSession(_id(d));
            uint256 balance = token.balanceOf(agent);
            token.setFailures(false, agent, false, 0);
            bytes memory data = i == 0 ? abi.encodeCall(IMPPI.withdraw, (d)) : abi.encodeCall(IMPPI.releaseIdle, (d));
            _fails(agent, data, MockUSDC.ForcedFailure.selector);
            _assertSession(mppi.getSession(_id(d)), before_);
            assertEq(token.balanceOf(agent), balance);
            token.setFailures(false, address(0), false, 0);
            vm.prank(agent);
            (bool success,) = address(mppi).call(data);
            assertTrue(success);
        }
    }

    function testZeroLegCombinationsSkipRestrictedToken() public {
        // Four close combinations: both positive, payout only, refund only, neither.
        for (uint256 i; i < 4; ++i) {
            token.setRestrictions(false, agent, false);
            token.setRestrictions(false, recipient, false);
            IMPPI.SessionDescriptor memory d = _open(100, bytes32(i));
            uint64 capture = i == 0 ? 40 : (i == 2 ? 0 : 100);
            if (i == 3) _settle(d, 100, 100);
            if (i == 1) token.setRestrictions(false, agent, true);
            if (i == 2) token.setRestrictions(false, recipient, true);
            if (i == 3) token.setRestrictions(true, address(0), false);
            uint256 calls = token.transferCalls();
            _close(d, 100, capture);
            assertEq(token.transferCalls() - calls, i == 0 ? 2 : (i == 3 ? 0 : 1));
            _assertStatus(d, IMPPI.SessionStatus.Closed);
        }
        token.setRestrictions(false, address(0), false);
        for (uint256 i; i < 2; ++i) {
            IMPPI.SessionDescriptor memory d = _open(100, bytes32(i + 4));
            _settle(d, 100, 100);
            if (i == 0) {
                vm.prank(agent);
                mppi.requestClose(d);
            }
            vm.warp(block.timestamp + 24 hours);
            uint256 calls = token.transferCalls();
            token.setRestrictions(true, address(0), false);
            if (i == 0) {
                vm.prank(agent);
                mppi.withdraw(d);
            } else {
                mppi.releaseIdle(d);
            }
            assertEq(token.transferCalls(), calls);
            _assertStatus(d, IMPPI.SessionStatus.Closed);
            token.setRestrictions(false, address(0), false);
        }
    }

    function testRestrictionsOnRequiredLegsRevert() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        address[3] memory accounts = [agent, recipient, address(mppi)];
        for (uint256 i; i < accounts.length; ++i) {
            token.setRestrictions(false, accounts[i], true);
            _fails(
                operator, abi.encodeCall(IMPPI.close, (d, 100, 40, _voucher(d, 100))), MockUSDC.TokenRestricted.selector
            );
            assertEq(mppi.getSession(_id(d)).settledCumulative, 0);
            token.setRestrictions(false, accounts[i], false);
        }
    }

    function testNonpayableRecipientDoesNotNeedFallback() public {
        recipient = address(new NonpayableRecipientFixture());
        registry.setProvider(PROVIDER_ID, operator, recipient, CATALOG);
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        _close(d, 100, 40);
        assertEq(token.balanceOf(recipient), 40);
    }

    function testUnsolicitedTokenAndNativeBalanceDoNotCreateCredit() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        token.mint(address(mppi), 777);
        vm.deal(address(mppi), 13 ether);
        assertEq(mppi.getSession(_id(d)).deposit, 100);
        assertEq(mppi.getSession(_id(_descriptor(bytes32("absent")))).deposit, 0);
        _close(d, 100, 40);
        assertEq(token.balanceOf(address(mppi)), 777);
        assertEq(address(mppi).balance, 13 ether);
    }

    function testRevertedCaptureRetryUsesCurrentDeadline() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        uint256 deadline = vm.getBlockTimestamp() + 24 hours;
        token.setFailures(false, recipient, false, 0);
        vm.warp(deadline - 1);
        _fails(operator, abi.encodeCall(IMPPI.settle, (d, 100, 40, _voucher(d, 100))), MockUSDC.ForcedFailure.selector);
        token.setFailures(false, address(0), false, 0);
        vm.warp(deadline);
        _fails(
            operator, abi.encodeCall(IMPPI.settle, (d, 100, 40, _voucher(d, 100))), IMPPI.ClaimDeadlinePassed.selector
        );
        mppi.releaseIdle(d);
    }

    function testNestedWalletReceiptNeedsSuccessfulInnerOpening() public {
        WalletForwarderFixture forwarder = new WalletForwarderFixture();
        vm.etch(agent, address(forwarder).code);
        IMPPI.SessionDescriptor memory d = _descriptor(0);
        IMPPI.EIP3009Auth memory auth = _auth(d, 100, 0);
        (bool success,) = WalletForwarderFixture(agent)
            .execute(address(mppi), abi.encodeCall(IMPPI.open, (d, 100, keccak256(CATALOG), auth)));
        assertTrue(success);
        assertEq(mppi.getSession(_id(d)).deposit, 100);
        IMPPI.SessionDescriptor memory failed = _descriptor(bytes32("failed"));
        auth = _auth(failed, 100, bytes32("failed"));
        token.setFailures(true, address(0), false, 0);
        vm.recordLogs();
        (success,) = WalletForwarderFixture(agent)
            .execute(address(mppi), abi.encodeCall(IMPPI.open, (failed, 100, keccak256(CATALOG), auth)));
        assertFalse(success);
        assertEq(mppi.getSession(_id(failed)).deposit, 0);
        assertEq(_mpLogs(vm.getRecordedLogs()), 0);
    }

    function testAbiRejectsAmountsAboveUint64() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        IMPPI.Session memory before_ = mppi.getSession(_id(d));
        IMPPI.EIP3009Auth memory auth = _auth(d, 1, bytes32("bad abi"));
        uint256 tooLarge = uint256(type(uint64).max) + 1;
        bytes[6] memory calls = [
            abi.encodeWithSelector(IMPPI.open.selector, d, tooLarge, keccak256(CATALOG), auth),
            abi.encodeWithSelector(IMPPI.topUp.selector, d, tooLarge, auth),
            abi.encodeWithSelector(IMPPI.settle.selector, d, tooLarge, uint256(1), bytes("")),
            abi.encodeWithSelector(IMPPI.settle.selector, d, uint256(1), tooLarge, bytes("")),
            abi.encodeWithSelector(IMPPI.close.selector, d, tooLarge, uint256(1), bytes("")),
            abi.encodeWithSelector(IMPPI.close.selector, d, uint256(1), tooLarge, bytes(""))
        ];
        for (uint256 i; i < calls.length; ++i) {
            vm.prank(agent);
            (bool success,) = address(mppi).call(calls[i]);
            assertFalse(success);
            _assertSession(mppi.getSession(_id(d)), before_);
        }
    }

    function testTimestampNarrowingNeverWraps() public {
        IMPPI.SessionDescriptor memory d = _descriptor(0);
        uint256 timestamp = uint256(type(uint64).max) + 1;
        vm.warp(timestamp);
        IMPPI.EIP3009Auth memory auth = _auth(d, 1, 0);
        vm.expectRevert(abi.encodeWithSelector(SafeCast.SafeCastOverflowedUintDowncast.selector, uint8(64), timestamp));
        vm.prank(agent);
        mppi.open(d, 1, keccak256(CATALOG), auth);
        assertFalse(token.authorizationState(agent, auth.nonce));
    }

    function _entryData(IMPPI.SessionDescriptor memory d, uint256 which) private view returns (bytes memory) {
        IMPPI.EIP3009Auth memory auth = _auth(d, 1, 0);
        if (which == 0) return abi.encodeCall(IMPPI.open, (d, 1, keccak256(CATALOG), auth));
        if (which == 1) return abi.encodeCall(IMPPI.topUp, (d, 1, auth));
        if (which == 2) return abi.encodeCall(IMPPI.settle, (d, 100, 1, bytes("")));
        if (which == 3) return abi.encodeCall(IMPPI.close, (d, 100, 1, bytes("")));
        if (which == 4) return abi.encodeCall(IMPPI.requestClose, (d));
        if (which == 5) return abi.encodeCall(IMPPI.withdraw, (d));
        if (which == 6) return abi.encodeCall(IMPPI.releaseIdle, (d));
        if (which == 7) {
            return abi.encodeCall(
                IMPPI.settleBatch, (new IMPPI.SessionDescriptor[](0), new uint64[](0), new uint64[](0), new bytes[](0))
            );
        }
        return abi.encodeCall(IMPPI._settleBatchItem, (operator, d, 100, 1, bytes("")));
    }
}
