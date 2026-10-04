// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {MPPIBase, MPPI, IMPPI, MockUSDC, MockIdentityRegistry, Vm, ERC1967Proxy} from "./MPPIBase.t.sol";
import {SafeCast} from "@openzeppelin/contracts/utils/math/SafeCast.sol";

contract MPPILifecycleTest is MPPIBase {
    function testFuzzOpenPositiveDeposit(uint64 deposit) public {
        vm.assume(deposit > 0);
        uint256 balance = token.balanceOf(agent);
        IMPPI.SessionDescriptor memory d = _descriptor(bytes32("open"));
        bytes32 id = _id(d);
        IMPPI.EIP3009Auth memory auth = _auth(d, deposit, bytes32("nonce"));
        vm.expectEmit(address(mppi));
        emit IMPPI.SessionOpened(
            id, agent, sessionSigner, PROVIDER_ID, recipient, operator, USDC, d.salt, deposit, keccak256(CATALOG)
        );
        vm.prank(agent);
        mppi.open(d, deposit, keccak256(CATALOG), auth);
        _assertSession(
            mppi.getSession(id),
            IMPPI.Session(deposit, 0, IMPPI.SessionStatus.Open, 0, uint64(block.timestamp), keccak256(CATALOG))
        );
        assertEq(token.balanceOf(agent), balance - deposit);
        assertEq(token.balanceOf(address(mppi)), deposit);
        assertTrue(token.authorizationState(agent, auth.nonce));
    }

    function testOpenGuardsRunBeforeToken() public {
        vm.expectCall(USDC, abi.encodeWithSelector(MockUSDC.receiveWithAuthorization.selector), uint64(0));
        for (uint8 case_; case_ < 9; ++case_) {
            IMPPI.SessionDescriptor memory d = _descriptor(bytes32(uint256(case_)));
            uint64 deposit = 1;
            bytes32 hash = keccak256(CATALOG);
            bytes4 expectedError;
            address caller = agent;
            registry.setProvider(PROVIDER_ID, operator, recipient, CATALOG);
            if (case_ == 0) {
                deposit = 0;
                expectedError = IMPPI.ZeroDeposit.selector;
            }
            if (case_ == 1) {
                caller = stranger;
                expectedError = IMPPI.NotAgent.selector;
            }
            if (case_ == 2) {
                d.sessionSigner = address(0);
                expectedError = IMPPI.InvalidSessionSigner.selector;
            }
            if (case_ == 3) {
                d.token = stranger;
                expectedError = IMPPI.TokenNotAllowed.selector;
            }
            if (case_ == 4) {
                d.recipient = stranger;
                expectedError = IMPPI.RecipientMismatch.selector;
            }
            if (case_ == 5) {
                d.operator = stranger;
                expectedError = IMPPI.OperatorMismatch.selector;
            }
            if (case_ == 6) {
                registry.setProvider(PROVIDER_ID, operator, recipient, "");
                expectedError = IMPPI.EmptyCatalog.selector;
            }
            if (case_ == 7) {
                hash = bytes32(0);
                expectedError = IMPPI.RateCardHashMismatch.selector;
            }
            if (case_ == 8) {
                registry.setProvider(PROVIDER_ID, operator, address(0), CATALOG);
                d.recipient = address(0);
                expectedError = IMPPI.RecipientMismatch.selector;
            }
            IMPPI.EIP3009Auth memory auth = _auth(d, deposit, bytes32(uint256(case_)));
            _fails(caller, abi.encodeCall(IMPPI.open, (d, deposit, hash, auth)), expectedError);
            assertEq(token.fundingCalls(), 0);
            assertEq(mppi.getSession(_id(d)).deposit, 0);
            assertFalse(token.authorizationState(agent, auth.nonce));
        }
    }

    function testCallerGuardPrecedesZeroSigner() public {
        vm.expectCall(USDC, abi.encodeWithSelector(MockUSDC.receiveWithAuthorization.selector), uint64(0));
        IMPPI.SessionDescriptor memory d = _descriptor(0);
        d.sessionSigner = address(0);
        IMPPI.EIP3009Auth memory auth = _auth(d, 1, 0);
        _fails(stranger, abi.encodeCall(IMPPI.open, (d, 1, keccak256(CATALOG), auth)), IMPPI.NotAgent.selector);
    }

    function testMissingProviderBubblesRegistryError() public {
        vm.expectCall(USDC, abi.encodeWithSelector(MockUSDC.receiveWithAuthorization.selector), uint64(0));
        IMPPI.SessionDescriptor memory d = _descriptor(0);
        d.providerId = PROVIDER_ID + 1;
        IMPPI.EIP3009Auth memory auth = _auth(d, 1, 0);
        vm.expectRevert(abi.encodeWithSelector(MockIdentityRegistry.MissingProvider.selector, d.providerId));
        vm.prank(agent);
        mppi.open(d, 1, keccak256(CATALOG), auth);
    }

    function testContractDoesNotParseCatalog() public {
        bytes[2] memory catalogs = [bytes("[]"), bytes("not JSON")];
        for (uint256 i; i < catalogs.length; ++i) {
            registry.setProvider(PROVIDER_ID, operator, recipient, catalogs[i]);
            IMPPI.SessionDescriptor memory d = _descriptor(bytes32(i));
            IMPPI.EIP3009Auth memory auth = _auth(d, 1, bytes32(i));
            vm.prank(agent);
            mppi.open(d, 1, keccak256(catalogs[i]), auth);
            assertEq(mppi.getSession(_id(d)).initialRateCardHash, keccak256(catalogs[i]));
        }
    }

    function testSignerWithCodeAndWalletSignerEqualityAreValid() public {
        vm.etch(sessionSigner, hex"00");
        IMPPI.SessionDescriptor memory first = _open(10, bytes32("code"));
        _settle(first, 10, 1);
        IMPPI.SessionDescriptor memory d = _descriptor(bytes32("same"));
        d.sessionSigner = agent;
        IMPPI.EIP3009Auth memory auth = _auth(d, 10, bytes32("same"));
        vm.prank(agent);
        mppi.open(d, 10, keccak256(CATALOG), auth);
        bytes memory signature = _sign(AGENT_KEY, mppi.voucherDigest(_id(d), 10));
        vm.prank(recipient);
        mppi.settle(d, 10, 1, signature);
        assertEq(token.balanceOf(recipient), 2);
    }

    function testSessionIdBindsEveryDescriptorFieldProxyAndChain() public {
        IMPPI.SessionDescriptor memory original = _descriptor(bytes32("salt"));
        bytes32 id = _id(original);
        assertEq(
            id,
            keccak256(
                abi.encode(
                    original.agent,
                    original.sessionSigner,
                    original.providerId,
                    original.recipient,
                    original.operator,
                    original.token,
                    original.salt,
                    address(mppi),
                    block.chainid
                )
            )
        );
        for (uint256 i; i < 7; ++i) {
            IMPPI.SessionDescriptor memory changed = _descriptor(bytes32("salt"));
            if (i == 0) changed.agent = stranger;
            if (i == 1) changed.sessionSigner = stranger;
            if (i == 2) changed.providerId++;
            if (i == 3) changed.recipient = stranger;
            if (i == 4) changed.operator = stranger;
            if (i == 5) changed.token = stranger;
            if (i == 6) changed.salt = bytes32("another");
            assertNotEq(_id(changed), id);
        }
        MPPI another = MPPI(address(new ERC1967Proxy(address(implementation), _initializer())));
        assertNotEq(another.computeSessionId(original), id);
        assertNotEq(implementation.computeSessionId(original), id);
        bytes32 digest = mppi.voucherDigest(id, 1);
        vm.chainId(block.chainid + 1);
        assertNotEq(_id(original), id);
        assertNotEq(mppi.voucherDigest(id, 1), digest);
    }

    function testRegistryChangesDoNotRebindSession() public {
        vm.roll(123);
        IMPPI.SessionDescriptor memory d = _descriptor(0);
        IMPPI.EIP3009Auth memory auth = _auth(d, 100, bytes32("open"));
        bytes memory openingCatalog = bytes("catalog published immediately before opening");
        bytes32 openingHash = keccak256(openingCatalog);
        registry.setProvider(PROVIDER_ID, operator, recipient, openingCatalog);
        // The rejected stale quote and valid retry share one authorization:
        // exactly one funding call proves the rejected attempt never reached USDC.
        vm.expectCall(
            USDC,
            abi.encodeCall(
                MockUSDC.receiveWithAuthorization,
                (agent, address(mppi), uint256(100), auth.validAfter, auth.validBefore, auth.nonce, auth.signature)
            ),
            uint64(1)
        );
        _fails(
            agent, abi.encodeCall(IMPPI.open, (d, 100, keccak256(CATALOG), auth)), IMPPI.RateCardHashMismatch.selector
        );
        assertEq(mppi.getSession(_id(d)).deposit, 0);
        assertFalse(token.authorizationState(agent, auth.nonce));
        vm.expectEmit(address(mppi));
        emit IMPPI.SessionOpened(
            _id(d), agent, sessionSigner, PROVIDER_ID, recipient, operator, USDC, d.salt, 100, openingHash
        );
        vm.prank(agent);
        mppi.open(d, 100, openingHash, auth);
        registry.setProvider(PROVIDER_ID, stranger, stranger, "changed catalog");
        assertEq(vm.getBlockNumber(), 123, "both catalog updates and opening occur in the same block");
        vm.expectCall(address(registry), abi.encodeWithSelector(MockIdentityRegistry.ownerOf.selector), uint64(0));
        vm.expectCall(
            address(registry), abi.encodeWithSelector(MockIdentityRegistry.getAgentWallet.selector), uint64(0)
        );
        vm.expectCall(address(registry), abi.encodeWithSelector(MockIdentityRegistry.getMetadata.selector), uint64(0));
        auth = _auth(d, 20, bytes32("topup"));
        vm.prank(agent);
        mppi.topUp(d, 20, auth);
        _settle(d, 100, 40);
        assertEq(token.balanceOf(recipient), 40);
        assertEq(token.balanceOf(stranger), 0);
        assertEq(mppi.getSession(_id(d)).initialRateCardHash, openingHash);
        _fails(stranger, abi.encodeCall(IMPPI.close, (d, 40, 40, bytes(""))), IMPPI.NotOperatorOrRecipient.selector);
    }

    function testFuzzTopUpUsesLifetimeDeposit(uint64 first, uint64 increment) public {
        vm.assume(first > 0 && increment > 0);
        IMPPI.SessionDescriptor memory d = _open(first, 0);
        _settle(d, first, first);
        vm.warp(block.timestamp + 1);
        IMPPI.EIP3009Auth memory auth = _auth(d, increment, bytes32("topup"));
        uint256 sum = uint256(first) + increment;
        if (sum > type(uint64).max) {
            vm.expectCall(USDC, abi.encodeWithSelector(MockUSDC.receiveWithAuthorization.selector), uint64(0));
            vm.expectRevert(abi.encodeWithSelector(SafeCast.SafeCastOverflowedUintDowncast.selector, uint8(64), sum));
            vm.prank(agent);
            mppi.topUp(d, increment, auth);
            assertFalse(token.authorizationState(agent, auth.nonce));
            assertEq(mppi.getSession(_id(d)).deposit, first);
        } else {
            bytes32 id = _id(d);
            vm.expectEmit(address(mppi));
            emit IMPPI.ToppedUp(id, increment, uint64(sum));
            vm.prank(agent);
            mppi.topUp(d, increment, auth);
            IMPPI.Session memory state = mppi.getSession(id);
            assertEq(state.deposit, sum);
            assertEq(state.settledCumulative, first);
            assertEq(state.lastActivity, block.timestamp);
            assertEq(token.balanceOf(address(mppi)), increment);
        }
    }

    function testTopUpAndRequestCloseRequireAgent() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        vm.expectCall(USDC, abi.encodeWithSelector(MockUSDC.receiveWithAuthorization.selector), uint64(0));
        IMPPI.EIP3009Auth memory auth = _auth(d, 1, bytes32("topup"));
        _fails(stranger, abi.encodeCall(IMPPI.topUp, (d, 1, auth)), IMPPI.NotAgent.selector);
        _fails(stranger, abi.encodeCall(IMPPI.requestClose, (d)), IMPPI.NotAgent.selector);
        _fails(agent, abi.encodeCall(IMPPI.topUp, (d, 0, auth)), IMPPI.ZeroTopUp.selector);
    }

    function testCaptureAmountIsUsageNotVoucherCeiling() public {
        IMPPI.SessionDescriptor memory d = _open(10_000_000, 0);
        _settle(d, 5_510_000, 5_000_000);
        bytes memory signature = _voucher(d, 5_510_000);
        bytes32 id = _id(d);
        vm.warp(block.timestamp + 1);
        vm.expectEmit(address(mppi));
        emit IMPPI.Settled(id, agent, recipient, 5_510_000, 5_420_000, 420_000, 5_420_000);
        vm.prank(recipient);
        mppi.settle(d, 5_510_000, 5_420_000, signature);
        assertEq(token.balanceOf(recipient), 5_420_000);
        assertEq(mppi.getSession(id).lastActivity, block.timestamp);
        assertEq(uint8(mppi.getSession(id).status), uint8(IMPPI.SessionStatus.Open));
        IMPPI.Session memory before_ = mppi.getSession(id);
        uint256 transfers = token.transferCalls();
        vm.warp(block.timestamp + 10);
        vm.recordLogs();
        vm.prank(operator);
        mppi.settle(d, 5_510_000, 5_420_000, signature);
        assertEq(vm.getRecordedLogs().length, 0);
        _assertSession(mppi.getSession(id), before_);
        assertEq(token.transferCalls(), transfers);
    }

    function testEqualCaptureStillValidatesVoucherAndBounds() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        _settle(d, 100, 40);
        _fails(operator, abi.encodeCall(IMPPI.settle, (d, 100, 40, bytes(""))), IMPPI.InvalidVoucher.selector);
        _fails(
            operator, abi.encodeCall(IMPPI.settle, (d, 101, 40, _voucher(d, 101))), IMPPI.AmountExceedsDeposit.selector
        );
        _fails(
            operator, abi.encodeCall(IMPPI.settle, (d, 39, 40, _voucher(d, 39))), IMPPI.CaptureAmountInvalid.selector
        );
        _fails(
            operator, abi.encodeCall(IMPPI.settle, (d, 100, 39, _voucher(d, 100))), IMPPI.CaptureAmountInvalid.selector
        );
        _fails(
            stranger,
            abi.encodeCall(IMPPI.settle, (d, 100, 40, _voucher(d, 100))),
            IMPPI.NotOperatorOrRecipient.selector
        );
    }

    function testFuzzBoundedCaptureConservesFunds(uint64 deposit, uint64 ceilingSeed, uint64 captureSeed) public {
        deposit = uint64(bound(deposit, 1, type(uint64).max));
        uint64 ceiling = uint64(bound(ceilingSeed, 0, deposit));
        uint64 capture = uint64(bound(captureSeed, 0, ceiling));
        IMPPI.SessionDescriptor memory d = _open(deposit, 0);
        _settle(d, ceiling, capture);
        assertEq(token.balanceOf(recipient), capture);
        assertEq(token.balanceOf(address(mppi)), deposit - capture);
        assertEq(mppi.getSession(_id(d)).settledCumulative, capture);
    }

    function testFuzzCloseAboveDepositCeilingOnlyChargesCapture(uint64 ceilingSeed, uint64 captureSeed) public {
        uint64 deposit = 1_000_000;
        uint64 ceiling = uint64(bound(ceilingSeed, deposit + 1, type(uint64).max));
        uint64 capture = uint64(bound(captureSeed, 0, deposit));
        IMPPI.SessionDescriptor memory d = _open(deposit, 0);
        uint256 agentBalance = token.balanceOf(agent);
        _close(d, ceiling, capture);
        assertEq(token.balanceOf(recipient), capture);
        assertEq(token.balanceOf(agent), agentBalance + deposit - capture);
        assertEq(token.balanceOf(address(mppi)), 0);
        _assertStatus(d, IMPPI.SessionStatus.Closed);
    }

    function testCloseChecksBoundsAndAuthorityEvenWithoutSignature() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        _settle(d, 100, 40);
        _fails(operator, abi.encodeCall(IMPPI.close, (d, 100, 39, bytes(""))), IMPPI.CaptureAmountInvalid.selector);
        _fails(operator, abi.encodeCall(IMPPI.close, (d, 39, 40, bytes(""))), IMPPI.CaptureAmountInvalid.selector);
        _fails(operator, abi.encodeCall(IMPPI.close, (d, 200, 101, bytes(""))), IMPPI.AmountExceedsDeposit.selector);
        _fails(stranger, abi.encodeCall(IMPPI.close, (d, 100, 40, bytes(""))), IMPPI.NotOperatorOrRecipient.selector);
        _fails(operator, abi.encodeCall(IMPPI.close, (d, 100, 41, bytes(""))), IMPPI.InvalidVoucher.selector);
    }

    function testRefundOnlyCloseIgnoresSignatureAtAndAfterBothDeadlines() public {
        for (uint8 case_; case_ < 6; ++case_) {
            IMPPI.SessionDescriptor memory d = _open(100, bytes32(uint256(case_)));
            _settle(d, 100, 40);
            bool closing = case_ >= 3;
            if (closing) {
                vm.prank(agent);
                mppi.requestClose(d);
            }
            uint256 deadline = block.timestamp + (closing ? 1 hours : 24 hours);
            vm.warp(deadline + case_ % 3 - 1);
            vm.prank(recipient);
            mppi.close(d, 100, 40, hex"badbad");
            _assertStatus(d, IMPPI.SessionStatus.Closed);
        }
    }

    function testAdditionalCloseCaptureHasStrictOpenAndClosingDeadline() public {
        for (uint8 case_; case_ < 6; ++case_) {
            IMPPI.SessionDescriptor memory d = _open(100, bytes32(uint256(case_)));
            bool closing = case_ >= 3;
            if (closing) {
                vm.prank(agent);
                mppi.requestClose(d);
            }
            uint256 deadline = block.timestamp + (closing ? 1 hours : 24 hours);
            vm.warp(deadline + case_ % 3 - 1);
            bytes memory signature = _voucher(d, 100);
            if (case_ % 3 == 0) {
                vm.prank(operator);
                mppi.close(d, 100, 1, signature);
            } else {
                _fails(
                    operator, abi.encodeCall(IMPPI.close, (d, 100, 1, signature)), IMPPI.ClaimDeadlinePassed.selector
                );
            }
        }
    }

    function testIdleDeadlineRejectsTopUpSettleAndRequestCloseWithoutRevival() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        _settle(d, 100, 10);
        vm.expectCall(USDC, abi.encodeWithSelector(MockUSDC.receiveWithAuthorization.selector), uint64(0));
        IMPPI.Session memory before_ = mppi.getSession(_id(d));
        for (uint256 offset; offset < 2; ++offset) {
            vm.warp(uint256(before_.lastActivity) + 24 hours + offset);
            IMPPI.EIP3009Auth memory auth = _auth(d, 1, bytes32("topup"));
            _fails(agent, abi.encodeCall(IMPPI.topUp, (d, 1, auth)), IMPPI.ClaimDeadlinePassed.selector);
            _fails(
                operator,
                abi.encodeCall(IMPPI.settle, (d, 100, 10, _voucher(d, 100))),
                IMPPI.ClaimDeadlinePassed.selector
            );
            _fails(agent, abi.encodeCall(IMPPI.requestClose, (d)), IMPPI.ClaimDeadlinePassed.selector);
            _assertSession(mppi.getSession(_id(d)), before_);
            assertFalse(token.authorizationState(agent, auth.nonce));
        }
    }

    function testCloseRequestRestartsFullHourAndBlocksOpenOperations() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        vm.expectCall(USDC, abi.encodeWithSelector(MockUSDC.receiveWithAuthorization.selector), uint64(0));
        vm.warp(block.timestamp + 24 hours - 1);
        bytes32 id = _id(d);
        vm.expectEmit(address(mppi));
        emit IMPPI.CloseRequested(id, uint64(block.timestamp + 1 hours));
        vm.prank(agent);
        mppi.requestClose(d);
        IMPPI.Session memory state = mppi.getSession(id);
        assertEq(state.lastActivity, block.timestamp);
        assertEq(state.closeRequestedAt, block.timestamp);
        IMPPI.EIP3009Auth memory auth = _auth(d, 1, bytes32("topup"));
        _fails(agent, abi.encodeCall(IMPPI.requestClose, (d)), IMPPI.SessionNotOpen.selector);
        _fails(agent, abi.encodeCall(IMPPI.topUp, (d, 1, auth)), IMPPI.SessionNotOpen.selector);
        _fails(operator, abi.encodeCall(IMPPI.settle, (d, 100, 0, _voucher(d, 100))), IMPPI.SessionNotOpen.selector);
        vm.warp(block.timestamp + 1 hours - 1);
        _fails(agent, abi.encodeCall(IMPPI.withdraw, (d)), IMPPI.DisputeWindowOpen.selector);
        _close(d, 100, 1);
    }

    function testWithdrawExactDeadlineAndAgentAuthority() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        _settle(d, 100, 40);
        _fails(agent, abi.encodeCall(IMPPI.withdraw, (d)), IMPPI.SessionNotClosing.selector);
        vm.prank(agent);
        mppi.requestClose(d);
        _fails(stranger, abi.encodeCall(IMPPI.withdraw, (d)), IMPPI.NotAgent.selector);
        vm.warp(block.timestamp + 1 hours);
        bytes32 id = _id(d);
        vm.expectEmit(address(mppi));
        emit IMPPI.SessionClosed(id, 40, 60);
        vm.prank(agent);
        mppi.withdraw(d);
        _assertStatus(d, IMPPI.SessionStatus.Closed);
        assertEq(token.balanceOf(address(mppi)), 0);
    }

    function testIdleReleaseIsPermissionlessAtExactDeadlineInEitherState() public {
        for (uint256 i; i < 2; ++i) {
            IMPPI.SessionDescriptor memory d = _open(100, bytes32(i));
            if (i == 1) {
                vm.prank(agent);
                mppi.requestClose(d);
            }
            vm.warp(block.timestamp + 24 hours - 1);
            _fails(stranger, abi.encodeCall(IMPPI.releaseIdle, (d)), IMPPI.NotIdle.selector);
            vm.warp(block.timestamp + 1);
            bytes32 id = _id(d);
            vm.expectEmit(address(mppi));
            emit IMPPI.IdleReleased(id, 100);
            vm.prank(stranger);
            mppi.releaseIdle(d);
            _assertStatus(d, IMPPI.SessionStatus.Closed);
        }
    }

    function testExistenceAndClosedGuardsPrecedeAmountsAndTimes() public {
        IMPPI.SessionDescriptor memory d = _descriptor(0);
        // Zero-value funding is forbidden throughout; the valid one-unit open below is allowed.
        vm.expectCall(
            USDC,
            abi.encodeWithSelector(MockUSDC.receiveWithAuthorization.selector, agent, address(mppi), uint256(0)),
            uint64(0)
        );
        assertEq(mppi.getSession(_id(d)).deposit, 0);
        _allMutationsReject(d, IMPPI.SessionNotFound.selector);
        d = _open(1, 0);
        vm.expectCall(USDC, abi.encodeWithSelector(MockUSDC.receiveWithAuthorization.selector), uint64(0));
        _close(d, 0, 0);
        _allMutationsReject(d, IMPPI.SessionIsClosed.selector);
        IMPPI.EIP3009Auth memory auth = _auth(d, 1, bytes32("reopen"));
        _fails(agent, abi.encodeCall(IMPPI.open, (d, 1, keccak256(CATALOG), auth)), IMPPI.SessionExists.selector);
        assertEq(mppi.getSession(_id(d)).deposit, 1);
    }

    function testDuplicateOpenRejectedWhileOpenAndClosing() public {
        IMPPI.SessionDescriptor memory d = _open(1, 0);
        vm.expectCall(USDC, abi.encodeWithSelector(MockUSDC.receiveWithAuthorization.selector), uint64(0));
        IMPPI.EIP3009Auth memory auth = _auth(d, 1, bytes32("duplicate"));
        bytes memory data = abi.encodeCall(IMPPI.open, (d, 1, keccak256(CATALOG), auth));
        _fails(agent, data, IMPPI.SessionExists.selector);
        vm.prank(agent);
        mppi.requestClose(d);
        _fails(agent, data, IMPPI.SessionExists.selector);
        assertFalse(token.authorizationState(agent, auth.nonce));
    }

    function testMaximumUint64DepositCaptureAndZeroRefund() public {
        IMPPI.SessionDescriptor memory d = _open(type(uint64).max, bytes32("maximum"));
        _settle(d, type(uint64).max, type(uint64).max);
        uint256 calls = token.transferCalls();
        bytes32 id = _id(d);
        vm.expectEmit(address(mppi));
        emit IMPPI.SessionClosed(id, type(uint64).max, 0);
        vm.prank(operator);
        mppi.close(d, type(uint64).max, type(uint64).max, "");
        assertEq(token.transferCalls(), calls);
        assertEq(mppi.getSession(id).deposit, type(uint64).max);
        assertEq(token.balanceOf(recipient), type(uint64).max);
    }

    function testQualifyingActivityBeforeCutoffExtendsBeyondOneDayLifetime() public {
        IMPPI.SessionDescriptor memory d = _open(100, 0);
        uint256 openedAt = vm.getBlockTimestamp();
        for (uint256 i; i < 3; ++i) {
            vm.warp(block.timestamp + 24 hours - 1);
            if (i % 2 == 0) {
                IMPPI.EIP3009Auth memory auth = _auth(d, 1, bytes32(i + 1));
                vm.prank(agent);
                mppi.topUp(d, 1, auth);
            } else {
                _settle(d, 100, 1);
            }
            assertEq(mppi.getSession(_id(d)).lastActivity, block.timestamp);
        }
        assertGt(block.timestamp, openedAt + 2 days);
        vm.warp(block.timestamp + 24 hours - 1);
        vm.prank(agent);
        mppi.requestClose(d);
        assertEq(mppi.getSession(_id(d)).lastActivity, block.timestamp);
    }

    function _allMutationsReject(IMPPI.SessionDescriptor memory d, bytes4 selector) private {
        IMPPI.EIP3009Auth memory auth = _auth(d, 0, 0);
        _fails(agent, abi.encodeCall(IMPPI.topUp, (d, 0, auth)), selector);
        _fails(operator, abi.encodeCall(IMPPI.settle, (d, 0, 0, bytes(""))), selector);
        _fails(operator, abi.encodeCall(IMPPI.close, (d, 0, 0, bytes(""))), selector);
        _fails(agent, abi.encodeCall(IMPPI.requestClose, (d)), selector);
        _fails(agent, abi.encodeCall(IMPPI.withdraw, (d)), selector);
        _fails(stranger, abi.encodeCall(IMPPI.releaseIdle, (d)), selector);
    }
}
