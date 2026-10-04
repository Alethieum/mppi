// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {Test} from "forge-std/Test.sol";
import {MPPIBase} from "./MPPIBase.t.sol";
import {MPPI} from "../src/MPPI.sol";
import {IMPPI} from "../src/IMPPI.sol";
import {MockUSDC} from "./MockUSDC.sol";

/// @dev Independent expected accounting; successful actions update ghosts from inputs,
/// never by copying observed MPPI state. Synthetic keys only, on the local test EVM.
contract MPPIHandler is Test {
    struct Model {
        IMPPI.SessionDescriptor descriptor;
        IMPPI.Session expected;
        uint256 actor;
    }

    MPPI public immutable target;
    MockUSDC private immutable token;
    address private immutable operator;
    address private immutable recipient;
    address private immutable guardian;
    address private immutable controller;
    bytes32 private immutable catalogHash;
    Model[] private models;
    uint256 private nonce;
    uint256 private paid;
    uint256 private donations;
    uint256[3] private funded;
    uint256[3] private refunded;
    uint256[3] private initial;
    IMPPI.PauseLevel private level;
    uint256 public successfulActions;

    function resetActionCount() external {
        successfulActions = 0;
    }

    /// @dev End-of-run recovery proof. This helper is excluded from fuzz targets.
    function finishCampaign() external {
        assertGt(successfulActions, 0, "fuzz run must execute actions beyond setup");
        if (level != IMPPI.PauseLevel.None) {
            vm.prank(controller);
            target.unpauseAll();
            level = IMPPI.PauseLevel.None;
        }
        vm.warp(block.timestamp + 2 days);
        for (uint256 i; i < models.length; ++i) {
            Model storage model = models[i];
            if (model.expected.status == IMPPI.SessionStatus.Closed) continue;
            vm.prank(address(0xCAFE));
            target.releaseIdle(model.descriptor);
            _closed(model);
        }
        assertEq(token.balanceOf(address(target)), donations, "all session liabilities discharged");
    }

    constructor(
        MPPI target_,
        MockUSDC token_,
        address operator_,
        address recipient_,
        address guardian_,
        address controller_,
        bytes32 catalogHash_
    ) {
        target = target_;
        token = token_;
        operator = operator_;
        recipient = recipient_;
        guardian = guardian_;
        controller = controller_;
        catalogHash = catalogHash_;
        for (uint256 i; i < 3; ++i) {
            address wallet = vm.addr(1000 + i);
            token_.mint(wallet, 1e24);
            initial[i] = token_.balanceOf(wallet);
        }
    }

    function open(uint64 amountSeed, uint256 actorSeed) public {
        if (level != IMPPI.PauseLevel.None || models.length >= 32) return;
        uint256 actor = actorSeed % 3;
        uint64 amount = uint64(bound(amountSeed, 1, 100_000_000));
        IMPPI.SessionDescriptor memory d = IMPPI.SessionDescriptor(
            vm.addr(1000 + actor), vm.addr(2000 + actor), 42, recipient, operator, address(token), bytes32(++nonce)
        );
        IMPPI.EIP3009Auth memory auth = _auth(d, actor, amount);
        vm.prank(d.agent);
        target.open(d, amount, catalogHash, auth);
        models.push(
            Model(d, IMPPI.Session(amount, 0, IMPPI.SessionStatus.Open, 0, uint64(block.timestamp), catalogHash), actor)
        );
        funded[actor] += amount;
        ++successfulActions;
    }

    function topUp(uint256 index, uint64 amountSeed) external {
        if (models.length == 0 || level != IMPPI.PauseLevel.None) return;
        Model storage model = models[index % models.length];
        if (!_activeOpen(model)) return;
        uint64 amount = uint64(bound(amountSeed, 1, 100_000_000));
        IMPPI.EIP3009Auth memory auth = _auth(model.descriptor, model.actor, amount);
        vm.prank(model.descriptor.agent);
        target.topUp(model.descriptor, amount, auth);
        model.expected.deposit += amount;
        model.expected.lastActivity = uint64(block.timestamp);
        funded[model.actor] += amount;
        ++successfulActions;
    }

    function capture(uint256 index, uint64 amountSeed) external {
        if (models.length == 0 || level != IMPPI.PauseLevel.None) return;
        Model storage model = models[index % models.length];
        if (!_activeOpen(model)) return;
        uint64 amount = uint64(bound(amountSeed, model.expected.settledCumulative, model.expected.deposit));
        bytes memory signature = _voucher(model, model.expected.deposit);
        vm.prank(operator);
        target.settle(model.descriptor, model.expected.deposit, amount, signature);
        _captured(model, amount);
        ++successfulActions;
    }

    function batchCapture(uint256 index, uint64 amountSeed) external {
        if (models.length == 0 || level != IMPPI.PauseLevel.None) return;
        Model storage model = models[index % models.length];
        if (!_activeOpen(model)) return;
        uint64 amount = uint64(bound(amountSeed, model.expected.settledCumulative, model.expected.deposit));
        IMPPI.SessionDescriptor[] memory ds = new IMPPI.SessionDescriptor[](3);
        uint64[] memory ceilings = new uint64[](3);
        uint64[] memory captures = new uint64[](3);
        bytes[] memory signatures = new bytes[](3);
        for (uint256 i; i < 3; ++i) {
            ds[i] = model.descriptor;
            ceilings[i] = model.expected.deposit;
            captures[i] = amount;
            signatures[i] = _voucher(model, model.expected.deposit);
        }
        signatures[1] = hex"00";
        vm.prank(operator);
        target.settleBatch(ds, ceilings, captures, signatures);
        _captured(model, amount);
        ++successfulActions;
    }

    function requestClose(uint256 index) external {
        if (models.length == 0 || level == IMPPI.PauseLevel.All) return;
        Model storage model = models[index % models.length];
        if (!_activeOpen(model)) return;
        vm.prank(model.descriptor.agent);
        target.requestClose(model.descriptor);
        model.expected.status = IMPPI.SessionStatus.Closing;
        model.expected.closeRequestedAt = uint64(block.timestamp);
        model.expected.lastActivity = uint64(block.timestamp);
        ++successfulActions;
    }

    function close(uint256 index, uint64 amountSeed) external {
        if (models.length == 0 || level != IMPPI.PauseLevel.None) return;
        Model storage model = models[index % models.length];
        if (model.expected.status == IMPPI.SessionStatus.Closed) return;
        uint256 deadline = model.expected.status == IMPPI.SessionStatus.Open
            ? uint256(model.expected.lastActivity) + 1 days
            : uint256(model.expected.closeRequestedAt) + 1 hours;
        uint64 amount = block.timestamp >= deadline
            ? model.expected.settledCumulative
            : uint64(bound(amountSeed, model.expected.settledCumulative, model.expected.deposit));
        bytes memory signature = _voucher(model, model.expected.deposit);
        vm.prank(operator);
        target.close(model.descriptor, model.expected.deposit, amount, signature);
        paid += amount - model.expected.settledCumulative;
        model.expected.settledCumulative = amount;
        _closed(model);
        ++successfulActions;
    }

    function withdraw(uint256 index) external {
        if (models.length == 0 || level == IMPPI.PauseLevel.All) return;
        Model storage model = models[index % models.length];
        if (
            model.expected.status != IMPPI.SessionStatus.Closing
                || block.timestamp < uint256(model.expected.closeRequestedAt) + 1 hours
        ) return;
        vm.prank(model.descriptor.agent);
        target.withdraw(model.descriptor);
        _closed(model);
        ++successfulActions;
    }

    function releaseIdle(uint256 index) external {
        if (models.length == 0 || level == IMPPI.PauseLevel.All) return;
        Model storage model = models[index % models.length];
        if (
            model.expected.status == IMPPI.SessionStatus.Closed
                || block.timestamp < uint256(model.expected.lastActivity) + 1 days
        ) return;
        vm.prank(address(0xCAFE));
        target.releaseIdle(model.descriptor);
        _closed(model);
        ++successfulActions;
    }

    function advanceTime(uint32 secondsSeed) external {
        vm.warp(block.timestamp + bound(secondsSeed, 0, 30 hours));
    }

    function donate(uint64 amountSeed) external {
        uint256 amount = bound(amountSeed, 1, 1_000_000);
        token.mint(address(this), amount);
        token.transfer(address(target), amount);
        donations += amount;
        ++successfulActions;
    }

    function setPause(uint8 choice) external {
        IMPPI.PauseLevel desired = IMPPI.PauseLevel(choice % 3);
        if (level == desired) return;
        if (desired == IMPPI.PauseLevel.All) {
            vm.prank(controller);
            target.pauseAll();
        } else {
            if (level != IMPPI.PauseLevel.None) {
                vm.prank(controller);
                target.unpauseAll();
            }
            if (desired == IMPPI.PauseLevel.Payments) {
                vm.prank(guardian);
                target.pause();
            }
        }
        level = desired;
        ++successfulActions;
    }

    function assertModel() external view {
        uint256 liabilities;
        for (uint256 i; i < models.length; ++i) {
            Model storage model = models[i];
            IMPPI.Session memory actual = target.getSession(target.computeSessionId(model.descriptor));
            assertEq(abi.encode(actual), abi.encode(model.expected), "independent session model");
            assertGt(actual.deposit, 0, "existence never erased");
            assertLe(actual.settledCumulative, actual.deposit, "bounded capture");
            if (actual.status != IMPPI.SessionStatus.Closed) liabilities += actual.deposit - actual.settledCumulative;
        }
        assertEq(
            token.balanceOf(address(target)), liabilities + donations, "escrow liability plus unallocated donations"
        );
        assertEq(token.balanceOf(recipient), paid, "only cumulative deltas reach frozen recipient");
        for (uint256 i; i < 3; ++i) {
            assertEq(
                token.balanceOf(vm.addr(1000 + i)), initial[i] - funded[i] + refunded[i], "funding wallet conservation"
            );
        }
        assertEq(uint8(target.pauseLevel()), uint8(level), "single global pause state");
    }

    function _activeOpen(Model storage model) private view returns (bool) {
        return model.expected.status == IMPPI.SessionStatus.Open
            && block.timestamp < uint256(model.expected.lastActivity) + 1 days;
    }

    function _captured(Model storage model, uint64 amount) private {
        if (amount > model.expected.settledCumulative) {
            paid += amount - model.expected.settledCumulative;
            model.expected.settledCumulative = amount;
            model.expected.lastActivity = uint64(block.timestamp);
        }
    }

    function _closed(Model storage model) private {
        refunded[model.actor] += model.expected.deposit - model.expected.settledCumulative;
        model.expected.status = IMPPI.SessionStatus.Closed;
    }

    function _auth(IMPPI.SessionDescriptor memory d, uint256 actor, uint64 amount)
        private
        returns (IMPPI.EIP3009Auth memory auth)
    {
        auth = IMPPI.EIP3009Auth(block.timestamp - 1, block.timestamp + 1 days, bytes32(++nonce), "");
        bytes32 digest =
            token.authorizationDigest(d.agent, address(target), amount, auth.validAfter, auth.validBefore, auth.nonce);
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(1000 + actor, digest);
        auth.signature = abi.encodePacked(r, s, v);
    }

    function _voucher(Model storage model, uint64 ceiling) private view returns (bytes memory) {
        bytes32 digest = target.voucherDigest(target.computeSessionId(model.descriptor), ceiling);
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(2000 + model.actor, digest);
        return abi.encodePacked(r, s, v);
    }
}

contract MPPIInvariantTest is MPPIBase {
    MPPIHandler private handler;

    function setUp() public override {
        super.setUp();
        handler = new MPPIHandler(mppi, token, operator, recipient, guardian, address(timelock), keccak256(CATALOG));
        for (uint256 i; i < 3; ++i) {
            handler.open(10_000_000, i);
        }
        handler.resetActionCount();
        bytes4[] memory selectors = new bytes4[](11);
        selectors[0] = handler.open.selector;
        selectors[1] = handler.topUp.selector;
        selectors[2] = handler.capture.selector;
        selectors[3] = handler.batchCapture.selector;
        selectors[4] = handler.requestClose.selector;
        selectors[5] = handler.close.selector;
        selectors[6] = handler.withdraw.selector;
        selectors[7] = handler.releaseIdle.selector;
        selectors[8] = handler.advanceTime.selector;
        selectors[9] = handler.donate.selector;
        selectors[10] = handler.setPause.selector;
        targetSelector(FuzzSelector(address(handler), selectors));
        targetContract(address(handler));
    }

    function invariant_AccountingAuthorityLifecycleAndSolvency() public view {
        handler.assertModel();
    }

    // Foundry's afterInvariant hook runs after each successful invariant sequence:
    // https://getfoundry.sh/forge/invariant-testing#after-invariant-hook
    function afterInvariant() public {
        handler.finishCampaign();
        handler.assertModel();
    }
}
