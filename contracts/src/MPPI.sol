// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {Initializable} from "@openzeppelin/contracts/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts/proxy/utils/UUPSUpgradeable.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {SafeCast} from "@openzeppelin/contracts/utils/math/SafeCast.sol";
import {ECDSA} from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {IMPPI} from "./IMPPI.sol";
import {MPPISigning} from "./MPPISigning.sol";

/// @dev The ERC-8004 reads used at opening. Registry changes do not rebind existing sessions.
interface IMPPIIdentityRegistry {
    function ownerOf(uint256 providerId) external view returns (address);
    function getAgentWallet(uint256 providerId) external view returns (address);
    function getMetadata(uint256 providerId, string calldata key) external view returns (bytes memory);
}

/// @dev Arc USDC's bytes-signature EIP-3009 receiver-authorized funding overload.
interface IMPPIUSDC {
    function receiveWithAuthorization(
        address from,
        address to,
        uint256 value,
        uint256 validAfter,
        uint256 validBefore,
        bytes32 nonce,
        bytes calldata signature
    ) external;
}

/// @notice Machine Payments Protocol for Inference: bounded USDC captures and session refunds.
/// @dev Deployed behind an atomically initialized ERC-1967 proxy. A voucher authorizes a
/// ceiling; the contract does not measure inference usage. Only the timelock may upgrade.
contract MPPI is IMPPI, Initializable, UUPSUpgradeable, ReentrancyGuard {
    using SafeERC20 for IERC20;
    using SafeCast for uint256;

    address internal constant USDC = 0x3600000000000000000000000000000000000000;
    uint256 internal constant DISPUTE_WINDOW = 1 hours;
    uint256 internal constant IDLE_TIMEOUT = 24 hours;

    /// @custom:storage-location erc7201:mppi.storage.MPPI
    struct MPPIStorage {
        mapping(bytes32 sessionId => Session) sessions;
        address registry;
        address guardian;
        address safe;
        address timelock;
        PauseLevel level;
    }

    // ERC-7201: keccak256(abi.encode(uint256(keccak256(namespace)) - 1)) & ~0xff.
    bytes32 private constant MPPI_STORAGE_LOCATION = 0x4b1dbc392bf045d8ec2e961eaa9ba0257a7915fd3e78f005eedd6085f9b77200;

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(address registry, address guardian, address safe, address timelock) external initializer {
        _requireNonzero(registry);
        _requireNonzero(guardian);
        _requireNonzero(safe);
        _requireNonzero(timelock);
        MPPIStorage storage state = _getMPPIStorage();
        state.registry = registry;
        state.guardian = guardian;
        state.safe = safe;
        state.timelock = timelock;
        state.level = PauseLevel.None;
    }

    function open(SessionDescriptor calldata d, uint64 deposit, bytes32 expectedRateCardHash, EIP3009Auth calldata auth)
        external
        nonReentrant
    {
        _requirePaymentsEnabled();
        bytes32 id = computeSessionId(d);
        MPPIStorage storage state = _getMPPIStorage();
        if (state.sessions[id].deposit != 0) revert SessionExists();
        if (deposit == 0) revert ZeroDeposit();
        _requireAgent(d);
        if (d.sessionSigner == address(0)) revert InvalidSessionSigner();
        _requireToken(d);

        bytes32 initialRateCardHash = _openingCatalog(d, expectedRateCardHash);
        uint64 timestamp = block.timestamp.toUint64();

        // Funding is deliberately pull-before-credit, with the guard held throughout.
        _fund(d, deposit, auth);
        state.sessions[id] = Session({
            deposit: deposit,
            settledCumulative: 0,
            status: SessionStatus.Open,
            closeRequestedAt: 0,
            lastActivity: timestamp,
            initialRateCardHash: initialRateCardHash
        });
        _emitSessionOpened(d, id, deposit, initialRateCardHash);
    }

    function topUp(SessionDescriptor calldata d, uint64 amount, EIP3009Auth calldata auth) external nonReentrant {
        _requirePaymentsEnabled();
        bytes32 id = computeSessionId(d);
        Session storage session = _existingSession(id);
        _requireOpen(session);
        _requireAgent(d);
        _requireToken(d);
        _requireBeforeIdleDeadline(session);
        if (amount == 0) revert ZeroTopUp();
        uint64 deposit = (uint256(session.deposit) + uint256(amount)).toUint64();
        uint64 timestamp = block.timestamp.toUint64();
        _fund(d, amount, auth);
        session.deposit = deposit;
        session.lastActivity = timestamp;
        emit ToppedUp(id, amount, deposit);
    }

    function settle(
        SessionDescriptor calldata d,
        uint64 cumulativeAmount,
        uint64 captureAmount,
        bytes calldata voucherSig
    ) external nonReentrant {
        _requirePaymentsEnabled();
        _settle(msg.sender, d, cumulativeAmount, captureAmount, voucherSig);
    }

    function settleBatch(
        SessionDescriptor[] calldata d,
        uint64[] calldata cumulativeAmounts,
        uint64[] calldata captureAmounts,
        bytes[] calldata voucherSigs
    ) external nonReentrant {
        _requirePaymentsEnabled();
        uint256 length = d.length;
        if (length != cumulativeAmounts.length || length != captureAmounts.length || length != voucherSigs.length) {
            revert ArrayLengthMismatch();
        }
        for (uint256 i; i < length; ++i) {
            // The proxy self-call isolates all item effects, including token writes/logs.
            // Bare catch deliberately avoids copying or decoding arbitrary revert data.
            try this._settleBatchItem(msg.sender, d[i], cumulativeAmounts[i], captureAmounts[i], voucherSigs[i]) {}
                catch {}
        }
    }

    function _settleBatchItem(
        address caller,
        SessionDescriptor calldata d,
        uint64 cumulativeAmount,
        uint64 captureAmount,
        bytes calldata voucherSig
    ) external {
        if (msg.sender != address(this) || !_reentrancyGuardEntered()) {
            revert InvalidBatchContext();
        }
        _settle(caller, d, cumulativeAmount, captureAmount, voucherSig);
    }

    function requestClose(SessionDescriptor calldata d) external nonReentrant {
        _requireExitsEnabled();
        bytes32 id = computeSessionId(d);
        Session storage session = _existingSession(id);
        _requireOpen(session);
        _requireAgent(d);
        _requireBeforeIdleDeadline(session);
        uint64 timestamp = block.timestamp.toUint64();
        uint64 graceEnd = (block.timestamp + DISPUTE_WINDOW).toUint64();
        session.status = SessionStatus.Closing;
        session.closeRequestedAt = timestamp;
        session.lastActivity = timestamp;
        emit CloseRequested(id, graceEnd);
    }

    function close(
        SessionDescriptor calldata d,
        uint64 cumulativeAmount,
        uint64 captureAmount,
        bytes calldata voucherSig
    ) external nonReentrant {
        _requirePaymentsEnabled();
        bytes32 id = computeSessionId(d);
        Session storage session = _existingSession(id);
        _requireProvider(msg.sender, d);
        uint64 previous = session.settledCumulative;
        if (captureAmount < previous || captureAmount > cumulativeAmount) revert CaptureAmountInvalid();
        if (captureAmount > session.deposit) revert AmountExceedsDeposit();
        if (captureAmount > previous) {
            uint256 deadline = session.status == SessionStatus.Open
                ? uint256(session.lastActivity) + IDLE_TIMEOUT
                : uint256(session.closeRequestedAt) + DISPUTE_WINDOW;
            if (block.timestamp >= deadline) revert ClaimDeadlinePassed();
            _verifyVoucher(d, id, cumulativeAmount, voucherSig);
        }

        uint64 refund = session.deposit - captureAmount;
        session.settledCumulative = captureAmount;
        session.status = SessionStatus.Closed;
        _pay(d.recipient, captureAmount - previous);
        _pay(d.agent, refund);
        emit SessionClosed(id, captureAmount, refund);
    }

    function withdraw(SessionDescriptor calldata d) external nonReentrant {
        _requireExitsEnabled();
        bytes32 id = computeSessionId(d);
        Session storage session = _existingSession(id);
        if (session.status != SessionStatus.Closing) revert SessionNotClosing();
        _requireAgent(d);
        if (block.timestamp < uint256(session.closeRequestedAt) + DISPUTE_WINDOW) revert DisputeWindowOpen();
        uint64 refund = session.deposit - session.settledCumulative;
        session.status = SessionStatus.Closed;
        _pay(d.agent, refund);
        emit SessionClosed(id, session.settledCumulative, refund);
    }

    function releaseIdle(SessionDescriptor calldata d) external nonReentrant {
        _requireExitsEnabled();
        bytes32 id = computeSessionId(d);
        Session storage session = _existingSession(id);
        if (block.timestamp < uint256(session.lastActivity) + IDLE_TIMEOUT) revert NotIdle();
        uint64 refund = session.deposit - session.settledCumulative;
        session.status = SessionStatus.Closed;
        _pay(d.agent, refund);
        emit IdleReleased(id, refund);
    }

    function computeSessionId(SessionDescriptor calldata d) public view returns (bytes32) {
        return MPPISigning.sessionId(d, address(this), block.chainid);
    }

    function getSession(bytes32 sessionId) external view returns (Session memory) {
        return _getMPPIStorage().sessions[sessionId];
    }

    function voucherDigest(bytes32 sessionId, uint64 cumulativeAmount) public view returns (bytes32) {
        return MPPISigning.digest(
            MPPISigning.domainSeparator(address(this), block.chainid),
            MPPISigning.voucherStructHash(sessionId, cumulativeAmount)
        );
    }

    function pauseLevel() external view returns (PauseLevel) {
        return _getMPPIStorage().level;
    }

    function pause() external {
        MPPIStorage storage state = _getMPPIStorage();
        _requireCaller(state.guardian);
        if (state.level != PauseLevel.None) revert EnforcedPause();
        _setPauseLevel(PauseLevel.Payments);
    }

    function unpause() external {
        MPPIStorage storage state = _getMPPIStorage();
        _requireCaller(state.safe);
        if (state.level == PauseLevel.None) revert ExpectedPause();
        if (state.level == PauseLevel.All) revert EnforcedPause();
        _setPauseLevel(PauseLevel.None);
    }

    function pauseAll() external {
        MPPIStorage storage state = _getMPPIStorage();
        _requireCaller(state.timelock);
        if (state.level == PauseLevel.All) revert EnforcedPause();
        _setPauseLevel(PauseLevel.All);
    }

    function unpauseAll() external {
        MPPIStorage storage state = _getMPPIStorage();
        _requireCaller(state.timelock);
        if (state.level == PauseLevel.None) revert ExpectedPause();
        _setPauseLevel(PauseLevel.None);
    }

    function _settle(
        address caller,
        SessionDescriptor calldata d,
        uint64 cumulativeAmount,
        uint64 captureAmount,
        bytes calldata voucherSig
    ) private {
        bytes32 id = computeSessionId(d);
        Session storage session = _existingSession(id);
        _requireOpen(session);
        _requireProvider(caller, d);
        _requireBeforeIdleDeadline(session);
        _verifyVoucher(d, id, cumulativeAmount, voucherSig);
        uint64 previous = session.settledCumulative;
        if (captureAmount < previous || captureAmount > cumulativeAmount) revert CaptureAmountInvalid();
        if (cumulativeAmount > session.deposit) revert AmountExceedsDeposit();
        if (captureAmount == previous) return;
        session.settledCumulative = captureAmount;
        session.lastActivity = block.timestamp.toUint64();
        uint64 delta = captureAmount - previous;
        _pay(d.recipient, delta);
        emit Settled(id, d.agent, d.recipient, cumulativeAmount, captureAmount, delta, captureAmount);
    }

    function _verifyVoucher(SessionDescriptor calldata d, bytes32 id, uint64 amount, bytes calldata signature)
        private
        view
    {
        // The pinned calldata verifier rejects non-65-byte input before reading r,s,v,
        // validates low-s/v and avoids copying attacker-supplied signature bytes.
        (address recovered, ECDSA.RecoverError recoveryError,) =
            ECDSA.tryRecoverCalldata(voucherDigest(id, amount), signature);
        if (recoveryError != ECDSA.RecoverError.NoError || recovered == address(0) || recovered != d.sessionSigner) {
            revert InvalidVoucher();
        }
    }

    function _fund(SessionDescriptor calldata d, uint64 amount, EIP3009Auth calldata auth) private {
        IMPPIUSDC(USDC)
            .receiveWithAuthorization(
                d.agent, address(this), uint256(amount), auth.validAfter, auth.validBefore, auth.nonce, auth.signature
            );
    }

    function _openingCatalog(SessionDescriptor calldata d, bytes32 expectedHash) private view returns (bytes32 hash) {
        IMPPIIdentityRegistry registry = IMPPIIdentityRegistry(_getMPPIStorage().registry);
        address recipient = registry.getAgentWallet(d.providerId);
        address operator = registry.ownerOf(d.providerId);
        bytes memory catalog = registry.getMetadata(d.providerId, "mppi.catalog");
        if (recipient == address(0) || d.recipient != recipient) revert RecipientMismatch();
        if (d.operator != operator) revert OperatorMismatch();
        if (catalog.length == 0) revert EmptyCatalog();
        hash = keccak256(catalog);
        if (hash != expectedHash) revert RateCardHashMismatch();
    }

    function _emitSessionOpened(SessionDescriptor calldata d, bytes32 id, uint64 deposit, bytes32 initialRateCardHash)
        private
    {
        emit SessionOpened(
            id,
            d.agent,
            d.sessionSigner,
            d.providerId,
            d.recipient,
            d.operator,
            d.token,
            d.salt,
            deposit,
            initialRateCardHash
        );
    }

    function _pay(address destination, uint64 amount) private {
        if (amount != 0) IERC20(USDC).safeTransfer(destination, uint256(amount));
    }

    function _existingSession(bytes32 id) private view returns (Session storage session) {
        session = _getMPPIStorage().sessions[id];
        if (session.deposit == 0) revert SessionNotFound();
        if (session.status == SessionStatus.Closed) revert SessionIsClosed();
    }

    function _requireOpen(Session storage session) private view {
        if (session.status != SessionStatus.Open) revert SessionNotOpen();
    }

    function _requireBeforeIdleDeadline(Session storage session) private view {
        if (block.timestamp >= uint256(session.lastActivity) + IDLE_TIMEOUT) revert ClaimDeadlinePassed();
    }

    function _requirePaymentsEnabled() private view {
        if (_getMPPIStorage().level != PauseLevel.None) revert EnforcedPause();
    }

    function _requireExitsEnabled() private view {
        if (_getMPPIStorage().level == PauseLevel.All) revert EnforcedPause();
    }

    function _requireAgent(SessionDescriptor calldata d) private view {
        if (msg.sender != d.agent) revert NotAgent();
    }

    function _requireProvider(address caller, SessionDescriptor calldata d) private pure {
        if (caller != d.operator && caller != d.recipient) revert NotOperatorOrRecipient();
    }

    function _requireToken(SessionDescriptor calldata d) private pure {
        if (d.token != USDC) revert TokenNotAllowed();
    }

    function _requireNonzero(address account) private pure {
        if (account == address(0)) revert InvalidInitializationAddress(account);
    }

    function _requireCaller(address requiredAuthority) private view {
        if (msg.sender != requiredAuthority) revert UnauthorizedCaller(msg.sender, requiredAuthority);
    }

    function _setPauseLevel(PauseLevel next) private {
        MPPIStorage storage state = _getMPPIStorage();
        PauseLevel previous = state.level;
        state.level = next;
        emit PauseLevelChanged(msg.sender, previous, next);
    }

    function _authorizeUpgrade(address) internal view override {
        _requireCaller(_getMPPIStorage().timelock);
    }

    function _getMPPIStorage() private pure returns (MPPIStorage storage state) {
        bytes32 slot = MPPI_STORAGE_LOCATION;
        assembly ("memory-safe") {
            state.slot := slot
        }
    }
}
