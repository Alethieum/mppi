// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

/// @notice MPPI v1 application interface. This interface contains no payment implementation.
/// @dev Amounts are six-decimal USDC base units. Authority and lifecycle requirements
///      are documented in docs/contract-interface.md from the public project root.
interface IMPPI {
    enum SessionStatus {
        Open,
        Closing,
        Closed
    }
    enum PauseLevel {
        None,
        Payments,
        All
    }

    struct SessionDescriptor {
        address agent;
        address sessionSigner;
        uint256 providerId;
        address recipient;
        address operator;
        address token;
        bytes32 salt;
    }

    struct Session {
        uint64 deposit;
        uint64 settledCumulative;
        SessionStatus status;
        uint64 closeRequestedAt;
        uint64 lastActivity;
        bytes32 initialRateCardHash;
    }

    struct EIP3009Auth {
        uint256 validAfter;
        uint256 validBefore;
        bytes32 nonce;
        bytes signature;
    }

    error SessionExists();
    error SessionNotFound();
    error ZeroDeposit();
    error SessionNotOpen();
    error SessionNotClosing();
    error SessionIsClosed();
    error InvalidVoucher();
    error InvalidSessionSigner();
    error CaptureAmountInvalid();
    error AmountExceedsDeposit();
    error ArrayLengthMismatch();
    error InvalidBatchContext();
    error NotOperatorOrRecipient();
    error NotAgent();
    error DisputeWindowOpen();
    error NotIdle();
    error ClaimDeadlinePassed();
    error ZeroTopUp();
    error RecipientMismatch();
    error OperatorMismatch();
    error EmptyCatalog();
    error RateCardHashMismatch();
    error TokenNotAllowed();
    error EnforcedPause();
    error ExpectedPause();
    error UnauthorizedCaller(address caller, address requiredAuthority);
    error InvalidInitializationAddress(address account);

    event SessionOpened(
        bytes32 indexed sessionId,
        address indexed agent,
        address sessionSigner,
        uint256 indexed providerId,
        address recipient,
        address operator,
        address token,
        bytes32 salt,
        uint64 deposit,
        bytes32 initialRateCardHash
    );
    event ToppedUp(bytes32 indexed sessionId, uint64 amount, uint64 newDeposit);
    event Settled(
        bytes32 indexed sessionId,
        address indexed agent,
        address indexed recipient,
        uint64 cumulativeAmount,
        uint64 captureAmount,
        uint64 deltaPaid,
        uint64 newSettled
    );
    event CloseRequested(bytes32 indexed sessionId, uint64 graceEnd);
    event SessionClosed(bytes32 indexed sessionId, uint64 captureAmount, uint64 refunded);
    event IdleReleased(bytes32 indexed sessionId, uint64 amount);
    event PauseLevelChanged(address indexed account, PauseLevel previousLevel, PauseLevel newLevel);

    function initialize(address registry, address guardian, address safe, address timelock) external;
    function open(SessionDescriptor calldata d, uint64 deposit, bytes32 expectedRateCardHash, EIP3009Auth calldata auth)
        external;
    function topUp(SessionDescriptor calldata d, uint64 amount, EIP3009Auth calldata auth) external;
    function settle(
        SessionDescriptor calldata d,
        uint64 cumulativeAmount,
        uint64 captureAmount,
        bytes calldata voucherSig
    ) external;
    function settleBatch(
        SessionDescriptor[] calldata d,
        uint64[] calldata cumulativeAmounts,
        uint64[] calldata captureAmounts,
        bytes[] calldata voucherSigs
    ) external;
    function requestClose(SessionDescriptor calldata d) external;
    function close(
        SessionDescriptor calldata d,
        uint64 cumulativeAmount,
        uint64 captureAmount,
        bytes calldata voucherSig
    ) external;
    function withdraw(SessionDescriptor calldata d) external;
    function releaseIdle(SessionDescriptor calldata d) external;
    function computeSessionId(SessionDescriptor calldata d) external view returns (bytes32);
    function getSession(bytes32 sessionId) external view returns (Session memory);
    function voucherDigest(bytes32 sessionId, uint64 cumulativeAmount) external view returns (bytes32);
    function pauseLevel() external view returns (PauseLevel);
    function pause() external;
    function unpause() external;
    function pauseAll() external;
    function unpauseAll() external;

    /// @dev Restricted self-call boundary inside an entered settleBatch guard.
    ///      Not an SDK operation; caller carries the original payment caller.
    function _settleBatchItem(
        address caller,
        SessionDescriptor calldata d,
        uint64 cumulativeAmount,
        uint64 captureAmount,
        bytes calldata voucherSig
    ) external;
}
