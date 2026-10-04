// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {ECDSA} from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

/// @dev Adversarial local USDC boundary fixture. Runtime is etched at Arc's token
/// address; all state is explicitly configured there, never in a constructor.
contract MockUSDC {
    function decimals() external pure returns (uint8) {
        return 6;
    }

    bytes32 public constant RECEIVE_TYPEHASH = keccak256(
        "ReceiveWithAuthorization(address from,address to,uint256 value,uint256 validAfter,uint256 validBefore,bytes32 nonce)"
    );
    bytes32 private constant DOMAIN_TYPEHASH =
        keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");

    error InvalidReceiver();
    error AuthorizationNotYetValid();
    error AuthorizationExpired();
    error AuthorizationUsed();
    error InvalidAuthorization();
    error TokenRestricted();
    error ForcedFailure();
    error InsufficientBalance();
    error HookFailed(bytes reason);

    event Transfer(address indexed from, address indexed to, uint256 value);
    event AuthorizationConsumed(address indexed from, bytes32 indexed nonce);

    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(bytes32 => bool)) public authorizationState;
    mapping(address => bool) public blocked;
    bool public paused;
    bool public failFundingAfterTransfer;
    address public failingRecipient;
    bool public transferReturnsFalse;
    uint256 public revertBytes;
    uint256 public fundingCalls;
    uint256 public transferCalls;
    address public hookTarget;
    bytes public hookData;
    bool public hookFunding;
    bool public hookTransfer;
    bool public bubbleHookFailure;
    bool public hookSucceeded;
    bytes public hookResult;

    function mint(address account, uint256 amount) external {
        balanceOf[account] += amount;
    }

    function setRestrictions(bool isPaused, address account, bool isBlocked) external {
        paused = isPaused;
        blocked[account] = isBlocked;
    }

    function cancelAuthorization(address account, bytes32 nonce) external {
        authorizationState[account][nonce] = true;
    }

    function setFailures(bool fundingAfter, address recipient, bool returnsFalse, uint256 payloadBytes) external {
        failFundingAfterTransfer = fundingAfter;
        failingRecipient = recipient;
        transferReturnsFalse = returnsFalse;
        revertBytes = payloadBytes;
    }

    function setHook(address target, bytes memory data, bool funding, bool payout, bool bubble) external {
        hookTarget = target;
        hookData = data;
        hookFunding = funding;
        hookTransfer = payout;
        bubbleHookFailure = bubble;
    }

    function domainSeparator() public view returns (bytes32) {
        return keccak256(abi.encode(DOMAIN_TYPEHASH, keccak256("USDC"), keccak256("2"), block.chainid, address(this)));
    }

    function authorizationDigest(
        address from,
        address to,
        uint256 value,
        uint256 validAfter,
        uint256 validBefore,
        bytes32 nonce
    ) public view returns (bytes32) {
        bytes32 hash = keccak256(abi.encode(RECEIVE_TYPEHASH, from, to, value, validAfter, validBefore, nonce));
        return keccak256(abi.encodePacked(hex"1901", domainSeparator(), hash));
    }

    function receiveWithAuthorization(
        address from,
        address to,
        uint256 value,
        uint256 validAfter,
        uint256 validBefore,
        bytes32 nonce,
        bytes calldata signature
    ) external {
        if (to != msg.sender) revert InvalidReceiver();
        if (block.timestamp <= validAfter) revert AuthorizationNotYetValid();
        if (block.timestamp >= validBefore) revert AuthorizationExpired();
        if (authorizationState[from][nonce]) revert AuthorizationUsed();
        (address signer, ECDSA.RecoverError recoveryError,) =
            ECDSA.tryRecoverCalldata(authorizationDigest(from, to, value, validAfter, validBefore, nonce), signature);
        if (recoveryError != ECDSA.RecoverError.NoError || signer == address(0) || signer != from) {
            revert InvalidAuthorization();
        }
        ++fundingCalls;
        authorizationState[from][nonce] = true;
        emit AuthorizationConsumed(from, nonce);
        _move(from, to, value);
        if (hookFunding) _hook();
        if (failFundingAfterTransfer) revert ForcedFailure();
    }

    function transfer(address to, uint256 value) external returns (bool) {
        ++transferCalls;
        _move(msg.sender, to, value);
        if (hookTransfer) _hook();
        if (to == failingRecipient) {
            uint256 length = revertBytes;
            if (length != 0) {
                // Large hostile revert payload, intentionally bounded by test inputs.
                bytes memory payload = new bytes(length);
                assembly ("memory-safe") {
                    revert(add(payload, 32), mload(payload))
                }
            }
            if (transferReturnsFalse) return false;
            revert ForcedFailure();
        }
        return true;
    }

    function _move(address from, address to, uint256 value) private {
        if (paused || blocked[from] || blocked[to]) revert TokenRestricted();
        if (balanceOf[from] < value) revert InsufficientBalance();
        balanceOf[from] -= value;
        balanceOf[to] += value;
        emit Transfer(from, to, value);
    }

    function _hook() private {
        (hookSucceeded, hookResult) = hookTarget.call(hookData);
        if (!hookSucceeded && bubbleHookFailure) revert HookFailed(hookResult);
    }
}
