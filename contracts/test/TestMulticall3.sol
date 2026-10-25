// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

/// @dev Local stand-in with Multicall3's snapshot interface, installed at its
/// canonical address on a test node. Its code differs from Multicall3's, so a
/// test points the SDK's one-time code check at this runtime instead.
contract TestMulticall3 {
    struct Call3 {
        address target;
        bool allowFailure;
        bytes callData;
    }

    struct Result {
        bool success;
        bytes returnData;
    }

    function aggregate3(Call3[] calldata calls) external payable returns (Result[] memory returnData) {
        returnData = new Result[](calls.length);
        for (uint256 i = 0; i < calls.length; ++i) {
            (bool success, bytes memory data) = calls[i].target.call(calls[i].callData);
            require(success || calls[i].allowFailure, "Multicall3: call failed");
            returnData[i] = Result({success: success, returnData: data});
        }
    }

    function getBlockNumber() external view returns (uint256) {
        return block.number;
    }

    function getCurrentBlockTimestamp() external view returns (uint256) {
        return block.timestamp;
    }

    function getLastBlockHash() external view returns (bytes32) {
        unchecked {
            return blockhash(block.number - 1);
        }
    }
}
