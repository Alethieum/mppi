// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {IMPPI} from "./IMPPI.sol";

/// @notice Pure MPPI identity and EIP-712 hashing, without session state or payment execution.
/// @dev Callers supply the verified chain and proxy context. Hashing proves no
///      descriptor eligibility, session existence, replay safety or usage claim.
library MPPISigning {
    bytes32 internal constant DOMAIN_TYPEHASH =
        keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
    bytes32 internal constant VOUCHER_TYPEHASH = keccak256("Voucher(bytes32 sessionId,uint64 cumulativeAmount)");
    bytes32 internal constant RUN_AUTH_TYPEHASH = keccak256(
        "RunAuth(bytes32 sessionId,uint64 controlGeneration,bytes32 runId,string model,bytes32 rateCardHash)"
    );
    bytes32 internal constant RECONNECT_AUTH_TYPEHASH = keccak256("ReconnectAuth(bytes32 sessionId,bytes32 nonce)");
    bytes32 internal constant CLOSE_AUTH_TYPEHASH = keccak256("CloseAuth(bytes32 sessionId)");

    function sessionId(IMPPI.SessionDescriptor memory d, address proxy, uint256 chainId)
        internal
        pure
        returns (bytes32)
    {
        return keccak256(
            abi.encode(d.agent, d.sessionSigner, d.providerId, d.recipient, d.operator, d.token, d.salt, proxy, chainId)
        );
    }

    function domainSeparator(address proxy, uint256 chainId) internal pure returns (bytes32) {
        return keccak256(abi.encode(DOMAIN_TYPEHASH, keccak256("MPPI"), keccak256("1"), chainId, proxy));
    }

    function voucherStructHash(bytes32 id, uint64 cumulativeAmount) internal pure returns (bytes32) {
        return keccak256(abi.encode(VOUCHER_TYPEHASH, id, cumulativeAmount));
    }

    function runAuthStructHash(bytes32 id, uint64 generation, bytes32 runId, string memory model, bytes32 rateCardHash)
        internal
        pure
        returns (bytes32)
    {
        return keccak256(abi.encode(RUN_AUTH_TYPEHASH, id, generation, runId, keccak256(bytes(model)), rateCardHash));
    }

    function reconnectAuthStructHash(bytes32 id, bytes32 nonce) internal pure returns (bytes32) {
        return keccak256(abi.encode(RECONNECT_AUTH_TYPEHASH, id, nonce));
    }

    function closeAuthStructHash(bytes32 id) internal pure returns (bytes32) {
        return keccak256(abi.encode(CLOSE_AUTH_TYPEHASH, id));
    }

    function digest(bytes32 domain, bytes32 structHash) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(hex"1901", domain, structHash));
    }
}
