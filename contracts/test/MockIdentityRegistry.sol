// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

/// @dev Local boundary fixture, not a substitute for testing the deployed registry.
contract MockIdentityRegistry {
    struct Provider {
        address owner;
        address wallet;
        bytes catalog;
        bool exists;
    }

    error MissingProvider(uint256 id);
    mapping(uint256 => Provider) private providers;

    function setProvider(uint256 id, address owner, address wallet, bytes memory catalog) external {
        providers[id] = Provider(owner, wallet, catalog, true);
    }

    function ownerOf(uint256 id) external view returns (address) {
        if (!providers[id].exists) revert MissingProvider(id);
        return providers[id].owner;
    }

    function getAgentWallet(uint256 id) external view returns (address) {
        if (!providers[id].exists) revert MissingProvider(id);
        return providers[id].wallet;
    }

    function getMetadata(uint256 id, string calldata key) external view returns (bytes memory) {
        if (!providers[id].exists) revert MissingProvider(id);
        require(keccak256(bytes(key)) == keccak256("mppi.catalog"), "unexpected metadata key");
        return providers[id].catalog;
    }
}
