// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {MockIdentityRegistry} from "./MockIdentityRegistry.sol";

/// @dev Disposable local discovery fixture. This does not implement ERC-8004.
contract DiscoveryRegistry is MockIdentityRegistry {
    mapping(uint256 => string) private uris;

    function setURI(uint256 id, string calldata uri) external {
        uris[id] = uri;
    }

    function tokenURI(uint256 id) external view returns (string memory) {
        return uris[id];
    }
}
