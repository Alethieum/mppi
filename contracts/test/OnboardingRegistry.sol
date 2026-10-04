// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {ECDSA} from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

/// @dev Disposable EOA onboarding fixture, NOT an ERC-8004 implementation.
/// Only the tested registration/owner/metadata/proof ABI is represented. No
/// transfer, approvals, ERC-721 receiver or ERC-1271 behavior is simulated here.
/// ABI: erc-8004/erc-8004-contracts b9e466c, IdentityRegistryUpgradeable.sol:39-164.
contract OnboardingRegistry {
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event Registered(uint256 indexed agentId, string agentURI, address indexed owner);
    event MetadataSet(
        uint256 indexed agentId, string indexed indexedMetadataKey, string metadataKey, bytes metadataValue
    );
    event URIUpdated(uint256 indexed agentId, string newURI, address indexed updatedBy);

    uint256 public nextId;
    bool private initialized;
    mapping(uint256 => address) private owners;
    mapping(uint256 => string) private uris;
    mapping(uint256 => mapping(string => bytes)) private metadata;

    function initialize() external {
        require(!initialized, "already initialized");
        initialized = true;
    }

    function ownerOf(uint256 id) public view returns (address) {
        require(owners[id] != address(0), "missing identity");
        return owners[id];
    }

    function register() external returns (uint256) {
        return _register("");
    }

    function register(string calldata uri) external returns (uint256) {
        return _register(uri);
    }

    function _register(string memory uri) private returns (uint256 id) {
        require(initialized, "not initialized");
        id = nextId++;
        owners[id] = msg.sender;
        uris[id] = uri;
        metadata[id]["agentWallet"] = abi.encodePacked(msg.sender);
        emit Transfer(address(0), msg.sender, id);
        emit Registered(id, uri, msg.sender);
        emit MetadataSet(id, "agentWallet", "agentWallet", abi.encodePacked(msg.sender));
    }

    function tokenURI(uint256 id) external view returns (string memory) {
        ownerOf(id);
        return uris[id];
    }

    function getAgentWallet(uint256 id) external view returns (address) {
        return address(bytes20(metadata[id]["agentWallet"]));
    }

    function getMetadata(uint256 id, string calldata key) external view returns (bytes memory) {
        return metadata[id][key];
    }

    function setAgentURI(uint256 id, string calldata uri) external {
        require(msg.sender == ownerOf(id), "owner only");
        uris[id] = uri;
        emit URIUpdated(id, uri, msg.sender);
    }

    function setMetadata(uint256 id, string calldata key, bytes calldata value) external {
        require(msg.sender == ownerOf(id), "owner only");
        require(keccak256(bytes(key)) != keccak256("agentWallet"), "reserved key");
        metadata[id][key] = value;
        emit MetadataSet(id, key, key, value);
    }

    function eip712Domain()
        external
        view
        returns (bytes1, string memory, string memory, uint256, address, bytes32, uint256[] memory)
    {
        return (hex"0f", "ERC8004IdentityRegistry", "1", block.chainid, address(this), bytes32(0), new uint256[](0));
    }

    function setAgentWallet(uint256 id, address wallet, uint256 deadline, bytes calldata signature) external {
        address owner = ownerOf(id);
        require(msg.sender == owner, "owner only");
        require(
            wallet != address(0) && block.timestamp <= deadline && deadline <= block.timestamp + 300, "invalid proof"
        );
        bytes32 domain = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256("ERC8004IdentityRegistry"),
                keccak256("1"),
                block.chainid,
                address(this)
            )
        );
        bytes32 body = keccak256(
            abi.encode(
                keccak256("AgentWalletSet(uint256 agentId,address newWallet,address owner,uint256 deadline)"),
                id,
                wallet,
                owner,
                deadline
            )
        );
        require(
            ECDSA.recover(keccak256(abi.encodePacked(hex"1901", domain, body)), signature) == wallet, "invalid signer"
        );
        metadata[id]["agentWallet"] = abi.encodePacked(wallet);
        emit MetadataSet(id, "agentWallet", "agentWallet", abi.encodePacked(wallet));
    }
}
