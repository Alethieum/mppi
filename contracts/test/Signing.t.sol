// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {IMPPI} from "../src/IMPPI.sol";
import {MPPISigning} from "../src/MPPISigning.sol";

// Only the documented Foundry cheatcodes used by these tests. No runtime dependency.
interface Vm {
    function readFile(string calldata path) external view returns (string memory);
    function parseJsonString(string calldata json, string calldata key) external pure returns (string memory);
    function parseJsonAddress(string calldata json, string calldata key) external pure returns (address);
    function parseJsonBytes32(string calldata json, string calldata key) external pure returns (bytes32);
    function parseJsonBytes(string calldata json, string calldata key) external pure returns (bytes memory);
    function parseUint(string calldata value) external pure returns (uint256);
    function toString(uint256 value) external pure returns (string memory);
    function addr(uint256 privateKey) external pure returns (address);
}

contract SigningTest {
    Vm private constant vm = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));
    string private fixture;
    IMPPI.SessionDescriptor private descriptor;
    address private proxy;
    uint256 private chainId;
    bytes32 private id;
    bytes32 private domain;

    function setUp() public {
        fixture = vm.readFile("../testvectors/signing.json");
        descriptor = IMPPI.SessionDescriptor({
            agent: vm.parseJsonAddress(fixture, ".descriptor.agent"),
            sessionSigner: vm.parseJsonAddress(fixture, ".descriptor.sessionSigner"),
            providerId: uintAt(".descriptor.providerId"),
            recipient: vm.parseJsonAddress(fixture, ".descriptor.recipient"),
            operator: vm.parseJsonAddress(fixture, ".descriptor.operator"),
            token: vm.parseJsonAddress(fixture, ".descriptor.token"),
            salt: vm.parseJsonBytes32(fixture, ".descriptor.salt")
        });
        proxy = vm.parseJsonAddress(fixture, ".domain.verifyingContract");
        chainId = uintAt(".domain.chainId");
        id = MPPISigning.sessionId(descriptor, proxy, chainId);
        domain = MPPISigning.domainSeparator(proxy, chainId);
    }

    function uintAt(string memory path) private view returns (uint256) {
        return vm.parseUint(vm.parseJsonString(fixture, path));
    }

    function testSharedSessionIdentityAndDomain() public view {
        require(id == vm.parseJsonBytes32(fixture, ".sessionId"), "session id");
        require(domain == vm.parseJsonBytes32(fixture, ".domainSeparator"), "domain separator");
        require(descriptor.agent != descriptor.sessionSigner, "distinct funding and proof authorities");
        require(descriptor.sessionSigner == vm.addr(1), "public test signer");
        require(descriptor.providerId > 2 ** 53, "provider id width vector");
    }

    function testAllFourProofsAndAmountBoundaries() public view {
        for (uint256 i; i < 7; ++i) {
            string memory path = string.concat(".cases[", vm.toString(i), "]");
            string memory message = string.concat(path, ".message");
            bytes32 proofId = vm.parseJsonBytes32(fixture, string.concat(message, ".sessionId"));
            bytes32 kind = keccak256(bytes(vm.parseJsonString(fixture, string.concat(path, ".kind"))));
            bytes32 structHash;
            if (kind == keccak256("Voucher")) {
                uint256 amount = uintAt(string.concat(message, ".cumulativeAmount"));
                require(amount <= type(uint64).max, "amount width");
                structHash = MPPISigning.voucherStructHash(proofId, uint64(amount));
            } else if (kind == keccak256("RunAuth")) {
                structHash = MPPISigning.runAuthStructHash(
                    proofId,
                    uint64(uintAt(string.concat(message, ".controlGeneration"))),
                    vm.parseJsonBytes32(fixture, string.concat(message, ".runId")),
                    vm.parseJsonString(fixture, string.concat(message, ".model")),
                    vm.parseJsonBytes32(fixture, string.concat(message, ".rateCardHash"))
                );
            } else if (kind == keccak256("ReconnectAuth")) {
                structHash = MPPISigning.reconnectAuthStructHash(
                    proofId, vm.parseJsonBytes32(fixture, string.concat(message, ".nonce"))
                );
            } else {
                require(kind == keccak256("CloseAuth"), "known proof type");
                structHash = MPPISigning.closeAuthStructHash(proofId);
            }
            require(structHash == vm.parseJsonBytes32(fixture, string.concat(path, ".structHash")), "struct hash");
            bytes32 digest = MPPISigning.digest(domain, structHash);
            require(digest == vm.parseJsonBytes32(fixture, string.concat(path, ".digest")), "typed digest");
            bytes memory sig = vm.parseJsonBytes(fixture, string.concat(path, ".signature"));
            require(sig.length == 65, "signature width");
            bytes32 r;
            bytes32 s;
            uint8 v;
            assembly ("memory-safe") {
                r := mload(add(sig, 32))
                s := mload(add(sig, 64))
                v := byte(0, mload(add(sig, 96)))
            }
            require(v == 27 || v == 28, "recovery id");
            require(uint256(s) <= 0x7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0, "low s");
            address recovered = ecrecover(digest, v, r, s);
            require(recovered == descriptor.sessionSigner && recovered != address(0), "signer");
            require(
                recovered == vm.parseJsonAddress(fixture, string.concat(path, ".expectedSigner")), "expected signer"
            );
        }
    }

    function testEveryDescriptorFieldAndDomainBound() public view {
        IMPPI.SessionDescriptor memory d = descriptor;
        d.agent = address(1);
        require(MPPISigning.sessionId(d, proxy, chainId) != id, "agent bound");
        d = descriptor;
        d.sessionSigner = address(1);
        require(MPPISigning.sessionId(d, proxy, chainId) != id, "signer bound");
        d = descriptor;
        d.providerId += 1;
        require(MPPISigning.sessionId(d, proxy, chainId) != id, "provider bound");
        d = descriptor;
        d.recipient = address(1);
        require(MPPISigning.sessionId(d, proxy, chainId) != id, "recipient bound");
        d = descriptor;
        d.operator = address(1);
        require(MPPISigning.sessionId(d, proxy, chainId) != id, "operator bound");
        d = descriptor;
        d.token = address(1);
        require(MPPISigning.sessionId(d, proxy, chainId) != id, "token bound");
        d = descriptor;
        d.salt = bytes32(uint256(1));
        require(MPPISigning.sessionId(d, proxy, chainId) != id, "salt bound");
        require(MPPISigning.sessionId(descriptor, address(1), chainId) != id, "proxy bound");
        require(MPPISigning.sessionId(descriptor, proxy, chainId + 1) != id, "chain bound");
        require(MPPISigning.domainSeparator(address(1), chainId) != domain, "proxy domain");
        require(MPPISigning.domainSeparator(proxy, chainId + 1) != domain, "chain domain");
    }

    function testDescriptorSelectorsAndEnumValues() public pure {
        require(
            IMPPI.open.selector
                == bytes4(
                    keccak256(
                        "open((address,address,uint256,address,address,address,bytes32),uint64,bytes32,(uint256,uint256,bytes32,bytes))"
                    )
                ),
            "open selector"
        );
        require(
            IMPPI.topUp.selector
                == bytes4(
                    keccak256(
                        "topUp((address,address,uint256,address,address,address,bytes32),uint64,(uint256,uint256,bytes32,bytes))"
                    )
                ),
            "topUp selector"
        );
        require(
            IMPPI.settle.selector
                == bytes4(
                    keccak256("settle((address,address,uint256,address,address,address,bytes32),uint64,uint64,bytes)")
                ),
            "settle selector"
        );
        require(
            IMPPI.settleBatch.selector
                == bytes4(
                    keccak256(
                        "settleBatch((address,address,uint256,address,address,address,bytes32)[],uint64[],uint64[],bytes[])"
                    )
                ),
            "batch selector"
        );
        require(
            IMPPI.close.selector
                == bytes4(
                    keccak256("close((address,address,uint256,address,address,address,bytes32),uint64,uint64,bytes)")
                ),
            "close selector"
        );
        require(
            IMPPI.requestClose.selector
                == bytes4(keccak256("requestClose((address,address,uint256,address,address,address,bytes32))")),
            "requestClose selector"
        );
        require(
            IMPPI.withdraw.selector
                == bytes4(keccak256("withdraw((address,address,uint256,address,address,address,bytes32))")),
            "withdraw selector"
        );
        require(
            IMPPI.releaseIdle.selector
                == bytes4(keccak256("releaseIdle((address,address,uint256,address,address,address,bytes32))")),
            "releaseIdle selector"
        );
        require(
            IMPPI.computeSessionId.selector
                == bytes4(keccak256("computeSessionId((address,address,uint256,address,address,address,bytes32))")),
            "id selector"
        );
        require(
            IMPPI._settleBatchItem.selector
                == bytes4(
                    keccak256(
                        "_settleBatchItem(address,(address,address,uint256,address,address,address,bytes32),uint64,uint64,bytes)"
                    )
                ),
            "restricted helper selector"
        );
        require(
            IMPPI.initialize.selector == bytes4(keccak256("initialize(address,address,address,address)")),
            "initializer selector"
        );
        require(
            uint8(IMPPI.SessionStatus.Open) == 0 && uint8(IMPPI.SessionStatus.Closing) == 1
                && uint8(IMPPI.SessionStatus.Closed) == 2,
            "contract status values"
        );
        require(
            uint8(IMPPI.PauseLevel.None) == 0 && uint8(IMPPI.PauseLevel.Payments) == 1
                && uint8(IMPPI.PauseLevel.All) == 2,
            "pause values"
        );
    }
}
