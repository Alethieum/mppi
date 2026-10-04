// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.37;

import {Initializable} from "@openzeppelin/contracts/proxy/utils/Initializable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts/proxy/utils/UUPSUpgradeable.sol";
import {MPPI} from "../src/MPPI.sol";

/// @dev Compiler layout probe only. Never a candidate implementation: this ordinary
/// storage variable lets tooling export the exact inherited namespace/member types.
contract MPPIStorageLayoutHarness is MPPI {
    MPPIStorage private layoutProbe;
}

/// @dev Negative control for the narrowly reviewed inherited-constructor exception.
contract MPPIBadConstructorFixture is MPPI {
    constructor() {
        assembly ("memory-safe") {
            sstore(0, 1)
        }
    }
}

/// @dev Deliberately incompatible upgrade fixture. The storage validator MUST reject
/// widening deposit, which moves the rest of each existing session's packed fields.
contract MPPIBadLayoutFixture is Initializable, UUPSUpgradeable {
    struct BrokenSession {
        uint256 deposit;
        uint64 settledCumulative;
        uint8 status;
        uint64 closeRequestedAt;
        uint64 lastActivity;
        bytes32 initialRateCardHash;
    }

    /// @custom:storage-location erc7201:mppi.storage.MPPI
    struct MPPIStorage {
        mapping(bytes32 sessionId => BrokenSession) sessions;
        address registry;
        address guardian;
        address safe;
        address timelock;
        uint8 level;
    }

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize() external initializer {}

    function _authorizeUpgrade(address) internal pure override {}
}
