//! Exact MPPI session identity and EIP-712 proof hashing.
//!
//! The fixed descriptor signer authorizes MPPI proofs. Funding and refunds use
//! `agent`; USDC funding authorization uses its own token domain. A valid
//! signature is only one prerequisite for admission. Verified chain state,
//! session lifecycle, replay protection and current attachment are caller duties.

use alloy_primitives::{Address, B256, Signature, U256, keccak256};
use alloy_sol_types::{Eip712Domain, SolStruct, SolValue, eip712_domain, sol};
use core::fmt;

sol! {
    /// The seven descriptor fields in the exact contract ABI order.
    #[derive(Debug, PartialEq, Eq)]
    struct SessionDescriptor {
        address agent;
        address sessionSigner;
        uint256 providerId;
        address recipient;
        address operator;
        address token;
        bytes32 salt;
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Voucher {
        bytes32 sessionId;
        uint64 cumulativeAmount;
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RunAuth {
        bytes32 sessionId;
        uint64 controlGeneration;
        bytes32 runId;
        string model;
        bytes32 rateCardHash;
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ReconnectAuth {
        bytes32 sessionId;
        bytes32 nonce;
    }

    #[derive(Debug, PartialEq, Eq)]
    struct CloseAuth {
        bytes32 sessionId;
    }
}

/// A verified chain and canonical proxy selected by the embedding provider.
/// Construction itself performs no RPC or deployment-identity verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SigningContext {
    pub chain_id: u64,
    pub verifying_contract: Address,
}

impl SigningContext {
    pub fn domain(&self) -> Eip712Domain {
        eip712_domain! {
            name: "MPPI",
            version: "1",
            chain_id: self.chain_id,
            verifying_contract: self.verifying_contract,
        }
    }

    /// Hash every descriptor field plus the proxy and ABI-uint256 chain id.
    /// Like the contract helper, this proves neither eligibility nor existence.
    pub fn session_id(&self, d: &SessionDescriptor) -> B256 {
        keccak256(
            (
                d.agent,
                d.sessionSigner,
                d.providerId,
                d.recipient,
                d.operator,
                d.token,
                d.salt,
                self.verifying_contract,
                U256::from(self.chain_id),
            )
                .abi_encode(),
        )
    }

    pub fn digest<T: SessionProof>(&self, proof: &T) -> B256 {
        proof.eip712_signing_hash(&self.domain())
    }

    /// Verify proof identity and canonical ECDSA authority. This does not make
    /// a stale generation, reused nonce, expired session or unfunded run valid.
    pub fn verify<T: SessionProof>(
        &self,
        descriptor: &SessionDescriptor,
        proof: &T,
        signature: &[u8],
    ) -> Result<(), VerificationError> {
        if proof.session_id() != self.session_id(descriptor) {
            return Err(VerificationError::SessionMismatch);
        }
        verify_signature(self.digest(proof), descriptor.sessionSigner, signature)
    }
}

mod sealed {
    pub trait Sealed {}
}

/// The four fixed MPPI proof types. Arbitrary Solidity structs cannot be used.
pub trait SessionProof: SolStruct + sealed::Sealed {
    fn session_id(&self) -> B256;
}

macro_rules! session_proof {
    ($($proof:ty),+ $(,)?) => {$ (
        impl sealed::Sealed for $proof {}
        impl SessionProof for $proof {
            fn session_id(&self) -> B256 { self.sessionId }
        }
    )+};
}
session_proof!(Voucher, RunAuth, ReconnectAuth, CloseAuth);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerificationError {
    SessionMismatch,
    ZeroSigner,
    SignatureLength,
    RecoveryId,
    HighS,
    InvalidSignature,
    WrongSigner,
}

impl fmt::Display for VerificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::SessionMismatch => "proof session id does not match descriptor and domain",
            Self::ZeroSigner => "session signer is zero",
            Self::SignatureLength => "MPPI signature must contain exactly 65 bytes",
            Self::RecoveryId => "MPPI signature v must be 27 or 28",
            Self::HighS => "MPPI signature must use low-s encoding",
            Self::InvalidSignature => "invalid secp256k1 signature",
            Self::WrongSigner => "signature does not recover to the fixed session signer",
        })
    }
}

impl std::error::Error for VerificationError {}

fn verify_signature(digest: B256, signer: Address, bytes: &[u8]) -> Result<(), VerificationError> {
    if signer.is_zero() {
        return Err(VerificationError::ZeroSigner);
    }
    if bytes.len() != 65 {
        return Err(VerificationError::SignatureLength);
    }
    if !matches!(bytes[64], 27 | 28) {
        return Err(VerificationError::RecoveryId);
    }
    let signature = Signature::from_raw(bytes).map_err(|_| VerificationError::InvalidSignature)?;
    // Alloy recovery normalizes s internally. Reject high-s before recovery so
    // a mathematically equivalent but forbidden encoding is never accepted.
    if signature.normalize_s().is_some() {
        return Err(VerificationError::HighS);
    }
    let recovered = signature
        .recover_address_from_prehash(&digest)
        .map_err(|_| VerificationError::InvalidSignature)?;
    if recovered != signer {
        return Err(VerificationError::WrongSigner);
    }
    Ok(())
}
