//! Wallet capabilities supplied by the embedding application.
//!
//! A transaction adapter must durably bind each operation reference to the exact
//! wallet, chain, destination and calldata before submission. Reconciliation
//! queries that original operation and must never submit a replacement.

use crate::errors::{ValidationCode, ValidationError};
use crate::types::OperationResult;
pub use crate::types::WalletCall;
use alloy_primitives::{Address, B256, U256};
use std::time::SystemTime;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FundingAuthorization {
    chain_id: u64,
    token: Address,
    domain_name: String,
    domain_version: String,
    owner: Address,
    recipient: Address,
    value: U256,
    valid_after: U256,
    valid_before: U256,
    nonce: B256,
}

impl FundingAuthorization {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_id: u64,
        token: Address,
        domain_name: String,
        domain_version: String,
        owner: Address,
        recipient: Address,
        value: U256,
        valid_after: U256,
        valid_before: U256,
        nonce: B256,
    ) -> Result<Self, ValidationError> {
        if chain_id == 0
            || token.is_zero()
            || owner.is_zero()
            || recipient.is_zero()
            || domain_name.is_empty()
            || domain_version.is_empty()
            || valid_after >= valid_before
        {
            return Err(ValidationError::new(
                ValidationCode::InvalidValue,
                "invalid funding authorization",
            ));
        }
        Ok(Self {
            chain_id,
            token,
            domain_name,
            domain_version,
            owner,
            recipient,
            value,
            valid_after,
            valid_before,
            nonce,
        })
    }
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }
    pub fn token(&self) -> Address {
        self.token
    }
    pub fn domain_name(&self) -> &str {
        &self.domain_name
    }
    pub fn domain_version(&self) -> &str {
        &self.domain_version
    }
    pub fn owner(&self) -> Address {
        self.owner
    }
    pub fn recipient(&self) -> Address {
        self.recipient
    }
    pub fn value(&self) -> U256 {
        self.value
    }
    pub fn valid_after(&self) -> U256 {
        self.valid_after
    }
    pub fn valid_before(&self) -> U256 {
        self.valid_before
    }
    pub fn nonce(&self) -> B256 {
        self.nonce
    }
}

/// Unavailable wallet evidence. Known identifiers should instead be returned in
/// an Unknown OperationResult so they remain available for reconciliation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalletError;
impl core::fmt::Display for WalletError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("wallet operation evidence unavailable")
    }
}
impl std::error::Error for WalletError {}

/// Executes and reconciles the original exact call bound to `operation_ref`.
/// A hashless `Failed` result attests that submission definitively never began,
/// and leaves no binding: the runtime may execute that reference again with a
/// newly prepared call, as a close continues under its reference. A `Failed`
/// result with a transaction hash, mined and reverted, stays bound to that
/// transaction: a close then continues under a successor reference.
/// Once submission was attempted, service-reported failure without canonical
/// transaction evidence remains `Unknown`; it must not authorize a replacement.
/// The runtime may call `execute` and `reconcile` for different operations,
/// or the same one, at once (a recovery reads before taking the runtime's
/// locks), so an implementation serializes access to its own records, as
/// [`CircleAgentWallet`](crate::circle::CircleAgentWallet) does.
/// Every requirement is in docs/sdk-interfaces.md, Wallet requirements.
#[tonic::async_trait]
pub trait TransactionWallet: Send + Sync {
    fn address(&self) -> Address;
    async fn execute(
        &self,
        call: &WalletCall,
        operation_ref: &str,
    ) -> Result<OperationResult, WalletError>;
    async fn reconcile(&self, operation_ref: &str) -> Result<OperationResult, WalletError>;
    /// Forget the bindings of `operation_refs` once `at` has passed: from
    /// then on they are never presented again. The runtime passes each
    /// reference as soon as no session it covers needs it, with the time from
    /// which none does: when the last of them retired, or up to
    /// `closed_retention` ahead for one a later confirmed collection
    /// superseded. An implementation that forgets keeps the earliest `at`
    /// given with each as durably as the binding, so a restart does not lose
    /// it, and a `reconcile` of a reference, or its record gaining a
    /// transaction hash, withdraws its `at`: a recovery from an older
    /// checkpoint may present it again, and the runtime passes it again once
    /// no session needs it. Keep any binding whose call may have reached the
    /// wallet's service without a transaction to show for it.
    /// Keeping every binding, the default, is always safe.
    async fn retire(&self, _operation_refs: &[String], _at: SystemTime) -> Result<(), WalletError> {
        Ok(())
    }
    /// Whether the adapter's own records are near the bound past which it
    /// refuses new calls as never submitted. The runtime reads it on every
    /// maintenance pass, so it must not block, and reports each rise as an
    /// `on_error` `HookError`. The default is never.
    fn near_capacity(&self) -> bool {
        false
    }
    /// The largest calldata, in bytes, the wallet accepts in one call. The
    /// runtime sizes each batch it builds to fit it, and refuses a larger
    /// batch before recording any intent. The default, 61,440, is the Circle
    /// adapter's bound. A wallet whose real limit is gas rather than bytes
    /// states a smaller size. The runtime refuses, at construction, a bound
    /// below one settlement call: 452 bytes.
    fn max_call_bytes(&self) -> usize {
        61_440
    }
}

/// An agent's funding wallet. Every requirement is in docs/sdk-interfaces.md,
/// Wallet requirements.
#[tonic::async_trait]
pub trait FundingWallet: TransactionWallet {
    async fn sign_funding_authorization(
        &self,
        authorization: &FundingAuthorization,
    ) -> Result<Vec<u8>, WalletError>;
}

/// Fixed ECDSA session credential, separate from funding-wallet authority.
/// Implementations return the MPPI 65-byte low-s r || s || v profile.
#[tonic::async_trait]
pub trait SessionSigner: Send + Sync {
    fn address(&self) -> Address;
    async fn sign_voucher(
        &self,
        domain: &crate::types::Domain,
        session_id: B256,
        cumulative_amount: u64,
    ) -> Result<Vec<u8>, WalletError>;
    async fn sign_run(
        &self,
        domain: &crate::types::Domain,
        context: &crate::types::RunContext,
    ) -> Result<Vec<u8>, WalletError>;
    async fn sign_close(
        &self,
        domain: &crate::types::Domain,
        session_id: B256,
    ) -> Result<Vec<u8>, WalletError>;
    async fn sign_reconnect(
        &self,
        domain: &crate::types::Domain,
        session_id: B256,
        nonce: B256,
    ) -> Result<Vec<u8>, WalletError>;
}
