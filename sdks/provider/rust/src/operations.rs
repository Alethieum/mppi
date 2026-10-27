//! Explicit one-shot wallet submission and read-only reconciliation.
//! Unknown results retain available handles and never cause an automatic retry.

use crate::{
    capabilities::TransactionWallet,
    chain::{ChainError, ChainReader},
    contract::{OperationKind, PreparedOperation},
    types::{OperationResult, OperationState},
};

fn validate(
    wallet: &dyn TransactionWallet,
    operation: &PreparedOperation,
    operation_ref: &str,
    chain: &ChainReader,
) -> Result<(), ChainError> {
    if operation_ref.is_empty()
        || operation.call().chain_id() != chain.profile().domain().chain_id()
        || operation.call().to() != chain.profile().domain().verifying_contract()
    {
        return Err(ChainError::InvalidEvidence(
            "operation differs from selected deployment or reference is empty",
        ));
    }
    let address = wallet.address();
    if address.is_zero() {
        return Err(ChainError::InvalidEvidence(
            "wallet address must be nonzero",
        ));
    }
    for d in operation.descriptors() {
        let permitted = match operation.kind() {
            OperationKind::Open
            | OperationKind::TopUp
            | OperationKind::RequestClose
            | OperationKind::Withdraw => address == d.agent,
            OperationKind::ReleaseIdle => true,
            _ => address == d.recipient || address == d.operator,
        };
        if !permitted {
            return Err(ChainError::InvalidEvidence(
                "wallet lacks authority for operation descriptor",
            ));
        }
    }
    Ok(())
}
fn unknown(reference: &str) -> Result<OperationResult, ChainError> {
    OperationResult::new(
        reference.to_owned(),
        OperationState::Unknown,
        None,
        None,
        None,
        vec![],
    )
    .map_err(ChainError::from)
}
async fn result(
    response: OperationResult,
    operation: &PreparedOperation,
    reference: &str,
    chain: &ChainReader,
) -> Result<OperationResult, ChainError> {
    if response.tx_hash().is_some() {
        return chain
            .reconcile(
                operation,
                reference,
                response.tx_hash(),
                response.wallet_operation_id(),
            )
            .await;
    }
    Ok(response)
}

// Runtime callers persist this wallet-only result before starting chain I/O.
// Wallet observations and per-session assertions are never canonical evidence.
fn wallet_result(
    response: OperationResult,
    reference: &str,
) -> Result<OperationResult, ChainError> {
    if response.operation_ref() != reference {
        return unknown(reference);
    }
    let state = if response.tx_hash().is_some() {
        OperationState::Unknown
    } else {
        match response.state() {
            OperationState::Failed => OperationState::Failed,
            OperationState::Pending if response.wallet_operation_id().is_some() => {
                OperationState::Pending
            }
            _ => OperationState::Unknown,
        }
    };
    let result = OperationResult::new(
        reference,
        state,
        response.wallet_operation_id().map(str::to_owned),
        response.tx_hash(),
        None,
        vec![],
    )?;
    Ok(match response.cause() {
        Some(cause) => result.with_cause(cause)?,
        None => result,
    })
}

pub(crate) async fn submit_wallet_operation(
    wallet: &dyn TransactionWallet,
    operation: &PreparedOperation,
    reference: &str,
    chain: &ChainReader,
) -> Result<OperationResult, ChainError> {
    validate(wallet, operation, reference, chain)?;
    match wallet.execute(operation.call(), reference).await {
        Ok(response) => wallet_result(response, reference),
        Err(_) => unknown(reference),
    }
}

pub(crate) async fn reconcile_wallet_operation(
    wallet: &dyn TransactionWallet,
    operation: &PreparedOperation,
    reference: &str,
    chain: &ChainReader,
) -> Result<OperationResult, ChainError> {
    validate(wallet, operation, reference, chain)?;
    match wallet.reconcile(reference).await {
        Ok(response) => wallet_result(response, reference),
        Err(_) => unknown(reference),
    }
}

/// Execute exactly once. The adapter owns durable reference binding before any
/// submission; cancellation cannot establish that the transaction did not execute.
pub async fn submit_operation(
    wallet: &dyn TransactionWallet,
    operation: &PreparedOperation,
    operation_ref: &str,
    chain: &ChainReader,
) -> Result<OperationResult, ChainError> {
    result(
        submit_wallet_operation(wallet, operation, operation_ref, chain).await?,
        operation,
        operation_ref,
        chain,
    )
    .await
}
/// Query only the original wallet operation. Never invokes execute.
pub async fn reconcile_operation(
    wallet: &dyn TransactionWallet,
    operation: &PreparedOperation,
    operation_ref: &str,
    chain: &ChainReader,
) -> Result<OperationResult, ChainError> {
    result(
        reconcile_wallet_operation(wallet, operation, operation_ref, chain).await?,
        operation,
        operation_ref,
        chain,
    )
    .await
}
