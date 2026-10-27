//! Compose an onboarded provider runtime from saved host configuration.
//! Setup checks network/account evidence but never provisions a wallet, publishes
//! identity, starts a listener, or starts the runtime. Provider callbacks and the
//! existing `ProviderLifecycle` and standalone serving APIs retain their roles.

use std::{path::Path, sync::Arc};

use alloy_primitives::{B256, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};

use crate::{
    capabilities::TransactionWallet,
    chain::{ChainReader, data, decode_return, eth_call},
    circle::{CircleAgentWallet, CircleOptions},
    cli::HostConfiguration,
    contract::{ARC_USDC, ContractCalls},
    follower::ChainFollower,
    interfaces::{CoverageProvider, ExecutionProvider},
    runtime::{Hooks, Mppi, RuntimeLimits},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SetupError(&'static str);
impl std::fmt::Display for SetupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for SetupError {}

/// Optional composition controls. Freshness and collection margin always come
/// from the provider's saved configuration. Polling defaults to the follower's
/// existing fraction of freshness; callers may select a shorter interval.
#[derive(Clone, Default)]
pub struct RuntimeOptions {
    pub circle: CircleOptions,
    pub poll_interval_ms: Option<u64>,
    pub limits: RuntimeLimits,
    pub hooks: Hooks,
}
/// Compose the existing payment runtime using the operator saved by onboarding.
/// `wallet_operation_path` must name its retained wallet operation record (the
/// CLI uses `<config>.wallet-operations.json`). Missing history is never replaced.
/// Circle authentication remains in the official CLI session; no credentials are accepted.
pub async fn runtime_from_config(
    host: HostConfiguration,
    wallet_operation_path: impl AsRef<Path>,
    coverage: Arc<dyn CoverageProvider>,
    execution: Arc<dyn ExecutionProvider>,
) -> Result<Mppi, SetupError> {
    runtime_from_config_with_options(
        host,
        wallet_operation_path,
        coverage,
        execution,
        RuntimeOptions::default(),
    )
    .await
}

/// Compose with provider-selected hooks, retention limits and RPC polling.
pub async fn runtime_from_config_with_options(
    host: HostConfiguration,
    wallet_operation_path: impl AsRef<Path>,
    coverage: Arc<dyn CoverageProvider>,
    execution: Arc<dyn ExecutionProvider>,
    options: RuntimeOptions,
) -> Result<Mppi, SetupError> {
    let wallet = Arc::new(
        CircleAgentWallet::open_existing(
            host.chain.clone(),
            host.config.identity().wallet_address(),
            wallet_operation_path.as_ref(),
            options.circle.clone(),
        )
        .map_err(|_| SetupError("restore the existing Circle Agent Wallet operation record"))?,
    );
    compose(
        host,
        wallet.clone(),
        Some(&wallet),
        coverage,
        execution,
        options,
    )
    .await
}

/// Compose with another `TransactionWallet`, such as one whose operator key an
/// HSM or KMS holds. Its address must be the saved operator; the deployment and
/// USDC domain are verified as for Circle. Setup cannot verify another wallet's
/// account, network or access: the wallet owns those checks. `options.circle`
/// is not used.
pub async fn runtime_from_config_with_wallet(
    host: HostConfiguration,
    wallet: Arc<dyn TransactionWallet>,
    coverage: Arc<dyn CoverageProvider>,
    execution: Arc<dyn ExecutionProvider>,
    options: RuntimeOptions,
) -> Result<Mppi, SetupError> {
    compose(host, wallet, None, coverage, execution, options).await
}

/// `circle` is the same wallet when it is the Circle adapter, whose account,
/// network and CLI version are then verified; any other wallet is checked by
/// its address alone.
async fn compose(
    host: HostConfiguration,
    wallet: Arc<dyn TransactionWallet>,
    circle: Option<&CircleAgentWallet>,
    coverage: Arc<dyn CoverageProvider>,
    execution: Arc<dyn ExecutionProvider>,
    options: RuntimeOptions,
) -> Result<Mppi, SetupError> {
    let HostConfiguration {
        config,
        profile,
        chain,
    } = host;
    if config.chain().chain_id() != profile.domain().chain_id() || chain.profile() != &profile {
        return Err(SetupError(
            "provider configuration and deployment profile differ",
        ));
    }
    if wallet.address() != config.identity().wallet_address() {
        return Err(SetupError(if circle.is_some() {
            "saved provider operator differs from Circle Agent Wallet"
        } else {
            "saved provider operator differs from the supplied wallet"
        }));
    }
    let follower = Arc::new(
        ChainFollower::new(
            chain.as_ref().clone(),
            config.chain().max_age_ms(),
            config.payment().collection_margin_ms(),
            options.poll_interval_ms,
        )
        .map_err(|_| SetupError("invalid provider chain polling policy"))?,
    );
    if let Some(circle) = circle {
        circle.verify_wallet().await.map_err(|error| match error {
            crate::circle::CircleError::Authentication => SetupError("Circle Agent Wallet session missing or expired; run circle wallet login EMAIL (mainnet) or circle wallet login EMAIL --testnet, then rerun"),
            _ => SetupError("Circle Agent Wallet account, network or CLI version could not be verified"),
        })?;
    }
    let contract = verified_usdc_contract(&chain).await?;
    Mppi::with_limits_and_hooks(
        config,
        follower,
        contract,
        wallet,
        coverage,
        execution,
        options.limits,
        options.hooks,
    )
    .map_err(|_| SetupError("provider runtime configuration is inconsistent"))
}

alloy_sol_types::sol! {
    interface ArcUsdc {
        function name() external view returns (string);
        function version() external view returns (string);
        function decimals() external view returns (uint8);
        function DOMAIN_SEPARATOR() external view returns (bytes32);
    }
}

/// Check the same fixed USDC/2/6 authorization domain as the agent setup, at one
/// canonical block. Deployment verification retains all release code/slot pins.
/// Sources: https://docs.arc.io/arc/references/contract-addresses (USDC),
/// https://eips.ethereum.org/EIPS/eip-712#definition-of-domainseparator and
/// https://eips.ethereum.org/EIPS/eip-1898 (requireCanonical).
async fn verified_usdc_contract(chain: &ChainReader) -> Result<ContractCalls, SetupError> {
    let invalid =
        |_| SetupError("Arc USDC deployment or authorization domain could not be verified");
    let block = chain.block(None).await.map_err(invalid)?;
    let with_code = chain.code_unchecked();
    let mut calls = chain.deployment_calls(with_code);
    for data in [
        ArcUsdc::nameCall {}.abi_encode(),
        ArcUsdc::versionCall {}.abi_encode(),
        ArcUsdc::decimalsCall {}.abi_encode(),
        ArcUsdc::DOMAIN_SEPARATORCall {}.abi_encode(),
    ] {
        calls.push(eth_call(ARC_USDC, &data));
    }
    let results = chain.read(&block, calls).await.map_err(invalid)?;
    let results = chain
        .check_deployment(&results, with_code)
        .map_err(invalid)?;
    fn decode<C: SolCall>(raw: &serde_json::Value) -> Result<C::Return, SetupError> {
        data(raw)
            .and_then(|raw| decode_return::<C>(&raw))
            .map_err(|_| SetupError("Arc USDC authorization domain is not canonical ABI"))
    }
    if decode::<ArcUsdc::nameCall>(&results[0])? != "USDC"
        || decode::<ArcUsdc::versionCall>(&results[1])? != "2"
        || decode::<ArcUsdc::decimalsCall>(&results[2])? != 6
    {
        return Err(SetupError(
            "Arc USDC authorization domain differs from supported values",
        ));
    }
    let expected: B256 = keccak256((
        keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
        keccak256("USDC"), keccak256("2"),
        U256::from(chain.profile().domain().chain_id()), ARC_USDC,
    ).abi_encode());
    if decode::<ArcUsdc::DOMAIN_SEPARATORCall>(&results[3])? != expected {
        return Err(SetupError(
            "Arc USDC authorization domain separator is inconsistent",
        ));
    }
    ContractCalls::new(*chain.profile().domain(), "USDC".into(), "2".into())
        .map_err(|_| SetupError("Arc USDC contract configuration is invalid"))
}

#[cfg(test)]
#[path = "setup_tests.rs"]
mod tests;
