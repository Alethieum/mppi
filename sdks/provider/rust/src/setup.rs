//! Compose an onboarded provider runtime from saved host configuration.
//! Setup checks network/account evidence but never provisions a wallet, publishes
//! identity, starts a listener, or starts the runtime. Provider callbacks and the
//! existing `ProviderLifecycle` and standalone serving APIs retain their roles.

use std::{path::Path, sync::Arc};

use alloy_primitives::{B256, U256, keccak256};
use alloy_sol_types::{SolCall, SolValue};

use crate::{
    chain::{ChainReader, decode_return},
    circle::{CircleClient, CircleCredentials, CircleDeveloperWallet},
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
#[derive(Clone)]
pub struct RuntimeOptions {
    pub circle_timeout_ms: u64,
    pub poll_interval_ms: Option<u64>,
    pub limits: RuntimeLimits,
    pub hooks: Hooks,
}
impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            circle_timeout_ms: 30_000,
            poll_interval_ms: None,
            limits: RuntimeLimits::default(),
            hooks: Hooks::default(),
        }
    }
}

/// Compose the existing payment runtime using the operator saved by onboarding.
/// `wallet_operation_path` must name its retained wallet operation record (the
/// CLI uses `<config>.wallet-operations.json`). Missing history is never replaced.
/// Credentials are supplied by the caller and never saved in provider config.
pub async fn runtime_from_config(
    host: HostConfiguration,
    credentials: CircleCredentials,
    wallet_operation_path: impl AsRef<Path>,
    coverage: Arc<dyn CoverageProvider>,
    execution: Arc<dyn ExecutionProvider>,
) -> Result<Mppi, SetupError> {
    runtime_from_config_with_options(
        host,
        credentials,
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
    credentials: CircleCredentials,
    wallet_operation_path: impl AsRef<Path>,
    coverage: Arc<dyn CoverageProvider>,
    execution: Arc<dyn ExecutionProvider>,
    options: RuntimeOptions,
) -> Result<Mppi, SetupError> {
    // Client construction performs no network I/O. All local selection checks in
    // compose precede its read-only Circle account lookup and chain verification.
    let client = Arc::new(
        CircleClient::new(credentials, options.circle_timeout_ms)
            .map_err(|_| SetupError("invalid Circle client configuration"))?,
    );
    compose(
        host,
        client,
        wallet_operation_path.as_ref(),
        coverage,
        execution,
        options,
    )
    .await
}

async fn compose(
    host: HostConfiguration,
    client: Arc<CircleClient>,
    wallet_operation_path: &Path,
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
    let wallet_id = config.identity().wallet_id().ok_or(SetupError(
        "saved provider configuration needs its Circle wallet id",
    ))?;
    if !std::fs::symlink_metadata(wallet_operation_path).is_ok_and(|metadata| metadata.is_file()) {
        return Err(SetupError(
            "restore the existing Circle wallet operation record",
        ));
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
    let identity = client
        .wallet(wallet_id, config.chain().chain_id())
        .await
        .map_err(|_| SetupError("saved Circle wallet is unavailable on the selected network"))?;
    if identity.address() != config.identity().wallet_address() {
        return Err(SetupError(
            "saved provider operator differs from Circle wallet identity",
        ));
    }
    let contract = verified_usdc_contract(&chain).await?;
    let wallet = Arc::new(
        CircleDeveloperWallet::open_existing(client, identity, wallet_operation_path).map_err(
            |_| SetupError("Circle wallet operation record unavailable or inconsistent"),
        )?,
    );
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
    chain.verify_deployment(&block).await.map_err(invalid)?;
    async fn read<C: SolCall>(
        chain: &ChainReader,
        call: C,
        block: &crate::chain::Block,
    ) -> Result<C::Return, SetupError> {
        let raw = chain
            .call_at(ARC_USDC, &call.abi_encode(), block)
            .await
            .map_err(|_| SetupError("Arc USDC authorization domain unavailable"))?;
        decode_return::<C>(&raw)
            .map_err(|_| SetupError("Arc USDC authorization domain is not canonical ABI"))
    }
    if read(chain, ArcUsdc::nameCall {}, &block).await? != "USDC"
        || read(chain, ArcUsdc::versionCall {}, &block).await? != "2"
        || read(chain, ArcUsdc::decimalsCall {}, &block).await? != 6
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
    if read(chain, ArcUsdc::DOMAIN_SEPARATORCall {}, &block).await? != expected {
        return Err(SetupError(
            "Arc USDC authorization domain separator is inconsistent",
        ));
    }
    chain.check_canonical(&block).await.map_err(invalid)?;
    ContractCalls::new(*chain.profile().domain(), "USDC".into(), "2".into())
        .map_err(|_| SetupError("Arc USDC contract configuration is invalid"))
}

#[cfg(test)]
#[path = "setup_tests.rs"]
mod tests;
