//! Provider command composition using an authenticated Circle Agent Wallet CLI.
//! Provider execution belongs to an explicitly selected trusted host.

use crate::{
    chain::{ChainError, ChainReader, DeploymentProfile},
    circle::{CircleAgentWallet, CircleOptions},
    config::{MAX_CONFIG_BYTES, ProviderConfig, ProviderDeclarations},
    deployment::{load_profile, official_mainnet_profile, official_testnet_profile},
    http::{HttpJsonRpc, HttpTransport, MAX_RPC_RESPONSE_BYTES},
    local_state::LocalState,
    onboarding::{
        OnboardingError, OnboardingOptions, OnboardingProgress, SignedRecipientProof, onboard,
    },
    registry::RegistryReader,
    verify::{VerifyPolicy, verify_endpoint},
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

const HELP: &str = "MPPI provider SDK\n\nUsage: mppi provider COMMAND [OPTIONS]\n\nCommands: init, onboard, update, serve, verify\n\nCommon: --config PATH (default mppi.toml), --profile PATH (override packaged network profile),\n        --mainnet | --testnet (override chain.network),\n        --rpc-env NAME (RPC secret reference), --timeout-ms N (default 30000)\n\nInit:    (--mainnet | --testnet) [--declarations PATH] (default provider-declarations.toml).\n         Creates an editable template without identity; never overwrites a file.\nOnboard: --declarations PATH (initial provider TOML without identity),\n         --wallet ADDRESS (authenticated Circle Agent Wallet), --provider-id UINT256 (adopt identity),\n         --recipient-proof PATH (JSON request and signature).\n         Missing recipient proof is emitted as JSON; sign with the recipient wallet\n         and rerun. Pending operations are checked on the same command.\nUpdate:  --changes PATH (explicit JSON changes), --recipient-proof PATH.\nServe:   --host ABSOLUTE_EXECUTABLE (your trusted SDK integration),\n         --upstream URL (this process only). Host receives config/profile references.\nVerify:  --ca PATH (explicit TLS trust bundle; defaults to system roots).\n\nWallet: install @circle-fin/cli@1.1.4; circle wallet login EMAIL (mainnet),\n        circle wallet login EMAIL --testnet (testnet); select its ARC wallet address.\n        Sessions expire after 28 days; reauthenticate then rerun the retained operation.\nExit: 0 complete; 1 invalid/unavailable; 2 pending or recipient proof required.\n";

/// A bounded CLI failure. A failed chain read keeps its typed cause, which
/// carries no endpoint, credential or remote text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CliError {
    message: &'static str,
    cause: Option<ChainError>,
}
impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.cause {
            Some(cause) => write!(f, "{}: {cause}", self.message),
            None => f.write_str(self.message),
        }
    }
}
impl std::error::Error for CliError {}
impl From<&'static str> for CliError {
    fn from(message: &'static str) -> Self {
        Self {
            message,
            cause: None,
        }
    }
}
impl From<OnboardingError> for CliError {
    fn from(error: OnboardingError) -> Self {
        Self {
            message: error.message(),
            cause: error.cause(),
        }
    }
}
type Result<T> = std::result::Result<T, CliError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Network {
    Mainnet,
    Testnet,
}
impl Network {
    fn name(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
        }
    }
}

#[derive(Clone, Debug)]
struct Arguments {
    command: String,
    flags: BTreeMap<String, OsString>,
    network: Option<Network>,
}
impl Arguments {
    fn parse(mut args: impl Iterator<Item = OsString>) -> Result<Self> {
        if args.next().as_deref() != Some(std::ffi::OsStr::new("provider")) {
            return Err(CliError::from("expected mppi provider COMMAND; use --help"));
        }
        let command = args
            .next()
            .and_then(|s| s.into_string().ok())
            .ok_or(CliError::from("missing provider command"))?;
        let specific: &[&str] = match command.as_str() {
            "init" => &["--declarations"],
            "onboard" => &[
                "--declarations",
                "--wallet",
                "--provider-id",
                "--recipient-proof",
            ],
            "update" => &["--changes", "--recipient-proof"],
            "serve" => &["--host", "--upstream"],
            "verify" => &["--ca"],
            _ => return Err(CliError::from("unknown provider command; use --help")),
        };
        let mut flags = BTreeMap::new();
        let mut network = None;
        while let Some(arg) = args.next() {
            let name = arg.to_str().ok_or(CliError::from("invalid option name"))?;
            if matches!(name, "--mainnet" | "--testnet") {
                let selected = if name == "--mainnet" {
                    Network::Mainnet
                } else {
                    Network::Testnet
                };
                if let Some(previous) = network {
                    return Err(CliError::from(if previous == selected {
                        "duplicate option"
                    } else {
                        "--mainnet and --testnet are mutually exclusive"
                    }));
                }
                network = Some(selected);
                continue;
            }
            if !(specific.contains(&name)
                || command != "init"
                    && ["--config", "--profile", "--rpc-env", "--timeout-ms"].contains(&name))
            {
                return Err(CliError::from("unknown option; use --help"));
            }
            let value = args
                .next()
                .filter(|v| !v.is_empty())
                .ok_or(CliError::from("option requires a value"))?;
            if flags.insert(name.to_owned(), value).is_some() {
                return Err(CliError::from("duplicate option"));
            }
        }
        if command == "init" && network.is_none() {
            return Err(CliError::from("init requires --mainnet or --testnet"));
        }
        Ok(Self {
            command,
            flags,
            network,
        })
    }
    fn string(&self, key: &str) -> Result<Option<&str>> {
        self.flags
            .get(key)
            .map(|s| s.to_str().ok_or(CliError::from("option requires UTF-8")))
            .transpose()
    }
    fn path(&self, key: &str) -> Option<PathBuf> {
        self.flags.get(key).map(PathBuf::from)
    }
    fn config_path(&self) -> PathBuf {
        self.path("--config").unwrap_or_else(|| "mppi.toml".into())
    }
    fn timeout(&self) -> Result<u64> {
        let value = self
            .string("--timeout-ms")?
            .unwrap_or("30000")
            .parse::<u64>()
            .map_err(|_| CliError::from("invalid timeout"))?;
        if !(1..=60_000).contains(&value) {
            return Err(CliError::from(
                "timeout must be 1 through 60000 milliseconds",
            ));
        }
        Ok(value)
    }
}

fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = File::open(path).map_err(|_| CliError::from("cannot read selected input file"))?;
    if !file
        .metadata()
        .map_err(|_| CliError::from("cannot inspect input file"))?
        .is_file()
    {
        return Err(CliError::from("input must be a regular file"));
    }
    let mut raw = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut raw)
        .map_err(|_| CliError::from("cannot read selected input file"))?;
    if raw.len() > limit {
        return Err(CliError::from("input file exceeds its size limit"));
    }
    Ok(raw)
}

fn initialize_declarations(path: &Path, network: Network) -> Result<()> {
    // create_new atomically rejects existing files and dangling symlinks:
    // https://doc.rust-lang.org/std/fs/struct.OpenOptions.html#method.create_new
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|_| {
        CliError::from("declarations output must be a new file in an existing directory")
    })?;
    let template = include_str!("provider-declarations.toml").replacen(
        "network = \"testnet\"",
        &format!("network = \"{}\"", network.name()),
        1,
    );
    file.write_all(template.as_bytes())
        .and_then(|_| file.sync_all())
        .map_err(|_| CliError::from("declarations write incomplete; inspect the selected file"))?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| {
            CliError::from("declarations durability uncertain; inspect the selected file")
        })?;
    Ok(())
}
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}
fn absolute(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(std::env::current_dir()
            .map_err(|_| CliError::from("working directory unavailable"))?
            .join(path))
    }
}
fn random_key() -> Result<String> {
    let mut raw = [0u8; 16];
    getrandom::fill(&mut raw).map_err(|_| CliError::from("secure randomness unavailable"))?;
    raw[6] = (raw[6] & 15) | 64;
    raw[8] = (raw[8] & 63) | 128;
    let value = alloy_primitives::hex::encode(raw);
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &value[..8],
        &value[8..12],
        &value[12..16],
        &value[16..20],
        &value[20..]
    ))
}

/// Configuration passed to the provider-owned standalone executable. The host
/// constructs its native integrations and calls `standalone::serve`.
pub struct HostConfiguration {
    pub config: ProviderConfig,
    pub profile: DeploymentProfile,
    pub chain: Arc<ChainReader>,
}
pub fn host_configuration(args: impl IntoIterator<Item = OsString>) -> Result<HostConfiguration> {
    let prefix = [OsString::from("provider"), OsString::from("serve")];
    let args = Arguments::parse(prefix.into_iter().chain(args))?;
    if args.flags.contains_key("--host") {
        return Err(CliError::from(
            "host integration does not recursively launch another host",
        ));
    }
    let config = load_config(&args)?;
    let profile = profile(&args, config.chain().chain_id())?;
    let chain = chain(&args, config.chain().rpc_url(), profile.clone())?;
    Ok(HostConfiguration {
        config,
        profile,
        chain,
    })
}
fn load_config(args: &Arguments) -> Result<ProviderConfig> {
    let mut overrides = json!({});
    if let Some(network) = args.network {
        overrides["chain"] = json!({"network":network.name()});
    }
    if let Some(url) = args.string("--upstream")? {
        overrides["upstream"] = json!({"url":url});
    }
    ProviderConfig::from_file(args.config_path(), Some(&overrides)).map_err(|_| CliError::from("invalid provider configuration; check fields, exact quoted amounts and selected network"))
}
fn profile(args: &Arguments, chain_id: u64) -> Result<DeploymentProfile> {
    let profile = match args.path("--profile") {
        Some(path) => load_profile(path),
        None if chain_id == 5042 => official_mainnet_profile(),
        None if chain_id == 5_042_002 => official_testnet_profile(),
        None => return Err(CliError::from("unsupported configuration network")),
    }
    .map_err(|_| CliError::from("invalid deployment profile"))?;
    if profile.domain().chain_id() != chain_id {
        return Err(CliError::from(
            "configuration network differs from selected deployment profile",
        ));
    }
    Ok(profile)
}
fn chain(
    args: &Arguments,
    default_url: &str,
    profile: DeploymentProfile,
) -> Result<Arc<ChainReader>> {
    let endpoint = if let Some(name) = args.string("--rpc-env")? {
        if name.is_empty()
            || !name
                .bytes()
                .enumerate()
                .all(|(i, c)| c == b'_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()))
        {
            return Err(CliError::from("invalid RPC environment variable name"));
        }
        std::env::var(name)
            .map_err(|_| CliError::from("selected RPC environment variable is missing"))?
    } else {
        default_url.to_owned()
    };
    let transport = HttpTransport::new(args.timeout()?, MAX_RPC_RESPONSE_BYTES, None)
        .map_err(|_| CliError::from("invalid RPC transport policy"))?;
    let rpc = HttpJsonRpc::new(endpoint, transport)
        .map_err(|_| CliError::from("invalid RPC endpoint"))?;
    Ok(Arc::new(ChainReader::new(Arc::new(rpc), profile)))
}

fn launch_host(args: &Arguments) -> Result<()> {
    let host=args.path("--host").ok_or(CliError::from("serve requires --host with your provider integration executable; see the standalone SDK example"))?;
    if !host.is_absolute() || !host.is_file() {
        return Err(CliError::from(
            "--host must be an absolute regular executable path",
        ));
    }
    if host.canonicalize().ok()
        == std::env::current_exe()
            .ok()
            .and_then(|p| p.canonicalize().ok())
    {
        return Err(CliError::from(
            "--host must be your integration executable, not mppi",
        ));
    }
    let config = load_config(args)?;
    profile(args, config.chain().chain_id())?;
    if config.listen().is_none() {
        return Err(CliError::from(
            "serve requires explicit listener and TLS configuration",
        ));
    }
    let mut command = host_command(args, &host)?;
    // Exec before creating a Tokio runtime. Signals and exit status belong to
    // the actual provider host, and there is no hidden child supervisor.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let _ = command.exec();
        Err(CliError::from(
            "provider integration executable could not start",
        ))
    }
    #[cfg(not(unix))]
    {
        let _ = command;
        Err(CliError::from(
            "standalone host launch currently requires Unix; embed the SDK on this platform",
        ))
    }
}

fn host_command(args: &Arguments, host: &Path) -> Result<std::process::Command> {
    let mut command = std::process::Command::new(host);
    command.arg("--config").arg(absolute(&args.config_path())?);
    if let Some(path) = args.path("--profile") {
        command.arg("--profile").arg(absolute(&path)?);
    }
    for key in ["--rpc-env", "--upstream", "--timeout-ms"] {
        if let Some(value) = args.flags.get(key) {
            command.arg(key).arg(value);
        }
    }
    if let Some(network) = args.network {
        command.arg(format!("--{}", network.name()));
    }
    Ok(command)
}

fn signed_proof(args: &Arguments) -> Result<Option<SignedRecipientProof>> {
    let Some(path) = args.path("--recipient-proof") else {
        return Ok(None);
    };
    let value = crate::json::parse_document(
        &read_bounded(&path, MAX_CONFIG_BYTES)?,
        MAX_CONFIG_BYTES,
        32,
    )
    .map_err(|_| CliError::from("invalid recipient proof JSON"))?;
    if value.as_object().map(|m| m.len()) != Some(2) || value.get("request").is_none() {
        return Err(CliError::from(
            "recipient proof requires request and signature fields",
        ));
    }
    let signature = value["signature"]
        .as_str()
        .and_then(|s| s.strip_prefix("0x"))
        .ok_or(CliError::from("recipient signature must be hexadecimal"))?;
    let signature = alloy_primitives::hex::decode(signature)
        .map_err(|_| CliError::from("invalid recipient signature"))?;
    SignedRecipientProof::new(value["request"].clone(), signature)
        .map(Some)
        .map_err(|_| CliError::from("invalid recipient proof"))
}

/// Process entry point. Diagnostics never echo external exceptions or supplied
/// argument values. Hosted application integration remains outside this binary.
pub fn main() -> i32 {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    if args.iter().any(|s| s == "--help" || s == "-h") {
        print!("{HELP}");
        return 0;
    }
    if args.as_slice() == [OsString::from("--version")] {
        println!("mppi {}", env!("CARGO_PKG_VERSION"));
        return 0;
    }
    let result = (|| -> Result<i32> {
        let args = Arguments::parse(args.into_iter())?;
        if args.command == "init" {
            let path = args
                .path("--declarations")
                .unwrap_or_else(|| PathBuf::from("provider-declarations.toml"));
            initialize_declarations(&path, args.network.expect("init requires a network"))?;
            println!("Created provider declarations; edit the placeholders before onboarding.");
            return Ok(0);
        }
        if args.command == "serve" {
            launch_host(&args)?;
            return Ok(0);
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| CliError::from("async runtime unavailable"))?;
        runtime.block_on(run(args))
    })();
    match result {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            1
        }
    }
}

async fn run(args: Arguments) -> Result<i32> {
    if args.command == "verify" {
        let config = load_config(&args)?;
        let profile = profile(&args, config.chain().chain_id())?;
        let chain = chain(&args, config.chain().rpc_url(), profile)?;
        let wallet = CircleAgentWallet::open_existing(
            chain.clone(),
            config.identity().wallet_address(),
            sidecar(&args.config_path(), ".wallet-operations.json"),
            CircleOptions::default(),
        )
        .map_err(|_| CliError::from("restore the original Circle Agent Wallet operation record"))?;
        wallet.verify_wallet().await.map_err(wallet_error)?;
        let ca = args
            .path("--ca")
            .map(|p| read_bounded(&p, MAX_CONFIG_BYTES))
            .transpose()?;
        let policy = VerifyPolicy::new(args.timeout()?, ca.as_deref())
            .map_err(|_| CliError::from("invalid verification TLS policy"))?;
        match verify_endpoint(&config, &chain, &policy).await {
            Ok(report) => {
                let limitations: Vec<_> = report
                    .not_tested()
                    .iter()
                    .map(|item| {
                        if *item == "wallet_access_and_transaction_submission" {
                            "transaction_submission"
                        } else {
                            *item
                        }
                    })
                    .collect();
                println!(
                    "{}",
                    json!({"complete":true,"operator_wallet_access":true,"checks":report.checks(),"limitations":limitations})
                );
                return Ok(0);
            }
            Err(error) => {
                eprintln!("{error}");
                return Ok(1);
            }
        }
    }
    let config_path = absolute(&args.config_path())?;
    // One command lock serializes onboard and update for this selected config.
    let _command = LocalState::open(sidecar(&config_path, ".command"), 1024).map_err(|_| {
        CliError::from("provider configuration is already in use or its local directory is unsafe")
    })?;
    if args.command == "onboard" {
        run_onboard(&args, &config_path).await
    } else {
        run_update(&args, &config_path).await
    }
}

fn wallet_error(error: crate::circle::CircleError) -> CliError {
    use crate::circle::CircleError;
    CliError::from(match error {
        CircleError::Authentication => {
            "Circle Agent Wallet session missing or expired; run circle wallet login EMAIL (mainnet) or circle wallet login EMAIL --testnet, then rerun with retained operation records"
        }
        CircleError::Version => "install the verified @circle-fin/cli@1.1.4",
        _ => {
            "Circle Agent Wallet unavailable or inconsistent; check selected wallet/network/login and retain original operation records"
        }
    })
}
fn selected_address(
    args: &Arguments,
    configured: Option<crate::Address>,
) -> Result<crate::Address> {
    let explicit = args
        .string("--wallet")?
        .map(crate::discovery::parse_address)
        .transpose()
        .map_err(|_| CliError::from("--wallet must be an EVM address"))?;
    if explicit.is_some() && configured.is_some() && explicit != configured {
        return Err(CliError::from(
            "selected Circle Agent Wallet differs from the established provider",
        ));
    }
    explicit.or(configured).ok_or(CliError::from(
        "initial onboarding requires --wallet with your authenticated Circle Agent Wallet address",
    ))
}

async fn run_onboard(args: &Arguments, path: &Path) -> Result<i32> {
    let existing = if path.exists() {
        Some(load_config(args)?)
    } else {
        None
    };
    let declarations = if let Some(input) = args.path("--declarations") {
        let mut mapping = crate::config::load_mapping(&input)
            .map_err(|_| CliError::from("invalid initial declarations"))?;
        if let Some(network) = args.network {
            mapping["chain"]["network"] = json!(network.name());
        }
        ProviderDeclarations::from_mapping(&mapping).map_err(|_| {
            CliError::from(
                "invalid initial declarations; omit identity until registration is confirmed",
            )
        })?
    } else if let Some(config) = &existing {
        let mut mapping = config.to_mapping();
        mapping
            .as_object_mut()
            .expect("configuration object")
            .remove("identity");
        ProviderDeclarations::from_mapping(&mapping)
            .map_err(|_| CliError::from("invalid saved provider declarations"))?
    } else {
        return Err(CliError::from(
            "initial onboarding requires --declarations with provider TOML excluding identity",
        ));
    };
    let profile = profile(args, declarations.chain().chain_id())?;
    let chain = chain(args, declarations.chain().rpc_url(), profile)?;
    let registry = Arc::new(RegistryReader::new(chain.clone()));
    registry
        .verified_block()
        .await
        .map_err(|_| CliError::from("selected deployment could not be verified"))?;
    let address = selected_address(
        args,
        existing.as_ref().map(|c| c.identity().wallet_address()),
    )?;
    if address == declarations.provider().recipient() {
        return Err(CliError::from(
            "recipient must differ from the provider operator wallet",
        ));
    }
    let adopted = args
        .string("--provider-id")?
        .map(crate::discovery::decimal_uint256)
        .transpose()
        .map_err(|_| CliError::from("provider id must be a canonical decimal uint256"))?;
    if adopted.is_some()
        && existing
            .as_ref()
            .is_some_and(|c| Some(c.identity().provider_id()) != adopted)
    {
        return Err(CliError::from(
            "selected identity differs from established provider",
        ));
    }
    let options = CircleOptions {
        allow_registration_activation: true,
        ..CircleOptions::default()
    };
    let wallet = Arc::new(
        CircleAgentWallet::new(
            chain,
            address,
            sidecar(path, ".wallet-operations.json"),
            options,
        )
        .map_err(wallet_error)?,
    );
    if existing.is_none() && adopted.is_none() {
        wallet
            .verify_registration_wallet()
            .await
            .map_err(wallet_error)?;
    } else {
        wallet.verify_wallet().await.map_err(wallet_error)?;
    }
    let result = onboard(
        &declarations,
        wallet.clone(),
        registry,
        OnboardingOptions {
            state_path: sidecar(path, ".onboarding.json"),
            adopted_identity: adopted
                .or_else(|| existing.as_ref().map(|c| c.identity().provider_id())),
            recipient_proof: signed_proof(args)?,
        },
    )
    .await
    .map_err(CliError::from)?;
    match result.progress() {
        OnboardingProgress::Complete => {
            wallet.verify_wallet().await.map_err(wallet_error)?;
            let config = result.config().ok_or(CliError::from(
                "confirmed provider configuration unavailable",
            ))?;
            write_configuration(path, config)?;
            println!(
                "{}",
                json!({"complete":true,"provider_id":config.identity().provider_id().to_string(),"wallet_address":format!("{address:#x}"),"endpoint_verified":false,"runtime_application_required":true})
            );
            Ok(0)
        }
        OnboardingProgress::RecipientProofRequired => {
            println!(
                "{}",
                json!({"complete":false,"recipient_proof_required":true,"request":result.recipient_proof()})
            );
            Ok(2)
        }
        OnboardingProgress::Pending => {
            println!(
                "{}",
                json!({"complete":false,"pending":true,"submitted":result.submitted(),"operation_ref":result.operation().map(|o|o.operation_ref()),"tx_hash":result.operation().and_then(|o|o.tx_hash()).map(|h|format!("{h:#x}"))})
            );
            Ok(2)
        }
    }
}

fn write_configuration(path: &Path, config: &ProviderConfig) -> Result<()> {
    let raw = config.to_toml().map_err(|_| {
        CliError::from("complete provider configuration cannot be represented as TOML")
    })?;
    if let Ok(existing) = read_bounded(path, MAX_CONFIG_BYTES)
        && existing == raw.as_bytes()
    {
        return Ok(());
    }
    atomic_file(path, raw.as_bytes())
}
fn atomic_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or(CliError::from("output directory unavailable"))?;
    let temp = parent.join(format!(".mppi-{}.tmp", random_key()?));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| {
        let mut file = options
            .open(&temp)
            .map_err(|_| CliError::from("cannot prepare complete configuration replacement"))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| CliError::from("cannot durably write configuration"))?;
        fs::rename(&temp, path).map_err(|_| CliError::from("cannot replace configuration"))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| CliError::from("configuration replacement durability uncertain"))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

async fn run_update(args: &Arguments, path: &Path) -> Result<i32> {
    use crate::{
        onboarding::refresh_completed_onboarding,
        provider_update::{UpdateChanges, UpdateOptions, update},
    };
    let config = load_config(args)?;
    let input = args.path("--changes").ok_or(CliError::from(
        "update requires --changes containing only explicitly requested fields",
    ))?;
    let raw = read_bounded(&input, MAX_CONFIG_BYTES)?;
    let mut mapping = crate::json::parse_document(&raw, MAX_CONFIG_BYTES, 32)
        .map_err(|_| CliError::from("invalid update JSON"))?;
    normalize_update_paths(&mut mapping, path)?;
    let changes = UpdateChanges::from_mapping(&mapping)
        .map_err(|_| CliError::from("invalid explicit update fields"))?;
    let profile = profile(args, config.chain().chain_id())?;
    let registry = Arc::new(RegistryReader::new(chain(
        args,
        config.chain().rpc_url(),
        profile,
    )?));
    registry
        .verified_block()
        .await
        .map_err(|_| CliError::from("selected deployment could not be verified"))?;
    let wallet = Arc::new(
        CircleAgentWallet::open_existing(
            registry.chain().clone(),
            config.identity().wallet_address(),
            sidecar(path, ".wallet-operations.json"),
            CircleOptions::default(),
        )
        .map_err(wallet_error)?,
    );
    wallet.verify_wallet().await.map_err(wallet_error)?;
    let result = update(
        &config,
        &changes,
        wallet,
        registry.clone(),
        UpdateOptions {
            state_path: sidecar(path, ".update.json"),
            recipient_proof: signed_proof(args)?,
        },
    )
    .await
    .map_err(CliError::from)?;
    match result.progress() {
        OnboardingProgress::Complete => {
            let applied = result
                .config()
                .ok_or(CliError::from("confirmed update configuration unavailable"))?;
            write_configuration(path, applied)?;
            let onboarding = sidecar(path, ".onboarding.json");
            if onboarding.exists() {
                refresh_completed_onboarding(applied,onboarding,&registry).await.map_err(|_|CliError::from("configuration saved; completed onboarding record still needs reconciliation"))?;
            }
            println!(
                "{}",
                json!({"complete":true,"provider_id":applied.identity().provider_id().to_string(),"runtime_application_required":true})
            );
            Ok(0)
        }
        OnboardingProgress::RecipientProofRequired => {
            println!(
                "{}",
                json!({"complete":false,"recipient_proof_required":true,"request":result.recipient_proof()})
            );
            Ok(2)
        }
        OnboardingProgress::Pending => {
            println!(
                "{}",
                json!({"complete":false,"pending":true,"operation_ref":result.operation().map(|o|o.operation_ref()),"tx_hash":result.operation().and_then(|o|o.tx_hash()).map(|h|format!("{h:#x}"))})
            );
            Ok(2)
        }
    }
}

fn normalize_update_paths(mapping: &mut serde_json::Value, config_path: &Path) -> Result<()> {
    if let Some(listener) = mapping
        .get_mut("listen")
        .and_then(serde_json::Value::as_object_mut)
    {
        for field in ["tls_cert", "tls_key"] {
            if let Some(value) = listener.get_mut(field).filter(|v| !v.is_null()) {
                let raw = value
                    .as_str()
                    .ok_or(CliError::from("TLS path must be a string"))?;
                let target = Path::new(raw);
                if target.is_relative() {
                    let target = config_path
                        .parent()
                        .ok_or(CliError::from("configuration directory unavailable"))?
                        .join(target);
                    *value = json!(
                        target
                            .to_str()
                            .ok_or(CliError::from("TLS path must be UTF-8"))?
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(raw: &[&str]) -> Result<Arguments> {
        Arguments::parse(raw.iter().map(OsString::from))
    }
    fn edited_template() -> String {
        include_str!("provider-declarations.toml")
            .replace("REPLACE_HTTPS_PROVIDER_ORIGIN", "https://provider.example")
            .replace("REPLACE_MODEL_ID", "provider/model")
            .replace(
                "REPLACE_RECIPIENT_EVM_ADDRESS",
                "0x2222222222222222222222222222222222222222",
            )
            .replace("REPLACE_INPUT_RATE", "300000")
            .replace("REPLACE_OUTPUT_RATE", "1200000")
    }
    #[test]
    fn command_failures_keep_a_sanitized_chain_cause() {
        let failed = crate::onboarding::read_failed("provider registry identity is unavailable");
        let error = CliError::from(failed(ChainError::Transport(
            crate::http::TransportError::Status(503),
        )));
        assert_eq!(
            error.to_string(),
            "provider registry identity is unavailable: chain RPC unavailable: JSON HTTP endpoint returned status 503"
        );
        assert_eq!(
            CliError::from("invalid RPC endpoint").to_string(),
            "invalid RPC endpoint"
        );
    }
    #[test]
    fn packaged_profile_follows_resolved_configuration_network() {
        for command in ["onboard", "update", "serve", "verify"] {
            for (network, chain_id, expected) in [
                ("--testnet", 5_042_002, official_testnet_profile().unwrap()),
                ("--mainnet", 5042, official_mainnet_profile().unwrap()),
            ] {
                for input in [
                    vec!["provider", command],
                    vec!["provider", command, network],
                ] {
                    let args = parse(&input).unwrap();
                    assert_eq!(profile(&args, chain_id).unwrap(), expected);
                    assert!(profile(&args, 1).is_err());
                }
                let explicit = parse(&[
                    "provider",
                    command,
                    network,
                    "--profile",
                    "missing-profile.json",
                ])
                .unwrap();
                // An explicit profile never silently falls back to packaged pins.
                assert!(profile(&explicit, chain_id).is_err());
            }
        }
    }

    #[test]
    fn network_flags_are_mutually_exclusive_for_every_command() {
        for command in ["init", "onboard", "update", "serve", "verify"] {
            for (first, second) in [
                ("--mainnet", "--testnet"),
                ("--testnet", "--mainnet"),
                ("--mainnet", "--mainnet"),
                ("--testnet", "--testnet"),
            ] {
                assert!(parse(&["provider", command, first, second]).is_err());
            }
        }
    }

    #[test]
    fn standalone_host_receives_same_implicit_or_explicit_profile_selection() {
        let directory = std::env::temp_dir().join(format!("mppi-host-{}", random_key().unwrap()));
        fs::create_dir(&directory).unwrap();
        let config = directory.join("mppi.toml");
        let mut template = edited_template().replace("max_age_ms = 60000", "max_age_ms = 1000");
        template.push_str("\n[identity]\nprovider_id = \"42\"\nwallet_address = \"0x1111111111111111111111111111111111111111\"\n");
        for network in [Network::Testnet, Network::Mainnet] {
            let expected = match network {
                Network::Testnet => official_testnet_profile().unwrap(),
                Network::Mainnet => official_mainnet_profile().unwrap(),
            };
            for network_override in [false, true] {
                let stored_network = if network_override {
                    match network {
                        Network::Testnet => Network::Mainnet,
                        Network::Mainnet => Network::Testnet,
                    }
                } else {
                    network
                };
                let content = template.replace(
                    "network = \"testnet\"",
                    &format!("network = \"{}\"", stored_network.name()),
                );
                fs::write(&config, &content).unwrap();
                for explicit in [false, true] {
                    let mut input = vec![
                        OsString::from("provider"),
                        OsString::from("serve"),
                        OsString::from("--config"),
                        config.as_os_str().to_owned(),
                    ];
                    if network_override {
                        input.push(OsString::from(format!("--{}", network.name())));
                    }
                    if explicit {
                        let pins = directory.join("deployment.json");
                        fs::write(
                            &pins,
                            serde_json::to_vec(&crate::deployment::profile_to_mapping(&expected))
                                .unwrap(),
                        )
                        .unwrap();
                        input.extend([OsString::from("--profile"), pins.as_os_str().to_owned()]);
                    }
                    let args = Arguments::parse(input.into_iter()).unwrap();
                    let selected_config = load_config(&args).unwrap();
                    assert_eq!(
                        profile(&args, selected_config.chain().chain_id()).unwrap(),
                        expected
                    );
                    let command = host_command(&args, Path::new("/provider/host")).unwrap();
                    let forwarded: Vec<_> = command.get_args().map(|s| s.to_os_string()).collect();
                    assert_eq!(forwarded.iter().any(|s| s == "--profile"), explicit);
                    assert_eq!(
                        forwarded
                            .iter()
                            .any(|s| s == "--testnet" || s == "--mainnet"),
                        network_override
                    );
                    let host = host_configuration(forwarded).unwrap();
                    assert_eq!(host.profile, expected);
                    assert_eq!(host.chain.profile(), &host.profile);
                    assert_eq!(host.config.chain().network(), network.name());
                    assert_eq!(host.config.chain().max_age_ms(), 1000);
                    assert_eq!(fs::read_to_string(&config).unwrap(), content);
                    if explicit {
                        let other: &[u8] = match network {
                            Network::Mainnet => include_bytes!("arc-testnet.json"),
                            Network::Testnet => include_bytes!("arc-mainnet.json"),
                        };
                        fs::write(args.path("--profile").unwrap(), other).unwrap();
                        assert!(
                            host_configuration(command.get_args().map(OsString::from)).is_err()
                        );
                    }
                }
            }
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn init_is_local_explicit_and_never_overwrites() {
        assert!(parse(&["provider", "init"]).is_err());
        assert!(parse(&["provider", "init", "--testnet", "--config", "mppi.toml"]).is_err());
        assert!(parse(&["provider", "init", "--testnet", "--profile", "pins.json"]).is_err());
        let directory = std::env::temp_dir().join(format!("mppi-init-{}", random_key().unwrap()));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("provider-declarations.toml");
        initialize_declarations(&path, Network::Testnet).unwrap();
        let original = fs::read(&path).unwrap();
        assert_eq!(original, include_bytes!("provider-declarations.toml"));
        let template = String::from_utf8(original.clone()).unwrap();
        let mapping: toml::Value = toml::from_str(&template).unwrap();
        assert!(mapping.get("identity").is_none());
        assert!(
            crate::config::ProviderDeclarations::from_mapping(
                &serde_json::to_value(&mapping).unwrap()
            )
            .is_err()
        );
        let edited = edited_template();
        let mapping: toml::Value = toml::from_str(&edited).unwrap();
        let declarations =
            ProviderDeclarations::from_mapping(&serde_json::to_value(mapping).unwrap()).unwrap();
        assert_eq!(declarations.chain().max_age_ms(), 60_000);
        assert_eq!(
            declarations.to_mapping()["payment"]["suggest_session_tokens"],
            10_000
        );
        assert!(initialize_declarations(&path, Network::Mainnet).is_err());
        assert_eq!(fs::read(&path).unwrap(), original);
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let linked = directory.join("linked.toml");
            symlink(&path, &linked).unwrap();
            assert!(initialize_declarations(&linked, Network::Mainnet).is_err());
            assert_eq!(fs::read(&path).unwrap(), original);
            let missing = directory.join("missing.toml");
            let dangling = directory.join("dangling.toml");
            symlink(&missing, &dangling).unwrap();
            assert!(initialize_declarations(&dangling, Network::Mainnet).is_err());
            assert!(!missing.exists());
        }
        assert!(!directory.join("mppi.toml").exists());
        let mainnet_path = directory.join("mainnet-declarations.toml");
        let args = parse(&["provider", "init", "--mainnet"]).unwrap();
        initialize_declarations(&mainnet_path, args.network.unwrap()).unwrap();
        let mainnet = fs::read_to_string(&mainnet_path).unwrap();
        assert_eq!(
            mainnet,
            template.replacen("network = \"testnet\"", "network = \"mainnet\"", 1)
        );
        let mapping: toml::Value = toml::from_str(&mainnet).unwrap();
        assert_eq!(mapping["chain"]["network"].as_str(), Some("mainnet"));
        assert!(mapping.get("identity").is_none());
        assert!(initialize_declarations(&mainnet_path, Network::Testnet).is_err());
        assert_eq!(fs::read_to_string(&mainnet_path).unwrap(), mainnet);
        fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn command_fields_are_explicit_and_secret_values_never_diagnosed() {
        assert!(
            parse(&[
                "provider",
                "update",
                "--profile",
                "p",
                "--upstream",
                "https://secret"
            ])
            .is_err()
        );
        assert!(parse(&["provider", "onboard", "--profile", "p", "--profile", "q"]).is_err());
        assert!(
            parse(&[
                "provider",
                "serve",
                "--profile",
                "p",
                "--testnet",
                "--testnet"
            ])
            .is_err()
        );
        let err = parse(&["provider", "verify", "--api-key-secret"]).unwrap_err();
        assert!(!err.to_string().contains("api-key-secret"));
        let args = parse(&[
            "provider",
            "serve",
            "--profile",
            "p",
            "--config",
            "some file.toml",
            "--testnet",
            "--upstream",
            "http://localhost:8000/custom/v1",
        ])
        .unwrap();
        assert_eq!(args.network, Some(Network::Testnet));
        assert_eq!(args.config_path(), PathBuf::from("some file.toml"));
    }
    #[test]
    fn mutation_keys_have_required_v4_shape() {
        let a = random_key().unwrap();
        let b = random_key().unwrap();
        assert_ne!(a, b);
        assert_eq!(a.len(), 36);
        assert_eq!(&a[14..15], "4");
        assert!([b'8', b'9', b'a', b'b'].contains(&a.as_bytes()[19]));
    }

    #[test]
    fn explicit_update_tls_paths_use_selected_configuration_directory() {
        let mut changes = json!({"listen":{"tls_cert":"certs/leaf.pem","tls_key":"/private/server.key"},"upstream":{"url":"http://localhost/v1"}});
        normalize_update_paths(&mut changes, Path::new("/provider/mppi.toml")).unwrap();
        assert_eq!(changes["listen"]["tls_cert"], "/provider/certs/leaf.pem");
        assert_eq!(changes["listen"]["tls_key"], "/private/server.key");
        assert_eq!(changes["upstream"]["url"], "http://localhost/v1");
        let original = changes.clone();
        normalize_update_paths(&mut changes, Path::new("/provider/mppi.toml")).unwrap();
        assert_eq!(changes, original);
    }

    #[test]
    fn agent_wallet_address_selection_rejects_obsolete_credentials_and_identity_changes() {
        let address = crate::Address::repeat_byte(1);
        let args = parse(&["provider", "onboard", "--wallet", &format!("{address:#x}")]).unwrap();
        assert_eq!(selected_address(&args, None).unwrap(), address);
        assert_eq!(selected_address(&args, Some(address)).unwrap(), address);
        assert!(selected_address(&args, Some(crate::Address::repeat_byte(2))).is_err());
        let saved = parse(&["provider", "onboard"]).unwrap();
        assert_eq!(selected_address(&saved, Some(address)).unwrap(), address);
        assert!(selected_address(&saved, None).is_err());
        for obsolete in [
            "--wallet-id",
            "--wallet-set-id",
            "--api-key",
            "--entity-secret",
        ] {
            assert!(parse(&["provider", "onboard", obsolete, "anything"]).is_err());
        }
    }
}
