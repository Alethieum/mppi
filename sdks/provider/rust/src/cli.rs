//! Provider command composition. Secret values are accepted from the environment
//! only; provider execution belongs to an explicitly selected trusted host.

use crate::{
    chain::{Block, ChainReader, DeploymentProfile},
    circle::{CircleClient, CircleCredentials, CircleDeveloperWallet, CircleWalletIdentity},
    config::{MAX_CONFIG_BYTES, ProviderConfig, ProviderDeclarations},
    deployment::load_profile,
    http::{HttpJsonRpc, HttpTransport, MAX_RPC_RESPONSE_BYTES},
    local_state::LocalState,
    onboarding::{OnboardingOptions, OnboardingProgress, SignedRecipientProof, onboard},
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

const HELP: &str = "MPPI provider SDK\n\nUsage: mppi provider COMMAND [OPTIONS]\n\nCommands: onboard, update, serve, verify\n\nCommon: --config PATH (default mppi.toml), --profile PATH (required), --testnet,\n        --rpc-env NAME (RPC secret reference), --timeout-ms N (default 30000)\n\nOnboard: --declarations PATH (initial provider TOML without identity),\n         --wallet-id UUID (existing Circle SCA), --provider-id UINT256 (adopt identity),\n         --wallet-set-id UUID (resolve an interrupted wallet-set creation),\n         --recipient-proof PATH (JSON request and signature).\n         Missing recipient proof is emitted as JSON; sign with the recipient wallet\n         and rerun. Pending operations are checked on the same command.\nUpdate:  --changes PATH (explicit JSON changes), --recipient-proof PATH.\nServe:   --host ABSOLUTE_EXECUTABLE (your trusted SDK integration),\n         --upstream URL (this process only). Host receives config/profile references.\nVerify:  --ca PATH (explicit TLS trust bundle; defaults to system roots).\n\nSecrets: CIRCLE_API_KEY, CIRCLE_ENTITY_SECRET. Never put secret values in flags.\nExit: 0 complete; 1 invalid/unavailable; 2 pending or recipient proof required.\n";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CliError(&'static str);
impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for CliError {}
type Result<T> = std::result::Result<T, CliError>;

#[derive(Clone, Debug)]
struct Arguments {
    command: String,
    flags: BTreeMap<String, OsString>,
    testnet: bool,
}
impl Arguments {
    fn parse(mut args: impl Iterator<Item = OsString>) -> Result<Self> {
        if args.next().as_deref() != Some(std::ffi::OsStr::new("provider")) {
            return Err(CliError("expected mppi provider COMMAND; use --help"));
        }
        let command = args
            .next()
            .and_then(|s| s.into_string().ok())
            .ok_or(CliError("missing provider command"))?;
        let specific: &[&str] = match command.as_str() {
            "onboard" => &[
                "--declarations",
                "--wallet-id",
                "--wallet-set-id",
                "--provider-id",
                "--recipient-proof",
            ],
            "update" => &["--changes", "--recipient-proof"],
            "serve" => &["--host", "--upstream"],
            "verify" => &["--ca"],
            _ => return Err(CliError("unknown provider command; use --help")),
        };
        let mut flags = BTreeMap::new();
        let mut testnet = false;
        while let Some(arg) = args.next() {
            let name = arg.to_str().ok_or(CliError("invalid option name"))?;
            if name == "--testnet" {
                if testnet {
                    return Err(CliError("duplicate option"));
                }
                testnet = true;
                continue;
            }
            if !["--config", "--profile", "--rpc-env", "--timeout-ms"].contains(&name)
                && !specific.contains(&name)
            {
                return Err(CliError("unknown option; use --help"));
            }
            let value = args
                .next()
                .filter(|v| !v.is_empty())
                .ok_or(CliError("option requires a value"))?;
            if flags.insert(name.to_owned(), value).is_some() {
                return Err(CliError("duplicate option"));
            }
        }
        if !flags.contains_key("--profile") {
            return Err(CliError(
                "--profile must select the verified deployment profile",
            ));
        }
        Ok(Self {
            command,
            flags,
            testnet,
        })
    }
    fn string(&self, key: &str) -> Result<Option<&str>> {
        self.flags
            .get(key)
            .map(|s| s.to_str().ok_or(CliError("option requires UTF-8")))
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
            .map_err(|_| CliError("invalid timeout"))?;
        if !(1..=60_000).contains(&value) {
            return Err(CliError("timeout must be 1 through 60000 milliseconds"));
        }
        Ok(value)
    }
}

fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = File::open(path).map_err(|_| CliError("cannot read selected input file"))?;
    if !file
        .metadata()
        .map_err(|_| CliError("cannot inspect input file"))?
        .is_file()
    {
        return Err(CliError("input must be a regular file"));
    }
    let mut raw = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut raw)
        .map_err(|_| CliError("cannot read selected input file"))?;
    if raw.len() > limit {
        return Err(CliError("input file exceeds its size limit"));
    }
    Ok(raw)
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
            .map_err(|_| CliError("working directory unavailable"))?
            .join(path))
    }
}
fn credentials() -> Result<CircleCredentials> {
    use zeroize::Zeroizing;
    let key = std::env::var("CIRCLE_API_KEY")
        .map_err(|_| CliError("configure CIRCLE_API_KEY through your secret environment"))?;
    let secret = Zeroizing::new(std::env::var("CIRCLE_ENTITY_SECRET").map_err(|_| {
        CliError("configure the registered CIRCLE_ENTITY_SECRET through your secret environment")
    })?);
    if secret.len() != 64 || !secret.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(CliError(
            "CIRCLE_ENTITY_SECRET must be the registered 32-byte hexadecimal secret",
        ));
    }
    let mut decoded = Zeroizing::new([0u8; 32]);
    alloy_primitives::hex::decode_to_slice(secret.as_bytes(), decoded.as_mut())
        .map_err(|_| CliError("invalid entity secret format"))?;
    CircleCredentials::new(key, *decoded).map_err(|_| CliError("invalid Circle credential format"))
}
fn random_key() -> Result<String> {
    let mut raw = [0u8; 16];
    getrandom::fill(&mut raw).map_err(|_| CliError("secure randomness unavailable"))?;
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
        return Err(CliError(
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
    if args.testnet {
        overrides["chain"] = json!({"network":"testnet"});
    }
    if let Some(url) = args.string("--upstream")? {
        overrides["upstream"] = json!({"url":url});
    }
    ProviderConfig::from_file(args.config_path(), Some(&overrides)).map_err(|_| CliError("invalid provider configuration; check fields, exact quoted amounts and selected network"))
}
fn profile(args: &Arguments, chain_id: u64) -> Result<DeploymentProfile> {
    let profile = load_profile(
        args.path("--profile")
            .ok_or(CliError("missing deployment profile"))?,
    )
    .map_err(|_| CliError("invalid deployment profile"))?;
    if profile.domain().chain_id() != chain_id {
        return Err(CliError(
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
            return Err(CliError("invalid RPC environment variable name"));
        }
        std::env::var(name).map_err(|_| CliError("selected RPC environment variable is missing"))?
    } else {
        default_url.to_owned()
    };
    let transport = HttpTransport::new(args.timeout()?, MAX_RPC_RESPONSE_BYTES, None)
        .map_err(|_| CliError("invalid RPC transport policy"))?;
    let rpc =
        HttpJsonRpc::new(endpoint, transport).map_err(|_| CliError("invalid RPC endpoint"))?;
    Ok(Arc::new(ChainReader::new(Arc::new(rpc), profile)))
}

fn launch_host(args: &Arguments) -> Result<()> {
    let host=args.path("--host").ok_or(CliError("serve requires --host with your provider integration executable; see the standalone SDK example"))?;
    if !host.is_absolute() || !host.is_file() {
        return Err(CliError(
            "--host must be an absolute regular executable path",
        ));
    }
    if host.canonicalize().ok()
        == std::env::current_exe()
            .ok()
            .and_then(|p| p.canonicalize().ok())
    {
        return Err(CliError(
            "--host must be your integration executable, not mppi",
        ));
    }
    let config = load_config(args)?;
    profile(args, config.chain().chain_id())?;
    if config.listen().is_none() {
        return Err(CliError(
            "serve requires explicit listener and TLS configuration",
        ));
    }
    let mut command = std::process::Command::new(host);
    command
        .arg("--config")
        .arg(absolute(&args.config_path())?)
        .arg("--profile")
        .arg(absolute(
            &args.path("--profile").ok_or(CliError("missing profile"))?,
        )?);
    for key in ["--rpc-env", "--upstream", "--timeout-ms"] {
        if let Some(value) = args.flags.get(key) {
            command.arg(key).arg(value);
        }
    }
    if args.testnet {
        command.arg("--testnet");
    }
    // Exec before creating a Tokio runtime. Signals and exit status belong to
    // the actual provider host, and there is no hidden child supervisor.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let _ = command.exec();
        Err(CliError("provider integration executable could not start"))
    }
    #[cfg(not(unix))]
    {
        let _ = command;
        Err(CliError(
            "standalone host launch currently requires Unix; embed the SDK on this platform",
        ))
    }
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
    .map_err(|_| CliError("invalid recipient proof JSON"))?;
    if value.as_object().map(|m| m.len()) != Some(2) || value.get("request").is_none() {
        return Err(CliError(
            "recipient proof requires request and signature fields",
        ));
    }
    let signature = value["signature"]
        .as_str()
        .and_then(|s| s.strip_prefix("0x"))
        .ok_or(CliError("recipient signature must be hexadecimal"))?;
    let signature = alloy_primitives::hex::decode(signature)
        .map_err(|_| CliError("invalid recipient signature"))?;
    SignedRecipientProof::new(value["request"].clone(), signature)
        .map(Some)
        .map_err(|_| CliError("invalid recipient proof"))
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
        if args.command == "serve" {
            launch_host(&args)?;
            return Ok(0);
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| CliError("async runtime unavailable"))?;
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
        let wallet_id = config.identity().wallet_id().ok_or(CliError(
            "verify needs the confirmed Circle wallet id saved by onboarding",
        ))?;
        let client = CircleClient::new(credentials()?, args.timeout()?)
            .map_err(|_| CliError("Circle client could not initialize"))?;
        let wallet = client
            .wallet(wallet_id, config.chain().chain_id())
            .await
            .map_err(|_| {
                CliError(
                    "configured Circle operator wallet is not accessible on the selected network",
                )
            })?;
        if wallet.address() != config.identity().wallet_address() {
            return Err(CliError(
                "configured operator differs from Circle wallet identity",
            ));
        }
        let ca = args
            .path("--ca")
            .map(|p| read_bounded(&p, MAX_CONFIG_BYTES))
            .transpose()?;
        let policy = VerifyPolicy::new(args.timeout()?, ca.as_deref())
            .map_err(|_| CliError("invalid verification TLS policy"))?;
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
        CliError("provider configuration is already in use or its local directory is unsafe")
    })?;
    if args.command == "onboard" {
        run_onboard(&args, &config_path).await
    } else {
        run_update(&args, &config_path).await
    }
}

async fn selected_wallet(
    args: &Arguments,
    config_path: &Path,
    chain_id: u64,
    configured_id: Option<&str>,
    configured_address: Option<crate::Address>,
) -> Result<(Arc<CircleClient>, CircleWalletIdentity)> {
    let client = Arc::new(
        CircleClient::new(credentials()?, args.timeout()?)
            .map_err(|_| CliError("Circle client could not initialize"))?,
    );
    let explicit = args.string("--wallet-id")?;
    if explicit.is_some() && configured_id.is_some() && explicit != configured_id {
        return Err(CliError(
            "selected wallet differs from established provider configuration",
        ));
    }
    let mut store = LocalState::open(sidecar(config_path, ".wallet-setup.json"), 4096)
        .map_err(|_| CliError("wallet setup record unavailable or already in use"))?;
    let mut record=store.read().map_err(|_|CliError("wallet setup record is invalid"))?.unwrap_or(json!({"version":1,"chain_id":chain_id,"set_key":random_key()?,"wallet_key":random_key()?,"set_id":null,"wallet_id":null,"set_attempted":false,"wallet_attempted":false}));
    let fields = [
        "version",
        "chain_id",
        "set_key",
        "wallet_key",
        "set_id",
        "wallet_id",
        "set_attempted",
        "wallet_attempted",
    ];
    if record
        .as_object()
        .is_none_or(|m| m.len() != fields.len() || fields.iter().any(|f| !m.contains_key(*f)))
        || record["version"] != 1
        || record["chain_id"].as_u64() != Some(chain_id)
        || !record["set_attempted"].is_boolean()
        || !record["wallet_attempted"].is_boolean()
    {
        return Err(CliError(
            "wallet setup record conflicts with selected network",
        ));
    }
    for field in ["set_id", "wallet_id"] {
        if !record[field].is_null() && !record[field].is_string() {
            return Err(CliError("invalid retained wallet resource identifier"));
        }
    }
    for field in ["set_key", "wallet_key"] {
        let key = record[field]
            .as_str()
            .ok_or(CliError("invalid retained wallet operation key"))?;
        let valid = key.len() == 36
            && key.bytes().enumerate().all(|(i, b)| {
                if [8, 13, 18, 23].contains(&i) {
                    b == b'-'
                } else {
                    b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
                }
            })
            && key.as_bytes()[14] == b'4'
            && [b'8', b'9', b'a', b'b'].contains(&key.as_bytes()[19]);
        if !valid {
            return Err(CliError("invalid retained wallet operation key"));
        }
    }
    if (record["wallet_attempted"] == true || !record["wallet_id"].is_null())
        && record["set_id"].is_null()
    {
        return Err(CliError(
            "wallet setup record is missing its original wallet set",
        ));
    }
    if let Some(id) = args.string("--wallet-set-id")? {
        if record["set_id"].as_str().is_some_and(|old| old != id) {
            return Err(CliError("selected wallet set differs from retained setup"));
        }
        client
            .get_wallet_set(id)
            .await
            .map_err(|_| CliError("selected Circle wallet set is not accessible"))?;
        if record["set_id"].is_null() {
            record["set_id"] = json!(id);
            store
                .write(&record)
                .map_err(|_| CliError("cannot retain selected wallet set"))?;
        }
    }
    let retained = record["wallet_id"].as_str();
    let selected = explicit.or(configured_id).or(retained);
    if selected.is_none() && configured_address.is_some() {
        return Err(CliError(
            "established provider wallet needs its Circle wallet id; rerun with --wallet-id",
        ));
    }
    if let Some(id) = selected {
        if retained.is_some_and(|known| known != id) {
            return Err(CliError(
                "wallet setup record names a different established wallet",
            ));
        }
        let wallet = client
            .wallet(id, chain_id)
            .await
            .map_err(|_| CliError("Circle wallet access, network or SCA identity check failed"))?;
        if configured_address.is_some_and(|expected| expected != wallet.address()) {
            return Err(CliError(
                "selected Circle wallet differs from established provider address",
            ));
        }
        if record["set_id"]
            .as_str()
            .is_some_and(|id| id != wallet.wallet_set_id())
        {
            return Err(CliError("Circle wallet differs from retained wallet set"));
        }
        if retained.is_none() {
            record["wallet_id"] = json!(wallet.wallet_id());
            record["set_id"] = json!(wallet.wallet_set_id());
            store
                .write(&record)
                .map_err(|_| CliError("cannot retain selected Circle wallet"))?;
        }
        return Ok((client, wallet));
    }
    // Exact intent is durable before either mutation. A lost resource response
    // cannot become an indefinite replay against undocumented key retention.
    store
        .write(&record)
        .map_err(|_| CliError("cannot retain wallet provisioning intent"))?;
    if record["set_id"].is_null() {
        if record["set_attempted"] == true {
            return Err(CliError(
                "original wallet-set creation is unresolved; check Circle Console and rerun with its --wallet-set-id; do not create a replacement",
            ));
        }
        let key = record["set_key"]
            .as_str()
            .ok_or(CliError("invalid wallet setup key"))?
            .to_owned();
        let id = client
            .create_wallet_set_before_submit("MPPI provider", &key, || {
                record["set_attempted"] = json!(true);
                store
                    .write(&record)
                    .map_err(|_| crate::circle::CircleError::Journal)
            })
            .await
            .map_err(|_| {
                CliError(
                    "Circle wallet-set setup failed; rerun checks the retained original operation",
                )
            })?;
        record["set_id"] = json!(id);
        store
            .write(&record)
            .map_err(|_| CliError("cannot retain created wallet set"))?;
    }
    let wallet = if record["wallet_attempted"] == true {
        client
            .find_created_wallet(
                record["set_id"]
                    .as_str()
                    .ok_or(CliError("invalid retained wallet set"))?,
                chain_id,
                record["wallet_key"]
                    .as_str()
                    .ok_or(CliError("invalid wallet setup key"))?,
            )
            .await
            .map_err(|_| CliError("original Circle wallet lookup is unavailable or ambiguous"))?
            .ok_or(CliError(
                "original Circle wallet creation remains unresolved; no replacement was submitted",
            ))?
    } else {
        let set_id = record["set_id"]
            .as_str()
            .ok_or(CliError("invalid retained wallet set"))?
            .to_owned();
        let key = record["wallet_key"]
            .as_str()
            .ok_or(CliError("invalid wallet setup key"))?
            .to_owned();
        client
            .create_wallet_before_submit(&set_id, chain_id, "MPPI provider", &key, || {
                record["wallet_attempted"] = json!(true);
                store
                    .write(&record)
                    .map_err(|_| crate::circle::CircleError::Journal)
            })
            .await
            .map_err(|_| {
                CliError("Circle SCA setup unresolved; rerun checks the retained original request")
            })?
    };
    record["wallet_id"] = json!(wallet.wallet_id());
    store
        .write(&record)
        .map_err(|_| CliError("cannot retain created wallet"))?;
    Ok((client, wallet))
}

/// Circle SCAs cannot sign arbitrary typed data before their first transaction.
/// Only initial registration may defer this preflight: its original, retained
/// contract execution still requires the entity secret and activates the SCA.
/// Source: https://developers.circle.com/wallets/signing-apis#sca-wallets-and-lazy-deployment
async fn onboarding_credentials(
    chain: &ChainReader,
    block: &Block,
    operator: crate::Address,
    initial_registration: bool,
    signing_check: impl std::future::Future<
        Output = std::result::Result<(), crate::circle::CircleError>,
    >,
) -> Result<()> {
    let code = chain
        .rpc()
        .call(
            "eth_getCode",
            json!([
                crate::chain::encoded(operator),
                chain
                    .position(block)
                    .map_err(|_| CliError("operator deployment evidence unavailable"))?
            ]),
        )
        .await
        .and_then(|value| crate::chain::data(&value))
        .map_err(|_| CliError("operator deployment evidence unavailable"))?;
    chain
        .check_canonical(block)
        .await
        .map_err(|_| CliError("operator deployment evidence is not canonical"))?;
    if code.is_empty() {
        return if initial_registration {
            Ok(())
        } else {
            Err(CliError(
                "existing provider operator SCA is not deployed on the selected chain",
            ))
        };
    }
    signing_check.await.map_err(|_| {
        CliError(
            "Circle registered entity-secret signing check failed; no registry write was started",
        )
    })
}

async fn run_onboard(args: &Arguments, path: &Path) -> Result<i32> {
    let existing = if path.exists() {
        Some(load_config(args)?)
    } else {
        None
    };
    let declarations = if let Some(input) = args.path("--declarations") {
        let mut mapping = crate::config::load_mapping(&input)
            .map_err(|_| CliError("invalid initial declarations"))?;
        if args.testnet {
            mapping["chain"]["network"] = json!("testnet");
        }
        ProviderDeclarations::from_mapping(&mapping).map_err(|_| {
            CliError("invalid initial declarations; omit identity until registration is confirmed")
        })?
    } else if let Some(config) = &existing {
        let mut mapping = config.to_mapping();
        mapping
            .as_object_mut()
            .expect("configuration object")
            .remove("identity");
        ProviderDeclarations::from_mapping(&mapping)
            .map_err(|_| CliError("invalid saved provider declarations"))?
    } else {
        return Err(CliError(
            "initial onboarding requires --declarations with provider TOML excluding identity",
        ));
    };
    let profile = profile(args, declarations.chain().chain_id())?;
    let chain = chain(args, declarations.chain().rpc_url(), profile)?;
    let registry = Arc::new(RegistryReader::new(chain));
    let verified = registry
        .verified_block()
        .await
        .map_err(|_| CliError("selected deployment could not be verified"))?;
    let (client, identity) = selected_wallet(
        args,
        path,
        declarations.chain().chain_id(),
        existing.as_ref().and_then(|c| c.identity().wallet_id()),
        existing.as_ref().map(|c| c.identity().wallet_address()),
    )
    .await?;
    if identity.address() == declarations.provider().recipient() {
        return Err(CliError(
            "recipient must differ from the provider operator wallet",
        ));
    }
    if existing
        .as_ref()
        .is_some_and(|c| c.identity().wallet_address() != identity.address())
    {
        return Err(CliError(
            "Circle wallet differs from established provider address",
        ));
    }
    let adopted = args
        .string("--provider-id")?
        .map(crate::discovery::decimal_uint256)
        .transpose()
        .map_err(|_| CliError("provider id must be a canonical decimal uint256"))?;
    if adopted.is_some()
        && existing
            .as_ref()
            .is_some_and(|c| Some(c.identity().provider_id()) != adopted)
    {
        return Err(CliError(
            "selected identity differs from established provider",
        ));
    }
    onboarding_credentials(
        registry.chain(),
        &verified,
        identity.address(),
        existing.is_none() && adopted.is_none(),
        client.verify_credentials(&identity),
    )
    .await?;
    let wallet = Arc::new(
        CircleDeveloperWallet::new(
            client,
            identity.clone(),
            sidecar(path, ".wallet-operations.json"),
        )
        .map_err(|_| CliError("Circle operation record unavailable or inconsistent"))?,
    );
    let result = onboard(
        &declarations,
        wallet,
        registry,
        OnboardingOptions {
            state_path: sidecar(path, ".onboarding.json"),
            wallet_id: Some(identity.wallet_id().into()),
            adopted_identity: adopted
                .or_else(|| existing.as_ref().map(|c| c.identity().provider_id())),
            recipient_proof: signed_proof(args)?,
        },
    )
    .await
    .map_err(|error| CliError(error.message()))?;
    match result.progress() {
        OnboardingProgress::Complete => {
            let config = result
                .config()
                .ok_or(CliError("confirmed provider configuration unavailable"))?;
            write_configuration(path, config)?;
            println!(
                "{}",
                json!({"complete":true,"provider_id":config.identity().provider_id().to_string(),"wallet_address":format!("{:#x}",identity.address()),"wallet_id":identity.wallet_id(),"endpoint_verified":false,"runtime_application_required":true})
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
    let raw = config
        .to_toml()
        .map_err(|_| CliError("complete provider configuration cannot be represented as TOML"))?;
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
        .ok_or(CliError("output directory unavailable"))?;
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
            .map_err(|_| CliError("cannot prepare complete configuration replacement"))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| CliError("cannot durably write configuration"))?;
        fs::rename(&temp, path).map_err(|_| CliError("cannot replace configuration"))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| CliError("configuration replacement durability uncertain"))?;
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
    let input = args.path("--changes").ok_or(CliError(
        "update requires --changes containing only explicitly requested fields",
    ))?;
    let raw = read_bounded(&input, MAX_CONFIG_BYTES)?;
    let mut mapping = crate::json::parse_document(&raw, MAX_CONFIG_BYTES, 32)
        .map_err(|_| CliError("invalid update JSON"))?;
    normalize_update_paths(&mut mapping, path)?;
    let changes = UpdateChanges::from_mapping(&mapping)
        .map_err(|_| CliError("invalid explicit update fields"))?;
    let profile = profile(args, config.chain().chain_id())?;
    let registry = Arc::new(RegistryReader::new(chain(
        args,
        config.chain().rpc_url(),
        profile,
    )?));
    registry
        .verified_block()
        .await
        .map_err(|_| CliError("selected deployment could not be verified"))?;
    let (client, identity) = selected_wallet(
        args,
        path,
        config.chain().chain_id(),
        config.identity().wallet_id(),
        Some(config.identity().wallet_address()),
    )
    .await?;
    client.verify_credentials(&identity).await.map_err(|_| {
        CliError(
            "Circle registered entity-secret signing check failed; no registry write was started",
        )
    })?;
    let wallet = Arc::new(
        CircleDeveloperWallet::new(client, identity, sidecar(path, ".wallet-operations.json"))
            .map_err(|_| CliError("Circle operation record unavailable or inconsistent"))?,
    );
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
    .map_err(|error| CliError(error.message()))?;
    match result.progress() {
        OnboardingProgress::Complete => {
            let applied = result
                .config()
                .ok_or(CliError("confirmed update configuration unavailable"))?;
            write_configuration(path, applied)?;
            let onboarding = sidecar(path, ".onboarding.json");
            if onboarding.exists() {
                refresh_completed_onboarding(applied,onboarding,&registry).await.map_err(|_|CliError("configuration saved; completed onboarding record still needs reconciliation"))?;
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
                    .ok_or(CliError("TLS path must be a string"))?;
                let target = Path::new(raw);
                if target.is_relative() {
                    let target = config_path
                        .parent()
                        .ok_or(CliError("configuration directory unavailable"))?
                        .join(target);
                    *value = json!(target.to_str().ok_or(CliError("TLS path must be UTF-8"))?);
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
        assert!(args.testnet);
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

    struct OperatorRpc {
        code: serde_json::Value,
        unavailable: bool,
        reorg: bool,
        calls: std::sync::Mutex<Vec<(String, serde_json::Value)>>,
    }
    #[tonic::async_trait]
    impl crate::chain::Rpc for OperatorRpc {
        async fn call(
            &self,
            method: &str,
            params: serde_json::Value,
        ) -> std::result::Result<serde_json::Value, crate::chain::ChainError> {
            self.calls.lock().unwrap().push((method.into(), params));
            match method {
                "eth_getCode" if self.unavailable => Err(crate::chain::ChainError::Unavailable),
                "eth_getCode" => Ok(self.code.clone()),
                "eth_chainId" => Ok(json!(format!("0x{:x}", 5_042_002))),
                "eth_getBlockByNumber" => Ok(json!({
                    "number":"0x7", "timestamp":"0xa",
                    "hash":crate::chain::encoded(crate::B256::repeat_byte(if self.reorg {8} else {7})),
                    "parentHash":crate::chain::encoded(crate::B256::repeat_byte(6))
                })),
                _ => panic!("unexpected RPC method: {method}"),
            }
        }
    }
    fn operator_chain(rpc: Arc<OperatorRpc>) -> (ChainReader, Block, crate::Address) {
        use crate::{
            Address, B256,
            chain::CodePin,
            types::{ChainObservation, Domain},
        };
        let address = Address::repeat_byte(1);
        let domain = Domain::new(5_042_002, address).unwrap();
        let pin = |byte| CodePin::new(Address::repeat_byte(byte), B256::repeat_byte(9)).unwrap();
        let profile = DeploymentProfile::new(
            domain,
            Address::repeat_byte(3),
            pin(1),
            pin(2),
            pin(3),
            pin(4),
        )
        .unwrap();
        let block = Block::new(
            ChainObservation::new(5_042_002, address, 7, B256::repeat_byte(7)).unwrap(),
            10,
            B256::repeat_byte(6),
        );
        (
            ChainReader::new(rpc, profile),
            block,
            Address::repeat_byte(5),
        )
    }
    fn operator_rpc(code: serde_json::Value, unavailable: bool, reorg: bool) -> Arc<OperatorRpc> {
        Arc::new(OperatorRpc {
            code,
            unavailable,
            reorg,
            calls: std::sync::Mutex::new(vec![]),
        })
    }

    #[tokio::test]
    async fn undeployed_sca_initial_registration_defers_impossible_signing_only() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let rpc = operator_rpc(json!("0x"), false, false);
        let (chain, block, operator) = operator_chain(rpc.clone());
        let calls = AtomicUsize::new(0);
        let unavailable_signing = async {
            calls.fetch_add(1, Ordering::Relaxed);
            Err(crate::circle::CircleError::Unavailable)
        };
        onboarding_credentials(&chain, &block, operator, true, unavailable_signing)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            rpc.calls.lock().unwrap()[0],
            (
                "eth_getCode".into(),
                json!([
                    crate::chain::encoded(operator),
                    {"blockHash":crate::chain::encoded(block.observation().block_hash()),"requireCanonical":true}
                ])
            )
        );
        // Existing configuration/adoption cannot become a credential-ready no-op
        // by selecting an undeployed account.
        assert!(
            onboarding_credentials(&chain, &block, operator, false, async { Ok(()) })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn deployed_sca_retains_required_fresh_signing_check() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (chain, block, operator) = operator_chain(operator_rpc(json!("0x00"), false, false));
        let calls = AtomicUsize::new(0);
        for initial in [true, false] {
            let failed = onboarding_credentials(&chain, &block, operator, initial, async {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(crate::circle::CircleError::Unavailable)
            })
            .await;
            assert!(failed.is_err());
            onboarding_credentials(&chain, &block, operator, initial, async {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .await
            .unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn uncertain_operator_deployment_never_authorizes_lazy_registration() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = AtomicUsize::new(0);
        for (code, unavailable, reorg) in [
            (json!("0x"), true, false),
            (json!("0x0"), false, false),
            (serde_json::Value::Null, false, false),
            (json!("0x"), false, true),
        ] {
            let (chain, block, operator) = operator_chain(operator_rpc(code, unavailable, reorg));
            assert!(
                onboarding_credentials(&chain, &block, operator, true, async {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })
                .await
                .is_err()
            );
        }
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
