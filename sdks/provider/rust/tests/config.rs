use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use mppi_provider::{
    U256,
    config::{AdditionalService, MAX_CONFIG_BYTES, MAX_DURATION_MS, ProviderConfig},
};
use serde_json::{Value, json};

#[path = "support/configuration.rs"]
mod support;
use support::configuration;

#[test]
fn setup_declarations_are_valid_without_an_identity_and_zero_remains_valid() {
    use mppi_provider::{
        Address,
        config::{ProviderDeclarations, ProviderIdentityConfig},
    };
    let mut values = configuration();
    values.as_object_mut().unwrap().remove("identity");
    let declarations = ProviderDeclarations::from_mapping(&values).unwrap();
    assert!(ProviderConfig::from_mapping(&values, None).is_err());
    let identity = ProviderIdentityConfig::new(U256::ZERO, Address::repeat_byte(0x31)).unwrap();
    let config = declarations.with_identity(identity).unwrap();
    assert_eq!(config.identity().provider_id(), U256::ZERO);
    assert_eq!(
        ProviderConfig::from_mapping(&config.to_mapping(), None).unwrap(),
        config
    );
    assert!(ProviderDeclarations::from_mapping(&config.to_mapping()).is_err());
    values["models"][0]["rates"]["input"] = json!("01");
    assert!(ProviderDeclarations::from_mapping(&values).is_err());
}

#[test]
fn declaration_roundtrip_preserves_exact_money_paths_and_optional_fields() {
    let mut values = configuration();
    values["models"][0]["rates"]["input"] = json!(u64::MAX.to_string());
    values["models"][0]["rates"]["cache"] = json!("0");
    values["settlement"]["amount"] = json!(u64::MAX.to_string());
    values["provider"]["image"] = json!("https://provider.example/image.png");
    values["provider"]["services"] =
        json!([{"name":"extension","endpoint":"https://extension.example"}]);
    values["listen"] = json!({"bind":"127.0.0.1:8443","tls_mode":"direct","tls_cert":"/example/cert.pem","tls_key":"/example/key.pem"});
    let config = ProviderConfig::from_mapping(&values, None).unwrap();
    let mapped = config.to_mapping();
    assert_eq!(mapped["models"][0]["rates"]["input"], u64::MAX.to_string());
    assert_eq!(mapped["models"][0]["rates"]["cache"], "0");
    assert_eq!(ProviderConfig::from_mapping(&mapped, None).unwrap(), config);
    let temp = Temp::new();
    let encoded = config.to_toml().unwrap();
    assert_eq!(
        ProviderConfig::from_file(temp.file(&encoded), None).unwrap(),
        config
    );
}

#[test]
fn oversized_configuration_rejects_before_override_can_hide_it() {
    let mut values = configuration();
    values["provider"]["services"] = json!([{"name":"extension","endpoint":"https://extension.example","data":"x".repeat(MAX_CONFIG_BYTES+1)}]);
    let overrides = json!({"provider":{"services":[]}});
    assert!(ProviderConfig::from_mapping(&values, Some(&overrides)).is_err());
    let mut overrides = overrides;
    overrides["provider"]["services"] = values["provider"]["services"].clone();
    assert!(ProviderConfig::from_mapping(&configuration(), Some(&overrides)).is_err());
}

#[test]
fn encoded_array_size_is_bounded_before_override_and_clone() {
    let mut values = configuration();
    values["provider"]["services"] = json!([{"name":"extension","endpoint":"https://extension.example","data":vec![true;MAX_CONFIG_BYTES/4]}]);
    assert!(
        ProviderConfig::from_mapping(&values, Some(&json!({"provider":{"services":[]}}))).is_err()
    );
}

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "mppi-rust-config-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self, raw: &str) -> PathBuf {
        let path = self.0.join("mppi.toml");
        fs::write(&path, raw).unwrap();
        path
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

const EXAMPLE: &str = include_str!("../../../../examples/provider/mppi.toml");
const LISTEN: &str = "\n[listen]\nbind = '127.0.0.1:8443'\ntls_mode = 'direct'\ntls_cert = 'certificates/server.pem'\ntls_key = 'certificates/key.pem'\n";

#[test]
fn shared_python_example_and_exact_catalog() {
    let temp = Temp::new();
    let config = ProviderConfig::from_file(temp.file(EXAMPLE), None).unwrap();
    assert_eq!(config.chain().chain_id(), 5042002);
    assert_eq!(config.models()[0].rates().cache(), Some(30000));
    assert_eq!(
        config.upstream().completions_url(),
        "http://127.0.0.1:8080/v1/chat/completions"
    );
    let config = ProviderConfig::from_mapping(&configuration(), None).unwrap();
    assert_eq!(
        config.provider().public_origin(),
        "https://provider.example"
    );
    assert_eq!(
        config.upstream().completions_url(),
        "http://127.0.0.1:8080/custom/v1/chat/completions"
    );
    assert_eq!(
        config.catalog().canonical_bytes(),
        br#"[{"input":"300000","model":"example/model","output":"1200000"}]"#
    );
    assert_eq!(config.payment().output_buffer_tokens(), 1000);
    assert_eq!(config.payment().suggest_session_tokens(), 1_000_000);
    assert_eq!(config.identity().provider_id(), U256::from(42));
    assert!(config.listen().is_none());
}

#[test]
fn exact_programmatic_amounts_and_omitted_cache() {
    let mut values = configuration();
    values["models"][0]["rates"]["input"] = json!(u64::MAX);
    values["models"][0]["rates"]["cache"] = json!(0);
    values["identity"]["provider_id"] = json!(U256::MAX.to_string());
    let config = ProviderConfig::from_mapping(&values, None).unwrap();
    assert_eq!(config.models()[0].rates().input(), u64::MAX);
    assert_eq!(config.models()[0].rates().cache(), Some(0));
    assert_eq!(config.identity().provider_id(), U256::MAX);
    assert!(
        String::from_utf8_lossy(config.catalog().canonical_bytes()).contains("\"cache\":\"0\"")
    );
}

#[test]
fn invalid_types_exact_spelling_and_secrets_reject() {
    let invalid = [
        ("/provider/name", json!("")),
        (
            "/provider/public_origin",
            json!("https://provider.example/path"),
        ),
        (
            "/provider/recipient",
            json!("0x1111111111111111111111111111111111111111"),
        ),
        ("/identity/provider_id", json!("01")),
        ("/identity/provider_id", json!(true)),
        (
            "/identity/provider_id",
            json!("115792089237316195423570985008687907853269984665640564039457584007913129639936"),
        ),
        ("/chain/network", json!("other")),
        ("/chain/max_age_ms", json!(true)),
        ("/chain/max_age_ms", json!(MAX_DURATION_MS + 1)),
        (
            "/upstream/url",
            json!("https://secret:password@upstream.example/v1"),
        ),
        (
            "/upstream/url",
            json!("https://upstream.example/v1?token=secret"),
        ),
        ("/upstream/api_key_env", json!("NOT A VARIABLE")),
        ("/models/0/max_output", json!(128001)),
        ("/models/0/streaming", json!(1)),
        ("/models/0/modalities", json!(["text", "text"])),
        ("/models/0/modalities", json!(["unknown"])),
        ("/models/0/rates/input", json!(1.0)),
        ("/models/0/rates/output", json!("18446744073709551616")),
        ("/payment/authorization_wait_ms", json!(0)),
        ("/payment/authorization_wait_ms", json!(MAX_DURATION_MS + 1)),
        ("/settlement/amount", json!("0")),
        ("/settlement/amount", json!("+1")),
        ("/settlement/amount", json!(" 1")),
        ("/settlement/amount", json!("1e2")),
    ];
    for (path, value) in invalid {
        let mut values = configuration();
        *values.pointer_mut(path).unwrap() = value;
        let error = ProviderConfig::from_mapping(&values, None).unwrap_err();
        assert!(!error.to_string().contains("secret"), "{path}");
        assert!(!format!("{error:?}").contains("password"), "{path}");
    }
    let mut values = configuration();
    values["models"][0]["rates"]["cache"] = json!(false);
    assert!(ProviderConfig::from_mapping(&values, None).is_err());
}

#[test]
fn required_fields_unknown_keys_duplicate_models_and_services_reject() {
    let mut values = configuration();
    values["payment"]
        .as_object_mut()
        .unwrap()
        .remove("authorization_wait_ms");
    assert_eq!(
        ProviderConfig::from_mapping(&values, None)
            .unwrap_err()
            .field(),
        "payment.authorization_wait_ms"
    );
    for section in [
        "",
        "provider",
        "identity",
        "chain",
        "upstream",
        "payment",
        "settlement",
    ] {
        let mut values = configuration();
        let target = if section.is_empty() {
            &mut values
        } else {
            &mut values[section]
        };
        target["SECRET"] = json!("secret");
        assert!(
            !ProviderConfig::from_mapping(&values, None)
                .unwrap_err()
                .to_string()
                .contains("SECRET")
        );
    }
    let mut values = configuration();
    values["models"] = json!([values["models"][0].clone(), values["models"][0].clone()]);
    assert!(ProviderConfig::from_mapping(&values, None).is_err());
    let mut values = configuration();
    values["provider"]["services"] = json!([{"name":"mppi","endpoint":"https://evil.example"}]);
    assert!(ProviderConfig::from_mapping(&values, None).is_err());
}

#[test]
fn overrides_are_shallow_per_section_and_records_are_snapshots() {
    let mut values = configuration();
    let overrides = json!({"payment":{"output_buffer_tokens":2048}});
    let config = ProviderConfig::from_mapping(&values, Some(&overrides)).unwrap();
    values["models"][0]["rates"]["input"] = json!(999);
    values["provider"]["services"] = json!([]);
    assert_eq!(config.models()[0].rates().input(), 300000);
    assert_eq!(config.payment().output_buffer_tokens(), 2048);
    assert_eq!(config.payment().authorization_wait_ms(), 10000);
    assert_eq!(
        config.provider().services()[0].as_object()["skills"],
        json!(["chat"])
    );
    assert!(ProviderConfig::from_mapping(&values, Some(&json!({"models":[]}))).is_err());
}

#[test]
fn toml_paths_resolve_before_caller_relative_overrides_without_provisioning() {
    let temp = Temp::new();
    let path = temp.file(&(EXAMPLE.to_owned() + LISTEN));
    let config =
        ProviderConfig::from_file(&path, Some(&json!({"listen":{"tls_key":"caller/key.pem"}})))
            .unwrap();
    let listen = config.listen().unwrap();
    assert_eq!(
        listen.tls_cert().unwrap(),
        temp.0.join("certificates/server.pem")
    );
    assert_eq!(
        listen.tls_key().unwrap(),
        std::env::current_dir().unwrap().join("caller/key.pem")
    );
    assert!(!temp.0.join("certificates").exists());
    let printed = format!("{config:?}");
    assert!(!printed.contains("caller/key.pem"));
    assert!(!printed.contains("127.0.0.1:8080"));
}

#[test]
fn quoted_money_duplicate_keys_toml_one_point_one_and_oversize_reject() {
    let temp = Temp::new();
    for raw in [
        EXAMPLE.replace("input = \"300000\"", "input = 12"),
        EXAMPLE.replace("provider_id = \"42\"", "provider_id = 42"),
        EXAMPLE.replace(
            "name = \"Example Inference\"",
            "name = \"Test\"\nname = \"SECRET\"",
        ),
        EXAMPLE.replace(
            "description = \"Provider-operated inference\"",
            "description = \"\\e\"",
        ),
        "name = \"SECRET\n".into(),
        "#".repeat(MAX_CONFIG_BYTES + 1),
    ] {
        let path = temp.file(&raw);
        let error = ProviderConfig::from_file(&path, None).unwrap_err();
        assert_eq!(error.source_path(), Some(path.as_path()));
        assert!(!format!("{error:?}").contains("SECRET"));
    }
    let raw = EXAMPLE.replace("input = \"300000\"", "input = 12");
    assert!(
        ProviderConfig::from_file(
            temp.file(&raw),
            Some(&json!({"models":configuration()["models"]}))
        )
        .is_err()
    );
}

#[test]
fn listener_modes_and_bounded_additional_services() {
    let mut values = configuration();
    for listener in [
        json!({"bind":"0.0.0.0:8443","tls_mode":"ingress","tls_key":"secret.pem"}),
        json!({"bind":"0.0.0.0:8443","tls_mode":"direct"}),
        json!({"bind":"0.0.0.0","tls_mode":"ingress"}),
    ] {
        values["listen"] = listener;
        assert!(ProviderConfig::from_mapping(&values, None).is_err());
    }
    values["listen"] = json!({"bind":"0.0.0.0:8443","tls_mode":"ingress"});
    assert!(
        ProviderConfig::from_mapping(&values, None)
            .unwrap()
            .listen()
            .is_some()
    );
    for raw in [
        br#"{"name":"other","endpoint":"ipfs://test","x":1,"\u0078":2}"#.as_slice(),
        br#"{"name":"other","endpoint":"ipfs://test","number":1e999}"#,
    ] {
        assert!(AdditionalService::from_bytes(raw).is_err());
    }
    let mut nested = Value::Null;
    for _ in 0..128 {
        nested = json!([nested]);
    }
    assert!(
        AdditionalService::from_mapping(
            &json!({"name":"other","endpoint":"ipfs://test","nested":nested})
        )
        .is_err()
    );
}
