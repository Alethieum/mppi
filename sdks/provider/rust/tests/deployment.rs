use mppi_provider::deployment::{load_profile, profile_from_mapping, profile_to_mapping};
use serde_json::{Value, json};

#[test]
fn official_testnet_profile_is_packaged_with_exact_release_bytes() {
    assert_eq!(
        include_bytes!("../src/arc-testnet.json").as_slice(),
        include_bytes!("../../../../deployments/arc-testnet.json").as_slice(),
    );
    let official = mppi_provider::deployment::official_testnet_profile().unwrap();
    let release = load_profile(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../deployments/arc-testnet.json"
    ))
    .unwrap();
    assert_eq!(official, release);
    assert_eq!(official.domain().chain_id(), 5_042_002);
}
#[test]
fn official_mainnet_profile_is_packaged_with_exact_release_bytes() {
    assert_eq!(
        include_bytes!("../src/arc-mainnet.json").as_slice(),
        include_bytes!("../../../../deployments/arc-mainnet.json").as_slice(),
    );
    let official = mppi_provider::deployment::official_mainnet_profile().unwrap();
    let release = load_profile(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../deployments/arc-mainnet.json"
    ))
    .unwrap();
    assert_eq!(official, release);
    assert_eq!(official.domain().chain_id(), 5042);
}
fn profile() -> Value {
    let pin = |n: u8| json!({"address":format!("0x{}",format!("{n:02x}").repeat(20)),"code_hash":format!("0x{}",format!("{:02x}",n+1).repeat(32))});
    json!({"version":1,"chain_id":5042002,"proxy":pin(1),"implementation":pin(3),"registry":pin(5),"registry_implementation":pin(7)})
}
#[test]
fn explicit_pins_roundtrip_without_defaults() {
    let value = profile();
    assert_eq!(
        profile_to_mapping(&profile_from_mapping(&value).unwrap()),
        value
    );
}
#[test]
fn malformed_pins_reject_before_chain_access() {
    for (path, bad) in [
        (vec!["version"], json!(true)),
        (vec!["version"], json!(1.0)),
        (vec!["chain_id"], json!(1)),
        (vec!["chain_id"], json!(true)),
        (
            vec!["proxy", "code_hash"],
            json!(format!("0x{}", "00".repeat(32))),
        ),
        (vec!["proxy", "address"], json!("bad")),
    ] {
        let mut value = profile();
        let mut field = &mut value;
        for key in path {
            field = &mut field[key];
        }
        *field = bad;
        assert!(profile_from_mapping(&value).is_err());
    }
    let mut value = profile();
    value["secret"] = json!("unexpected");
    assert!(profile_from_mapping(&value).is_err());
    assert!(
        load_profile("a-missing-private-path.json")
            .unwrap_err()
            .to_string()
            .find("private-path")
            .is_none()
    );
}
