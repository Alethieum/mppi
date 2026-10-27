//! The public local record a wallet adapter journals its operations with.
use mppi_provider::local_state::{LocalState, StateError};
use serde_json::json;

#[test]
fn a_wallet_journal_survives_reopening_and_excludes_a_second_holder() {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    let directory = std::env::temp_dir().join(format!(
        "mppi-wallet-journal-{}",
        alloy_primitives::hex::encode(nonce)
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("operations.json");
    let mut journal = LocalState::open(&path, 1024).unwrap();
    assert_eq!(
        journal.path(),
        directory.canonicalize().unwrap().join("operations.json")
    );
    assert_eq!(journal.read().unwrap(), None);
    let intent = json!({"operations":{"settle-1":{"call":"0xdeadbeef","state":"unknown"}}});
    journal.write(&intent).unwrap();
    assert_eq!(LocalState::open(&path, 1024).unwrap_err(), StateError);
    assert!(journal.write(&json!(["not an object"])).is_err());
    drop(journal);
    let reopened = LocalState::open(&path, 1024).unwrap();
    assert_eq!(reopened.read().unwrap(), Some(intent));
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}
