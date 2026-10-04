use mppi_provider::{
    Address, B256, U256, ValidationCode,
    config::ProviderConfig,
    discovery::{
        DiscoveryDocuments, MAX_DOCUMENT_BYTES, ProviderIdentity, normalize_origin,
        origin_from_discover, parse_document, verify_models, verify_registration,
    },
    types::ChainObservation,
};
use serde_json::{Value, json};

#[path = "support/configuration.rs"]
mod support;

fn declarations() -> (ProviderConfig, ProviderIdentity) {
    let mut values = support::configuration();
    values["identity"]["provider_id"] = json!(U256::MAX.to_string());
    let config = ProviderConfig::from_mapping(&values, None).unwrap();
    let identity = ProviderIdentity::new(
        ChainObservation::new(
            5042002,
            Address::repeat_byte(b'c'),
            10,
            B256::repeat_byte(b'b'),
        )
        .unwrap(),
        Address::repeat_byte(b'i'),
        config.identity().provider_id(),
        config.identity().wallet_address(),
        config.provider().recipient(),
        "https://provider.example/discover".into(),
        Some(config.catalog().clone()),
    )
    .unwrap();
    (config, identity)
}

#[test]
fn origin_normalization_matches_python_with_ipv6_mapped_addresses() {
    for (input, expected) in [
        ("HTTPS://Provider.Example:443/", "https://provider.example"),
        (
            "https://provider.example:8443",
            "https://provider.example:8443",
        ),
        ("https://[2001:0db8::1]:443/", "https://[2001:db8::1]"),
        ("https://[::ffff:192.0.2.1]", "https://[::ffff:c000:201]"),
        ("https://[2001:0:0:1:0:0:1:1]", "https://[2001::1:0:0:1:1]"),
        (
            "https://provider.example:000443",
            "https://provider.example",
        ),
    ] {
        assert_eq!(normalize_origin(input).unwrap(), expected);
    }
    for input in [
        "http://provider.example",
        "https://user@provider.example",
        "https://provider.example?",
        "https://provider.example#",
        "https://provider.example/path",
        "https://provider.example//",
        "https://provider.example:\t443",
        " https://provider.example",
        "https://provider.example\\evil",
        "https://provider.example:0",
        "https://provider.example:",
        "https://provider.example:65536",
        "https://prov%69der.example",
        "https://provider..example",
        "https://provider.example\n",
        "https://-bad.example",
        "https://prövider.example",
        "https://[::1]evil",
        "https://[::1%eth0]",
        "https://[v1.1234]",
    ] {
        assert!(normalize_origin(input).is_err(), "{input}");
    }
    assert_eq!(
        origin_from_discover("HTTPS://PROVIDER.EXAMPLE:443/discover").unwrap(),
        "https://provider.example"
    );
    for input in [
        "https://provider.example//discover",
        "https://provider.example/prefix/discover",
        "https://provider.example/discover?x=1",
        "https://provider.example/discover/",
    ] {
        assert!(origin_from_discover(input).is_err());
    }
}

#[test]
fn routes_publish_one_exact_identity_and_no_private_configuration() {
    let (config, identity) = declarations();
    let documents = DiscoveryDocuments::new(&config, &identity).unwrap();
    let response = documents.get("/discover").unwrap();
    assert_eq!(
        response,
        documents
            .get("/.well-known/agent-registration.json")
            .unwrap()
    );
    assert!(
        response
            .headers
            .contains(&("content-length".into(), response.body.len().to_string()))
    );
    assert!(response.headers.contains(&(
        "content-type".into(),
        "application/json; charset=utf-8".into()
    )));
    assert!(
        response
            .headers
            .contains(&("cache-control".into(), "no-store".into()))
    );
    let raw = String::from_utf8(response.body.clone()).unwrap();
    assert!(raw.contains(&format!("\"agentId\":{}", U256::MAX)));
    let registration = parse_document(&response.body, MAX_DOCUMENT_BYTES).unwrap();
    assert_eq!(registration["services"][1]["skills"], json!(["chat"]));
    assert!(registration.get("image").is_none());
    assert!(registration.get("supportedTrust").is_none());
    assert_eq!(
        verify_registration(&response.body, &identity, None).unwrap(),
        "https://provider.example"
    );
    let cards = documents.get("/models").unwrap();
    assert_eq!(
        verify_models(&cards.body, identity.catalog().unwrap()).unwrap(),
        vec!["example/model"]
    );
    assert!(
        parse_document(&cards.body, MAX_DOCUMENT_BYTES).unwrap()["models"][0]
            .get("rates")
            .is_none()
    );
    for body in [&response.body, &cards.body] {
        let text = String::from_utf8_lossy(body);
        for private in [
            "127.0.0.1:8080",
            "UPSTREAM_KEY",
            "collection_margin_ms",
            "wallet_id",
        ] {
            assert!(!text.contains(private));
        }
    }
    for path in [
        "/v1/chat/completions",
        "/discover/",
        "/unknown",
        "/models?token=secret",
    ] {
        assert!(documents.get(path).is_none());
    }
}

#[test]
fn mismatched_identity_or_prices_cannot_produce_documents() {
    let (config, identity) = declarations();
    for field in [
        "provider_id",
        "owner",
        "recipient",
        "chain",
        "origin",
        "catalog",
    ] {
        let catalog = if field == "catalog" {
            let mut values = support::configuration();
            values["models"][0]["rates"]["output"] = json!("1");
            Some(
                ProviderConfig::from_mapping(&values, None)
                    .unwrap()
                    .catalog()
                    .clone(),
            )
        } else {
            identity.catalog().cloned()
        };
        let changed = ProviderIdentity::new(
            ChainObservation::new(
                if field == "chain" { 5042 } else { 5042002 },
                Address::repeat_byte(b'c'),
                10,
                B256::repeat_byte(b'b'),
            )
            .unwrap(),
            identity.registry_address(),
            if field == "provider_id" {
                U256::from(1)
            } else {
                identity.provider_id()
            },
            if field == "owner" {
                Address::repeat_byte(b'x')
            } else {
                identity.owner()
            },
            if field == "recipient" {
                Address::repeat_byte(b'x')
            } else {
                identity.recipient()
            },
            if field == "origin" {
                "https://other.example/discover".into()
            } else {
                identity.agent_uri().into()
            },
            catalog,
        )
        .unwrap();
        assert!(
            DiscoveryDocuments::new(&config, &changed).is_err(),
            "{field}"
        );
    }
    let no_catalog = ProviderIdentity::new(
        *identity.observation(),
        identity.registry_address(),
        identity.provider_id(),
        identity.owner(),
        identity.recipient(),
        identity.agent_uri().into(),
        None,
    )
    .unwrap();
    let documents = DiscoveryDocuments::new(&config, &no_catalog).unwrap();
    assert_eq!(
        parse_document(documents.models(), MAX_DOCUMENT_BYTES).unwrap(),
        json!({"models":[]})
    );
    assert!(verify_registration(documents.registration(), &no_catalog, None).is_ok());
}

#[test]
fn registration_binds_uint256_without_float_or_string_coercion() {
    let (config, identity) = declarations();
    let documents = DiscoveryDocuments::new(&config, &identity).unwrap();
    let original = parse_document(documents.registration(), MAX_DOCUMENT_BYTES).unwrap();
    let mut doc = original.clone();
    doc["custom"] = json!({"fraction":0.1});
    doc["registrations"].as_array_mut().unwrap().push(
        json!({"agentId":5,"agentRegistry":format!("eip155:1:{:#x}",Address::repeat_byte(b'x'))}),
    );
    assert!(
        verify_registration(
            &serde_json::to_vec(&doc).unwrap(),
            &identity,
            Some("HTTPS://PROVIDER.EXAMPLE:443/")
        )
        .is_ok()
    );
    for value in [
        json!(true),
        json!(1.0),
        json!(U256::MAX.to_string()),
        json!(1),
        serde_json::from_str::<Value>(
            "115792089237316195423570985008687907853269984665640564039457584007913129639936",
        )
        .unwrap(),
    ] {
        let mut doc = original.clone();
        doc["registrations"][0]["agentId"] = value;
        assert!(verify_registration(&serde_json::to_vec(&doc).unwrap(), &identity, None).is_err());
    }
    let mut doc = original.clone();
    let duplicate = doc["registrations"][0].clone();
    doc["registrations"].as_array_mut().unwrap().push(duplicate);
    assert!(verify_registration(&serde_json::to_vec(&doc).unwrap(), &identity, None).is_err());
    assert!(
        verify_registration(
            documents.registration(),
            &identity,
            Some("https://other.example")
        )
        .is_err()
    );
}

#[test]
fn identity_service_and_card_substitutions_reject() {
    let (config, identity) = declarations();
    let documents = DiscoveryDocuments::new(&config, &identity).unwrap();
    let original = parse_document(documents.registration(), MAX_DOCUMENT_BYTES).unwrap();
    for (pointer, value) in [
        (
            "/registrations/0/agentRegistry",
            json!(format!("eip155:1:{:#x}", identity.registry_address())),
        ),
        (
            "/registrations/0/agentRegistry",
            json!(format!(
                "eip155:05042002:{:#x}",
                identity.registry_address()
            )),
        ),
        (
            "/registrations/0/agentRegistry",
            json!(format!("eip155:5042002:{:#x}", Address::repeat_byte(b'x'))),
        ),
        ("/active", json!(false)),
        ("/x402Support", json!(true)),
        (
            "/services/0/endpoint",
            json!("https://provider.example.evil"),
        ),
        (
            "/services/0/models",
            json!("https://provider.example.evil/models"),
        ),
        ("/services/0/version", json!(1)),
    ] {
        let mut doc = original.clone();
        *doc.pointer_mut(pointer).unwrap() = value;
        assert!(
            verify_registration(&serde_json::to_vec(&doc).unwrap(), &identity, None).is_err(),
            "{pointer}"
        );
    }
    let original = parse_document(documents.models(), MAX_DOCUMENT_BYTES).unwrap();
    let mut doc = original.clone();
    doc["models"][0]["rates"] = json!({"input":"0","output":"0"});
    assert!(
        verify_models(
            &serde_json::to_vec(&doc).unwrap(),
            identity.catalog().unwrap()
        )
        .is_ok()
    );
    assert_eq!(
        identity
            .catalog()
            .unwrap()
            .rates_for("example/model")
            .unwrap()
            .output(),
        1200000
    );
    for (pointer, value) in [
        ("/models/0/model", json!("other/model")),
        ("/models/0/context_window", json!(true)),
        ("/models/0/max_output", json!(128001)),
        ("/models/0/modalities", json!(["text", "text"])),
        ("/models/0/tools", json!(1)),
    ] {
        let mut doc = original.clone();
        *doc.pointer_mut(pointer).unwrap() = value;
        assert!(
            verify_models(
                &serde_json::to_vec(&doc).unwrap(),
                identity.catalog().unwrap()
            )
            .is_err()
        );
    }
}

#[test]
fn untrusted_documents_reject_hidden_duplicates_unicode_and_limits() {
    for raw in [
        br#"{"a":1,"\u0061":2}"#.as_slice(),
        b"\xef\xbb\xbf{}",
        br#"{"x":NaN}"#,
        b"[]",
        b"{\"a\":\"\xff\"}",
        br#"{"x":"\ud800"}"#,
        br#"{"extension":[{"key":1,"key":2}]}"#,
    ] {
        assert!(parse_document(raw, MAX_DOCUMENT_BYTES).is_err());
    }
    let deep = format!("{{\"x\":{}0{}}}", "[".repeat(128), "]".repeat(128));
    assert_eq!(
        parse_document(deep.as_bytes(), MAX_DOCUMENT_BYTES)
            .unwrap_err()
            .code(),
        ValidationCode::LimitExceeded
    );
    assert!(parse_document(b"{}", 1).is_err());
    assert!(parse_document(b"{}", 0).is_err());
    let exact = format!("{{\"agentId\":{}}}", U256::MAX);
    assert_eq!(
        parse_document(exact.as_bytes(), MAX_DOCUMENT_BYTES).unwrap()["agentId"]
            .as_number()
            .unwrap()
            .to_string(),
        U256::MAX.to_string()
    );
    // Serde's private arbitrary-precision marker is ordinary untrusted object data.
    let marker = br#"{"$serde_json::private::Number":"42"}"#;
    assert!(
        parse_document(marker, MAX_DOCUMENT_BYTES)
            .unwrap()
            .is_object()
    );
}

#[test]
fn proposed_registration_does_not_fabricate_publication_evidence() {
    let (config, identity) = declarations();
    let mut desired = config.to_mapping();
    desired["provider"]["public_origin"] = "https://next.example".into();
    let desired = ProviderConfig::from_mapping(&desired, None).unwrap();
    let proposed =
        mppi_provider::discovery::proposed_registration(&desired, identity.registry_address())
            .unwrap();
    let doc = parse_document(&proposed, MAX_DOCUMENT_BYTES).unwrap();
    assert_eq!(doc["services"][0]["endpoint"], "https://next.example");
    assert_eq!(
        doc["registrations"][0]["agentId"].to_string(),
        U256::MAX.to_string()
    );
    assert!(DiscoveryDocuments::new(&desired, &identity).is_err());
    assert!(mppi_provider::discovery::proposed_registration(&desired, Address::ZERO).is_err());
    let current = DiscoveryDocuments::new(&config, &identity).unwrap();
    assert_eq!(
        mppi_provider::discovery::proposed_registration(&config, identity.registry_address())
            .unwrap(),
        current.registration()
    );
}
