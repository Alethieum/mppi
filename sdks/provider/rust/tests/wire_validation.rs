use mppi_provider::{Address, B256, U256, ValidationCode, proto as pb, types::*, wire::*};
use prost::Message;

fn request() -> pb::CompleteRequest {
    pb::CompleteRequest { model: Some("example/model".into()), stream: Some(true), session_id: Some(vec![1;32]),
        run_id: Some(vec![2;32]), control_generation: Some(1), rate_card_hash: Some(vec![3;32]),
        auth_signature: Some(vec![27;65]),
        payload: Some(br#" {"model":"example/model","stream":true,"seed":9007199254740993,"n":1e10000000000} "#.to_vec()) }
}
fn row() -> pb::RequiredTokens {
    pb::RequiredTokens {
        model: Some("example/model".into()),
        rate_card_hash: Some(vec![3; 32]),
        input_tokens: Some(0),
        cache_tokens: Some(0),
        output_tokens: Some(0),
    }
}
fn terminal() -> pb::Terminal {
    pb::Terminal {
        input_tokens: Some(0),
        cache_tokens: Some(0),
        output_tokens: Some(0),
        amount: Some(0),
        session_id: Some(vec![1; 32]),
        run_id: Some(vec![2; 32]),
        model: Some("example/model".into()),
        rate_card_hash: Some(vec![3; 32]),
        status: Some(1),
        final_meter_seq: Some(1),
        settlement_reference: None,
    }
}

#[test]
fn inference_preserves_original_bytes_and_never_coerces_numbers() {
    let value = request();
    validate_message(&value).unwrap();
    let raw = value.payload.as_ref().unwrap();
    assert_eq!(
        validate_json(raw, Some("example/model"), Some(true)).unwrap(),
        raw
    );
    assert!(validate_json(raw, Some("example/other"), Some(true)).is_err());
    assert!(validate_json(raw, Some("example/model"), Some(false)).is_err());
    assert!(
        validate_json(
            br#"{"model":"example/model"}"#,
            Some("example/model"),
            Some(false)
        )
        .is_ok()
    );
    assert!(validate_json(br#"{"stream":0}"#, None, Some(false)).is_err());
    assert!(validate_json(br#"{"stream":null}"#, None, Some(false)).is_err());
    let value =
        decode_message::<pb::CompleteRequest>(&value.encode_to_vec(), &WireLimits::default())
            .unwrap();
    assert_eq!(value.payload.as_ref().unwrap(), raw);
}

#[test]
fn rejects_malformed_unicode_duplicates_and_non_json_at_every_level() {
    for value in [
        b"\xef\xbb\xbf{}".as_slice(),
        b"{}\xef\xbb\xbf",
        b"[]",
        b"null",
        b"{\"x\":\xff}",
        br#"{"x":"\ud800"}"#,
        br#"{"x":["\udfff"]}"#,
        br#"{"\udfff":1}"#,
        br#"{"x":NaN}"#,
        br#"{"x":Infinity}"#,
        br#"{"x":-Infinity}"#,
        br#"{"a":1,"\u0061":2}"#,
        br#"{"x":[{"a":1,"a":2}]}"#,
        br#"{"x":01}"#,
        br#"{"x":1.}"#,
        br#"{"x":1e}"#,
        b"{} {}",
    ] {
        assert!(
            validate_json(value, None, None).is_err(),
            "accepted {value:?}"
        );
    }
    let utf16 = br#"{"x":"\ud83d\ude00","deep":[{},true,false,null,-0,2.50e+20]}"#;
    assert_eq!(validate_json(utf16, None, None).unwrap(), utf16);
    // serde_json's private arbitrary-precision marker must remain an ordinary key.
    let marker = br#"{"$serde_json::private::Number":"not a number"}"#;
    assert_eq!(validate_json(marker, None, None).unwrap(), marker);
    assert!(
        validate_json(
            br#"{"$serde_json::private::Number":"1","$serde_json::private::Number":"2"}"#,
            None,
            None
        )
        .is_err()
    );
}

#[test]
fn depth_and_bytes_are_checked_before_decoding() {
    let at_limit = format!("{{\"x\":{}0{}}}", "[".repeat(127), "]".repeat(127));
    assert!(validate_json(at_limit.as_bytes(), None, None).is_ok());
    let too_deep = format!("{{\"x\":{}0{}}}", "[".repeat(128), "]".repeat(128));
    assert_eq!(
        validate_json(too_deep.as_bytes(), None, None)
            .unwrap_err()
            .code(),
        ValidationCode::LimitExceeded
    );
    let limits = WireLimits::new(64, 32, 32, 2, 1, 32).unwrap();
    assert!(validate_json_with_limits(br#"{"x":[{}]}"#, None, None, &limits).is_err());
    assert!(validate_json_with_limits(br#"{"x":"[[[[[[[[["}"#, None, None, &limits).is_ok());
    assert_eq!(
        decode_message::<pb::CompleteRequest>(&[0; 65], &limits)
            .unwrap_err()
            .code(),
        ValidationCode::LimitExceeded
    );
    assert!(WireLimits::new(MAX_MESSAGE_BYTES + 1, 1, 1, 1, 1, 1).is_err());
}

#[test]
fn decoded_presence_width_counters_enums_and_standard_protobuf_semantics() {
    let original = request();
    let mut value = original.clone();
    value.stream = None;
    assert!(validate_message(&value).is_err());
    let mut value = original.clone();
    value.control_generation = Some(0);
    assert!(validate_message(&value).is_err());
    let mut value = original.clone();
    value.auth_signature = Some(vec![0; 64]);
    assert!(validate_message(&value).is_err());
    let mut value = original.clone();
    value.session_id = Some(vec![1; 31]);
    assert!(validate_message(&value).is_err());
    let mut value = original.clone();
    value.rate_card_hash = None;
    assert!(validate_message(&value).is_err());
    assert!(validate_message(&pb::ControlMessage::default()).is_err());
    assert!(decode_message::<pb::ChatChunk>(b"\xa2\x06\x00", &WireLimits::default()).is_err());
    // Unknown ordinary fields are tolerated; repeated singular tags use last value.
    let mut raw = original.encode_to_vec();
    raw.extend_from_slice(b"\xb8\x3e\x01\x10\x01");
    assert_eq!(
        decode_message::<pb::CompleteRequest>(&raw, &WireLimits::default()).unwrap(),
        original
    );
    let mut value = terminal();
    value.status = Some(99);
    assert!(validate_message(&value).is_err());
    value.status = Some(0);
    assert!(validate_message(&value).is_err());
    value.status = Some(4);
    assert!(validate_message(&value).is_err());
    value.status = Some(1);
    value.final_meter_seq = Some(0);
    assert!(validate_message(&value).is_err());
    value.final_meter_seq = Some(1);
    value.cache_tokens = Some(1);
    assert!(validate_message(&value).is_err());
    assert!(
        validate_message(&pb::UpstreamError {
            body: Some(b"{}".to_vec()),
            http_status: Some(200)
        })
        .is_err()
    );
}

#[test]
fn optional_groups_pending_ids_coverage_and_not_admitted_are_exact() {
    let mut topup = pb::TopUpNotice {
        session_id: Some(vec![1; 32]),
        tx: Some(vec![2; 32]),
        ..Default::default()
    };
    validate_message(&topup).unwrap();
    topup.model = Some("example/model".into());
    assert!(validate_message(&topup).is_err());
    topup.rate_card_hash = Some(vec![3; 32]);
    topup.tokens = Some(0);
    assert!(validate_message(&topup).is_err());
    topup.tokens = Some(1);
    validate_message(&topup).unwrap();
    let mut coverage = pb::CoverageRequest {
        run_id: Some(vec![2; 32]),
        control_generation: Some(1),
        required_by_price: vec![],
    };
    assert!(validate_message(&coverage).is_err());
    coverage.required_by_price.push(row());
    validate_message(&coverage).unwrap();
    coverage.required_by_price.push(row());
    assert!(validate_message(&coverage).is_err());
    let mut run = pb::RunState {
        run_id: Some(vec![2; 32]),
        model: Some(String::new()),
        status: Some(4),
        input_tokens: Some(0),
        cache_tokens: Some(0),
        output_tokens: Some(0),
        ..Default::default()
    };
    validate_message(&run).unwrap();
    run.rate_card_hash = Some(vec![3; 32]);
    assert!(validate_message(&run).is_err());
    run.rate_card_hash = None;
    run.output_tokens = Some(1);
    assert!(validate_message(&run).is_err());
    let mut reconnect = pb::ReconnectRequest {
        session_id: Some(vec![1; 32]),
        nonce: Some(vec![2; 32]),
        signature: Some(vec![27; 65]),
        last_meter_seq: Some(0),
        pending_run_ids: vec![vec![1; 32]],
        latest_voucher: None,
    };
    validate_message(&reconnect).unwrap();
    reconnect.pending_run_ids.push(vec![1; 32]);
    assert!(validate_message(&reconnect).is_err());
    reconnect.pending_run_ids = (0..1025u32)
        .map(|n| {
            let mut id = vec![0; 32];
            id[..4].copy_from_slice(&n.to_be_bytes());
            id
        })
        .collect();
    assert_eq!(
        validate_message(&reconnect).unwrap_err().code(),
        ValidationCode::LimitExceeded
    );
}

#[test]
fn ordinary_control_limit_counts_unknown_wire_bytes_and_direction() {
    let value = pb::ControlMessage {
        kind: Some(pb::control_message::Kind::ReconnectChallenge(
            pb::ReconnectChallenge {
                nonce: Some(vec![1; 32]),
            },
        )),
    };
    validate_control_direction(&value, ControlSender::Provider).unwrap();
    assert!(validate_control_direction(&value, ControlSender::Agent).is_err());
    let mut raw = value.encode_to_vec();
    // Unknown field 100, length 65536, still counts toward received bytes even
    // though prost does not retain its contents in the decoded struct.
    raw.extend_from_slice(&[0xa2, 0x06, 0x80, 0x80, 0x04]);
    raw.extend(vec![0; 65536]);
    assert!(raw.len() < MAX_MESSAGE_BYTES);
    assert_eq!(
        decode_message::<pb::ControlMessage>(&raw, &WireLimits::default())
            .unwrap_err()
            .code(),
        ValidationCode::LimitExceeded
    );
}

#[test]
fn fixed_width_conversions_preserve_uint256_zero_and_high_bit() {
    let mut descriptor = mppi_provider::signing::SessionDescriptor {
        agent: Address::repeat_byte(1),
        sessionSigner: Address::repeat_byte(2),
        providerId: U256::MAX,
        recipient: Address::repeat_byte(3),
        operator: Address::repeat_byte(4),
        token: Address::repeat_byte(5),
        salt: B256::ZERO,
    };
    let wire = descriptor_to_wire(&descriptor).unwrap();
    assert_eq!(wire.provider_id.as_ref().unwrap().len(), 32);
    assert_eq!(descriptor_from_wire(&wire).unwrap(), descriptor);
    descriptor.providerId = U256::ZERO;
    assert_eq!(
        descriptor_to_wire(&descriptor).unwrap().provider_id,
        Some(vec![0; 32])
    );
    descriptor.sessionSigner = Address::ZERO;
    assert!(descriptor_to_wire(&descriptor).is_err());
    let observation = ChainObservation::new(1, Address::repeat_byte(1), 0, B256::ZERO).unwrap();
    assert_eq!(
        observation_from_wire(&observation_to_wire(&observation)).unwrap(),
        observation
    );
    let settlement =
        ConfirmedSettlementRef::new(observation, B256::ZERO, B256::repeat_byte(1), 2, 1).unwrap();
    assert_eq!(
        settlement_from_wire(&settlement_to_wire(&settlement)).unwrap(),
        settlement
    );
}

fn failure(reason: &str) -> PaymentFailure {
    PaymentFailure::new(
        PaymentCode::SessionNotReady,
        Some(reason.into()),
        Some(B256::repeat_byte(1)),
        Some(B256::repeat_byte(2)),
        None,
        None,
        None,
    )
    .unwrap()
}
#[test]
fn payment_error_native_status_and_rich_diagnostics_are_bounded() {
    let failures = [
        failure("AUTHORIZATION_REQUIRED"),
        failure("future_unknown_reason"),
        PaymentFailure::new(
            PaymentCode::InsufficientDeposit,
            None,
            Some(B256::ZERO),
            Some(B256::ZERO),
            None,
            None,
            None,
        )
        .unwrap(),
        PaymentFailure::new(
            PaymentCode::SessionNotReady,
            Some("CONTRACT_PAUSED".into()),
            None,
            None,
            None,
            None,
            Some(PauseLevel::All),
        )
        .unwrap(),
        PaymentFailure::new(
            PaymentCode::SessionNotReady,
            Some("PRICE_CHANGED".into()),
            None,
            None,
            Some(B256::ZERO),
            Some(B256::repeat_byte(1)),
            None,
        )
        .unwrap(),
    ];
    for value in failures {
        let status = payment_status(&value);
        assert!(status.details().len() <= 2048);
        assert_eq!(parse_payment_error(&status).unwrap(), Some(value));
        assert!(
            parse_payment_error_details(
                tonic::Code::Ok,
                &[status.details()],
                &WireLimits::default()
            )
            .is_err()
        );
        assert!(
            parse_payment_error_details(
                status.code(),
                &[status.details(), status.details()],
                &WireLimits::default()
            )
            .is_err()
        );
    }
    let status = payment_status(&failure(&"x".repeat(3000)));
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.details().is_empty());
    assert!(parse_payment_error(&status).unwrap().is_none());
    assert!(
        parse_payment_error_details(
            tonic::Code::FailedPrecondition,
            &[&vec![0; 2049]],
            &WireLimits::default()
        )
        .is_err()
    );
    assert!(
        parse_payment_error_details(
            tonic::Code::FailedPrecondition,
            &[b"\x80"],
            &WireLimits::default()
        )
        .is_err()
    );
    // A carrier with a matching code and exactly one Any but a wrong type URL.
    let raw = b"\x08\x09\x1a\x07\x0a\x05wrong";
    assert!(
        parse_payment_error_details(
            tonic::Code::FailedPrecondition,
            &[raw],
            &WireLimits::default()
        )
        .is_err()
    );
}

#[test]
fn payment_diagnostics_reject_missing_and_inapplicable_evidence() {
    for value in [
        pb::PaymentError {
            code: Some(4),
            ..Default::default()
        },
        pb::PaymentError {
            code: Some(4),
            reason: Some("PRICE_CHANGED".into()),
            ..Default::default()
        },
        pb::PaymentError {
            code: Some(4),
            reason: Some("CONTRACT_PAUSED".into()),
            pause_level: Some(0),
            ..Default::default()
        },
        pb::PaymentError {
            code: Some(1),
            run_id: Some(vec![0; 32]),
            ..Default::default()
        },
        pb::PaymentError {
            code: Some(1),
            requested_rate_card_hash: Some(vec![0; 32]),
            ..Default::default()
        },
        pb::PaymentError {
            code: Some(4),
            reason: Some("unknown".into()),
            pause_level: Some(99),
            ..Default::default()
        },
    ] {
        assert!(validate_message(&value).is_err());
    }
}
