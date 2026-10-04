use alloy_primitives::{Address, B256, U256, hex};
use mppi_provider::signing::*;
use serde_json::Value;

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../../testvectors/signing.json")).unwrap()
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap()
}
fn b256(value: &Value, key: &str) -> B256 {
    text(value, key).parse().unwrap()
}

fn context_and_descriptor(f: &Value) -> (SigningContext, SessionDescriptor) {
    let d = &f["descriptor"];
    (
        SigningContext {
            chain_id: text(&f["domain"], "chainId").parse().unwrap(),
            verifying_contract: text(&f["domain"], "verifyingContract").parse().unwrap(),
        },
        SessionDescriptor {
            agent: text(d, "agent").parse().unwrap(),
            sessionSigner: text(d, "sessionSigner").parse().unwrap(),
            providerId: text(d, "providerId").parse().unwrap(),
            recipient: text(d, "recipient").parse().unwrap(),
            operator: text(d, "operator").parse().unwrap(),
            token: text(d, "token").parse().unwrap(),
            salt: b256(d, "salt"),
        },
    )
}

fn assert_case<T: SessionProof>(
    c: &Value,
    proof: T,
    context: SigningContext,
    d: &SessionDescriptor,
) {
    assert_eq!(T::eip712_encode_type(), text(c, "typeString"));
    assert_eq!(proof.eip712_hash_struct(), b256(c, "structHash"));
    assert_eq!(context.digest(&proof), b256(c, "digest"));
    assert_eq!(
        d.sessionSigner,
        text(c, "expectedSigner").parse::<Address>().unwrap()
    );
    let signature = hex::decode(text(c, "signature")).unwrap();
    context.verify(d, &proof, &signature).unwrap();
    let wrong_chain = SigningContext {
        chain_id: context.chain_id + 1,
        ..context
    };
    assert_eq!(
        wrong_chain.verify(d, &proof, &signature),
        Err(VerificationError::SessionMismatch)
    );
    let wrong_proxy = SigningContext {
        verifying_contract: Address::repeat_byte(0x88),
        ..context
    };
    assert_eq!(
        wrong_proxy.verify(d, &proof, &signature),
        Err(VerificationError::SessionMismatch)
    );
    assert_eq!(
        context.verify(d, &proof, &signature[..64]),
        Err(VerificationError::SignatureLength)
    );
    let mut invalid_v = signature.clone();
    invalid_v[64] -= 27;
    assert_eq!(
        context.verify(d, &proof, &invalid_v),
        Err(VerificationError::RecoveryId)
    );
    let mut high_s = signature.clone();
    let order: U256 = "0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141"
        .parse()
        .unwrap();
    let s = U256::from_be_slice(&signature[32..64]);
    high_s[32..64].copy_from_slice(&(order - s).to_be_bytes::<32>());
    high_s[64] = if signature[64] == 27 { 28 } else { 27 };
    assert_eq!(
        context.verify(d, &proof, &high_s),
        Err(VerificationError::HighS)
    );
    let mut invalid = vec![0u8; 65];
    invalid[64] = 27;
    assert_eq!(
        context.verify(d, &proof, &invalid),
        Err(VerificationError::InvalidSignature)
    );
}

#[test]
fn all_shared_proofs_and_canonical_signature_failures() {
    let f = fixture();
    let (context, d) = context_and_descriptor(&f);
    assert_eq!(context.session_id(&d), b256(&f, "sessionId"));
    assert_eq!(context.domain().separator(), b256(&f, "domainSeparator"));
    for c in f["cases"].as_array().unwrap() {
        let m = &c["message"];
        let id = b256(m, "sessionId");
        match text(c, "kind") {
            "Voucher" => assert_case(
                c,
                Voucher {
                    sessionId: id,
                    cumulativeAmount: text(m, "cumulativeAmount").parse().unwrap(),
                },
                context,
                &d,
            ),
            "RunAuth" => assert_case(
                c,
                RunAuth {
                    sessionId: id,
                    controlGeneration: text(m, "controlGeneration").parse().unwrap(),
                    runId: b256(m, "runId"),
                    model: text(m, "model").into(),
                    rateCardHash: b256(m, "rateCardHash"),
                },
                context,
                &d,
            ),
            "ReconnectAuth" => assert_case(
                c,
                ReconnectAuth {
                    sessionId: id,
                    nonce: b256(m, "nonce"),
                },
                context,
                &d,
            ),
            "CloseAuth" => assert_case(c, CloseAuth { sessionId: id }, context, &d),
            kind => panic!("unknown fixture proof {kind}"),
        }
    }
}

#[test]
fn proof_binds_amount_generation_run_model_price_and_nonce() {
    let f = fixture();
    let (context, d) = context_and_descriptor(&f);
    let cases = f["cases"].as_array().unwrap();
    let voucher_case = cases.iter().find(|c| text(c, "name") == "voucher").unwrap();
    let voucher_sig = hex::decode(text(voucher_case, "signature")).unwrap();
    let id = context.session_id(&d);
    assert_eq!(
        context.verify(
            &d,
            &Voucher {
                sessionId: id,
                cumulativeAmount: 5_510_001
            },
            &voucher_sig
        ),
        Err(VerificationError::WrongSigner)
    );
    let run_case = cases
        .iter()
        .find(|c| text(c, "name") == "run_auth")
        .unwrap();
    let sig = hex::decode(text(run_case, "signature")).unwrap();
    let m = &run_case["message"];
    let original = RunAuth {
        sessionId: id,
        controlGeneration: 7,
        runId: b256(m, "runId"),
        model: text(m, "model").into(),
        rateCardHash: b256(m, "rateCardHash"),
    };
    for changed in [
        RunAuth {
            controlGeneration: 8,
            ..original.clone()
        },
        RunAuth {
            runId: B256::ZERO,
            ..original.clone()
        },
        RunAuth {
            model: "other-model".into(),
            ..original.clone()
        },
        RunAuth {
            rateCardHash: B256::ZERO,
            ..original
        },
    ] {
        assert_eq!(
            context.verify(&d, &changed, &sig),
            Err(VerificationError::WrongSigner)
        );
    }
    let reconnect = cases
        .iter()
        .find(|c| text(c, "kind") == "ReconnectAuth")
        .unwrap();
    assert_eq!(
        context.verify(
            &d,
            &ReconnectAuth {
                sessionId: id,
                nonce: B256::ZERO
            },
            &hex::decode(text(reconnect, "signature")).unwrap()
        ),
        Err(VerificationError::WrongSigner)
    );
    // The funding wallet is intentionally different from the proof signer.
    let mut changed = d.clone();
    changed.sessionSigner = d.agent;
    assert_ne!(context.session_id(&changed), id);
    assert_eq!(
        context.verify(
            &changed,
            &Voucher {
                sessionId: id,
                cumulativeAmount: 5_510_000
            },
            &voucher_sig
        ),
        Err(VerificationError::SessionMismatch)
    );
    changed.sessionSigner = Address::ZERO;
    let zero_proof = Voucher {
        sessionId: context.session_id(&changed),
        cumulativeAmount: 0,
    };
    assert_eq!(
        context.verify(&changed, &zero_proof, &voucher_sig),
        Err(VerificationError::ZeroSigner)
    );
}
