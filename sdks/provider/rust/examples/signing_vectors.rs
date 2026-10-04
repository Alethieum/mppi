//! Generate public, insecure test fixtures. No wallet secrets are used.
use alloy_primitives::{Address, B256, U256, hex};
use k256::ecdsa::SigningKey;
use mppi_provider::signing::*;
use serde_json::{Value, json};

fn case<T: SessionProof>(
    name: &str,
    kind: &str,
    proof: T,
    message: Value,
    context: SigningContext,
) -> Value {
    let key = SigningKey::from_slice(&[&[0u8; 31][..], &[1u8][..]].concat()).unwrap();
    let digest = context.digest(&proof);
    let (signature, recovery) = key.sign_prehash_recoverable(digest.as_slice()).unwrap();
    let mut bytes = signature.to_bytes().to_vec();
    bytes.push(27 + recovery.to_byte());
    json!({
        "name": name, "kind": kind, "typeString": T::eip712_encode_type(), "message": message,
        "structHash": format!("{:#x}", proof.eip712_hash_struct()),
        "digest": format!("{digest:#x}"), "signature": format!("0x{}", hex::encode(bytes)),
        "expectedSigner": "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
    })
}

fn main() {
    let context = SigningContext {
        chain_id: 5_042_002,
        verifying_contract: Address::repeat_byte(0x99),
    };
    let descriptor = SessionDescriptor {
        agent: Address::repeat_byte(0x11),
        sessionSigner: "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
            .parse()
            .unwrap(),
        providerId: U256::from(9_007_199_254_740_993u64),
        recipient: Address::repeat_byte(0x22),
        operator: Address::repeat_byte(0x33),
        token: "0x3600000000000000000000000000000000000000"
            .parse()
            .unwrap(),
        salt: B256::repeat_byte(0x44),
    };
    let id = context.session_id(&descriptor);
    let run_id = B256::repeat_byte(0x55);
    let rate_hash = B256::repeat_byte(0x66);
    let nonce = B256::repeat_byte(0x77);
    let mut cases = Vec::new();
    for (name, amount) in [
        ("voucher", 5_510_000),
        ("voucher_zero", 0),
        ("voucher_max", u64::MAX),
    ] {
        cases.push(case(
            name,
            "Voucher",
            Voucher {
                sessionId: id,
                cumulativeAmount: amount,
            },
            json!({"sessionId": format!("{id:#x}"), "cumulativeAmount": amount.to_string()}),
            context,
        ));
    }
    for (name, model) in [
        ("run_auth", "example-org/example-model"),
        ("run_auth_utf8", "example/模型"),
    ] {
        cases.push(case(name, "RunAuth", RunAuth { sessionId: id, controlGeneration: 7, runId: run_id,
                model: model.into(), rateCardHash: rate_hash },
            json!({"sessionId":format!("{id:#x}"),"controlGeneration":"7","runId":format!("{run_id:#x}"),
                "model":model,"rateCardHash":format!("{rate_hash:#x}")}), context));
    }
    cases.push(case(
        "reconnect_auth",
        "ReconnectAuth",
        ReconnectAuth {
            sessionId: id,
            nonce,
        },
        json!({"sessionId":format!("{id:#x}"),"nonce":format!("{nonce:#x}")}),
        context,
    ));
    cases.push(case(
        "close_auth",
        "CloseAuth",
        CloseAuth { sessionId: id },
        json!({"sessionId":format!("{id:#x}")}),
        context,
    ));
    let fixture = json!({
        "schemaVersion":1,
        "caseNotes":{"run_auth_utf8":"EIP-712 UTF-8 hashing only; this model does not satisfy MPPI's admissible catalog model syntax."},
        "testKeyWarning":"Public insecure test-only private key. Never fund this key or use it for a real session.",
        "privateKey":"0x0000000000000000000000000000000000000000000000000000000000000001",
        "domain":{"name":"MPPI","version":"1","chainId":context.chain_id.to_string(),"verifyingContract":format!("{:#x}",context.verifying_contract)},
        "domainSeparator":format!("{:#x}",context.domain().separator()),
        "descriptor":{"agent":format!("{:#x}",descriptor.agent),"sessionSigner":format!("{:#x}",descriptor.sessionSigner),
            "providerId":descriptor.providerId.to_string(),"recipient":format!("{:#x}",descriptor.recipient),
            "operator":format!("{:#x}",descriptor.operator),"token":format!("{:#x}",descriptor.token),"salt":format!("{:#x}",descriptor.salt)},
        "sessionId":format!("{id:#x}"), "cases": cases
    });
    println!("{}", serde_json::to_string_pretty(&fixture).unwrap());
}
