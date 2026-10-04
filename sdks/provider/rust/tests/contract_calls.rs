//! Independent ABI word/offset oracle from contracts/src/IMPPI.sol.
use alloy_primitives::{Address, B256, U256, keccak256};
use mppi_provider::{
    capabilities::FundingAuthorization,
    contract::{ARC_USDC, ContractCalls},
    signing::SessionDescriptor,
    types::Domain,
};

fn descriptor() -> SessionDescriptor {
    SessionDescriptor {
        agent: Address::repeat_byte(0x11),
        sessionSigner: Address::repeat_byte(0x12),
        providerId: (U256::from(1) << 255) + U256::from(3),
        recipient: Address::repeat_byte(0x22),
        operator: Address::repeat_byte(0x33),
        token: ARC_USDC,
        salt: B256::repeat_byte(0x44),
    }
}
fn word(value: u64) -> Vec<u8> {
    U256::from(value).to_be_bytes::<32>().to_vec()
}
fn address(value: Address) -> Vec<u8> {
    let mut bytes = vec![0; 12];
    bytes.extend(value.as_slice());
    bytes
}
fn descriptor_words(d: &SessionDescriptor) -> Vec<u8> {
    [
        address(d.agent),
        address(d.sessionSigner),
        d.providerId.to_be_bytes::<32>().to_vec(),
        address(d.recipient),
        address(d.operator),
        address(d.token),
        d.salt.to_vec(),
    ]
    .concat()
}
fn bytes(value: &[u8]) -> Vec<u8> {
    let mut out = word(u64::try_from(value.len()).unwrap());
    out.extend(value);
    out.resize(32 + value.len().div_ceil(32) * 32, 0);
    out
}
fn call(signature: &str, args: Vec<u8>) -> Vec<u8> {
    let mut result = keccak256(signature.as_bytes())[..4].to_vec();
    result.extend(args);
    result
}
fn calls() -> ContractCalls {
    ContractCalls::new(
        Domain::new(5042002, Address::repeat_byte(0x99)).unwrap(),
        "USDC".into(),
        "2".into(),
    )
    .unwrap()
}
const DESC: &str = "(address,address,uint256,address,address,address,bytes32)";

#[test]
fn every_call_matches_independent_descriptor_and_dynamic_offsets() {
    let d = descriptor();
    let calls = calls();
    let proof = vec![0x01; 65];
    let raw_descriptor = descriptor_words(&d);
    for (name, prepared) in [
        ("requestClose", calls.request_close(&d).unwrap()),
        ("withdraw", calls.withdraw(&d).unwrap()),
        ("releaseIdle", calls.release_idle(&d).unwrap()),
    ] {
        assert_eq!(
            prepared.call().data(),
            call(&format!("{name}({DESC})"), raw_descriptor.clone())
        );
    }
    for (name, prepared) in [
        ("settle", calls.settle(&d, 100, 50, &proof).unwrap()),
        ("close", calls.close(&d, 100, 50, &proof).unwrap()),
    ] {
        assert_eq!(
            prepared.call().data(),
            call(
                &format!("{name}({DESC},uint64,uint64,bytes)"),
                [
                    raw_descriptor.clone(),
                    word(100u64),
                    word(50u64),
                    word(320u64),
                    bytes(&proof)
                ]
                .concat()
            )
        );
    }
    let start = U256::from(1) << 80;
    let end = start + U256::from(3600);
    let auth = FundingAuthorization::new(
        5042002,
        ARC_USDC,
        "USDC".into(),
        "2".into(),
        d.agent,
        Address::repeat_byte(0x99),
        U256::from(100),
        start,
        end,
        B256::repeat_byte(0x77),
    )
    .unwrap();
    let opaque = (0..81).collect::<Vec<u8>>();
    let auth_words = [
        start.to_be_bytes::<32>().to_vec(),
        end.to_be_bytes::<32>().to_vec(),
        vec![0x77; 32],
        word(128u64),
        bytes(&opaque),
    ]
    .concat();
    let opened = calls
        .open(&d, 100, B256::repeat_byte(0x66), &auth, &opaque)
        .unwrap();
    assert_eq!(
        opened.call().data(),
        call(
            &format!("open({DESC},uint64,bytes32,(uint256,uint256,bytes32,bytes))"),
            [
                raw_descriptor.clone(),
                word(100u64),
                vec![0x66; 32],
                word(320u64),
                auth_words.clone()
            ]
            .concat()
        )
    );
    let top_up = calls.top_up(&d, 100, &auth, &opaque).unwrap();
    assert_eq!(
        top_up.call().data(),
        call(
            &format!("topUp({DESC},uint64,(uint256,uint256,bytes32,bytes))"),
            [
                raw_descriptor.clone(),
                word(100u64),
                word(288u64),
                auth_words
            ]
            .concat()
        )
    );
    let batch = calls.settle_batch(&[(&d, 100, 50, &proof)]).unwrap();
    assert_eq!(
        batch.call().data(),
        call(
            &format!("settleBatch({DESC}[],uint64[],uint64[],bytes[])"),
            [
                word(128u64),
                word(384u64),
                word(448u64),
                word(512u64),
                word(1u64),
                raw_descriptor,
                word(1u64),
                word(100u64),
                word(1u64),
                word(50u64),
                word(1u64),
                word(32u64),
                bytes(&proof),
            ]
            .concat()
        )
    );
    assert_eq!(
        batch.session_ids(),
        vec![calls.domain().signing_context().session_id(&d)]
    );
}

#[test]
fn authorization_and_capture_reject_mismatches_before_wallet_handoff() {
    let d = descriptor();
    let calls = calls();
    let proof = vec![1; 65];
    assert!(calls.settle(&d, 49, 50, &proof).is_err());
    assert!(calls.settle(&d, 50, 50, &[]).is_err());
    assert!(calls.settle(&d, 50, 50, &[1; 64]).is_err());
    assert!(calls.close(&d, 50, 50, &[]).is_ok());
    let mut bad = d.clone();
    bad.token = Address::repeat_byte(0x55);
    assert!(calls.request_close(&bad).is_err());
    bad = d.clone();
    bad.sessionSigner = Address::ZERO;
    assert!(calls.withdraw(&bad).is_err());
    for (chain, token, name, owner, recipient, value) in [
        (
            5042,
            ARC_USDC,
            "USDC",
            d.agent,
            Address::repeat_byte(0x99),
            100,
        ),
        (
            5042002,
            Address::repeat_byte(0x55),
            "USDC",
            d.agent,
            Address::repeat_byte(0x99),
            100,
        ),
        (
            5042002,
            ARC_USDC,
            "wrong",
            d.agent,
            Address::repeat_byte(0x99),
            100,
        ),
        (
            5042002,
            ARC_USDC,
            "USDC",
            d.sessionSigner,
            Address::repeat_byte(0x99),
            100,
        ),
        (5042002, ARC_USDC, "USDC", d.agent, d.recipient, 100),
        (
            5042002,
            ARC_USDC,
            "USDC",
            d.agent,
            Address::repeat_byte(0x99),
            101,
        ),
    ] {
        let auth = FundingAuthorization::new(
            chain,
            token,
            name.into(),
            "2".into(),
            owner,
            recipient,
            U256::from(value),
            U256::from(1),
            U256::from(2),
            B256::ZERO,
        )
        .unwrap();
        assert!(calls.top_up(&d, 100, &auth, &[1]).is_err());
    }
}
