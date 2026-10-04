//! Ordinary funding warnings and continued coverage over original admitted work.
use super::*;

async fn funded(deposit: u64) -> Fixture {
    Fixture::with_wallet(1_000_000, Arc::new(Bindings), |rpc| {
        rpc.deposit.store(deposit, Ordering::SeqCst);
        rpc
    })
    .await
}

async fn messages(fixture: &Fixture) -> Vec<proto::control_message::Kind> {
    let queue = fixture.provider.inner.lock().unwrap().attachments[&1]
        .queue
        .clone();
    let mut result = Vec::new();
    while queue.buffered().unwrap().0 > 0 {
        result.push(
            proto::ControlMessage::decode(queue.get().await.unwrap().as_slice())
                .unwrap()
                .kind
                .unwrap(),
        );
    }
    result
}

fn warning(messages: &[proto::control_message::Kind]) -> &proto::LowCredit {
    assert_eq!(messages.len(), 2, "expected Meter followed by LowCredit");
    assert!(matches!(
        messages[0],
        proto::control_message::Kind::Meter(_)
    ));
    let proto::control_message::Kind::LowCredit(value) = &messages[1] else {
        panic!("missing ordinary LowCredit after Meter");
    };
    assert!(value.coverage_request.is_none());
    value
}

#[tokio::test]
async fn ordinary_low_credit_arrives_before_current_coverage_is_exhausted() {
    let fixture = funded(2999).await;
    fixture.voucher(1000);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(100), None)
        .unwrap();
    let sent = messages(&fixture).await;
    let advice = warning(&sent);
    assert_eq!(advice.remaining_tokens, Some(1999));
    assert_eq!(advice.suggest_top_up_tokens, Some(1_000_000));
    assert_eq!(advice.session_id.as_deref(), Some(fixture.sid.as_slice()));
    assert_eq!(
        advice.rate_card_hash.as_deref(),
        Some(run.context.rate_card_hash().as_slice())
    );
    assert!(!run.stop.is_cancelled());
    let state = fixture.provider.inner.lock().unwrap();
    let session = &state.sessions[&fixture.sid];
    assert_eq!(session.voucher.as_ref().unwrap().cumulative, Some(1000));
    assert_eq!(
        session.runs[&hash(1)].accounting.remaining_numerator(),
        U256::from(900_000_000u64)
    );
}

#[tokio::test]
async fn ordinary_warning_uses_strict_exact_threshold_and_deduplicates_headroom() {
    let fixture = funded(3000).await;
    fixture.voucher(3000);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(0), None)
        .unwrap();
    assert_eq!(messages(&fixture).await.len(), 1); // Exactly two intervals remains.
    assert!(
        fixture
            .provider
            .inner
            .require_coverage_sync(&run.context, counts(1001))
            .unwrap()
            .granted()
    );
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(1), None)
        .unwrap();
    assert_eq!(
        warning(&messages(&fixture).await).remaining_tokens,
        Some(1999)
    );
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(1), None)
        .unwrap();
    assert!(messages(&fixture).await.is_empty());
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(2), None)
        .unwrap();
    assert_eq!(messages(&fixture).await.len(), 1); // N rises exactly as R falls.
    assert!(
        fixture
            .provider
            .inner
            .require_coverage_sync(&run.context, counts(1000))
            .unwrap()
            .granted()
    );
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(3), None)
        .unwrap();
    assert_eq!(
        warning(&messages(&fixture).await).remaining_tokens,
        Some(1998)
    );
}

#[tokio::test]
async fn confirmed_deposit_refill_clears_warning_and_later_low_headroom_warns_again() {
    let fixture = funded(2999).await;
    fixture.voucher(2999);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(100), None)
        .unwrap();
    warning(&messages(&fixture).await);
    fixture.rpc.deposit.store(5000, Ordering::SeqCst);
    fixture.provider.inner.chain.refresh().await.unwrap();
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(101), None)
        .unwrap();
    assert_eq!(messages(&fixture).await.len(), 1);
    fixture.voucher(5000);
    assert!(
        fixture
            .provider
            .inner
            .require_coverage_sync(&run.context, counts(3000))
            .unwrap()
            .granted()
    );
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(102), None)
        .unwrap();
    assert_eq!(
        warning(&messages(&fixture).await).remaining_tokens,
        Some(1899)
    );
}

#[tokio::test]
async fn other_admitted_reservations_count_but_prepared_requests_do_not() {
    let fixture = funded(3999).await;
    fixture.voucher(3999);
    let first = fixture.admit(1);
    let second = fixture.begin(2);
    fixture
        .provider
        .inner
        .report_sync(&first.context, counts(1), None)
        .unwrap();
    assert_eq!(messages(&fixture).await.len(), 1);
    assert!(matches!(
        fixture
            .provider
            .inner
            .attempt_admission(&second, counts(1000))
            .unwrap(),
        Admission::Granted
    ));
    fixture
        .provider
        .inner
        .report_sync(&first.context, counts(2), None)
        .unwrap();
    assert_eq!(
        warning(&messages(&fixture).await).remaining_tokens,
        Some(1999)
    );
}

async fn published_run(fixture: &Fixture, rates: Rates) -> RunHandle {
    let catalog = publish_catalog(
        vec![CatalogRow::new("example/model", rates).unwrap()],
        crate::DEFAULT_CATALOG_BYTES,
    )
    .unwrap();
    *fixture.rpc.catalog.lock().unwrap() = catalog.clone();
    fixture.provider.inner.chain.refresh().await.unwrap();
    let proof = crate::signing::RunAuth {
        sessionId: fixture.sid,
        controlGeneration: 1,
        runId: hash(1),
        model: "example/model".into(),
        rateCardHash: catalog.rate_card_hash(),
    };
    let mut request = fixture.request(1);
    request.rate_card_hash = Some(proof.rateCardHash.to_vec());
    request.auth_signature = Some(sign(&proof));
    let run = fixture.provider.inner.begin_run(request).unwrap();
    assert!(matches!(
        fixture
            .provider
            .inner
            .attempt_admission(&run, counts(1000))
            .unwrap(),
        Admission::Granted
    ));
    run
}

#[tokio::test]
async fn warning_threshold_uses_output_but_scalar_and_version_use_admitted_rates() {
    let fixture = funded(2999).await;
    fixture.voucher(2999);
    let run = published_run(&fixture, Rates::new(5_000_000, 1_000_000, Some(7_000_000))).await;
    // A later current catalog must not reprice this admitted warning.
    *fixture.rpc.catalog.lock().unwrap() = publish_catalog(
        vec![CatalogRow::new("example/model", Rates::new(0, 1, None)).unwrap()],
        crate::DEFAULT_CATALOG_BYTES,
    )
    .unwrap();
    fixture.provider.inner.chain.refresh().await.unwrap();
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(1), None)
        .unwrap();
    let sent = messages(&fixture).await;
    let advice = warning(&sent);
    assert_eq!(advice.remaining_tokens, Some(285)); // floor(1999 / 7), not 1999.
    assert_eq!(
        advice.rate_card_hash.as_deref(),
        Some(run.context.rate_card_hash().as_slice())
    );
}

#[tokio::test]
async fn warning_threshold_keeps_fractional_exact_headroom() {
    let fixture = Fixture::new(33_334).await;
    fixture.voucher(100);
    let run = fixture.admit(1);
    fixture
        .provider
        .inner
        .report_sync(&run.context, counts(1), None)
        .unwrap();
    // 100 - 33.334 = 66.666, strictly less than 2000 * .033334 = 66.668.
    assert_eq!(
        warning(&messages(&fixture).await).remaining_tokens,
        Some(1999)
    );
}

#[tokio::test]
async fn final_zero_output_stopped_stale_and_paused_reports_do_not_advise_funding() {
    for case in ["final", "zero_output", "stopped", "stale", "paused"] {
        let fixture = funded(2999).await;
        fixture.voucher(2999);
        let run = if case == "zero_output" {
            published_run(&fixture, Rates::new(1_000_000, 0, None)).await
        } else {
            fixture.admit(1)
        };
        match case {
            "stopped" => run.stop.cancel(),
            "stale" => fixture.advance_clock(1001),
            "paused" => {
                fixture.rpc.pause.store(1, Ordering::SeqCst);
                fixture.provider.inner.chain.refresh().await.unwrap();
            }
            _ => {}
        }
        fixture
            .provider
            .inner
            .report_sync(
                &run.context,
                counts(1),
                (case == "final").then_some(RunStatus::Completed),
            )
            .unwrap();
        let sent = messages(&fixture).await;
        assert!(
            matches!(sent[0], proto::control_message::Kind::Meter(_)),
            "{case}"
        );
        assert!(
            !sent
                .iter()
                .any(|m| matches!(m, proto::control_message::Kind::LowCredit(_))),
            "{case}"
        );
    }
}

#[tokio::test]
async fn warning_queue_pressure_keeps_bounded_detach_and_stop_behavior() {
    let fixture = funded(2999).await;
    fixture.voucher(1000);
    let run = fixture.admit(1);
    let queue = Arc::new(ByteQueue::new(1, wire::MAX_MESSAGE_BYTES, 1000).unwrap());
    fixture
        .provider
        .inner
        .lock()
        .unwrap()
        .attachments
        .get_mut(&1)
        .unwrap()
        .queue = queue.clone();
    let result = fixture
        .provider
        .inner
        .report_sync(&run.context, counts(1), None)
        .unwrap();
    assert_eq!(result.meter_seq(), 1);
    assert_eq!(queue.buffered().unwrap().0, 1);
    assert!(run.stop.is_cancelled());
    assert!(!fixture.provider.inner.lock().unwrap().attachments[&1].live);
}

#[tokio::test]
async fn confirmed_topup_requires_higher_voucher_and_denied_run_never_revives() {
    let fixture = funded(1000).await;
    fixture.voucher(1000);
    let first = fixture.admit(1);
    fixture.rpc.deposit.store(3000, Ordering::SeqCst);
    fixture.provider.inner.chain.refresh().await.unwrap();
    let denied = fixture
        .provider
        .inner
        .require_coverage_sync(&first.context, counts(1001))
        .unwrap();
    assert!(!denied.granted());
    assert_eq!(
        denied.failure().unwrap().reason(),
        Some("AUTHORIZATION_REQUIRED")
    );
    fixture.voucher(3000);
    assert!(
        !fixture
            .provider
            .inner
            .require_coverage_sync(&first.context, counts(1001))
            .unwrap()
            .granted()
    );
    assert!(first.stop.is_cancelled());
    fixture
        .provider
        .inner
        .report_sync(&first.context, counts(0), Some(RunStatus::Cancelled))
        .unwrap();
    let second = fixture.admit(2);
    assert!(
        fixture
            .provider
            .inner
            .require_coverage_sync(&second.context, counts(2000))
            .unwrap()
            .granted()
    );
    assert!(!second.stop.is_cancelled());
}
