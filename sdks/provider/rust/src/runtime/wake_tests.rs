//! A run waiting for admission reads only its own session's state and the
//! runtime-wide state, so only a change to one of those wakes it: its session's
//! changes, shutdown and a change in chain readiness. One runtime-wide wake-up
//! made every run waiting in any session re-check on every change in every
//! session, about N² checks under the one state lock for N waiting runs.
//! Time is paused and never moves while checks are counted, so no 50 ms poll
//! fires: each check counted followed a wake-up, and each run admitted was
//! admitted by wake-ups alone.
use super::retirement_tests::{
    Chain, descriptor_for, fixture as chain_fixture, notice, reconnect, sid_of, stream,
};
use super::*;
use crate::{interfaces::ProviderLifecycle, signing::RunAuth};
use proto::chat_server::Chat;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// Runs from this id up ask for 50,000 output tokens (35 units at the
/// fixture's 700 per million), more than any voucher here covers; the others
/// ask for 1,000 (one unit).
const LARGE: u8 = 0x80;
type Call = tokio::task::JoinHandle<Result<tonic::Response<RpcStream<proto::ChatChunk>>, Status>>;

/// Admitted runs stream without end and report nothing.
struct Endless;
#[tonic::async_trait]
impl CoverageProvider for Endless {
    async fn coverage(
        &self,
        _: &PreparedRequest,
        context: &RunContext,
    ) -> IntegrationResult<TokenCounts> {
        Ok(counts(if context.run_id()[0] >= LARGE {
            50_000
        } else {
            1000
        }))
    }
}
impl ExecutionProvider for Endless {
    fn execute(
        &self,
        _: PreparedRequest,
        _: RunContext,
        _: Arc<dyn ProviderRuns>,
        _: CancellationToken,
    ) -> ExecutionStream {
        Box::pin(Endless)
    }
}
impl futures_core::Stream for Endless {
    type Item = IntegrationResult<Vec<u8>>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

async fn runtime() -> (Fixture, Arc<Chain>) {
    let (mut fixture, chain) = chain_fixture().await;
    let runtime = Arc::get_mut(&mut fixture.provider.inner).unwrap();
    runtime.coverage = Arc::new(Endless);
    runtime.execution = Arc::new(Endless);
    (fixture, chain)
}
/// Session `n`, opened and owned at generation one by Control stream `1000 + n`.
async fn owned(fixture: &Fixture, chain: &Chain, n: u64) -> B256 {
    let sid = sid_of(n);
    let tx = keccak256(sid);
    chain.openings.lock().unwrap().insert(tx, descriptor_for(n));
    let id = 1000 + n;
    stream(fixture, id);
    let runtime = &fixture.provider.inner;
    runtime.opening(id, notice(sid, tx)).await.unwrap();
    runtime
        .attach(id, reconnect(sid, keccak256(id.to_be_bytes()), &[]))
        .unwrap();
    sid
}
fn context(fixture: &Fixture, sid: B256, id: u8) -> RunContext {
    RunContext::new(sid, hash(id), 1, "example/model", fixture.rpc.initial_hash).unwrap()
}
/// Run `id` of session `sid` through the real Complete service in its own
/// task. The task's response keeps the admitted run alive.
fn complete(fixture: &Fixture, sid: B256, id: u8) -> Call {
    let proof = RunAuth {
        sessionId: sid,
        controlGeneration: 1,
        runId: hash(id),
        model: "example/model".into(),
        rateCardHash: fixture.rpc.initial_hash,
    };
    let request = proto::CompleteRequest {
        model: Some(proof.model.clone()),
        stream: Some(true),
        session_id: Some(sid.to_vec()),
        run_id: Some(proof.runId.to_vec()),
        control_generation: Some(1),
        rate_card_hash: Some(proof.rateCardHash.to_vec()),
        auth_signature: Some(sign(&proof)),
        payload: Some(br#"{"model":"example/model","stream":true}"#.to_vec()),
    };
    let provider = fixture.provider.clone();
    tokio::spawn(async move { provider.completions(tonic::Request::new(request)).await })
}
fn voucher(sid: B256, amount: u64) -> proto::Voucher {
    proto::Voucher {
        session_id: Some(sid.to_vec()),
        cumulative: Some(amount),
        signature: Some(sign(&Voucher {
            sessionId: sid,
            cumulativeAmount: amount,
        })),
    }
}
/// Every task that can make progress runs; paused time does not move.
async fn settle() {
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
}
fn checks(fixture: &Fixture, sid: B256) -> u64 {
    fixture.provider.inner.lock().unwrap().sessions[&sid].checks
}
fn admitted(fixture: &Fixture, sid: B256) -> usize {
    fixture.provider.inner.lock().unwrap().sessions[&sid]
        .runs
        .values()
        .filter(|run| run.admitted())
        .count()
}
async fn refused(call: Call) -> Option<String> {
    reason(
        &call
            .await
            .unwrap()
            .err()
            .expect("the waiting run was refused"),
    )
}

#[tokio::test(start_paused = true)]
async fn admission_checks_grow_linearly_with_sessions_under_concurrent_load() {
    const RUNS: u64 = 10;
    let mut totals = Vec::new();
    for sessions in [5u64, 10, 20] {
        let (fixture, chain) = runtime().await;
        let mut sids = Vec::new();
        for n in 1..=sessions {
            sids.push(owned(&fixture, &chain, n).await);
        }
        let start = Instant::now();
        let calls = sids
            .iter()
            .flat_map(|sid| (1..=RUNS as u8).map(|id| complete(&fixture, *sid, id)))
            .collect::<Vec<_>>();
        settle().await;
        for sid in &sids {
            assert_eq!(
                checks(&fixture, *sid),
                RUNS,
                "each run checks once, then waits"
            );
        }
        for (n, sid) in (1..).zip(&sids) {
            fixture
                .provider
                .inner
                .receive_voucher(1000 + n, voucher(*sid, RUNS))
                .await
                .unwrap();
            settle().await;
        }
        assert_eq!(Instant::now(), start, "wake-ups alone admitted every run");
        for sid in &sids {
            assert_eq!(admitted(&fixture, *sid), RUNS as usize);
        }
        let total = sids.iter().map(|sid| checks(&fixture, *sid)).sum::<u64>();
        // Per session: one check each to start and one each when its voucher
        // arrives, then at most one more each time an earlier run is admitted.
        assert!(
            total <= sessions * (2 * RUNS + RUNS * (RUNS - 1) / 2),
            "{sessions} sessions of {RUNS} runs took {total} checks"
        );
        totals.push(total / sessions);
        drop(calls);
    }
    // Each session's runs take as many checks however many sessions wait.
    assert_eq!(
        totals, [totals[0]; 3],
        "checks per session for 5, 10 and 20"
    );
}

#[derive(Clone, Copy, Debug)]
enum Change {
    Voucher,
    Cancelled,
    FinalReport,
    StreamEnded,
    ControlLost,
    Stopped,
    Reattached,
}

#[tokio::test(start_paused = true)]
async fn a_change_wakes_only_its_own_sessions_waiting_runs() {
    use Change::*;
    let mut woke = Vec::new();
    for change in [
        Voucher,
        Cancelled,
        FinalReport,
        StreamEnded,
        ControlLost,
        Stopped,
        Reattached,
    ] {
        let (fixture, chain) = runtime().await;
        let runtime = &fixture.provider.inner;
        let (a, b) = (
            owned(&fixture, &chain, 1).await,
            owned(&fixture, &chain, 2).await,
        );
        let mut calls = Vec::new();
        if matches!(change, FinalReport | StreamEnded) {
            runtime.receive_voucher(1001, voucher(a, 1)).await.unwrap();
            calls.push(complete(&fixture, a, 1));
            settle().await;
            assert_eq!(admitted(&fixture, a), 1);
        }
        // Two runs of A and one of B wait for coverage nobody has signed.
        calls.extend([
            complete(&fixture, a, LARGE),
            complete(&fixture, a, LARGE + 1),
            complete(&fixture, b, LARGE),
        ]);
        settle().await;
        let start = Instant::now();
        let before = (checks(&fixture, a), checks(&fixture, b));
        match change {
            Voucher => runtime.receive_voucher(1001, voucher(a, 2)).await.unwrap(),
            Cancelled => runtime.cancel_run(&context(&fixture, a, LARGE + 1)),
            FinalReport => {
                runtime
                    .report_sync(
                        &context(&fixture, a, 1),
                        counts(500),
                        Some(RunStatus::Completed),
                    )
                    .unwrap();
            }
            StreamEnded => {
                runtime.finish_stream(&context(&fixture, a, 1)).unwrap();
            }
            ControlLost => runtime.detach_locked(&mut runtime.lock().unwrap(), 1001),
            Stopped => runtime.stop_session_locked(&mut runtime.lock().unwrap(), a),
            Reattached => {
                stream(&fixture, 2001);
                runtime
                    .attach(2001, reconnect(a, keccak256(2001u64.to_be_bytes()), &[]))
                    .unwrap();
            }
        }
        settle().await;
        assert_eq!(Instant::now(), start);
        assert!(
            checks(&fixture, a) > before.0,
            "{change:?}: A's runs re-check"
        );
        if checks(&fixture, b) != before.1 {
            woke.push(change);
        }
        drop(calls);
    }
    assert!(woke.is_empty(), "changes to A that woke B's run: {woke:?}");
}

#[tokio::test(start_paused = true)]
async fn maintenance_wakes_every_waiting_run_only_when_chain_readiness_changes() {
    let (fixture, chain) = runtime().await;
    let runtime = &fixture.provider.inner;
    let sids = [
        owned(&fixture, &chain, 1).await,
        owned(&fixture, &chain, 2).await,
    ];
    let calls = sids.map(|sid| complete(&fixture, sid, LARGE));
    runtime.maintain_once(); // The first pass sees chain readiness first.
    settle().await;
    let start = Instant::now();
    let before = sids.map(|sid| checks(&fixture, sid));
    runtime.maintain_once();
    settle().await;
    assert_eq!(
        sids.map(|sid| checks(&fixture, sid)),
        before,
        "an unchanged pass"
    );
    fixture.rpc.pause.store(1, Ordering::SeqCst);
    runtime.chain.refresh().await.unwrap();
    runtime.maintain_once();
    settle().await;
    for call in calls {
        assert!(
            call.is_finished(),
            "a paused contract ends every wait at once"
        );
        assert_eq!(refused(call).await.as_deref(), Some("CONTRACT_PAUSED"));
    }
    assert_eq!(Instant::now(), start);
}

#[tokio::test(start_paused = true)]
async fn shutdown_wakes_every_session_and_ends_at_the_last_final_report() {
    let (fixture, chain) = runtime().await;
    let runtime = &fixture.provider.inner;
    let (a, b) = (
        owned(&fixture, &chain, 1).await,
        owned(&fixture, &chain, 2).await,
    );
    runtime.receive_voucher(1001, voucher(a, 1)).await.unwrap();
    let active = complete(&fixture, a, 1);
    settle().await;
    let waiting = [complete(&fixture, a, LARGE), complete(&fixture, b, LARGE)];
    settle().await;
    let start = Instant::now();
    let provider = fixture.provider.clone();
    let shutdown =
        tokio::spawn(async move { provider.shutdown(start + Duration::from_secs(10)).await });
    settle().await;
    for call in waiting {
        assert!(call.is_finished(), "shutdown ends every wait at once");
        assert_eq!(refused(call).await.as_deref(), Some("CONTROL_NOT_READY"));
    }
    assert!(!shutdown.is_finished(), "an active run has not reported");
    runtime
        .report_sync(
            &context(&fixture, a, 1),
            counts(500),
            Some(RunStatus::Cancelled),
        )
        .unwrap();
    settle().await;
    assert!(shutdown.is_finished(), "its final report ends the drain");
    assert_eq!(Instant::now(), start);
    assert!(shutdown.await.unwrap().unwrap().unresolved_runs.is_empty());
    drop(active);
}
