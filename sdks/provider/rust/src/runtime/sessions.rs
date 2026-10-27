//! Ordered collection over retained immutable wallet intents.

use super::{
    hooks::{Part, Timing},
    *,
};
use crate::{
    chain::ChainSession,
    contract::OperationKind,
    interfaces::{ProviderSessions, RecoveryEvidence, RecoveryOperation, RecoveryRun},
    operations::{reconcile_wallet_operation, submit_wallet_operation},
    types::{OperationState, PauseLevel, Readiness, SessionPage, SessionView, validate_page_limit},
};
use alloy_primitives::hex;
use std::time::{Duration, SystemTime};

fn unknown(reference: &str) -> Result<OperationResult, ValidationError> {
    OperationResult::new(reference, OperationState::Unknown, None, None, None, vec![])
}

// Retain positive knowledge that execution has not entered the wallet. The
// shared marker lets deadline-bounded shutdown discard an unsubmitted intent
// without waiting for Tokio to destroy the suspended task. A recovered intent
// never receives this marker: its original submission may still have happened.
struct UnsubmittedCollection {
    runtime: Arc<Runtime>,
    reference: String,
}
impl Drop for UnsubmittedCollection {
    fn drop(&mut self) {
        if let Ok(mut state) = self.runtime.lock()
            && state.unsubmitted_collection.as_deref() == Some(self.reference.as_str())
        {
            self.runtime
                .discard_unsubmitted_collection_locked(&mut state);
        }
    }
}

pub(super) fn target(operation: &PendingOperation, sid: B256) -> Option<u64> {
    operation
        .prepared
        .session_ids()
        .into_iter()
        .zip(operation.prepared.amounts().iter().copied())
        .filter_map(|(id, amount)| (id == sid).then_some(amount))
        .max()
}

pub(super) fn covered(operation: &PendingOperation, sid: B256) -> bool {
    operation.result.observation().is_some()
        && target(operation, sid).is_some_and(|target| {
            operation.result.sessions().iter().any(|item| {
                item.session_id() == sid
                    && item.target() == Some(target)
                    && item.target_covered() == Some(true)
            })
        })
}

fn unresolved(operation: &PendingOperation, sid: B256) -> bool {
    matches!(
        operation.result.state(),
        OperationState::Pending | OperationState::Unknown
    ) && operation.result.observation().is_none()
        && !covered(operation, sid)
}

/// A close mined and reverted: the wallet's binding of its reference holds
/// that transaction, so a retry needs another reference.
fn reverted(operation: &PendingOperation) -> bool {
    operation.prepared.kind() == OperationKind::Close
        && operation.result.state() == OperationState::Failed
        && operation.result.tx_hash().is_some()
}

/// The reference a close takes after one mined and reverted: the same close
/// reference with its retry count after a dot, `close:<id>.1`, `.2`, and so on.
/// It derives from the reverted reference alone, so a runtime restored from
/// evidence older than a retry derives the same successor, and the wallet's
/// binding of it continues that retry rather than sending another.
fn successor(reference: &str) -> String {
    if let Some((base, count)) = reference.rsplit_once('.')
        && let Ok(retries) = count.parse::<u64>()
        && retries.to_string() == count
        && let Some(next) = retries.checked_add(1)
    {
        return format!("{base}.{next}");
    }
    format!("{reference}.1")
}

/// When no session `reference` covers needs it any longer: the latest of their
/// times, once every one has one.
fn due(covered: &BTreeMap<B256, Option<SystemTime>>) -> Option<SystemTime> {
    covered
        .values()
        .try_fold(SystemTime::UNIX_EPOCH, |latest, at| {
            at.map(|at| latest.max(at))
        })
}

/// Hand each reference that no covered session needs any longer to retirement,
/// due at `due`, as soon as that is known: the wallet keeps the time durably.
pub(super) fn hand_over(state: &mut State) {
    let State {
        references,
        retirable,
        ..
    } = state;
    references.retain(|reference, covered| match due(covered) {
        Some(at) => {
            retirable.push((reference.clone(), at));
            false
        }
        None => true,
    });
}

fn deadline(state: &ChainSession) -> Option<Duration> {
    let timestamp = match state.status() {
        SessionStatus::Open => state.last_activity().checked_add(86400),
        SessionStatus::Closing => state.close_requested_at().checked_add(3600),
        SessionStatus::Closed => return None,
    }?;
    Some(Duration::from_secs(timestamp))
}

fn retained_identity(
    known: &OperationResult,
    incoming: OperationResult,
) -> Result<(OperationResult, bool), ValidationError> {
    let conflicts = incoming.operation_ref() != known.operation_ref()
        || matches!((known.tx_hash(), incoming.tx_hash()), (Some(a), Some(b)) if a != b)
        || matches!((known.wallet_operation_id(), incoming.wallet_operation_id()), (Some(a), Some(b)) if a != b);
    let lost_hash = known.tx_hash().is_some() && incoming.tx_hash().is_none();
    if conflicts || lost_hash {
        return Ok((
            OperationResult::new(
                known.operation_ref(),
                OperationState::Unknown,
                known.wallet_operation_id().map(str::to_owned),
                known.tx_hash(),
                None,
                vec![],
            )?,
            true,
        ));
    }
    let wallet_id = incoming
        .wallet_operation_id()
        .or(known.wallet_operation_id())
        .map(str::to_owned);
    let result = OperationResult::new(
        known.operation_ref(),
        incoming.state(),
        wallet_id,
        incoming.tx_hash(),
        incoming.observation(),
        incoming.sessions().to_vec(),
    )?;
    let result = match incoming.cause() {
        Some(cause) => result.with_cause(cause)?,
        None => result,
    };
    Ok((result, false))
}

fn operation_evidence(operation: &PendingOperation) -> RecoveryOperation {
    RecoveryOperation {
        prepared: operation.prepared.as_ref().clone(),
        result: operation.result.as_ref().clone(),
        units: operation
            .units
            .iter()
            .map(|(sid, units)| (*sid, *units))
            .collect(),
    }
}

impl Session {
    /// Nothing remains to serve, collect or reconcile: the follower shows the
    /// session `Closed`, which is terminal on chain (every payment path reverts
    /// `SessionIsClosed`, MPPI.sol `_existingSession`); no run is active or
    /// preparing; every retained operation is resolved, and a confirmed close
    /// of ours has its result; and it is not inconsistent, which waits for its
    /// operator however long. A close another exit completed (`withdraw`,
    /// `releaseIdle`) is final with no close result.
    pub(super) fn is_final(&self, snapshot: &ChainSnapshot) -> bool {
        let awaiting_result = self.close_result.is_none()
            && self.operation.as_ref().is_some_and(|operation| {
                operation.prepared.kind() == OperationKind::Close
                    && operation.result.state() == OperationState::Confirmed
            });
        snapshot
            .sessions()
            .get(&self.sid)
            .is_some_and(|chain| chain.status() == SessionStatus::Closed)
            && self
                .runs
                .values()
                .all(|run| !run.active() && run.phase != RunPhase::Preparing)
            && [&self.operation, &self.baseline]
                .into_iter()
                .flatten()
                .all(|operation| !unresolved(operation, self.sid))
            && !awaiting_result
            && !self.inconsistent
    }
}

impl Runtime {
    /// Retire each session final for `closed_retention` that no live
    /// attachment owns. Its watch is released unless an opening or recovery
    /// holds it; the host's checkpoints keep its history. A retired session is
    /// never reinstalled: openings and final recovery evidence for a closed
    /// session are refused `SESSION_CLOSED`. A session that retires no longer
    /// needs any reference it covers, from the time it retired: retirement
    /// counts as no delay (`State::references`).
    pub(super) fn retire_locked(&self, state: &mut State) {
        let Some(snapshot) = self.chain.current() else {
            return;
        };
        let now = Instant::now();
        let State {
            sessions,
            attachments,
            ..
        } = state;
        let retired = sessions
            .iter_mut()
            .filter_map(|(sid, session)| {
                if !session.is_final(&snapshot) {
                    session.final_since = None;
                    return None;
                }
                let since = *session.final_since.get_or_insert(now);
                let owned = session
                    .owner
                    .and_then(|id| attachments.get(&id))
                    .is_some_and(|owner| owner.live);
                (!owned && now.duration_since(since) >= self.limits.closed_retention)
                    .then_some(*sid)
            })
            .collect::<BTreeSet<_>>();
        for &sid in &retired {
            let session = state.sessions.remove(&sid).expect("final session");
            state.retained_request_bytes -= session
                .runs
                .values()
                .map(|run| run.retained_bytes)
                .sum::<usize>();
            if !state.pending_watch.contains_key(&sid) {
                let _ = self.chain.unwatch_session(sid);
            }
            self.observers.emit(hooks::Event::SessionRetired(sid));
        }
        if !retired.is_empty() {
            let now = SystemTime::now();
            for covered in state.references.values_mut() {
                for (sid, at) in covered.iter_mut() {
                    if retired.contains(sid) {
                        *at = Some(now);
                    }
                }
            }
            hand_over(state);
        }
    }

    pub(super) fn view_locked(
        &self,
        state: &State,
        session: &Session,
    ) -> IntegrationResult<SessionView> {
        let snapshot = self
            .chain
            .current()
            .ok_or_else(|| not_ready("CHAIN_STATE_STALE", Some(session.sid), None))?;
        let observed = snapshot
            .sessions()
            .get(&session.sid)
            .ok_or_else(|| not_ready("CHAIN_STATE_STALE", Some(session.sid), None))?;
        let readiness = match self.gate_locked(state, session, None) {
            Ok(_) => Readiness::new(true, vec![])?,
            Err(error) => {
                let failure = wire::parse_payment_error(&error)?.unwrap_or(PaymentFailure::new(
                    PaymentCode::SessionNotReady,
                    Some("STATE_UNAVAILABLE".into()),
                    Some(session.sid),
                    None,
                    None,
                    None,
                    None,
                )?);
                Readiness::new(false, vec![failure])?
            }
        };
        Ok(SessionView::new(
            session.sid,
            session.opening.descriptor().clone(),
            observed.deposit(),
            observed.settled_cumulative(),
            session.voucher.as_ref().and_then(|v| v.cumulative),
            if session.history_available {
                Some(session.usage()?)
            } else {
                None
            },
            observed.status(),
            *snapshot.block().observation(),
            self.chain.snapshot().is_ok(),
            readiness,
        )?)
    }
}

#[tonic::async_trait]
impl ProviderSessions for Mppi {
    async fn checkpoint(&self, sid: B256) -> IntegrationResult<RecoveryEvidence> {
        let runtime = &self.inner;
        let _operations = runtime.operations.lock().await;
        let state = runtime.lock()?;
        let session = state
            .sessions
            .get(&sid)
            .ok_or_else(|| super::recovery::missing(sid, "the session is not held"))?;
        if !session.history_available {
            return Err(
                super::recovery::missing(sid, "the session's history is unavailable").into(),
            );
        }
        if session
            .runs
            .values()
            .any(|run| run.phase == RunPhase::Preparing)
        {
            return Err(super::recovery::missing(sid, "a run is still preparing").into());
        }
        let snapshot = runtime.session_state_locked(&state, session, &[0; 32], true)?;
        let current = runtime.chain.snapshot()?;
        // The follower can refresh on another executor thread while this short
        // runtime lock is held. Never export two different block observations.
        if snapshot.chain_block_number != Some(current.block().observation().block_number())
            || snapshot.chain_block_hash.as_deref()
                != Some(current.block().observation().block_hash().as_slice())
        {
            return Err(not_ready("CHAIN_STATE_STALE", Some(sid), None).into());
        }
        let mut publications = session.publications.clone();
        if let Some(identity) = current
            .identities()
            .get(&runtime.config.identity().provider_id())
            && let Some(catalog) = identity.catalog()
        {
            publications.insert(catalog.rate_card_hash(), *identity.observation());
        }
        Ok(RecoveryEvidence {
            snapshot,
            reservations: session
                .active()
                .map(|run| (run.context.clone(), run.accounting))
                .collect(),
            observation: *current.block().observation(),
            opening_tx: Some(session.opening.tx_hash()),
            runs: session
                .runs
                .values()
                .map(|run| RecoveryRun {
                    context: run.context.clone(),
                    signature: run.signature,
                    accounting: run.accounting,
                    covered_counts: run.covered_counts,
                    meter_seq: run.meter_seq,
                    admitted: run.admitted(),
                    not_admitted: run.phase == RunPhase::NotAdmitted,
                })
                .collect(),
            publications: publications
                .into_iter()
                .map(|(hash, observation)| (hash.to_vec(), observation))
                .collect(),
            operation: session.operation.as_ref().map(operation_evidence),
            baseline: session.baseline.as_ref().map(operation_evidence),
            confirmed_units: session.confirmed_units,
            close_reference: session.close_reference.clone(),
            inconsistent: session.inconsistent,
        })
    }

    async fn recover(&self, evidence: RecoveryEvidence) -> IntegrationResult<SessionView> {
        self.recover_evidence(evidence, false).await
    }

    async fn recover_current(&self, evidence: RecoveryEvidence) -> IntegrationResult<SessionView> {
        self.recover_evidence(evidence, true).await
    }

    async fn settle(&self, sid: B256) -> IntegrationResult<OperationResult> {
        self.collect_owned(sid, false).await
    }

    async fn settle_batch(&self, ids: &[B256]) -> IntegrationResult<OperationResult> {
        // Bound allocation before cloning caller input. The encoded call is
        // checked separately; this is a resource ceiling, not a gas guarantee.
        if ids.len() > wire::MAX_MESSAGE_BYTES / 512 {
            return Err(Status::resource_exhausted("batch exceeds resource limit").into());
        }
        self.collect_many_owned(ids.to_vec(), false, true).await
    }

    async fn close(&self, sid: B256) -> IntegrationResult<OperationResult> {
        self.collect_owned(sid, true).await
    }

    async fn get(&self, sid: B256) -> IntegrationResult<SessionView> {
        let state = self.inner.lock()?;
        let session = state
            .sessions
            .get(&sid)
            .ok_or_else(|| not_ready("STATE_UNAVAILABLE", Some(sid), None))?;
        self.inner.view_locked(&state, session)
    }

    async fn list(&self, cursor: Option<&str>, limit: usize) -> IntegrationResult<SessionPage> {
        let limit = validate_page_limit(limit)?;
        let after = cursor
            .map(|value| {
                if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(Status::invalid_argument("invalid session cursor"));
                }
                hex::decode(value)
                    .map(|bytes| B256::from_slice(&bytes))
                    .map_err(|_| Status::invalid_argument("invalid session cursor"))
            })
            .transpose()?;
        let state = self.inner.lock()?;
        let mut candidates = state
            .sessions
            .iter()
            .filter(|(id, _)| after.is_none_or(|after| **id > after));
        let selected = candidates.by_ref().take(limit).collect::<Vec<_>>();
        let next_cursor = if candidates.next().is_some() {
            selected.last().map(|(id, _)| hex::encode(id.as_slice()))
        } else {
            None
        };
        let sessions = selected
            .into_iter()
            .map(|(_, session)| self.inner.view_locked(&state, session))
            .collect::<IntegrationResult<Vec<_>>>()?;
        Ok(SessionPage {
            sessions,
            next_cursor,
        })
    }
}

impl Mppi {
    /// Select and reserve maintenance work; the caller clears each reservation
    /// with `collection_finished`, including after an error or task cancellation.
    pub(super) fn collections_due(&self) -> Result<Vec<(B256, bool)>, Status> {
        let runtime = &self.inner;
        let coherent = runtime.chain.snapshot().ok().and_then(|snapshot| {
            runtime
                .chain
                .estimated_timestamp_for_snapshot(&snapshot)
                .ok()
                .map(|time| (snapshot, time))
        });
        let accepted = runtime.chain.current();
        let interval = Duration::from_millis(runtime.config.settlement().interval_ms());
        let margin = Duration::from_millis(runtime.config.payment().collection_margin_ms())
            .checked_mul(2)
            .ok_or_else(|| Status::internal("collection margin overflow"))?;
        let mut state = runtime.lock()?;
        if !state.started || state.stopping {
            return Ok(vec![]);
        }
        let mut due = Vec::new();
        for (sid, session) in &state.sessions {
            if state.collecting.contains(sid)
                || session.close_result.is_some()
                || accepted
                    .as_ref()
                    .is_some_and(|snapshot| session.is_final(snapshot))
            {
                continue;
            }
            let timed = session.last_collection.elapsed() >= interval;
            if session.close_pending {
                if session.active().next().is_some() {
                    continue;
                }
                if let Some(operation) = &session.operation
                    && operation.prepared.kind() == OperationKind::Close
                    && operation.result.state() == OperationState::Failed
                {
                    // A failed transaction does not invalidate close intent: a
                    // close that never began, or was mined and reverted, is
                    // prepared again at most once per interval.
                    if timed {
                        due.push((*sid, true));
                    }
                    continue;
                }
                // Cooperative-close reconciliation continues independently of
                // the periodic capture interval. collect_many retains this
                // exact operation and never resubmits an uncertain close.
                due.push((*sid, true));
                continue;
            }
            if session
                .operation
                .as_ref()
                .is_some_and(|operation| unresolved(operation, *sid))
            {
                if timed {
                    due.push((*sid, false));
                }
                continue;
            }
            if session.inconsistent || !session.history_available {
                continue;
            }
            let Some((snapshot, chain_time)) = coherent.as_ref() else {
                continue;
            };
            let Some(observed) = snapshot.sessions().get(sid) else {
                continue;
            };
            if observed.status() != SessionStatus::Open {
                continue;
            }
            let Ok(usage) = session.usage() else {
                continue;
            };
            let amount = usage.saturating_sub(observed.settled_cumulative());
            if amount == 0 {
                continue;
            }
            let Some(units) = session
                .units()
                .ok()
                .and_then(|units| units.checked_sub(session.confirmed_units))
            else {
                continue;
            };
            let amount_due = runtime
                .config
                .settlement()
                .amount()
                .is_some_and(|target| amount >= target);
            let units_due = runtime
                .config
                .settlement()
                .units()
                .is_some_and(|target| units >= target);
            let deadline_due = deadline(observed).is_some_and(|end| {
                chain_time
                    .checked_add(margin)
                    .is_none_or(|time| time >= end)
            });
            if timed || amount_due || units_due || deadline_due {
                due.push((*sid, false));
            }
        }
        state.collecting.extend(due.iter().map(|(sid, _)| *sid));
        Ok(due)
    }

    pub(super) fn collection_finished(&self, sid: B256) {
        if let Ok(mut state) = self.inner.lock() {
            state.collecting.remove(&sid);
        }
    }

    async fn collect_owned(&self, sid: B256, close: bool) -> IntegrationResult<OperationResult> {
        self.collect_many_owned(vec![sid], close, false).await
    }

    async fn collect_many_owned(
        &self,
        ids: Vec<B256>,
        close: bool,
        batch: bool,
    ) -> IntegrationResult<OperationResult> {
        let sid = ids.first().copied();
        let sdk = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.inner.spawn(async move {
            let _ = sender.send(sdk.collect_many(&ids, close, batch).await);
        })?;
        receiver
            .await
            .map_err(|_| not_ready("CONTROL_NOT_READY", sid, None))?
    }

    pub(super) async fn collect(
        &self,
        sid: B256,
        close: bool,
    ) -> IntegrationResult<OperationResult> {
        self.collect_many(&[sid], close, false).await
    }

    pub(super) async fn collect_many(
        &self,
        ids: &[B256],
        close: bool,
        batch: bool,
    ) -> IntegrationResult<OperationResult> {
        let operation = if close {
            "close"
        } else if batch {
            "settleBatch"
        } else {
            "settle"
        };
        let timing = Timing::new(&self.inner.observers, operation, Instant::now());
        let result = self.collect_timed(ids, close, batch, &timing).await;
        timing.finish(result.as_ref().ok().map(|result| result.state().as_str()));
        result
    }

    async fn collect_timed(
        &self,
        ids: &[B256],
        close: bool,
        batch: bool,
        timing: &Timing<'_>,
    ) -> IntegrationResult<OperationResult> {
        let runtime = &self.inner;
        let _operations = runtime.operations.lock().await;
        // The call a reverted close's successor last simulated, below.
        let mut simulated = None;
        let (mut pending, submit, close_reference) = 'collect: loop {
            let next_close = 'locked: {
                let mut state = runtime.lock()?;
                if !state.started || state.stopping {
                    return Err(not_ready("CONTROL_NOT_READY", ids.first().copied(), None).into());
                }
                for sid in ids {
                    if !state.sessions.contains_key(sid) {
                        return Err(not_ready("STATE_UNAVAILABLE", Some(*sid), None).into());
                    }
                }
                if close {
                    let sid = ids[0];
                    let session = state.sessions.get_mut(&sid).expect("known session");
                    if session.close_result.is_some()
                        && let Some(operation) = &session.operation
                    {
                        return Ok(operation.result.as_ref().clone());
                    }
                    if session.close_reference.is_none() {
                        session.close_reference =
                            Some(format!("close:{}", hex::encode(random_id()?.as_slice())));
                    }
                    // Close intent is accepted once: its runs stop and its owner is
                    // told once (Drain). A later call, the agent's or maintenance's,
                    // only continues the close.
                    let accepted = !session.close_pending;
                    session.close_pending = true;
                    if accepted {
                        runtime.stop_session_locked(&mut state, sid);
                    }
                } else {
                    for sid in ids {
                        if state.sessions[sid].close_pending {
                            return Err(not_ready("CLOSE_PENDING", Some(*sid), None).into());
                        }
                    }
                }
                let close_reference = ids
                    .first()
                    .and_then(|sid| state.sessions[sid].close_reference.clone());
                // One intersecting unresolved submission must be reconciled before
                // any new calldata is prepared. This includes a larger old batch
                // when the caller now requests only one of its sessions.
                let previous = ids
                    .iter()
                    .find_map(|sid| {
                        state.sessions[sid]
                            .operation
                            .as_ref()
                            .filter(|op| unresolved(op, *sid))
                    })
                    .or_else(|| {
                        ids.first()
                            .and_then(|sid| state.sessions[sid].operation.as_ref())
                    })
                    .or_else(|| {
                        ids.is_empty()
                            .then_some(state.empty_batch.as_ref())
                            .flatten()
                    })
                    .cloned();
                // A close mined and reverted changed no state; it is prepared again
                // under its successor reference, at most once per settlement
                // interval, since each attempt is a transaction.
                let retry = previous.as_ref().filter(|op| close && reverted(op));
                if let Some(operation) = retry
                    && state.sessions[&ids[0]].last_collection.elapsed()
                        < Duration::from_millis(runtime.config.settlement().interval_ms())
                {
                    return Ok(operation.result.as_ref().clone());
                }
                let close_reference = retry
                    .map(|operation| successor(operation.result.operation_ref()))
                    .or(close_reference);
                if let Some(operation) = previous.as_ref()
                    && operation.prepared.kind() == OperationKind::Close
                    && operation.result.state() != OperationState::Failed
                {
                    // A close reference identifies its submitted call: continue it.
                    break 'collect (operation.clone(), false, close_reference);
                } else if let Some(operation) = previous.as_ref().filter(|op| {
                    matches!(
                        op.result.state(),
                        OperationState::Pending | OperationState::Unknown
                    ) && op.result.observation().is_none()
                }) {
                    break 'collect (operation.clone(), false, close_reference);
                } else if ids.is_empty()
                    && let Some(operation) = previous.as_ref().filter(|op| {
                        op.result.observation().is_some()
                            && op.result.state() != OperationState::Failed
                    })
                {
                    return Ok(operation.result.as_ref().clone());
                } else {
                    let snapshot = runtime.chain.snapshot()?;
                    if snapshot.pause_level() != PauseLevel::None {
                        return Err(wire::payment_status(&PaymentFailure::new(
                            PaymentCode::SessionNotReady,
                            Some("CONTRACT_PAUSED".into()),
                            ids.first().copied(),
                            None,
                            None,
                            None,
                            Some(snapshot.pause_level()),
                        )?)
                        .into());
                    }
                    let mut items = Vec::with_capacity(ids.len());
                    let mut units = BTreeMap::new();
                    for sid in ids {
                        let session = &state.sessions[sid];
                        if session.inconsistent || !session.history_available {
                            return Err(not_ready("INCONSISTENT_STATE", Some(*sid), None).into());
                        }
                        if close && session.active().next().is_some() {
                            return Ok(unknown(
                                close_reference.as_deref().expect("close reference"),
                            )?);
                        }
                        let observed = snapshot
                            .sessions()
                            .get(sid)
                            .ok_or_else(|| not_ready("STATE_UNAVAILABLE", Some(*sid), None))?;
                        if !observed.exists() {
                            return Err(not_ready("STATE_UNAVAILABLE", Some(*sid), None).into());
                        }
                        if observed.status() == SessionStatus::Closed {
                            return Err(not_ready("SESSION_CLOSED", Some(*sid), None).into());
                        }
                        #[cfg(test)]
                        tests::before_timestamp(*sid);
                        let expired = runtime
                            .chain
                            .estimated_timestamp_for_snapshot(&snapshot)
                            .map_err(|_| not_ready("CHAIN_STATE_STALE", Some(*sid), None))?
                            >= deadline(observed)
                                .ok_or_else(|| not_ready("CLAIM_DEADLINE", Some(*sid), None))?;
                        if !close && (expired || observed.status() != SessionStatus::Open) {
                            return Err(not_ready("CLAIM_DEADLINE", Some(*sid), None).into());
                        }
                        let capture = if close && expired {
                            observed.settled_cumulative()
                        } else {
                            session.usage()?.max(observed.settled_cumulative())
                        };
                        let (cumulative, signature) = if capture > observed.settled_cumulative()
                            || !close
                        {
                            let voucher = session.voucher.as_ref().ok_or_else(|| {
                                not_ready("AUTHORIZATION_REQUIRED", Some(*sid), None)
                            })?;
                            let cumulative = voucher.cumulative.ok_or_else(|| {
                                not_ready("AUTHORIZATION_REQUIRED", Some(*sid), None)
                            })?;
                            if capture > cumulative || cumulative > observed.deposit() {
                                return Err(
                                    not_ready("AUTHORIZATION_REQUIRED", Some(*sid), None).into()
                                );
                            }
                            let signature = voucher.signature.as_deref().ok_or_else(|| {
                                not_ready("AUTHORIZATION_REQUIRED", Some(*sid), None)
                            })?;
                            (cumulative, signature)
                        } else {
                            (capture, &[][..])
                        };
                        items.push((session.opening.descriptor(), cumulative, capture, signature));
                        units.insert(*sid, session.units()?);
                    }
                    if !close
                        && let Some(operation) = previous.as_ref()
                        && operation.prepared.session_ids() == ids
                        && operation
                            .prepared
                            .amounts()
                            .iter()
                            .copied()
                            .eq(items.iter().map(|i| i.2))
                        && ids.iter().all(|sid| covered(operation, *sid))
                    {
                        return Ok(operation.result.as_ref().clone());
                    }
                    let prepared = if close {
                        let (descriptor, cumulative, capture, signature) = items[0];
                        runtime
                            .contract
                            .close(descriptor, cumulative, capture, signature)?
                    } else if batch {
                        runtime.contract.settle_batch(&items)?
                    } else {
                        let (descriptor, cumulative, capture, signature) = items[0];
                        runtime
                            .contract
                            .settle(descriptor, cumulative, capture, signature)?
                    };
                    // A call the wallet would refuse records no intent.
                    if prepared.call().data().len()
                        > wire::MAX_MESSAGE_BYTES.min(runtime.max_call_bytes)
                    {
                        return Err(Status::resource_exhausted(
                            "batch calldata exceeds resource limit",
                        )
                        .into());
                    }
                    // A reverted close's successor is recorded and sent only as
                    // the call last simulated, below.
                    if retry.is_some() && simulated.as_ref() != Some(prepared.call()) {
                        break 'locked prepared;
                    }
                    let reference = if close {
                        close_reference.clone().expect("close reference")
                    } else {
                        format!("settle:{}", hex::encode(random_id()?.as_slice()))
                    };
                    let pending = PendingOperation {
                        prepared: Arc::new(prepared),
                        result: Arc::new(unknown(&reference)?),
                        units: Arc::new(units),
                    };
                    // No await or external call may precede retention of this exact
                    // ordered intent, including duplicated session ids and targets.
                    if ids.is_empty() {
                        state.empty_batch = Some(pending.clone());
                    } else {
                        state
                            .references
                            .entry(reference.clone())
                            .or_insert_with(|| ids.iter().map(|sid| (*sid, None)).collect());
                        for sid in ids {
                            let session = state.sessions.get_mut(sid).expect("known session");
                            session.operation = Some(pending.clone());
                            session.last_collection = Instant::now();
                            if close {
                                // The close intent names its current reference.
                                session.close_reference = Some(reference.clone());
                            }
                        }
                    }
                    state.unsubmitted_collection = Some(reference);
                    break 'collect (pending, true, close_reference);
                }
            };
            // Simulated outside the lock, as a batch is. One that fails records
            // no intent and sends nothing; the attempt counts toward the
            // interval. One that passes is prepared again under the lock, with
            // every check, and recorded only if it is still the simulated call.
            if let Err(error) = timing
                .part(
                    Part::Chain,
                    runtime
                        .reader
                        .simulate(&next_close, runtime.wallet.address()),
                )
                .await
            {
                if let Some(session) = runtime.lock()?.sessions.get_mut(&ids[0]) {
                    session.last_collection = Instant::now();
                }
                return Err(error.into());
            }
            simulated = Some(next_close.call().clone());
        };

        let _unsubmitted = submit.then(|| UnsubmittedCollection {
            runtime: runtime.clone(),
            reference: pending.result.operation_ref().to_owned(),
        });
        if submit && pending.prepared.kind() == OperationKind::SettleBatch {
            timing
                .part(
                    Part::Chain,
                    runtime
                        .reader
                        .simulate(&pending.prepared, runtime.wallet.address()),
                )
                .await?;
        }
        #[cfg(test)]
        if let Some(sid) = ids.first() {
            tests::before_submit(*sid).await;
        }
        // Whether the wallet is asked here: a confirmed result is kept as is.
        let asked = submit || pending.result.state() != OperationState::Confirmed;
        let incoming = if submit {
            {
                let mut state = runtime.lock()?;
                if state.stopping
                    || runtime.halt.is_cancelled()
                    || state.unsubmitted_collection.as_deref()
                        != Some(pending.result.operation_ref())
                {
                    return Err(not_ready("CONTROL_NOT_READY", ids.first().copied(), None).into());
                }
                // From this point the wallet call may begin. Shutdown must keep
                // the intent Unknown even if cancellation interrupts its result.
                state.unsubmitted_collection = None;
            }
            timing
                .part(
                    Part::Wallet,
                    submit_wallet_operation(
                        runtime.wallet.as_ref(),
                        &pending.prepared,
                        pending.result.operation_ref(),
                        &runtime.reader,
                    ),
                )
                .await?
        } else if pending.result.state() == OperationState::Confirmed {
            pending.result.as_ref().clone()
        } else {
            timing
                .part(
                    Part::Wallet,
                    reconcile_wallet_operation(
                        runtime.wallet.as_ref(),
                        &pending.prepared,
                        pending.result.operation_ref(),
                        &runtime.reader,
                    ),
                )
                .await?
        };
        let (mut result, _) = retained_identity(&pending.result, incoming.clone())?;
        if asked {
            // A record kept against an answer that disagrees with it is
            // handed to retire only through the wallet check
            // (`State::unconfirmed`); an answer that matches clears it.
            let mut state = runtime.lock()?;
            let reference = result.operation_ref();
            if super::recovery::agrees(&result, &incoming) {
                state.unconfirmed.remove(reference);
            } else if state.references.contains_key(reference) {
                state
                    .unconfirmed
                    .insert(reference.to_owned(), result.clone());
            }
        }
        // No suspension between returned wallet handles and their retention on
        // every session. Cancellation during chain I/O preserves both handles.
        pending.result = Arc::new(result.clone());
        runtime.retain_collection(&pending)?;
        if result.state() != OperationState::Confirmed && result.tx_hash().is_some() {
            result = timing
                .part(
                    Part::Chain,
                    runtime.reader.reconcile(
                        &pending.prepared,
                        result.operation_ref(),
                        result.tx_hash(),
                        result.wallet_operation_id(),
                    ),
                )
                .await?;
        }
        pending.result = Arc::new(result.clone());
        runtime.retain_collection(&pending)?;
        if result.state() == OperationState::Confirmed
            && matches!(
                pending.prepared.kind(),
                OperationKind::Settle | OperationKind::SettleBatch
            )
        {
            runtime.observers.emit(hooks::Event::Settle(result.clone()));
        }
        let closing = if pending.prepared.kind() == OperationKind::Close
            && result.state() == OperationState::Confirmed
        {
            if let Some(tx) = result.tx_hash() {
                timing
                    .part(
                        Part::Chain,
                        runtime
                            .reader
                            .closing(tx, pending.prepared.session_ids()[0]),
                    )
                    .await
                    .ok()
            } else {
                None
            }
        } else {
            None
        };
        // A refresh outage neither erases evidence nor invents another operation.
        let _ = timing.part(Part::Chain, runtime.chain.refresh()).await;
        if let Some(closing) = closing {
            let sid = pending.prepared.session_ids()[0];
            let mut state = runtime.lock()?;
            let message = proto::CloseResult {
                session_id: Some(sid.to_vec()),
                tx_hash: Some(closing.tx_hash().to_vec()),
                capture_amount: Some(closing.capture_amount()),
                refunded: Some(closing.refunded()),
            };
            let session = state
                .sessions
                .get_mut(&sid)
                .expect("retained operation session");
            session.close_result = Some(message.clone());
            runtime.observers.emit(hooks::Event::Close(message.clone()));
            if let Some(owner) = session.owner {
                let _ = runtime.send_locked(
                    &mut state,
                    owner,
                    control(proto::control_message::Kind::CloseResult(message)),
                );
            }
        }
        if close && pending.prepared.kind() != OperationKind::Close {
            return Ok(unknown(
                close_reference.as_deref().expect("close reference"),
            )?);
        }
        Ok(result)
    }
}

impl Runtime {
    pub(super) fn discard_unsubmitted_collection_locked(&self, state: &mut State) {
        let Some(reference) = state.unsubmitted_collection.take() else {
            return;
        };
        for session in state.sessions.values_mut() {
            if session
                .operation
                .as_ref()
                .is_some_and(|operation| operation.result.operation_ref() == reference)
            {
                session.operation = None;
            }
        }
        if state
            .empty_batch
            .as_ref()
            .is_some_and(|operation| operation.result.operation_ref() == reference)
        {
            state.empty_batch = None;
        }
    }

    fn retain_collection(&self, pending: &PendingOperation) -> Result<(), Status> {
        let mut state = self.lock()?;
        let ids = pending.prepared.session_ids();
        if ids.is_empty() {
            state.empty_batch = Some(pending.clone());
        }
        for sid in ids.into_iter().collect::<BTreeSet<_>>() {
            let Some(session) = state.sessions.get_mut(&sid) else {
                // Supplied history can restore a subset of batch members.
                continue;
            };
            if session
                .operation
                .as_ref()
                .is_some_and(|known| known.result.operation_ref() == pending.result.operation_ref())
            {
                session.operation = Some(pending.clone());
                session.last_collection = Instant::now();
            }
            let next = target(pending, sid).unwrap_or(0);
            let old = session
                .baseline
                .as_ref()
                .and_then(|op| target(op, sid))
                .unwrap_or(0);
            if next > old && covered(pending, sid) {
                session.confirmed_units = *pending
                    .units
                    .get(&sid)
                    .ok_or_else(|| not_ready("INCONSISTENT_STATE", Some(sid), None))?;
                session.baseline = Some(pending.clone());
            }
        }
        if pending.result.state() == OperationState::Confirmed {
            self.supersede_locked(&mut state, pending);
        }
        Ok(())
    }

    /// A confirmed collection supersedes, for each session it covers, every
    /// other reference that session's checkpoint no longer names (neither its
    /// operation nor its baseline): a checkpoint taken before then may still
    /// present one, for `closed_retention` (the age rule, docs/rust-runtime.md).
    fn supersede_locked(&self, state: &mut State, pending: &PendingOperation) {
        let at = SystemTime::now() + self.limits.closed_retention;
        let confirmed = pending.result.operation_ref();
        let ids = pending
            .prepared
            .session_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let State {
            sessions,
            references,
            ..
        } = &mut *state;
        for (reference, covered) in references.iter_mut() {
            if reference == confirmed {
                continue;
            }
            for (sid, since) in covered.iter_mut() {
                let named = |operation: &Option<PendingOperation>| {
                    operation
                        .as_ref()
                        .is_some_and(|operation| operation.result.operation_ref() == reference)
                };
                if since.is_none()
                    && ids.contains(sid)
                    && sessions
                        .get(sid)
                        .is_some_and(|held| !named(&held.operation) && !named(&held.baseline))
                {
                    *since = Some(at);
                }
            }
        }
        hand_over(state);
    }
}

#[cfg(test)]
mod batch_tests;

#[cfg(test)]
mod tests {
    use super::super::tests::{Fixture, TIME, TestRpc};
    use super::*;
    use crate::{
        capabilities::{WalletCall, WalletError},
        chain::{ChainError, Rpc},
        interfaces::ProviderLifecycle,
        types::ChainObservation,
    };
    use serde_json::Value;
    use std::{
        cell::RefCell,
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
    };
    use tokio::sync::oneshot;

    // Synchronization only: one shot, scoped to this test executor thread and
    // session. Production builds contain neither hooks nor alternate decisions.
    type TimestampHook = (B256, Box<dyn FnOnce()>);
    type SubmitHook = (B256, oneshot::Sender<()>, oneshot::Receiver<()>);
    thread_local! {
        static TIMESTAMP_HOOK: RefCell<Option<TimestampHook>> = const { RefCell::new(None) };
        static SUBMIT_HOOK: RefCell<Option<SubmitHook>> = const { RefCell::new(None) };
    }
    struct HookScope;
    impl Drop for HookScope {
        fn drop(&mut self) {
            TIMESTAMP_HOOK.with(|hook| hook.borrow_mut().take());
            SUBMIT_HOOK.with(|hook| hook.borrow_mut().take());
        }
    }
    pub(super) fn before_timestamp(sid: B256) {
        let callback = TIMESTAMP_HOOK.with(|hook| {
            let mut hook = hook.borrow_mut();
            if hook.as_ref().is_some_and(|(id, _)| *id == sid) {
                hook.take()
            } else {
                None
            }
        });
        if let Some((_, callback)) = callback {
            callback();
        }
    }
    pub(super) async fn before_submit(sid: B256) {
        let gate = SUBMIT_HOOK.with(|hook| {
            let mut hook = hook.borrow_mut();
            if hook.as_ref().is_some_and(|(id, _, _)| *id == sid) {
                hook.take()
            } else {
                None
            }
        });
        if let Some((_, entered, resume)) = gate {
            let _ = entered.send(());
            let _ = resume.await;
        }
    }

    #[derive(Default)]
    struct Wallet {
        executions: AtomicU64,
        lookups: AtomicU64,
        return_hash: AtomicBool,
        block_execute: AtomicBool,
        not_submitted: AtomicBool,
        lose_result: AtomicBool,
        entered: Notify,
    }
    #[tonic::async_trait]
    impl TransactionWallet for Wallet {
        fn address(&self) -> crate::Address {
            crate::Address::repeat_byte(0x11)
        }
        async fn execute(
            &self,
            _: &WalletCall,
            reference: &str,
        ) -> Result<OperationResult, WalletError> {
            self.executions.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            if self.block_execute.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self.not_submitted.load(Ordering::SeqCst) {
                // The adapter's own record proves nothing reached the wallet.
                return Ok(OperationResult::new(
                    reference,
                    OperationState::Failed,
                    None,
                    None,
                    None,
                    vec![],
                )
                .unwrap());
            }
            if self.lose_result.load(Ordering::SeqCst) {
                return Ok(unknown(reference)
                    .unwrap()
                    .with_cause("Unavailable (TIMEOUT)")
                    .unwrap());
            }
            Ok(OperationResult::new(
                reference,
                OperationState::Pending,
                Some("wallet-original".into()),
                self.return_hash
                    .load(Ordering::SeqCst)
                    .then_some(B256::repeat_byte(0xdd)),
                None,
                vec![],
            )
            .unwrap())
        }
        async fn reconcile(&self, reference: &str) -> Result<OperationResult, WalletError> {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            Ok(unknown(reference).unwrap())
        }
    }

    #[derive(Default)]
    struct ReceiptProbe {
        mode: AtomicU64,
        reads: AtomicU64,
        entered: Notify,
    }
    struct ReceiptRpc {
        base: Arc<TestRpc>,
        probe: Arc<ReceiptProbe>,
    }
    #[tonic::async_trait]
    impl Rpc for ReceiptRpc {
        async fn call(&self, method: &str, params: Value) -> Result<Value, ChainError> {
            if method == "eth_getTransactionReceipt"
                && params[0].as_str()
                    == Some(&format!("0x{}", hex::encode(B256::repeat_byte(0xdd))))
            {
                self.probe.reads.fetch_add(1, Ordering::SeqCst);
                self.probe.entered.notify_one();
                match self.probe.mode.load(Ordering::SeqCst) {
                    1 => return Err(ChainError::Unavailable),
                    2 => std::future::pending::<()>().await,
                    _ => {}
                }
                return Ok(Value::Null);
            }
            self.base.call(method, params).await
        }
    }

    async fn paid_fixture(wallet: Arc<Wallet>, probe: Arc<ReceiptProbe>) -> Fixture {
        let fixture = Fixture::with_wallet(1000, wallet, move |base| {
            Arc::new(ReceiptRpc { base, probe })
        })
        .await;
        fixture.voucher(100);
        // The fixture's standard reservation is 1000 tokens, so use one unit
        // per thousand tokens while keeping a positive whole settled amount.
        let run = fixture.admit(1);
        fixture
            .provider
            .inner
            .report_sync(
                &run.context,
                TokenCounts::new(0, 0, 1000).unwrap(),
                Some(RunStatus::Completed),
            )
            .unwrap();
        fixture
    }

    #[tokio::test]
    async fn collection_rejects_snapshot_replaced_before_timestamp_without_wallet_submission() {
        let _scope = HookScope;
        let wallet = Arc::new(Wallet::default());
        let fixture = Fixture::with_wallet(1000, wallet.clone(), |rpc| {
            rpc.last_activity.store(TIME - 86401, Ordering::SeqCst);
            rpc
        })
        .await;
        // Retain a real finalized accounting history from an earlier fresh
        // observation, before testing expiry against the current follower.
        let paid = paid_fixture(
            Arc::new(Wallet::default()),
            Arc::new(ReceiptProbe::default()),
        )
        .await;
        let completed = paid
            .provider
            .inner
            .lock()
            .unwrap()
            .sessions
            .get_mut(&paid.sid)
            .unwrap()
            .runs
            .remove(&B256::repeat_byte(1))
            .unwrap();
        fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .sessions
            .get_mut(&fixture.sid)
            .unwrap()
            .runs
            .insert(B256::repeat_byte(1), completed);
        fixture.voucher(100);
        assert_eq!(
            fixture.provider.inner.lock().unwrap().sessions[&fixture.sid]
                .usage()
                .unwrap(),
            1
        );
        let old = fixture.provider.inner.chain.snapshot().unwrap();
        assert!(
            fixture
                .provider
                .inner
                .chain
                .estimated_timestamp_for_snapshot(&old)
                .unwrap()
                >= deadline(&old.sessions()[&fixture.sid]).unwrap()
        );
        let rpc = fixture.rpc.clone();
        let follower = fixture.provider.inner.chain.clone();
        TIMESTAMP_HOOK.with(|hook| {
            *hook.borrow_mut() = Some((
                fixture.sid,
                Box::new(move || {
                    std::thread::spawn(move || {
                        rpc.last_activity.store(TIME, Ordering::SeqCst);
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap()
                            .block_on(follower.refresh())
                            .unwrap();
                    })
                    .join()
                    .unwrap();
                }),
            ))
        });
        let result = fixture.provider.close(fixture.sid).await;
        let fresh = fixture.provider.inner.chain.snapshot().unwrap();
        assert!(!Arc::ptr_eq(&old, &fresh));
        assert_eq!(fresh.sessions()[&fixture.sid].last_activity(), TIME);
        assert_eq!(
            wallet.executions.load(Ordering::SeqCst),
            0,
            "superseded expiry must never authorize a wallet call"
        );
        let error = result.unwrap_err();
        let status = error
            .downcast_ref::<Status>()
            .expect("explicit stale payment status");
        assert_eq!(
            wire::parse_payment_error(status).unwrap().unwrap().reason(),
            Some("CHAIN_STATE_STALE")
        );
    }

    #[tokio::test]
    async fn cancelled_chain_reconciliation_retains_wallet_handles_and_never_resubmits() {
        let wallet = Arc::new(Wallet::default());
        wallet.return_hash.store(true, Ordering::SeqCst);
        let probe = Arc::new(ReceiptProbe::default());
        probe.mode.store(2, Ordering::SeqCst);
        let fixture = paid_fixture(wallet.clone(), probe.clone()).await;
        let provider = fixture.provider.clone();
        let sid = fixture.sid;
        // Direct internal collection isolates cancellation at this exact await;
        // public collections are separately tested for runtime task ownership.
        let task = tokio::spawn(async move { provider.collect(sid, false).await });
        probe.entered.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let evidence = fixture.provider.checkpoint(sid).await.unwrap();
        let retained = evidence.operation.unwrap().result;
        assert_eq!(retained.tx_hash(), Some(B256::repeat_byte(0xdd)));
        assert_eq!(retained.wallet_operation_id(), Some("wallet-original"));
        probe.mode.store(0, Ordering::SeqCst);
        let reconciled = fixture.provider.settle(sid).await.unwrap();
        assert_eq!(reconciled.operation_ref(), retained.operation_ref());
        assert_eq!(reconciled.tx_hash(), retained.tx_hash());
        assert_eq!(
            reconciled.wallet_operation_id(),
            retained.wallet_operation_id()
        );
        assert_eq!(reconciled.state(), OperationState::Pending);
        assert_eq!(probe.reads.load(Ordering::SeqCst), 2);
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
        assert_eq!(wallet.lookups.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rpc_failure_preserves_wallet_handles_without_terminal_claims() {
        let wallet = Arc::new(Wallet::default());
        wallet.return_hash.store(true, Ordering::SeqCst);
        let probe = Arc::new(ReceiptProbe::default());
        probe.mode.store(1, Ordering::SeqCst);
        let fixture = paid_fixture(wallet.clone(), probe).await;
        let result = fixture.provider.settle(fixture.sid).await.unwrap();
        assert_eq!(result.state(), OperationState::Unknown);
        assert_eq!(result.tx_hash(), Some(B256::repeat_byte(0xdd)));
        assert_eq!(result.wallet_operation_id(), Some("wallet-original"));
        assert!(result.observation().is_none());
        assert_eq!(
            fixture
                .provider
                .checkpoint(fixture.sid)
                .await
                .unwrap()
                .operation
                .unwrap()
                .result,
            result
        );
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_unresolved_settlement_keeps_the_wallet_cause() {
        let wallet = Arc::new(Wallet::default());
        wallet.lose_result.store(true, Ordering::SeqCst);
        let fixture = paid_fixture(wallet.clone(), Arc::new(ReceiptProbe::default())).await;
        let result = fixture.provider.settle(fixture.sid).await.unwrap();
        assert_eq!(result.state(), OperationState::Unknown);
        assert_eq!(result.cause(), Some("Unavailable (TIMEOUT)"));
        let retained = fixture.provider.checkpoint(fixture.sid).await.unwrap();
        assert_eq!(
            retained.operation.unwrap().result.cause(),
            Some("Unavailable (TIMEOUT)")
        );
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_fences_public_collection_paused_before_wallet_submission() {
        let _scope = HookScope;
        let wallet = Arc::new(Wallet::default());
        let fixture = Fixture::with_wallet(700, wallet.clone(), |rpc| rpc).await;
        let (entered_tx, entered_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        SUBMIT_HOOK.with(|hook| *hook.borrow_mut() = Some((fixture.sid, entered_tx, resume_rx)));
        let provider = fixture.provider.clone();
        let sid = fixture.sid;
        let task = tokio::spawn(async move { provider.close(sid).await });
        entered_rx.await.unwrap();
        let shutdown = fixture
            .provider
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(shutdown.pending_operations.is_empty());
        assert!(
            fixture.provider.inner.lock().unwrap().sessions[&sid]
                .operation
                .is_none()
        );
        let _ = resume_tx.send(());
        let _ = task.await.unwrap();
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 0);
        assert!(fixture.provider.close(sid).await.is_err());
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn shutdown_bounds_inflight_wallet_and_preserves_unknown_intent() {
        let wallet = Arc::new(Wallet::default());
        wallet.block_execute.store(true, Ordering::SeqCst);
        let fixture = Fixture::with_wallet(700, wallet.clone(), |rpc| rpc).await;
        let provider = fixture.provider.clone();
        let sid = fixture.sid;
        let task = tokio::spawn(async move { provider.close(sid).await });
        wallet.entered.notified().await;
        let shutdown = tokio::time::timeout(
            Duration::from_secs(1),
            fixture
                .provider
                .shutdown(Instant::now() + Duration::from_millis(100)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(shutdown.pending_operations.len(), 1);
        let retained = &shutdown.pending_operations[0];
        assert_eq!(retained.state(), OperationState::Unknown);
        assert!(retained.tx_hash().is_none());
        assert!(task.await.unwrap().is_err());
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture.provider.inner.lock().unwrap().sessions[&sid]
                .operation
                .as_ref()
                .unwrap()
                .result
                .as_ref(),
            retained
        );
    }

    #[test]
    fn wallet_lookup_outage_keeps_original_hash_and_operation_identity() {
        let original = OperationResult::new(
            "first",
            OperationState::Pending,
            Some("wallet-first".into()),
            Some(B256::repeat_byte(1)),
            None,
            vec![],
        )
        .unwrap();
        for incoming in [
            unknown("first").unwrap(),
            OperationResult::new(
                "first",
                OperationState::Pending,
                Some("wallet-other".into()),
                Some(B256::repeat_byte(2)),
                None,
                vec![],
            )
            .unwrap(),
            unknown("other-reference").unwrap(),
        ] {
            let (retained, reconcile) = retained_identity(&original, incoming).unwrap();
            assert!(reconcile);
            assert_eq!(retained.state(), OperationState::Unknown);
            assert_eq!(retained.operation_ref(), "first");
            assert_eq!(retained.tx_hash(), original.tx_hash());
            assert_eq!(
                retained.wallet_operation_id(),
                original.wallet_operation_id()
            );
        }
    }

    #[test]
    fn chain_evidence_keeps_a_known_wallet_handle_when_the_adapter_omits_it() {
        let hash = B256::repeat_byte(1);
        let original = OperationResult::new(
            "first",
            OperationState::Pending,
            Some("wallet-first".into()),
            Some(hash),
            None,
            vec![],
        )
        .unwrap();
        let observation = ChainObservation::new(
            5042002,
            crate::Address::repeat_byte(2),
            3,
            B256::repeat_byte(3),
        )
        .unwrap();
        let incoming = OperationResult::new(
            "first",
            OperationState::Confirmed,
            None,
            Some(hash),
            Some(observation),
            vec![],
        )
        .unwrap();
        let (retained, reconcile) = retained_identity(&original, incoming).unwrap();
        assert!(!reconcile);
        assert_eq!(retained.state(), OperationState::Confirmed);
        assert_eq!(retained.observation(), Some(observation));
        assert_eq!(retained.wallet_operation_id(), Some("wallet-first"));
    }

    #[tokio::test]
    async fn pending_close_is_due_before_periodic_settlement_interval() {
        let wallet = Arc::new(Wallet::default());
        let fixture = Fixture::with_wallet(1000, wallet.clone(), |rpc| rpc).await;
        let initial = fixture.provider.close(fixture.sid).await.unwrap();
        assert_eq!(initial.state(), OperationState::Pending);
        assert!(initial.tx_hash().is_none());
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
        assert_eq!(wallet.lookups.load(Ordering::SeqCst), 0);
        assert!(
            fixture.provider.inner.lock().unwrap().sessions[&fixture.sid]
                .last_collection
                .elapsed()
                < Duration::from_millis(fixture.provider.inner.config.settlement().interval_ms())
        );
        // No clock advancement or sleep: explicit cooperative close must not
        // wait for the unrelated periodic-capture trigger to reconcile.
        assert_eq!(
            fixture.provider.collections_due().unwrap(),
            vec![(fixture.sid, true)]
        );
    }

    /// Drain messages queued for the fixture's owner since the last call.
    async fn drains(fixture: &Fixture) -> usize {
        let queue = fixture.provider.inner.lock().unwrap().attachments[&1]
            .queue
            .clone();
        let mut drains = 0;
        while queue.buffered().unwrap().0 > 0 {
            let message =
                proto::ControlMessage::decode(queue.get().await.unwrap().as_slice()).unwrap();
            drains += usize::from(matches!(
                message.kind,
                Some(proto::control_message::Kind::Drain(_))
            ));
        }
        drains
    }

    #[tokio::test]
    async fn a_submitted_close_is_reconciled_every_pass_and_drains_its_owner_once() {
        let wallet = Arc::new(Wallet::default());
        let fixture = Fixture::with_wallet(1000, wallet.clone(), |rpc| rpc).await;
        let initial = fixture.provider.close(fixture.sid).await.unwrap();
        assert_eq!(initial.state(), OperationState::Pending);
        assert_eq!(drains(&fixture).await, 1);
        for _ in 0..3 {
            fixture.provider.inner.maintain_once();
            while !fixture.provider.inner.lock().unwrap().collecting.is_empty() {
                tokio::task::yield_now().await;
            }
        }
        fixture.provider.close(fixture.sid).await.unwrap();
        // Each pass and the repeated call reconciled it, with no new Drain.
        assert_eq!(wallet.lookups.load(Ordering::SeqCst), 4);
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
        assert_eq!(drains(&fixture).await, 0, "a close drained its owner again");
    }

    /// A runtime restarted over the same chain, wallet and configuration,
    /// holding the fixture's session from its checkpoint.
    async fn restored(fixture: &Fixture) -> Mppi {
        let evidence = fixture.provider.checkpoint(fixture.sid).await.unwrap();
        let source = &fixture.provider.inner;
        let provider = Mppi::new(
            source.config.clone(),
            source.chain.clone(),
            source.contract.clone(),
            source.wallet.clone(),
            source.coverage.clone(),
            source.execution.clone(),
        )
        .unwrap();
        {
            let mut state = provider.inner.lock().unwrap();
            state.started = true;
            state.start_block = Some(100);
        }
        provider.recover(evidence).await.unwrap();
        provider
    }

    /// Restored, a close that never began was Unknown (chain reconciliation
    /// of no transaction) and never prepared again.
    #[tokio::test]
    async fn a_restored_close_that_never_began_is_prepared_again_under_its_reference() {
        let wallet = Arc::new(Wallet::default());
        wallet.not_submitted.store(true, Ordering::SeqCst);
        let fixture = Fixture::with_wallet(1000, wallet.clone(), |rpc| rpc).await;
        let first = fixture.provider.close(fixture.sid).await.unwrap();
        assert_eq!(first.state(), OperationState::Failed);
        assert!(first.tx_hash().is_none());
        let provider = restored(&fixture).await;
        let retained = provider.inner.lock().unwrap().sessions[&fixture.sid]
            .operation
            .clone()
            .unwrap();
        assert_eq!(retained.result.state(), OperationState::Failed);
        wallet.not_submitted.store(false, Ordering::SeqCst);
        let again = provider.close(fixture.sid).await.unwrap();
        assert_eq!(again.operation_ref(), first.operation_ref());
        assert_eq!(again.state(), OperationState::Pending);
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 2);
    }

    /// Restored, a settlement that never began stranded its session, only
    /// ever reconciled.
    #[tokio::test]
    async fn a_restored_settlement_that_never_began_is_replaced_at_the_next_collection() {
        let wallet = Arc::new(Wallet::default());
        wallet.not_submitted.store(true, Ordering::SeqCst);
        let fixture = paid_fixture(wallet.clone(), Arc::new(ReceiptProbe::default())).await;
        let first = fixture.provider.settle(fixture.sid).await.unwrap();
        assert_eq!(first.state(), OperationState::Failed);
        let provider = restored(&fixture).await;
        wallet.not_submitted.store(false, Ordering::SeqCst);
        let again = provider.settle(fixture.sid).await.unwrap();
        assert_ne!(again.operation_ref(), first.operation_ref());
        assert_eq!(again.state(), OperationState::Pending);
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 2);
        // Nothing was read from the wallet for the call that never began.
        assert_eq!(wallet.lookups.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_close_that_never_began_is_prepared_again_under_its_reference() {
        let wallet = Arc::new(Wallet::default());
        wallet.not_submitted.store(true, Ordering::SeqCst);
        let fixture = Fixture::with_wallet(1000, wallet.clone(), |rpc| rpc).await;
        let first = fixture.provider.close(fixture.sid).await.unwrap();
        assert_eq!(first.state(), OperationState::Failed);
        assert!(first.tx_hash().is_none());
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
        // Maintenance prepares it again once per settlement interval, so a
        // failure that repeats never spins the wallet.
        assert!(fixture.provider.collections_due().unwrap().is_empty());
        let interval =
            Duration::from_millis(fixture.provider.inner.config.settlement().interval_ms());
        fixture
            .provider
            .inner
            .lock()
            .unwrap()
            .sessions
            .get_mut(&fixture.sid)
            .unwrap()
            .last_collection = Instant::now().checked_sub(interval).unwrap();
        assert_eq!(
            fixture.provider.collections_due().unwrap(),
            vec![(fixture.sid, true)]
        );
        fixture.provider.collection_finished(fixture.sid);
        // The close intent survives the failure; the same reference continues it.
        wallet.not_submitted.store(false, Ordering::SeqCst);
        let second = fixture.provider.close(fixture.sid).await.unwrap();
        assert_eq!(second.operation_ref(), first.operation_ref());
        assert_eq!(second.state(), OperationState::Pending);
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 2);
        assert!(fixture.provider.inner.lock().unwrap().sessions[&fixture.sid].close_pending);
    }

    #[tokio::test]
    async fn pending_close_public_retry_reconciles_original_without_resubmitting() {
        let wallet = Arc::new(Wallet::default());
        let fixture = Fixture::with_wallet(1000, wallet.clone(), |rpc| rpc).await;
        let original = fixture.provider.close(fixture.sid).await.unwrap();
        let retained = fixture.provider.inner.lock().unwrap().sessions[&fixture.sid]
            .operation
            .clone()
            .unwrap();
        let repeated = fixture.provider.close(fixture.sid).await.unwrap();
        assert_eq!(repeated.operation_ref(), original.operation_ref());
        assert_eq!(
            repeated.wallet_operation_id(),
            original.wallet_operation_id()
        );
        assert_eq!(wallet.executions.load(Ordering::SeqCst), 1);
        assert_eq!(wallet.lookups.load(Ordering::SeqCst), 1);
        let state = fixture.provider.inner.lock().unwrap();
        let current = state.sessions[&fixture.sid].operation.as_ref().unwrap();
        assert_eq!(current.prepared.call(), retained.prepared.call());
        assert!(state.sessions[&fixture.sid].close_pending);
        assert!(state.sessions[&fixture.sid].close_result.is_none());
    }
}
