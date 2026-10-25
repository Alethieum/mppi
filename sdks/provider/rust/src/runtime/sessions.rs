//! Ordered collection over retained immutable wallet intents.

use super::*;
use crate::{
    chain::ChainSession,
    contract::OperationKind,
    interfaces::{ProviderSessions, RecoveryEvidence, RecoveryOperation, RecoveryRun},
    operations::{reconcile_wallet_operation, submit_wallet_operation},
    types::{OperationState, PauseLevel, Readiness, SessionPage, SessionView, validate_page_limit},
};
use alloy_primitives::hex;
use std::time::Duration;

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
    Ok((
        OperationResult::new(
            known.operation_ref(),
            incoming.state(),
            wallet_id,
            incoming.tx_hash(),
            incoming.observation(),
            incoming.sessions().to_vec(),
        )?,
        false,
    ))
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

impl Runtime {
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
            .ok_or_else(|| not_ready("STATE_UNAVAILABLE", Some(sid), None))?;
        if !session.history_available
            || session
                .runs
                .values()
                .any(|run| run.phase == RunPhase::Preparing)
        {
            return Err(not_ready("STATE_UNAVAILABLE", Some(sid), None).into());
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
            if state.collecting.contains(sid) || session.close_result.is_some() {
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
        let runtime = &self.inner;
        let _operations = runtime.operations.lock().await;
        let (mut pending, submit, close_reference) = {
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
                session.close_pending = true;
                runtime.stop_session_locked(&mut state, sid);
                runtime.changed.notify_waiters();
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
            if let Some(operation) = previous.as_ref()
                && operation.prepared.kind() == OperationKind::Close
            {
                // A close reference always identifies this exact call; even a
                // terminal failure never licenses replacing its calldata.
                if operation.result.state() == OperationState::Failed {
                    return Ok(operation.result.as_ref().clone());
                }
                (operation.clone(), false, close_reference)
            } else if let Some(operation) = previous.as_ref().filter(|op| {
                matches!(
                    op.result.state(),
                    OperationState::Pending | OperationState::Unknown
                ) && op.result.observation().is_none()
            }) {
                (operation.clone(), false, close_reference)
            } else if ids.is_empty()
                && let Some(operation) = previous.as_ref().filter(|op| {
                    op.result.observation().is_some() && op.result.state() != OperationState::Failed
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
                    let (cumulative, signature) =
                        if capture > observed.settled_cumulative() || !close {
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
                if prepared.call().data().len() > wire::MAX_MESSAGE_BYTES {
                    return Err(Status::resource_exhausted(
                        "batch calldata exceeds resource limit",
                    )
                    .into());
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
                    for sid in ids {
                        let session = state.sessions.get_mut(sid).expect("known session");
                        session.operation = Some(pending.clone());
                        session.last_collection = Instant::now();
                    }
                }
                state.unsubmitted_collection = Some(reference);
                (pending, true, close_reference)
            }
        };

        let _unsubmitted = submit.then(|| UnsubmittedCollection {
            runtime: runtime.clone(),
            reference: pending.result.operation_ref().to_owned(),
        });
        if submit && pending.prepared.kind() == OperationKind::SettleBatch {
            runtime
                .reader
                .simulate(&pending.prepared, runtime.wallet.address())
                .await?;
        }
        #[cfg(test)]
        if let Some(sid) = ids.first() {
            tests::before_submit(*sid).await;
        }
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
            submit_wallet_operation(
                runtime.wallet.as_ref(),
                &pending.prepared,
                pending.result.operation_ref(),
                &runtime.reader,
            )
            .await?
        } else if pending.result.state() == OperationState::Confirmed {
            pending.result.as_ref().clone()
        } else {
            reconcile_wallet_operation(
                runtime.wallet.as_ref(),
                &pending.prepared,
                pending.result.operation_ref(),
                &runtime.reader,
            )
            .await?
        };
        let (mut result, _) = retained_identity(&pending.result, incoming)?;
        // No suspension between returned wallet handles and their retention on
        // every session. Cancellation during chain I/O preserves both handles.
        pending.result = Arc::new(result.clone());
        runtime.retain_collection(&pending)?;
        if result.state() != OperationState::Confirmed && result.tx_hash().is_some() {
            result = runtime
                .reader
                .reconcile(
                    &pending.prepared,
                    result.operation_ref(),
                    result.tx_hash(),
                    result.wallet_operation_id(),
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
                runtime
                    .reader
                    .closing(tx, pending.prepared.session_ids()[0])
                    .await
                    .ok()
            } else {
                None
            }
        } else {
            None
        };
        // A refresh outage neither erases evidence nor invents another operation.
        let _ = runtime.chain.refresh().await;
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
        runtime.changed.notify_waiters();
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
        self.changed.notify_waiters();
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
        Ok(())
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
