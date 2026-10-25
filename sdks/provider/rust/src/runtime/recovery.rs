//! Detached validation of provider-supplied payment history. Recovery reads
//! canonical evidence; it never submits payment, resumes execution or attaches
//! a Control stream. The provider owns retention and supplies the history.

use super::*;
use crate::{
    contract::OperationKind,
    interfaces::{RecoveryEvidence, RecoveryOperation},
    signing::{RunAuth, Voucher},
    types::{OperationState, SessionView},
};

fn inconsistent(sid: B256) -> Status {
    not_ready("INCONSISTENT_STATE", Some(sid), None)
}
fn missing(sid: B256) -> Status {
    not_ready("STATE_UNAVAILABLE", Some(sid), None)
}
fn wire_status(status: SessionStatus) -> i32 {
    match status {
        SessionStatus::Open => proto::SessionStatus::Open as i32,
        SessionStatus::Closing => proto::SessionStatus::Closing as i32,
        SessionStatus::Closed => proto::SessionStatus::Closed as i32,
    }
}
fn final_status(status: RunStatus) -> i32 {
    match status {
        RunStatus::Completed => proto::RunStatus::Completed as i32,
        RunStatus::Failed => proto::RunStatus::Failed as i32,
        RunStatus::Cancelled => proto::RunStatus::Cancelled as i32,
        RunStatus::NotAdmitted => proto::RunStatus::NotAdmitted as i32,
    }
}
fn same_prepared(left: &PreparedOperation, right: &PreparedOperation) -> bool {
    left.kind() == right.kind()
        && left.call() == right.call()
        && left.descriptors() == right.descriptors()
        && left.amounts() == right.amounts()
        && left.expected_rate_card_hash() == right.expected_rate_card_hash()
}
fn preserves_identity(known: &OperationResult, incoming: &OperationResult) -> bool {
    known.operation_ref() == incoming.operation_ref()
        && known
            .tx_hash()
            .is_none_or(|hash| incoming.tx_hash() == Some(hash))
        && known
            .wallet_operation_id()
            .is_none_or(|id| incoming.wallet_operation_id() == Some(id))
}
fn unresolved_operation(operation: &PendingOperation) -> bool {
    matches!(
        operation.result.state(),
        OperationState::Pending | OperationState::Unknown
    ) && operation.result.observation().is_none()
}

impl Runtime {
    fn check_recovery_limits(&self, evidence: &RecoveryEvidence) -> IntegrationResult<()> {
        wire::validate_recovery_snapshot(&evidence.snapshot, &self.limits.wire)?;
        if evidence.snapshot.control_generation == Some(0)
            && (!evidence.runs.is_empty()
                || !evidence.reservations.is_empty()
                || evidence.confirmed_units != 0)
        {
            return Err(Status::invalid_argument(
                "unattached checkpoint contains private accounting history",
            )
            .into());
        }
        if evidence.runs.len() > self.limits.wire.message_bytes() / 32
            || evidence.reservations.len() > self.limits.wire.pending_runs()
            || evidence.publications.len() > evidence.snapshot.catalogs.len()
            || evidence
                .publications
                .iter()
                .any(|(hash, _)| hash.len() != 32)
        {
            return Err(Status::resource_exhausted("recovery history limit").into());
        }
        if evidence
            .snapshot
            .run_states
            .len()
            .checked_add(evidence.reservations.len())
            != Some(evidence.runs.len())
        {
            return Err(Status::invalid_argument("recovery run history is incomplete").into());
        }
        for operation in [&evidence.operation, &evidence.baseline]
            .into_iter()
            .flatten()
        {
            if operation.prepared.call().data().len() > self.limits.wire.message_bytes()
                || operation.prepared.descriptors().len() > self.limits.wire.message_bytes() / 32
                || operation.units.len() > operation.prepared.descriptors().len()
            {
                return Err(Status::resource_exhausted("recovery operation limit").into());
            }
        }
        Ok(())
    }

    async fn restore_operation(
        &self,
        evidence: Option<&RecoveryOperation>,
        sid: B256,
    ) -> IntegrationResult<Option<PendingOperation>> {
        let Some(evidence) = evidence else {
            return Ok(None);
        };
        let prepared = &evidence.prepared;
        let ids = prepared.session_ids().into_iter().collect::<BTreeSet<_>>();
        let units = evidence.units.iter().copied().collect::<BTreeMap<_, _>>();
        if !matches!(
            prepared.kind(),
            OperationKind::Settle | OperationKind::SettleBatch | OperationKind::Close
        ) || prepared.call().chain_id() != self.contract.domain().chain_id()
            || prepared.call().to() != self.contract.domain().verifying_contract()
            || !ids.contains(&sid)
            || units.len() != evidence.units.len()
            || units.keys().copied().collect::<BTreeSet<_>>() != ids
        {
            return Err(inconsistent(sid).into());
        }
        // PreparedOperation is immutable and only ContractCalls constructs it;
        // the exact signed ABI payload and ordered targets cannot be substituted.
        // Saved hashes are untrusted, including hashes still marked pending.
        // A canonical reverted receipt proves failure of that transaction, but
        // does not bind it to this operation. The wallet capability durably binds
        // the original reference to its exact call; corroborate every supplied
        // hash before allowing chain evidence to establish this intent's state.
        let wallet_result = if evidence.result.tx_hash().is_some() {
            let result = self
                .wallet
                .reconcile(evidence.result.operation_ref())
                .await
                .map_err(|_| missing(sid))?;
            if !preserves_identity(&evidence.result, &result) {
                return Err(inconsistent(sid).into());
            }
            Some(result)
        } else {
            None
        };
        // Saved terminal state and wallet terminal assertions are not canonical
        // chain evidence. Re-read the independently bound transaction. A newly
        // discovered wallet handle is retained together with its supplied hash.
        let result = self
            .reader
            .reconcile(
                prepared,
                evidence.result.operation_ref(),
                evidence.result.tx_hash(),
                wallet_result
                    .as_ref()
                    .and_then(OperationResult::wallet_operation_id)
                    .or_else(|| evidence.result.wallet_operation_id()),
            )
            .await?;
        if !preserves_identity(&evidence.result, &result) {
            return Err(inconsistent(sid).into());
        }
        Ok(Some(PendingOperation {
            prepared: Arc::new(prepared.clone()),
            result: Arc::new(result),
            units: Arc::new(units),
        }))
    }

    async fn restore_history(&self, evidence: &RecoveryEvidence) -> IntegrationResult<Session> {
        let record = &evidence.snapshot;
        // Structural presence/width checks ran before any external read.
        let sid = B256::from_slice(record.session_id.as_deref().unwrap());
        let descriptor = wire::descriptor_from_wire(record.descriptor.as_ref().unwrap())?;
        if descriptor.providerId != self.config.identity().provider_id()
            || descriptor.operator != self.wallet.address()
            || record.close_pending != Some(evidence.close_reference.is_some())
            || evidence
                .close_reference
                .as_ref()
                .is_some_and(String::is_empty)
        {
            return Err(inconsistent(sid).into());
        }
        let mut catalogs = BTreeMap::new();
        for row in &record.catalogs {
            let hash = B256::from_slice(row.rate_card_hash.as_deref().unwrap());
            let catalog = crate::parse_catalog(
                row.catalog.as_deref().unwrap(),
                self.limits.wire.catalog_bytes(),
            )?;
            if hash != catalog.rate_card_hash() || catalogs.insert(hash, catalog).is_some() {
                return Err(inconsistent(sid).into());
            }
        }
        let initial = B256::from_slice(record.initial_rate_card_hash.as_deref().unwrap());
        let opening = self
            .reader
            .opening(
                evidence.opening_tx.ok_or_else(|| missing(sid))?,
                sid,
                Some(catalogs.get(&initial).ok_or_else(|| missing(sid))?),
            )
            .await?;
        if opening.descriptor() != &descriptor {
            return Err(inconsistent(sid).into());
        }
        let block = self
            .reader
            .block(Some(evidence.observation.block_number()))
            .await?;
        if block.observation() != &evidence.observation
            || record.chain_block_number != Some(block.observation().block_number())
            || record.chain_block_hash.as_deref()
                != Some(block.observation().block_hash().as_slice())
            || block.observation().block_number() < opening.block().observation().block_number()
            || (block.observation().block_number() == opening.block().observation().block_number()
                && block != *opening.block())
        {
            return Err(inconsistent(sid).into());
        }
        let chain = self.reader.session(sid, &block).await?;
        if !chain.exists()
            || record.deposit != Some(chain.deposit())
            || record.settled_cumulative != Some(chain.settled_cumulative())
            || record.status != Some(wire_status(chain.status()))
            || initial != chain.initial_rate_card_hash()
        {
            return Err(inconsistent(sid).into());
        }
        let identity = self
            .reader
            .identity(descriptor.providerId, &block, true)
            .await?;
        if record.current_rate_card_hash.as_deref()
            != identity
                .catalog()
                .map(|catalog| catalog.rate_card_hash())
                .as_ref()
                .map(|hash| hash.as_slice())
        {
            return Err(inconsistent(sid).into());
        }
        if identity
            .catalog()
            .is_some_and(|catalog| catalogs.get(&catalog.rate_card_hash()) != Some(catalog))
        {
            return Err(missing(sid).into());
        }
        let mut publications = BTreeMap::new();
        for (raw, observation) in &evidence.publications {
            let hash = B256::from_slice(raw);
            if !catalogs.contains_key(&hash) || publications.insert(hash, *observation).is_some() {
                return Err(inconsistent(sid).into());
            }
        }
        let mut checked_blocks = BTreeMap::from([
            (
                opening.block().observation().block_number(),
                opening.block().clone(),
            ),
            (block.observation().block_number(), block.clone()),
        ]);
        for (hash, catalog) in &catalogs {
            if *hash == initial {
                continue;
            }
            let observed = publications.get(hash).ok_or_else(|| missing(sid))?;
            if observed.block_number() > block.observation().block_number() {
                return Err(missing(sid).into());
            }
            let published = self.reader.block(Some(observed.block_number())).await?;
            if published.observation() != observed
                || self
                    .reader
                    .identity(descriptor.providerId, &published, false)
                    .await?
                    .catalog()
                    != Some(catalog)
            {
                return Err(inconsistent(sid).into());
            }
            if checked_blocks
                .get(&observed.block_number())
                .is_some_and(|known| known != &published)
            {
                return Err(inconsistent(sid).into());
            }
            checked_blocks.insert(observed.block_number(), published);
        }
        let mut session = Session::new(sid, opening, true);
        session.catalogs = catalogs;
        session.publications = publications;
        session.generation = record.control_generation.unwrap();
        session.meter_seq = record.meter_seq.unwrap();
        session.close_pending = record.close_pending.unwrap();
        session.close_reference = evidence.close_reference.clone();
        session.inconsistent = evidence.inconsistent;
        let mut totals = BTreeMap::<(String, B256), TokenCounts>::new();
        let mut cursors = BTreeSet::new();
        for item in &evidence.runs {
            let context = &item.context;
            if context.session_id() != sid
                || session.runs.contains_key(&context.run_id())
                || context.control_generation() > session.generation
                || item.admitted == item.not_admitted
                || item.meter_seq > session.meter_seq
                || (item.meter_seq != 0 && !cursors.insert(item.meter_seq))
            {
                return Err(inconsistent(sid).into());
            }
            self.contract
                .domain()
                .signing_context()
                .verify(
                    &descriptor,
                    &RunAuth {
                        sessionId: sid,
                        controlGeneration: context.control_generation(),
                        runId: context.run_id(),
                        model: context.model().into(),
                        rateCardHash: context.rate_card_hash(),
                    },
                    &item.signature,
                )
                .map_err(|_| inconsistent(sid))?;
            let rates = *session
                .catalogs
                .get(&context.rate_card_hash())
                .and_then(|catalog| catalog.rates_for(context.model()))
                .ok_or_else(|| missing(sid))?;
            let accounting = item.accounting;
            let total = priced_numerator(accounting.counts(), rates)?
                .checked_add(accounting.remaining_numerator())
                .ok_or_else(|| inconsistent(sid))?;
            let bound = priced_numerator(item.covered_counts, rates)?;
            if item.admitted {
                if total > bound
                    || (accounting.final_status().is_none() && total != bound)
                    || ((accounting.counts() != zero_counts()
                        || accounting.final_status().is_some())
                        && item.meter_seq == 0)
                {
                    return Err(inconsistent(sid).into());
                }
                let key = (context.model().to_owned(), context.rate_card_hash());
                let previous = totals.get(&key).copied().unwrap_or_else(zero_counts);
                let add = |left: u64, right: u64| {
                    left.checked_add(right).ok_or_else(|| inconsistent(sid))
                };
                totals.insert(
                    key,
                    TokenCounts::new(
                        add(previous.input_tokens(), accounting.counts().input_tokens())?,
                        add(previous.cache_tokens(), accounting.counts().cache_tokens())?,
                        add(
                            previous.output_tokens(),
                            accounting.counts().output_tokens(),
                        )?,
                    )?,
                );
            } else if accounting != RunAccounting::new(zero_counts(), U256::ZERO, None)?
                || item.covered_counts != zero_counts()
                || item.meter_seq != 0
            {
                return Err(inconsistent(sid).into());
            }
            let stop = CancellationToken::new();
            stop.cancel();
            session.runs.insert(
                context.run_id(),
                Run {
                    context: context.clone(),
                    signature: item.signature,
                    rates,
                    accounting,
                    covered_counts: item.covered_counts,
                    phase: if item.admitted {
                        RunPhase::Admitted
                    } else {
                        RunPhase::NotAdmitted
                    },
                    stop,
                    terminal: None,
                    meter_seq: item.meter_seq,
                    unresolved: item.admitted && accounting.final_status().is_none(),
                    retained_bytes: 0,
                    last_low_credit: None,
                    queued: None,
                },
            );
        }
        if session.meter_seq != cursors.last().copied().unwrap_or(0) {
            return Err(inconsistent(sid).into());
        }
        let mut reservations = BTreeMap::new();
        for (context, accounting) in &evidence.reservations {
            if reservations
                .insert(context.run_id(), (context, accounting))
                .is_some()
            {
                return Err(inconsistent(sid).into());
            }
        }
        let expected = session
            .active()
            .map(|run| (run.context.run_id(), (&run.context, &run.accounting)))
            .collect::<BTreeMap<_, _>>();
        if reservations != expected {
            return Err(inconsistent(sid).into());
        }
        let mut stated = BTreeMap::new();
        for row in &record.usage_by_price {
            let rid = B256::from_slice(row.acceptance_run_id.as_deref().unwrap());
            let context = RunContext::new(
                sid,
                rid,
                row.acceptance_control_generation.unwrap(),
                row.model.as_deref().unwrap(),
                B256::from_slice(row.rate_card_hash.as_deref().unwrap()),
            )?;
            let run = session.runs.get(&rid).ok_or_else(|| missing(sid))?;
            if !run.admitted()
                || run.context != context
                || row.acceptance_signature.as_deref() != Some(run.signature.as_slice())
            {
                return Err(inconsistent(sid).into());
            }
            stated.insert(
                (context.model().to_owned(), context.rate_card_hash()),
                TokenCounts::new(
                    row.input_tokens.unwrap(),
                    row.cache_tokens.unwrap(),
                    row.output_tokens.unwrap(),
                )?,
            );
        }
        if stated != totals {
            return Err(inconsistent(sid).into());
        }
        let mut finals = record
            .run_states
            .iter()
            .map(|row| (B256::from_slice(row.run_id.as_deref().unwrap()), row))
            .collect::<BTreeMap<_, _>>();
        for run in session.runs.values() {
            let row = finals.remove(&run.context.run_id());
            if run.active() {
                if row.is_some() {
                    return Err(inconsistent(sid).into());
                }
                continue;
            }
            let row = row.ok_or_else(|| missing(sid))?;
            if run.phase == RunPhase::NotAdmitted {
                if row.status != Some(proto::RunStatus::NotAdmitted as i32) {
                    return Err(inconsistent(sid).into());
                }
            } else if row.status != run.accounting.final_status().map(final_status)
                || row.model.as_deref() != Some(run.context.model())
                || row.rate_card_hash.as_deref() != Some(run.context.rate_card_hash().as_slice())
                || row.control_generation != Some(run.context.control_generation())
                || row.auth_signature.as_deref() != Some(run.signature.as_slice())
                || TokenCounts::new(
                    row.input_tokens.unwrap(),
                    row.cache_tokens.unwrap(),
                    row.output_tokens.unwrap(),
                )? != run.accounting.counts()
            {
                return Err(inconsistent(sid).into());
            }
        }
        if !finals.is_empty() {
            return Err(inconsistent(sid).into());
        }
        let required = required_coverage(
            std::iter::once(session.usage_numerator()?).chain(
                session
                    .active()
                    .map(|run| run.accounting.remaining_numerator()),
            ),
        )?;
        if let Some(voucher) = &record.accepted_voucher {
            let cumulative = voucher.cumulative.unwrap();
            self.contract
                .domain()
                .signing_context()
                .verify(
                    &descriptor,
                    &Voucher {
                        sessionId: B256::from_slice(voucher.session_id.as_deref().unwrap()),
                        cumulativeAmount: cumulative,
                    },
                    voucher.signature.as_deref().unwrap(),
                )
                .map_err(|_| inconsistent(sid))?;
            if cumulative < required || cumulative > chain.deposit() {
                return Err(inconsistent(sid).into());
            }
            session.voucher = Some(voucher.clone());
        } else if session.runs.values().any(Run::admitted) || chain.settled_cumulative() != 0 {
            return Err(missing(sid).into());
        }
        if session.usage()? < chain.settled_cumulative() {
            return Err(inconsistent(sid).into());
        }
        session.operation = self
            .restore_operation(evidence.operation.as_ref(), sid)
            .await?;
        session.baseline = self
            .restore_operation(evidence.baseline.as_ref(), sid)
            .await?;
        for operation in [&session.operation, &session.baseline]
            .into_iter()
            .flatten()
        {
            if operation.units[&sid] > session.units()? {
                return Err(inconsistent(sid).into());
            }
            for ((owner, target), operation_sid) in operation
                .prepared
                .descriptors()
                .iter()
                .zip(operation.prepared.amounts())
                .zip(operation.prepared.session_ids())
            {
                if operation_sid == sid && (owner != &descriptor || *target > session.usage()?) {
                    return Err(inconsistent(sid).into());
                }
            }
            if operation.prepared.kind() == OperationKind::Close {
                if !session.close_pending
                    || session.close_reference.as_deref() != Some(operation.result.operation_ref())
                    || session.active().next().is_some()
                {
                    return Err(inconsistent(sid).into());
                }
                if operation.result.state() == OperationState::Confirmed {
                    let closing = self
                        .reader
                        .closing(operation.result.tx_hash().ok_or_else(|| missing(sid))?, sid)
                        .await?;
                    session.close_result = Some(proto::CloseResult {
                        session_id: Some(sid.to_vec()),
                        tx_hash: Some(closing.tx_hash().to_vec()),
                        capture_amount: Some(closing.capture_amount()),
                        refunded: Some(closing.refunded()),
                    });
                }
            }
        }
        if evidence.confirmed_units > session.units()? {
            return Err(inconsistent(sid).into());
        }
        if evidence.confirmed_units != 0
            && session.baseline.as_ref().is_none_or(|baseline| {
                baseline.units.get(&sid) != Some(&evidence.confirmed_units)
                    || !super::sessions::covered(baseline, sid)
            })
        {
            return Err(missing(sid).into());
        }
        session.confirmed_units = evidence.confirmed_units;
        self.history_fits_locked(&session, None, None)?;
        for checked in checked_blocks.values() {
            self.reader.check_canonical(checked).await?;
        }
        Ok(session)
    }

    fn check_replacement(&self, state: &State, candidate: &Session) -> IntegrationResult<()> {
        let sid = candidate.sid;
        let Some(previous) = state.sessions.get(&sid) else {
            return Ok(());
        };
        if previous
            .owner
            .and_then(|id| state.attachments.get(&id))
            .is_some_and(|owner| owner.live)
            || previous.active().next().is_some()
            || previous
                .runs
                .values()
                .any(|run| run.phase == RunPhase::Preparing)
        {
            return Err(not_ready("CONTROL_NOT_READY", Some(sid), None).into());
        }
        if previous.opening.tx_hash() != candidate.opening.tx_hash()
            || previous.opening.descriptor() != candidate.opening.descriptor()
            || previous.opening.block() != candidate.opening.block()
            || previous.opening.deposit() != candidate.opening.deposit()
            || previous.opening.initial_catalog() != candidate.opening.initial_catalog()
            || previous.generation > candidate.generation
            || previous.meter_seq > candidate.meter_seq
            || (previous.close_pending && !candidate.close_pending)
            || (previous.inconsistent && !candidate.inconsistent)
            || previous.confirmed_units > candidate.confirmed_units
            || previous
                .close_reference
                .as_ref()
                .is_some_and(|value| candidate.close_reference.as_ref() != Some(value))
            || previous
                .close_result
                .as_ref()
                .is_some_and(|value| candidate.close_result.as_ref() != Some(value))
            || previous.voucher.as_ref().is_some_and(|voucher| {
                candidate
                    .voucher
                    .as_ref()
                    .is_none_or(|incoming| incoming.cumulative < voucher.cumulative)
            })
        {
            return Err(inconsistent(sid).into());
        }
        if let Some(known) = &previous.operation
            && (unresolved_operation(known) || known.prepared.kind() == OperationKind::Close)
            && candidate.operation.as_ref().is_none_or(|incoming| {
                !same_prepared(&known.prepared, &incoming.prepared)
                    || known.units != incoming.units
                    || !preserves_identity(&known.result, &incoming.result)
            })
        {
            return Err(missing(sid).into());
        }
        for (rid, run) in &previous.runs {
            let incoming = candidate.runs.get(rid).ok_or_else(|| missing(sid))?;
            if run.context != incoming.context
                || run.signature != incoming.signature
                || run.admitted() != incoming.admitted()
                || (run.phase == RunPhase::NotAdmitted) != (incoming.phase == RunPhase::NotAdmitted)
            {
                return Err(inconsistent(sid).into());
            }
            report_transition(
                &run.accounting,
                incoming.accounting.counts(),
                run.rates,
                incoming.accounting.final_status(),
            )?;
        }
        Ok(())
    }
}

impl Mppi {
    /// `current`: the host asserts this evidence is the session's current
    /// history (`ProviderSessions::recover_current`).
    pub(super) async fn recover_evidence(
        &self,
        evidence: RecoveryEvidence,
        current: bool,
    ) -> IntegrationResult<SessionView> {
        let runtime = &self.inner;
        runtime.check_recovery_limits(&evidence)?;
        let sid = B256::from_slice(evidence.snapshot.session_id.as_deref().unwrap());
        let _operations = runtime.operations.lock().await;
        let _openings = runtime.openings.lock().await;
        let _watch = PendingWatch::new(runtime, sid)?;
        {
            let state = runtime.lock()?;
            runtime.snapshot_locked(&state)?;
            if !state.sessions.contains_key(&sid)
                && state.sessions.len() >= runtime.limits.max_sessions
            {
                return Err(Status::resource_exhausted("session retention limit").into());
            }
        }
        let mut candidate = runtime.restore_history(&evidence).await?;
        candidate.admissions_known = current;
        runtime.chain.watch_session(sid).await?;
        let mut state = runtime.lock()?;
        let current = runtime.snapshot_locked(&state)?;
        let observed = current.sessions().get(&sid).ok_or_else(|| missing(sid))?;
        if !observed.exists()
            || candidate.usage()? < observed.settled_cumulative()
            || observed.deposit() < evidence.snapshot.deposit.unwrap()
            || observed.initial_rate_card_hash()
                != candidate.opening.initial_catalog().rate_card_hash()
            || wire_status(observed.status()) < evidence.snapshot.status.unwrap()
            || current.block().observation().block_number() < evidence.observation.block_number()
            || (current.block().observation().block_number() == evidence.observation.block_number()
                && current.block().observation() != &evidence.observation)
        {
            return Err(inconsistent(sid).into());
        }
        runtime.check_replacement(&state, &candidate)?;
        // A restored batch cannot coexist with a different unresolved call
        // against any of its members. Require that original intent's evidence
        // to be reconciled first; do not silently overwrite it from this record.
        let mut incoming_ids = candidate
            .operation
            .as_ref()
            .map_or_else(BTreeSet::new, |operation| {
                operation.units.keys().copied().collect()
            });
        incoming_ids.insert(sid);
        for other in state.sessions.values().filter(|other| other.sid != sid) {
            if let Some(known) = &other.operation
                && unresolved_operation(known)
                && known.units.keys().any(|id| incoming_ids.contains(id))
                && candidate.operation.as_ref().is_none_or(|incoming| {
                    incoming.result.operation_ref() != known.result.operation_ref()
                })
            {
                return Err(missing(sid).into());
            }
        }
        // A batch's immutable intent and known identifiers agree across every
        // retained member before replacing any authoritative state.
        if let Some(incoming) = &candidate.operation {
            for other in state.sessions.values().filter(|other| other.sid != sid) {
                if let Some(known) = &other.operation
                    && known.result.operation_ref() == incoming.result.operation_ref()
                    && (!same_prepared(&known.prepared, &incoming.prepared)
                        || known.units != incoming.units
                        || !preserves_identity(&known.result, &incoming.result))
                {
                    return Err(inconsistent(sid).into());
                }
            }
        }
        let view = runtime.view_locked(&state, &candidate)?;
        let retained = state.sessions.get(&sid).map_or(0, |previous| {
            previous.runs.values().map(|run| run.retained_bytes).sum()
        });
        state.retained_request_bytes = state
            .retained_request_bytes
            .checked_sub(retained)
            .ok_or_else(|| Status::internal("request retention accounting mismatch"))?;
        if let Some(incoming) = &candidate.operation {
            for other in state.sessions.values_mut().filter(|other| other.sid != sid) {
                if other.operation.as_ref().is_some_and(|known| {
                    known.result.operation_ref() == incoming.result.operation_ref()
                }) {
                    other.operation = Some(incoming.clone());
                }
            }
        }
        state.sessions.insert(sid, candidate);
        runtime.changed.notify_waiters();
        Ok(view)
    }
}
