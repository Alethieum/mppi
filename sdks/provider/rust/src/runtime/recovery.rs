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
use std::time::SystemTime;

/// The evidence contradicts itself, the chain, the wallet or a session already
/// held. The payment failure stays `INCONSISTENT_STATE`; the message names the
/// check that failed, in the SDK's words, never remote text. Each check is
/// listed in `recovery_tests.rs`, whose test keeps every refusal here to one
/// literal from that list.
fn inconsistent(sid: B256, check: &str) -> Status {
    labelled("INCONSISTENT_STATE", sid, check)
}
/// Evidence recovery needs is missing or unavailable. The payment failure stays
/// `STATE_UNAVAILABLE`; the message names the check that failed, in the SDK's
/// words, never remote text.
pub(super) fn missing(sid: B256, check: &str) -> Status {
    labelled("STATE_UNAVAILABLE", sid, check)
}
/// A `SESSION_NOT_READY` refusal for `reason` whose message names `check`.
fn labelled(reason: &str, sid: B256, check: &str) -> Status {
    let status = not_ready(reason, Some(sid), None);
    Status::with_details(
        status.code(),
        format!("{}: {check}", status.message()),
        status.details().to_vec().into(),
    )
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
/// The wallet's answer for a reference matches the runtime's record of it: the
/// same reference, the same transaction hash or none on both, and the same
/// wallet operation id when the record names one.
pub(super) fn agrees(record: &OperationResult, answer: &OperationResult) -> bool {
    answer.tx_hash() == record.tx_hash() && preserves_identity(record, answer)
}
fn unresolved_operation(operation: &PendingOperation) -> bool {
    matches!(
        operation.result.state(),
        OperationState::Pending | OperationState::Unknown
    ) && operation.result.observation().is_none()
}

impl Runtime {
    /// A collection of this deployment's contract that covers `sid`, the
    /// first check of any supplied operation.
    fn collects(&self, prepared: &PreparedOperation, sid: B256) -> bool {
        matches!(
            prepared.kind(),
            OperationKind::Settle | OperationKind::SettleBatch | OperationKind::Close
        ) && prepared.call().chain_id() == self.contract.domain().chain_id()
            && prepared.call().to() == self.contract.domain().verifying_contract()
            && prepared.session_ids().contains(&sid)
    }

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
        if !self.collects(prepared, sid)
            || units.len() != evidence.units.len()
            || units.keys().copied().collect::<BTreeSet<_>>() != ids
        {
            return Err(inconsistent(
                sid,
                "a saved operation does not collect this session or misstates its units",
            )
            .into());
        }
        let operation = |result| PendingOperation {
            prepared: Arc::new(prepared.clone()),
            result: Arc::new(result),
            units: Arc::new(units.clone()),
        };
        // A Failed result with no handle at all is the wallet's attestation
        // that submission never began (TransactionWallet): no call is bound to
        // its reference and there is nothing to read. It stays Failed, so the
        // live rules apply again: a settlement is replaced by a new reference,
        // and a close continues under its own. Neither can pay twice: captures
        // are cumulative on chain, and the wallet checks a reused close
        // reference against the call bound to it.
        let source = &evidence.result;
        if source.state() == OperationState::Failed
            && source.tx_hash().is_none()
            && source.wallet_operation_id().is_none()
            && source.observation().is_none()
            && source.sessions().is_empty()
        {
            let result = OperationResult::new(
                source.operation_ref(),
                OperationState::Failed,
                None,
                None,
                None,
                vec![],
            )?;
            let result = match source.cause() {
                Some(cause) => result.with_cause(cause)?,
                None => result,
            };
            return Ok(Some(operation(result)));
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
                .map_err(|_| missing(sid, "the wallet could not look up a saved transaction"))?;
            if !preserves_identity(&evidence.result, &result) {
                return Err(inconsistent(
                    sid,
                    "the wallet's record of a saved transaction differs from the evidence",
                )
                .into());
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
            return Err(inconsistent(
                sid,
                "the chain's record of a saved transaction differs from the evidence",
            )
            .into());
        }
        Ok(Some(operation(result)))
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
            return Err(inconsistent(
                sid,
                "the descriptor is not this provider's, or the close state is malformed",
            )
            .into());
        }
        let mut catalogs = BTreeMap::new();
        for row in &record.catalogs {
            let hash = B256::from_slice(row.rate_card_hash.as_deref().unwrap());
            let catalog = crate::parse_catalog(
                row.catalog.as_deref().unwrap(),
                self.limits.wire.catalog_bytes(),
            )?;
            if hash != catalog.rate_card_hash() || catalogs.insert(hash, catalog).is_some() {
                return Err(
                    inconsistent(sid, "a catalog does not match its hash or repeats").into(),
                );
            }
        }
        let initial = B256::from_slice(record.initial_rate_card_hash.as_deref().unwrap());
        let opening = self
            .reader
            .opening(
                evidence
                    .opening_tx
                    .ok_or_else(|| missing(sid, "the evidence names no opening transaction"))?,
                sid,
                Some(
                    catalogs
                        .get(&initial)
                        .ok_or_else(|| missing(sid, "the evidence lacks the initial catalog"))?,
                ),
            )
            .await?;
        if opening.descriptor() != &descriptor {
            return Err(
                inconsistent(sid, "the opening transaction shows another descriptor").into(),
            );
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
            return Err(inconsistent(
                sid,
                "the checkpoint block contradicts the chain or the opening",
            )
            .into());
        }
        let chain = self.reader.session(sid, &block).await?;
        if !chain.exists()
            || record.deposit != Some(chain.deposit())
            || record.settled_cumulative != Some(chain.settled_cumulative())
            || record.status != Some(wire_status(chain.status()))
            || initial != chain.initial_rate_card_hash()
        {
            return Err(inconsistent(
                sid,
                "the session's chain state at the checkpoint block differs from the evidence",
            )
            .into());
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
            return Err(inconsistent(
                sid,
                "the current catalog differs from the provider's at the checkpoint block",
            )
            .into());
        }
        if identity
            .catalog()
            .is_some_and(|catalog| catalogs.get(&catalog.rate_card_hash()) != Some(catalog))
        {
            return Err(missing(sid, "the evidence lacks the current catalog").into());
        }
        let mut publications = BTreeMap::new();
        for (raw, observation) in &evidence.publications {
            let hash = B256::from_slice(raw);
            if !catalogs.contains_key(&hash) || publications.insert(hash, *observation).is_some() {
                return Err(inconsistent(
                    sid,
                    "a publication names no catalog of the evidence or repeats",
                )
                .into());
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
            let observed = publications
                .get(hash)
                .ok_or_else(|| missing(sid, "a catalog has no publication evidence"))?;
            if observed.block_number() > block.observation().block_number() {
                return Err(
                    missing(sid, "a catalog was published after the checkpoint block").into(),
                );
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
                return Err(
                    inconsistent(sid, "a catalog's publication differs from the chain").into(),
                );
            }
            if checked_blocks
                .get(&observed.block_number())
                .is_some_and(|known| known != &published)
            {
                return Err(inconsistent(sid, "two blocks read at one height differ").into());
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
                return Err(inconsistent(
                    sid,
                    "a run's context, admission or meter sequence contradicts the evidence",
                )
                .into());
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
                .map_err(|_| inconsistent(sid, "a run's acceptance signature is invalid"))?;
            let rates = *session
                .catalogs
                .get(&context.rate_card_hash())
                .and_then(|catalog| catalog.rates_for(context.model()))
                .ok_or_else(|| missing(sid, "a run's model has no rates in its catalog"))?;
            let accounting = item.accounting;
            let total = priced_numerator(accounting.counts(), rates)?
                .checked_add(accounting.remaining_numerator())
                .ok_or_else(|| inconsistent(sid, "a run's accounting overflows"))?;
            let bound = priced_numerator(item.covered_counts, rates)?;
            if item.admitted {
                if total > bound
                    || (accounting.final_status().is_none() && total != bound)
                    || ((accounting.counts() != zero_counts()
                        || accounting.final_status().is_some())
                        && item.meter_seq == 0)
                {
                    return Err(inconsistent(
                        sid,
                        "an admitted run's accounting contradicts its coverage or meter sequence",
                    )
                    .into());
                }
                let key = (context.model().to_owned(), context.rate_card_hash());
                let previous = totals.get(&key).copied().unwrap_or_else(zero_counts);
                let add = |left: u64, right: u64| {
                    left.checked_add(right)
                        .ok_or_else(|| inconsistent(sid, "the priced usage overflows"))
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
                return Err(inconsistent(sid, "a run that was not admitted has accounting").into());
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
                    refusal: None,
                },
            );
        }
        if session.meter_seq != cursors.last().copied().unwrap_or(0) {
            return Err(
                inconsistent(sid, "the session's meter sequence is not its runs' last").into(),
            );
        }
        let mut reservations = BTreeMap::new();
        for (context, accounting) in &evidence.reservations {
            if reservations
                .insert(context.run_id(), (context, accounting))
                .is_some()
            {
                return Err(
                    inconsistent(sid, "the reservations differ from the active runs").into(),
                );
            }
        }
        let expected = session
            .active()
            .map(|run| (run.context.run_id(), (&run.context, &run.accounting)))
            .collect::<BTreeMap<_, _>>();
        if reservations != expected {
            return Err(inconsistent(sid, "the reservations differ from the active runs").into());
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
            let run = session
                .runs
                .get(&rid)
                .ok_or_else(|| missing(sid, "a priced usage row names no run in the evidence"))?;
            if !run.admitted()
                || run.context != context
                || row.acceptance_signature.as_deref() != Some(run.signature.as_slice())
            {
                return Err(inconsistent(sid, "a priced usage row differs from its run").into());
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
            return Err(inconsistent(sid, "the priced usage differs from the runs' totals").into());
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
                    return Err(inconsistent(sid, "an unfinished run has a final state").into());
                }
                continue;
            }
            let row = row.ok_or_else(|| missing(sid, "a finished run has no final state"))?;
            if run.phase == RunPhase::NotAdmitted {
                if row.status != Some(proto::RunStatus::NotAdmitted as i32) {
                    return Err(inconsistent(
                        sid,
                        "a run that was not admitted has another final status",
                    )
                    .into());
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
                return Err(inconsistent(sid, "a final state differs from its run").into());
            }
        }
        if !finals.is_empty() {
            return Err(inconsistent(sid, "a final state names no run of the evidence").into());
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
                .map_err(|_| inconsistent(sid, "the accepted voucher's signature is invalid"))?;
            if cumulative < required || cumulative > chain.deposit() {
                return Err(inconsistent(
                    sid,
                    "the accepted voucher does not cover the usage or exceeds the deposit",
                )
                .into());
            }
            session.voucher = Some(voucher.clone());
        } else if session.runs.values().any(Run::admitted) || chain.settled_cumulative() != 0 {
            return Err(missing(sid, "admitted usage has no accepted voucher").into());
        }
        if session.usage()? < chain.settled_cumulative() {
            return Err(inconsistent(sid, "the usage is below the settled amount").into());
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
                return Err(inconsistent(
                    sid,
                    "a saved operation exceeds the session's usage or names another descriptor",
                )
                .into());
            }
            for ((owner, target), operation_sid) in operation
                .prepared
                .descriptors()
                .iter()
                .zip(operation.prepared.amounts())
                .zip(operation.prepared.session_ids())
            {
                if operation_sid == sid && (owner != &descriptor || *target > session.usage()?) {
                    return Err(inconsistent(
                        sid,
                        "a saved operation exceeds the session's usage or names another descriptor",
                    )
                    .into());
                }
            }
            if operation.prepared.kind() == OperationKind::Close {
                if !session.close_pending
                    || session.close_reference.as_deref() != Some(operation.result.operation_ref())
                    || session.active().next().is_some()
                {
                    return Err(inconsistent(
                        sid,
                        "a saved close does not match the session's close intent",
                    )
                    .into());
                }
                if operation.result.state() == OperationState::Confirmed {
                    let closing = self
                        .reader
                        .closing(
                            operation.result.tx_hash().ok_or_else(|| {
                                missing(sid, "a confirmed close has no transaction hash")
                            })?,
                            sid,
                        )
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
            return Err(inconsistent(sid, "the confirmed units exceed the session's units").into());
        }
        if evidence.confirmed_units != 0
            && session.baseline.as_ref().is_none_or(|baseline| {
                baseline.units.get(&sid) != Some(&evidence.confirmed_units)
                    || !super::sessions::covered(baseline, sid)
            })
        {
            return Err(missing(sid, "confirmed units have no covered baseline").into());
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
            return Err(inconsistent(
                sid,
                "the evidence is older than the held session or contradicts it",
            )
            .into());
        }
        if let Some(known) = &previous.operation
            && (unresolved_operation(known) || known.prepared.kind() == OperationKind::Close)
            && candidate.operation.as_ref().is_none_or(|incoming| {
                !same_prepared(&known.prepared, &incoming.prepared)
                    || known.units != incoming.units
                    || !preserves_identity(&known.result, &incoming.result)
            })
        {
            return Err(missing(
                sid,
                "the evidence lacks the held session's unresolved or close operation",
            )
            .into());
        }
        for (rid, run) in &previous.runs {
            let incoming = candidate
                .runs
                .get(rid)
                .ok_or_else(|| missing(sid, "the evidence lacks a run the held session has"))?;
            if run.context != incoming.context
                || run.signature != incoming.signature
                || run.admitted() != incoming.admitted()
                || (run.phase == RunPhase::NotAdmitted) != (incoming.phase == RunPhase::NotAdmitted)
            {
                return Err(inconsistent(sid, "a run differs from the held session's").into());
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
        // Every read comes first, holding only this session's watch interest,
        // so sessions recover concurrently while collections and openings go
        // on. Every check against held state (the follower's snapshot, a session
        // already held, other sessions' operations) runs below against what was
        // read, under the operation, opening and state locks with no read under
        // them, and the candidate is installed under the same locks.
        let _watch = PendingWatch::new(runtime, sid)?;
        runtime.snapshot_locked(&*runtime.lock()?)?;
        // Nothing remains to serve or collect for final evidence of a closed
        // session: it is never reinstalled, whether or not it retired here.
        if evidence.is_final()
            && runtime.chain.watch_session(sid).await?.status() == SessionStatus::Closed
        {
            // Hosts pass such records at startup: the session no longer needs
            // the references it names, from now, as if it retired here. One
            // that only it covers goes at once; a batch waits for the rest.
            // A reference goes only if the wallet's own record of it agrees
            // with the record, as restore_operation corroborates a saved hash:
            // both name the same transaction, or neither names one. A mixed-up
            // or corrupt record could pair this session's call with another
            // session's reference, and a wallet record with a transaction the
            // record lacks is traceable, so it stays for the wallet's own
            // reconciliation. Keeping one is always safe.
            let mut bound = Vec::new();
            for operation in [&evidence.operation, &evidence.baseline]
                .into_iter()
                .flatten()
                .filter(|operation| runtime.collects(&operation.prepared, sid))
            {
                if let Ok(answer) = runtime
                    .wallet
                    .reconcile(operation.result.operation_ref())
                    .await
                    && agrees(&operation.result, &answer)
                {
                    bound.push(operation);
                }
            }
            let mut state = runtime.lock()?;
            if !state.sessions.contains_key(&sid) {
                let now = SystemTime::now();
                for operation in bound {
                    let covered = state
                        .references
                        .entry(operation.result.operation_ref().to_owned())
                        .or_insert_with(|| {
                            operation
                                .prepared
                                .session_ids()
                                .into_iter()
                                .map(|id| (id, None))
                                .collect()
                        });
                    covered.entry(sid).or_insert(None).get_or_insert(now);
                }
                super::sessions::hand_over(&mut state);
            }
            return Err(not_ready("SESSION_CLOSED", Some(sid), None).into());
        }
        let mut candidate = runtime.restore_history(&evidence).await?;
        candidate.admissions_known = current;
        runtime.chain.watch_session(sid).await?;
        let _operations = runtime.operations.lock().await;
        let _openings = runtime.openings.lock().await;
        let mut state = runtime.lock()?;
        let current = runtime.snapshot_locked(&state)?;
        let observed = current
            .sessions()
            .get(&sid)
            .ok_or_else(|| missing(sid, "the chain follower does not show the session"))?;
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
            return Err(
                inconsistent(sid, "the chain follower's view contradicts the evidence").into(),
            );
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
                return Err(missing(
                    sid,
                    "another held session's unresolved operation includes this session",
                )
                .into());
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
                    return Err(inconsistent(
                        sid,
                        "another held session holds this reference for a different operation",
                    )
                    .into());
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
        // A restored reference covers sessions this process may not hold yet;
        // each needs it until it retires here or a later confirmed collection
        // supersedes it. The installed session names it again, so it leaves
        // the retirement queue; a retire task already under way checks again
        // before its call. One restored without a transaction hash was not
        // looked up in the wallet, so its hand-over takes the wallet check
        // until a wallet answer matches it.
        for operation in [&candidate.operation, &candidate.baseline]
            .into_iter()
            .flatten()
        {
            let reference = operation.result.operation_ref();
            state.retirable.retain(|(queued, _)| queued != reference);
            if operation.result.tx_hash().is_none() {
                state
                    .unconfirmed
                    .insert(reference.to_owned(), operation.result.as_ref().clone());
            } else {
                state.unconfirmed.remove(reference);
            }
            state
                .references
                .entry(operation.result.operation_ref().to_owned())
                .or_insert_with(|| {
                    operation
                        .prepared
                        .session_ids()
                        .into_iter()
                        .map(|id| (id, None))
                        .collect()
                })
                .insert(sid, None);
        }
        state.sessions.insert(sid, candidate);
        Ok(view)
    }
}
