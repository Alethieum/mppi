//! Atomic admission and accounting; no provider callbacks or awaits under state.
use super::*;

/// Candidate Control binding used only while staging a reconnect snapshot.
pub(super) struct ReconnectState<'a> {
    pub(super) id: u64,
    pub(super) generation: u64,
    pub(super) voucher: &'a Option<proto::Voucher>,
}

fn add_counts(left: TokenCounts, right: TokenCounts) -> Result<TokenCounts, Status> {
    let add = |a: u64, b: u64| {
        a.checked_add(b)
            .ok_or_else(|| Status::resource_exhausted("price-group token count overflow"))
    };
    TokenCounts::new(
        add(left.input_tokens(), right.input_tokens())?,
        add(left.cache_tokens(), right.cache_tokens())?,
        add(left.output_tokens(), right.output_tokens())?,
    )
    .map_err(validation_status)
}
fn run_status(status: RunStatus) -> i32 {
    (match status {
        RunStatus::Completed => proto::RunStatus::Completed,
        RunStatus::Failed => proto::RunStatus::Failed,
        RunStatus::Cancelled => proto::RunStatus::Cancelled,
        RunStatus::NotAdmitted => proto::RunStatus::NotAdmitted,
    }) as i32
}
fn session_status(status: SessionStatus) -> i32 {
    (match status {
        SessionStatus::Open => proto::SessionStatus::Open,
        SessionStatus::Closing => proto::SessionStatus::Closing,
        SessionStatus::Closed => proto::SessionStatus::Closed,
    }) as i32
}

impl Runtime {
    pub(super) fn snapshot_locked(&self, state: &State) -> Result<Arc<ChainSnapshot>, Status> {
        if !state.started || state.stopping {
            return Err(not_ready("CONTROL_NOT_READY", None, None));
        }
        self.chain
            .snapshot()
            .map_err(|_| not_ready("CHAIN_STATE_STALE", None, None))
    }

    pub(super) fn gate_locked(
        &self,
        state: &State,
        session: &Session,
        context: Option<&RunContext>,
    ) -> Result<Arc<ChainSnapshot>, Status> {
        let fail = |reason| not_ready(reason, Some(session.sid), context.map(RunContext::run_id));
        if !state.started || state.stopping {
            return Err(fail("CONTROL_NOT_READY"));
        }
        let snapshot = self
            .chain
            .snapshot()
            .map_err(|_| fail("CHAIN_STATE_STALE"))?;
        self.gate_for_snapshot_locked(state, session, context, &snapshot, None)?;
        Ok(snapshot)
    }

    fn gate_for_snapshot_locked(
        &self,
        state: &State,
        session: &Session,
        context: Option<&RunContext>,
        snapshot: &ChainSnapshot,
        replacement: Option<(u64, u64)>,
    ) -> Result<(), Status> {
        let sid = Some(session.sid);
        let rid = context.map(RunContext::run_id);
        let fail = |reason| not_ready(reason, sid, rid);
        if !state.started || state.stopping {
            return Err(fail("CONTROL_NOT_READY"));
        }
        let readiness = self.chain.readiness_for_snapshot(snapshot, sid);
        if let Some(failure) = readiness.failures().first() {
            let scoped = PaymentFailure::new(
                failure.code(),
                failure.reason().map(str::to_owned),
                sid,
                rid,
                failure.requested_rate_card_hash(),
                failure.current_rate_card_hash(),
                failure.pause_level(),
            )
            .map_err(validation_status)?;
            return Err(wire::payment_status_with_limits(&scoped, &self.limits.wire));
        }
        if !session.history_available {
            return Err(fail("STATE_UNAVAILABLE"));
        }
        if session.inconsistent {
            return Err(fail("INCONSISTENT_USAGE"));
        }
        if session.runs.values().any(|run| run.unresolved) {
            return Err(fail("STATE_UNAVAILABLE"));
        }
        let chain = snapshot
            .sessions()
            .get(&session.sid)
            .ok_or_else(|| fail("STATE_UNAVAILABLE"))?;
        if session.usage().map_err(validation_status)? < chain.settled_cumulative() {
            return Err(fail("INCONSISTENT_STATE"));
        }
        if session.close_pending {
            return Err(fail("CLOSE_PENDING"));
        }
        let (owner_id, generation) = replacement
            .map(|(id, generation)| (Some(id), generation))
            .unwrap_or((session.owner, session.generation));
        let owner = owner_id
            .and_then(|id| state.attachments.get(&id))
            .ok_or_else(|| fail("CONTROL_NOT_READY"))?;
        let binding_matches = if replacement.is_some() {
            owner.sid.is_none_or(|bound| bound == session.sid)
        } else {
            owner.generation == generation && owner.sid == sid
        };
        if !owner.live
            || !binding_matches
            || context.is_some_and(|ctx| ctx.control_generation() != generation)
        {
            return Err(fail("CONTROL_NOT_READY"));
        }
        if session
            .active()
            .any(|run| run.context.control_generation() != generation)
        {
            return Err(fail("STATE_UNAVAILABLE"));
        }
        Ok(())
    }

    pub(super) fn prices_locked(
        &self,
        session: &Session,
        context: &RunContext,
        snapshot: &ChainSnapshot,
    ) -> Result<Rates, Status> {
        let catalog = snapshot
            .identities()
            .get(&self.config.identity().provider_id())
            .and_then(|identity| identity.catalog())
            .ok_or_else(|| {
                not_ready(
                    "CHAIN_STATE_STALE",
                    Some(session.sid),
                    Some(context.run_id()),
                )
            })?;
        if catalog.rate_card_hash() != context.rate_card_hash() {
            return Err(wire::payment_status_with_limits(
                &PaymentFailure::new(
                    PaymentCode::SessionNotReady,
                    Some("PRICE_CHANGED".into()),
                    Some(session.sid),
                    Some(context.run_id()),
                    Some(context.rate_card_hash()),
                    Some(catalog.rate_card_hash()),
                    None,
                )
                .map_err(validation_status)?,
                &self.limits.wire,
            ));
        }
        catalog
            .rates_for(context.model())
            .copied()
            .ok_or_else(|| Status::not_found("model unavailable"))
    }

    pub(super) fn send_locked(
        &self,
        state: &mut State,
        id: u64,
        message: proto::ControlMessage,
    ) -> Result<(), Status> {
        wire::validate_message_with_limits(&message, &self.limits.wire)
            .map_err(validation_status)?;
        let attachment = state
            .attachments
            .get(&id)
            .ok_or_else(|| Status::unavailable("Control detached"))?;
        if !attachment.live {
            return Err(Status::unavailable("Control detached"));
        }
        if attachment
            .queue
            .put_nowait(&message.encode_to_vec())
            .is_err()
        {
            self.detach_locked(state, id);
            if let Some(attachment) = state.attachments.get(&id) {
                let _ = attachment
                    .queue
                    .close(Some(Status::resource_exhausted("Control queue limit")));
            }
            return Err(Status::resource_exhausted("Control queue limit"));
        }
        match &message.kind {
            Some(proto::control_message::Kind::Challenge(value)) => {
                self.observers.emit(hooks::Event::Challenge(value.clone()))
            }
            Some(proto::control_message::Kind::LowCredit(value)) => {
                self.observers.emit(hooks::Event::LowCredit(value.clone()))
            }
            Some(proto::control_message::Kind::Drain(value)) => {
                self.observers.emit(hooks::Event::Drain(value.clone()))
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn detach_locked(&self, state: &mut State, id: u64) {
        let sid = state.attachments.get_mut(&id).and_then(|attachment| {
            attachment.live = false;
            attachment.sid
        });
        if let Some(session) = sid.and_then(|sid| state.sessions.get(&sid))
            && session.owner == Some(id)
        {
            for run in session.active() {
                run.stop.cancel();
            }
        }
        self.changed.notify_waiters();
    }

    pub(super) fn stop_session_locked(&self, state: &mut State, sid: B256) {
        let owner = state.sessions.get(&sid).and_then(|session| {
            for run in session.active() {
                run.stop.cancel();
            }
            session.owner
        });
        if let Some(owner) = owner {
            let _ = self.send_locked(
                state,
                owner,
                control(proto::control_message::Kind::Drain(proto::Drain {
                    session_id: Some(sid.to_vec()),
                    run_id: None,
                    reason: Some(proto::DrainReason::ProviderInitiated as i32),
                })),
            );
        }
        self.changed.notify_waiters();
    }

    pub(super) fn begin_run(&self, request: proto::CompleteRequest) -> Result<RunHandle, Status> {
        wire::validate_message_with_limits(&request, &self.limits.wire)
            .map_err(validation_status)?;
        // Wire validation above establishes every required field and width.
        let context = RunContext::new(
            B256::from_slice(request.session_id.as_deref().unwrap()),
            B256::from_slice(request.run_id.as_deref().unwrap()),
            request.control_generation.unwrap(),
            request.model.unwrap(),
            B256::from_slice(request.rate_card_hash.as_deref().unwrap()),
        )
        .map_err(validation_status)?;
        let signature: [u8; 65] = request
            .auth_signature
            .unwrap()
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid run proof length"))?;
        let prepared = PreparedRequest::with_limits(
            request.payload.unwrap(),
            request.stream.unwrap(),
            &context,
            &self.limits.wire,
        )
        .map_err(validation_status)?;
        let mut state = self.lock()?;
        let session = state
            .sessions
            .get(&context.session_id())
            .ok_or_else(|| Status::unauthenticated("session authority unavailable"))?;
        self.contract
            .domain()
            .signing_context()
            .verify(
                session.opening.descriptor(),
                &crate::signing::RunAuth {
                    sessionId: context.session_id(),
                    controlGeneration: context.control_generation(),
                    runId: context.run_id(),
                    model: context.model().to_owned(),
                    rateCardHash: context.rate_card_hash(),
                },
                &signature,
            )
            .map_err(|_| Status::unauthenticated("invalid run proof"))?;
        if session.runs.contains_key(&context.run_id()) {
            return Err(Status::already_exists("run id already observed"));
        }
        let snapshot = self.gate_locked(&state, session, Some(&context))?;
        let rates = self.prices_locked(session, &context, &snapshot)?;
        let retained = state
            .retained_request_bytes
            .checked_add(prepared.payload().len())
            .filter(|bytes| *bytes <= self.limits.wire.message_bytes())
            .ok_or_else(|| Status::resource_exhausted("retained request limit"))?;
        let stop = CancellationToken::new();
        let output = Arc::new(
            ByteQueue::new(
                64,
                self.limits.wire.message_bytes(),
                self.config.payment().authorization_wait_ms(),
            )
            .map_err(validation_status)?,
        );
        let run = Run {
            context: context.clone(),
            signature,
            rates,
            accounting: RunAccounting::new(zero_counts(), U256::ZERO, None)
                .map_err(validation_status)?,
            covered_counts: zero_counts(),
            phase: RunPhase::Preparing,
            stop: stop.clone(),
            terminal: None,
            meter_seq: 0,
            last_low_credit: None,
            unresolved: false,
            retained_bytes: prepared.payload().len(),
        };
        let identity = snapshot
            .identities()
            .get(&self.config.identity().provider_id())
            .ok_or_else(|| {
                not_ready(
                    "CHAIN_STATE_STALE",
                    Some(session.sid),
                    Some(context.run_id()),
                )
            })?;
        let catalog = identity.catalog().ok_or_else(|| {
            not_ready(
                "CHAIN_STATE_STALE",
                Some(session.sid),
                Some(context.run_id()),
            )
        })?;
        self.history_fits_locked(session, Some(&run), Some(catalog))?;
        let session = state.sessions.get_mut(&context.session_id()).unwrap();
        session
            .catalogs
            .insert(catalog.rate_card_hash(), catalog.clone());
        session
            .publications
            .insert(catalog.rate_card_hash(), *identity.observation());
        session.runs.insert(context.run_id(), run);
        state.retained_request_bytes = retained;
        Ok(RunHandle {
            context,
            request: prepared,
            stop,
            output,
        })
    }

    #[cfg(test)]
    pub(super) fn attempt_admission(
        &self,
        handle: &RunHandle,
        bound: TokenCounts,
    ) -> Result<Admission, Status> {
        self.attempt_admission_before(handle, bound, None)
    }

    pub(super) fn attempt_admission_before(
        &self,
        handle: &RunHandle,
        bound: TokenCounts,
        expires: Option<Instant>,
    ) -> Result<Admission, Status> {
        if bound.output_tokens() < self.config.payment().output_buffer_tokens() {
            return Err(Status::invalid_argument(
                "provider must supply configured output coverage",
            ));
        }
        let mut state = self.lock()?;
        let context = &handle.context;
        let timely = || {
            if expires.is_some_and(|end| Instant::now() >= end) {
                Err(not_ready(
                    "AUTHORIZATION_REQUIRED",
                    Some(context.session_id()),
                    Some(context.run_id()),
                ))
            } else {
                Ok(())
            }
        };
        timely()?;
        let session = state.sessions.get(&context.session_id()).ok_or_else(|| {
            not_ready(
                "STATE_UNAVAILABLE",
                Some(context.session_id()),
                Some(context.run_id()),
            )
        })?;
        let run = session
            .runs
            .get(&context.run_id())
            .filter(|run| &run.context == context)
            .ok_or_else(|| Status::invalid_argument("unknown prepared run"))?;
        if run.phase != RunPhase::Preparing || run.stop.is_cancelled() {
            return Err(not_ready(
                "CONTROL_NOT_READY",
                Some(session.sid),
                Some(context.run_id()),
            ));
        }
        let snapshot = self.gate_locked(&state, session, Some(context))?;
        self.prices_locked(session, context, &snapshot)?;
        self.history_fits_locked(session, None, None)?;
        let (rows, required) =
            self.required_rows_locked(session, Some((context.run_id(), bound)))?;
        let guidance = self.guidance_locked(session, run, rows, &snapshot)?;
        let deposit = snapshot.sessions().get(&session.sid).unwrap().deposit();
        if required > deposit {
            let owner = session.owner.unwrap();
            self.send_locked(&mut state, owner, guidance)?;
            return Err(wire::payment_status_with_limits(
                &PaymentFailure::new(
                    PaymentCode::InsufficientDeposit,
                    None,
                    Some(context.session_id()),
                    Some(context.run_id()),
                    None,
                    None,
                    None,
                )
                .map_err(validation_status)?,
                &self.limits.wire,
            ));
        }
        if session
            .voucher
            .as_ref()
            .and_then(|voucher| voucher.cumulative)
            .is_none_or(|amount| amount < required)
        {
            return Ok(Admission::Wait(Box::new(guidance)));
        }
        if session.active().count() >= self.limits.wire.pending_runs() {
            return Err(Status::resource_exhausted("active run recovery limit"));
        }
        let accounting = RunAccounting::new(
            zero_counts(),
            priced_numerator(bound, run.rates).map_err(validation_status)?,
            None,
        )
        .map_err(validation_status)?;
        // Check again after fallible admission work, under the state lock and
        // immediately before committing a reservation. A late voucher cannot
        // turn an expired authorization wait into a grant.
        timely()?;
        let run = state
            .sessions
            .get_mut(&context.session_id())
            .unwrap()
            .runs
            .get_mut(&context.run_id())
            .unwrap();
        run.covered_counts = bound;
        run.accounting = accounting;
        run.phase = RunPhase::Admitted;
        self.changed.notify_waiters();
        Ok(Admission::Granted)
    }

    pub(super) fn recheck_dispatch(&self, context: &RunContext) -> Result<(), Status> {
        let mut state = self.lock()?;
        let (session, run) = self.run_locked(&state, context)?;
        self.gate_locked(&state, session, Some(context))?;
        if run.phase != RunPhase::Admitted || run.stop.is_cancelled() {
            return Err(not_ready(
                "CONTROL_NOT_READY",
                Some(context.session_id()),
                Some(context.run_id()),
            ));
        }
        state
            .sessions
            .get_mut(&context.session_id())
            .unwrap()
            .runs
            .get_mut(&context.run_id())
            .unwrap()
            .phase = RunPhase::Dispatched;
        Ok(())
    }

    pub(super) fn cancel_run(&self, context: &RunContext) {
        if let Ok(mut state) = self.lock() {
            let mut released = 0;
            if let Some(run) = state
                .sessions
                .get_mut(&context.session_id())
                .and_then(|s| s.runs.get_mut(&context.run_id()))
                && &run.context == context
            {
                run.stop.cancel();
                if run.phase == RunPhase::Preparing {
                    run.phase = RunPhase::NotAdmitted;
                    released = std::mem::take(&mut run.retained_bytes);
                }
            }
            state.retained_request_bytes -= released;
            self.changed.notify_waiters();
        }
    }

    pub(super) fn finish_stream(
        &self,
        context: &RunContext,
    ) -> Result<Option<proto::Terminal>, Status> {
        let mut state = self.lock()?;
        let run = state
            .sessions
            .get_mut(&context.session_id())
            .and_then(|s| s.runs.get_mut(&context.run_id()))
            .filter(|run| &run.context == context)
            .ok_or_else(|| Status::invalid_argument("unknown run"))?;
        if run.phase == RunPhase::Preparing {
            run.phase = RunPhase::NotAdmitted;
        }
        if run.active() {
            run.unresolved = true;
            run.stop.cancel();
        }
        let terminal = run.terminal.clone();
        let released = std::mem::take(&mut run.retained_bytes);
        state.retained_request_bytes -= released;
        self.changed.notify_waiters();
        Ok(terminal)
    }

    pub(super) fn run_locked<'a>(
        &self,
        state: &'a State,
        context: &RunContext,
    ) -> Result<(&'a Session, &'a Run), Status> {
        let session = state
            .sessions
            .get(&context.session_id())
            .ok_or_else(|| Status::invalid_argument("report has no matching admitted run"))?;
        let run = session
            .runs
            .get(&context.run_id())
            .filter(|run| run.admitted() && &run.context == context)
            .ok_or_else(|| Status::invalid_argument("report has no matching admitted run"))?;
        Ok((session, run))
    }

    pub(super) fn required_rows_locked(
        &self,
        session: &Session,
        candidate: Option<(B256, TokenCounts)>,
    ) -> Result<(Vec<proto::RequiredTokens>, u64), Status> {
        let mut grouped = BTreeMap::<(String, B256), (TokenCounts, Rates)>::new();
        for run in session.runs.values() {
            let counts = if let Some((id, bound)) =
                candidate.filter(|(id, _)| *id == run.context.run_id())
            {
                let _ = id;
                bound
            } else if run.admitted() {
                if run.accounting.final_status().is_some() {
                    run.accounting.counts()
                } else {
                    run.covered_counts
                }
            } else {
                continue;
            };
            let entry = grouped
                .entry((run.context.model().to_owned(), run.context.rate_card_hash()))
                .or_insert((zero_counts(), run.rates));
            entry.0 = add_counts(entry.0, counts)?;
        }
        let numerators = grouped
            .values()
            .map(|(counts, rates)| priced_numerator(*counts, *rates))
            .collect::<Result<Vec<_>, _>>()
            .map_err(validation_status)?;
        let amount = required_coverage(numerators).map_err(validation_status)?;
        let rows = grouped
            .into_iter()
            .map(|((model, hash), (counts, _))| proto::RequiredTokens {
                model: Some(model),
                rate_card_hash: Some(hash.to_vec()),
                input_tokens: Some(counts.input_tokens()),
                cache_tokens: Some(counts.cache_tokens()),
                output_tokens: Some(counts.output_tokens()),
            })
            .collect();
        Ok((rows, amount))
    }

    pub(super) fn guidance_locked(
        &self,
        session: &Session,
        run: &Run,
        rows: Vec<proto::RequiredTokens>,
        snapshot: &ChainSnapshot,
    ) -> Result<proto::ControlMessage, Status> {
        let deposit = snapshot
            .sessions()
            .get(&session.sid)
            .ok_or_else(|| {
                not_ready(
                    "STATE_UNAVAILABLE",
                    Some(session.sid),
                    Some(run.context.run_id()),
                )
            })?
            .deposit();
        let available = remaining_funding_tokens(
            deposit,
            session.usage_numerator().map_err(validation_status)?,
            session
                .active()
                .map(|item| item.accounting.remaining_numerator()),
            run.rates,
        )
        .map_err(validation_status)?;
        let result = control(proto::control_message::Kind::LowCredit(proto::LowCredit {
            session_id: Some(session.sid.to_vec()),
            model: Some(run.context.model().to_owned()),
            rate_card_hash: Some(run.context.rate_card_hash().to_vec()),
            remaining_tokens: available,
            suggest_top_up_tokens: available
                .map(|_| self.config.payment().suggest_session_tokens()),
            coverage_request: Some(proto::CoverageRequest {
                run_id: Some(run.context.run_id().to_vec()),
                control_generation: Some(run.context.control_generation()),
                required_by_price: rows,
            }),
        }));
        wire::validate_message_with_limits(&result, &self.limits.wire)
            .map_err(validation_status)?;
        Ok(result)
    }

    pub(super) fn require_coverage_sync(
        &self,
        context: &RunContext,
        remaining_tokens: TokenCounts,
    ) -> Result<CoverageResult, Status> {
        let mut state = self.lock()?;
        let (session, run) = self.run_locked(&state, context)?;
        let existing_required = required_coverage(
            std::iter::once(session.usage_numerator().map_err(validation_status)?).chain(
                session
                    .active()
                    .map(|item| item.accounting.remaining_numerator()),
            ),
        )
        .map_err(validation_status)?;
        let checked: Result<_, Status> = (|| {
            let snapshot = self.gate_locked(&state, session, Some(context))?;
            if run.stop.is_cancelled() || run.accounting.final_status().is_some() {
                return Err(not_ready(
                    "CONTROL_NOT_READY",
                    Some(session.sid),
                    Some(context.run_id()),
                ));
            }
            let remaining =
                priced_numerator(remaining_tokens, run.rates).map_err(validation_status)?;
            if remaining <= run.accounting.remaining_numerator() {
                return Ok((existing_required, None));
            }
            let bound = add_counts(run.accounting.counts(), remaining_tokens)?;
            let (rows, required) =
                self.required_rows_locked(session, Some((context.run_id(), bound)))?;
            // Retain full wire-representation validation. Admitted work renews
            // from Meters; this pending-request guidance is deliberately not sent.
            self.guidance_locked(session, run, rows, &snapshot)?;
            if required > snapshot.sessions().get(&session.sid).unwrap().deposit() {
                return Err(wire::payment_status_with_limits(
                    &PaymentFailure::new(
                        PaymentCode::InsufficientDeposit,
                        None,
                        Some(session.sid),
                        Some(context.run_id()),
                        None,
                        None,
                        None,
                    )
                    .map_err(validation_status)?,
                    &self.limits.wire,
                ));
            }
            if session
                .voucher
                .as_ref()
                .and_then(|voucher| voucher.cumulative)
                .is_none_or(|amount| amount < required)
            {
                return Err(not_ready(
                    "AUTHORIZATION_REQUIRED",
                    Some(session.sid),
                    Some(context.run_id()),
                ));
            }
            let accounting = RunAccounting::new(run.accounting.counts(), remaining, None)
                .map_err(validation_status)?;
            Ok((required, Some((bound, accounting))))
        })();
        match checked {
            Ok((required, update)) => {
                if let Some((bound, accounting)) = update {
                    let run = state
                        .sessions
                        .get_mut(&context.session_id())
                        .unwrap()
                        .runs
                        .get_mut(&context.run_id())
                        .unwrap();
                    run.covered_counts = bound;
                    run.accounting = accounting;
                }
                CoverageResult::new(true, required, None).map_err(validation_status)
            }
            Err(status) => {
                run.stop.cancel();
                let failure = wire::parse_payment_error(&status)
                    .map_err(validation_status)?
                    .unwrap_or(
                        PaymentFailure::new(
                            PaymentCode::SessionNotReady,
                            Some("CONTROL_NOT_READY".into()),
                            Some(context.session_id()),
                            Some(context.run_id()),
                            None,
                            None,
                            None,
                        )
                        .map_err(validation_status)?,
                    );
                let reason = if failure.code() == PaymentCode::InsufficientDeposit {
                    proto::DrainReason::DepositExhausted
                } else if failure.reason() == Some("AUTHORIZATION_REQUIRED") {
                    proto::DrainReason::AuthorizationExhausted
                } else {
                    proto::DrainReason::ProviderInitiated
                };
                let owner = session.owner;
                if let Some(owner) = owner {
                    let _ = self.send_locked(
                        &mut state,
                        owner,
                        control(proto::control_message::Kind::Drain(proto::Drain {
                            session_id: Some(context.session_id().to_vec()),
                            run_id: Some(context.run_id().to_vec()),
                            reason: Some(reason as i32),
                        })),
                    );
                }
                CoverageResult::new(false, existing_required, Some(failure))
                    .map_err(validation_status)
            }
        }
    }

    pub(super) fn report_sync(
        &self,
        context: &RunContext,
        counts: TokenCounts,
        status: Option<RunStatus>,
    ) -> Result<AccountingResult, Status> {
        let mut state = self.lock()?;
        let (session, run) = self.run_locked(&state, context)?;
        let calculated: Result<_, Status> = (|| {
            let transition = report_transition(&run.accounting, counts, run.rates, status)
                .map_err(validation_status)?;
            // Reports can change free token counters without changing exact cost.
            // Their price-group totals still have to fit the uint64 wire fields.
            let mut group_counts = zero_counts();
            for item in session.runs.values().filter(|item| {
                item.admitted()
                    && item.context.model() == context.model()
                    && item.context.rate_card_hash() == context.rate_card_hash()
            }) {
                group_counts = add_counts(
                    group_counts,
                    if item.context.run_id() == context.run_id() {
                        counts
                    } else {
                        item.accounting.counts()
                    },
                )?;
            }
            if status.is_some() {
                // A definitive final replaces this run's bound in every future
                // coverage target. Free counters must not make that target
                // impossible to encode while other work is still reserved.
                self.required_rows_locked(session, Some((context.run_id(), counts)))?;
            }
            let total = session
                .usage_numerator()
                .map_err(validation_status)?
                .checked_add(transition.delta_numerator())
                .ok_or_else(|| Status::invalid_argument("session numerator overflow"))?;
            let usage = cumulative_usage([total]).map_err(validation_status)?;
            if transition.duplicate() && run.meter_seq > 0 {
                return Ok((
                    AccountingResult::new(context.clone(), counts, run.meter_seq, usage, true)
                        .map_err(validation_status)?,
                    None,
                ));
            }
            let seq = session
                .meter_seq
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("meter sequence exhausted"))?;
            let meter = control(proto::control_message::Kind::Meter(proto::Meter {
                session_id: Some(session.sid.to_vec()),
                run_id: Some(context.run_id().to_vec()),
                seq: Some(seq),
                input_tokens: Some(counts.input_tokens()),
                cache_tokens: Some(counts.cache_tokens()),
                output_tokens: Some(counts.output_tokens()),
            }));
            wire::validate_message_with_limits(&meter, &self.limits.wire)
                .map_err(validation_status)?;
            let mut low_credit = None;
            let mut low_credit_marker = run.last_low_credit;
            if status.is_none()
                && !run.stop.is_cancelled()
                && let Ok(snapshot) = self.gate_locked(&state, session, Some(context))
            {
                let deposit = snapshot.sessions()[&session.sid].deposit();
                let reservations = session
                    .active()
                    .map(|item| {
                        if item.context.run_id() == context.run_id() {
                            transition.state().remaining_numerator()
                        } else {
                            item.accounting.remaining_numerator()
                        }
                    })
                    .collect::<Vec<_>>();
                let committed = reservations.iter().try_fold(total, |sum, remaining| {
                    sum.checked_add(*remaining)
                        .ok_or_else(|| Status::invalid_argument("session numerator overflow"))
                })?;
                // Both products fit U256: their operands are bounded uint64
                // amounts/rates and fixed protocol constants.
                let funded = U256::from(deposit) * U256::from(crate::amounts::RATE_SCALE);
                let available = funded.saturating_sub(committed);
                // Two ordinary 1,000-output-token meter intervals. Execution
                // buffer configuration does not change this warning threshold.
                let threshold = U256::from(run.rates.output()) * U256::from(2000);
                if available >= threshold {
                    low_credit_marker = None;
                } else {
                    let marker = (deposit, available);
                    if run.last_low_credit != Some(marker) {
                        let remaining =
                            remaining_funding_tokens(deposit, total, reservations, run.rates)
                                .map_err(validation_status)?;
                        let advice =
                            control(proto::control_message::Kind::LowCredit(proto::LowCredit {
                                session_id: Some(session.sid.to_vec()),
                                model: Some(context.model().to_owned()),
                                rate_card_hash: Some(context.rate_card_hash().to_vec()),
                                remaining_tokens: remaining,
                                suggest_top_up_tokens: remaining
                                    .map(|_| self.config.payment().suggest_session_tokens()),
                                coverage_request: None,
                            }));
                        wire::validate_message_with_limits(&advice, &self.limits.wire)
                            .map_err(validation_status)?;
                        low_credit = Some(advice);
                    }
                    low_credit_marker = Some(marker);
                }
            }
            let mut terminal = None;
            let mut receipt = None;
            if let Some(status) = status {
                let amount = cumulative_usage([
                    priced_numerator(counts, run.rates).map_err(validation_status)?
                ])
                .map_err(validation_status)?;
                let final_record = proto::Terminal {
                    input_tokens: Some(counts.input_tokens()),
                    cache_tokens: Some(counts.cache_tokens()),
                    output_tokens: Some(counts.output_tokens()),
                    amount: Some(amount),
                    session_id: Some(session.sid.to_vec()),
                    run_id: Some(context.run_id().to_vec()),
                    model: Some(context.model().to_owned()),
                    rate_card_hash: Some(context.rate_card_hash().to_vec()),
                    status: Some(run_status(status)),
                    final_meter_seq: Some(seq),
                    settlement_reference: None,
                };
                wire::validate_message_with_limits(&final_record, &self.limits.wire)
                    .map_err(validation_status)?;
                terminal = Some(final_record);
                if let Some(snapshot) = self.chain.current()
                    && let Some(chain) = snapshot.sessions().get(&session.sid)
                {
                    let message = control(proto::control_message::Kind::Receipt(proto::Receipt {
                        session_id: Some(session.sid.to_vec()),
                        run_id: Some(context.run_id().to_vec()),
                        model: Some(context.model().to_owned()),
                        rate_card_hash: Some(context.rate_card_hash().to_vec()),
                        input_tokens: Some(counts.input_tokens()),
                        cache_tokens: Some(counts.cache_tokens()),
                        output_tokens: Some(counts.output_tokens()),
                        amount: Some(amount),
                        status: Some(run_status(status)),
                        final_meter_seq: Some(seq),
                        settled_cumulative: Some(chain.settled_cumulative()),
                        chain_observation: Some(wire::observation_to_wire(
                            snapshot.block().observation(),
                        )),
                        settlement_reference: None,
                    }));
                    wire::validate_message_with_limits(&message, &self.limits.wire)
                        .map_err(validation_status)?;
                    receipt = Some(message);
                }
            }
            let result = AccountingResult::new(context.clone(), counts, seq, usage, false)
                .map_err(validation_status)?;
            Ok((
                result,
                Some((
                    transition.state(),
                    meter,
                    terminal,
                    receipt,
                    low_credit_marker,
                    low_credit,
                )),
            ))
        })();
        let (result, commit) = match calculated {
            Ok(result) => result,
            Err(error) => {
                state
                    .sessions
                    .get_mut(&context.session_id())
                    .unwrap()
                    .inconsistent = true;
                self.stop_session_locked(&mut state, context.session_id());
                return Err(error);
            }
        };
        if let Some((accounting, meter, terminal, receipt, low_credit_marker, low_credit)) = commit
        {
            let session = state.sessions.get_mut(&context.session_id()).unwrap();
            let run = session.runs.get_mut(&context.run_id()).unwrap();
            run.accounting = accounting;
            run.meter_seq = result.meter_seq();
            run.last_low_credit = low_credit_marker;
            session.meter_seq = result.meter_seq();
            if terminal.is_some() {
                run.terminal = terminal;
                run.unresolved = false;
            }
            if let Some(proto::control_message::Kind::Meter(value)) = &meter.kind {
                self.observers.emit(hooks::Event::Meter(value.clone()));
            }
            let owner = session.owner;
            if let Some(owner) = owner {
                let _ = self.send_locked(&mut state, owner, meter);
                if let Some(low_credit) = low_credit {
                    let _ = self.send_locked(&mut state, owner, low_credit);
                }
                if let Some(receipt) = receipt {
                    let _ = self.send_locked(&mut state, owner, receipt);
                }
            }
            self.changed.notify_waiters();
        }
        Ok(result)
    }

    pub(super) fn history_fits_locked(
        &self,
        session: &Session,
        candidate: Option<&Run>,
        current: Option<&Catalog>,
    ) -> Result<(), Status> {
        let mut record = proto::SessionState {
            session_id: Some(session.sid.to_vec()),
            nonce: Some(vec![0; 32]),
            control_generation: Some(u64::MAX),
            ready: Some(false),
            close_pending: Some(false),
            descriptor: Some(
                wire::descriptor_to_wire(session.opening.descriptor())
                    .map_err(validation_status)?,
            ),
            meter_seq: Some(u64::MAX),
            initial_rate_card_hash: Some(
                session.opening.initial_catalog().rate_card_hash().to_vec(),
            ),
            current_rate_card_hash: Some(vec![0; 32]),
            deposit: Some(u64::MAX),
            settled_cumulative: Some(u64::MAX),
            status: Some(proto::SessionStatus::Open as i32),
            chain_block_number: Some(u64::MAX),
            chain_block_hash: Some(vec![0; 32]),
            accepted_voucher: Some(proto::Voucher {
                session_id: Some(session.sid.to_vec()),
                cumulative: Some(u64::MAX),
                signature: Some(vec![0; 65]),
            }),
            ..Default::default()
        };
        let mut catalogs = session
            .catalogs
            .iter()
            .map(|(hash, catalog)| (*hash, catalog))
            .collect::<BTreeMap<_, _>>();
        if let Some(current) = current {
            catalogs.insert(current.rate_card_hash(), current);
        }
        record.catalogs = catalogs
            .into_iter()
            .map(|(hash, catalog)| proto::CatalogVersion {
                rate_card_hash: Some(hash.to_vec()),
                catalog: Some(catalog.canonical_bytes().to_vec()),
            })
            .collect();
        let mut groups = std::collections::BTreeSet::new();
        for run in session.runs.values().chain(candidate) {
            record.run_states.push(proto::RunState {
                run_id: Some(run.context.run_id().to_vec()),
                model: Some(run.context.model().to_owned()),
                rate_card_hash: Some(run.context.rate_card_hash().to_vec()),
                control_generation: Some(u64::MAX),
                auth_signature: Some(run.signature.to_vec()),
                status: Some(proto::RunStatus::Completed as i32),
                input_tokens: Some(u64::MAX),
                cache_tokens: Some(u64::MAX),
                output_tokens: Some(u64::MAX),
            });
            if groups.insert((run.context.model(), run.context.rate_card_hash())) {
                record.usage_by_price.push(proto::PricedUsage {
                    model: Some(run.context.model().to_owned()),
                    rate_card_hash: Some(run.context.rate_card_hash().to_vec()),
                    input_tokens: Some(u64::MAX),
                    cache_tokens: Some(u64::MAX),
                    output_tokens: Some(u64::MAX),
                    acceptance_run_id: Some(run.context.run_id().to_vec()),
                    acceptance_control_generation: Some(u64::MAX),
                    acceptance_signature: Some(run.signature.to_vec()),
                });
            }
            if record.encoded_len() > self.limits.wire.message_bytes() {
                return Err(Status::resource_exhausted(
                    "complete recovery history limit",
                ));
            }
        }
        if control(proto::control_message::Kind::SessionState(record)).encoded_len()
            > self.limits.wire.message_bytes()
        {
            return Err(Status::resource_exhausted(
                "complete recovery history limit",
            ));
        }
        Ok(())
    }

    pub(super) fn session_state_locked(
        &self,
        state: &State,
        session: &Session,
        nonce: &[u8],
        private: bool,
    ) -> Result<proto::SessionState, Status> {
        self.session_state_for_attachment_locked(state, session, nonce, private, None)
    }

    pub(super) fn session_state_for_attachment_locked(
        &self,
        state: &State,
        session: &Session,
        nonce: &[u8],
        private: bool,
        replacement: Option<ReconnectState<'_>>,
    ) -> Result<proto::SessionState, Status> {
        let snapshot = self.snapshot_locked(state)?;
        let chain = snapshot
            .sessions()
            .get(&session.sid)
            .ok_or_else(|| not_ready("STATE_UNAVAILABLE", Some(session.sid), None))?;
        if !private && session.active().next().is_some() {
            return Err(not_ready("STATE_UNAVAILABLE", Some(session.sid), None));
        }
        let mut result = proto::SessionState {
            session_id: Some(session.sid.to_vec()),
            nonce: Some(nonce.to_vec()),
            control_generation: Some(
                replacement
                    .as_ref()
                    .map_or(session.generation, |binding| binding.generation),
            ),
            ready: Some(
                !private
                    && self
                        .gate_for_snapshot_locked(
                            state,
                            session,
                            None,
                            &snapshot,
                            replacement
                                .as_ref()
                                .map(|binding| (binding.id, binding.generation)),
                        )
                        .is_ok(),
            ),
            close_pending: Some(session.close_pending),
            descriptor: Some(
                wire::descriptor_to_wire(session.opening.descriptor())
                    .map_err(validation_status)?,
            ),
            accepted_voucher: replacement
                .as_ref()
                .map_or(&session.voucher, |binding| binding.voucher)
                .clone(),
            meter_seq: Some(session.meter_seq),
            initial_rate_card_hash: Some(
                session.opening.initial_catalog().rate_card_hash().to_vec(),
            ),
            deposit: Some(chain.deposit()),
            settled_cumulative: Some(chain.settled_cumulative()),
            status: Some(session_status(chain.status())),
            chain_block_number: Some(snapshot.block().observation().block_number()),
            chain_block_hash: Some(snapshot.block().observation().block_hash().to_vec()),
            ..Default::default()
        };
        let mut catalogs = session
            .catalogs
            .iter()
            .map(|(hash, catalog)| (*hash, catalog))
            .collect::<BTreeMap<_, _>>();
        if let Some(current) = snapshot
            .identities()
            .get(&self.config.identity().provider_id())
            .and_then(|identity| identity.catalog())
        {
            result.current_rate_card_hash = Some(current.rate_card_hash().to_vec());
            catalogs.insert(current.rate_card_hash(), current);
        }
        result.catalogs = catalogs
            .into_iter()
            .map(|(hash, catalog)| proto::CatalogVersion {
                rate_card_hash: Some(hash.to_vec()),
                catalog: Some(catalog.canonical_bytes().to_vec()),
            })
            .collect();
        let mut usage = BTreeMap::<(String, B256), (TokenCounts, &Run)>::new();
        for run in session.runs.values() {
            if run.admitted() {
                let entry = usage
                    .entry((run.context.model().to_owned(), run.context.rate_card_hash()))
                    .or_insert((zero_counts(), run));
                entry.0 = add_counts(entry.0, run.accounting.counts())?;
            }
            if run.phase == RunPhase::NotAdmitted {
                result.run_states.push(proto::RunState {
                    run_id: Some(run.context.run_id().to_vec()),
                    model: Some(String::new()),
                    status: Some(proto::RunStatus::NotAdmitted as i32),
                    input_tokens: Some(0),
                    cache_tokens: Some(0),
                    output_tokens: Some(0),
                    ..Default::default()
                });
            } else if let Some(status) = run.accounting.final_status() {
                let counts = run.accounting.counts();
                result.run_states.push(proto::RunState {
                    run_id: Some(run.context.run_id().to_vec()),
                    model: Some(run.context.model().to_owned()),
                    rate_card_hash: Some(run.context.rate_card_hash().to_vec()),
                    control_generation: Some(run.context.control_generation()),
                    auth_signature: Some(run.signature.to_vec()),
                    status: Some(run_status(status)),
                    input_tokens: Some(counts.input_tokens()),
                    cache_tokens: Some(counts.cache_tokens()),
                    output_tokens: Some(counts.output_tokens()),
                });
            }
        }
        result.usage_by_price = usage
            .into_iter()
            .map(|((model, hash), (counts, run))| proto::PricedUsage {
                model: Some(model),
                rate_card_hash: Some(hash.to_vec()),
                input_tokens: Some(counts.input_tokens()),
                cache_tokens: Some(counts.cache_tokens()),
                output_tokens: Some(counts.output_tokens()),
                acceptance_run_id: Some(run.context.run_id().to_vec()),
                acceptance_control_generation: Some(run.context.control_generation()),
                acceptance_signature: Some(run.signature.to_vec()),
            })
            .collect();
        if private {
            wire::validate_recovery_snapshot(&result, &self.limits.wire)
        } else {
            wire::validate_message_with_limits(&result, &self.limits.wire)
        }
        .map_err(validation_status)?;
        Ok(result)
    }
}

#[tonic::async_trait]
impl ProviderRuns for Mppi {
    async fn require_coverage(
        &self,
        context: &RunContext,
        remaining_tokens: TokenCounts,
    ) -> IntegrationResult<CoverageResult> {
        self.inner
            .require_coverage_sync(context, remaining_tokens)
            .map_err(Into::into)
    }
    async fn report_usage(
        &self,
        context: &RunContext,
        counts: TokenCounts,
    ) -> IntegrationResult<AccountingResult> {
        self.inner
            .report_sync(context, counts, None)
            .map_err(Into::into)
    }
    async fn report_terminal(
        &self,
        context: &RunContext,
        counts: TokenCounts,
        status: RunStatus,
    ) -> IntegrationResult<FinalAccountingResult> {
        let result = self.inner.report_sync(context, counts, Some(status))?;
        Ok(FinalAccountingResult::new(result, status)?)
    }
}
