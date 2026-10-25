//! Native RPC adapters. Provider callbacks and network waits never hold state locks.
use super::*;
use crate::{
    interfaces::IntegrationError, queue::QueueError, signing, transport::request_deadline,
};
use futures_core::Stream;
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Weak,
    task::{Context, Poll},
};
use tokio::{
    task::AbortHandle,
    time::{Duration, Sleep},
};
use tonic::{Request, Response, Streaming};

/// Original inference error supplied by the provider integration. A definitive
/// final report is still required before the SDK can issue a Terminal.
#[derive(Clone)]
pub struct UpstreamFailure {
    body: Vec<u8>,
    http_status: Option<u32>,
}
impl UpstreamFailure {
    pub fn new(body: Vec<u8>, http_status: Option<u32>) -> Result<Self, ValidationError> {
        wire::validate_json(&body, None, None)?;
        wire::validate_message(&proto::UpstreamError {
            body: Some(body.clone()),
            http_status,
        })?;
        Ok(Self { body, http_status })
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    pub fn http_status(&self) -> Option<u32> {
        self.http_status
    }
    fn chunk(&self) -> proto::ChatChunk {
        proto::ChatChunk {
            kind: Some(proto::chat_chunk::Kind::UpstreamError(
                proto::UpstreamError {
                    body: Some(self.body.clone()),
                    http_status: self.http_status,
                },
            )),
        }
    }
}
impl std::fmt::Display for UpstreamFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("provider inference failed")
    }
}
impl std::error::Error for UpstreamFailure {}
impl std::fmt::Debug for UpstreamFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamFailure")
            .field("http_status", &self.http_status)
            .finish_non_exhaustive()
    }
}

enum Scope {
    Control(u64),
    Run(RunContext),
}
struct RpcGuard {
    runtime: Weak<Runtime>,
    scope: Scope,
    reader: Option<AbortHandle>,
}
impl Drop for RpcGuard {
    fn drop(&mut self) {
        if let Some(reader) = &self.reader {
            reader.abort();
        }
        if let Some(runtime) = self.runtime.upgrade() {
            match &self.scope {
                Scope::Control(id) => runtime.release_attachment(*id),
                Scope::Run(context) => runtime.cancel_run(context),
            }
        }
    }
}
type NextMessage = Pin<Box<dyn Future<Output = Result<Vec<u8>, QueueError>> + Send>>;
/// Response stream owns cancellation of its one RPC; dropping it never closes funds.
pub struct RpcStream<T> {
    queue: Arc<ByteQueue>,
    next: NextMessage,
    deadline: Option<Pin<Box<Sleep>>>,
    guard: Option<RpcGuard>,
    done: bool,
    marker: PhantomData<fn() -> T>,
}
fn next_message(queue: Arc<ByteQueue>) -> NextMessage {
    Box::pin(async move { queue.get().await })
}
impl<T> RpcStream<T> {
    fn new(queue: Arc<ByteQueue>, guard: RpcGuard, deadline: Option<Instant>) -> Self {
        Self {
            next: next_message(queue.clone()),
            queue,
            guard: Some(guard),
            deadline: deadline.map(|end| Box::pin(tokio::time::sleep_until(end))),
            done: false,
            marker: PhantomData,
        }
    }
}
impl<T: Message + Default> Stream for RpcStream<T> {
    type Item = Result<T, Status>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        if this
            .deadline
            .as_mut()
            .is_some_and(|sleep| sleep.as_mut().poll(cx).is_ready())
        {
            this.done = true;
            this.guard.take();
            return Poll::Ready(Some(Err(Status::deadline_exceeded("RPC deadline elapsed"))));
        }
        match this.next.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(bytes)) => {
                this.next = next_message(this.queue.clone());
                Poll::Ready(Some(
                    T::decode(bytes.as_slice())
                        .map_err(|_| Status::internal("invalid queued protocol message")),
                ))
            }
            Poll::Ready(Err(error)) => {
                this.done = true;
                this.guard.take();
                Poll::Ready(match error {
                    QueueError::Closed => None,
                    QueueError::Failed(status) => Some(Err(status)),
                    _ => Some(Err(Status::resource_exhausted("application queue limit"))),
                })
            }
        }
    }
}

#[tonic::async_trait]
impl proto::control_server::Control for Mppi {
    type SessionStream = RpcStream<proto::ControlMessage>;
    async fn session(
        &self,
        request: Request<Streaming<proto::ControlMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let deadline = request_deadline(&request)?;
        if deadline.is_some_and(|end| Instant::now() >= end) {
            return Err(Status::deadline_exceeded("RPC deadline elapsed"));
        }
        let authentication_deadline = Instant::now()
            .checked_add(self.inner.limits.control_auth_timeout)
            .ok_or_else(|| Status::resource_exhausted("Control authentication deadline limit"))?;
        let nonce = random_id()?;
        let (id, queue) = {
            let mut state = self.inner.lock()?;
            if state.stopping {
                return Err(not_ready("CONTROL_NOT_READY", None, None));
            }
            if state.attachments.len() >= self.inner.limits.max_control_streams {
                return Err(Status::resource_exhausted("Control stream retention limit"));
            }
            let id = state
                .next_attachment
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("Control identifier limit"))?;
            let queue = Arc::new(
                ByteQueue::new(
                    64,
                    self.inner.limits.wire.message_bytes(),
                    self.inner.config.payment().authorization_wait_ms(),
                )
                .map_err(validation_status)?,
            );
            // Every fallible setup operation precedes retention: an RPC guard
            // does not exist yet to release an attachment on an early error.
            let challenge = control(proto::control_message::Kind::ReconnectChallenge(
                proto::ReconnectChallenge {
                    nonce: Some(nonce.to_vec()),
                },
            ));
            wire::validate_message_with_limits(&challenge, &self.inner.limits.wire)
                .map_err(validation_status)?;
            queue
                .put_nowait(&challenge.encode_to_vec())
                .map_err(|_| Status::resource_exhausted("Control queue limit"))?;
            state.next_attachment = id;
            state.attachments.insert(
                id,
                Attachment {
                    queue: queue.clone(),
                    nonce,
                    live: true,
                    sid: None,
                    generation: 0,
                    request: None,
                    challenges: BTreeMap::new(),
                    catalogs: BTreeMap::new(),
                    challenge_bytes: 0,
                },
            );
            (id, queue)
        };
        let runtime = self.inner.clone();
        let guard = RpcGuard {
            runtime: Arc::downgrade(&runtime),
            scope: Scope::Control(id),
            reader: None,
        };
        let reader = runtime.clone().spawn(async move {
            runtime
                .read_control(id, request.into_inner(), deadline, authentication_deadline)
                .await;
        })?;
        let mut guard = guard;
        guard.reader = Some(reader);
        Ok(Response::new(RpcStream::new(queue, guard, deadline)))
    }
}

impl Runtime {
    fn release_attachment(&self, id: u64) {
        if let Ok(mut state) = self.lock() {
            self.detach_locked(&mut state, id);
            if let Some(attachment) = state.attachments.remove(&id) {
                let _ = attachment.queue.close(None);
                if let Some(sid) = attachment.sid {
                    // Generation zero alone does not identify temporary state:
                    // recovered catalogs and close intent are authoritative.
                    let provisional = state.sessions.get(&sid).is_some_and(|session| {
                        session.provisional_owner == Some(id)
                            && session.generation == 0
                            && session.runs.is_empty()
                            && session.voucher.is_none()
                            && session.operation.is_none()
                            && session.baseline.is_none()
                            && !session.close_pending
                            && session.close_reference.is_none()
                    });
                    if provisional {
                        state.sessions.remove(&sid);
                        // This synchronous watch removal shares the state lock
                        // with deletion; it cannot race a recovered replacement.
                        if state.pending_watch != Some(sid) {
                            let _ = self.chain.unwatch_session(sid);
                        }
                    }
                }
            }
            self.changed.notify_waiters();
        }
    }

    async fn read_control(
        self: Arc<Self>,
        id: u64,
        mut requests: Streaming<proto::ControlMessage>,
        deadline: Option<Instant>,
        authentication_deadline: Instant,
    ) {
        let result = async {
            let mut authenticated = false;
            loop {
                let pending_authentication = !authenticated;
                let message = async {
                    if deadline.is_some_and(|end| Instant::now() >= end) {
                        return Err(Status::deadline_exceeded("RPC deadline elapsed"));
                    }
                    if pending_authentication && Instant::now() >= authentication_deadline {
                        return Err(Status::deadline_exceeded(
                            "Control authentication deadline elapsed",
                        ));
                    }
                    let Some(message) = requests.message().await? else {
                        return Ok(None);
                    };
                    if deadline.is_some_and(|end| Instant::now() >= end) {
                        return Err(Status::deadline_exceeded("RPC deadline elapsed"));
                    }
                    // Tokio timeout polls its inner future first. A ready late
                    // message must not consume a nonce or acquire session ownership.
                    if pending_authentication && Instant::now() >= authentication_deadline {
                        return Err(Status::deadline_exceeded(
                            "Control authentication deadline elapsed",
                        ));
                    }
                    wire::validate_message_with_limits(&message, &self.limits.wire)
                        .map_err(validation_status)?;
                    let proves_authentication = matches!(
                        &message.kind,
                        Some(proto::control_message::Kind::ReconnectRequest(_))
                    );
                    match message
                        .kind
                        .ok_or_else(|| Status::invalid_argument("missing Control kind"))?
                    {
                        proto::control_message::Kind::ChallengeRequest(value) => {
                            self.challenge(id, value)?
                        }
                        proto::control_message::Kind::SessionOpen(value) => {
                            self.opening(id, value).await?
                        }
                        proto::control_message::Kind::ReconnectRequest(value) => {
                            self.attach(id, value)?
                        }
                        proto::control_message::Kind::Voucher(value) => {
                            self.receive_voucher(id, value).await?;
                        }
                        proto::control_message::Kind::TopUpNotice(value) => {
                            let state = self.lock()?;
                            self.owner_locked(&state, id, value.session_id.as_deref().unwrap())?;
                            // Only a notice: the follower must observe any deposit change.
                        }
                        proto::control_message::Kind::CloseRequest(value) => {
                            let collection = {
                                let mut state = self.lock()?;
                                let sid = self.owner_locked(
                                    &state,
                                    id,
                                    value.session_id.as_deref().unwrap(),
                                )?;
                                let session = state.sessions.get_mut(&sid).ok_or_else(|| {
                                    not_ready("STATE_UNAVAILABLE", Some(sid), None)
                                })?;
                                self.contract
                                    .domain()
                                    .signing_context()
                                    .verify(
                                        session.opening.descriptor(),
                                        &signing::CloseAuth { sessionId: sid },
                                        value.signature.as_deref().unwrap(),
                                    )
                                    .map_err(|_| Status::unauthenticated("invalid close proof"))?;
                                if let Some(result) = session.close_result.clone() {
                                    // Authenticated retries return the original confirmed
                                    // outcome even when its first delivery was lost.
                                    self.send_locked(
                                        &mut state,
                                        id,
                                        control(proto::control_message::Kind::CloseResult(result)),
                                    )?;
                                    None
                                } else {
                                    if session.close_reference.is_none() {
                                        session.close_reference =
                                            Some(format!("close:{:x}", random_id()?));
                                    }
                                    session.close_pending = true;
                                    self.stop_session_locked(&mut state, sid);
                                    self.changed.notify_waiters();
                                    Some(sid)
                                }
                            };
                            if let Some(sid) = collection {
                                self.schedule_collection(sid, true)?;
                            }
                        }
                        _ => return Err(Status::invalid_argument("invalid Control direction")),
                    }
                    Ok::<_, Status>(Some(proves_authentication))
                };
                let next = if pending_authentication {
                    tokio::time::timeout_at(authentication_deadline, message)
                        .await
                        .map_err(|_| {
                            Status::deadline_exceeded("Control authentication deadline elapsed")
                        })??
                } else {
                    message.await?
                };
                let Some(proved) = next else {
                    break;
                };
                authenticated |= proved;
            }
            Ok::<(), Status>(())
        };
        let result = if let Some(end) = deadline {
            tokio::time::timeout_at(end, result)
                .await
                .unwrap_or_else(|_| Err(Status::deadline_exceeded("RPC deadline elapsed")))
        } else {
            result.await
        };
        if let Err(error) = &result {
            self.observers.emit(hooks::Event::Error(HookError {
                operation: "control",
                code: error.code(),
            }));
        }
        if let Ok(state) = self.lock()
            && let Some(attachment) = state.attachments.get(&id)
        {
            let _ = attachment.queue.close(result.err());
        }
        // Free retention even when a hostile peer never reads the terminal
        // response. RpcGuard remains an idempotent fallback on response drop.
        self.release_attachment(id);
    }

    fn owner_locked(&self, state: &State, id: u64, raw_sid: &[u8]) -> Result<B256, Status> {
        let sid = B256::from_slice(raw_sid);
        let attachment = state
            .attachments
            .get(&id)
            .ok_or_else(|| Status::unauthenticated("Control does not own session"))?;
        let session = state
            .sessions
            .get(&sid)
            .ok_or_else(|| Status::unauthenticated("Control does not own session"))?;
        if !attachment.live
            || attachment.sid != Some(sid)
            || session.owner != Some(id)
            || attachment.generation != session.generation
        {
            return Err(Status::unauthenticated("Control does not own session"));
        }
        Ok(sid)
    }

    fn challenge(&self, id: u64, request: proto::ChallengeRequest) -> Result<(), Status> {
        let mut state = self.lock()?;
        let rid = B256::from_slice(request.request_id.as_deref().unwrap());
        let attachment = state
            .attachments
            .get(&id)
            .ok_or_else(|| not_ready("CONTROL_NOT_READY", None, None))?;
        if !attachment.live {
            return Err(not_ready("CONTROL_NOT_READY", None, None));
        }
        if let Some(old) = attachment.challenges.get(&rid).cloned() {
            if old.model != request.model {
                return Err(Status::invalid_argument("conflicting challenge request id"));
            }
            return self.send_locked(
                &mut state,
                id,
                control(proto::control_message::Kind::Challenge(old)),
            );
        }
        let snapshot = self.snapshot_locked(&state)?;
        let identity = snapshot
            .identities()
            .get(&self.config.identity().provider_id())
            .ok_or_else(|| not_ready("STATE_UNAVAILABLE", None, None))?;
        let catalog = identity
            .catalog()
            .ok_or_else(|| not_ready("STATE_UNAVAILABLE", None, None))?;
        if catalog
            .rates_for(request.model.as_deref().unwrap())
            .is_none()
        {
            return Err(Status::not_found("model unavailable"));
        }
        let challenge = proto::Challenge {
            request_id: request.request_id,
            model: request.model,
            rate_card_hash: Some(catalog.rate_card_hash().to_vec()),
            provider_id: Some(identity.provider_id().to_be_bytes::<32>().to_vec()),
            contract: Some(self.contract.domain().verifying_contract().to_vec()),
            chain_id: Some(self.contract.domain().chain_id()),
            operator: Some(identity.owner().to_vec()),
            cover_output_tokens: Some(self.config.payment().output_buffer_tokens()),
            suggest_session_tokens: Some(self.config.payment().suggest_session_tokens()),
        };
        let attachment = state.attachments.get_mut(&id).unwrap();
        if challenge.encoded_len()
            > self
                .limits
                .wire
                .control_bytes()
                .saturating_sub(attachment.challenge_bytes)
        {
            return Err(Status::resource_exhausted("challenge history limit"));
        }
        if !attachment.catalogs.contains_key(&catalog.rate_card_hash())
            && catalog.canonical_bytes().len()
                > self.limits.wire.message_bytes().saturating_sub(
                    attachment
                        .catalogs
                        .values()
                        .map(|c| c.canonical_bytes().len())
                        .sum(),
                )
        {
            return Err(Status::resource_exhausted("challenge catalog limit"));
        }
        attachment.challenge_bytes += challenge.encoded_len();
        attachment.challenges.insert(rid, challenge.clone());
        attachment
            .catalogs
            .insert(catalog.rate_card_hash(), catalog.clone());
        self.send_locked(
            &mut state,
            id,
            control(proto::control_message::Kind::Challenge(challenge)),
        )
    }

    async fn opening(self: &Arc<Self>, id: u64, notice: proto::SessionOpen) -> Result<(), Status> {
        let _serial = self.openings.lock().await;
        let sid = B256::from_slice(notice.session_id.as_deref().unwrap());
        let tx = B256::from_slice(notice.open_tx.as_deref().unwrap());
        let _watch = PendingWatch::new(self, sid)?;
        let candidates = {
            let state = self.lock()?;
            self.snapshot_locked(&state)?;
            let attachment = state
                .attachments
                .get(&id)
                .filter(|a| a.live)
                .ok_or_else(|| not_ready("CONTROL_NOT_READY", Some(sid), None))?;
            if attachment.sid.is_some_and(|old| old != sid) {
                return Err(Status::invalid_argument(
                    "Control already bound to a session",
                ));
            }
            if !state.sessions.contains_key(&sid)
                && state.sessions.len() >= self.limits.max_sessions
            {
                return Err(Status::resource_exhausted("session retention limit"));
            }
            attachment
                .catalogs
                .values()
                .cloned()
                .map(Some)
                .chain(std::iter::once(None))
                .collect::<Vec<_>>()
        };
        let mut evidence = None;
        for catalog in &candidates {
            if let Ok(value) = self.reader.opening(tx, sid, catalog.as_ref()).await {
                evidence = Some(value);
                break;
            }
        }
        let evidence = evidence.ok_or_else(|| not_ready("STATE_UNAVAILABLE", Some(sid), None))?;
        let descriptor = evidence.descriptor();
        if descriptor.providerId != self.config.identity().provider_id()
            || descriptor.operator != self.wallet.address()
        {
            return Err(Status::unauthenticated("opening is not for this provider"));
        }
        if let Some(agent) = &notice.agent_id {
            let wallet = self
                .reader
                .agent_wallet(U256::from_be_slice(agent), evidence.block())
                .await
                .map_err(|_| not_ready("STATE_UNAVAILABLE", Some(sid), None))?;
            if wallet != descriptor.agent {
                return Err(Status::unauthenticated(
                    "agent identity differs from funding wallet",
                ));
            }
        }
        {
            let mut state = self.lock()?;
            if let Some(existing) = state.sessions.get(&sid) {
                if existing.opening.descriptor() != evidence.descriptor()
                    || existing.opening.tx_hash() != evidence.tx_hash()
                {
                    return Err(not_ready("INCONSISTENT_STATE", Some(sid), None));
                }
                let attachment = state
                    .attachments
                    .get_mut(&id)
                    .filter(|a| a.live)
                    .ok_or_else(|| not_ready("CONTROL_NOT_READY", Some(sid), None))?;
                attachment.sid = Some(sid);
                return Ok(());
            }
            if state
                .start_block
                .is_none_or(|block| evidence.block().observation().block_number() <= block)
            {
                return Err(not_ready("STATE_UNAVAILABLE", Some(sid), None));
            }
        }
        self.chain
            .watch_session(sid)
            .await
            .map_err(|_| not_ready("STATE_UNAVAILABLE", Some(sid), None))?;
        let mut state = self.lock()?;
        self.snapshot_locked(&state)?;
        let attachment = state
            .attachments
            .get_mut(&id)
            .filter(|a| a.live)
            .ok_or_else(|| not_ready("CONTROL_NOT_READY", Some(sid), None))?;
        attachment.sid = Some(sid);
        let mut session = Session::new(sid, evidence, true);
        session.provisional_owner = Some(id);
        state.sessions.insert(sid, session);
        self.observers.emit(hooks::Event::SessionOpen(notice));
        Ok(())
    }

    async fn receive_voucher(&self, id: u64, voucher: proto::Voucher) -> Result<(), Status> {
        let sid = {
            let mut state = self.lock()?;
            let sid = self.owner_locked(&state, id, voucher.session_id.as_deref().unwrap())?;
            let snapshot = self.snapshot_locked(&state)?;
            let chain = snapshot
                .sessions()
                .get(&sid)
                .ok_or_else(|| not_ready("STATE_UNAVAILABLE", Some(sid), None))?;
            if voucher.cumulative.unwrap() <= chain.deposit() {
                self.accept_voucher_locked(&mut state, sid, voucher)?;
                self.changed.notify_waiters();
                return Ok(());
            }
            // Unauthenticated or forged messages must never trigger RPC work.
            self.verify_voucher_proof_locked(&state, sid, &voucher)?;
            self.gate_locked(&state, &state.sessions[&sid], None)?;
            sid
        };
        // A confirmed top-up can precede the next follower poll. Only a signed
        // ceiling above the cache requests one bounded canonical refresh; a
        // TopUpNotice remains a hint and creates neither credit nor an ACK.
        tokio::time::timeout(
            Duration::from_millis(self.chain.max_age_ms()),
            self.chain.refresh(),
        )
        .await
        .map_err(|_| not_ready("CHAIN_STATE_STALE", Some(sid), None))?
        .map_err(|_| not_ready("CHAIN_STATE_STALE", Some(sid), None))?;
        let mut state = self.lock()?;
        self.owner_locked(&state, id, sid.as_slice())?;
        // Ownership, closure and pause may change while network I/O is pending.
        self.gate_locked(&state, &state.sessions[&sid], None)?;
        self.accept_voucher_locked(&mut state, sid, voucher)?;
        self.changed.notify_waiters();
        Ok(())
    }

    fn verify_voucher_proof_locked(
        &self,
        state: &State,
        sid: B256,
        voucher: &proto::Voucher,
    ) -> Result<(), Status> {
        if voucher.session_id.as_deref() != Some(sid.as_slice()) {
            return Err(Status::unauthenticated("voucher session mismatch"));
        }
        let session = state
            .sessions
            .get(&sid)
            .ok_or_else(|| not_ready("STATE_UNAVAILABLE", Some(sid), None))?;
        self.contract
            .domain()
            .signing_context()
            .verify(
                session.opening.descriptor(),
                &signing::Voucher {
                    sessionId: sid,
                    cumulativeAmount: voucher.cumulative.unwrap(),
                },
                voucher.signature.as_deref().unwrap(),
            )
            .map_err(|_| Status::unauthenticated("invalid voucher proof"))
    }

    fn accept_voucher_locked(
        &self,
        state: &mut State,
        sid: B256,
        voucher: proto::Voucher,
    ) -> Result<(), Status> {
        if self.voucher_advances_locked(state, sid, &voucher)? {
            state.sessions.get_mut(&sid).unwrap().voucher = Some(voucher.clone());
            self.observers.emit(hooks::Event::Voucher(voucher));
        }
        Ok(())
    }

    fn voucher_advances_locked(
        &self,
        state: &State,
        sid: B256,
        voucher: &proto::Voucher,
    ) -> Result<bool, Status> {
        self.verify_voucher_proof_locked(state, sid, voucher)?;
        let snapshot = self.snapshot_locked(state)?;
        let session = state
            .sessions
            .get(&sid)
            .ok_or_else(|| not_ready("STATE_UNAVAILABLE", Some(sid), None))?;
        let cumulative = voucher.cumulative.unwrap();
        let chain = snapshot
            .sessions()
            .get(&sid)
            .ok_or_else(|| not_ready("STATE_UNAVAILABLE", Some(sid), None))?;
        if cumulative > chain.deposit() {
            return Err(Status::invalid_argument(
                "voucher exceeds confirmed deposit",
            ));
        }
        if session
            .voucher
            .as_ref()
            .is_none_or(|old| cumulative > old.cumulative.unwrap())
        {
            if cumulative
                < session
                    .usage()
                    .map_err(validation_status)?
                    .max(chain.settled_cumulative())
            {
                return Err(not_ready("INCONSISTENT_STATE", Some(sid), None));
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// The agent records a run before its Complete reaches the provider, so a refused
    /// or lost Complete stays pending at the agent although this provider never held
    /// it. Admission is commit-before-effect, so when every admission is known
    /// (`Session::admissions_known`) a requested pending run absent from them was
    /// never admitted. It gets the protocol's NOT_ADMITTED answer (protocol.md,
    /// Recovery); nothing is recorded, and the reattach's new generation refuses a
    /// late Complete for it at the gate.
    fn answer_unheld(session: &Session, pending: &[Vec<u8>], snapshot: &mut proto::SessionState) {
        if !session.admissions_known {
            return; // The attachment was refused had any such run been named.
        }
        for rid in pending {
            if !session.runs.contains_key(&B256::from_slice(rid)) {
                snapshot.run_states.push(proto::RunState {
                    run_id: Some(rid.clone()),
                    model: Some(String::new()),
                    status: Some(proto::RunStatus::NotAdmitted as i32),
                    input_tokens: Some(0),
                    cache_tokens: Some(0),
                    output_tokens: Some(0),
                    ..Default::default()
                });
            }
        }
    }

    pub(super) fn attach(&self, id: u64, request: proto::ReconnectRequest) -> Result<(), Status> {
        let mut state = self.lock()?;
        let sid = B256::from_slice(request.session_id.as_deref().unwrap());
        let session = state
            .sessions
            .get(&sid)
            .ok_or_else(|| not_ready("STATE_UNAVAILABLE", Some(sid), None))?;
        let attachment = state
            .attachments
            .get(&id)
            .filter(|a| a.live)
            .ok_or_else(|| not_ready("CONTROL_NOT_READY", Some(sid), None))?;
        if attachment.sid.is_some_and(|old| old != sid) {
            return Err(Status::unauthenticated("Control session mismatch"));
        }
        if let Some(old) = &attachment.request {
            if old != &request || session.owner != Some(id) {
                return Err(Status::unauthenticated("replayed attachment proof"));
            }
            let mut snapshot = self.session_state_locked(
                &state,
                session,
                request.nonce.as_deref().unwrap(),
                false,
            )?;
            Self::answer_unheld(session, &request.pending_run_ids, &mut snapshot);
            return self.send_locked(
                &mut state,
                id,
                control(proto::control_message::Kind::SessionState(snapshot)),
            );
        }
        if request.nonce.as_deref() != Some(attachment.nonce.as_slice()) {
            return Err(Status::unauthenticated("invalid attachment nonce"));
        }
        self.contract
            .domain()
            .signing_context()
            .verify(
                session.opening.descriptor(),
                &signing::ReconnectAuth {
                    sessionId: sid,
                    nonce: attachment.nonce,
                },
                request.signature.as_deref().unwrap(),
            )
            .map_err(|_| Status::unauthenticated("invalid attachment proof"))?;
        // Unless every admission is known, an absent pending run is unknown, never
        // NOT_ADMITTED: a restored checkpoint older than the host's newest may lack
        // a run that was served. Nor before any Control generation: Complete needs
        // generation 1 or more, so a session a new process re-created from its
        // opening (a start block can lag a crashed one's) never admitted a run
        // an agent names then.
        if !session.history_available
            || request.last_meter_seq.unwrap() > session.meter_seq
            || ((!session.admissions_known || session.generation == 0)
                && request
                    .pending_run_ids
                    .iter()
                    .any(|rid| !session.runs.contains_key(&B256::from_slice(rid))))
        {
            return Err(not_ready("STATE_UNAVAILABLE", Some(sid), None));
        }
        let generation = session
            .generation
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("Control generation limit"))?;
        let old_owner = session.owner;
        let mut voucher = session.voucher.clone();
        let mut advanced_voucher = None;
        if let Some(candidate) = &request.latest_voucher
            && self.voucher_advances_locked(&state, sid, candidate)?
        {
            voucher = Some(candidate.clone());
            advanced_voucher = voucher.clone();
        }
        // Stage the complete response and its queue admission before consuming
        // the nonce or changing payment/ownership state. In particular an active
        // run or an unrepresentable snapshot must leave the old owner intact.
        let mut snapshot = self.session_state_for_attachment_locked(
            &state,
            session,
            request.nonce.as_deref().unwrap(),
            false,
            Some(core::ReconnectState {
                id,
                generation,
                voucher: &voucher,
            }),
        )?;
        Self::answer_unheld(session, &request.pending_run_ids, &mut snapshot);
        let message = control(proto::control_message::Kind::SessionState(snapshot));
        wire::validate_message_with_limits(&message, &self.limits.wire)
            .map_err(validation_status)?;
        attachment
            .queue
            .put_nowait(&message.encode_to_vec())
            .map_err(|_| Status::resource_exhausted("Control queue limit"))?;
        // No fallible operation remains. State users serialize on this lock, so
        // the queued snapshot and the binding are observed as one transition.
        if let Some(old) = old_owner.filter(|old| *old != id) {
            self.detach_locked(&mut state, old);
            if let Some(attachment) = state.attachments.get(&old) {
                let _ =
                    attachment
                        .queue
                        .close(Some(not_ready("CONTROL_NOT_READY", Some(sid), None)));
            }
        }
        let session = state.sessions.get_mut(&sid).unwrap();
        session.generation = generation;
        session.owner = Some(id);
        session.provisional_owner = None;
        session.voucher = voucher;
        let attachment = state.attachments.get_mut(&id).unwrap();
        attachment.sid = Some(sid);
        attachment.generation = generation;
        attachment.request = Some(request.clone());
        if let Some(value) = advanced_voucher {
            self.observers.emit(hooks::Event::Voucher(value));
        }
        self.changed.notify_waiters();
        Ok(())
    }
}
#[tonic::async_trait]
impl proto::chat_server::Chat for Mppi {
    type CompletionsStream = RpcStream<proto::ChatChunk>;
    async fn completions(
        &self,
        request: Request<proto::CompleteRequest>,
    ) -> Result<Response<Self::CompletionsStream>, Status> {
        let deadline = request_deadline(&request)?;
        let run = self
            .inner
            .begin_run(request.into_inner())
            .inspect_err(|error| {
                self.inner.observers.emit(hooks::Event::Error(HookError {
                    operation: "complete",
                    code: error.code(),
                }));
            })?;
        let guard = RpcGuard {
            runtime: Arc::downgrade(&self.inner),
            scope: Scope::Run(run.context.clone()),
            reader: None,
        };
        let prepared = async {
            let bound = self
                .inner
                .coverage
                .coverage(&run.request, &run.context)
                .await
                .map_err(|error| {
                    if error.is::<UpstreamFailure>() {
                        error
                    } else {
                        Box::new(not_ready(
                            "STATE_UNAVAILABLE",
                            Some(run.context.session_id()),
                            Some(run.context.run_id()),
                        )) as IntegrationError
                    }
                })?;
            if bound.output_tokens() < self.inner.config.payment().output_buffer_tokens() {
                return Err::<(), IntegrationError>(Box::new(Status::invalid_argument(
                    "provider must supply configured output coverage",
                )));
            }
            let mut expires = None;
            loop {
                let changed = self.inner.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let checked_at = Instant::now();
                match self.inner.attempt_admission_before(&run, bound, expires)? {
                    Admission::Granted => return Ok(()),
                    Admission::Authorizing => {
                        if expires.is_none() {
                            expires = Some(
                                checked_at
                                    .checked_add(Duration::from_millis(
                                        self.inner.config.payment().authorization_wait_ms(),
                                    ))
                                    .ok_or_else(|| {
                                        Status::resource_exhausted("authorization deadline limit")
                                    })?,
                            );
                        }
                        if expires.is_some_and(|end| Instant::now() >= end) {
                            return Err(Box::new(not_ready(
                                "AUTHORIZATION_REQUIRED",
                                Some(run.context.session_id()),
                                Some(run.context.run_id()),
                            )));
                        }
                    }
                    // Covered: its turn comes within TURN, before any expiry.
                    Admission::Deferred => {}
                }
                // A changed follower also invalidates admission even without
                // an incoming Control message. This never resets the deadline.
                let poll = Instant::now() + Duration::from_millis(50);
                let wake = expires.map_or(poll, |end| end.min(poll));
                let _ = tokio::time::timeout_at(wake, changed).await;
            }
        };
        let result = if let Some(end) = deadline {
            tokio::time::timeout_at(end, prepared)
                .await
                .unwrap_or_else(|_| {
                    Err(Box::new(Status::deadline_exceeded("RPC deadline elapsed")))
                })
        } else {
            prepared.await
        };
        if let Err(error) = result {
            self.inner.observers.emit(hooks::Event::Error(HookError {
                operation: "complete",
                code: error
                    .downcast_ref::<Status>()
                    .map_or(tonic::Code::FailedPrecondition, Status::code),
            }));
            if let Some(upstream) = error.downcast_ref::<UpstreamFailure>() {
                self.inner.cancel_run(&run.context);
                self.inner
                    .enqueue_chunk(&run.output, upstream.chunk())
                    .await?;
                run.output
                    .close(Some(Status::failed_precondition(
                        "provider request preparation failed",
                    )))
                    .map_err(queue_status)?;
                return Ok(Response::new(RpcStream::new(run.output, guard, deadline)));
            }
            return Err(error.downcast_ref::<Status>().cloned().unwrap_or_else(|| {
                not_ready(
                    "STATE_UNAVAILABLE",
                    Some(run.context.session_id()),
                    Some(run.context.run_id()),
                )
            }));
        }
        let queue = run.output.clone();
        let runtime = self.inner.clone();
        self.inner.spawn(async move {
            runtime.execute_run(run, deadline).await;
        })?;
        Ok(Response::new(RpcStream::new(queue, guard, deadline)))
    }
}

fn queue_status(_: QueueError) -> Status {
    Status::resource_exhausted("application queue limit")
}
struct ExecutionGuard {
    runtime: Arc<Runtime>,
    context: RunContext,
    queue: Arc<ByteQueue>,
    completed: bool,
}
impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.runtime.cancel_run(&self.context);
            let _ = self.runtime.finish_stream(&self.context);
            let _ = self.queue.close(Some(not_ready(
                "STATE_UNAVAILABLE",
                Some(self.context.session_id()),
                Some(self.context.run_id()),
            )));
        }
    }
}
impl Runtime {
    async fn enqueue_chunk(
        &self,
        queue: &ByteQueue,
        chunk: proto::ChatChunk,
    ) -> Result<(), Status> {
        wire::validate_message_with_limits(&chunk, &self.limits.wire).map_err(validation_status)?;
        queue.put(chunk.encode_to_vec()).await.map_err(queue_status)
    }

    async fn execute_run(self: Arc<Self>, run: RunHandle, deadline: Option<Instant>) {
        let mut guard = ExecutionGuard {
            runtime: self.clone(),
            context: run.context.clone(),
            queue: run.output.clone(),
            completed: false,
        };
        let execution = async {
            self.recheck_dispatch(&run.context)?;
            let reporter: Arc<dyn ProviderRuns> = Arc::new(Mppi {
                inner: self.clone(),
            });
            let mut outputs = self.execution.execute(
                run.request.clone(),
                run.context.clone(),
                reporter,
                run.stop.clone(),
            );
            let mut count = 0usize;
            while let Some(result) = std::future::poll_fn(|cx| outputs.as_mut().poll_next(cx)).await
            {
                match result {
                    Ok(payload) => {
                        count = count
                            .checked_add(1)
                            .ok_or_else(|| Status::resource_exhausted("response count limit"))?;
                        if !run.request.stream() && count != 1 {
                            return Err(not_ready(
                                "INCONSISTENT_USAGE",
                                Some(run.context.session_id()),
                                Some(run.context.run_id()),
                            ));
                        }
                        self.enqueue_chunk(
                            &run.output,
                            proto::ChatChunk {
                                kind: Some(proto::chat_chunk::Kind::Payload(payload)),
                            },
                        )
                        .await?;
                    }
                    Err(error) => {
                        if let Some(upstream) = error.downcast_ref::<UpstreamFailure>() {
                            self.observers.emit(hooks::Event::Error(HookError {
                                operation: "complete",
                                code: tonic::Code::FailedPrecondition,
                            }));
                            self.enqueue_chunk(&run.output, upstream.chunk()).await?;
                            let terminal = self.finish_stream(&run.context)?;
                            if terminal.as_ref().and_then(|value| value.status)
                                != Some(proto::RunStatus::Failed as i32)
                            {
                                return Err(not_ready(
                                    "STATE_UNAVAILABLE",
                                    Some(run.context.session_id()),
                                    Some(run.context.run_id()),
                                ));
                            }
                            return Ok(terminal);
                        }
                        return Err(not_ready(
                            "STATE_UNAVAILABLE",
                            Some(run.context.session_id()),
                            Some(run.context.run_id()),
                        ));
                    }
                }
            }
            let terminal = self.finish_stream(&run.context)?;
            if !run.request.stream()
                && count != 1
                && terminal.as_ref().and_then(|value| value.status)
                    == Some(proto::RunStatus::Completed as i32)
            {
                return Err(not_ready(
                    "INCONSISTENT_USAGE",
                    Some(run.context.session_id()),
                    Some(run.context.run_id()),
                ));
            }
            Ok(terminal)
        };
        let result = if let Some(end) = deadline {
            tokio::time::timeout_at(end, execution)
                .await
                .unwrap_or_else(|_| Err(Status::deadline_exceeded("RPC deadline elapsed")))
        } else {
            execution.await
        };
        let error = match result {
            Ok(Some(terminal)) => self
                .enqueue_chunk(
                    &run.output,
                    proto::ChatChunk {
                        kind: Some(proto::chat_chunk::Kind::Terminal(Box::new(terminal))),
                    },
                )
                .await
                .err(),
            Ok(None) => Some(not_ready(
                "STATE_UNAVAILABLE",
                Some(run.context.session_id()),
                Some(run.context.run_id()),
            )),
            Err(status) => Some(status),
        };
        if let Some(error) = &error {
            self.observers.emit(hooks::Event::Error(HookError {
                operation: "complete",
                code: error.code(),
            }));
            self.cancel_run(&run.context);
        }
        let _ = self.finish_stream(&run.context);
        let _ = run.output.close(error);
        guard.completed = true;
    }
}
