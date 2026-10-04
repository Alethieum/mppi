use super::*;
use crate::{
    interfaces::{DiscoveryResponse, ProviderLifecycle, ProviderSessions},
    transport::LimitsService,
    types::{OperationState, Readiness, ShutdownResult},
};
use std::{future::Future, time::Duration};
use tokio::{task::AbortHandle, time::timeout_at};

impl Runtime {
    pub(super) fn spawn(
        self: &Arc<Self>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<AbortHandle, Status> {
        let mut tasks = self
            .tasks
            .lock()
            .map_err(|_| Status::internal("runtime tasks unavailable"))?;
        if self.halt.is_cancelled() {
            return Err(not_ready("CONTROL_NOT_READY", None, None));
        }
        while tasks.try_join_next().is_some() {}
        Ok(tasks.spawn(future))
    }
    fn initialize(&self) -> Result<(), Status> {
        let snapshot = self
            .chain
            .snapshot()
            .map_err(|_| not_ready("CHAIN_STATE_STALE", None, None))?;
        let identity = snapshot
            .identities()
            .get(&self.config.identity().provider_id())
            .ok_or_else(|| not_ready("STATE_UNAVAILABLE", None, None))?;
        let documents = DiscoveryDocuments::new(&self.config, identity)
            .map_err(|_| not_ready("STATE_UNAVAILABLE", None, None))?;
        let mut state = self.lock()?;
        if state.stopping {
            return Err(not_ready("CONTROL_NOT_READY", None, None));
        }
        if !state.started {
            state.documents = Some(documents);
            state.start_block = Some(snapshot.block().observation().block_number());
            state.started = true;
        }
        Ok(())
    }
    pub(super) fn schedule_collection(
        self: &Arc<Self>,
        sid: B256,
        close: bool,
    ) -> Result<(), Status> {
        {
            let mut state = self.lock()?;
            if state.stopping || !state.collecting.insert(sid) {
                return Ok(());
            }
        }
        self.spawn_collection(sid, close)
    }
    fn spawn_collection(self: &Arc<Self>, sid: B256, close: bool) -> Result<(), Status> {
        let sdk = Mppi {
            inner: self.clone(),
        };
        let guard = CollectionGuard {
            sdk: sdk.clone(),
            sid,
        };
        self.spawn(async move {
            let _guard = guard;
            let _ = sdk.collect(sid, close).await;
        })
        .map(|_| ())
    }
    fn spawn_batch_collection(self: &Arc<Self>, ids: Vec<B256>) -> Result<(), Status> {
        let sdk = Mppi {
            inner: self.clone(),
        };
        let guards = ids
            .iter()
            .map(|sid| CollectionGuard {
                sdk: sdk.clone(),
                sid: *sid,
            })
            .collect::<Vec<_>>();
        self.spawn(async move {
            let _guards = guards;
            let mut groups = ids
                .chunks(wire::MAX_MESSAGE_BYTES / 512)
                .rev()
                .map(<[B256]>::to_vec)
                .collect::<Vec<_>>();
            while let Some(group) = groups.pop() {
                // Each attempt rechecks retained operations before preparation.
                // Splitting an unavailable preparation/simulation therefore
                // cannot replace a possibly submitted economic intent.
                let result = sdk.collect_many(&group, false, group.len() > 1).await;
                if result.is_err() && group.len() > 1 && !sdk.inner.halt.is_cancelled() {
                    let middle = group.len() / 2;
                    groups.push(group[middle..].to_vec());
                    groups.push(group[..middle].to_vec());
                }
            }
        })
        .map(|_| ())
    }
    pub(super) fn maintain_once(self: &Arc<Self>) {
        let started = self.lock().map(|s| s.started).unwrap_or(false);
        if !started && self.initialize().is_err() {
            return;
        }
        if let Ok(mut state) = self.lock() {
            let stop = state
                .sessions
                .iter()
                .filter_map(|(sid, session)| {
                    (session.active().any(|run| !run.stop.is_cancelled())
                        && self.gate_locked(&state, session, None).is_err())
                    .then_some(*sid)
                })
                .collect::<Vec<_>>();
            for sid in stop {
                self.stop_session_locked(&mut state, sid);
            }
            self.changed.notify_waiters();
        }
        let sdk = Mppi {
            inner: self.clone(),
        };
        if let Ok(due) = sdk.collections_due() {
            let mut batch = Vec::new();
            for (sid, close) in due {
                if close {
                    let _ = self.spawn_collection(sid, true);
                } else {
                    batch.push(sid);
                }
            }
            if !batch.is_empty() {
                let _ = self.spawn_batch_collection(batch);
            }
        }
        if let Ok(mut tasks) = self.tasks.lock() {
            while tasks.try_join_next().is_some() {}
        }
    }
}
struct CollectionGuard {
    sdk: Mppi,
    sid: B256,
}
impl Drop for CollectionGuard {
    fn drop(&mut self) {
        self.sdk.collection_finished(self.sid);
    }
}

#[tonic::async_trait]
impl ProviderLifecycle for Mppi {
    type Chat = Self;
    type Control = Self;
    fn runs(&self) -> Arc<dyn ProviderRuns> {
        Arc::new(self.clone())
    }
    fn sessions(&self) -> Arc<dyn ProviderSessions> {
        Arc::new(self.clone())
    }
    fn grpc_services(&self) -> crate::interfaces::GrpcServices<Self, Self> {
        let limits = self.inner.limits.wire;
        let chat = proto::chat_server::ChatServer::new(self.clone())
            .max_decoding_message_size(limits.message_bytes())
            .max_encoding_message_size(limits.message_bytes());
        let control = proto::control_server::ControlServer::new(self.clone())
            .max_decoding_message_size(limits.control_bytes())
            .max_encoding_message_size(limits.message_bytes());
        (
            LimitsService::new(chat, limits.message_bytes()).expect("validated message bound"),
            LimitsService::new(control, limits.control_bytes()).expect("validated Control bound"),
        )
    }
    fn discovery(&self, path: &str) -> Option<DiscoveryResponse> {
        self.inner.lock().ok()?.documents.as_ref()?.get(path)
    }
    async fn start(&self) -> IntegrationResult<Readiness> {
        let _serial = self.inner.lifecycle.lock().await;
        if self.inner.halt.is_cancelled() {
            return Ok(unready("CONTROL_NOT_READY"));
        }
        if !self.inner.lock()?.started {
            tokio::select! {
                _ = self.inner.halt.cancelled() => return Ok(unready("CONTROL_NOT_READY")),
                _ = self.inner.chain.start() => {}
            }
            tokio::select! {
                _ = self.inner.halt.cancelled() => return Ok(unready("CONTROL_NOT_READY")),
                _ = self.inner.chain.watch_provider(self.inner.config.identity().provider_id()) => {}
            }
            let _ = self.inner.initialize();
        }
        let mut task = self
            .inner
            .maintenance
            .lock()
            .map_err(|_| Status::internal("runtime maintenance unavailable"))?;
        if task.is_none() && !self.inner.halt.is_cancelled() {
            let weak = Arc::downgrade(&self.inner);
            let halt = self.inner.halt.clone();
            let interval = Duration::from_millis(
                self.inner
                    .config
                    .settlement()
                    .interval_ms()
                    .min((self.inner.config.chain().max_age_ms() / 2).max(1))
                    .min(50),
            );
            *task = Some(tokio::spawn(async move {
                loop {
                    tokio::select! { _ = halt.cancelled() => break, _ = tokio::time::sleep(interval) => {} }
                    let Some(runtime) = weak.upgrade() else {
                        break;
                    };
                    runtime.maintain_once();
                }
            }));
        }
        if !self.inner.lock()?.started {
            return Ok(unready("CHAIN_STATE_STALE"));
        }
        Ok(self.inner.chain.readiness(None))
    }
    async fn shutdown(&self, deadline: Instant) -> IntegrationResult<ShutdownResult> {
        self.inner.halt.cancel();
        {
            let mut state = self.inner.lock()?;
            state.stopping = true;
            self.inner.discard_unsubmitted_collection_locked(&mut state);
            for sid in state.sessions.keys().copied().collect::<Vec<_>>() {
                self.inner.stop_session_locked(&mut state, sid);
            }
            self.inner.changed.notify_waiters();
        }
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let active = self
                .inner
                .lock()?
                .sessions
                .values()
                .any(|s| s.active().next().is_some());
            if !active || Instant::now() >= deadline {
                break;
            }
            let _ = timeout_at(deadline, changed).await;
        }
        let maintenance = self
            .inner
            .maintenance
            .lock()
            .map_err(|_| Status::internal("runtime maintenance unavailable"))?
            .take();
        if let Some(task) = maintenance {
            task.abort();
            let _ = timeout_at(deadline, task).await;
        }
        {
            let mut state = self.inner.lock()?;
            for attachment in state.attachments.values_mut() {
                attachment.live = false;
                let _ = attachment.queue.close(None);
            }
        }
        let mut tasks = {
            std::mem::take(
                &mut *self
                    .inner
                    .tasks
                    .lock()
                    .map_err(|_| Status::internal("runtime tasks unavailable"))?,
            )
        };
        tasks.abort_all();
        while !tasks.is_empty() && Instant::now() < deadline {
            if timeout_at(deadline, tasks.join_next()).await.is_err() {
                break;
            }
        }
        self.inner.chain.request_stop()?;
        let _ = timeout_at(deadline, self.inner.chain.stop()).await;
        self.inner.observers.close();
        let state = self.inner.lock()?;
        Ok(ShutdownResult {
            unresolved_session_ids: state
                .sessions
                .values()
                .filter(|s| s.inconsistent || s.active().next().is_some())
                .map(|s| s.sid)
                .collect(),
            unresolved_runs: state
                .sessions
                .values()
                .flat_map(|s| s.active().map(|run| run.context.clone()))
                .collect(),
            pending_operations: state
                .sessions
                .values()
                .filter_map(|s| s.operation.as_ref())
                .chain(state.empty_batch.iter())
                .filter(|op| {
                    matches!(
                        op.result.state(),
                        OperationState::Pending | OperationState::Unknown
                    )
                })
                .map(|op| {
                    (
                        op.result.operation_ref().to_owned(),
                        op.result.as_ref().clone(),
                    )
                })
                .collect::<BTreeMap<_, _>>()
                .into_values()
                .collect(),
        })
    }
}
fn unready(reason: &str) -> Readiness {
    Readiness::new(
        false,
        vec![
            PaymentFailure::new(
                PaymentCode::SessionNotReady,
                Some(reason.into()),
                None,
                None,
                None,
                None,
                None,
            )
            .expect("fixed readiness failure"),
        ],
    )
    .expect("fixed readiness result")
}
