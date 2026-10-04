//! Coherent cached chain evidence. Readiness performs no RPC work. A repeated
//! head, stale timestamp, reorg, failed refresh or cancelled read fails closed.
use crate::{
    B256, U256,
    chain::{Block, ChainError, ChainReader, ChainSession},
    discovery::ProviderIdentity,
    types::{PauseLevel, PaymentCode, PaymentFailure, Readiness, SessionStatus},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Mutex as AsyncMutex, task::JoinHandle};

const MAX_MS: u64 = (i64::MAX as u64) / 1_000_000;
fn unavailable() -> ChainError {
    ChainError::Unavailable
}

/// Caller-supplied clocks must advance consistently with local chain time.
/// Monotonic time measures elapsed duration; wall time is since Unix epoch.
pub trait Clock: Send + Sync {
    fn monotonic(&self) -> Duration;
    fn wall(&self) -> Result<Duration, ChainError>;
}
struct SystemClock {
    start: Instant,
}
impl Clock for SystemClock {
    fn monotonic(&self) -> Duration {
        self.start.elapsed()
    }
    fn wall(&self) -> Result<Duration, ChainError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| unavailable())
    }
}

#[derive(Clone, Debug)]
pub struct ChainSnapshot {
    block: Block,
    pause_level: PauseLevel,
    identities: BTreeMap<U256, ProviderIdentity>,
    sessions: BTreeMap<B256, ChainSession>,
    observed_at: Duration,
}
impl ChainSnapshot {
    pub fn block(&self) -> &Block {
        &self.block
    }
    pub fn pause_level(&self) -> PauseLevel {
        self.pause_level
    }
    pub fn identities(&self) -> &BTreeMap<U256, ProviderIdentity> {
        &self.identities
    }
    pub fn sessions(&self) -> &BTreeMap<B256, ChainSession> {
        &self.sessions
    }
    pub fn observed_at(&self) -> Duration {
        self.observed_at
    }
}
struct State {
    providers: BTreeSet<U256>,
    sessions: BTreeSet<B256>,
    current: Option<Arc<ChainSnapshot>>,
    progress_at: Duration,
    failed: bool,
    running: bool,
    generation: u64,
}
struct Inner {
    reader: ChainReader,
    clock: Arc<dyn Clock>,
    max_age: Duration,
    collection_margin: Duration,
    poll_interval: Duration,
    state: Mutex<State>,
    refresh: AsyncMutex<()>,
}
/// Dropping the follower aborts its background task. `stop` also waits for that
/// task to finish. Read failures retain the last evidence without renewing it.
pub struct ChainFollower {
    inner: Arc<Inner>,
    task: Mutex<Option<JoinHandle<()>>>,
    lifecycle: AsyncMutex<()>,
}
impl core::fmt::Debug for ChainFollower {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ChainFollower").finish_non_exhaustive()
    }
}
impl Drop for ChainFollower {
    fn drop(&mut self) {
        if let Ok(mut task) = self.task.lock()
            && let Some(task) = task.take()
        {
            task.abort();
        }
    }
}
impl ChainFollower {
    /// The reader and timing policy used by this follower. Runtime composition
    /// checks these against its declared deployment and collection policy.
    pub fn reader(&self) -> &ChainReader {
        &self.inner.reader
    }
    pub fn max_age_ms(&self) -> u64 {
        self.inner.max_age.as_millis() as u64
    }
    pub fn collection_margin_ms(&self) -> u64 {
        self.inner.collection_margin.as_millis() as u64
    }
    pub fn new(
        reader: ChainReader,
        max_age_ms: u64,
        collection_margin_ms: u64,
        poll_interval_ms: Option<u64>,
    ) -> Result<Self, ChainError> {
        Self::with_clock(
            reader,
            max_age_ms,
            collection_margin_ms,
            poll_interval_ms,
            Arc::new(SystemClock {
                start: Instant::now(),
            }),
        )
    }
    pub fn with_clock(
        reader: ChainReader,
        max_age_ms: u64,
        collection_margin_ms: u64,
        poll_interval_ms: Option<u64>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ChainError> {
        let poll = poll_interval_ms.unwrap_or((max_age_ms / 4).max(1));
        if [max_age_ms, collection_margin_ms, poll]
            .iter()
            .any(|n| !(1..=MAX_MS).contains(n))
            || poll > max_age_ms
        {
            return Err(unavailable());
        }
        Ok(Self {
            inner: Arc::new(Inner {
                reader,
                clock,
                max_age: Duration::from_millis(max_age_ms),
                collection_margin: Duration::from_millis(collection_margin_ms),
                poll_interval: Duration::from_millis(poll),
                state: Mutex::new(State {
                    providers: BTreeSet::new(),
                    sessions: BTreeSet::new(),
                    current: None,
                    progress_at: Duration::ZERO,
                    failed: true,
                    running: false,
                    generation: 0,
                }),
                refresh: AsyncMutex::new(()),
            }),
            task: Mutex::new(None),
            lifecycle: AsyncMutex::new(()),
        })
    }
    pub async fn start(&self) -> Result<(), ChainError> {
        let _lifecycle = self.lifecycle.lock().await;
        {
            let mut state = self.inner.state.lock().map_err(|_| unavailable())?;
            if state.running {
                return Ok(());
            }
            state.generation = state.generation.checked_add(1).ok_or_else(unavailable)?;
            state.running = true;
            state.failed = true;
        }
        let inner = self.inner.clone();
        // Install the task before the first await: cancelled startup cannot
        // strand a running follower without a poller.
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(inner.poll_interval).await;
                if !inner.state.lock().map(|s| s.running).unwrap_or(false) {
                    break;
                }
                let _ = inner.refresh().await;
            }
        });
        *self.task.lock().map_err(|_| unavailable())? = Some(task);
        self.inner.refresh().await.map(|_| ())
    }
    pub async fn stop(&self) -> Result<(), ChainError> {
        let _lifecycle = self.lifecycle.lock().await;
        {
            let mut state = self.inner.state.lock().map_err(|_| unavailable())?;
            state.running = false;
            state.failed = true;
        }
        let task = self.task.lock().map_err(|_| unavailable())?.take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        Ok(())
    }
    /// Invalidate and signal owned polling synchronously before a bounded join.
    pub(crate) fn request_stop(&self) -> Result<(), ChainError> {
        let mut state = self.inner.state.lock().map_err(|_| unavailable())?;
        state.running = false;
        state.failed = true;
        drop(state);
        if let Some(task) = self.task.lock().map_err(|_| unavailable())?.as_ref() {
            task.abort();
        }
        Ok(())
    }
    /// Last observation, including stale evidence; never an admission grant.
    pub fn current(&self) -> Option<Arc<ChainSnapshot>> {
        self.inner.state.lock().ok()?.current.clone()
    }
    pub fn snapshot(&self) -> Result<Arc<ChainSnapshot>, ChainError> {
        self.inner.snapshot()
    }
    pub async fn refresh(&self) -> Result<Arc<ChainSnapshot>, ChainError> {
        self.inner.refresh().await
    }
    pub async fn watch_provider(&self, id: U256) -> Result<ProviderIdentity, ChainError> {
        self.inner
            .state
            .lock()
            .map_err(|_| unavailable())?
            .providers
            .insert(id);
        self.refresh()
            .await?
            .identities
            .get(&id)
            .cloned()
            .ok_or_else(unavailable)
    }
    pub async fn watch_session(&self, id: B256) -> Result<ChainSession, ChainError> {
        self.inner
            .state
            .lock()
            .map_err(|_| unavailable())?
            .sessions
            .insert(id);
        self.refresh()
            .await?
            .sessions
            .get(&id)
            .cloned()
            .ok_or_else(unavailable)
    }
    /// Release a watch installed by a failed admission/setup attempt. Retained
    /// historical evidence is not erased or made fresh by removing a watch.
    pub fn unwatch_session(&self, id: B256) -> Result<(), ChainError> {
        self.inner
            .state
            .lock()
            .map_err(|_| unavailable())?
            .sessions
            .remove(&id);
        Ok(())
    }
    pub fn estimated_timestamp(&self) -> Result<Duration, ChainError> {
        self.inner.estimated_timestamp(self.snapshot()?.as_ref())
    }
    pub fn readiness(&self, id: Option<B256>) -> Readiness {
        let current = self.snapshot().ok();
        self.readiness_checked(current.as_deref(), id)
    }
    /// Runtime admission must price and gate the same coherent observation.
    /// A superseded observation fails closed even when its age is acceptable.
    pub(crate) fn readiness_for_snapshot(
        &self,
        snapshot: &ChainSnapshot,
        id: Option<B256>,
    ) -> Readiness {
        let current = self.snapshot().ok();
        let same = current
            .as_deref()
            .filter(|value| std::ptr::eq(*value, snapshot));
        self.readiness_checked(same, id)
    }
    pub(crate) fn estimated_timestamp_for_snapshot(
        &self,
        snapshot: &ChainSnapshot,
    ) -> Result<Duration, ChainError> {
        let current = self.snapshot()?;
        if !std::ptr::eq(current.as_ref(), snapshot) {
            return Err(unavailable());
        }
        self.inner.estimated_timestamp(snapshot)
    }
    fn readiness_checked(&self, current: Option<&ChainSnapshot>, id: Option<B256>) -> Readiness {
        let failure = |reason: &str, code: PaymentCode, pause: Option<PauseLevel>| {
            Readiness::new(
                false,
                vec![
                    PaymentFailure::new(code, Some(reason.into()), id, None, None, None, pause)
                        .expect("fixed failure structure"),
                ],
            )
            .expect("fixed readiness structure")
        };
        let fail = |reason: &str| failure(reason, PaymentCode::SessionNotReady, None);
        let Some(current) = current else {
            return fail("CHAIN_STATE_STALE");
        };
        if current.pause_level != PauseLevel::None {
            return failure(
                "CONTRACT_PAUSED",
                PaymentCode::SessionNotReady,
                Some(current.pause_level),
            );
        }
        if let Some(id) = id {
            if !self
                .inner
                .state
                .lock()
                .map(|s| s.sessions.contains(&id))
                .unwrap_or(false)
            {
                return fail("STATE_UNAVAILABLE");
            }
            let Some(state) = current.sessions.get(&id) else {
                return fail("STATE_UNAVAILABLE");
            };
            if !state.exists() {
                return failure("SESSION_NOT_FOUND", PaymentCode::SessionNotFound, None);
            }
            match state.status() {
                SessionStatus::Closed => return fail("SESSION_CLOSED"),
                SessionStatus::Closing => return fail("CLOSE_PENDING"),
                SessionStatus::Open => {}
            }
            let Ok(now) = self.inner.estimated_timestamp(current) else {
                return fail("CHAIN_STATE_STALE");
            };
            let deadline =
                Duration::from_secs(state.last_activity()).checked_add(Duration::from_secs(86400));
            let margin = now.checked_add(self.inner.collection_margin);
            if deadline.is_none() || margin.is_none() || margin >= deadline {
                return fail("CLAIM_DEADLINE");
            }
        }
        Readiness::new(true, vec![]).expect("fixed ready structure")
    }
}
// A refresh future dropped while awaiting RPC must invalidate readiness. It
// must not invalidate a successful refresh from a newer start generation.
struct RefreshGuard<'a> {
    inner: &'a Inner,
    generation: u64,
    committed: bool,
}
impl Drop for RefreshGuard<'_> {
    fn drop(&mut self) {
        if !self.committed
            && let Ok(mut state) = self.inner.state.lock()
            && state.generation == self.generation
        {
            state.failed = true;
        }
    }
}
fn non_regressing(previous: &ChainSnapshot, current: &ChainSnapshot) -> Result<(), ChainError> {
    let old = &previous.block;
    let new = &current.block;
    if new.observation().block_number() < old.observation().block_number()
        || new.timestamp() < old.timestamp()
        || (new.observation().block_number() == old.observation().block_number() && new != old)
    {
        return Err(ChainError::InvalidEvidence(
            "canonical observation regressed or changed",
        ));
    }
    let order = |s| match s {
        SessionStatus::Open => 0,
        SessionStatus::Closing => 1,
        SessionStatus::Closed => 2,
    };
    for (id, state) in &current.sessions {
        if let Some(prior) = previous.sessions.get(id).filter(|s| s.exists())
            && (state.deposit() < prior.deposit()
                || state.settled_cumulative() < prior.settled_cumulative()
                || order(state.status()) < order(prior.status())
                || state.last_activity() < prior.last_activity()
                || state.initial_rate_card_hash() != prior.initial_rate_card_hash()
                || (prior.close_requested_at() != 0
                    && state.close_requested_at() != prior.close_requested_at()))
        {
            return Err(ChainError::InvalidEvidence(
                "confirmed session evidence regressed",
            ));
        }
    }
    Ok(())
}
impl Inner {
    fn snapshot(&self) -> Result<Arc<ChainSnapshot>, ChainError> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        let result = state.current.as_ref().ok_or_else(unavailable)?;
        let now = self.clock.monotonic();
        let wall = self.clock.wall()?;
        let block_time = Duration::from_secs(result.block.timestamp());
        if !state.running
            || state.failed
            || now < result.observed_at
            || now - result.observed_at >= self.max_age
            || now < state.progress_at
            || now - state.progress_at >= self.max_age
            || wall < block_time
            || wall - block_time >= self.max_age
        {
            return Err(unavailable());
        }
        Ok(result.clone())
    }
    fn estimated_timestamp(&self, current: &ChainSnapshot) -> Result<Duration, ChainError> {
        let elapsed = self
            .clock
            .monotonic()
            .checked_sub(current.observed_at)
            .ok_or_else(unavailable)?;
        let estimate = Duration::from_secs(current.block.timestamp())
            .checked_add(elapsed)
            .ok_or_else(unavailable)?;
        Ok(self.clock.wall()?.max(estimate))
    }
    async fn refresh(&self) -> Result<Arc<ChainSnapshot>, ChainError> {
        let _serial = self.refresh.lock().await;
        let (generation, providers, sessions, previous) = {
            let state = self.state.lock().map_err(|_| unavailable())?;
            if !state.running {
                return Err(unavailable());
            }
            (
                state.generation,
                state.providers.clone(),
                state.sessions.clone(),
                state.current.clone(),
            )
        };
        let mut guard = RefreshGuard {
            inner: self,
            generation,
            committed: false,
        };
        let block = self.reader.block(None).await?;
        if let Some(previous) = &previous {
            let old = &previous.block;
            if self
                .reader
                .block(Some(old.observation().block_number()))
                .await?
                != *old
            {
                return Err(ChainError::InvalidEvidence(
                    "accepted canonical block changed",
                ));
            }
            if old.observation().block_number().checked_add(1)
                == Some(block.observation().block_number())
                && block.parent_hash() != old.observation().block_hash()
            {
                return Err(ChainError::InvalidEvidence(
                    "canonical head ancestry mismatch",
                ));
            }
        }
        self.reader.verify_deployment(&block).await?;
        let pause_level = self.reader.pause_level(&block).await?;
        let mut identities = BTreeMap::new();
        for id in providers {
            identities.insert(id, self.reader.identity(id, &block, true).await?);
        }
        let mut states = BTreeMap::new();
        for id in sessions {
            states.insert(id, self.reader.session(id, &block).await?);
        }
        self.reader.check_canonical(&block).await?;
        let current = Arc::new(ChainSnapshot {
            block,
            pause_level,
            identities,
            sessions: states,
            observed_at: self.clock.monotonic(),
        });
        if let Some(previous) = &previous {
            non_regressing(previous, &current)?;
        }
        {
            let mut state = self.state.lock().map_err(|_| unavailable())?;
            if !state.running || state.generation != generation {
                return Err(unavailable());
            }
            if previous.as_ref().is_none_or(|p| {
                current.block.observation().block_number() > p.block.observation().block_number()
            }) {
                state.progress_at = current.observed_at;
            }
            state.current = Some(current);
            state.failed = false;
        }
        let result = self.snapshot()?;
        guard.committed = true;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Address,
        chain::{CodePin, DeploymentProfile, Rpc},
        types::{ChainObservation, Domain},
    };
    use serde_json::Value;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    const TIME: u64 = 1_800_000_000;
    struct TestClock {
        mono: AtomicU64,
        wall: AtomicU64,
    }
    impl Clock for TestClock {
        fn monotonic(&self) -> Duration {
            Duration::from_millis(self.mono.load(Ordering::SeqCst))
        }
        fn wall(&self) -> Result<Duration, ChainError> {
            Ok(Duration::from_millis(self.wall.load(Ordering::SeqCst)))
        }
    }
    struct PendingRpc {
        calls: AtomicUsize,
    }
    #[tonic::async_trait]
    impl Rpc for PendingRpc {
        async fn call(&self, _: &str, _: Value) -> Result<Value, ChainError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        }
    }
    fn setup() -> (ChainFollower, Arc<TestClock>, Arc<PendingRpc>) {
        let rpc = Arc::new(PendingRpc {
            calls: AtomicUsize::new(0),
        });
        let clock = Arc::new(TestClock {
            mono: AtomicU64::new(0),
            wall: AtomicU64::new(TIME * 1000),
        });
        let pin = |n| CodePin::new(Address::repeat_byte(n), B256::repeat_byte(n)).unwrap();
        let domain = Domain::new(5042002, Address::repeat_byte(1)).unwrap();
        let profile = DeploymentProfile::new(
            domain,
            Address::repeat_byte(3),
            pin(1),
            pin(2),
            pin(3),
            pin(4),
        )
        .unwrap();
        let follower = ChainFollower::with_clock(
            ChainReader::new(rpc.clone(), profile),
            10000,
            1000,
            Some(2500),
            clock.clone(),
        )
        .unwrap();
        let current = ChainSnapshot {
            block: Block::new(
                ChainObservation::new(5042002, Address::repeat_byte(1), 10, B256::repeat_byte(10))
                    .unwrap(),
                TIME,
                B256::repeat_byte(9),
            ),
            pause_level: PauseLevel::None,
            identities: BTreeMap::new(),
            sessions: BTreeMap::from([(
                B256::repeat_byte(5),
                session(100, 2, SessionStatus::Open, 0, TIME, B256::repeat_byte(6)),
            )]),
            observed_at: Duration::ZERO,
        };
        {
            let mut state = follower.inner.state.lock().unwrap();
            state.current = Some(Arc::new(current));
            state.sessions.insert(B256::repeat_byte(5));
            state.running = true;
            state.failed = false;
        }
        (follower, clock, rpc)
    }
    fn session(
        deposit: u64,
        paid: u64,
        status: SessionStatus,
        closing: u64,
        activity: u64,
        hash: B256,
    ) -> ChainSession {
        ChainSession::new(deposit, paid, status, closing, activity, hash).unwrap()
    }
    fn replace(follower: &ChainFollower, change: impl FnOnce(&mut ChainSnapshot)) {
        let mut state = follower.inner.state.lock().unwrap();
        let mut current = state.current.as_ref().unwrap().as_ref().clone();
        change(&mut current);
        state.current = Some(Arc::new(current));
    }
    #[test]
    fn cached_readiness_does_not_query_rpc_and_repeated_head_cannot_renew_progress() {
        let (follower, clock, rpc) = setup();
        for _ in 0..100 {
            assert!(follower.readiness(Some(B256::repeat_byte(5))).ready());
        }
        assert_eq!(rpc.calls.load(Ordering::SeqCst), 0);
        clock.mono.store(9000, Ordering::SeqCst);
        replace(&follower, |s| s.observed_at = Duration::from_secs(9));
        assert!(follower.snapshot().is_ok());
        clock.mono.store(10000, Ordering::SeqCst);
        assert!(follower.snapshot().is_err());
        assert!(follower.current().is_some());
    }
    #[test]
    fn coherent_readiness_rejects_superseded_evidence_and_released_watches() {
        let (follower, _, rpc) = setup();
        let id = B256::repeat_byte(5);
        let snapshot = follower.snapshot().unwrap();
        assert!(follower.readiness_for_snapshot(&snapshot, Some(id)).ready());
        assert!(follower.estimated_timestamp_for_snapshot(&snapshot).is_ok());
        replace(&follower, |_| {});
        assert!(!follower.readiness_for_snapshot(&snapshot, Some(id)).ready());
        assert!(
            follower
                .estimated_timestamp_for_snapshot(&snapshot)
                .is_err()
        );
        assert!(follower.readiness(Some(id)).ready());
        follower.unwatch_session(id).unwrap();
        assert!(!follower.readiness(Some(id)).ready());
        assert!(follower.current().unwrap().sessions().contains_key(&id));
        assert_eq!(rpc.calls.load(Ordering::SeqCst), 0);
        assert_eq!(follower.max_age_ms(), 10_000);
        assert_eq!(follower.collection_margin_ms(), 1_000);
    }
    #[test]
    fn wall_head_age_and_monotonic_regression_fail_closed() {
        for wall in [TIME * 1000 - 1, TIME * 1000 + 10000] {
            let (follower, clock, _) = setup();
            clock.wall.store(wall, Ordering::SeqCst);
            assert!(follower.snapshot().is_err());
        }
        let (follower, _, _) = setup();
        replace(&follower, |s| s.observed_at = Duration::from_millis(1));
        assert!(follower.snapshot().is_err());
    }
    #[test]
    fn lifecycle_pause_and_collection_margin() {
        let id = B256::repeat_byte(5);
        let cases = [
            (
                session(0, 0, SessionStatus::Open, 0, 0, B256::ZERO),
                "SESSION_NOT_FOUND",
            ),
            (
                session(100, 2, SessionStatus::Closed, 1, TIME, B256::repeat_byte(6)),
                "SESSION_CLOSED",
            ),
            (
                session(
                    100,
                    2,
                    SessionStatus::Closing,
                    1,
                    TIME,
                    B256::repeat_byte(6),
                ),
                "CLOSE_PENDING",
            ),
            (
                session(
                    100,
                    2,
                    SessionStatus::Open,
                    0,
                    TIME - 86400 + 1,
                    B256::repeat_byte(6),
                ),
                "CLAIM_DEADLINE",
            ),
        ];
        for (session, reason) in cases {
            let (follower, _, _) = setup();
            replace(&follower, |s| {
                s.sessions.insert(id, session);
            });
            assert_eq!(
                follower.readiness(Some(id)).failures()[0].reason(),
                Some(reason)
            );
        }
        for pause in [PauseLevel::Payments, PauseLevel::All] {
            let (follower, _, _) = setup();
            replace(&follower, |s| s.pause_level = pause);
            assert_eq!(
                follower.readiness(None).failures()[0].pause_level(),
                Some(pause)
            );
        }
        let (follower, _, _) = setup();
        assert_eq!(
            follower.readiness(Some(B256::ZERO)).failures()[0].reason(),
            Some("STATE_UNAVAILABLE")
        );
    }
    #[test]
    fn accepted_head_and_session_evidence_cannot_regress() {
        let (follower, _, _) = setup();
        let previous = follower.current().unwrap();
        let cases = [
            session(99, 2, SessionStatus::Open, 0, TIME, B256::repeat_byte(6)),
            session(100, 1, SessionStatus::Open, 0, TIME, B256::repeat_byte(6)),
            session(
                100,
                2,
                SessionStatus::Open,
                0,
                TIME - 1,
                B256::repeat_byte(6),
            ),
            session(100, 2, SessionStatus::Open, 0, TIME, B256::repeat_byte(7)),
        ];
        for session in cases {
            let mut next = previous.as_ref().clone();
            next.sessions.insert(B256::repeat_byte(5), session);
            assert!(non_regressing(&previous, &next).is_err());
        }
        let mut old = previous.as_ref().clone();
        old.sessions.insert(
            B256::repeat_byte(5),
            session(
                100,
                2,
                SessionStatus::Closing,
                1,
                TIME,
                B256::repeat_byte(6),
            ),
        );
        assert!(non_regressing(&old, &previous).is_err());
        let mut next = old.clone();
        next.sessions.insert(
            B256::repeat_byte(5),
            session(
                100,
                2,
                SessionStatus::Closing,
                2,
                TIME,
                B256::repeat_byte(6),
            ),
        );
        assert!(non_regressing(&old, &next).is_err());
        next = previous.as_ref().clone();
        next.block = Block::new(
            ChainObservation::new(5042002, Address::repeat_byte(1), 10, B256::repeat_byte(11))
                .unwrap(),
            TIME,
            B256::repeat_byte(9),
        );
        assert!(non_regressing(&previous, &next).is_err());
        next = previous.as_ref().clone();
        next.block = Block::new(
            ChainObservation::new(5042002, Address::repeat_byte(1), 9, B256::repeat_byte(9))
                .unwrap(),
            TIME,
            B256::repeat_byte(8),
        );
        assert!(non_regressing(&previous, &next).is_err());
    }
    #[tokio::test]
    async fn cancelled_refresh_retains_evidence_and_invalidates_readiness() {
        let (follower, _, rpc) = setup();
        let previous = follower.current().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), follower.refresh())
                .await
                .is_err()
        );
        assert_eq!(rpc.calls.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&previous, &follower.current().unwrap()));
        assert_eq!(
            follower.readiness(None).failures()[0].reason(),
            Some("CHAIN_STATE_STALE")
        );
        follower.stop().await.unwrap();
        assert!(follower.snapshot().is_err());
    }
    #[tokio::test]
    async fn cancelled_start_has_a_stoppable_task() {
        let (follower, _, _) = setup();
        follower.stop().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), follower.start())
                .await
                .is_err()
        );
        assert!(follower.task.lock().unwrap().is_some());
        assert!(!follower.readiness(None).ready());
        follower.stop().await.unwrap();
        assert!(follower.task.lock().unwrap().is_none());
    }
}
