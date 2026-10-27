//! Optional lifecycle callbacks; observers never authorize work or move funds.
use crate::{B256, proto, types::OperationResult};
use prost::Message;
use std::{
    collections::VecDeque,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

/// Sanitized boundary error. Prompt contents and upstream bodies are omitted.
#[derive(Clone, Debug)]
pub struct HookError {
    pub operation: &'static str,
    pub code: tonic::Code,
}

/// Where one payment step's time went, in milliseconds. `operation` is
/// `"admission"` for a paid request, from its arrival until its run is admitted
/// or refused (only a request whose run was registered reports), or
/// `"settle"`, `"settleBatch"` or `"close"` for one collection call, by the host
/// or by maintenance. `state` is `"admitted"`, or the collection result's
/// `OperationState::as_str`; `None` when the step was refused, failed or was
/// cancelled. `coverage_ms` is the host's coverage callback; `chain_ms` this
/// SDK's chain reads: a batch's simulation, the transaction's proofs and the
/// refresh after it; `wallet_ms` the wallet's `execute` or `reconcile` until it
/// returned. The measured parts are whole milliseconds rounded down;
/// `waiting_ms` is the rest of `total_ms`, never negative: the wait for the
/// agent's voucher, an earlier run's turn or an earlier wallet operation, and
/// local work. No prompt, payload or identifier is included.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct OperationTiming {
    pub operation: &'static str,
    pub state: Option<&'static str>,
    pub total_ms: u64,
    pub coverage_ms: u64,
    pub chain_ms: u64,
    pub wallet_ms: u64,
    pub waiting_ms: u64,
}

/// One serial, bounded observer worker. Callbacks receive owned snapshots.
/// A blocked callback can retain one detached worker; it never blocks shutdown.
#[derive(Clone, Default)]
pub struct Hooks {
    pub on_challenge: Option<Arc<dyn Fn(proto::Challenge) + Send + Sync>>,
    pub on_session_open: Option<Arc<dyn Fn(proto::SessionOpen) + Send + Sync>>,
    pub on_voucher: Option<Arc<dyn Fn(proto::Voucher) + Send + Sync>>,
    pub on_meter: Option<Arc<dyn Fn(proto::Meter) + Send + Sync>>,
    pub on_low_credit: Option<Arc<dyn Fn(proto::LowCredit) + Send + Sync>>,
    pub on_drain: Option<Arc<dyn Fn(proto::Drain) + Send + Sync>>,
    pub on_settle: Option<Arc<dyn Fn(OperationResult) + Send + Sync>>,
    pub on_close: Option<Arc<dyn Fn(proto::CloseResult) + Send + Sync>>,
    /// A final session left the runtime after its retention window.
    pub on_session_retired: Option<Arc<dyn Fn(B256) + Send + Sync>>,
    pub on_error: Option<Arc<dyn Fn(HookError) + Send + Sync>>,
    pub on_timing: Option<Arc<dyn Fn(OperationTiming) + Send + Sync>>,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct ObserverStats {
    pub delivered: u64,
    pub dropped: u64,
    pub failed: u64,
    pub queued: usize,
}

pub(super) enum Event {
    Challenge(proto::Challenge),
    SessionOpen(proto::SessionOpen),
    Voucher(proto::Voucher),
    Meter(proto::Meter),
    LowCredit(proto::LowCredit),
    Drain(proto::Drain),
    Settle(OperationResult),
    Close(proto::CloseResult),
    SessionRetired(B256),
    Error(HookError),
    Timing(OperationTiming),
}
impl Event {
    fn size(&self) -> usize {
        match self {
            Self::Challenge(value) => value.encoded_len(),
            Self::SessionOpen(value) => value.encoded_len(),
            Self::Voucher(value) => value.encoded_len(),
            Self::Meter(value) => value.encoded_len(),
            Self::LowCredit(value) => value.encoded_len(),
            Self::Drain(value) => value.encoded_len(),
            Self::Settle(value) => 1024usize
                .saturating_add(value.sessions().len().saturating_mul(256))
                .saturating_add(value.operation_ref().len())
                .saturating_add(value.wallet_operation_id().map_or(0, str::len)),
            Self::Close(value) => value.encoded_len(),
            Self::SessionRetired(_) => 32,
            Self::Error(_) | Self::Timing(_) => 128,
        }
    }
}
impl Hooks {
    fn accepts(&self, event: &Event) -> bool {
        match event {
            Event::Challenge(..) => self.on_challenge.is_some(),
            Event::SessionOpen(..) => self.on_session_open.is_some(),
            Event::Voucher(..) => self.on_voucher.is_some(),
            Event::Meter(..) => self.on_meter.is_some(),
            Event::LowCredit(..) => self.on_low_credit.is_some(),
            Event::Drain(..) => self.on_drain.is_some(),
            Event::Settle(..) => self.on_settle.is_some(),
            Event::Close(..) => self.on_close.is_some(),
            Event::SessionRetired(..) => self.on_session_retired.is_some(),
            Event::Error(..) => self.on_error.is_some(),
            Event::Timing(..) => self.on_timing.is_some(),
        }
    }
    fn dispatch(&self, event: Event) {
        match event {
            Event::Challenge(value) => {
                if let Some(callback) = &self.on_challenge {
                    callback(value);
                }
            }
            Event::SessionOpen(value) => {
                if let Some(callback) = &self.on_session_open {
                    callback(value);
                }
            }
            Event::Voucher(value) => {
                if let Some(callback) = &self.on_voucher {
                    callback(value);
                }
            }
            Event::Meter(value) => {
                if let Some(callback) = &self.on_meter {
                    callback(value);
                }
            }
            Event::LowCredit(value) => {
                if let Some(callback) = &self.on_low_credit {
                    callback(value);
                }
            }
            Event::Drain(value) => {
                if let Some(callback) = &self.on_drain {
                    callback(value);
                }
            }
            Event::Settle(value) => {
                if let Some(callback) = &self.on_settle {
                    callback(value);
                }
            }
            Event::Close(value) => {
                if let Some(callback) = &self.on_close {
                    callback(value);
                }
            }
            Event::SessionRetired(value) => {
                if let Some(callback) = &self.on_session_retired {
                    callback(value);
                }
            }
            Event::Error(value) => {
                if let Some(callback) = &self.on_error {
                    callback(value);
                }
            }
            Event::Timing(value) => {
                if let Some(callback) = &self.on_timing {
                    callback(value);
                }
            }
        }
    }
}
#[derive(Default)]
struct Queue {
    events: VecDeque<(Event, usize)>,
    bytes: usize,
    stats: ObserverStats,
    closed: bool,
    started: bool,
}
#[derive(Default)]
struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
}
pub(super) struct Observers {
    shared: Arc<Shared>,
    hooks: Arc<Hooks>,
}
impl Observers {
    pub(super) fn new(hooks: Hooks) -> Self {
        Self {
            shared: Arc::default(),
            hooks: Arc::new(hooks),
        }
    }
    pub(super) fn emit(&self, event: Event) {
        if !self.hooks.accepts(&event) {
            return;
        }
        let size = event.size();
        let Ok(mut queue) = self.shared.queue.lock() else {
            return;
        };
        if queue.closed || queue.events.len() >= 64 || size > 1048576 - queue.bytes {
            queue.stats.dropped = queue.stats.dropped.saturating_add(1);
            return;
        }
        if !queue.started {
            let shared = self.shared.clone();
            let hooks = self.hooks.clone();
            if std::thread::Builder::new()
                .name("mppi-observers".into())
                .spawn(move || {
                    loop {
                        let event = {
                            let Ok(mut queue) = shared.queue.lock() else {
                                return;
                            };
                            while queue.events.is_empty() && !queue.closed {
                                let Ok(next) = shared.ready.wait(queue) else {
                                    return;
                                };
                                queue = next;
                            }
                            if queue.closed {
                                return;
                            }
                            let (event, size) =
                                queue.events.pop_front().expect("nonempty observer queue");
                            queue.bytes -= size;
                            event
                        };
                        let failed =
                            catch_unwind(AssertUnwindSafe(|| hooks.dispatch(event))).is_err();
                        let Ok(mut queue) = shared.queue.lock() else {
                            return;
                        };
                        if failed {
                            queue.stats.failed = queue.stats.failed.saturating_add(1);
                        } else {
                            queue.stats.delivered = queue.stats.delivered.saturating_add(1);
                        }
                    }
                })
                .is_err()
            {
                queue.stats.failed = queue.stats.failed.saturating_add(1);
                return;
            }
            queue.started = true;
        }
        queue.bytes += size;
        queue.events.push_back((event, size));
        self.shared.ready.notify_one();
    }
    pub(super) fn stats(&self) -> ObserverStats {
        self.shared
            .queue
            .lock()
            .map(|queue| ObserverStats {
                queued: queue.events.len(),
                ..queue.stats
            })
            .unwrap_or_default()
    }
    pub(super) fn close(&self) {
        if let Ok(mut queue) = self.shared.queue.lock() {
            queue.closed = true;
            queue.stats.dropped = queue
                .stats
                .dropped
                .saturating_add(queue.events.len() as u64);
            queue.events.clear();
            queue.bytes = 0;
            self.shared.ready.notify_all();
        }
    }
}
impl Drop for Observers {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Clone, Copy)]
pub(super) enum Part {
    Coverage,
    Chain,
    Wallet,
}
/// Measures one payment step for `on_timing` (see `OperationTiming`). It
/// reports when dropped, so a step reports once whether it returns, fails or is
/// cancelled; `finish` names the state of a step that returned.
pub(super) struct Timing<'a> {
    observers: &'a Observers,
    operation: &'static str,
    start: Instant,
    /// Nanoseconds in each `Part`.
    parts: [AtomicU64; 3],
    state: Option<&'static str>,
}
impl<'a> Timing<'a> {
    pub(super) fn new(observers: &'a Observers, operation: &'static str, start: Instant) -> Self {
        Self {
            observers,
            operation,
            start,
            parts: Default::default(),
            state: None,
        }
    }
    /// Await `work`, adding its time to `part`, also when it is cancelled.
    pub(super) async fn part<T>(&self, part: Part, work: impl Future<Output = T>) -> T {
        struct Elapsed<'b>(&'b AtomicU64, Instant);
        impl Drop for Elapsed<'_> {
            fn drop(&mut self) {
                self.0.fetch_add(nanos(self.1.elapsed()), Ordering::Relaxed);
            }
        }
        let _elapsed = Elapsed(&self.parts[part as usize], Instant::now());
        work.await
    }
    pub(super) fn finish(mut self, state: Option<&'static str>) {
        self.state = state;
    }
}
impl Drop for Timing<'_> {
    fn drop(&mut self) {
        const MS: u64 = 1_000_000;
        // Parts are rounded down and the total to nearest: parts measured
        // within the step never add up to more than its total.
        let [coverage_ms, chain_ms, wallet_ms] = self
            .parts
            .each_ref()
            .map(|part| part.load(Ordering::Relaxed) / MS);
        let total_ms = nanos(self.start.elapsed()).saturating_add(MS / 2) / MS;
        self.observers.emit(Event::Timing(OperationTiming {
            operation: self.operation,
            state: self.state,
            total_ms,
            coverage_ms,
            chain_ms,
            wallet_ms,
            waiting_ms: total_ms
                .saturating_sub(coverage_ms)
                .saturating_sub(chain_ms)
                .saturating_sub(wallet_ms),
        }));
    }
}
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, time::Duration};

    #[test]
    fn observer_failure_does_not_stop_later_records() {
        let (sent, received) = mpsc::channel();
        let hooks = Hooks {
            on_meter: Some(Arc::new(move |mut value| {
                let seq = value.seq.unwrap();
                value.seq = Some(999);
                if seq == 1 {
                    panic!("application observer failure");
                }
                sent.send(seq).unwrap();
            })),
            ..Hooks::default()
        };
        let observer = Observers::new(hooks);
        let original = proto::Meter {
            seq: Some(1),
            ..Default::default()
        };
        observer.emit(Event::Meter(original.clone()));
        observer.emit(Event::Meter(proto::Meter {
            seq: Some(2),
            ..Default::default()
        }));
        assert_eq!(received.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
        assert_eq!(original.seq, Some(1));
        assert_eq!(observer.stats().failed, 1);
    }

    #[test]
    fn blocked_observer_has_bounded_queue_and_shutdown_never_joins_callback() {
        let (entered, waiting) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let blocked = Mutex::new(blocked);
        let observer = Observers::new(Hooks {
            on_meter: Some(Arc::new(move |_| {
                entered.send(()).unwrap();
                blocked
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap();
            })),
            ..Hooks::default()
        });
        observer.emit(Event::Meter(proto::Meter::default()));
        waiting.recv_timeout(Duration::from_secs(1)).unwrap();
        for _ in 0..65 {
            observer.emit(Event::Meter(proto::Meter::default()));
        }
        assert_eq!(observer.stats().queued, 64);
        assert_eq!(observer.stats().dropped, 1);
        observer.close();
        assert_eq!(observer.stats().queued, 0);
        assert_eq!(observer.stats().dropped, 65);
        release.send(()).unwrap();
    }

    /// Parts of 36.7, 27.7 and 58.7 ms in a step of 123.2 ms: each rounded to
    /// nearest, they would sum to 124, more than the step's 123.
    #[tokio::test(start_paused = true)]
    async fn a_steps_measured_parts_never_add_up_to_more_than_its_total() {
        let (sent, received) = mpsc::channel();
        let observers = Observers::new(Hooks {
            on_timing: Some(Arc::new(move |timing| sent.send(timing).unwrap())),
            ..Hooks::default()
        });
        let timing = Timing::new(&observers, "settle", tokio::time::Instant::now());
        for (part, micros) in [
            (Part::Coverage, 36_700),
            (Part::Chain, 27_700),
            (Part::Wallet, 58_700),
        ] {
            timing
                .part(part, tokio::time::advance(Duration::from_micros(micros)))
                .await;
        }
        tokio::time::advance(Duration::from_micros(100)).await;
        timing.finish(Some("confirmed"));
        assert_eq!(
            received.recv_timeout(Duration::from_secs(1)).unwrap(),
            OperationTiming {
                operation: "settle",
                state: Some("confirmed"),
                total_ms: 123,
                coverage_ms: 36,
                chain_ms: 27,
                wallet_ms: 58,
                waiting_ms: 2,
            }
        );
    }

    #[test]
    fn oversized_observer_record_is_dropped_before_starting_worker() {
        let observer = Observers::new(Hooks {
            on_meter: Some(Arc::new(|_| panic!("oversized record was delivered"))),
            ..Hooks::default()
        });
        observer.emit(Event::Meter(proto::Meter {
            session_id: Some(vec![0; 1048577]),
            ..Default::default()
        }));
        assert_eq!(observer.stats().dropped, 1);
        assert_eq!(observer.stats().queued, 0);
    }
}
