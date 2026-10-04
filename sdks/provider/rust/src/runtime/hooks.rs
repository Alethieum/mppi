//! Optional lifecycle callbacks; observers never authorize work or move funds.
use crate::{proto, types::OperationResult};
use prost::Message;
use std::{
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex},
};

/// Sanitized boundary error. Prompt contents and upstream bodies are omitted.
#[derive(Clone, Debug)]
pub struct HookError {
    pub operation: &'static str,
    pub code: tonic::Code,
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
    pub on_error: Option<Arc<dyn Fn(HookError) + Send + Sync>>,
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
    Error(HookError),
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
            Self::Error(_) => 128,
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
            Event::Error(..) => self.on_error.is_some(),
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
            Event::Error(value) => {
                if let Some(callback) = &self.on_error {
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
