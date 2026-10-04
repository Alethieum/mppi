//! Cancellation-safe queue bounds for encoded application messages.
//!
//! A stalled consumer cannot grow this queue without bound. The embedding
//! runtime decides how a timeout affects its attachment and admitted work.

use crate::{ValidationCode, ValidationError};
use std::{collections::VecDeque, sync::Mutex, time::Duration};
use tokio::{
    sync::Notify,
    time::{Instant, timeout_at},
};

#[derive(Clone, Debug)]
pub enum QueueError {
    Full,
    Closed,
    TooLarge,
    TimedOut,
    Failed(tonic::Status),
}
impl core::fmt::Display for QueueError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Full => "queue is full",
            Self::Closed => "queue is closed",
            Self::TooLarge => "message exceeds queue byte limit",
            Self::TimedOut => "queue wait timed out",
            Self::Failed(_) => "queue failed",
        })
    }
}
impl std::error::Error for QueueError {}

#[derive(Default)]
struct State {
    items: VecDeque<Vec<u8>>,
    bytes: usize,
    closed: bool,
    error: Option<tonic::Status>,
}

pub struct ByteQueue {
    max_messages: usize,
    max_bytes: usize,
    wait: Duration,
    state: Mutex<State>,
    changed: Notify,
}
impl ByteQueue {
    pub fn new(
        max_messages: usize,
        max_bytes: usize,
        wait_ms: u64,
    ) -> Result<Self, ValidationError> {
        if !(1..=64).contains(&max_messages)
            || !(1..=crate::wire::MAX_MESSAGE_BYTES).contains(&max_bytes)
            || wait_ms == 0
            || wait_ms > (i64::MAX as u64) / 1_000_000
        {
            return Err(ValidationError::new(
                ValidationCode::InvalidValue,
                "invalid queue limits",
            ));
        }
        Ok(Self {
            max_messages,
            max_bytes,
            wait: Duration::from_millis(wait_ms),
            state: Mutex::new(State::default()),
            changed: Notify::new(),
        })
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>, QueueError> {
        self.state
            .lock()
            .map_err(|_| QueueError::Failed(tonic::Status::internal("queue state unavailable")))
    }
    /// Copy only after count and byte bounds allow enqueueing.
    pub fn put_nowait(&self, data: &[u8]) -> Result<(), QueueError> {
        let mut state = self.lock()?;
        if state.closed {
            return Err(QueueError::Closed);
        }
        if data.len() > self.max_bytes {
            return Err(QueueError::TooLarge);
        }
        if state.items.len() >= self.max_messages || data.len() > self.max_bytes - state.bytes {
            return Err(QueueError::Full);
        }
        state.items.push_back(data.to_vec());
        state.bytes += data.len();
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    /// Waiting is bounded by one fixed deadline. Dropping this future before it
    /// completes leaves the queue unchanged and releases the owned input.
    pub async fn put(&self, data: Vec<u8>) -> Result<(), QueueError> {
        let deadline = Instant::now()
            .checked_add(self.wait)
            .ok_or(QueueError::TimedOut)?;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.lock()?;
                if state.closed {
                    return Err(QueueError::Closed);
                }
                if data.len() > self.max_bytes {
                    return Err(QueueError::TooLarge);
                }
                if state.items.len() < self.max_messages
                    && data.len() <= self.max_bytes - state.bytes
                {
                    state.bytes += data.len();
                    state.items.push_back(data);
                    drop(state);
                    self.changed.notify_waiters();
                    return Ok(());
                }
            }
            timeout_at(deadline, changed)
                .await
                .map_err(|_| QueueError::TimedOut)?;
        }
    }
    /// Buffered messages drain before the terminal close/error is returned.
    pub async fn get(&self) -> Result<Vec<u8>, QueueError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.lock()?;
                if let Some(data) = state.items.pop_front() {
                    state.bytes -= data.len();
                    drop(state);
                    self.changed.notify_waiters();
                    return Ok(data);
                }
                if state.closed {
                    return Err(state
                        .error
                        .clone()
                        .map_or(QueueError::Closed, QueueError::Failed));
                }
            }
            changed.await;
        }
    }
    /// Idempotent: the first close retains its original terminal status.
    pub fn close(&self, error: Option<tonic::Status>) -> Result<(), QueueError> {
        let mut state = self.lock()?;
        if !state.closed {
            state.closed = true;
            state.error = error;
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    pub fn buffered(&self) -> Result<(usize, usize), QueueError> {
        let state = self.lock()?;
        Ok((state.items.len(), state.bytes))
    }
}
impl Default for ByteQueue {
    fn default() -> Self {
        Self::new(64, crate::wire::MAX_MESSAGE_BYTES, 1000).expect("valid queue defaults")
    }
}
