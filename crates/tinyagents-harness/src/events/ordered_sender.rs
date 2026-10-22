//! Ordered, never-dropping forwarder from a synchronous listener to a bounded
//! async channel.
//!
//! [`EventListener::on_event`](crate::events::EventListener::on_event) is
//! synchronous, so a listener cannot `.await` a bounded `send()`. The fast path
//! is `try_send`, which stays synchronous and ordered whenever the channel has
//! room. When the channel is momentarily **full**, the item spills into an
//! ordered overflow queue drained by a single spawned task that awaits `send()`
//! per item in FIFO order. While any item is spilled, every later item queues
//! behind it, so a fast-path `try_send` can never overtake an earlier spilled
//! item and scramble start/completed ordering. A closed channel means the
//! receiver is gone, where dropping is correct.
//!
//! Nothing is lost silently. The overflow queue is bounded
//! ([`DEFAULT_MAX_OVERFLOW`]), and when an item cannot be queued (the bound is
//! reached, or the channel is full and there is no Tokio runtime to drain it)
//! [`OrderedSender::send`] hands it back in a [`SendFailure`] so the caller
//! decides what to do.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;

/// Default bound on items spilled behind a full channel.
pub const DEFAULT_MAX_OVERFLOW: usize = 4096;

/// Why [`OrderedSender::send`] did not accept an item; carries it back.
#[derive(Debug, PartialEq, Eq)]
pub enum SendFailure<T> {
    /// The receiver is gone; dropping is correct.
    Closed(T),
    /// The channel is full and the item could not be queued: the overflow
    /// bound was reached, or there is no Tokio runtime to drain a spill.
    Backpressure(T),
}

struct OverflowState<T> {
    queue: VecDeque<T>,
    draining: bool,
}

impl<T> Default for OverflowState<T> {
    fn default() -> Self {
        Self {
            queue: VecDeque::new(),
            draining: false,
        }
    }
}

/// Forwards items to `tx` in order, spilling under backpressure.
pub struct OrderedSender<T> {
    tx: Option<Sender<T>>,
    overflow: Arc<Mutex<OverflowState<T>>>,
    max_overflow: usize,
}

impl<T: Send + 'static> OrderedSender<T> {
    /// Forward to `tx`; `None` makes [`send`](Self::send) a no-op.
    pub fn new(tx: Option<Sender<T>>) -> Self {
        Self {
            tx,
            overflow: Arc::default(),
            max_overflow: DEFAULT_MAX_OVERFLOW,
        }
    }

    /// Bound the spill queue at `max` items (default [`DEFAULT_MAX_OVERFLOW`]).
    #[must_use]
    pub fn with_max_overflow(mut self, max: usize) -> Self {
        self.max_overflow = max;
        self
    }

    /// Whether a channel is attached.
    pub fn is_attached(&self) -> bool {
        self.tx.is_some()
    }

    /// Forward `item` without blocking and without reordering.
    ///
    /// Returns the item in a [`SendFailure`] when it was not accepted. With no
    /// channel attached it is a no-op that reports success.
    pub fn send(&self, item: T) -> Result<(), SendFailure<T>> {
        let Some(tx) = &self.tx else {
            return Ok(());
        };
        // Hold the lock across the ordering decision so "are we draining?" and
        // the queue mutation are atomic (`try_send` never blocks).
        let mut ov = self.overflow.lock().unwrap_or_else(|p| p.into_inner());
        if ov.draining {
            if ov.queue.len() >= self.max_overflow {
                tracing::warn!(
                    max = self.max_overflow,
                    "[ordered-sender] overflow queue full; rejecting one item"
                );
                return Err(SendFailure::Backpressure(item));
            }
            ov.queue.push_back(item);
            return Ok(());
        }
        match tx.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Closed(item)) => Err(SendFailure::Closed(item)),
            Err(TrySendError::Full(item)) => {
                let Ok(handle) = tokio::runtime::Handle::try_current() else {
                    tracing::warn!(
                        "[ordered-sender] channel full and no runtime to defer send; rejecting one item"
                    );
                    return Err(SendFailure::Backpressure(item));
                };
                if self.max_overflow == 0 {
                    return Err(SendFailure::Backpressure(item));
                }
                ov.queue.push_back(item);
                ov.draining = true;
                let overflow = Arc::clone(&self.overflow);
                let tx = tx.clone();
                drop(ov);
                handle.spawn(async move {
                    loop {
                        let next = {
                            let mut ov = overflow.lock().unwrap_or_else(|p| p.into_inner());
                            match ov.queue.pop_front() {
                                Some(item) => item,
                                None => {
                                    ov.draining = false;
                                    break;
                                }
                            }
                        };
                        if tx.send(next).await.is_err() {
                            let mut ov = overflow.lock().unwrap_or_else(|p| p.into_inner());
                            ov.queue.clear();
                            ov.draining = false;
                            break;
                        }
                    }
                });
                Ok(())
            }
        }
    }
}

#[cfg(test)]
#[path = "ordered_sender_tests.rs"]
mod tests;
