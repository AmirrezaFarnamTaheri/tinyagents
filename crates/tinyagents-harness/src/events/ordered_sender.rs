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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;

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
}

impl<T: Send + 'static> OrderedSender<T> {
    /// Forward to `tx`; `None` makes [`send`](Self::send) a no-op.
    pub fn new(tx: Option<Sender<T>>) -> Self {
        Self {
            tx,
            overflow: Arc::default(),
        }
    }

    /// Whether a channel is attached.
    pub fn is_attached(&self) -> bool {
        self.tx.is_some()
    }

    /// Forward `item` without blocking and without reordering.
    pub fn send(&self, item: T) {
        let Some(tx) = &self.tx else {
            return;
        };
        // Hold the lock across the ordering decision so "are we draining?" and
        // the queue mutation are atomic (`try_send` never blocks).
        let mut ov = self.overflow.lock().unwrap_or_else(|p| p.into_inner());
        if ov.draining {
            ov.queue.push_back(item);
            return;
        }
        match tx.try_send(item) {
            Ok(()) | Err(TrySendError::Closed(_)) => {}
            Err(TrySendError::Full(item)) => {
                let Ok(handle) = tokio::runtime::Handle::try_current() else {
                    tracing::debug!(
                        "[ordered-sender] channel full and no runtime to defer send; dropping one item"
                    );
                    return;
                };
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
            }
        }
    }
}

#[cfg(test)]
#[path = "ordered_sender_tests.rs"]
mod tests;
