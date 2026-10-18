//! Goal lifecycle observers and the shared budget-meter publish threshold.

use std::collections::HashMap;
use std::sync::Mutex;

use super::types::{ThreadGoal, ThreadGoalStatus};

/// Receives persisted goal lifecycle changes from [`super::GoalStore`].
pub trait GoalObserver: Send + Sync {
    /// A goal was created or updated.
    fn on_updated(&self, goal: &ThreadGoal);

    /// A goal was removed.
    fn on_cleared(&self, thread_id: &str);
}

#[derive(Clone, Copy)]
struct PublishedGoal {
    goal_id: String,
    status: ThreadGoalStatus,
    tokens_used: u64,
}

/// Decides when account updates should refresh a budget meter.
///
/// Status and goal-version changes publish immediately. Otherwise, a budgeted
/// goal publishes after cumulative token usage since its previous publish
/// reaches five percent of its budget (at least one token). Goals without a
/// token budget publish each account update.
#[derive(Default)]
pub struct GoalUpdateThrottle {
    published: Mutex<HashMap<String, PublishedGoal>>,
}

impl GoalUpdateThrottle {
    /// Returns whether `updated` should be published after an account operation.
    pub fn should_publish(&self, before: &ThreadGoal, updated: &ThreadGoal) -> bool {
        let mut published = self
            .published
            .lock()
            .unwrap_or_else(|_| panic!("goal update throttle poisoned"));
        let previous = published
            .entry(updated.thread_id.clone())
            .or_insert_with(|| PublishedGoal {
                goal_id: before.goal_id.clone(),
                status: before.status,
                tokens_used: before.tokens_used,
            });

        if previous.goal_id != updated.goal_id || previous.status != updated.status {
            *previous = PublishedGoal {
                goal_id: updated.goal_id.clone(),
                status: updated.status,
                tokens_used: updated.tokens_used,
            };
            return true;
        }

        let Some(budget) = updated.token_budget else {
            return updated.tokens_used != before.tokens_used
                || updated.time_used_seconds != before.time_used_seconds
                || updated.time_used_subsec_nanos != before.time_used_subsec_nanos;
        };
        let threshold = (budget / 20).max(1);
        if updated.tokens_used.saturating_sub(previous.tokens_used) >= threshold {
            previous.tokens_used = updated.tokens_used;
            true
        } else {
            false
        }
    }

    /// Resets the meter baseline after a non-account lifecycle update.
    pub fn reset(&self, goal: &ThreadGoal) {
        self.published
            .lock()
            .unwrap_or_else(|_| panic!("goal update throttle poisoned"))
            .insert(
                goal.thread_id.clone(),
                PublishedGoal {
                    goal_id: goal.goal_id.clone(),
                    status: goal.status,
                    tokens_used: goal.tokens_used,
                },
            );
    }

    /// Drops the published baseline when a goal is cleared.
    pub fn clear(&self, thread_id: &str) {
        self.published
            .lock()
            .unwrap_or_else(|_| panic!("goal update throttle poisoned"))
            .remove(thread_id);
    }
}
