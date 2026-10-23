//! Caller-owned per-thread reasoning preferences.
//!
//! A host may keep one store alongside its runtime and explicitly snapshot a
//! thread's effort at the start of a turn. The store does not read ambient
//! context or alter requests; hosts decide which turns (for example, root
//! turns only) consume the snapshot. Cloned handles share the same store,
//! while separately constructed stores remain isolated.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::{Result, TinyAgentsError};
use crate::ids::ThreadId;
use crate::tinyinference_llm::model::{ReasoningConfig, ReasoningEffort};

/// Reasoning preferences owned by one host/runtime instance and keyed by the
/// durable conversation thread id.
#[derive(Clone, Debug, Default)]
pub struct ThreadReasoningStore {
    efforts: Arc<RwLock<HashMap<ThreadId, ReasoningEffort>>>,
}

impl ThreadReasoningStore {
    /// Returns the configured effort for `thread_id`, if one is stored.
    #[must_use]
    pub fn get(&self, thread_id: &ThreadId) -> Option<ReasoningEffort> {
        self.efforts
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(thread_id)
            .copied()
    }

    /// Sets the effort for `thread_id`, returning its previous value.
    pub fn set_effort(
        &self,
        thread_id: &ThreadId,
        effort: ReasoningEffort,
    ) -> Option<ReasoningEffort> {
        self.efforts
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(thread_id.clone(), effort)
    }

    /// Removes the stored effort for `thread_id`, returning its previous value.
    pub fn clear(&self, thread_id: &ThreadId) -> Option<ReasoningEffort> {
        self.efforts
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(thread_id)
    }

    /// Applies a host-provided request to change a thread's effort.
    ///
    /// A missing thread id or request is a no-op. Empty, `default`, and `auto`
    /// values clear the existing choice. Other values use
    /// [`ReasoningEffort::parse`]. Invalid values leave the previous choice
    /// untouched and return a validation error. The previous value is returned
    /// for hosts that need to report whether the setting changed.
    pub fn apply_requested(
        &self,
        thread_id: Option<&ThreadId>,
        requested: Option<&str>,
    ) -> Result<Option<ReasoningEffort>> {
        let Some(thread_id) = thread_id else {
            return Ok(None);
        };
        let Some(requested) = requested else {
            return Ok(self.get(thread_id));
        };
        let value = requested.trim();
        if value.is_empty()
            || value.eq_ignore_ascii_case("default")
            || value.eq_ignore_ascii_case("auto")
        {
            return Ok(self.clear(thread_id));
        }
        let effort = ReasoningEffort::parse(value).ok_or_else(|| {
            TinyAgentsError::Validation(format!("invalid reasoning effort `{value}`"))
        })?;
        Ok(self.set_effort(thread_id, effort))
    }

    /// Snapshots this thread's effort into a provider-neutral request config.
    ///
    /// Hosts call this once when a turn starts and place the returned config in
    /// that turn's [`crate::runtime::RunPolicy::default_reasoning`]. Later store
    /// changes therefore affect only later turns. A missing thread or stored
    /// choice leaves the caller's policy unchanged.
    #[must_use]
    pub fn reasoning_for_turn(
        &self,
        thread_id: Option<&ThreadId>,
        max_output_tokens: Option<u32>,
    ) -> Option<ReasoningConfig> {
        let effort = self.get(thread_id?)?;
        Some(ReasoningConfig::effort(effort).with_output_room(max_output_tokens))
    }
}

#[cfg(test)]
#[path = "thread_reasoning_tests.rs"]
mod tests;
