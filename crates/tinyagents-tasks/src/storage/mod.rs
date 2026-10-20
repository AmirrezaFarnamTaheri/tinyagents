//! The tasks crate's v2 storage declarations: its `storage_schema()`, the
//! stored forms of task and completion records, and the steps that bring the v1
//! collections forward.

mod dto;
mod steps;

use tinystoragedrivers_core::{CollectionSpec, IndexSpec, Owner, Schema, ScopeClass};

pub(crate) use dto::{CompletionDoc, TaskDoc, history_id_key, task_id_key, to_ms};

/// The body version of every `tasks.*` collection a v1 -> v2 step produces.
pub(crate) const SCHEMA_VERSION: u32 = 2;

/// The latest record per task.
pub(crate) const RECORDS: &str = "tasks.records";

/// One immutable snapshot per task transition.
pub(crate) const HISTORY: &str = "tasks.history";

/// The latest completion record per child task.
pub(crate) const COMPLETIONS: &str = "tasks.completions";

/// `tasks.records`: the scalar fields [`TaskStore::list`](crate::TaskStore::list)
/// filters on are indexed.
pub(crate) fn records_spec() -> CollectionSpec {
    CollectionSpec::new(RECORDS)
        .index(IndexSpec::new("status", ["status"]))
        .index(IndexSpec::new("root", ["root_run_id"]))
        .index(IndexSpec::new("parent", ["parent_run_id"]))
        .index(IndexSpec::new("thread", ["thread_id"]))
        .index(IndexSpec::new("node", ["node_id"]))
        .index(IndexSpec::new("kind", ["kind_label"]))
}

/// `tasks.history`: one row per `(task, revision)`.
pub(crate) fn history_spec() -> CollectionSpec {
    CollectionSpec::new(HISTORY).index(IndexSpec::new("task_rev", ["task_id", "rev"]).unique())
}

/// `tasks.completions`.
pub(crate) fn completions_spec() -> CollectionSpec {
    CollectionSpec::new(COMPLETIONS)
        .index(IndexSpec::new("by_parent", ["parent_key"]))
        .index(IndexSpec::new("by_state", ["state"]))
}

/// The tasks crate's storage declarations: what it stores and the steps that
/// bring the v1 names and shapes forward. The host composes it with
/// `Registry::compose` and runs `Registry::migrate` per scope.
///
/// | v1 | v2 |
/// |---|---|
/// | `orchestration_tasks` | `tasks.records` |
/// | `orchestration_task_history` | `tasks.history` |
/// | `orchestration_completions` | `tasks.completions` |
///
/// Ids become `Key::of` ids (`[task_id]`, `[task_id, rev]`), bodies are bare
/// snake_case with `*_ms` times, and the last-update time moves to the
/// document's `updated_ms` metadata. A migrated record's `updated_ms` is the
/// migration time, because a step can keep a first-write time but not an
/// update time.
#[must_use]
pub fn storage_schema() -> Schema {
    let owner = Owner::new("tasks").expect("`tasks` is a valid owner");
    Schema::new(owner)
        .collection_with(SCHEMA_VERSION, records_spec(), ScopeClass::Agent)
        .collection_with(SCHEMA_VERSION, history_spec(), ScopeClass::Agent)
        .collection_with(SCHEMA_VERSION, completions_spec(), ScopeClass::Agent)
        .step(steps::records())
        .step(steps::history())
        .step(steps::completions())
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
