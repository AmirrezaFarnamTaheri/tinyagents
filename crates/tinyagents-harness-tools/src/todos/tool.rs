//! `todo` — the harness [`Tool`] over the per-thread todo list.
//!
//! One call writes the whole list: `{"todos": [{"content", "status"}]}`, and
//! starts its first open step. After that the model ticks steps off in order:
//! `{"complete": N}` completes step N (1-based) and starts the next,
//! `{"cancel": N, "reason": "..."}` drops one. The order rules are the
//! `sequence` module's. No arguments reads the current list. The list is bound to the
//! caller's [`ToolExecutionContext::thread_id`] (never a tool argument), so a
//! model can't address another thread's list; the bare [`Tool::call`] entry
//! point (no context) errors. Returns the updated items plus a markdown
//! rendering.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use super::store;
use super::types::{TodoItem, TodoStatus, TodosSnapshot, parse_status};
use tinyagents_harness::error::{Result, TinyAgentsError};
use tinyagents_harness::store::Store;
use tinytools::{Tool, ToolPolicy, ToolResult, ToolRunContext, ToolSideEffects};

pub(super) const TODO_TOOL_NAME: &str = "todo";

/// Paid on every request of every agent that carries the tool, so each rule
/// is stated once, tersely. The rules themselves are load-bearing: see the
/// `description_states_…` test for the failure each one prevents.
const TODO_DESCRIPTION: &str = "This thread's plan, for work with 3+ steps. Write it once with \
    `todos` (the complete list; it replaces the old one, and step 1 starts). Steps run one at a \
    time, in order. Tick a step with `{\"complete\": N}` (N from 1) only after its work has run \
    and its result is in the conversation, before starting the next: that call starts step N+1. \
    Drop a step with `{\"cancel\": N, \"reason\": \"...\"}`. No arguments reads the list. \
    Ticking is bookkeeping, not work: in the same response (or your next turn, if you cannot call \
    tools in parallel) do the next step unless the list is finished. Replace the list at most \
    once per assistant response.";

/// The `todo` harness [`Tool`], backed by a [`Store`](tinyagents_harness::store::Store).
pub struct TodoTool {
    store: Arc<dyn Store>,
}

/// One item as the model writes it. `status` accepts the Claude-style
/// `pending` / `in_progress` / `completed` plus the aliases [`parse_status`]
/// knows (`todo`, `done`, ...).
///
/// `content` is canonical, but models also name the item's text `title`,
/// `item`, `text`, `task` or `description` (all seen in production). Each is
/// its own field rather than a serde alias so an item carrying two of them is
/// not a "duplicate field" error: the first non-blank one in
/// [`TodoArg::text`]'s order wins. Unknown keys (`id`, `priority`) are
/// ignored.
#[derive(Deserialize)]
struct TodoArg {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    item: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    task: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

impl TodoArg {
    /// The item's text: `content`, else the first non-blank alias.
    fn text(&self) -> &str {
        [
            &self.content,
            &self.title,
            &self.item,
            &self.text,
            &self.task,
            &self.description,
        ]
        .into_iter()
        .filter_map(|field| field.as_deref().map(str::trim))
        .find(|text| !text.is_empty())
        .unwrap_or_default()
    }
}

impl TodoTool {
    /// Creates the `todo` tool backed by `store`.
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }

    async fn dispatch(&self, thread_id: &str, args: &Value) -> Result<TodoOutcome> {
        let Some(args) = args.as_object() else {
            return Ok(TodoOutcome::Error(
                "arguments must be an object".to_string(),
            ));
        };
        let unknown = |allowed: &[&str]| {
            let extra: Vec<&String> = args
                .keys()
                .filter(|key| !allowed.contains(&key.as_str()))
                .collect();
            (!extra.is_empty()).then(|| {
                TodoOutcome::Error(format!(
                    "unknown arguments {extra:?} next to {:?}",
                    allowed[0]
                ))
            })
        };
        if let Some(step) = args.get("complete") {
            if let Some(error) = unknown(&["complete"]) {
                return Ok(error);
            }
            let step = match step_number(step) {
                Ok(step) => step,
                Err(message) => return Ok(TodoOutcome::Error(message)),
            };
            return Ok(outcome(store::complete(&self.store, thread_id, step).await));
        }
        if let Some(step) = args.get("cancel") {
            if let Some(error) = unknown(&["cancel", "reason"]) {
                return Ok(error);
            }
            let step = match step_number(step) {
                Ok(step) => step,
                Err(message) => return Ok(TodoOutcome::Error(message)),
            };
            let reason = args
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or_default();
            return Ok(outcome(
                store::cancel(&self.store, thread_id, step, reason).await,
            ));
        }
        let snap = match args.get("todos") {
            None if args.is_empty() => store::list(&self.store, thread_id).await,
            None => {
                return Ok(TodoOutcome::Error(format!(
                    "unknown arguments {:?}: pass `todos`, `complete`, `cancel`, or no arguments \
                     to read the list",
                    args.keys().collect::<Vec<_>>()
                )));
            }
            Some(Value::Null) if args.len() == 1 => store::list(&self.store, thread_id).await,
            Some(Value::Null) => {
                return Ok(TodoOutcome::Error(format!(
                    "unknown arguments {:?}: pass only `todos` to read the list",
                    args.keys().collect::<Vec<_>>()
                )));
            }
            Some(raw) => {
                let items = match parse_items(raw) {
                    Ok(items) => items,
                    Err(message) => return Ok(TodoOutcome::Error(message)),
                };
                store::replace(&self.store, thread_id, items).await
            }
        };
        Ok(outcome(snap))
    }
}

/// A store outcome as the model reads it. A domain error (an order the plan
/// refuses) is surfaced to the model rather than failing the whole run.
fn outcome(snap: Result<TodosSnapshot>) -> TodoOutcome {
    match snap {
        Ok(snap) => {
            let mut payload = json!({
                "threadId": snap.thread_id,
                "todos": snap.items,
                "markdown": snap.markdown,
            });
            if !snap.notes.is_empty() {
                payload["notes"] = json!(snap.notes);
            }
            TodoOutcome::Ok(payload)
        }
        Err(TinyAgentsError::Validation(message)) => TodoOutcome::Error(message),
        Err(e) => TodoOutcome::Error(e.to_string()),
    }
}

/// A 1-based step number.
fn step_number(raw: &Value) -> std::result::Result<usize, String> {
    match raw.as_u64() {
        Some(step) if step >= 1 => Ok(step as usize),
        _ => Err(format!("a step number is a positive integer, got {raw}")),
    }
}

/// Internal dispatch outcome: a structured payload or a model-facing error.
enum TodoOutcome {
    Ok(Value),
    Error(String),
}

/// Decodes the model's `todos` array into items, rejecting blank content and
/// unknown statuses with a message the model can act on.
fn parse_items(raw: &Value) -> std::result::Result<Vec<TodoItem>, String> {
    let args: Vec<TodoArg> =
        serde_json::from_value(raw.clone()).map_err(|e| format!("invalid `todos`: {e}"))?;
    let mut items = Vec::with_capacity(args.len());
    for arg in args {
        let content = arg.text();
        if content.is_empty() {
            return Err("every todo needs non-empty `content`".to_string());
        }
        let status = match arg.status.as_deref() {
            None => TodoStatus::Pending,
            Some(raw) => parse_status(raw)?,
        };
        items.push(TodoItem::with_status(content, status));
    }
    Ok(items)
}

fn parameters_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "todos": {
                "type": ["array", "null"],
                "description": "The full plan, in order. Pass null to read the current list.",
                "items": {
                    "type": "object",
                    // `content` is the advertised field. It is not
                    // `required`: the tool also reads the `title` / `task` /
                    // ... spellings models drift to (see `TodoArg`), and a
                    // schema rejection here would stop the call before the
                    // tool could. A blank item is still refused, by the tool.
                    "properties": {
                        "content": { "type": "string" },
                        "status": {
                            "type": "string",
                            "enum": [
                                "pending", "todo", "open", "not_started",
                                "in_progress", "in-progress", "inprogress", "started", "active",
                                "completed", "complete", "done", "finished",
                                "cancelled", "canceled", "skipped"
                            ]
                        }
                    }
                }
            },
            "complete": {
                "type": "integer",
                "minimum": 1,
                "description": "Complete step N (from 1) and start the next one."
            },
            "cancel": {
                "type": "integer",
                "minimum": 1,
                "description": "Cancel step N (from 1); needs `reason`."
            },
            "reason": { "type": "string" }
        },
        "additionalProperties": false
    })
}

fn error_result(message: impl Into<String>) -> ToolResult {
    ToolResult::error(message)
}

#[async_trait]
impl Tool for TodoTool {
    fn name(&self) -> &str {
        TODO_TOOL_NAME
    }

    fn description(&self) -> &str {
        TODO_DESCRIPTION
    }

    fn parameters_schema(&self) -> Value {
        parameters_schema()
    }

    fn policy(&self) -> ToolPolicy {
        ToolPolicy {
            classified: true,
            side_effects: ToolSideEffects {
                read_only: false,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        Ok(error_result(
            "todo tool requires an active thread (no thread_id in tool context)",
        ))
    }

    async fn execute_with_context(
        &self,
        args: Value,
        _options: tinytools::ToolCallOptions,
        context: Option<&dyn ToolRunContext>,
    ) -> anyhow::Result<ToolResult> {
        let Some(thread_id) = context.and_then(ToolRunContext::thread_id) else {
            return Ok(error_result(
                "todo tool requires an active thread (no thread_id in tool context)",
            ));
        };
        match self.dispatch(thread_id, &args).await? {
            TodoOutcome::Ok(payload) => Ok(ToolResult::json(payload)),
            TodoOutcome::Error(message) => Ok(error_result(message)),
        }
    }
}
