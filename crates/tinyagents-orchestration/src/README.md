# `tinyagents-orchestration` — subagent orchestration

This crate owns subagent orchestration policy and job lifecycle. Generic tool
shells for its registry live in `tinyagents-harness-tools`, which depends on
this crate only under its `subagents` feature.

## Public surface

- `subagent::SubAgent` runs an `AgentHarness` as a child with inherited
  lineage, cancellation, events, host authority, and recursion limits.
- `subagent::SubAgentTool` starts a child asynchronously through typed
  parent-context tool dispatch and immediately returns a stable job id. Hosts
  Hosts that use the prepared lifecycle construct
  `tinyagents_harness_tools::subagents::DriverSubAgentTool` directly.
- `subagent::SubAgentJobRegistry` records queued, running, completed, failed,
  and cancelled jobs on top of `tinyagents_tasks::DetachedTaskRegistry`.
  `tinyagents_harness_tools::subagents::SubAgentJobsTool` queries them and
  `SubAgentMessageTool` sends messages to live children.
- `tinyagents_harness_tools::subagents::SubAgentJobsTool::with_sessions` overlays scoped durable sessions
  on list/query/close/await; `with_task_store` adds parent-run task records.
  `tinyagents_harness_tools::subagents::SubAgentMessageTool::with_sessions` accepts durable session ids for live
  steering. The host supplies parent-scope mapping closures so durable stores
  do not decide authorization.
- `subagent::MultiTargetDelegationTool` exposes a configured target enum and
  structured task envelope. Its `blocking` field maps to the standard `mode`
  field, and exposure can be set to `Hidden` for host-only tools.
- `subagent::DelegationTool` runs the graph plan/execute/review cycle through
  an injected stage worker. Its default keeps the one-shot execution and
  standalone `APPROVE` parser; `with_resumable_execution` opts checkpointed
  threads into classify-and-resume behavior. Hosts can retain their worker's
  approval decision with `ReviewApprovalPolicy::WorkerOutput`, project the
  complete state, pending approval, or graph error into a `ToolResult` through
  `with_result_projection`, and preserve declaration metadata with
  `with_metadata`.
- `parallel::ParallelFanoutTool` composes host task parsing, admission policy,
  asynchronous preflight, branch-context creation, and execution with ordered
  results and explicit rejection records.
- `subagent::DriverSubagentSessionStore` persists reusable child-session
  identity and history on the host document-store port. Hosts can continue by
  durable id, close sessions, and prevent stale task completions from replacing
  a newer assignment.
- `subagent::SubAgentSession` reuses one child and its transcript across turns.
- `subagent::SpawnPolicy` / `SpawnAdmission` bound child fan-out (per-parent
  live cap, per-root total budget, target allowlist) with atomic reservation;
  see `subagent/README.md`.
- `subagent::SubagentDriver` coordinates durable resume, preparation,
  execution, pause, and terminal persistence through host-supplied traits.
  It is the single engine for an agent step: team member steps and workflow
  agent children run through it too (`subagent::run_agent_step`), so they get
  the same spawn admission, policy, result policy and typed outcome.
- `steering::{steer_detached, steer_detached_with_request_id}` sends lane-checked
  steering to a live child handle, or delegates it to the host's
  `SteerFallback` when the handle is absent or the retained task has finished.
  `SteeringRegistry` remains in `tinyagents-tasks`, where
  `DetachedTaskRegistry` receives it at construction. `steering::cancel_for_thread`
  cancels registered children belonging to a parent thread.

The direct invocation implementation and its tool-focused tests live in
`subagent/invocation/`. Durable lifecycle files live beside it in `subagent/`.
End-to-end and live subagent tests live in the crate-level `tests/` directory,
and `tests/live_orchestrator_subagents.rs` exercises network-backed job-based
delegation.

## Status vocabularies

`OrchestrationTaskStatus` (crate `tinyagents-tasks`) is the one durable
lifecycle status for a subagent or task run. The other enums stay because their
serialized forms are persisted or consumed elsewhere, but each is expressed in
terms of it through `From` / `TryFrom` impls (`NoEquivalentStatus` is the error
of the fallible ones). The single mapping table, with the lossy cases, is in the
`tinyagents-tasks` README. `DetachedSubagentStatus` stays the live type that
carries a payload (output, question, error).

| Type | Crate | Role |
| --- | --- | --- |
| `OrchestrationTaskStatus` | tasks | Canonical durable status. |
| `DetachedSubagentStatus` | orchestration (`subagent::detached`) | Live status published to waiters, with payload. Not serialized. |
| `SubAgentJobStatus` | orchestration (`subagent::invocation`) | Status on `SubAgentJob` snapshots. |
| `SubagentOutcomeKind` | orchestration (`subagent`) | What a driver-level run returned, with its pause/incomplete payload. |
| `AgentRunStatus` | session (`run_ledger`) | Durable ledger row status. |
| `TranscriptSubagentStatus` | session (`transcript::view`) | Display projection read back from a transcript. |
| `CompletionStatus` | tasks (`completions`) | How a finished child ended, for the completion router. |

The old `SubagentStatus` names remain as `#[deprecated]` type aliases at their
original paths. `status::task_status_to_run_status` and
`status::run_status_to_task_status` are `#[deprecated]` in favour of
`AgentRunStatus::from` / `OrchestrationTaskStatus::from`. No wire format
changed; golden serde tests pin each one.

Dependency direction decides where an impl lives: ledger and transcript-view
conversions are in `tinyagents-session`, `CompletionStatus` in
`tinyagents-tasks`, and the job, detached and outcome ones beside their types
in this crate. `to_task_status()` / `to_run_status()` on the job and detached
types are kept as thin wrappers over the `From` impls.

## Boundaries

The crate owns subagent invocation and lifecycle (`subagent/`), durable team
coordination (`teams/`: `TeamService`, `TeamLedger`, `run_member_graph`) and
durable phase-DAG workflows (`workflow/`: `WorkflowEngine`, `WorkflowStore`).
Generic graph execution remains in `tinyagents-graph`, while provider calls,
tool dispatch mechanics, events, and run contexts remain in
`tinyagents-harness`. This crate composes those primitives into child-agent
behavior.

`subagent::SubagentDriver` is the single agent-step engine (#349). Team member
steps (`teams`) and workflow child steps (`workflow`) do not hand-roll their
own lifecycle; both run through `subagent::run_agent_step`, which adapts a
host-supplied worker onto the driver, so every agent step gets the same spawn
admission, policy, result policy and typed outcome. Workflow steps are the one
exception for structured output: `workflow::run_child_step` clears
`result_policy.schema` before calling `run_agent_step` and validates the
structured value itself (`workflow/child_step.rs`). See `teams/README.md` and
`workflow/README.md`.

Hosts remain responsible for agent definitions, model selection, credentials,
workspace policy, durable storage implementations, and authorization. Hosted
child invocations always re-enter through the exact capability bundle carried
by their parent context.
