# `tinyagents-harness-tools`

This crate provides reusable tool implementations and stateful support for
`tinyagents-harness`. It keeps product policy and presentation outside the
agent loop while making common harness capabilities available to Rust hosts.

## Features

The default feature set enables `todos`, `goals`, and `skills`. The `subagents`
feature enables the generic job and message tools and brings in
`tinyagents-orchestration` and its task registry dependencies. Time,
clarification, plan-exit, per-call binding, and thread-lock support are
available without optional features.

The opt-in `fs`, `net`, and `exec` features expose the standard tool families
under `tinyagents_harness_tools::{fs,net,exec}`. The `media` feature exposes
the generic artifact-filing wrapper at `tinyagents_harness_tools::media`, and
`plan-review` exposes the host-backed review tool at
`tinyagents_harness_tools::plan_review`. These features do not change the
default set.

## Public modules and tools

- `goals` provides `GoalStore`, `GoalTool`, observer notifications, atomic
  turn accounting, budget guards, and goal context. A host calls
  `GoalStore::prepare_turn` and composes a budget stop hook for the turn.
- `todos` provides `TodoTool`, the persistent sequence model, and
  `TodoSequenceMiddleware`.
- `skills` provides `UseSkillTool`, `PackCatalog`, `PackRegistryHandle`, and
  `UseSkillDispatch` for binding a selected tool from a named pack.
- `subagents` provides `DriverSubAgentTool`, `SubAgentJobsTool`, and
  `SubAgentMessageTool` over the orchestration crate's shared
  `SubAgentJobRegistry`.
- `thread_locks` provides the public `ThreadLockMap` concurrency primitive.
- `fs` provides filesystem tools and file-state operations in `fs`.
- `net` provides network tools in `net`.
- `exec` provides `ExecTool`, `InstallTool`, process timeout utilities, and
  host-supplied execution ports in `exec`.
- `media` provides `MediaArtifactTool` and its `ArtifactFiling` port in
  `media`.
- `plan-review` provides `RequestPlanReviewTool` and its `PlanReviewPort` in
  `plan_review`.
- The crate root exports `CurrentTimeTool`, `ResolveTimeTool`,
  `AskClarificationTool`, `PlanExitTool`, and the `per_call` binding adapter.

Subagent registry policy stays in `tinyagents-orchestration`; this crate owns
the reusable tool declarations. The harness owns tool registration,
admission, schema preparation, discovery, and dispatch infrastructure.
