# `tinyagents-orchestration` — subagent lifecycle

This crate owns the host-neutral policy and lifecycle for asynchronous
subagents. Hosts provide planning, execution, persistence, authorization, and
product effects.

## Public surface

- `subagent::SubagentDriver` coordinates durable resume, preparation,
  execution, pauses, retries, and terminal persistence through host-supplied
  traits.
- `subagent::SubAgentJobRegistry` records spawned jobs and supports status,
  cancellation, steering, messages, and descendant settlement. The
  `DriverSubAgentTool` in `tinyagents-harness-tools` starts jobs and returns a
  task reference immediately; registry wait and detached wait APIs are
  separate host-facing operations.
- Detached lifecycle helpers in `subagent::detached` support completion
  routing and settlement, including `wait_all_detached_with_owners` for
  headless hosts.
- `subagent::SpawnPolicy` / `SpawnAdmission` bound child fan-out, and
  `SubAgentPolicy`, `SubAgentBudget`, and `SubAgentOutput` define execution
  policy and accounting.
- `DriverSubagentSessionStore` persists reusable child-session identity and
  history through a host document-store port.
- `steering` provides lane-checked control of live child runs.

## Status vocabularies

`OrchestrationTaskStatus` in `tinyagents-tasks` is the canonical durable task
status. `DetachedSubagentStatus`, `SubAgentJobStatus`, `SubagentOutcomeKind`,
`AgentRunStatus`, transcript status, and completion status retain their
role-specific payloads and serialized forms, with conversions documented in
`tinyagents-tasks`.

## Boundaries

This crate owns async subagent lifecycle only. Generic graph execution stays
in `tinyagents-orchestration`; tool dispatch, events, and run contexts stay in
`tinyagents-harness`. Hosts remain responsible for agent definitions, model
selection, credentials, workspace policy, storage implementations, and
authorization.
