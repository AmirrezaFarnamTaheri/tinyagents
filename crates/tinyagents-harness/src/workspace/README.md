# harness::workspace

Workspace isolation and sandbox hooks for tools that run over real files or
command executors.

## Why this exists

Application-specific worktree/sandbox providers need a common seam: a
`tinytools::WorkspaceDescriptor` tells a tool which filesystem root it may
touch, and a [`WorkspaceIsolation`] provider trait prepares/tears down a
per-agent environment. TinyAgents does not own any concrete isolation policy
— it owns the interface and a trivial single-shared-root provider.

## Public surface

- [`WorkspaceIsolation`] — the provider trait: `prepare(run_id, agent) ->
  WorkspaceDescriptor` and `cleanup(&descriptor)`.
- [`prepare_workspace`] / [`cleanup_workspace`] — free functions that drive a
  `WorkspaceIsolation` provider *and* emit the corresponding
  `AgentEvent::WorkspacePrepared` / `AgentEvent::WorkspaceCleanup` on the
  run's event sink, so isolation setup/teardown is observable.
- [`SharedRootWorkspace`] — the trivial provider: scopes every agent to one
  shared root without per-agent copying (`prepare` is a descriptor
  construction, `cleanup` is a no-op). A sensible default and a test double.
- [`enforce_workspace_path`] — the fail-closed path gate a tool calls
  *before* touching a path: emits `AgentEvent::WorkspaceViolation` and returns
  a validation error when the path is outside every allowed root.
## Files

| File | Role |
| --- | --- |
| `mod.rs` | `WorkspaceIsolation` driving helpers (`prepare_workspace`/`cleanup_workspace`) and `SharedRootWorkspace`. |
| `types.rs` | The `WorkspaceIsolation` trait. |
| `policy.rs` | `enforce_workspace_path`, the fail-closed path gate (paired with the descriptor's lexical `allows` check, which lives in `tinytools`). |
| `mod_tests.rs` | Tests for the descriptor-allows and `SharedRootWorkspace`/event-emission hooks. |

## Operational constraints

- `WorkspaceDescriptor`'s allowed-root policy and its lexical `allows` check
  live in `tinytools`, which owns the tool vocabulary; this module only
  supplies the event-emitting wrapper (`enforce_workspace_path`) and the
  isolation providers.
