# OpenHuman adoption map for #481

This leaf moves only generic harness mechanics into TinyAgents. OpenHuman keeps
the product routing, tenant policy, capability catalog, filesystem roots, and
redaction rules.

| OpenHuman copy | TinyAgents API | OpenHuman work after the gitlink is merged |
| --- | --- | --- |
| `crates/openhuman-core/src/agent/tinyagents/use_skill_dispatch.rs` | `tinyagents_harness::tool::packs::{UseSkillDispatch, PackDispatchSelector, PackSelection}`. The dispatch pins one `PackSelection` at `bind_call`; the host selector builds its existing typed dispatch from `selection.registry()` and `selection.tool_name()`. That dispatch receives the original State, CallId, options, and live `RunContext`. | Delete `use_skill_dispatch.rs` and its `UseSkillDispatch`/`BoundUseSkillDispatch` implementation. Keep `tool_registration::typed_dispatch_for`, `shared_tool_adapter`, the catalog, registry binding, and the call site that registers `UseSkillDispatch`. The selector closure is host wiring, not generic dispatch logic. |
| `crates/openhuman-core/src/agent/tools/plan_exit.rs` | `tinyagents_harness::tool::plan_exit::{PlanExitTool, PlanExitModeLookup, PLAN_EXIT_MARKER}`. `PlanExitTool::new(Arc<PlanExitModeLookup>)` validates `plan`, emits the stable marker, and sets a returned `RunModeHandle` to `Build`. | Delete the schema, validation, result formatting, and marker implementation. Keep the callback that resolves the thread's live handle and checks tenant context plus a parked plan review before returning `Some(handle)`. Keep registration and mode middleware wiring. |
| `crates/openhuman-core/src/agent/harness/artifact_offload/mod.rs` | `ArtifactOffload::{validate_materialization_target, materialize_to_fs}` and `artifacts::offload_oversized_result_to_fs`. The filesystem entry point checks the actual target through the installed `ArtifactPathPolicy` before the blob write, then materializes the stored redacted body with a same-directory atomic rename. Unix uses directory file descriptors and no-follow traversal. | Delete the host's pre-store target check and private filesystem `materialize` helper. Route oversized results through `offload_oversized_result_to_fs`. Keep `WorkspaceGuard`, `SanitizingRedactor`, scoped blob-store selection, root selection, prompt contract/tool names, and host logging/soft-failure integration. |

`UseSkillDispatch` currently requires `State: 'static` and `Ctx: 'static`, the
same constraint as the harness's existing `ToolSetDispatchBridge`, because the
call-scoped dispatch is held in an `Arc<dyn ToolDispatch<State, Ctx>>`.

The host should retain its focused integration tests for actual approval,
timeout, tool visibility/revocation, and pending-review behavior. TinyAgents'
tests cover the generic pinning, live-parent forwarding, path-policy ordering,
and materialization mechanics without importing OpenHuman policy.
