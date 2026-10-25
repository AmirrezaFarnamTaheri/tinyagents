# OpenHuman adoption map for #481

This leaf moves only generic harness mechanics into TinyAgents. OpenHuman keeps
the product routing, tenant policy, capability catalog, filesystem roots, and
redaction rules.

| OpenHuman copy | TinyAgents API | OpenHuman work after the gitlink is merged |
| --- | --- | --- |
| `crates/openhuman-core/src/agent/tinyagents/use_skill_dispatch.rs` | `tinyagents_harness::tool::packs::{UseSkillDispatch, PackDispatchSelector, PackSelection}`. The dispatch pins one `PackSelection` at `bind_call`; the host selector builds its existing typed dispatch from `selection.registry()` and `selection.tool_name()`. That dispatch receives the original State, CallId, options, and live `RunContext`. | Delete `use_skill_dispatch.rs` and its `UseSkillDispatch`/`BoundUseSkillDispatch` implementation. Keep `tool_registration::typed_dispatch_for`, `shared_tool_adapter`, the catalog, registry binding, and the call site that registers `UseSkillDispatch`. The selector closure is host wiring, not generic dispatch logic. |
| `crates/openhuman-core/src/agent/tools/plan_exit.rs` | `tinyagents_harness_tools::PlanExitTool` validates `plan`, emits the stable marker, refuses a parked review, and switches to Build through the injected `RunModeStore` (which publishes the mode change). Its host seam resolves the opaque scope/thread identity. | Delete the schema, validation, result formatting, and marker implementation. Keep the host identity resolver, store wiring, and mode middleware wiring. `PlanExitTool::new` receives the host resolver, run-mode store, and optional plan-review store. |
| `crates/openhuman-core/src/agent/harness/artifact_offload/mod.rs` | `ArtifactOffload::{validate_materialization_target, materialize_to_fs}` and `artifacts::offload_oversized_result_to_fs`. The filesystem entry point checks the actual target through the installed `ArtifactPathPolicy` before the blob write, then materializes the stored redacted body with a same-directory atomic rename. Unix uses directory file descriptors, `mkdirat`/`openat`, and no-follow traversal; other platforms use `cap_std` directory capabilities for relative directory creation and rename. | Delete the host's pre-store target check and private filesystem `materialize` helper. Route oversized results through `offload_oversized_result_to_fs`. Keep `WorkspaceGuard`, `SanitizingRedactor`, scoped blob-store selection, root selection, prompt contract/tool names, and host logging/soft-failure integration. |

`UseSkillDispatch` currently requires `State: 'static` and `Ctx: 'static`, the
same constraint as the harness's existing `ToolSetDispatchBridge`, because the
call-scoped dispatch is held in an `Arc<dyn ToolDispatch<State, Ctx>>`.

TinyAgents' real `AgentHarness` tests register `UseSkillDispatch` and prove the
selected inner tool receives the same live parent and call ID; a second run
uses `RunPolicy.tool_rules` to deny the pinned inner tool and verifies typed
execution never runs. The pack tests also cover binding immutability across a
registry rebind and outer approval/timeout metadata preservation. Artifact
tests verify policy denial happens before any blob-store write or filesystem
change, reject a symlinked output ancestor, and exercise both atomic writer
implementations. PlanExit tests cover the returned mode handle and the host's
missing-handle case. OpenHuman should keep product integration tests for its
actual approval prompt, scoped tool visibility/revocation and pending-review
lookup, since those policies and host adapters remain OpenHuman-owned.
