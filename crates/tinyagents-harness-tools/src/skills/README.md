# Tool packs

`PackCatalog` describes host-owned groups of tool names. `PackRegistryHandle`
binds that catalog to live tool registries. `UseSkillTool` exposes one compact
schema: a skill name alone discloses its tools; a skill, tool, and argument
object invoke one tool. The host owns membership, playbooks, and visibility.

## Permission and execution boundaries

`UseSkillTool::permission_level()` reports `ReadOnly`, the minimum permission
supported by schema disclosure. A pack that includes a write or a dangerous
tool therefore remains visible to a read-only session. This static declaration
does not authorize any particular packed invocation.

`permission_level_with_args()` resolves a named target and forwards the inner
arguments to its permission classifier. A missing named target reports
`Dangerous`, even when the bound registry contains only read tools. A request
without a tool name only discloses schemas and remains `ReadOnly`.
Result-only calls pin that admission permission too, so a wrapper cannot
reclassify an unresolved named call as disclosure after binding.

The harness binds an indirect target after preparing arguments and before
authorization. `PinnedPackCall` retains the selected implementation and its
arguments across authorization, wrap retries, and execution. Later registry
rebinding or wrapper argument changes cannot substitute a different tool.
The bound declaration preserves outer approval requirements and keeps inner
argument-aware permissions, external effects, and timeout policies.

`ToolDispatch::admission_target()` exposes that same bound implementation to
rules and execution observers. `Middleware::observe_tool_execution` receives
its concrete name and arguments only when the executor starts. The outer
transcript identity remains `use_skill`; schema disclosure and invalid targets
never create evidence that a named inner tool ran.

## Files and verification

`types.rs` holds catalog and pinned-target types; `tool.rs` owns disclosure,
dispatch, and forwarding; `handle.rs` keeps weak registry bindings; `render.rs`
renders disclosures. `mod_tests.rs` covers routing and binding, while
`mod_permissions_tests.rs` covers mixed-permission policy, read-only previews,
invalid targets, and pinned argument isolation.
