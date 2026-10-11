# Agent definitions

`tinyagents-definition` owns portable agent authoring types and a read-only
catalogue port. Hosts own bundled catalogues, directory selection, dynamic
prompt functions, security policy, and execution.

`AgentDefinition<X>` contains model selection, tool scope, portable prompt
sources, iteration policy, delegation entries, tiers, and provenance. Its
flattened `extensions: X` field defaults to a JSON map that preserves unknown
host fields. A host may supply a typed serde extension and skip runtime-only
fields, then use `with_extensions` to project into another extension type.

`load_file` reads a TOML definition and stamps its source path. `load_dir` reads
only direct `.toml` children and skips invalid files with diagnostics. Neither
chooses host directories nor maintains a global catalogue. `DefinitionRegistry`
and `InMemoryDefinitionRegistry` retain their async read-only lookup contract.

Model and tool scopes accept the richer TOML vocabulary and legacy JSON strings
or lists. Serialization emits the canonical vocabulary. Definition fields now
use `display_name` and `when_to_use`, accepting the old `name` and `description`
as input aliases. Source provenance remains runtime-only metadata. This Rust
API change requires consumers to replace direct string/list access with the
model and tool scope types.

`resolve_tool_scope` materializes extra tools and denials. An explicit empty
named scope denies every tool. A wildcard with exclusions requires a registered
catalogue to expand and otherwise fails closed. Plain wildcards retain the
host policy for undeclared tools. Skill selectors never add delegate ids.

`validate_tier_transition` rejects worker parents and same-tier coordinator
edges. It permits reasoning-to-chat handoffs. Orchestration maps both
coordinator tiers to its orchestrator role and workers to its worker role;
host spawn depth and authorization checks remain separate.

The tests include all 12 OpenHuman bundled definition TOMLs, generic loader and
serde tests, and a hosted empty-scope regression. Run:

```bash
cargo test -p tinyagents-definition
cargo test -p tinyagents-harness explicit_empty_tool_scope
```
