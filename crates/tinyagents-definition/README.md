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
only direct `.toml` children and skips invalid files with diagnostics. These
default entry points retain strict semantic validation. The explicit
`load_file_with_validation` and `load_dir_with_validation` entry points accept
`DefinitionValidation::NonemptyInlinePrompt` for hosts that must preserve older
authored prompt admission: omitted or empty inline prompts are rejected, while
inline whitespace and file/verbatim contents are preserved. Neither
chooses host directories nor maintains a global catalogue. `DefinitionRegistry`
and `InMemoryDefinitionRegistry` retain their async read-only lookup contract.

Model and tool scopes accept the richer TOML vocabulary and legacy JSON strings
or lists. Serialization emits the canonical vocabulary. Definition fields now
use `display_name` and `when_to_use`, accepting the old `name` and `description`
as input aliases. Source provenance remains runtime-only metadata. This Rust
API change requires consumers to replace direct string/list access with the
model and tool scope types.

`resolve_tool_scope` includes authored extra and deferred tools, then applies
the shared case-insensitive deny globs and stable deduplication. The explicit
`resolve_tool_scope_with` entry point lets hosts choose whether to include those
additions and whether to materialize a bare wildcard. Materialization needs a
catalogue and fails closed without one. An empty named scope with no admitted
additions stays empty. Skill selectors never add delegate ids.

An absent routing `role` stays absent in serialized payloads. Hosts can mark a
runtime-built prompt with `PromptSource::Dynamic(())`; its serialized inert
marker contains no callback and cannot be authored through deserialization.
The executable builder stays in the host's typed extension. `with_extensions`
preserves every generic definition field when replacing that extension.

`validate_tier_transition` rejects worker parents and same-tier coordinator
edges. It permits reasoning-to-chat handoffs. Orchestration maps both
coordinator tiers to its orchestrator role and workers to its leaf role;
host spawn depth and authorization checks remain separate.

The tests include all 12 OpenHuman bundled definition TOMLs, generic loader and
serde tests, and a hosted empty-scope regression. Run:

```bash
cargo test -p tinyagents-definition
cargo test -p tinyagents-harness explicit_empty_tool_scope
```

Routing hints stay separate from exact model pins: ModelSpec::pin returns only
an Exact selection, while hint returns the authored Hint value. The legacy
resolve helper retains its hint-prefixed alias for callers using that vocabulary.
Named tool scopes include extra and deferred names before deny filtering and
deduplication. Wildcards with additions materialize a supplied catalogue;
without one they fail closed. A bare wildcard retains the host fallback policy.


The bundled compatibility fixtures preserve authored fields and iteration caps.
The orchestrator fixture removes one redundant `run_workflow` allowlist entry:
membership and rendered tool schemas are unchanged, and every fixture passes
semantic validation. Hosts migrating the original bundled source should make
the same duplicate-entry cleanup before invoking it through the new vocabulary.
