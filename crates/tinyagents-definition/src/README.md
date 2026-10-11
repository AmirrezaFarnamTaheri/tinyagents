# `tinyagents-definition` — portable authored agent definitions

`AgentDefinition<X>` owns the provider-neutral TOML vocabulary: `id`,
`display_name`, `when_to_use`, model selection, tool and subagent scopes,
portable prompt sources, iteration policy, tier, skills, and execution caps.
The flattened extension `X` holds host-specific fields; the default
`Extensions` preserves unknown values. Filesystem roots, prompt composition,
credentials, authorization, and runtime installation remain with the host.

`ModelSpec` represents inherit, exact model, or routing hint. `pin()` returns
only a concrete exact model; `hint()` returns a separate routing hint.
`ToolScope::Named([])` explicitly denies all tools. Extra and deferred names
join the declared scope before exact exclusions; deferred tools enter discovery
instead of the initial wire catalogue. A hosted explicit wildcard policy
covers live toolsets as well as static registrations. Bare wildcard fallback
still belongs to the host. Hidden registrations cannot be promoted by deferral.

`PromptSource` supports inline text, a file path, or verbatim text. `AgentTier`
represents worker, chat coordinator, and reasoning coordinator. The legacy
worker-leaf spelling remains accepted. Legacy JSON `name`/`description`,
string models, and tool arrays are accepted on input; serialization emits the
rich vocabulary.

`diagnostics()` reports missing required fields, empty entries, and duplicate
tool/subagent names; `is_valid()` checks that no diagnostics remain.
`load_file()` requires a custom prompt, rejects parse and semantic failures,
and stamps file provenance only on success. `load_dir()` appends valid
lowercase `.toml` files non-recursively and skips invalid entries with warnings.

`DefinitionRegistry` provides asynchronous lookup, listing, and delegate
resolution. `InMemoryDefinitionRegistry` keeps first-write-wins identities
and insertion order. Execution uses host capabilities rather than a global
registry installed by this crate.
