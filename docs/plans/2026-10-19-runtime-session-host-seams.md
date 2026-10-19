# Runtime and session host seams

Issue: `senamakel/tinyagents#63`, under `tinyhumansai/openhuman#8088`.

This plan moves generic session behavior into TinyAgents and leaves OpenHuman
with adapters for tenant authority, SaaS storage leases, approvals, progress,
memory, and learning. Implementation starts only after the integrated
`senamakel/tinyagents#60` changes are merged into this fork branch and the
affected baseline has been re-audited. The starting worktree currently contains
the `#57`, `#59`, and `#67` changes; do not recreate their types or lifecycle.

The source state already provides `SessionRef`, `TranscriptMeta`,
`RebindableSessionStores`, `WorkspaceStores`, `CommitReceipt`,
`SessionHooks::after_commit`, harness `TurnSummary` and `ToolOutcomeRecord`,
runtime tool admission, and harness `ToolSnapshot`. In particular,
`ToolSnapshot::new` rejects different specs sharing a name; it is not the
first-wins deduplicator this migration needs.

## Boundaries and invariants

* Keep session identity and transcript repair in `tinyagents-session`; runtime
  lifecycle and generic tool policy in `tinyagents-runtime`; context
  configuration and compaction strategy in `tinyagents-harness` (the
  `#7390`/`#8088` audit found no upstream equivalent).
* No `tinyagents-harness` or `tinyagents-session` dependency on runtime, no
  runtime re-export of `ToolSnapshot`, and no compatibility shims in the host.
  Runtime may use the already existing harness snapshot type.
* Tenant authority is resolved by the host. A session key, model argument,
  transcript metadata field, or tool argument never selects a tenant or store
  scope.
* Preserve frozen prompt and recorded-tool resume behavior. Persisted tools
  remain available through deferred executors when the host can rebuild them;
  explicit per-turn overrides and host-only belts remain authoritative.
* Preserve two distinct rebind behaviors: generic retained `AgentStores`
  handles keep following a provider rebind; an explicit host lease pins the
  backend and tenant admitted when that lease was opened. New handles opened
  after rebind use the new backend. Rebinding never changes tenant scope.
* Preserve every existing test assertion unless behavior is deliberately
  retired by this issue. Add regression tests that fail against the old
  behavior before implementing each fix. Keep Rust tests in sibling
  `*_tests.rs` files, declared using `#[cfg(test)]` and
  `#[path = "<module>_tests.rs"]`; each test file begins with `use super::*;`.

## Proposed public seams

These signatures are the intended API shape. Error and lifetime details should
follow each crate's existing conventions; avoid adding new dependency edges.

### Transcript identity and repair (`tinyagents-session`)

In `crates/tinyagents-session/src/transcript/session.rs`, add a legacy identity
constructor alongside `SessionRef::{root,scoped,child_of}`:

```rust
impl SessionRef {
    pub fn from_legacy_stem(
        legacy_stem: &str,
        session_key: impl Into<String>,
        agent_id: impl Into<String>,
    ) -> Self;
}
```

The builder must preserve the old root and `parent__child` stem mapping for
the existing legacy host format, while representing the identity as a normal
`SessionRef`. Test the actual old stem-to-reference mapping and verify that
the new `session_stem` resolves to the same persisted transcript. Do not infer
tenant scope from the legacy string. Keep `TranscriptMeta` as the wire record;
add a constructor/builder in `transcript/types.rs` that accepts a
`SessionRef` and copies `session_id` and `parent_session_id` while requiring
callers to provide the existing descriptive metadata fields. Do not change
the serialized field names or defaults.

Add `repair_orphaned_tool_head(&mut Vec<TranscriptMessage>)` in
`transcript/adoption.rs` (or an adjacent `transcript/repair.rs` if that keeps
the existing adoption API clearer), exported from `transcript.rs`. It removes
only the incomplete trailing assistant tool-call row and unmatched trailing
tool-result rows that cannot be replayed without their call; complete earlier
tool rounds remain byte-for-byte/semantically unchanged. It must handle both
typed transcript rows (`tool_calls`, `tool_call_id`) and the legacy envelopes
already normalized by `TranscriptMessage::normalized`. Centralize the rule so
the two OpenHuman replay paths can call the same implementation.

### Host identity resolution and store leases (`tinyagents-session`)

Keep authority resolution at the host boundary and keep generic rebind behavior
intact. Add an explicit lease wrapper in `port/types.rs`:

```rust
#[derive(Clone)]
pub struct SessionStoreLease {
    stores: AgentStores,
}

impl SessionStoreLease {
    pub fn stores(&self) -> &AgentStores;
}
```

Add `SessionStoreProvider::lease_for_agent(&self, agent_id: &str)` with a
default implementation that opens `try_for_agent(agent_id)` once and wraps the
returned handles. `RebindableSessionStores` overrides it by snapshotting the
current `DriverSessionStores` and returning ordinary driver-backed handles,
not the live forwarding wrappers. Keep `for_agent` and existing handles
live-following `rebind` exactly as they are today. Add a provider capability
for resolving the `SessionRef` only after the host has resolved its actor
identity; prefer a host-supplied resolver in runtime (below), and do not put a
tenant type or tenant lookup implementation in TinyAgents.

For backend-specific leased opens, provide a named provider constructor or
method that retains the caller-supplied backend and stable `Scope` for the
lease lifetime. Rebind may update the provider's current backend for future
opens, but must not mutate an already returned lease. `WorkspaceStores::open`
continues to cache its ordinary workspace-bound handles; it must not become a
global mutable routing point.

### Post-commit bridge (`tinyagents-runtime`)

Add `PostCommitSink<C>` next to `SessionHooks` in `runtime/hooks.rs`:

```rust
#[async_trait]
pub trait PostCommitSink<C: Clone + Send + Sync + 'static = ()>: Send + Sync {
    async fn after_commit(&self, receipt: CommitReceipt<C>) -> Result<(), RuntimeError>;
}
```

The runtime invokes installed sinks once, after durable transcript commit, and
does not turn sink errors or cancellation into turn failure. Supply a no-op
default and builder registration that composes multiple sinks in registration
order. Preserve `SessionHooks::after_commit` compatibility internally during
the migration, then remove the duplicate callback only if all in-repo call
sites are migrated in this issue. Add a generic adapter that builds
`tinyagents_harness::host::TurnSummary` and content-free `ToolOutcomeRecord`
from `CommitReceipt`; it may hash serialized arguments but must not retain
arguments or tool output. This adapter reports facts; OpenHuman supplies the
progress fence, memory ingest, and learning sinks.

### Tool surface policy and durable extensions (`tinyagents-runtime`,
`tinyagents-harness`)

In `runtime/tool_registration.rs`, add an explicit policy value:

```rust
#[derive(Clone, Debug, Default)]
pub struct HostOnlyToolPolicy {
    pub names: HashSet<String>,
    pub allow_empty: bool,
}
```

Integrate it with `ToolAdmission` so a host-only turn admits exactly the
host-provided belt, and still invokes the host's own gate for every admitted
tool. Empty belt is deny-all. Do not implement OpenHuman approvals or security
rules here. Extract first-wins visible-spec deduplication as a separate helper
that preserves input order and the original winning `Arc<ToolSpec>` allocation.
Keep the current `ToolSnapshot::new` conflict rejection for callers that
require validation. Before deduplication, drop synthesized declarations whose
names collide with durable host declarations, so durable declarations win.

Extend the existing harness-owned `ToolSnapshot` in
`harness/src/tool/snapshot.rs` with a typed opaque extension map:

```rust
pub struct ToolSnapshot {
    specs: Vec<ToolSpec>,
    exact: bool,
    extensions: BTreeMap<String, serde_json::Value>,
}

impl ToolSnapshot {
    pub fn with_extension(
        self,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> Self;
    pub fn extension(&self, key: &str) -> Option<&serde_json::Value>;
    pub fn extensions(&self) -> &BTreeMap<String, serde_json::Value>;
}
```

Define a versioned snapshot record for transcript persistence. Decode the
existing array-of-specs representation as version 0, and encode the versioned
record only when extensions are present; preserve exactness semantics and
round-trip extension values. Extension keys are namespaced strings, values
are inert JSON, and the harness does not interpret Composio/REPL payloads.
Update `retaining` and `with_retained` to preserve extensions. Explicit
`Turn::tools` overrides remain authoritative and are not augmented from a
recorded snapshot.

### Context configuration (`tinyagents-harness`)

Move the generic `ContextConfig`, `CompactionStrategy`, and
`CompactionSettings` from OpenHuman's `config/schema/context.rs` into a
harness-owned config module. Preserve every existing field, serde alias,
environment/config name mapping, and default, including `task_state`/`typed`/
`s4` and `summary`/`free_form`/`s0`. Keep the generic micro-compaction
keep-recent default in harness rather than reaching into
`crate::agent::context`. Existing TOML remains deserializable with identical
meaning. Product-only settings may remain host-side only when their policy
boundary is documented and the generic harness types do not import OpenHuman.

### Host integration contract (future OpenHuman #212)

The follow-on host adapter will implement `SessionIdentityResolver` in
OpenHuman and will return a `SessionRef` plus host-owned actor metadata after
resolving the authenticated `CoreContext`. The planned contract is:

```rust
pub struct SessionIdentity {
    pub session: SessionRef,
    pub agent_id: String,
}

pub trait SessionIdentityResolver: Send + Sync {
    fn resolve(&self, request: &SessionIdentityRequest)
        -> Result<SessionIdentity, RuntimeError>;
}
```

`SessionIdentityRequest` carries only logical thread/task and parent-session
inputs. It carries no authority-bearing tenant field. The host uses the
resolved actor context to select a `SessionStoreLease` separately. This
contract is intentionally documented now; OpenHuman adoption/deletions are
outside this TinyAgents-only plan.

## TDD work sequence

Each step is independently reviewable. For each behavior, first add the
described failing test, run only that focused test to observe the regression,
then implement the smallest API/code change and rerun it. Keep existing
assertions and run the named crate's full tests after each API family. No host
source edits are part of this plan.

### 1. Repair orphaned tool-call transcript tails

**Files:** `crates/tinyagents-session/src/transcript/adoption.rs`,
`transcript.rs`, and new `transcript/adoption_tests.rs` (or
`transcript/repair_tests.rs` if a new module is chosen).

Add cases for an orphan assistant call followed by no result, a partial set of
results, a complete call/result round, and a legacy-envelope form. Assert the
helper removes only the incomplete head and preserves the complete prefix.
Run `cargo test -p tinyagents-session orphaned_tool_head`, verify red,
implement repair, then run
`cargo test -p tinyagents-session`.

### 2. Build identity and metadata from durable session references

**Files:** `crates/tinyagents-session/src/transcript/session.rs`,
`transcript/types.rs`, their sibling `session_tests.rs` and
`transcript/transcript_tests.rs`.

Pin the legacy root and nested `parent__child` mapping from the host's historic
`{prefix}__{key}` encoding. Assert the result has stable `session_id`, correct
`parent_session_id`, and round-trips the old transcript to the same session
reference without rewriting its legacy file. Add metadata constructor tests
for roots and compacted generations while comparing serialized JSON field
names/default omission to the existing format. Run focused tests red, then
`cargo test -p tinyagents-session transcript`.

### 3. Add explicit pinned store leases without changing live rebind

**Files:** `crates/tinyagents-session/src/port/{mod.rs,types.rs,drivers/rebound.rs,workspace.rs}`
and sibling `rebound_tests.rs`, `workspace_tests.rs`, and
`testkit/conformance.rs` as needed.

First add a regression where (a) a retained generic handle writes before and
after rebind and follows the provider, (b) a lease opened before rebind
continues reading/writing its original backend, and (c) an open after rebind
uses the new backend. Run two tenant scopes against one backend and assert no
agent/key visibility crosses scopes; also assert a registry rebind leaves
unregistered providers unchanged. Then add the lease API and run both
`session_store_isolation_conformance` and the full crate tests. Preserve the
existing tests that assert retained live handles follow rebind and pinned
host-opened handles keep their backend; neither assertion may be silently
weakened.

### 4. Add post-commit sink and generic summary adapter

**Files:** `crates/tinyagents-runtime/src/{hooks.rs,session.rs,types.rs}` and
new `hooks_tests.rs`/existing runtime sibling tests.

Use a recording sink to assert one call after a successful durable append and
after a replacement/generation commit; assert zero calls for pre-commit
failure. Use a failing sink and assert the committed turn remains successful.
Assert summary tool outcomes contain name, digest, success/failure class, and
duration but no argument or result payload. Run focused tests red, implement
sink composition in registration order, then `cargo test -p tinyagents-runtime`.

### 5. Move tool policies and preserve durable tool declarations

**Files:** `crates/tinyagents-runtime/src/{tool_registration.rs,managed_tools.rs}`,
`crates/tinyagents-harness/src/tool/{snapshot.rs,mod.rs}`, plus sibling
`tool_registration_tests.rs`, `managed_tools_tests.rs`, and
`snapshot_tests.rs`.

Test first-wins dedup by pointer identity and order; synthesized collision
with a durable host declaration; host-only empty deny-all; host-only exact
name admission with the host gate still consulted; snapshot extension JSON
round-trip; old array snapshot decode; `retaining`/`with_retained` extension
preservation; and exact override remaining unaugmented on resume. Add the
behavioral failing tests first, keep the existing conflict-rejection tests,
then implement and run `cargo test -p tinyagents-runtime -p tinyagents-harness`.

### 6. Move context configuration into harness

**Files:** `crates/tinyagents-harness/src/config/` plus its sibling tests and
`crates/tinyagents-harness/Cargo.toml` only if existing dependencies require
an already available serde/schema dependency. Update harness compaction
consumers and delete only the generic duplicate implementation from the host
in this upstream repo (there should be no OpenHuman code changes here).

Add TOML deserialization regression fixtures for every existing setting,
aliases, default values, zero trigger normalization, strategy parsing, and
serialization compatibility before moving the type. Verify old TOML fixtures
still deserialize to equal values; verify the harness no longer has an
OpenHuman-style backedge. Run focused tests red, then
`cargo test -p tinyagents-harness config` and
`cargo test -p tinyagents-runtime`.

### 7. Reconcile full crate and workspace integration

Update remaining TinyAgents call sites to the new upstream-owned APIs without
adding a runtime-to-host or harness-to-runtime dependency. Re-run all
previously affected tests from the audit, including tenant routing, recorded
tool resume, host-only tool gating, transcript adoption, workspace isolation,
and post-commit behavior. Do not delete a test merely because an assertion
currently disagrees; map it to the explicit live-handle or lease contract
above.

Run the final gates from the TinyAgents workspace root:

```sh
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -Dwarnings
cargo test --workspace
```

Long commands must use `scripts/ci-cancel-aware.sh`, one job, and debug info
disabled according to the workflow wrapper. Do not set `CARGO_TARGET_DIR`.

## Risks to resolve during implementation

* A legacy stem can be lossy if it is parsed without the original key/agent
  inputs. The builder must accept those inputs and use the old stem only to
  represent parent lineage; it must not pretend arbitrary old strings are
  reversible.
* Lease wrappers must snapshot both backend and tenant scope atomically.
  Capturing a backend while deriving scope later would permit cross-tenant
  reads after actor context changes.
* Snapshot encoding must preserve existing transcript records and exact
  override behavior. Additive extension metadata must not change model-facing
  tool specs or silently revive revoked tools.
* `PostCommitSink` errors occur after the durable boundary and therefore can
  only be reported; they cannot veto or roll back the committed turn.
* `ContextConfig` has product-only fields mixed with generic context knobs.
  Before moving each field, document its owner and preserve existing config
  aliases/defaults. Do not leave a generic implementation or dependency
  backedge in OpenHuman as a compatibility shim.

This document is the plan phase for issue #63. It does not claim the issue is
implemented or complete. Open the upstream PR against `senamakel/tinyagents`
with base `epic/8088-tinyagents-slim` only after implementation, review, and
the required workspace gates pass; do not squash the individual commits.
