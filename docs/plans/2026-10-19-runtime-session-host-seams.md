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

The signatures below are concrete API contracts. Implement them as written,
using the named module's existing error types and without adding dependency
edges beyond the existing crate chain.

### Transcript identity and repair (`tinyagents-session`)

Do not add a raw-stem override or `SessionRef::from_legacy_stem`. The
canonical stem builder in `transcript/session.rs` sanitizes and digests
components, so it cannot reproduce arbitrary historical filenames. Build the
canonical identity from the stable logical thread key and resolved agent id
using the existing `SessionRef::scoped` or `SessionRef::child_of`, then use
`TranscriptLocator::adopt_legacy` to read old timestamped stems and adopt
their rows into that canonical identity. Adoption is one-time and read-only
with respect to the old files. `TranscriptTarget::for_session` and
`SessionBuilder::session` already derive `session_id` and `parent_session_id`;
only add a `TranscriptMeta` constructor if it removes a real duplicate without
changing the wire schema.

Add both replay helpers to the exact existing module
`crates/tinyagents-session/src/transcript/adoption.rs` and export them from
`transcript.rs`. The transcript representation is used by codec replay; the
inference `Message` representation is used by the runtime driver. Both must
remove exactly the same leading tool head after the contiguous system prefix.

```rust
pub fn repair_orphaned_tool_head(
    rows: &[TranscriptMessage],
) -> Cow<'_, [TranscriptMessage]> {
    let prefix = rows.iter().take_while(|row| row.role == "system").count();
    let orphans = rows[prefix..]
        .iter()
        .take_while(|row| row.role == "tool")
        .count();
    if orphans == 0 {
        Cow::Borrowed(rows)
    } else {
        Cow::Owned(rows[..prefix].iter()
            .chain(&rows[prefix + orphans..])
            .cloned()
            .collect())
    }
}

pub fn repair_orphaned_tool_head_messages(
    history: &[tinyinference_llm::message::Message],
) -> Cow<'_, [tinyinference_llm::message::Message]> {
    use tinyinference_llm::message::Message;
    let prefix = history.iter()
        .take_while(|message| matches!(message, Message::System(_)))
        .count();
    let orphans = history[prefix..].iter()
        .take_while(|message| matches!(message, Message::Tool(_)))
        .count();
    if orphans == 0 {
        Cow::Borrowed(history)
    } else {
        Cow::Owned(history[..prefix].iter()
            .chain(&history[prefix + orphans..])
            .cloned()
            .collect())
    }
}
```

Add `use std::borrow::Cow;` in `adoption.rs`. Do not parse tool-call ids or
delete assistant rows: the historical repair is the leading run of orphan
tool-result rows only.

This deliberately removes only leading `tool` rows immediately after the
contiguous system prefix, matching OpenHuman
`driver.rs::repair_orphaned_tool_head` and `codec.rs::replayable_host_rows`.
It does not remove a trailing assistant
tool-call round, does not remove later orphan rows, and does not rewrite the
durable source during resume. Later orphans remain visible to normal strict
transcript validation and fail there. The return value is a replay view; when
there are no leading orphans, the helper returns the original slice through
`Cow::Borrowed` rather than cloning it.

### Host identity resolution and store leases (`tinyagents-session`,
`tinyagents-runtime`)

Keep authority resolution at the host boundary and keep generic rebind behavior
intact. Add an explicit lease wrapper in `port/types.rs`:

```rust
#[derive(Clone)]
pub struct SessionStoreLease {
    stores: AgentStores,
}

impl SessionStoreLease {
    pub fn stores(&self) -> &AgentStores { &self.stores }
}
```

Its constructor remains private to `port`; only
`SessionStoreProvider::lease_for_agent` creates it, preventing a host from
wrapping a live forwarding handle and accidentally calling that a pinned
lease.

Add `SessionStoreProvider::lease_for_agent(&self, agent_id: &str)` with a
default implementation that opens `try_for_agent(agent_id)` once and wraps the
returned handles. `RebindableSessionStores` overrides it by snapshotting the
current `DriverSessionStores` and returning ordinary driver-backed handles,
not the live forwarding wrappers. Keep `for_agent` and existing handles
live-following `rebind` exactly as they are today. The host establishes the
lease by resolving its actor first and supplying the corresponding backend
and `Scope`; TinyAgents stores those supplied values but performs no tenant
lookup.

Rebind may update the provider's current backend for future opens, but must
not mutate an already returned lease. `WorkspaceStores::open`
continues to cache its ordinary workspace-bound handles; it must not become a
global mutable routing point.

### Post-commit bridge (`tinyagents-runtime`)

`CommitReceipt<C>` contains only `SessionTurnOutcome`, transcript options and
the optional transcript commit record. `SessionTurnOutcome` has history,
output, and interrupted status; it has no tool call durations, argument
digests, or failure classifications. Do not derive those values from
transcript prose. The runtime must capture actual tool outcomes at the
`tinyagents_harness::turn_outcome::ToolCallOutcome` boundary and pass a
separate observation alongside the receipt. Add this exact value in
`runtime/types.rs`:

```rust
#[derive(Clone, Debug)]
pub struct PostCommitObservation<C: Clone + Send + Sync + 'static = ()> {
    pub receipt: CommitReceipt<C>,
    pub input: tinyinference_llm::message::Message,
    pub usage: Option<tinyagents_session::transcript::TurnUsage>,
    pub tool_outcomes: Vec<tinyagents_harness::turn_outcome::ToolCallOutcome>,
    pub thread_id: Option<String>,
    pub agent_id: Option<String>,
}
```

Thread
the generic observation sink from `DriverOutcome` through the runtime-owned
finalization task. Add a `tool_outcomes` field to `DriverOutcome`, populated
by the TinyAgents harness adapter by draining its existing
`ToolOutcomeSink`; custom drivers default to an empty vector. Keep the
argument and result text only for the duration of the callback. The summary
adapter hashes serialized arguments, classifies failure using
`classify_tool_failure`, and emits only the digest, name, success, class and
duration in `ToolOutcomeRecord`.

Add `PostCommitSink<C>` next to `SessionHooks` in `runtime/hooks.rs`:

```rust
#[async_trait]
pub trait PostCommitSink<C: Clone + Send + Sync + 'static = ()>: Send + Sync {
    async fn after_commit(&self, observation: &PostCommitObservation<C>)
        -> Result<(), RuntimeError>;
}
```

The finalization owner already spawns a task after durable append and awaits
its `JoinHandle` without observing cancellation (`runtime/session.rs`, commit
path). Invoke sinks inside that same owned task so dropping the caller future
cannot cancel a durable transition's finalization. Do not put additional
awaits before the existing commit/finalization handoff. Sink errors remain
observational. Registration is an explicit ordered `Vec<Arc<dyn
PostCommitSink<C>>>` on `SessionBuilder`; no-op is an empty vector. Preserve
`SessionHooks::after_commit` as the existing lifecycle hook and call it once
from the same task; do not replace it or introduce an independently
cancellable second tail. Add the `TurnSummary` adapter as a helper that
consumes `PostCommitObservation`, while hosts continue to supply product
progress/memory/learning sinks.

### Tool surface policy and durable extensions (`tinyagents-runtime`,
`tinyagents-harness`)

Do not implement host-only behavior as a registration filter alone. The
OpenHuman behavior is a deny-by-default `ToolCallPolicy` that wraps the
host's own gate and is installed in the actual call-policy chain. Add the
generic equivalent in `runtime/tool_registration.rs`:

```rust
pub struct HostOnlyToolPolicy<C> {
    allowed: HashSet<String>,
    host: Option<Arc<dyn tinyagents_harness::middleware::ToolCallPolicy<C>>>,
}

#[async_trait]
impl<C: Send + Sync + 'static> ToolCallPolicy<C> for HostOnlyToolPolicy<C> {
    fn name(&self) -> &str { "host_only" }

    async fn check(
        &self,
        ctx: &RunContext<C>,
        call: &ToolCall,
    ) -> PolicyDecision {
        if !self.allowed.contains(&call.name) {
            return PolicyDecision::deny(format!(
                "tool `{}` is not in this host-only belt", call.name
            ));
        }
        match &self.host {
            Some(host) => host.check(ctx, call).await,
            None => PolicyDecision::Allow,
        }
    }
}
```

Use the harness's actual `ToolCallPolicy<C>` trait and `RunContext<C>` call
signature rather than a new policy protocol. The `check` implementation
denies names outside `allowed` before delegating admitted names to `host`;
empty `allowed` denies all. Runtime admission still controls model-visible
registration, while this policy independently guards actual invocations.
OpenHuman approvals remain the wrapped host policy. Extract first-wins
visible-spec deduplication as a separate helper over `Vec<Arc<ToolSpec>>` that
preserves input order and the winning allocation. Remove synthesized name
collisions before deduplication so the durable declaration wins. Keep
`ToolSnapshot::new` conflict rejection unchanged. Implement the two helpers
with these signatures: `dedup_visible_tool_specs(Vec<Arc<ToolSpec>>) ->
Vec<Arc<ToolSpec>>`, which retains the first name and original allocation in
input order, and `drop_synthesized_name_collisions(&[Arc<ToolSpec>],
Vec<Arc<ToolSpec>>) -> Vec<Arc<ToolSpec>>`, which removes synthesized specs
whose names occur in the durable set before the first-wins pass.

Extend harness `ToolSnapshot` in `harness/src/tool/snapshot.rs` with a
Rust-type keyed extension carrier. TinyAgents defines no Composio/REPL names
or payload schemas; the host owns each payload type and its lifecycle:

```rust
pub trait ToolSnapshotExtension: std::any::Any + std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
}
impl<T: std::any::Any + std::fmt::Debug + Send + Sync> ToolSnapshotExtension for T {
    fn as_any(&self) -> &dyn std::any::Any { self }
}

#[derive(Clone, Default)]
pub struct ToolSnapshotExtensions {
    values: HashMap<TypeId, Arc<dyn ToolSnapshotExtension>>,
}

impl ToolSnapshotExtensions {
    pub fn insert<T: ToolSnapshotExtension>(&mut self, value: T) {
        self.values.insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub fn get<T: ToolSnapshotExtension>(&self) -> Option<&T> {
        self.values.get(&TypeId::of::<T>())?.as_ref().as_any().downcast_ref::<T>()
    }
}

pub struct ToolSnapshot {
    specs: Vec<ToolSpec>,
    exact: bool,
    extensions: ToolSnapshotExtensions,
}

impl ToolSnapshot {
    pub fn with_extension<T: ToolSnapshotExtension>(mut self, extension: T) -> Self {
        self.extensions.insert(extension);
        self
    }
    pub fn extension<T: ToolSnapshotExtension>(&self) -> Option<&T> {
        self.extensions.get::<T>()
    }
}
```

Implement `Debug` for the collection by printing its length, so `ToolSnapshot`
keeps its current `Debug` derive. Replacing an existing value of the same Rust
type replaces that value; different host types coexist. Clones share immutable
`Arc` payloads. `retaining` and `with_retained` copy the extension collection;
`exact` semantics remain unchanged.

Keep `ToolSnapshot::to_json` and `from_json` as the existing array-of-specs
format with no extension serialization. The harness transcript record already
stores arbitrary JSON, but TinyAgents cannot serialize a host-defined Rust
type without a host-owned codec. The host rebuilds typed extension values when
it reconstructs the current snapshot after resume. If a concrete host later
proves it needs durable extension payloads, that host adds a separate codec
and transcript record while the generic runtime carrier remains typed.

### Context configuration (`tinyagents-harness`)

Move `ContextConfig`, `CompactionSettings`, and `CompactionStrategy` from
OpenHuman `crates/openhuman-core/src/config/schema/context.rs` into the exact
TinyAgents files `crates/tinyagents-harness/src/config/context.rs`,
`config/mod.rs`, and `src/lib.rs`. Preserve the complete field set and serde
shape: `enabled`, `microcompact_enabled`, `autocompact_enabled`,
`microcompact_keep_recent`, `tool_result_budget_bytes`,
`summarizer_payload_threshold_tokens` (alias
`summarizer_payload_threshold_bytes`), `summarizer_max_payload_tokens` (alias
`summarizer_max_payload_bytes`), `summarizer_model`,
`prefer_markdown_tool_output`, `compaction_enabled`,
`compaction_trigger_tokens`, and `compaction_strategy`. Keep every default
function's current value, `#[serde(default)]` behavior, strategy spelling,
and `CompactionSettings` normalization of a zero trigger to `None`.
`CompactionStrategy::parse` must continue accepting
`task_state`/`typed`/`s4` and `summary`/`free_form`/`s0` after trimming,
case-folding and converting hyphens to underscores.

The moved type may retain the fields whose comments refer to TinyJuice or
OpenHuman's compaction setting only if TinyAgents already consumes the
matching generic knob. The host retains environment variable loading and
product policy; `ContextConfig` itself owns only serializable context knobs.
The default for `microcompact_keep_recent` becomes a harness constant with the
same value as `DEFAULT_KEEP_RECENT_TOOL_RESULTS`; it must not import OpenHuman.
The existing host `CompactionSettings` call site and full TOML compatibility
are part of this task, even though host deletion/adoption of the rest of
`session_host` is later in #212.

### Session identity resolution (`tinyagents-runtime`)

Issue #63 implements and wires this seam; OpenHuman's concrete resolver and
adapter migration remain in #212. Place the request and result in
`runtime/types.rs`, the trait in `runtime/identity.rs`, register it on
`SessionBuilder`, and invoke it in `Session::turn_inner` after its initial
cancellation check and before `before_resume`, target binding, locking, or
resume:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionIdentityRequest {
    pub thread_id: Option<String>,
    pub session_key: String,
    pub parent: Option<SessionRef>,
    pub child_key: Option<String>,
}

#[derive(Clone)]
pub struct SessionIdentity {
    pub session: SessionRef,
    pub agent_id: String,
    pub stores: tinyagents_session::port::SessionStoreLease,
}

pub trait SessionIdentityResolver<C: Send + Sync + 'static>: Send + Sync {
    fn resolve(
        &self,
        request: &SessionIdentityRequest,
        context: &C,
    ) -> Result<SessionIdentity, RuntimeError>;
}
```

The request has logical naming inputs only; there is no caller-provided
agent/tenant scope field. The resolver uses `context` to authorize the actor,
choose the resolved agent id, and open a tenant-scoped lease. It returns a
root `SessionRef::scoped(request.session_key, resolved_agent_id)` or child
`SessionRef::child_of(parent, child_key)`. Runtime binds the target locator
from `identity.stores.stores().transcripts`, sets target `session`, `stem`,
`meta.agent_id`, `meta.session_id`, `meta.parent_session_id`, then continues
to hooks/resume. Resolver failure returns immediately without touching the
current or fallback locator. A session-bound target may keep its own fixed
locator only when no resolver is installed.

Put the resolved identity in `SessionStateView` as
`Option<&SessionIdentity>` so `before_resume` can observe it without
re-resolving. Store it on `Session<C>` for the entire turn, clear it before
each resolution attempt, and reject a later identity whose `first_generation`
or admitted lease destination differs from the established identity. This
prevents one live `Session` from crossing tenant boundaries. The resolver is
called on every turn before resume so a newly constructed runtime/process
repeats host identity admission; host adapters do not cache authority in the
serialized transcript. Compare each resolved `SessionRef::first_generation()`,
agent id, and locator `destination_key()` with the established binding. Reject
a change with `RuntimeError::InvalidSessionState` before opening a history
handle. When a locator has no destination key, require `Arc::ptr_eq`; a host
resolver may cache its lease for repeated calls in one actor context.

## TDD work sequence

Each step is independently reviewable. For each behavior, first add the
described failing test, run only that focused test to observe the regression,
then implement the smallest API/code change and rerun it. Keep existing
assertions and run the named crate's full tests after each API family. No host
source edits are part of this plan.

### 1. Repair only the historical leading orphan-tool head

**Files:** `crates/tinyagents-session/src/transcript/adoption.rs`,
`transcript.rs`, and new `transcript/adoption_tests.rs` declared from
`transcript.rs` as `#[cfg(test)] #[path = "transcript/adoption_tests.rs"] mod adoption_tests;`.

First add this failing runtime behavior test in
`crates/tinyagents-runtime/src/session_tests.rs`: build a session with the
existing in-memory transcript locator, append rows `[system, tool orphan,
user next]`, resume it, and assert the driver's resumed history is
`[system, user next]` while `read_session_transcript` still returns all three
durable rows. This exercises a currently callable public path and fails on the
wrong history contents; it is not a test that merely names a missing helper.
Then add direct parity tests in the new session `adoption_tests.rs` for both
`TranscriptMessage` and inference `Message` representations. The direct rows
fixture is:

```rust
#[test]
fn repair_removes_only_tool_rows_after_the_leading_system_prefix() {
    let rows = vec![
        TranscriptMessage::system("frozen"),
        TranscriptMessage::tool("legacy orphan").with_id("orphan"),
        TranscriptMessage::user("next turn"),
        TranscriptMessage::tool("later invalid").with_id("later"),
    ];
    let repaired = repair_orphaned_tool_head(&rows);
    assert_eq!(repaired.len(), 3);
    assert_eq!(repaired[0], rows[0]);
    assert_eq!(repaired[1], rows[2]);
    assert_eq!(repaired[2], rows[3]);
    assert_eq!(rows[1].id.as_deref(), Some("orphan")); // source untouched
}
```

Add the corresponding inference-view test in the same test file:

```rust
#[test]
fn message_repair_removes_the_same_leading_tool_head() {
    use tinyinference_llm::message::Message;
    let history = vec![
        Message::system("frozen"),
        Message::tool("orphan", "old result"),
        Message::user("next"),
        Message::tool("later", "invalid"),
    ];
    let repaired = repair_orphaned_tool_head_messages(&history);
    assert_eq!(repaired.len(), 3);
    assert!(matches!(repaired[0], Message::System(_)));
    assert!(matches!(repaired[1], Message::User(_)));
    assert!(matches!(repaired[2], Message::Tool(_)));
    assert_eq!(history.len(), 4);
}
```

`Message::tool` takes the call id and result content in that order.

After the runtime red test fails, add the helper implementation to `adoption.rs`, call it on raw rows before codec decoding and on decoded `Message` history before dispatch, and keep `persisted` raw rows unchanged. Add a second test with a later orphan and assert it remains in the returned replay view, plus a no-orphan test asserting `matches!(..., Cow::Borrowed(_))`. Run `cargo test -p tinyagents-runtime orphaned_tool_head` for the red/green behavioral regression, then `cargo test -p tinyagents-session adoption_tests`, and `cargo test -p tinyagents-runtime`.

### 2. Adopt legacy transcripts into canonical identity without changing old files

**Files:** `crates/tinyagents-session/src/transcript/session_tests.rs` and
`crates/tinyagents-session/src/transcript/transcript_tests.rs`.

Do not parse a legacy filename into a new canonical stem. Add a regression to
`transcript/session_tests.rs` using `SessionRef::scoped("thread-legacy",
"agent-legacy")`; assert its `session_stem` is deterministic across two
constructions and does not equal a timestamped legacy stem. In
`transcript/transcript_tests.rs`, add an adoption test that creates the old
stem fixture via the existing in-memory transcript locator, calls
`TranscriptLocator::adopt_legacy(&canonical_session, "thread-legacy", &meta)`,
reads the head with `head_generation`, and asserts the imported messages and
canonical metadata match while the legacy fixture remains readable and
unchanged. This is a compatibility characterization for adoption; the
behavioral red test for the missing host identity binding is in step 3. Run
`cargo test -p tinyagents-session legacy_session_adoption`; then ensure the
runtime calls adoption only through the already existing `ResumeMode::Session`
path. Do not add a constructor that claims to map arbitrary old raw filenames
to `SessionRef`.

### 3. Resolve identity before resume and bind the admitted lease

**Files:** new `crates/tinyagents-runtime/src/identity.rs`,
`runtime/types.rs`, `runtime/builder.rs`, `runtime/session.rs`, `runtime/lib.rs`,
and new `runtime/identity_tests.rs` declared from `lib.rs`.

First add a recording resolver and locator test. The resolver reads a marker
from `RunContext::data` and returns a scoped identity plus a lease whose
transcript locator records its methods. Assert `Session::turn` resumes through
that locator and its `TranscriptTarget` metadata names the resolved agent.
Add a second resolver that returns `Err` and assert the builder's configured
fallback locator receives no `head_generation`, `read_session_transcript`, or
write call. Both tests fail because the resolver API is absent. Implement the
interface above; store the resolver on `SessionBuilder` and `Session`; invoke
it in `turn_inner` after cancellation and before `before_resume`; build the
target from the lease locator, canonical `SessionRef`, and existing metadata
seed; expose the identity through `SessionStateView`. Require the builder
metadata seed when a resolver is installed and reject missing seed at build.
Check identity, agent, and locator destination against the existing binding
before every handle open. Do not fall back after resolver errors. Run
`cargo test -p tinyagents-runtime identity_resolver` and then
`cargo test -p tinyagents-runtime`.

### 4. Add explicit pinned store leases without changing live rebind

**Files:** `crates/tinyagents-session/src/port/{mod.rs,types.rs,drivers/rebound.rs}`,
`port/drivers/rebound_tests.rs`, `port/workspace_tests.rs`, and
`testkit/conformance.rs`.

In `rebound_tests.rs`, add `a_lease_keeps_its_backend_while_new_opens_follow_rebind`: create provider over `MemoryStorage`, open `lease_for_agent`, write key `("prefs", "key")`, rebind to a fresh `MemoryStorage`, assert the lease still reads/writes the old backend, and assert `lease_for_agent` after rebind reads/writes the new backend. Run `cargo test -p tinyagents-session a_lease_keeps_its_backend_while_new_opens_follow_rebind`; it must fail because `lease_for_agent` is missing. Implement `SessionStoreLease` as an owning `AgentStores` wrapper; add the default provider method wrapping `try_for_agent`; override it in `RebindableSessionStores` by calling `self.current().try_for_agent(agent_id)` and wrapping the returned non-live stores. Add tenant test with two `Scope`s and one backend proving each lease sees only its own agent records. Retain and run the existing `retained_and_cloned_handles_follow_rebind_without_crossing_agent_scopes` test unchanged. Then run `cargo test -p tinyagents-session port::drivers::rebound` and `cargo test -p tinyagents-session`.

### 5. Capture tool observations and route post-commit sinks in the owned finalizer

**Files:** `crates/tinyagents-runtime/src/{driver.rs,hooks.rs,session.rs,types.rs}`,
`turn_driver/driver.rs`, and their sibling `driver_tests.rs`,
`hooks_tests.rs`, `session_tests.rs`, `turn_driver/driver_tests.rs`.

First add a recording `SessionDriver` returning one `ToolCallOutcome`; pin
that `DriverOutcome` carries the observation after execution. Then add a
recording sink test that verifies it runs after the transcript append by
reading the appended row inside the sink, and a cancellation test that drops
the caller future after durable commit but still observes sink completion.
Add a failure test proving pre-commit failure never invokes the sink and a
sink-error test proving it cannot change the successful result. Each test
must fail before the corresponding API/code exists. Extend `DriverOutcome`
with `Vec<ToolCallOutcome>` defaulted empty; have `TurnDriver` build a shared
`ToolOutcomeSink`, install `ToolOutcomeCaptureMiddleware` into its per-turn
middleware chain, drain it into the outcome, and keep custom drivers' empty
default. Add the sink list to `SessionBuilder`, pass it to `Session`, and
invoke each sink in registration order from the existing owned
`terminal_guard.finalize_commit(receipt)` task. Keep `SessionHooks::after_commit`
in that task and in its current position. The `TurnSummary` adapter consumes
outcomes and must not persist the raw argument/result. Verify with
`cargo test -p tinyagents-runtime post_commit` and then
`cargo test -p tinyagents-runtime`.

### 6. Move tool policy and declaration preservation

**Files:** `crates/tinyagents-runtime/src/{tool_registration.rs,managed_tools.rs}`,
`crates/tinyagents-harness/src/tool/{snapshot.rs,mod.rs}`, plus sibling
`tool_registration_tests.rs`, `managed_tools_tests.rs`, and
`snapshot_tests.rs`.

In `tool_registration_tests.rs`, add tests that run the wrapper's real
`ToolCallPolicy::check`: unknown name denies without calling the host gate;
allowed name delegates exactly once; empty set denies all. Implement
`HostOnlyToolPolicy<C>` as `ToolCallPolicy<C>` with
`RunContext<C>`, `ToolCall`, and `PolicyDecision`, then expose a helper to
compose the wrapper around an optional host policy. Add first-wins spec test
that checks names/order and `Arc::ptr_eq` with the winning allocation; add
durable-wins-synthesized collision test. In `snapshot_tests.rs`, assert old
array decode and byte-compatible `to_json` without extensions, typed
retrieval and replacement using two host-defined fixture types, clone
preservation, extension preservation through `retaining` and `with_retained`,
and exact snapshots never retaining recorded tools. Do not add object-envelope
decoding or extension serialization. Run each new filter and verify red before
code, then run
`cargo test -p tinyagents-runtime -p tinyagents-harness`.

### 7. Move context configuration into harness

**Files:** `crates/tinyagents-harness/src/config/context.rs`,
`config/mod.rs`, `src/lib.rs`, and new `config/context_tests.rs` declared
through `#[cfg(test)] #[path = "context_tests.rs"] mod tests;`. Update
TinyAgents compaction consumers and config exports; do not change OpenHuman
sources in this TinyAgents plan.

Before moving the type, add exact tests: deserialize `{}` and compare its
serialized value with serialized `ContextConfig::default()` (the existing type
does not need a new `PartialEq` derive); a TOML fixture with all 12 canonical
fields and a second fixture exercising both legacy aliases; strategy parse
table for all six accepted spellings plus rejected unknown value; zero trigger yields
`CompactionSettings { trigger_tokens: None, strategy: configured_strategy }`;
nonzero trigger survives; and serializing/deserializing the full default
preserves equality. The tests fail because harness has no `ContextConfig`.
Move the fields/default helpers exactly, replacing only the OpenHuman
microcompact default reference with a harness constant equal to its current
value. Then run `cargo test -p tinyagents-harness config::context` and
`cargo test -p tinyagents-runtime`.

### 8. Reconcile full crate and workspace integration

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
disabled. Run exactly:

```sh
CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 scripts/ci-cancel-aware.sh cargo check --workspace --all-targets
CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 scripts/ci-cancel-aware.sh cargo clippy --workspace --all-targets -- -Dwarnings
CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 scripts/ci-cancel-aware.sh cargo test --workspace
```

Do not set `CARGO_TARGET_DIR`.

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
