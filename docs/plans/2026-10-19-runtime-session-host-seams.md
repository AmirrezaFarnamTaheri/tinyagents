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
do not add a raw legacy-stem identity constructor or a duplicate identity
builder.

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
Its constructor remains private; only a provider implementation that can
attest to pinned lifetime may create it.

Add this error and lease contract in `port/types.rs`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum SessionStoreLeaseError {
    #[error("provider cannot guarantee pinned session stores")]
    Unsupported,
    #[error("opening pinned session stores failed: {0}")]
    Open(#[source] tinystoragedrivers_core::StorageError),
}

#[derive(Clone)]
pub struct SessionStoreLease {
    stores: AgentStores,
}

impl SessionStoreLease {
    pub fn stores(&self) -> &AgentStores { &self.stores }
    pub(crate) fn pinned(stores: AgentStores) -> Self {
        Self { stores }
    }
}
```

Add `SessionStoreProvider::lease_for_agent(&self, agent_id: &str)` returning
`Result<SessionStoreLease, SessionStoreLeaseError>`. Its default returns
`Err(SessionStoreLeaseError::Unsupported)`; it must never wrap
`try_for_agent`, because that may return forwarding handles and does not prove
pinned lifetime. Add the method body to the trait:

```rust
fn lease_for_agent(
    &self,
    _agent_id: &str,
) -> Result<SessionStoreLease, SessionStoreLeaseError> {
    Err(SessionStoreLeaseError::Unsupported)
}
```

`DriverSessionStores` and `RebindableSessionStores` support leases. The former
is immutable after construction, including actor providers returned by
`bind_actor`, so it may open direct stores and return them in the lease. The
latter takes one `current()` snapshot and delegates lease opening to that
immutable provider. In both cases, the lease owns the returned non-forwarding
`AgentStores`; each direct store adapter owns its scoped backend handle. If
agent open fails, return `SessionStoreLeaseError::Open(error)` without
creating a lease. Keep `for_agent`, `try_for_agent`, and existing retained handles
live-following `rebind` exactly as they are today. `bind_actor` remains the
host-admission seam: the host resolves the actor and supplies the already
admitted backend; TinyAgents neither resolves tenant scope nor infers it from
agent/session keys. A provider that cannot attest that its handles pin their
backend returns `Unsupported`.

The immutable driver implementation is:

```rust
fn lease_for_agent(
    &self,
    agent_id: &str,
) -> Result<SessionStoreLease, SessionStoreLeaseError> {
    let stores = DriverSessionStores::try_for_agent(self, agent_id)
        .map_err(SessionStoreLeaseError::Open)?;
    Ok(SessionStoreLease::pinned(stores))
}
```

The rebindable implementation uses one immutable snapshot:

```rust
fn lease_for_agent(
    &self,
    agent_id: &str,
) -> Result<SessionStoreLease, SessionStoreLeaseError> {
    self.current().lease_for_agent(agent_id)
}
```

`current()` takes and releases the `RwLock` read guard while cloning the
current provider `Arc`; `lease_for_agent` then runs on that immutable
snapshot. `DriverSessionStores` carries the tenant `Scope`, so no second scope
lookup occurs after opening. Because `bind_actor` in `drivers/mod.rs` returns
`Arc<DriverSessionStores>` as `Arc<dyn SessionStoreProvider>`, its result
supports `lease_for_agent` directly; do not rely on the trait's unsupported
default for actor-bound storage.

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

Add imports for `PostCommitObservation` and the trait-object `Arc`. Store
`Vec<Arc<dyn PostCommitSink<C>>>` on `SessionBuilder` and `Session`, default
empty, and add `.post_commit_sink(Arc<dyn PostCommitSink<C>>) -> Self` that
appends in registration order. The finalization owner already spawns a task after durable append and awaits
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

The existing `TerminalGuard::finalize_commit` owns terminal delivery. Change
its signature to
`finalize_commit(receipt, observation, sinks) -> JoinHandle<()>`, where
`observation: PostCommitObservation<C>` and `sinks` is the already-cloned
ordered sink vector. Inside the spawned task, preserve this order exactly:
`hooks.after_commit(receipt.clone())`, each `sink.after_commit(&observation)`
in vector order, `hooks.on_terminal_outcome(outcome)`, then
`hooks.on_terminal(terminal)`. Ignore/log each postcommit error without
short-circuiting. The existing call site after durable append constructs the
observation and immediately stores/awaits the returned join handle; add no
await between the successful append and spawning the owner task.

Construct summary metadata using only captured facts. Convert input/output to
their message text; hash `serde_json::to_vec(&call.arguments)` with SHA-256;
classify failure text only when `success == false`; copy measured duration.
Never invent a duration, success flag, thread id, agent id, or usage value.
Because `TurnSummary` requires a thread, its adapter returns `None` when
`thread_id` is absent or empty. For usage, map captured input/output/cache
token counts and context window; leave charge absent unless an explicit
provider charge was captured.

Implement that adapter as this pure function in `runtime/summary.rs`:

```rust
pub fn turn_summary<C: Clone + Send + Sync + 'static>(
    observation: &PostCommitObservation<C>,
) -> Option<tinyagents_harness::host::TurnSummary> {
    use sha2::{Digest, Sha256};
    let thread = observation.thread_id.as_deref()?.trim();
    if thread.is_empty() { return None; }
    let usage = observation.usage.as_ref().map_or_else(Default::default, |usage| {
        tinyinference_llm::usage::Usage {
            input_tokens: usage.usage.input,
            output_tokens: usage.usage.output,
            cache_read_tokens: usage.usage.cached_input,
            context_window_tokens: (usage.usage.context_window > 0)
                .then_some(usage.usage.context_window),
            ..Default::default()
        }
    });
    let mut summary = tinyagents_harness::host::TurnSummary::new(
        tinyagents_harness::ids::ThreadId::from(thread),
        observation.agent_id.as_deref()?,
    ).with_text(
        observation.input.text(),
        observation.receipt.outcome.output.clone().unwrap_or_default(),
    ).with_usage(usage);
    for call in &observation.tool_outcomes {
        let bytes = serde_json::to_vec(&call.arguments).ok()?;
        let digest = Sha256::digest(bytes);
        let args_digest = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        summary.record_tool_outcome(tinyagents_harness::host::ToolOutcomeRecord {
            name: call.name.clone(),
            args_digest,
            success: call.success,
            failure_class: (!call.success).then(|| {
                tinyagents_harness::host::classify_tool_failure(&call.content).to_owned()
            }),
            duration_ms: call.duration_ms,
        });
    }
    Some(summary)
}
```

Add `sha2` as a normal runtime dependency only if it is not already present in
the runtime manifest. Test missing/blank identity returns `None`, a failed
tool gets a digest and failure class without storing arguments/content, a
successful tool has no failure class, and the captured usage fields map
exactly.

### Tool surface policy and durable extensions (`tinyagents-runtime`,
`tinyagents-harness`)

Do not implement host-only behavior as a registration filter alone. The
OpenHuman behavior is a deny-by-default `ToolCallPolicy` that wraps the
host's own gate and is installed in the actual call-policy chain. Add the
generic equivalent in `runtime/tool_registration.rs`:

```rust
pub struct HostOnlyToolPolicy<C: Send + Sync + 'static> {
    allowed: HashSet<String>,
    host: Option<Arc<dyn tinyagents_harness::middleware::ToolCallPolicy<C>>>,
}

impl<C: Send + Sync + 'static> HostOnlyToolPolicy<C> {
    pub fn new(
        allowed: impl IntoIterator<Item = String>,
        host: Option<Arc<dyn tinyagents_harness::middleware::ToolCallPolicy<C>>>,
    ) -> Self {
        Self { allowed: allowed.into_iter().collect(), host }
    }
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

The generic declaration and impl both carry `C: Send + Sync + 'static` (the
same bound as `ToolCallPolicy<C>`); put the bound on the struct, impl, and
constructor so the public type is well-formed. Install it on the actual
`TurnPlan::tool_policy`, which `TurnDriver::prepare` wraps with
`ToolPolicyGateMiddleware`; registration filtering is only the model-visible
half of this policy.

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

Implement those helpers with this code (imports are `HashSet`, `Arc`, and
`tinytools::ToolSpec`):

```rust
pub fn dedup_visible_tool_specs(specs: Vec<Arc<ToolSpec>>) -> Vec<Arc<ToolSpec>> {
    let mut names = HashSet::new();
    specs.into_iter()
        .filter(|spec| names.insert(spec.name.clone()))
        .collect()
}

pub fn drop_synthesized_name_collisions(
    durable: &[Arc<ToolSpec>],
    synthesized: Vec<Arc<ToolSpec>>,
) -> Vec<Arc<ToolSpec>> {
    let durable_names: HashSet<&str> = durable.iter()
        .map(|spec| spec.name.as_str())
        .collect();
    synthesized.into_iter()
        .filter(|spec| !durable_names.contains(spec.name.as_str()))
        .collect()
}
```

Test order and allocation directly with two `Arc<ToolSpec>` values named
`"read"`: the returned first element is `Arc::ptr_eq` to the first allocation,
and the second distinct name remains second. For durable collision, pass a
durable `"search"` and synthesized `["search", "render"]`; assert only
`"render"` survives before deduplication.

The host-policy test uses a counter-backed host policy and calls
`ToolCallPolicy::check` directly with a real `RunContext::new(RunConfig::new("test"), ())`:
assert unknown name yields `PolicyDecision::Deny` with counter `0`, admitted
name forwards the same call and counter becomes `1`, and empty allowlist
denies. Also run it through `TurnPlan::tool_policy` and assert the underlying
tool executor is never invoked for an unknown call.

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

Implement `Debug` for the collection as:

```rust
impl std::fmt::Debug for ToolSnapshotExtensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolSnapshotExtensions")
            .field("len", &self.values.len())
            .finish()
    }
}
```

This lets `ToolSnapshot` keep its current `Debug` derive. Replacing an existing value of the same Rust
type replaces that value; different host types coexist. Clones share immutable
`Arc` payloads. `retaining` and `with_retained` copy the extension collection;
`exact` semantics remain unchanged.

In the existing `ToolSnapshot::new`, initialize `extensions` to
`ToolSnapshotExtensions::default()`. Replace `retaining` with this complete
body:

```rust
pub fn retaining(&self, keep: impl Fn(&ToolSpec) -> bool) -> Self {
    Self {
        specs: self.specs.iter().filter(|spec| keep(spec)).cloned().collect(),
        exact: self.exact,
        extensions: self.extensions.clone(),
    }
}
```

Keep the existing early return in `with_retained`, and replace its success
tail with this exact code. The `Self::new` call continues to validate name
collisions:

```rust
let count = retained.len();
let mut merged = Self::new(self.specs.iter().cloned().chain(retained).collect())?;
merged.extensions = self.extensions.clone();
Ok((merged, count))
```

Keep `from_json` going through `Self::new`; decoded snapshots therefore have
an empty extension collection, and the existing array wire format stays
unchanged.

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

In `config/context_tests.rs`, use a TOML string with exactly these keys and
values: `enabled=false`, `microcompact_enabled=false`,
`autocompact_enabled=true`, `microcompact_keep_recent=9`,
`tool_result_budget_bytes=1234`,
`summarizer_payload_threshold_tokens=4000`,
`summarizer_max_payload_tokens=64000`, `summarizer_model="test-model"`,
`prefer_markdown_tool_output=false`, `compaction_enabled=true`,
`compaction_trigger_tokens=123`, `compaction_strategy="summary"`; assert
every field. A second fixture substitutes both aliases for their canonical
threshold keys and asserts the same parsed values. Check default round-trip by
serializing both values to `serde_json::Value` and comparing; do not add
`PartialEq` solely for this test.

### Session identity resolution (`tinyagents-runtime`)

Issue #63 implements and wires this seam; OpenHuman's concrete resolver and
adapter migration remain in #212. Put request/result in `runtime/types.rs`,
the resolver trait in `runtime/identity.rs`, and the resolver on
`SessionBuilder`/`Session`. One authority operation, `resolve_and_bind`, is
used by every transcript-reading path. `Session::turn` calls it after
`before_resume` and `apply_resume_preparation`, so hook-provided target
replacements are visible, but before locks or locator methods. Public
`Session::resume` has no hook in its existing contract; it calls the same
helper directly before any locator method and never reuses a prior operation's
identity. Both then call private `resume_admitted`. A
resolver-enabled session rejects absent identity, conflicting
`TurnOptions::session`, or changed hook-selected target before
`head_generation`, `read_session_transcript`, legacy adoption, and lock lookup.

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionIdentityRequest {
    pub thread_id: Option<String>,
    pub session_key: String,
}

#[derive(Clone)]
pub struct ResolvedSessionStores {
    pub agent_id: String,
    pub stores: tinyagents_session::port::SessionStoreLease,
}

#[derive(Clone)]
pub struct SessionIdentity {
    pub(crate) session: SessionRef,
    pub(crate) agent_id: String,
    pub(crate) stores: tinyagents_session::port::SessionStoreLease,
}

impl SessionIdentity {
    pub fn session(&self) -> &SessionRef { &self.session }
    pub fn agent_id(&self) -> &str { &self.agent_id }
}

pub trait SessionIdentityResolver<C: Send + Sync + 'static>: Send + Sync {
    fn resolve(
        &self,
        request: &SessionIdentityRequest,
        context: &C,
    ) -> Result<ResolvedSessionStores, RuntimeError>;
}
```

Add the builder/storage wiring with these exact ownership fields. Put the
resolver and identity seed in a private builder config so existing
`.transcript(...)` and `.session(...)` remain source-compatible:

```rust
struct IdentityConfig<C: Send + Sync + 'static> {
    resolver: Arc<dyn SessionIdentityResolver<C>>,
    session_key: String,
    meta: TranscriptMeta,
}
pub fn identity_resolver(
    mut self,
    session_key: impl Into<String>,
    meta: TranscriptMeta,
    resolver: Arc<dyn SessionIdentityResolver<C>>,
) -> Self {
    self.identity = Some(IdentityConfig {
        resolver,
        session_key: session_key.into(),
        meta,
    });
    self
}
```

In `builder.rs`, add `identity: Option<IdentityConfig<C>>` to
`SessionBuilder<C>`, initialize it to `None` in `new`, and add the method
implementation below. Keep `IdentityConfig<C>` in a private field so the
existing `.transcript(...)` and `.session(...)` methods remain source
compatible:

```rust
pub fn identity_resolver(
    mut self,
    session_key: impl Into<String>,
    meta: TranscriptMeta,
    resolver: Arc<dyn SessionIdentityResolver<C>>,
) -> Self {
    self.identity = Some(IdentityConfig {
        resolver,
        session_key: session_key.into(),
        meta,
    });
    self
}
```

In `build`, destructure the identity configuration before moving other
builder fields. Require `TranscriptCodec` if either transcript or identity
configuration exists, preserve the existing target construction, and pass
`identity` as the final constructor argument:

```rust
let identity = self.identity;
let target = self.transcript.map(|config| crate::TranscriptTarget {
    locator: config.locator,
    stem: config.stem,
    resume_agent: config.resume_agent,
    session: config.session,
    meta: config.meta,
});
if (target.is_some() || identity.is_some()) && self.codec.is_none() {
    return Err(RuntimeError::MissingDependency("TranscriptCodec"));
}
let mut session = Session::<C>::new(
    self.driver, self.codec, self.hooks, self.prefix, self.tools, target, identity,
);
session.set_retain_recorded_tools(self.retain_recorded_tools);
Ok(session)
```

Change `Session::new` to accept `identity_config: Option<IdentityConfig<C>>`
after `target` and initialize `identity_config`,
`established_identity: None`, and `identity: None`. Add
`identity: Option<&'a SessionIdentity>` to `SessionStateView<'a>` and populate
it from `established_identity` (never the in-flight result). Export
`SessionIdentity` and `SessionIdentityRequest` from `types` and
`ResolvedSessionStores`/`SessionIdentityResolver` from `identity` in `lib.rs`.

In `SessionBuilder::build`, require a codec when either transcript or
identity config is present, then pass the identity config into `Session::new`.
Do not require a fallback `TranscriptTarget`; the resolver's lease is the
target. Retain an optional legacy transcript target only as an unselected
fallback: resolver failure must not access it. Add this exact helper body to
`Session<C>`; pass the hook-selected target separately so the builder's
fallback is never mistaken for hook authority:

```rust
fn resolve_and_bind(
    &mut self,
    options: &TurnOptions<C>,
    hook_target: Option<&TranscriptTarget>,
) -> Result<(), RuntimeError> {
    let Some(config) = self.identity_config.as_ref() else { return Ok(()); };
    self.identity = None;
    let request = SessionIdentityRequest {
        thread_id: options.thread_id.clone(),
        session_key: config.session_key.clone(),
    };
    let resolved = config.resolver.resolve(&request, &options.run_context.data)?;
    if resolved.agent_id.is_empty() {
        return Err(RuntimeError::InvalidSessionState("resolver returned empty agent id".into()));
    }
    let session = SessionRef::scoped(&request.session_key, &resolved.agent_id);
    let identity = SessionIdentity {
        session,
        agent_id: resolved.agent_id,
        stores: resolved.stores,
    };
    if options.session.as_ref().is_some_and(|selected|
        selected.first_generation() != identity.session.first_generation())
    {
        return Err(RuntimeError::InvalidSessionState("TurnOptions.session conflicts with resolved identity".into()));
    }
    let mut meta = config.meta.clone();
    meta.agent_id = Some(identity.agent_id.clone());
    let target = TranscriptTarget::for_session(
        identity.stores.stores().transcripts.clone(),
        identity.session.clone(),
        meta,
    );
    if hook_target.is_some_and(|selected| !selected.same_binding(&target)) {
        return Err(RuntimeError::InvalidSessionState("before_resume target conflicts with resolved identity".into()));
    }
    if let Some(previous) = self.established_identity.as_ref()
        && !same_admission(previous, &identity)
    {
        return Err(RuntimeError::InvalidSessionState("session identity or pinned store changed".into()));
    }
    self.target = Some(target);
    self.established_identity = Some(identity.clone());
    self.identity = Some(identity);
    Ok(())
}
```

`SessionIdentity` is a runtime-created private binding; the host result is
only `ResolvedSessionStores { agent_id, stores }`. Runtime constructs the
canonical root `SessionRef` from the configured logical key and resolved agent
id; it accepts no host-provided stem. `same_admission` compares the identity's first generation, agent id, and
locator key/pointer as specified above.

Implement the binding predicate in `runtime/identity.rs` with the following
comparison semantics (the current target locator is read from the target that
will actually be resumed, after hook preparation):

```rust
fn same_admission(a: &SessionIdentity, b: &SessionIdentity) -> bool {
    if a.session.first_generation() != b.session.first_generation()
        || a.agent_id != b.agent_id
    {
        return false;
    }
    let left = a.stores.stores().transcripts.destination_key();
    let right = b.stores.stores().transcripts.destination_key();
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        (None, None) => Arc::ptr_eq(
            &a.stores.stores().transcripts,
            &b.stores.stores().transcripts,
        ),
        _ => false,
    }
}
```

`resolve_and_bind` returns `InvalidSessionState` if this predicate fails for
the established identity, if `options.session` has another
`first_generation()`, or if the hook-selected target locator/session does not
match the newly resolved admission. It updates the target only after all
checks pass. `Session::turn` must call it after `apply_resume_preparation`;
public `Session::resume` calls it before the first locator access. This order
is intentional: hooks may prepare a lazy target but cannot grant authority to
change its destination.

Extract the storage-reading body of `Session::resume` into private
`resume_admitted`. Public `resume` invokes `resolve_and_bind(options, None)`
and then `resume_admitted`; it must not add hooks or a synthetic request.
`turn_inner` retains its existing `before_resume` and preparation calls, then
calls `resolve_and_bind(options, self.target.as_ref())`, acquires
`lock_turn(options)`, and calls `resume_admitted` directly. Both paths share
one authority predicate without changing public resume hook semantics. The
private reader performs no hook, resolver, lock, or target-selection work.
Validate `TurnOptions::session` and any hook-selected target before the lock
or any locator operation.

The request has logical naming inputs only; there is no caller-provided
agent/tenant scope field. The resolver uses `options.run_context.data` to
authorize the actor, choose the resolved agent id, and open a tenant-scoped
lease. Map `SessionStoreLeaseError::Unsupported` or `Open(_)` into
`RuntimeError::InvalidSessionState(error.to_string())`; host resolver errors
remain the host's `RuntimeError`. Runtime constructs a root
`SessionRef::scoped(request.session_key, resolved.agent_id)`. Runtime binds
the target locator from `resolved.stores.stores().transcripts`, sets
`meta.agent_id`, `meta.session_id`, `meta.parent_session_id`, and canonical
stem, then continues to resume. Resolver failure returns immediately without
touching the current or fallback locator. A session-bound target may keep its
own fixed locator only when no resolver is installed.

Put the last established binding in `SessionStateView` as
`Option<&SessionIdentity>` for diagnostics and hook decisions; it is not
authority for the current turn. The current resolver result is produced only
after `before_resume` and `apply_resume_preparation`, then checked before any
lock or locator access. Store it on `Session<C>` for the duration of each operation, clear it before
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
and `runtime/lib_tests.rs` (the existing sibling test module).

Add these red tests first: (a) direct `Session::resume` uses the resolved
locator; (b) a `before_resume` hook replacing `ResumePreparation.transcript`
with another destination is rejected before either locator runs; (c)
`TurnOptions::session` naming a different first generation is rejected before
locator access; (d) resolver failure leaves the fallback locator untouched;
(e) changing agent, first generation, or destination on a later turn returns
`InvalidSessionState` before locator access; (f) a new generation with the
same first generation and destination is accepted. Each recording locator
asserts zero calls to `head_generation`, `read_session_transcript`,
`adopt_legacy`, and lock lookup after rejection. Implement
`resolve_and_bind(options)` in `session.rs`; turn calls it after hooks and
public `resume` calls it directly, then both invoke private `resume_bound`.
Bind the target locator from the lease, canonical `SessionRef`, and metadata
seed; expose the identity through `SessionStateView`. Require metadata seed at
builder build time if a resolver is installed. Check any explicit option or
hook-selected target against this identity; they cannot replace authority.
Add these test helpers to the top of `lib_tests.rs` and use the exact direct
resume regression below:

```rust
struct FixedIdentityResolver {
    provider: Arc<tinyagents_session::port::DriverSessionStores>,
    calls: Arc<Mutex<usize>>,
    agent_id: Arc<Mutex<String>>,
}

impl crate::SessionIdentityResolver<()> for FixedIdentityResolver {
    fn resolve(
        &self,
        request: &crate::SessionIdentityRequest,
        _: &(),
    ) -> Result<crate::ResolvedSessionStores, RuntimeError> {
        assert_eq!(request.session_key, "thread-identity");
        *self.calls.lock().unwrap() += 1;
        let agent_id = self.agent_id.lock().unwrap().clone();
        let stores = tinyagents_session::port::SessionStoreProvider::lease_for_agent(
            self.provider.as_ref(), &agent_id,
        ).map_err(|error| RuntimeError::InvalidSessionState(error.to_string()))?;
        Ok(crate::ResolvedSessionStores { agent_id, stores })
    }
}

#[tokio::test]
async fn direct_resume_resolves_and_uses_the_admitted_lease() {
    use tinystoragedrivers_core::{MemoryStorage, Scope};
    let provider = Arc::new(
        tinyagents_session::port::DriverSessionStores::new(
            Arc::new(MemoryStorage::new()), Scope::local(),
        ).unwrap(),
    );
    let calls = Arc::new(Mutex::new(0));
    let resolver = Arc::new(FixedIdentityResolver {
        provider, calls: calls.clone(), agent_id: Arc::new(Mutex::new("agent-resolved".into())),
    });
    let mut session = SessionBuilder::new(Arc::new(Driver::new(vec![])))
        .codec(Arc::new(Codec::default()))
        .identity_resolver("thread-identity", meta(), resolver)
        .build().unwrap();
    let mut options = TurnOptions::default();
    options.resume = ResumeMode::Session;
    assert!(!session.resume(&options).await.unwrap().loaded);
    assert_eq!(*calls.lock().unwrap(), 1);
}
```

Use these exact adversarial test bodies. They reuse `locator(None)`, `meta()`,
and `Driver` from `lib_tests.rs`:

```rust
struct ReplaceTargetHook {
    target: Arc<dyn TranscriptLocator>,
    session_override: Option<SessionRef>,
}

#[async_trait]
impl SessionHooks for ReplaceTargetHook {
    async fn before_resume(
        &self, _: &mut SessionTurnRequest, options: &mut TurnOptions,
        _: SessionStateView<'_>,
    ) -> Result<ResumePreparation, RuntimeError> {
        options.session = self.session_override.clone();
        Ok(ResumePreparation { transcript: Some(TranscriptTarget::for_session(
            self.target.clone(), SessionRef::scoped("thread-identity", "agent-resolved"), meta(),
        )) })
    }
    async fn before_turn(
        &self, _: &mut SessionTurnRequest, _: &mut TurnOptions,
        _: SessionStateView<'_>,
    ) -> Result<TurnPreparation, RuntimeError> { Ok(TurnPreparation::default()) }
    async fn before_commit(
        &self, _: &SessionTurnOutcome, _: &TranscriptTurnOptions,
    ) -> Result<(), RuntimeError> { Ok(()) }
    async fn on_terminal(&self, _: SessionTerminal) -> Result<(), RuntimeError> { Ok(()) }
}

#[tokio::test]
async fn hook_cannot_replace_the_identity_locator() {
    use tinystoragedrivers_core::{MemoryStorage, Scope};
    use tinyagents_session::port::DriverSessionStores;
    let provider = Arc::new(DriverSessionStores::new(Arc::new(MemoryStorage::new()), Scope::local()).unwrap());
    let calls = Arc::new(Mutex::new(0));
    let resolver = Arc::new(FixedIdentityResolver {
        provider, calls, agent_id: Arc::new(Mutex::new("agent-resolved".into())),
    });
    let (wrong, wrong_history) = locator(None);
    let driver = Arc::new(Driver::new(vec![]));
    let mut session = SessionBuilder::new(driver.clone())
        .codec(Arc::new(Codec::default()))
        .hooks(Arc::new(ReplaceTargetHook {
            target: wrong.clone(), session_override: None,
        }))
        .identity_resolver("thread-identity", meta(), resolver)
        .build().unwrap();
    let options = TurnOptions { resume: ResumeMode::Session, ..TurnOptions::default() };
    assert!(matches!(session.turn(Message::user("next"), options).await,
        Err(RuntimeError::InvalidSessionState(_))));
    assert!(driver.requests.lock().unwrap().is_empty());
    assert_eq!(*wrong_history.opens.lock().unwrap(), 0);
    assert!(wrong.latest_agents.lock().unwrap().is_empty());
}

#[tokio::test]
async fn hook_cannot_override_the_resolved_session_option() {
    use tinystoragedrivers_core::{MemoryStorage, Scope};
    use tinyagents_session::port::DriverSessionStores;
    let provider = Arc::new(DriverSessionStores::new(
        Arc::new(MemoryStorage::new()), Scope::local(),
    ).unwrap());
    let resolver = Arc::new(FixedIdentityResolver {
        provider, calls: Arc::new(Mutex::new(0)),
        agent_id: Arc::new(Mutex::new("agent-resolved".into())),
    });
    let (target_locator, target_history) = locator(None);
    let (foreign_locator, foreign_history) = locator(None);
    let driver = Arc::new(Driver::new(vec![]));
    let mut session = SessionBuilder::new(driver.clone())
        .codec(Arc::new(Codec::default()))
        .hooks(Arc::new(ReplaceTargetHook {
            target: target_locator.clone(),
            session_override: Some(SessionRef::scoped("foreign-thread", "agent-resolved")),
        }))
        .transcript(foreign_locator.clone(), "fallback", meta())
        .identity_resolver("thread-identity", meta(), resolver)
        .build().unwrap();
    let options = TurnOptions { resume: ResumeMode::Session, ..TurnOptions::default() };
    assert!(matches!(session.turn(Message::user("next"), options).await,
        Err(RuntimeError::InvalidSessionState(_))));
    assert!(driver.requests.lock().unwrap().is_empty());
    assert!(target_locator.latest_agents.lock().unwrap().is_empty());
    assert_eq!(*target_history.opens.lock().unwrap(), 0);
    assert!(foreign_locator.latest_agents.lock().unwrap().is_empty());
    assert_eq!(*foreign_history.opens.lock().unwrap(), 0);
}

#[tokio::test]
async fn public_resume_rejects_foreign_session_before_locator_access() {
    use tinyagents_session::port::DriverSessionStores;
    use tinystoragedrivers_core::{MemoryStorage, Scope};
    let provider = Arc::new(DriverSessionStores::new(
        Arc::new(MemoryStorage::new()), Scope::local(),
    ).unwrap());
    let resolver = Arc::new(FixedIdentityResolver {
        provider, calls: Arc::new(Mutex::new(0)),
        agent_id: Arc::new(Mutex::new("agent-resolved".into())),
    });
    let (foreign, foreign_history) = locator(None);
    let mut session = SessionBuilder::new(Arc::new(Driver::new(vec![])))
        .codec(Arc::new(Codec::default()))
        .transcript(foreign.clone(), "foreign-fallback", meta())
        .identity_resolver("thread-identity", meta(), resolver)
        .build().unwrap();
    let mut options = TurnOptions { resume: ResumeMode::Session, ..TurnOptions::default() };
    options.session = Some(SessionRef::scoped("foreign-thread", "agent-resolved"));
    assert!(matches!(session.resume(&options).await,
        Err(RuntimeError::InvalidSessionState(_))));
    assert!(foreign.latest_agents.lock().unwrap().is_empty());
    assert_eq!(*foreign_history.opens.lock().unwrap(), 0);
}

#[tokio::test]
async fn explicit_session_override_cannot_change_resolved_identity() {
    use tinyagents_session::port::DriverSessionStores;
    use tinystoragedrivers_core::{MemoryStorage, Scope};
    let provider = Arc::new(DriverSessionStores::new(Arc::new(MemoryStorage::new()), Scope::local()).unwrap());
    let calls = Arc::new(Mutex::new(0));
    let resolver = Arc::new(FixedIdentityResolver {
        provider, calls: calls.clone(), agent_id: Arc::new(Mutex::new("agent-resolved".into())),
    });
    let mut session = SessionBuilder::new(Arc::new(Driver::new(vec![])))
        .codec(Arc::new(Codec::default()))
        .identity_resolver("thread-identity", meta(), resolver)
        .build().unwrap();
    let options = TurnOptions {
        resume: ResumeMode::Session,
        session: Some(SessionRef::scoped("other-thread", "agent-resolved")),
        ..TurnOptions::default()
    };
    assert!(matches!(session.resume(&options).await,
        Err(RuntimeError::InvalidSessionState(_))));
    assert_eq!(*calls.lock().unwrap(), 1);
}

struct FailingIdentityResolver;

impl SessionIdentityResolver<()> for FailingIdentityResolver {
    fn resolve(
        &self,
        _: &SessionIdentityRequest,
        _: &(),
    ) -> Result<ResolvedSessionStores, RuntimeError> {
        Err(RuntimeError::Driver("actor admission denied".into()))
    }
}

#[tokio::test]
async fn resolver_error_does_not_touch_builder_fallback_locator() {
    let (fallback, history) = locator(None);
    let mut session = SessionBuilder::new(Arc::new(Driver::new(vec![])))
        .codec(Arc::new(Codec::default()))
        .transcript(fallback.clone(), "fallback", meta())
        .identity_resolver("thread-identity", meta(), Arc::new(FailingIdentityResolver))
        .build().unwrap();
    let options = TurnOptions {
        resume: ResumeMode::LatestForAgent,
        ..TurnOptions::default()
    };
    assert!(matches!(session.resume(&options).await, Err(RuntimeError::Driver(_))));
    assert!(fallback.latest_agents.lock().unwrap().is_empty());
    assert_eq!(*history.opens.lock().unwrap(), 0);
}

#[tokio::test]
async fn later_agent_change_is_rejected_before_second_driver_call() {
    use tinystoragedrivers_core::{MemoryStorage, Scope};
    use tinyagents_session::port::DriverSessionStores;
    let provider = Arc::new(DriverSessionStores::new(Arc::new(MemoryStorage::new()), Scope::local()).unwrap());
    let calls = Arc::new(Mutex::new(0));
    let agent_id = Arc::new(Mutex::new("agent-one".into()));
    let resolver = Arc::new(FixedIdentityResolver { provider, calls, agent_id: agent_id.clone() });
    let driver = Arc::new(Driver::new(vec![
        Ok(outcome(vec![Message::user("one"), Message::assistant("done")])),
        Ok(outcome(vec![Message::user("two"), Message::assistant("done")])),
    ]));
    let mut session = SessionBuilder::new(driver.clone())
        .codec(Arc::new(Codec::default()))
        .identity_resolver("thread-identity", meta(), resolver)
        .build().unwrap();
    let options = || TurnOptions { resume: ResumeMode::Session, ..TurnOptions::default() };
    session.turn(Message::user("one"), options()).await.unwrap();
    *agent_id.lock().unwrap() = "agent-two".into();
    assert!(matches!(session.turn(Message::user("two"), options()).await,
        Err(RuntimeError::InvalidSessionState(_))));
    assert_eq!(driver.requests.lock().unwrap().len(), 1);
}
```

The option mismatch must precede the `head_generation` call; the hook mismatch
must precede both locators; the later identity mismatch must precede the
second driver invocation. Also add the accepted same-destination/new-generation
case by binding a second generation under the same first generation and
asserting two driver requests. Run the direct test filter and the runtime suite.
Run `cargo test -p tinyagents-runtime direct_resume_resolves_and_uses_the_admitted_lease`
and `cargo test -p tinyagents-runtime identity`, then the runtime crate suite.

### 4. Add explicit pinned store leases without changing live rebind

**Files:** `crates/tinyagents-session/src/port/{mod.rs,types.rs,drivers/rebound.rs}`,
`port/drivers/rebound_tests.rs`, `port/workspace_tests.rs`, and
`testkit/conformance.rs`.

In `rebound_tests.rs`, add `a_lease_keeps_its_backend_while_new_opens_follow_rebind`: open a lease over `MemoryStorage`, write a transcript row, rebind to fresh storage, and assert the lease still uses the old backend while a new ordinary open follows rebind. Add `actor_bound_driver_provider_can_open_a_pinned_lease`: call `bind_actor(admitted_backend)`, then open a lease from the returned provider and assert its tenant-scoped stores access the admitted backend. Add `unsupported_provider_cannot_issue_a_pinned_lease` for a provider implementing only `for_agent`; assert `Unsupported` and no attempted handle open. Add two-tenant isolation coverage with distinct `Scope`s. Implement the preceding `SessionStoreLease { stores }` exactly, with private construction and a read-only `stores()` accessor. The `SessionStoreProvider` default returns `Err(Unsupported)`; `DriverSessionStores::lease_for_agent` opens immutable, direct handles, and `RebindableSessionStores::lease_for_agent` delegates to `self.current()` so a live forwarding handle never escapes. Map open errors to `Open(error)`. Do not change the host-admitted `bind_actor` contract; test unsupported providers return `Ok(None)` and do not silently borrow another actor. Retain the existing live-handle rebind test unchanged. Run focused lease tests, `cargo test -p tinyagents-session port::drivers::rebound`, then the crate suite.

Use the existing `rebound_tests.rs` fixture imports and these exact bodies:

```rust
#[tokio::test]
async fn a_lease_keeps_its_backend_while_new_opens_follow_rebind() {
    let registry = SessionStoreRegistry::default();
    let old_backend = Arc::new(MemoryStorage::new());
    let provider = RebindableSessionStores::new_in(&registry, old_backend.clone(), false).unwrap();
    let lease = provider.lease_for_agent("agent-one").unwrap();
    lease.stores().kv.put("prefs", "key", serde_json::json!("leased"))
        .await.unwrap();
    let new_backend = Arc::new(MemoryStorage::new());
    assert_eq!(registry.rebind(new_backend.clone()).unwrap(), 1);
    assert_eq!(lease.stores().kv.get("prefs", "key").await.unwrap(),
        Some(serde_json::json!("leased")));
    let new_lease = provider.lease_for_agent("agent-one").unwrap();
    assert_eq!(new_lease.stores().kv.get("prefs", "key").await.unwrap(), None);
    new_lease.stores().kv.put("prefs", "key", serde_json::json!("new"))
        .await.unwrap();
    let old = DriverSessionStores::new(old_backend, Scope::local()).unwrap()
        .try_for_agent("agent-one").unwrap();
    assert_eq!(old.kv.get("prefs", "key").await.unwrap(), Some(serde_json::json!("leased")));
}

#[tokio::test]
async fn actor_bound_driver_provider_can_open_a_pinned_lease() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let root = DriverSessionStores::new(Arc::new(MemoryStorage::new()), Scope::tenant("alice").unwrap()).unwrap();
    let actor = root.bind_actor(Arc::clone(&backend)).unwrap().unwrap();
    let lease = actor.lease_for_agent("agent").unwrap();
    lease.stores().kv.put("prefs", "key", serde_json::json!("actor"))
        .await.unwrap();
    let direct = DriverSessionStores::new(backend, Scope::tenant("alice").unwrap()).unwrap()
        .try_for_agent("agent").unwrap();
    assert_eq!(direct.kv.get("prefs", "key").await.unwrap(), Some(serde_json::json!("actor")));
}

#[tokio::test]
async fn actor_bound_leases_keep_the_admitted_tenant_separate() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let alice_root = DriverSessionStores::new(
        Arc::new(MemoryStorage::new()), Scope::tenant("alice").unwrap(),
    ).unwrap();
    let bob_root = DriverSessionStores::new(
        Arc::new(MemoryStorage::new()), Scope::tenant("bob").unwrap(),
    ).unwrap();
    let alice_provider = alice_root.bind_actor(Arc::clone(&backend)).unwrap().unwrap();
    let bob_provider = bob_root.bind_actor(Arc::clone(&backend)).unwrap().unwrap();
    let alice = alice_provider.lease_for_agent("same-agent").unwrap();
    let bob = bob_provider.lease_for_agent("same-agent").unwrap();
    alice.stores().kv.put("prefs", "key", serde_json::json!("alice"))
        .await.unwrap();
    assert_eq!(bob.stores().kv.get("prefs", "key").await.unwrap(), None);
    assert_eq!(alice.stores().kv.get("prefs", "key").await.unwrap(),
        Some(serde_json::json!("alice")));
}

#[tokio::test]
async fn leases_keep_tenant_scope_and_agent_isolation() {
    let backend = Arc::new(MemoryStorage::new());
    let alice = DriverSessionStores::new(backend.clone(), Scope::tenant("alice").unwrap()).unwrap();
    let bob = DriverSessionStores::new(backend, Scope::tenant("bob").unwrap()).unwrap();
    let alice_lease = alice.lease_for_agent("agent").unwrap();
    let bob_lease = bob.lease_for_agent("agent").unwrap();
    alice_lease.stores().kv.put("prefs", "key", serde_json::json!("alice"))
        .await.unwrap();
    assert_eq!(bob_lease.stores().kv.get("prefs", "key").await.unwrap(), None);
}
```

Add this default-provider negative test in `port/mod_tests.rs`:

```rust
struct NonPinningProvider(InMemorySessionStores);

impl SessionStoreProvider for NonPinningProvider {
    fn for_agent(&self, agent_id: &str) -> AgentStores {
        self.0.for_agent(agent_id)
    }
}

#[test]
fn providers_without_a_pin_contract_fail_closed() {
    let provider = NonPinningProvider(InMemorySessionStores::new());
    assert!(matches!(provider.lease_for_agent("agent"),
        Err(SessionStoreLeaseError::Unsupported)));
}
```

Keep `retained_and_cloned_handles_follow_rebind_without_crossing_agent_scopes`
unchanged; the new test proves leases pin while that test proves ordinary
handles continue to follow rebind.

### 5. Capture tool observations and route post-commit sinks in the owned finalizer

**Files:** `crates/tinyagents-runtime/src/{driver.rs,hooks.rs,session.rs,types.rs}`,
`turn_driver/driver.rs`, and their sibling `driver_tests.rs`,
`hooks_tests.rs`, `session_tests.rs`, `turn_driver/driver_tests.rs`.

First add a recording `SessionDriver` returning one `ToolCallOutcome`; assert
the returned `DriverOutcome.tool_outcomes` equals it. Then extend the existing
`a_split_turn_is_driven_by_the_host_and_settled_by_its_tail` test: after
`tail.finish(Some(&run))`, assert `report.tool_outcomes.len() == 1`, its name
is `ask_user`, and its content is the result after `RewriteResult` middleware.
Add a paired `prepared_run_exposes_the_same_rewritten_tool_outcome` test that
uses the same scripted tool/model setup but calls `prepared.run().await`; assert
the same three fields on `TurnRun.report.tool_outcomes`. Put a
`RewriteResult` middleware after capture in the planner's `TurnPlan.context`
and assert both public drive paths observe the rewritten text
`host-enriched final text`. The capture is registered first/outermost; the
harness runs `after_tool` in reverse order, so capture observes the result
after each planner rewriter. Then add a recording
sink test that reads the appended transcript row inside the callback, a
cancellation test that drops the caller future after durable commit and
awaits a oneshot sent by the sink, a pre-commit failure test asserting no
callback, and a sink-error test asserting the turn result remains successful.
Each test must fail before the corresponding API/code exists. `DriverOutcome` derives
`PartialEq`, so add `PartialEq` to `ToolCallOutcome`'s existing `Debug, Clone`
derive and preserve the `DriverOutcome` derive and its existing assertions.
Add a value test comparing call id, name, structured arguments, success,
content, and duration. Extend `DriverOutcome`
with `Vec<ToolCallOutcome>` defaulted empty; have `TurnDriver` build a shared
`ToolOutcomeSink` during `prepare` and carry it through `PreparedTurn` into
`TurnTail`, which drains it for both `run` and `split`. Keep custom drivers'
empty default. Add the sink list to `SessionBuilder`, pass it to `Session`, and
invoke each sink in registration order from the existing owned
`terminal_guard.finalize_commit(receipt, observation, sinks)` task. Keep `SessionHooks::after_commit`
in that task and in its current position. The `TurnSummary` adapter consumes
outcomes and must not persist the raw argument/result. Verify with
`cargo test -p tinyagents-runtime post_commit` and then
`cargo test -p tinyagents-runtime`.

Capture must live in `TurnDriver::prepare`, not only `execute`: callers may
drive `PreparedTurn::split` themselves and never call `execute`. Create the
sink while preparing, push capture middleware before planner middleware so it
is outermost and observes rewritten final results, and carry the sink in
`PreparedTurn` into `TurnTail`. `TurnTail::finish` drains it into a new
`TurnReport.tool_outcomes` field; `PreparedTurn::run` already settles through
`TurnTail::finish`, and split callers do the same. `TurnDriver::execute` copies
the report's outcomes into `DriverOutcome`; `outcome_from_partial` initializes
custom-driver outcomes to an empty vector. This gives direct `prepare/run`,
`prepare/split`, and `execute` the same captured result.

Make these exact structural edits in `turn_driver/types.rs` and
`turn_driver/driver.rs`:

```rust
// TurnTail
pub outcomes: tinyagents_harness::turn_outcome::ToolOutcomeSink,

// TurnReport
pub tool_outcomes: Vec<tinyagents_harness::turn_outcome::ToolCallOutcome>,

// In TurnDriver::prepare, before pushing planner middleware:
let outcome_sink = tinyagents_harness::turn_outcome::new_tool_outcome_sink();
harness.push_middleware(Arc::new(
    tinyagents_harness::middleware::library::ToolOutcomeCaptureMiddleware::new(
        Arc::clone(&outcome_sink),
    ),
));

// In the existing TurnTail initializer:
outcomes: Arc::clone(&outcome_sink),

// In TurnTail::finish, before constructing TurnReport:
let tool_outcomes =
    tinyagents_harness::turn_outcome::drain_tool_outcomes(&self.outcomes);
// In the TurnReport literal:
tool_outcomes,
```

In `TurnDriver::prepare`, create `outcome_sink`, push
`Arc::new(tinyagents_harness::middleware::library::ToolOutcomeCaptureMiddleware::new(Arc::clone(&outcome_sink)))` into
the harness before the `for middleware in context` loop, and place the sink in
`TurnTail`. In `TurnTail::finish`, call `drain_tool_outcomes(&self.outcomes)`
and put the result into `TurnReport`. Add `tool_outcomes: Vec<ToolCallOutcome>`
to `TurnReport`; add `tool_outcomes: Vec<ToolCallOutcome>` to
`DriverOutcome` and initialize it to `Vec::new()` in `outcome_from_partial`
and each custom-driver literal. In `TurnDriver::execute`, preserve observations
on both result branches:

```rust
let TurnRun { partial, report, input_len } = prepared.run().await?;
let tool_outcomes = report.tool_outcomes;
match outcome_from_partial(partial, input_len) {
    Ok(mut outcome) => {
        outcome.tool_outcomes = tool_outcomes;
        Ok(outcome)
    }
    Err(mut failure) => {
        failure.partial.tool_outcomes = tool_outcomes;
        Err(failure)
    }
}
```

Retain the existing `outcome_from_partial` argument list at this call site;
the code above shows the required extraction and assignments. The outcome is
an in-memory runtime value.

Change the owned finalizer to accept the separate observation and sinks, not
to reconstruct observations from `CommitReceipt`:

```rust
tokio::spawn(async move {
    let _ = hooks.after_commit(receipt.clone()).await;
    for sink in sinks {
        let _ = sink.after_commit(&observation).await;
    }
    let _ = hooks.on_terminal_outcome(outcome).await;
    let _ = hooks.on_terminal(terminal).await;
})
```

Keep the existing `after_commit` call at the same place in the task. Build
`PostCommitObservation` from the committed receipt, the original request
input, usage captured from the completed turn, `DriverOutcome.tool_outcomes`,
and the resolved thread/agent identity before transferring ownership to the
spawned task. On `DriverFailure`, use observations only if a durable partial
commit is actually produced; pre-commit errors do not invoke sinks.

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

The typed-carrier regression test is concrete and host-neutral:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
struct HostAlpha(u8);
#[derive(Clone, Debug, PartialEq, Eq)]
struct HostBeta(&'static str);

#[test]
fn typed_extensions_survive_snapshot_copies_without_changing_json() {
    let original_json = serde_json::json!([]);
    let snapshot = ToolSnapshot::from_json(&original_json).unwrap()
        .with_extension(HostAlpha(7))
        .with_extension(HostBeta("host-owned"));
    assert_eq!(snapshot.extension::<HostAlpha>(), Some(&HostAlpha(7)));
    assert_eq!(snapshot.extension::<HostBeta>(), Some(&HostBeta("host-owned")));
    assert_eq!(snapshot.to_json(), original_json);

    let clone = snapshot.clone();
    let narrowed = snapshot.retaining(|_| true);
    let (retained, count) = snapshot.with_retained(&ToolSnapshot::default()).unwrap();
    assert_eq!(count, 0);
    for copy in [&clone, &narrowed, &retained] {
        assert_eq!(copy.extension::<HostAlpha>(), Some(&HostAlpha(7)));
        assert_eq!(copy.extension::<HostBeta>(), Some(&HostBeta("host-owned")));
        assert_eq!(copy.to_json(), original_json);
    }
}
```

Add `let replaced = clone.with_extension(HostAlpha(8));` and assert lookup
returns `HostAlpha(8)` in `replaced` while `snapshot` remains `HostAlpha(7)`.
This pins replacement semantics while retaining immutable sharing for clones
not changed by the builder.

### 7. Move context configuration into harness

**Files:** `crates/tinyagents-harness/src/config/context.rs`,
`config/mod.rs`, `src/lib.rs`, and new `config/context_tests.rs` declared
through `#[cfg(test)] #[path = "context_tests.rs"] mod tests;`. Update
TinyAgents compaction consumers and config exports; do not change OpenHuman
sources in this TinyAgents plan.

Add the sibling `config/context_tests.rs` first. The test body is:

```rust
use super::*;

#[test]
fn context_config_preserves_defaults_aliases_and_strategy_spellings() {
    let default = ContextConfig::default();
    let parsed: ContextConfig = toml::from_str("").unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), serde_json::to_value(default).unwrap());

    let canonical = r#"
enabled = false
microcompact_enabled = false
autocompact_enabled = true
microcompact_keep_recent = 9
tool_result_budget_bytes = 1234
summarizer_payload_threshold_tokens = 4000
summarizer_max_payload_tokens = 64000
summarizer_model = "test-model"
prefer_markdown_tool_output = false
compaction_enabled = true
compaction_trigger_tokens = 123
compaction_strategy = "summary"
"#;
    let value: ContextConfig = toml::from_str(canonical).unwrap();
    assert!(!value.enabled);
    assert!(!value.microcompact_enabled);
    assert!(value.autocompact_enabled);
    assert_eq!(value.microcompact_keep_recent, 9);
    assert_eq!(value.tool_result_budget_bytes, 1234);
    assert_eq!(value.summarizer_payload_threshold_tokens, 4000);
    assert_eq!(value.summarizer_max_payload_tokens, 64000);
    assert_eq!(value.summarizer_model.as_deref(), Some("test-model"));
    assert!(!value.prefer_markdown_tool_output);
    assert!(value.compaction_enabled);
    assert_eq!(value.compaction_trigger_tokens, Some(123));
    assert_eq!(value.compaction_settings().trigger_tokens, Some(123));
    assert_eq!(value.compaction_settings().strategy, CompactionStrategy::Summary);

    let aliases: ContextConfig = toml::from_str(
        "summarizer_payload_threshold_bytes=41\nsummarizer_max_payload_bytes=42",
    ).unwrap();
    assert_eq!(aliases.summarizer_payload_threshold_tokens, 41);
    assert_eq!(aliases.summarizer_max_payload_tokens, 42);

    for spelling in ["task_state", "typed", "s4"] {
        assert_eq!(CompactionStrategy::parse(spelling), Some(CompactionStrategy::TaskState));
    }
    for spelling in ["summary", "free_form", "s0"] {
        assert_eq!(CompactionStrategy::parse(spelling), Some(CompactionStrategy::Summary));
    }
    assert_eq!(CompactionStrategy::parse("  TASK-STATE  "), Some(CompactionStrategy::TaskState));
    assert_eq!(CompactionStrategy::parse("unknown"), None);
    let mut zero = value.clone();
    zero.compaction_trigger_tokens = Some(0);
    assert_eq!(zero.compaction_settings().trigger_tokens, None);
    let encoded = toml::to_string(&default).unwrap();
    let round_trip: ContextConfig = toml::from_str(&encoded).unwrap();
    assert_eq!(serde_json::to_value(round_trip).unwrap(), serde_json::to_value(default).unwrap());
}
```

Then move the full type definitions below into `config/context.rs`, preserving
the 12 fields and serde attributes exactly. No host `JsonSchema` derive or
OpenHuman imports belong in the harness. The tests fail to compile before the
move because the harness has no `ContextConfig`; run the focused test after
each migration substep.

```rust
use serde::{Deserialize, Serialize};

pub const DEFAULT_KEEP_RECENT_TOOL_RESULTS: usize = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ContextConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub microcompact_enabled: bool,
    #[serde(default = "default_true")]
    pub autocompact_enabled: bool,
    #[serde(default = "default_microcompact_keep_recent")]
    pub microcompact_keep_recent: usize,
    #[serde(default = "default_tool_result_budget_bytes")]
    pub tool_result_budget_bytes: usize,
    #[serde(default = "default_summarizer_payload_threshold_tokens", alias = "summarizer_payload_threshold_bytes")]
    pub summarizer_payload_threshold_tokens: usize,
    #[serde(default = "default_summarizer_max_payload_tokens", alias = "summarizer_max_payload_bytes")]
    pub summarizer_max_payload_tokens: usize,
    #[serde(default)]
    pub summarizer_model: Option<String>,
    #[serde(default = "default_true")]
    pub prefer_markdown_tool_output: bool,
    #[serde(default = "default_true")]
    pub compaction_enabled: bool,
    #[serde(default)]
    pub compaction_trigger_tokens: Option<u64>,
    #[serde(default)]
    pub compaction_strategy: CompactionStrategy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionSettings {
    pub trigger_tokens: Option<u64>,
    pub strategy: CompactionStrategy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionStrategy {
    #[default]
    TaskState,
    Summary,
}

impl ContextConfig {
    pub fn compaction_settings(&self) -> CompactionSettings {
        CompactionSettings {
            trigger_tokens: self.compaction_trigger_tokens.filter(|value| *value > 0),
            strategy: self.compaction_strategy,
        }
    }
}

impl CompactionStrategy {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "task_state" | "typed" | "s4" => Some(Self::TaskState),
            "summary" | "free_form" | "s0" => Some(Self::Summary),
            _ => None,
        }
    }
}

fn default_enabled() -> bool { true }
fn default_true() -> bool { true }
fn default_microcompact_keep_recent() -> usize { DEFAULT_KEEP_RECENT_TOOL_RESULTS }
fn default_tool_result_budget_bytes() -> usize {
    crate::config::TurnConfig::default().tool_result_budget_bytes
}
fn default_summarizer_payload_threshold_tokens() -> usize { 4_000 }
fn default_summarizer_max_payload_tokens() -> usize { 64_000 }

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            microcompact_enabled: default_true(),
            autocompact_enabled: default_true(),
            microcompact_keep_recent: default_microcompact_keep_recent(),
            tool_result_budget_bytes: default_tool_result_budget_bytes(),
            summarizer_payload_threshold_tokens: default_summarizer_payload_threshold_tokens(),
            summarizer_max_payload_tokens: default_summarizer_max_payload_tokens(),
            summarizer_model: None,
            prefer_markdown_tool_output: default_true(),
            compaction_enabled: default_true(),
            compaction_trigger_tokens: None,
            compaction_strategy: CompactionStrategy::default(),
        }
    }
}
```

Export all three types and the default constant from `config/mod.rs` and the
harness crate root. Keep the `ContextConfig` field docs describing only the
generic behavior; host-only product policy and environment loading stay in
OpenHuman.

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

Long commands must use the super-worktree cancel-aware wrapper, one job, and
debug info disabled. Run these from the TinyAgents workspace root:

```sh
cd -- "/home/enamakel/work/openhuman/worktrees/epic-8088-63-session-runtime/vendor/tinyagents" && CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 "/home/enamakel/work/openhuman/worktrees/epic-8088-63-session-runtime/scripts/ci-cancel-aware.sh" cargo check --workspace --all-targets
cd -- "/home/enamakel/work/openhuman/worktrees/epic-8088-63-session-runtime/vendor/tinyagents" && CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 "/home/enamakel/work/openhuman/worktrees/epic-8088-63-session-runtime/scripts/ci-cancel-aware.sh" cargo clippy --workspace --all-targets -- -Dwarnings
cd -- "/home/enamakel/work/openhuman/worktrees/epic-8088-63-session-runtime/vendor/tinyagents" && CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 "/home/enamakel/work/openhuman/worktrees/epic-8088-63-session-runtime/scripts/ci-cancel-aware.sh" cargo test --workspace
```

Do not set `CARGO_TARGET_DIR`.

## Risks to resolve during implementation

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
