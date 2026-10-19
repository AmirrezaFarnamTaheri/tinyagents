# Plan: move standard tools into TinyAgents and add gated `exec`

This plan implements `senamakel/tinyagents#65`, which is part of
`tinyhumansai/openhuman#8088`. It covers the whole leaf: moving the
`tinytools-std` implementation families and their tests into
`tinyagents-harness-tools`, adding shell and install tools behind generic host
ports, moving the plan-review and media-artifact tool wrappers found by the
OpenHuman core-shrink audit, and removing the harness's last dependency on
`tinytools-std`.

The implementation belongs in TinyAgents. `tinytools` remains the protocol,
types, and policy vocabulary owner; `tinyagents-harness` owns the agent loop,
portable runtime utilities, and multimodal resolution; `tinyagents-harness-tools`
owns reusable tool implementations; OpenHuman keeps the concrete policy,
approval, sandbox, audit, plan-review, and artifact-storage adapters for its
later #213 integration. No OpenHuman type or dependency is introduced here.

## Starting state and source audit

The plan was prepared against TinyAgents `aca9fd54666ce8fdcc02ae4d6499baa2a3354cd5`
on branch `epic-8088-65-harness-tools`. Issue #65 is open and has no comments.
OpenHuman audit comment 6254297338 adds three rows to the issue's original
scope: `sandbox/shell_progress.rs`, `plan_review/tool.rs`, and
`media/generation/artifact_tool.rs`. The OpenHuman task #213 will consume these
new generic APIs later; it does not block this crate-local change.

The current `tinytools-std` public modules are `detect_tools`, `file_state`,
`filesystem`, `network`, `sanitize`, `timeout`, and `url_guard`. The prior
`tinytools#4` change already moved `sanitize` into `tinytools`; do not copy it
back. The harness currently references `tinytools-std` only from
`crates/tinyagents-harness/src/multimodal/resolve.rs` and its tests, for
`url_guard`. That is the only dependency edge that would otherwise prevent
removing `tinytools-std` from the harness manifest.

The important dependency shape is:

```text
tinyagents-harness-tools ──depends on──> tinyagents-harness ──does not depend on──> harness-tools
```

Do not reverse this edge. Move the shared URL-validation implementation into
an unconditional `tinyagents-harness::url_guard` module. It uses only `std`,
`anyhow`, Tokio's already-enabled `rt` support, and `tracing`; convert the
source `log::{debug,warn}` calls to equivalent `tracing` macros. It must not
add reqwest, flate2, tar, zip, a resolver, or any multimodal feature as a
dependency. Multimodal resolution uses it directly and `harness-tools/net`
consumes it through its existing dependency on `tinyagents-harness`. This removes the optional
`tinytools-std` dependency without creating a crate cycle or adding a shim
crate. The module must retain the current `ValidatedUrl`, validation,
DNS-resolution, public-address checks, and exact address-pinning contract;
preserve all URL-guard tests with the move.

Before coding, re-read the current source and tests from the exact parent
checkout that the OpenHuman #213 adapter will use. In particular inspect:

| Current OpenHuman source | Behavior to preserve / destination |
| --- | --- |
| `crates/openhuman-core/src/tools/impl/system/shell.rs` and sibling `shell_tests*.rs` | `shell` tool metadata, schema, output limits, command analysis and policy call order, sandbox/backend selection, audit, progress, cancellation, and errors; rename the generic tool to `exec` |
| `tools/impl/system/shell_support.rs`, `shell_description.rs` | Shell descriptions, argv/environment helpers, timeout text, exit-124 attribution, status mapping, and output formatting |
| `tools/impl/system/install_tool.rs` and `install_tool_tests.rs` | Package validation, allowed managers and exact argv, interactive/feature/autonomy/rate checks, timeout, output cap, and error messages |
| `tools/schema_cache.rs` | `static_schema!` and parse-once embedded JSON behavior |
| `sandbox/shell_progress.rs` and `shell_progress_tests.rs` | Byte counting, bounded last-line tracking, heartbeat interval/text, and context progress events |
| `plan_review/tool.rs` and tests | Tool contract and current behavior; the host's gate/event state machine is behind `PlanReviewPort` |
| `media/generation/artifact_tool.rs` and tests | Inner-tool metadata/results, multiple files, blob/path handling, artifact errors, markdown replacement, and result preservation; host filing/storage is behind `ArtifactFiling` |

The tests described in issue #65 are the source of truth for behavior. Copy
them with their assertions and fixtures, then adapt only test imports and
host-owned fakes to the new ports. Do not copy OpenHuman security policy,
`SecurityPolicy`, `AuditLogger`, TinyBox/TinyBus contracts, artifact stores,
`Production`, SaaS switches, Docker/TinyBox selection, or OpenHuman context
types into TinyAgents. The generic tool asks its injected ports; OpenHuman's
later adapter implements those ports. Keep the existing host's approval gate
and turn routing in the host. The tool must still run `ExecGate::authorize`
inside `execute` immediately before invoking the backend, so a denied policy
gate never spawns a process even if the outer approval gate let the call reach
the tool.

## Contract to establish before porting

Put public seams in `crates/tinyagents-harness-tools/src/exec/ports.rs`,
`plan_review/port.rs`, and `media/artifact_filing.rs`. Keep the port types free
of host-specific dependencies. These definitions are complete contracts for
the implementation; adapt names only if the inspected current
`ToolRunContext` or media-output types require a mechanical signature change.

### `exec` host boundary

`ExecGate` decides whether an operation may proceed and owns host audit
recording. Its authorization call is awaited before backend execution. It
returns a permit only for an allowed call; a denial is returned to the model
as a stable `ToolResult::error` and does not call `ExecBackend`. The host can
use `ToolRunContext::host_extension()` to read its own turn state without
TinyAgents importing or downcasting to a host type.

```rust
use std::{collections::BTreeMap, ffi::OsString, path::PathBuf, time::Duration};
use async_trait::async_trait;
use tinytools::ToolRunContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecOperation {
    Shell { command: String, category: Option<String> },
    Install { package: String, manager: String },
}

#[derive(Debug, Clone)]
pub struct ExecAuthorizationRequest {
    pub operation: ExecOperation,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct ExecPermit {
    pub action_dir: PathBuf,
    pub environment: BTreeMap<OsString, OsString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecDenied(pub String);

#[derive(Debug, Clone)]
pub struct ExecAuditEvent {
    pub operation: ExecOperation,
    pub allowed: bool,
    pub success: bool,
    pub duration: Duration,
}

#[async_trait]
pub trait ExecGate: Send + Sync {
    async fn authorize(
        &self,
        request: &ExecAuthorizationRequest,
        context: Option<&dyn ToolRunContext>,
    ) -> Result<ExecPermit, ExecDenied>;

    fn record(&self, event: ExecAuditEvent);
}

#[derive(Debug, Clone)]
pub struct ExecRequest {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub action_dir: PathBuf,
    pub environment: BTreeMap<OsString, OsString>,
    pub timeout: Option<Duration>,
    pub shell_command: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellFlavor {
    Posix,
    Cmd,
}

#[async_trait]
pub trait ExecBackend: Send + Sync {
    fn shell_flavor(&self) -> ShellFlavor;

    async fn execute(
        &self,
        request: ExecRequest,
        context: Option<&dyn ToolRunContext>,
        progress: Option<ExecProgress>,
    ) -> anyhow::Result<ExecOutput>;
}
```

`ExecOperation` is input to authorization/audit. `ExecRequest` is the
backend-neutral launched process request. `shell_command` is only populated
for the shell operation, where the backend chooses the host's configured shell;
install always uses `program` plus separate `args`, with no shell interpolation.
`ExecBackend::shell_flavor` supplies the model-facing shell description and
parameter text. The permit supplies the authorized working directory and
environment. The backend receives the original tool context so the host
implementation can select native, sandbox, Docker, TinyBox, or SaaS execution
using its own existing rules. `ExecProgress` is the cloneable generic output meter, not a
host type; it provides `record(&[u8])` and `snapshot() -> (u64, Option<String>)`.
The adapter feeds it for both stdout and stderr chunks. A denied operation is
recorded once with `allowed = false` before returning; a permitted operation
is recorded once after completion with `allowed = true` and the final success
flag. Process launch failures are audited as unsuccessful.

The shell result conversion must preserve the current behavior: output is
decoded lossily as UTF-8; each stdout/stderr stream is capped at 1 MiB at a
valid UTF-8 boundary with the existing truncation marker; the tool's
`max_result_size_chars` remains 30,000; successful stderr is appended as
`[stderr]`; nonzero/signal exit uses the current `tinytools::command_failure`
mapping; exit 124 adds the existing “command's own timeout” note; and an
explicit tool timeout produces the current timeout message including captured
stdout and stderr. Docker/module output uses the same conversion. The existing
Python UTF-8 environment defaults and caller overrides remain unchanged.

The built-in process backend must own child cleanup. Every timeout, dropped
execution future, and cancellation path kills the entire process group and
reaps the child before returning/dropping. Keep the existing `timeout`
settings and `output_or_kill` semantics. The generic `ExecBackend` may be
host-provided, but the crate's native backend tests must prove descendants do
not survive timeout or cancellation. The authorization denial test uses a fake
backend with an invocation counter and asserts zero calls after denial.

### Plan-review port

`RequestPlanReviewTool` owns only parsing, public tool metadata, and mapping the
resolution to the existing stable result text. The port owns the host review
surface, user interaction, event publication, expiry, plan-mode check, and
non-interactive policy. A caller without an installed port fails closed with
an actionable error; it must not silently auto-approve. The host adapter can
preserve OpenHuman's non-interactive auto-approve rule explicitly.

```rust
use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanReviewRequest {
    pub summary: String,
    pub steps: Vec<String>,
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanReviewResolution {
    Approve,
    Reject,
    Revise { feedback: String },
}

#[async_trait]
pub trait PlanReviewPort: Send + Sync {
    async fn request_review(
        &self,
        request: PlanReviewRequest,
        context: Option<&dyn tinytools::ToolRunContext>,
    ) -> Result<PlanReviewResolution, String>;
}
```

The tool keeps `request_plan_review`, `PermissionLevel::None`, no external
effect, and an unbounded timeout because the port's review deadline is
authoritative. It trims summary and nonempty string steps exactly as the source
does. Preserve the approve/reject/revise result wording and make the port's
missing/expired/error result fail closed; never turn port errors into approval.

### Media artifact filing port

The generic wrapper is feature-gated as `media`; this feature may add only the
optional `tinyinference-video` dependency needed for its existing
`ProgressFn` callback type. It must not enable `tinyagents-harness/multimodal`
or pull `flate2`, `tar`, `zip`, or reqwest through that feature. Keep
`MediaArtifactTool<T>` generic over `T: tinytools::Tool` and pass the
`ToolRunContext` to the per-call inner-tool factory so the host adapter can
read its own approval/chat extension. The factory remains
`Arc<dyn Fn(ProgressFn, Option<&dyn ToolRunContext>) -> T + Send + Sync>`;
it must not mention `ApprovalChatContext`. The generic wrapper keeps the inner
tool's name, description, schema, policy, permissions, scope, category, result
content, and progress behavior. It parses
each JSON result's `artifacts` array and invokes the injected `ArtifactFiling`
port once per recognizable output. It does not store files, create product
artifact records, or depend on TinyArtifacts/OpenHuman.

```rust
use async_trait::async_trait;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedArtifact {
    pub path: PathBuf,
    pub kind: String,
    pub media_type: Option<String>,
    pub bytes: Option<u64>,
    pub title: String,
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiledArtifact {
    pub id: String,
    pub path: String,
}

#[async_trait]
pub trait ArtifactFiling: Send + Sync {
    async fn file(&self, generated: GeneratedArtifact) -> Result<FiledArtifact, String>;
}
```

The wrapper replaces only the entry's `path`, adds `artifact_id` on success,
or adds `artifact_error` on a per-entry failure; it keeps processing later
entries and leaves unrecognized shapes untouched. It rewrites matching source
paths in `markdown_formatted`. Keep the `n > 1` title suffix and 60-character
prompt title cap. OpenHuman's later adapter implements `ArtifactFiling` and
retains the existing versioning, trusted-file-root checks, blob cleanup, and
reference allowlist policy.

## Bite-sized implementation tasks

Complete these in order. Each task can be a separate commit; do not combine or
squash checkpoints. Every changed Rust test stays in the repository's sibling
`*_tests.rs` convention and begins with `use super::*;`. No inline test modules,
`tests.rs`, `test.rs`, or `<module>_test.rs`. Ported tests retain the original
assertions. Tests must use fakes or loopback listeners and never call a real
third-party service.

### 1. Move the shared URL guard and detach the harness from `tinytools-std`

Files: `crates/tinyagents-harness/src/url_guard.rs` (new),
`crates/tinyagents-harness/src/lib.rs`,
`crates/tinyagents-harness/src/multimodal/resolve.rs`,
`crates/tinyagents-harness/src/multimodal/resolve_tests.rs`,
`crates/tinyagents-harness/Cargo.toml`,
`crates/tinyagents-harness-tools/Cargo.toml`.

Move `vendor/tinytools/crates/tinytools-std/src/url_guard/mod.rs` to
`crates/tinyagents-harness/src/url_guard.rs` and
`url_guard/mod_tests.rs` to `crates/tinyagents-harness/src/url_guard_tests.rs`.
Preserve its full implementation and tests, changing only the logging macros
and crate paths. Export `pub mod url_guard;` from the harness root. Change
multimodal resolution and tests to use
`crate::url_guard`. Update the public docs so the SSRF rules and DNS pinning
contract name the new owner. Remove the optional `tinytools-std` dependency and
the `dep:tinytools-std` edge from the harness `multimodal` feature. Do not add
the URL module to `harness-tools`; its `net` module imports the utility from
its existing harness dependency.

Red/green: run the focused `tinyagents-harness` URL guard and multimodal
resolve tests before the move and record their baseline; after the move run the
same tests plus `cargo tree -p tinyagents-harness -e normal` and confirm no
`tinytools-std` edge remains. This is a relocation, so compare every test
assertion and fixture, not just the test count.

### 2. Add feature gates and port all remaining standard tools

Files: `crates/tinyagents-harness-tools/Cargo.toml`,
`crates/tinyagents-harness-tools/src/lib.rs`,
new `src/fs/`, `src/net/`, and `src/exec/` modules; remove the harness's
`tinytools-std` manifest entry. Add opt-in `fs`, `net`, and `exec` features;
preserve the existing `default = ["todos", "goals", "skills"]` behavior.
Make dependencies optional at the narrowest family feature that uses them.
The `fs` feature owns filesystem/search/git/file-state helpers and their
dependencies, `net` owns reqwest/futures/sha helpers and uses
`tinyagents_harness::url_guard`, and `exec` owns process, timeout, and
`detect_tools` dependencies. Preserve target-specific Unix/Windows
dependencies. Do not make a new feature activate an unrelated old default.

Move all `tinytools-std` files except `sanitize` with their fixtures and tests,
preserving modules and public APIs under `tinyagents_harness_tools::{fs,net,exec}`:

| New module | Source modules and contents |
| --- | --- |
| `fs` | `filesystem/` including `FsGate`, every listed filesystem tool (`file_read`, `file_write`, `edit_file`, `apply_patch`, `grep`, `glob_search`, `list_files`, `csv_export`, `read_diff`, `git_operations`, `run_linter`, `run_tests`, `update_memory_md`, `image_info`, `workspace_state`), `file_sink`, `read_only`, text helpers, fixtures, contract tests, and test support; `file_state/` including agent context, ops, types, tests; preserve `WorkspaceStateTool` and `FsGate` APIs |
| `net` | `network/` (`http_request`, `web_fetch`, `curl`, `pushover`, `NetGate`, payment/extractor traits, test support, fixtures, contract tests); `url_guard` call sites are changed to harness utility imports |
| `exec` | `detect_tools/` and `timeout/` (settings, command environment, output collection, process-group kill/reap, cleanup tracking, tests); `ExecGate`, `ExecBackend`, shell/install, and progress are added in later tasks |

The exact source-root map is:

| Source root | Destination root |
| --- | --- |
| `vendor/tinytools/crates/tinytools-std/src/filesystem/**` | `crates/tinyagents-harness-tools/src/fs/filesystem/**` |
| `vendor/tinytools/crates/tinytools-std/src/file_state/**` | `crates/tinyagents-harness-tools/src/fs/file_state/**` |
| `vendor/tinytools/crates/tinytools-std/src/network/**` | `crates/tinyagents-harness-tools/src/net/network/**` |
| `vendor/tinytools/crates/tinytools-std/src/detect_tools/**` | `crates/tinyagents-harness-tools/src/exec/detect_tools/**` |
| `vendor/tinytools/crates/tinytools-std/src/timeout/**` | `crates/tinyagents-harness-tools/src/exec/timeout/**` |
| `vendor/tinytools/crates/tinytools-std/src/url_guard/mod.rs` | `crates/tinyagents-harness/src/url_guard.rs` |
| `vendor/tinytools/crates/tinytools-std/src/url_guard/mod_tests.rs` | `crates/tinyagents-harness/src/url_guard_tests.rs` |

`**` includes all source files, nested modules, sibling test files, and
fixtures below that directory, keeping relative paths. Do not copy
`tinytools-std/src/lib.rs`, its manifest, or `sanitize/**`; explicitly declare
the new root modules and keep sanitization with TinyTools. The remaining
source map is directory-to-directory, with file contents preserved
mechanically. Before deleting the source dependency, search all workspace
members and examples for `tinytools_std` and `tinytools-std`; replace each
actual implementation import with the new owning crate/module. Do not retain a
compatibility re-export in TinyTools. Check every former `tinytools-std` public
item and test/fixture is accounted for; `sanitize` is the only intentional
retirement from the source crate because tinytools#4 owns it now.

At the end of this task, these must compile independently:

```sh
cargo check -p tinyagents-harness-tools --no-default-features --features fs
cargo check -p tinyagents-harness-tools --no-default-features --features net
cargo check -p tinyagents-harness-tools --no-default-features --features exec
cargo check -p tinyagents-harness-tools --no-default-features
```

The final command proves a disabled-tools build remains lean and does not
accidentally rely on a new module's imports. Add or update crate docs showing
the three features and their public module paths.

### 3. Establish `ExecGate`, backend, result, and progress contract

Files: `crates/tinyagents-harness-tools/src/exec/ports.rs`,
`exec/progress.rs`, `exec/types.rs`, `exec/mod.rs`, plus sibling
`ports_tests.rs` and `progress_tests.rs`.

Implement the contract above with imports and derives exactly as shown. `Exec`
types are public through `exec`; the progress meter itself is cloneable and
thread-safe and follows the current `ShellOutputMeter` behavior: count bytes
from both streams, process `\n` and `\r` as line endings, retain a maximum
1,024 characters of pending line and only the last complete nonempty line of
up to 120 characters, and recover poisoned mutexes with `into_inner()`. Keep
the 15-second interval and exact progress messages (`running Ns · no output
yet`, byte/KB/MB figures, optional last line). The progress wrapper reports
through `ToolRunContext::report_progress` when context is present and performs
no ticker task otherwise.

Tests use a fake `ExecGate` and `ExecBackend` to prove the public seam is
object-safe, thread-safe, and that no-context calls work. For this API-addition
task, the expected first red is compile failure because the public types do
not exist yet; it is not a behavioral bug regression. In task 4, first add
the required denial-before-backend test and verify its assertion fails against
an implementation that calls the backend before authorization, then implement
the ordered call sequence and confirm it passes.

### 4. Port `shell` as `ExecTool` and map the host contract

Files: `crates/tinyagents-harness-tools/src/exec/tool.rs`,
`exec/tool_tests.rs`, `exec/shell_support.rs`,
`exec/shell_support_tests.rs`, `exec/shell_description.rs`, and static schema
JSON beside the tool. Copy `shell.rs`, `shell_support.rs`,
`shell_description.rs`, and the source behavior tests from the OpenHuman
checkout into this module. Rename the tool's public struct to `ExecTool` and
wire it through required injected `Arc<dyn ExecGate>` and
`Arc<dyn ExecBackend>` fields. It is named `exec` (wire identity), preserving
the shell schema (`command`, optional `category`, `timeout_secs`), exact shell
description for POSIX/cmd flavors supplied as a generic `ShellFlavor`,
`PermissionLevel::Execute`, unbounded default timeout, and 30,000-character
result cap.

Execution order is a contract and has a focused test: parse/validate the
command; form `ExecAuthorizationRequest`; await `ExecGate::authorize`; on
denial map the gate reason to the source refusal result and return without
calling backend or recording an allowed completion; on permit, build
backend-neutral `ExecRequest` from the permit, call the injected backend with
the call context and output meter, map output to `ToolResult`, then call
`ExecGate::record` exactly once with allowed=true and the final success flag.
On backend launch error, return the stable sandbox-execution failure result
and record an unsuccessful result. Do not call policy a second time after
approval; the host's gate must perform its final revalidation immediately
before spawn. Host-specific authorization must deny before the backend's
process creation.

Port/retain behavior assertions for schema/name/permission/timeout; output
Unicode and per-stream truncation boundaries; success with/without stderr;
nonzero exit and signal mapping; exit 124 note; explicit timeout text and
captured output; progress heartbeats and last-line accounting; environment
defaults/override; native, sandbox, Docker and TinyBox adapter fixture results
where they are representable through `ExecOutput`. Keep adapter-specific
scenario tests in OpenHuman #213; the TinyAgents suite tests only fake ports
and the in-crate native backend. Verify installer argv is not routed through
shell string construction.

### 5. Port process supervision and argv-only install

Files: `crates/tinyagents-harness-tools/src/exec/install_tool.rs`,
`exec/install_tool_tests.rs`, and `exec/native_backend.rs` with its sibling
`native_backend_tests.rs` if the existing timeout module does not fully cover
the behavior after relocation.

Copy the install tool into the new `exec` module as `InstallTool`, requiring
the same injected gate/backend as `ExecTool`. Preserve the manager allowlist
and each exact binary/argv prefix; validate package names against the source
ASCII set and 200-byte cap; reject missing PATH executables; preserve the
interactive approval, feature-enabled, autonomy, and action-budget checks by
representing these as `ExecGate` denials in the host adapter, not as
TinyAgents policy. The tool creates `ExecOperation::Install`, awaits the gate,
then gives the backend an `ExecRequest` with `shell_command: None`, exact
separate `program` and `args`, workspace action dir, 300-second timeout, and
authorized environment. There is no `sh -c`, `cmd /C`, or concatenated argv
path for installs. Keep source error/output/truncation wording.

The native backend configures child process groups, streams stdout and stderr
into the bounded progress meter and captured output, applies explicit timeout,
kills the process group and reaps on timeout, cancellation, and dropped future,
and returns a completed `ExecOutput`. On Windows use the existing
platform-supported process cleanup path; do not claim process-tree cleanup
where the platform does not provide it. Tests use a temporary executable
script/process and assert: only the expected argv reaches it; malformed
package inputs never launch; denied calls have a zero backend count; timeout
terminates a spawned child and descendant and waits for the child; cancel/drop
also terminates and reaps; separate stdout/stderr truncation remains Unicode
safe; and exit status survives. No real package manager or network install is
run.

Port the OpenHuman `install_tool` behavioral tests with fake ports. Write
behavioral RED first for injection attempts in the package parameter and
denial-before-spawn, then make the tests green. The process timeout/cancel
cases must fail if only the direct child is killed or if the child is not
waited/reaped.

### 6. Move parse-once schema utility without the host module tree

Files: `crates/tinyagents-harness-tools/src/schema.rs` (or
`src/schema_cache.rs`), `src/lib.rs`, and sibling schema tests. Copy the
`static_schema!` macro and `parse_embedded` semantics from OpenHuman's
`tools/schema_cache.rs`, including call-site panic text and cloned
`LazyLock<Value>`. Export the macro and parse helper only as required by
downstream tool crates; do not create an OpenHuman-shaped `tools` tree. Move
the shell and install schema JSON into their respective exec tool directories,
use the macro in both tools, and retain malformed-schema and parsed-once/value
tests. No dynamic schema cache or host type is introduced.

### 7. Move plan review and media artifact wrappers behind ports

Files: `crates/tinyagents-harness-tools/src/plan_review/{mod.rs,port.rs,tool.rs,tool_tests.rs}`,
`src/media/{mod.rs,artifact_filing.rs,artifact_tool.rs,artifact_tool_tests.rs}`,
and `src/lib.rs`.

Port `RequestPlanReviewTool` to `plan_review::RequestPlanReviewTool` with an
`Arc<dyn PlanReviewPort>`. Preserve tool name, schema, permission,
external-effect flag, unbounded timeout, argument trimming, and result text.
The port owns interactive/noninteractive policy, review event delivery,
plan-mode gating, and its own expiry. If absent or failing, return a clear
error and never approve. Tests use a deterministic fake port to assert request
fields, preserved resolution-to-result mapping, missing-port fail-closed
behavior, and that feedback is not silently discarded.

Port `MediaArtifactTool<T>` to `media::MediaArtifactTool<T>` with an injected
`Arc<dyn ArtifactFiling>`. Add optional `tinyinference-video` under the new
`media` feature solely for `ProgressFn`; update the per-call inner factory to
accept `Option<&dyn ToolRunContext>` instead of `ApprovalChatContext`, letting
the host-specific factory downcast its own context in the adapter. Remove OpenHuman `Production`, artifact store,
TinyArtifacts, TinyStorageDrivers, file-root, blob-store, approval-chat, and
product reference-allowlist dependencies. Keep the generic output walk,
per-entry continuation, ID/error insertion, path rewrite, title cap/suffix,
and all `Tool` metadata/progress delegation. Where a generated `path` is a
blob key, pass that key as `PathBuf` to the port unchanged; the host filing
implementation decides how to fetch it from its own blob store. Tests use
fake filers and retain the source tests for single/multiple outputs, success,
failure isolation, unknown result shape, markdown path replacement, and
per-call progress reporting. OpenHuman's file-root/trusted-reference and
blob-cleanup checks remain host-adapter responsibilities in #213.

### 8. Public API, manifest cleanup, docs, and migration audit

Files: all previous changed files plus crate README/docs if applicable.

Remove `tinytools-std` from every workspace package manifest and from
`Cargo.lock` through Cargo resolution. Keep TinyTools' definitions-only
boundary intact and do not touch the TinyTools gitlink. Search the full
TinyAgents workspace and test fixtures for `tinytools_std`,
`tinytools-std`, `tinytools_std::sanitize`, OpenHuman module names, and
OpenHuman crate paths. The only remaining `sanitize` imports must resolve to
the already-merged TinyTools API; there must be no `tinytools-std` dependency
or compatibility path. Update harness-tools README with feature selection,
ports, constructors, and the distinction between reusable tool mechanisms
and host authorization/storage.

## Final verification

Run these in the TinyAgents checkout after all tasks, through
`scripts/ci-cancel-aware.sh` for long commands. Do not export
`CARGO_TARGET_DIR` or place build output under `/tmp`.

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check -p tinyagents-harness-tools --no-default-features
cargo check -p tinyagents-harness-tools --no-default-features --features fs
cargo check -p tinyagents-harness-tools --no-default-features --features net
cargo check -p tinyagents-harness-tools --no-default-features --features exec
cargo tree -p tinyagents-harness -e normal
cargo tree -p tinyagents-harness-tools -e normal
cargo machete
```

Also re-run focused `tinyagents-harness` multimodal/URL guard tests and the
`tinyagents-harness-tools` tests for each feature, both no-default and
combined-feature builds. Confirm the resolved dependency tree has no
`tinytools-std` package. Review copied tests against source and report any
assertion that cannot be retained with the reason; do not silently drop
coverage. Formatting, check, clippy, workspace tests, feature-disabled builds,
and dependency audit are required before the TinyAgents PR. The OpenHuman
adapter and its integration tests are separate work in #213 and are not
claimed by this leaf.
