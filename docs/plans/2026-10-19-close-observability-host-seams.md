# Plan: move OpenHuman close and observability seams into TinyAgents

Issue: [senamakel/tinyagents#64](https://github.com/senamakel/tinyagents/issues/64), part of [tinyhumansai/openhuman#8088](https://github.com/tinyhumansai/openhuman/issues/8088).

This plan targets the current `epic/8088-tinyagents-slim` fork branch. It is based on the TinyAgents fork at `aca9fd54666ce8fdcc02ae4d6499baa2a3354cd5` and the OpenHuman super-worktree's current source. It does not assume future interfaces from issue #60 (session reuse), #63 (session phases), or OpenHuman #212. Recheck this plan against the merged #60 integration before starting implementation; merge/review intervening fork changes and adapt the plan without discarding either side's behavior.

## Goal and boundaries

Move generic close, turn outcome, journal, trace, no-progress ladder, and model-budget behavior from OpenHuman's TinyAgents adapter into `tinyagents-harness`. Keep OpenHuman-specific model sourcing, stop taxonomy/copy, recovery headroom, terminal-inference fast-halt policy, security and sandbox decisions, provider usage accounting, and persistence adapters in OpenHuman. No `openhuman_core` types or dependencies may enter TinyAgents.

The current harness already owns `close.rs`, `turn_stop.rs`, `turn_outcome.rs`, `observability` (including `StoreEventJournal`), `no_progress`, and `cost::ModelBudget`. Treat existing harness implementations as the baseline and extend or consolidate them; do not duplicate equivalent algorithms under new names. Issue #64's “rows are gone” means the generic implementations and duplicate shims disappear from the specified OpenHuman source directories once the host adapts to existing/new upstream APIs.

Required user-visible invariant: using OpenHuman's copy injection produces byte-for-byte identical checkpoint, deterministic final-close, and stop-summary output to current OpenHuman. Port the existing host golden assertions before deleting their source. Keep every behavior assertion unless the corresponding capability is expressly removed by this issue; none of the listed capabilities is being removed.

## Repository-grounded ownership and dependencies

| Existing OpenHuman source | Destination / action | Host behavior retained |
|---|---|---|
| `crates/openhuman-core/src/agent/session_host/driver/grounded_close.rs`: `RepairUsage` (30-98), `SummarizerSourceModel` (116-225), `classified_halt_close` (235-249), `repair_required_output` (253-334), and `close_if_needed` (365-464) | Move generic close accounting, source-model decorator, halt-close selector, required-output repair and orchestration to `crates/tinyagents-harness/src/close.rs`; use host source/cost ports below | `TurnModelSource` implementation, provider/model selection, thread context and output token cap, native `BilledUsage` conversion/catalog `CallCost`, provider pricing lookup, progress event delivery, host `RequiredOutput` construction and `Prepend` fallback selection |
| `agent/session_host/turn_checkpoint.rs`: result truncation/rendering, harness-instruction matching, duplicate `CloseViolation`, deterministic checkpoint/final summary, repair and verifier prompt assembly | Reuse `harness::close`; delete duplicate helpers after call sites migrate | OpenHuman's `CloseCopy` implementation containing product strings and exact existing rendering behavior |
| `agent/session_host/stop_summary.rs`: wrappers (36-82), `StopReasonKind` (85-220), scrubbed grouping/renderer (223-319) | Move generic bounded grouping/rendering to `harness::turn_stop` | Product `StopReasonKind` parser and failure-class mapping, stable `turn_stop.*` keys, scrubber, host `StopCopy` implementation |
| `agent/tinyagents/turn_outcome.rs`: `DeferredTurnTerminal` (12-94) | Move deferred `TurnStopped`, `RunFailed`, and `RunFinalized` event ownership/emission to `harness::turn_outcome` | `TinyagentsTurnOutcome`, session sidecar plumbing, and durable status calls currently reached through its `TurnJournal` field |
| `agent/tinyagents/journal.rs`: `TurnJournal` (123-225) | No new harness facade: generic event persistence already uses `StoreEventJournal`, `JournalSink`, and `HarnessEventJournal` | Keep status snapshots, `DispatchCtx`, request/run map, storage selection, journal attachment and host replay APIs |
| `agent/tinyagents/supplemental_trace.rs` (13-169) | Move call-id lifecycle and event emission to a generic harness decorator accepting an injected enrichment callback | Keep provider rate lookup, billed usage conversion, cost estimation, pricing-model selection and host usage projection |
| `agent/tinyagents/middleware/tool_outcome_capture.rs` (22-188) | Delete redundant wrapper and use existing harness library `ToolOutcomeCaptureMiddleware` directly | Keep policy-block enrichment, TinyTools failure classification, metadata filtering, and `ToolFailureMap` UI compatibility projection |
| `agent/tinyagents/middleware/repeated_failure.rs` (23-540) | Current source already delegates generic ladder mechanics to harness. Move only a source-audited generic driver with no OpenHuman-specific type dependency; do not add a duplicate ladder. | Keep `HostFailurePolicy`, classification/scope/assessment, sanitization, security and recovery copy, flow body failures, `StopLatch`, steering, additional recoverable-failure headroom, and fast halt for terminal inference failures. |
| `agent/tinyagents/budget.rs` (17-37) `ModelBudget` wrapper | Replace with existing `harness::cost::ModelBudget` (`new`, `wait_for_capacity`, `wrap_model`) | Keep host depth constants, `depth`, `install_depth`, and `TurnModels::with_budget` route wiring |

Harness already exposes `close`, `cost`, `no_progress`, `observability`, `turn_outcome`, and `turn_stop`. `close.rs` owns tool-result rendering, deterministic summaries, quote detection, repair, and grounded-close state machine. `turn_stop.rs` owns classification and typed stops. `turn_outcome.rs` owns tool outcomes/sinks. Observability owns `StoreEventJournal` over `AppendStore`. Cost owns `ModelBudget::new`, `wait_for_capacity`, and `wrap_model`. `middleware::library::ToolOutcomeCaptureMiddleware` is already available. Add only the copy and source/cost ports needed for issue #64; introduce no OpenHuman/RPC/TinyHumans/storage dependency.

## Copy seams: complete API contract

Expose host-injectable copy using owned, immutable values so the harness is product-neutral and the host can preserve every byte of existing wording. Add these public structs in `crates/tinyagents-harness/src/close.rs` and `turn_stop.rs`; derive `Debug, Clone, PartialEq, Eq` and keep fields public for construction by downstream hosts.

```rust
// close.rs
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseCopy {
    pub max_iteration_instruction: String,
    pub final_write_instruction: String,
    pub final_answer_instruction: String,
    pub truncated_answer_instruction: String,
    pub stop_note_preamble: String,
    pub no_tool_calls: String,
    pub omitted_results: String,
    pub deterministic_checkpoint_heading: String,
    pub deterministic_summary_heading: String,
    pub empty_checkpoint: String,
    pub empty_final_summary: String,
    pub repair_no_reply: String,
    pub repair_quoted_instruction: String,
    pub repair_unverified: String,
}

impl CloseCopy {
    pub fn english() -> Self { /* move the current harness English defaults verbatim */ }
    pub fn checkpoint_instruction(&self, rendered_results: &str) -> String;
    pub fn final_instruction(&self, stop_reason: Option<&str>, truncated: bool,
                             rendered_results: &str) -> String;
    pub fn repair_instruction(&self, instruction: &str, violation: CloseViolation) -> String;
}

// turn_stop.rs
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopCopy {
    pub stopped_because: String,
    pub what_happened_heading: String,
    pub success: String,
    pub failed: String,
    pub more_calls: String,
    pub proceed_prompt: String,
}

impl StopCopy {
    pub fn english() -> Self { /* neutral English defaults */ }
    pub fn render(&self, reason: &str, groups: &[StopSummaryGroup]) -> String;
}
```

The comment bodies above are signatures, not permission to leave `todo!()` or unimplemented methods. Implement each method in the same change. `StopSummaryGroup` is a new generic data record `{ name: String, success: bool, error: Option<String>, count: usize }`; it contains already-scrubbed display data, not raw tool arguments. The harness performs chronological grouping, last-eight selection, character truncation, and count formatting as it does today. `StopCopy` supplies only localized labels/sentences, with placeholder insertion performed by harness formatting. `CloseCopy` supplies the actual host instruction and headings, including the exact existing OpenHuman text. It must also include every literal that contributes to deterministic checkpoint/final summary output; do not merely translate the main headings and leave hard-coded English in the algorithm.

Keep existing public `*_with_options` APIs source-compatible. Add `*_with_copy` variants that accept `&CloseCopy` or `&StopCopy`; old entry points delegate to `CloseCopy::english()` / `StopCopy::english()`. The exact function names should match the module's established naming (`build_deterministic_checkpoint_with_copy`, `build_deterministic_final_summary_with_copy`, `grounded_close_with_copy`, `render_stop_summary_with_copy`). The host call path must select the `with_copy` functions explicitly. `CloseViolation` remains one canonical harness enum with variants `NoReply`, `QuotedInstruction`, and `Unverified`; remove OpenHuman's duplicate and map no strings or public core types into the harness.

## TDD implementation sequence

Do each step as a small red/green/refactor slice. Rust tests must be sibling `*_tests.rs` files declared with `#[cfg(test)]` and `#[path = "<module>_tests.rs"] mod tests;`, start with `use super::*;`, and contain no inline tests. For behavior that already exists upstream, first add/port a test that exercises the required public behavior and fails for the actual gap; do not claim that a missing API alone is a behavioral regression.

### 1. Establish copy compatibility before moving call sites

Files: `crates/tinyagents-harness/src/close.rs`, `close_tests.rs`, `turn_stop.rs`, `turn_stop_tests.rs`, and corresponding `mod.rs` exports if needed.

1. Port assertions from OpenHuman `agent/session_host/turn_checkpoint_tests.rs` for deterministic checkpoint, final summary, truncation and omission disclosure, repair instructions for all three violations, harness instruction quote detection, and exact final/checkpoint instruction content. Port `stop_summary_tests.rs` expected full strings, stop categories, grouping, scrubbing/truncation, last-eight groups, and hidden-call count. Assertions in the OpenHuman source are the golden source; retain exact expected strings.
2. Add `CloseCopy` and `StopCopy`, their English defaults, and copy-aware APIs. Existing APIs delegate to defaults. Use a deliberately distinct custom-copy test as well as the OpenHuman golden; a default-only test would not prove injection works.
3. Make the red test fail on a real mismatch, then implement. No host migration yet. Confirm old API tests still pass unchanged.

### 2. Consolidate checkpoint and grounded close

Files: `close.rs`, `close_tests.rs`; public paths remain `tinyagents_harness::close`.

1. Inventory existing `close.rs` entry points before editing: `CloseToolResult`, `results_from_tool_outcomes`, truncation/rendering, deterministic builders, `CloseViolation`, repair config/options and usage callbacks, required-output repair, grounded close and fallback. Extend those APIs rather than creating parallel algorithms.
2. Add focused tests for both checkpoint and final close with a custom `CloseCopy` and byte equality against the OpenHuman host golden. Cover successful grounded close, verification reject/unclear, no reply, quoted instruction, exactly one repair, deterministic fallback, deadline/cancellation, and provider usage callback on completed/failed calls.
3. Preserve `RepairUsage` and `SummarizerSourceModel` in OpenHuman because both depend on host model sourcing, provider usage/pricing, correlation, and trace. The harness API must continue to accept `ChatModel<()>`, `ToolDialect`, native messages, `RequiredOutput`, a caller-supplied verifier prompt/repair copy, budget and deterministic fallback closure. Keep provider callbacks in `CloseRequestOptions`; do not make the harness calculate host billing.
4. Preserve classified halt's no-provider-call path by expressing it through the generic deterministic summary path plus host-owned classification check. Do not move `"Stopping after "`, the product's classifier or that condition into the harness.

### 3. Adopt generic turn-stop output and typed terminal outcome

Files: `turn_stop.rs`, `turn_stop_tests.rs`, `turn_outcome.rs`, `turn_outcome_tests.rs`, `lib.rs` exports as needed.

1. Add tests for `StopCopy` full-string output using the ported OpenHuman goldens and for custom labels. Keep all existing `TurnStop` classification and serde tests, especially content-free typed causes and legacy fallback behavior.
2. Add `DeferredTurnTerminal` in `turn_outcome.rs` with the same ownership/lifecycle semantics as the host version: it owns the deferred terminal event and emits exactly once when committed; dropping an uncommitted value emits nothing. Test commit once, duplicate commit, and drop. Keep the event type in harness (`AgentEvent::TurnStopped`) and avoid host outcome structures.
3. Host-specific `StopReasonKind::from_stop_note` / stable product keys remain in OpenHuman. `TurnStop` construction can call the existing generic `classify`, `classify_with_cause`, and `from_incomplete_reason`; product fallback closure stays supplied by OpenHuman. Remove only forwarding wrappers once all uses call canonical API.

### 4. Move observability bridge behavior without moving host persistence

Files: `observability/mod.rs`, `observability/mod_journal_tests.rs` and a new sibling `observability/turn_journal_tests.rs` only if the current test module cannot host focused coverage; update `observability/mod.rs` declarations. Host files: `agent/tinyagents/journal.rs`, `supplemental_trace.rs`.

1. Add a harness-level `TurnJournal` facade over the existing `StoreEventJournal<A: AppendStore>`; it owns only generic run/thread lineage, append/read/flush composition. Its constructor takes the `Arc<dyn HarnessEventJournal>` and the `RunId`/`ThreadId` inputs that the current `TurnJournal` needs. Do not copy the backend `AppendStore` implementation or `DispatchCtx` correlation map.
2. Add tests using the existing in-memory append store/journal: event ordering and offsets, lineage, flush visibility, readback, and merge ordering. Port every assertion in OpenHuman `journal_tests.rs` that describes these generic semantics; keep request correlation and session stream integration tests on the host.
3. Move `SupplementalTrace` only if its remaining contract can be expressed using harness-owned `AgentEvent`, `GenerationEnrichment`, `GenerationPricing`, and ids. Preserve start/complete/failure terminal pairing, cached response metadata, zero-price routes, input correlation, and no usage charge on failed calls. If any field requires provider catalog or host billing types, leave that field/projection on OpenHuman and move the generic lifecycle only. Add unit tests in harness `observability` sibling test file; host integration verifies the adapter still emits the same durable records.
4. Keep all persistence through `AppendStore` and the existing journal ports. No database/storage dependencies may be added to `tinyagents-harness`.

### 5. Move capture middleware and repeated-failure ladder core

Files: `turn_outcome.rs`, `turn_outcome_tests.rs`, `no_progress/classified_ladder.rs`, `classified_ladder_tests.rs`, `no_progress/mod.rs` exports. Host files: middleware capture and repeated failure.

1. Capture tests pin exact tool call id/name/arguments/success/content/duration behavior, one append per execution, failure results, and poison-safe draining. The current harness `ToolCallOutcome`/sink is the canonical record; move generic middleware around that record into `turn_outcome`, preserving ordering and error semantics.
2. Add a generic driver callback/port to the existing `classified_step_with_cause` ladder rather than copying the classifier. The harness owns per-class attempt accounting, blocker clearing, nudge/halt decision and `ClassifiedStopCause`. OpenHuman continues to choose the failure class and scope, apply its extra recoverable-failure headroom, enforce terminal-inference fast halt, sanitize content, select product recovery copy, and perform host steering/security effects.
3. Port host tests for repeated same failure, distinct scopes, successful recovery clearing, uncertain side effects, typed cause and no-progress stop into harness tests where generic. Keep host policy tests that distinguish OpenHuman headroom/fast-halt behavior with the adapter.
4. Do not relocate `HostFailurePolicy`, inference `failure_copy`, `OpenHumanRunContext`, fetched-site metadata, or `StopLatch` into TinyAgents.

### 6. Replace the host model-budget wrapper

Files: existing `crates/tinyagents-harness/src/cost/model_budget.rs` and `model_budget_tests.rs`; host `agent/tinyagents/budget.rs` and budget-focused tests.

1. Verify existing `ModelBudget::new`, `wait_for_capacity`, and `wrap_model` contract and tests. Extend only for behavior used by OpenHuman; don't create a second wrapper. Port tests that verify shared ledger clones, child accounting, cache/retry placement expectations, normalization and reservation release.
2. Update OpenHuman to use `tinyagents_harness::cost::ModelBudget` directly or retain a type alias only if host call sites need the distinct host name. Keep depth constants and policy installation in `budget.rs`.
3. Ensure issue #60's runtime/job changes do not create a harness dependency on orchestration. No speculative session/runtime API is part of this step.

### 7. Adapt OpenHuman and remove the migrated duplicate rows

Files limited to the listed OpenHuman source/tests and their module exports. This is a later implementation phase after the upstream PR is available; do not make these edits in the current plan phase.

1. Change host close/checkpoint calls to the new `*_with_copy` APIs and inject a `CloseCopy` built from the exact existing constants and render behavior. Keep `SummarizerSourceModel`, `RepairUsage`, trace callback, provider limits, custom stop classifier, required-output host contract, and progress projection.
2. Change stop rendering to inject OpenHuman `StopCopy`; retain `StopReasonKind`, classifier mapping and security-sensitive scrubber in OpenHuman. Confirm every existing complete output string remains byte-identical.
3. Change journal, supplemental trace, tool outcome capture, repeated-failure ladder and budget imports to the upstream generic types. Remove host generic implementation files only after all call sites and tests compile against the new APIs.
4. Remove duplicate `CloseViolation`, `truncate_chars`, `render_tool_results`, `harness_instruction_needles`, `DeferredTurnTerminal`, `TurnJournal`, and model-budget wrapper only when no call sites remain. Preserve host integration tests for storage, request correlation, provider accounting, headroom and fast halt.
5. Host test names/path ownership must follow the OpenHuman sibling-test rule; keep behavior tests next to host adapters and remove only assertions now ported to harness. Do not delete a test merely because its old implementation file is deleted.

## Deferred dependencies and integration order

- TinyAgents #60 is the delivery/full-source change; it has no phase pin. Integrate its actual public source where needed and review/merge against the latest fork epic tip. Do not assume a phase-only API from #60.
- TinyAgents #63's session-only approved phase is `120410141b9e59896072a657985073b11cb53768`. Its later source work waits on #60; no API beyond that approved phase is assumed.
- TinyAgents #65 plan-v3 is `bc4910f49b98839d16e9dd91f192f02a589e3a8b`; it is not #63 and this work does not depend on its proposed APIs.
- OpenHuman #212 is the future host-adoption leaf. This #64 plan covers TinyAgents only; #212 adapts and deletes the host rows after #64 lands on the senamakel epic branch and the fork gitlink is advanced there.
- Preserve all unrelated #8088 tracker requirements. The issue #64 scope is the rows and behaviors in its body; it does not narrow the epic's other leaves.

## Acceptance and verification

Behavioral acceptance before adapting/removing host code:

1. Byte-identical golden comparisons for host-injected `CloseCopy` across deterministic checkpoint, deterministic final summary, grounded-close fallback, required-output repair, breaker halt, output-token exhaustion, iteration cap, and no completed tool calls.
2. Byte-identical host-injected `StopCopy` output for classified halt, repeated failures, successful repeats, scrubbed/truncated errors, grouped counts, hidden older groups, and each `StopReasonKind`.
3. All prior close, stop, journal, trace, tool-outcome, repeated-failure and budget assertions remain represented at the owning layer. Preserve public compatibility tests for old default-English APIs.
4. Source audit confirms the generic implementations are gone from the listed host directories, host policy and persistence seams remain, and no TinyAgents dependency references OpenHuman types.

Run required repository gates from the super-worktree root through its long-command cancellation wrapper, pointing Cargo at the TinyAgents manifest (no temp build directory and no `CARGO_TARGET_DIR`):

```sh
scripts/ci-cancel-aware.sh cargo fmt --manifest-path vendor/tinyagents/Cargo.toml --all -- --check
scripts/ci-cancel-aware.sh cargo check --manifest-path vendor/tinyagents/Cargo.toml --workspace --all-targets
scripts/ci-cancel-aware.sh cargo clippy --manifest-path vendor/tinyagents/Cargo.toml --workspace --all-targets -- -D warnings
scripts/ci-cancel-aware.sh cargo test --manifest-path vendor/tinyagents/Cargo.toml --workspace
```

Also run focused tests for changed harness modules during implementation. The OpenHuman adapter phase separately runs its required focused tests and layout checks. This plan phase runs no builds or tests and makes no gate claim.

## Resolved source audits

1. CloseCopy inventory is complete: exact host text/rendering is in `turn_checkpoint.rs:49-126,152-169,198-258,260-372,388-514`. Existing generic algorithms are in harness `close.rs`; the new seam injects copy while reusing those algorithms. No string inventory is deferred.
2. Do not add `TurnJournal` to harness. `journal.rs:123-225` couples terminal status writes to host `DriverStatusStore`; the generic event side already uses `StoreEventJournal`, `JournalSink`, and `HarnessEventJournal`. Keep attachment, status lifecycle, correlation, and replay host-side. Move only event terminal emission from `DeferredTurnTerminal` and leave durable status finalization with its host owner.
3. `SupplementalTrace` (13-169) uses harness `EventSink`, `AgentEvent`, ids, `ModelResponse`, `GenerationPricing`, and `GenerationEnrichment` for call lifecycle. It uses host `agent::cost` and provider conversion for catalog pricing, charge estimates, and `RepairUsage` fields. Move event lifecycle to harness behind a host enrichment callback; retain catalog/billing and final cost projection on the host callback.
4. Upstream `ModelBudget::new`, `wait_for_capacity`, and `wrap_model` are verified in `tinyagents-harness/src/cost/model_budget.rs:22-42`; retain host depth/policy setup and replace only the wrapper type.
