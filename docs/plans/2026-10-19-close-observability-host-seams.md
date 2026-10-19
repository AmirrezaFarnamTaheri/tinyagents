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

The harness already accepts copy as template arguments in `deterministic_checkpoint`, `deterministic_final_summary`, `close_repair_instruction`, and `GroundedCloseConfig`. Bundle these existing seams rather than creating string fields unrelated to the current renderers. Add the following values in `close.rs` and `turn_stop.rs`:

```rust
// close.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseCopy {
    pub checkpoint_template: &'static str,
    pub final_summary_template: &'static str,
    pub repair_template: &'static str,
}

pub const ENGLISH_CLOSE_COPY: CloseCopy = CloseCopy {
    checkpoint_template: "I reached the tool-call limit for this turn ({steps} steps), so I paused here before I could write up what I found.\n\n**Raw results from this turn**\n{results}\n**Next steps:** I'll continue from here — just reply (e.g. \\\"continue\\\") and I'll pick up where I left off.",
    final_summary_template: "I finished this turn without writing up a result. Here is what each tool call returned:\n{results}\nTell me how you'd like to proceed.",
    repair_template: "{repair_reason} Write the message again.\n\n{instruction}",
};

pub fn checkpoint_with_copy(copy: &CloseCopy, steps: usize, rendered_results: &str) -> String {
    copy.checkpoint_template
        .replace("{steps}", &steps.to_string())
        .replace("{results}", rendered_results)
}

pub fn final_summary_with_copy(copy: &CloseCopy, rendered_results: &str) -> String {
    copy.final_summary_template.replace("{results}", rendered_results)
}

pub fn repair_with_copy(copy: &CloseCopy, instruction: &str, violation: CloseViolation) -> String {
    let reason = match violation {
        CloseViolation::NoReply => "Your previous reply was empty or tried to call a tool. No tools will run: write the message itself.",
        CloseViolation::QuotedInstruction => "Your previous reply repeated these directions, or the stop note, back to the user. That text is addressed to you alone and must never appear in the message. Do not reproduce any part of it, do not refer to it, and do not explain that you were given it.",
        CloseViolation::Unverified => "Your previous reply did not pass the check that it reports what actually happened, grounded in the records below.",
    };
    copy.repair_template.replace("{repair_reason}", reason).replace("{instruction}", instruction)
}

// turn_stop.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopCopy {
    pub opening_template: &'static str,
    pub what_happened_heading: &'static str,
    pub success_template: &'static str,
    pub failure_template: &'static str,
    pub hidden_calls_template: &'static str,
    pub proceed_prompt: &'static str,
}

pub const ENGLISH_STOP_COPY: StopCopy = StopCopy {
    opening_template: "I stopped this turn early because {reason}.\n",
    what_happened_heading: "\n**What happened**\n",
    success_template: "- `{name}` succeeded{count}",
    failure_template: "- `{name}` failed{count}{error}",
    hidden_calls_template: "- …and {count} more tool call(s)",
    proceed_prompt: "\nTell me how you'd like to proceed, or ask me to try again.",
};

pub struct StopSummaryGroup { pub name: String, pub success: bool, pub error: Option<String>, pub count: usize }

pub fn render_stop_summary_with_copy(reason: &str, groups: &[StopSummaryGroup], copy: &StopCopy) -> String {
    use std::fmt::Write as _;
    let mut out = copy.opening_template.replace("{reason}", reason);
    if !groups.is_empty() {
        out.push_str(copy.what_happened_heading);
        let shown_from = groups.len().saturating_sub(8);
        for group in groups.iter().skip(shown_from) {
            let name: String = group.name.chars().take(60).collect();
            let count = if group.count == 1 { String::new() } else { format!(" {} times", group.count) };
            let line = if group.success {
                copy.success_template.replace("{name}", &name).replace("{count}", &count)
            } else {
                copy.failure_template.replace("{name}", &name).replace("{count}", &count)
                    .replace("{error}", &group.error.as_ref().map(|e| format!(": {e}")).unwrap_or_default())
            };
            let _ = writeln!(out, "{line}");
        }
        let hidden: usize = groups.iter().take(shown_from).map(|group| group.count).sum();
        if hidden > 0 {
            let _ = writeln!(out, "{}", copy.hidden_calls_template.replace("{count}", &hidden.to_string()));
        }
    }
    out.push_str(copy.proceed_prompt);
    out
}
```

`StopSummaryGroup.error` contains only the already-scrubbed `short_error_line` output. Move the host grouping and scrub logic into harness and pass the resulting groups to the renderer above. Host `StopCopy` provides the reason sentence through `StopReasonKind.copy()`; it does not move the product's failure-note parser or security scrubber. Keep the existing total-budget result rendering APIs and let OpenHuman supply its existing `CloseResultRenderOptions` block/omission closures.

Keep existing APIs source-compatible. Existing `deterministic_checkpoint` and `deterministic_final_summary` remain; add the exact functions above. Existing `CloseViolation` has `NoReply`, `QuotedInstruction`, and `Unverified`; map OpenHuman's `QuotedHarnessText` variant to `QuotedInstruction` at its host boundary. The host passes the copy-aware close renderers explicitly.

## TDD implementation sequence

Do each step as a small red/green/refactor slice. Rust tests must be sibling `*_tests.rs` files declared with `#[cfg(test)]` and `#[path = "<module>_tests.rs"] mod tests;`, start with `use super::*;`, and contain no inline tests. For behavior that already exists upstream, first add/port a test that exercises the required public behavior and fails for the actual gap; do not claim that a missing API alone is a behavioral regression.

### 1. Establish copy compatibility before moving call sites

Files: `crates/tinyagents-harness/src/close.rs`, `close_tests.rs`, `turn_stop.rs`, `turn_stop_tests.rs`, and corresponding `mod.rs` exports if needed.

1. Port the exact behavior assertions from OpenHuman `turn_checkpoint_tests.rs`: `the_deterministic_checkpoint_reproduces_each_result_body`, `checkpoint_records_keep_the_captured_tool_failure_status_and_body`, `tool_result_renderer_preserves_the_openhuman_block_format`, `results_over_the_budget_are_omitted_with_a_disclosure`, `one_oversized_result_is_still_shown`, `no_completed_tools_says_so`, `newline_heavy_results_are_charged_what_they_render`, `the_final_summary_quotes_each_failure_message`, `the_final_summary_of_a_halted_turn_gives_a_reason_and_names_the_tools`, `the_final_answer_instruction_carries_the_records_and_the_stop_note`, `the_close_verification_prompt_holds_request_records_and_reply`, `the_final_answer_instruction_is_framed_and_forbids_quoting_tools_and_deliberation`, both quote guards, the repair re-ask, truncated/breaker copy, and both final-write checks. Keep full equality assertions unchanged, including `"\n- `read_file` — ok\n  > first line\n  > second line\n"`.
2. Port `stop_summary_tests.rs`: exact expected full output from `a_halted_turn_gives_one_reason_and_collapses_identical_failures`, redaction/truncation, secret suppression, successful-repeat semantics, every key/copy, all stop-note categories, policy-refusal copy, and newest-eight retention. The golden has one reason line, `**What happened**`, grouped lines, and the exact final prompt; retain its assertions verbatim.
3. Add the custom copy test with unmistakable sentinels in every copy field, then verify copy-aware checkpoint, final summary, repair, and stop renderers use them. This proves injection beyond the default English golden.
4. Make the red test fail on a real behavior mismatch, then implement. No host migration occurs in #64. Confirm existing APIs and tests still pass unchanged.

### 2. Consolidate checkpoint and grounded close

Files: `close.rs`, `close_tests.rs`; public paths remain `tinyagents_harness::close`.

1. Inventory existing `close.rs` entry points before editing: `CloseToolResult`, `results_from_tool_outcomes`, truncation/rendering, deterministic builders, `CloseViolation`, repair config/options and usage callbacks, required-output repair, grounded close and fallback. Extend those APIs rather than creating parallel algorithms.
2. Add focused tests for both checkpoint and final close with custom copy and byte equality against OpenHuman goldens. Preserve the cases in host `grounded_close_tests.rs`: classified halt with no provider usage; selected model and verifier excluded from final-context gauge; failed close request counted without usage; correlated native start/completion events; EOF and timeout traces without timeout usage; all required-output placement variants; truncated and capped turns; timeout repair retains completed-call usage; cache event clears provider charge; free-route catalog rates are zero. Assert output bytes, request sequence, model/temperature/output cap, tool-less calls, callback kind/response presence, token gauges, and event correlation.
3. Move `SummarizerSourceModel` to harness as `CloseSourceModel<'a> { source: &'a dyn CloseModelSource, model: &'a str, temperature: f64, thread_id: Option<&'a str> }`, implementing `ChatModel<()>` by obtaining the per-call model and forwarding request/invoke/stream unchanged. Define `pub trait CloseModelSource: Send + Sync { fn build_summarizer(&self, model: &str, temperature: f64, thread_id: Option<&str>) -> Result<Arc<dyn ChatModel<()>>, String>; }`. Map the `String` error to `tinyinference_llm::Error::Model`; OpenHuman implements the source port by delegating to `TurnModelSource`. Keep the supplemental trace wrapper separate in the following step.
4. Move `RepairUsage`'s generic fields and accumulation into harness `CloseUsage`: `model_calls`, `input_tokens`, `output_tokens`, `cached_input_tokens`, `last_call_input_tokens`, `last_call_output_tokens`, plus `cost: CostTally` and `pricing_model: String` (both types already belong to harness/inference contracts). Implement `for_model(model)`, `record(kind, Option<&Usage>, measures_context)`, and `record_close_call(kind, Option<&ModelResponse>)`. Preserve exact rules from source lines 45-98: count each attempted request; add usage totals only when present; record final context tokens only for non-verification requests; a successful close/repair with context tokens supersedes the loop's final call. Keep the callback seam so OpenHuman's `BilledUsage` conversion and catalog pricing call `CostTally::add(CallCost)` without importing OpenHuman types upstream. Existing `grounded_close_tests.rs` covers callback complete/fail/timeout paths.
5. Move `classified_halt_close` into harness as a generic selector with exact contract `pub fn classified_halt_close<T>(stop_reason: Option<&str>, is_classified_halt: impl FnOnce(&str) -> bool, build_close: impl FnOnce() -> T) -> Option<T>`. Its complete implementation is: `let reason = stop_reason?; is_classified_halt(reason).then(build_close)`. Host predicate is `|reason| reason.starts_with("Stopping after ")`; its fallback closure renders the host `StopCopy` summary from tool outcomes. This keeps the product marker and classification host-side while moving the generic short-circuit. Port `classified_halt_returns_partial_work_without_provider_usage`; assert no model request/usage, partial successes listed, success payload omitted, and failed line retained.
6. `repair_required_output` already exists in harness at `close.rs:559-698`; consolidate the host call to it rather than porting a duplicate. Preserve host construction of `RequiredOutput`, selected repair instruction, `Prepend` placement and post-repair progress delta. Port its exact behavior assertions from `required_output_repair_preserves_all_host_output_placements` and retain model-repaired vs deterministic fallback results.

### 3. Adopt generic turn-stop output and typed terminal outcome

Files: `turn_stop.rs`, `turn_stop_tests.rs`, `turn_outcome.rs`, `turn_outcome_tests.rs`, `lib.rs` exports as needed.

1. Add tests for custom stop copy using exact host golden output and retain every existing `turn_stop_tests.rs` assertion: typed content-free causes, legacy fallback, serde sanitization, stop precedence, incomplete-reason mapping, and status text.
2. Move `DeferredTurnTerminal`'s event-owned fields (`RunId`, `EventSink`) and methods to `turn_outcome.rs`; remove its host `TurnJournal` field. Preserve event order and exact error/outcome literals: success emits optional `TurnStopped` then `RunFinalized`; cancellation/failure emits `RunFailed` then `RunFinalized`. Host session sidecar owns both the harness terminal token and journal; it emits terminal events then calls the appropriate journal status method. Add tests asserting exact event variants/order, not just call counts. Keep `TinyagentsTurnOutcome` and unobserved usage tests on the host.
3. Host-specific `StopReasonKind::from_stop_note` / stable product keys remain in OpenHuman. `TurnStop` construction can call the existing generic `classify`, `classify_with_cause`, and `from_incomplete_reason`; product fallback closure stays supplied by OpenHuman. Remove only forwarding wrappers once all uses call canonical API.

### 4. Move observability bridge behavior without moving host persistence

Files: `observability/mod.rs`, `observability/mod_journal_tests.rs` and a new sibling `observability/turn_journal_tests.rs` only if the current test module cannot host focused coverage; update `observability/mod.rs` declarations. Host files: `agent/tinyagents/journal.rs`, `supplemental_trace.rs`.

1. Do not add/move `TurnJournal` into harness. The inspected type at host lines 123-225 includes `DriverStatusStore`, `HarnessRunStatus`, sealing, and host logging. The generic event path already uses `StoreEventJournal`, `JournalSink`, and `HarnessEventJournal`.
2. Keep attachment, status lifecycle and `merge_run_observations` on the host. `journal_tests.rs` only tests that the request/run map stays scoped to the registering `DispatchCtx`; keep it there. Existing harness observability tests cover generic append/read/offset/lineage behavior; add no duplicate tests.
3. Move `SupplementalTrace` call lifecycle into `harness::observability::SupplementalCallTrace`. Exact public port: `pub trait SupplementalEnricher: Send + Sync { fn enrich(&self, model: &str, response: &ModelResponse) -> (Option<String>, GenerationEnrichment); }`. The harness trace owns `EventSink`, `RunId`, `AtomicUsize`, `Mutex<Option<String>>` actual model, and `AtomicBool` cache flag; methods retain current signatures `new(events, run_id, enricher)`, `begin(model)->(CallId,u64)`, `failed(call_id,model,started_at_ms)`, `completed(call_id,model,started_at_ms,input,response)`, `actual_model()`, `is_cache_hit()`, and `run_id()`. The host enricher reproduces lines 101-148: actual-route model fallback, provider id validation, provider pricing, `usage_info_from_response`, and estimate suppression for cached/reported charges. Preserve event order: started; on failure only generic ModelFailed; on success cached marker (and clear charged amount), UsageRecorded, ModelCompleted with input/output, then GenerationEnriched when nonempty. Move `SupplementalCall` RAII and stream item completion/failure handling from `grounded_close.rs:124-225` too. Test cached response, zero-price route, failure and stream-drop paths; no external calls.
4. Keep all persistence through `AppendStore` and the existing journal ports. No database/storage dependencies may be added to `tinyagents-harness`.

### 5. Move capture middleware and repeated-failure ladder core

Files: `turn_outcome.rs`, `turn_outcome_tests.rs`, `no_progress/classified_ladder.rs`, `classified_ladder_tests.rs`, `no_progress/mod.rs` exports. Host files: middleware capture and repeated failure.

1. The host capture wrapper delegates to `tinyagents_harness::middleware::library::ToolOutcomeCaptureMiddleware` already. In #212 delete only this adapter after direct construction is wired. Keep host policy enrichment, TinyTools failure classification, `mcp_result` metadata filtering and `ToolFailureMap`. Preserve the host tests for same-name calls by call id, forwarding structured MCP presentation, and dropping undecorated MCP envelopes. Harness `turn_outcome_tests.rs` already tests poison-safe drain and one-time take.
2. The inspected 541-line `RepeatedToolFailureMiddleware` already delegates to harness `FailureScopeMiddleware`, `FailurePolicy`, `ClassifiedFailureTracker`, and `classified_step`; its remaining `HostFailurePolicy` owns host `RunContext`, call-resource scope, sanitization, flow-body failure detection, TinyTools classifier, recoverable headroom, terminal-inference fast halt, StopLatch and OpenHuman copy. Make no source move for this file in #64; preserving host tests is the acceptance evidence.
3. Existing harness `classified_ladder_tests.rs`, `recoverable_tests.rs`, and failure-scope tests own generic ladder behavior. Do not mirror them in OpenHuman or duplicate them in harness.
4. Do not relocate `HostFailurePolicy`, inference `failure_copy`, `OpenHumanRunContext`, fetched-site metadata, or `StopLatch` into TinyAgents.

### 6. Replace the host model-budget wrapper

Files: existing `crates/tinyagents-harness/src/cost/model_budget.rs` and `model_budget_tests.rs`; host `agent/tinyagents/budget.rs` and budget-focused tests.

1. Keep the existing `wrapped_model_settles_the_reported_charge_once_into_shared_ledger` test in `model_budget_tests.rs`; its assertion requires normalized charge `7` micros and ledger spend `{ tokens: 5, cost_micros: 7 }`. Add a behavior test only if changing upstream wrapper behavior; issue #64 requires no accounting algorithm rewrite.
2. Make the upstream wrapper public from the harness facade if it is not already exported, then replace the host `ModelBudget` struct with a type alias to `tinyagents_harness::cost::ModelBudget` if host call-site visibility allows it. Keep `TurnModels::with_budget` route wrapping and depth policy in OpenHuman.
3. Ensure issue #60's runtime/job changes do not create a harness dependency on orchestration. No speculative session/runtime API is part of this step.

### 7. Future host adoption in OpenHuman #212 (not a #64 implementation task)

After the TinyAgents #64 PR merges into the fork epic branch, OpenHuman #212 consumes that published fork API and moves the gitlink. This section gives #212 acceptance constraints only; #64 changes no OpenHuman source.

1. Implement `CloseModelSource` for `TurnModelSource`; inject OpenHuman `CloseCopy` templates and the host result-render options. Keep model selection, output caps, billing/pricing callback, progress deltas, required-output contract construction, and fallback placement in host adapters.
2. Implement the supplemental enricher with OpenHuman provider/catalog lookups. Keep `StopReasonKind`, failure-note parsing, identifier/secret scrub, failure classification, `HostFailurePolicy`, `StopLatch`, recoverable headroom, terminal-inference fast halt, flow-specific body failures, request correlation, status storage and replay in OpenHuman.
3. Replace the host budget wrapper with the upstream `ModelBudget` at `TurnModels::with_budget`; retain depth and policy setup.
4. Remove only generic duplicates actually consumed from TinyAgents; retain host policy/adapters. Ported goldens remain in the owning tests. Keep `journal_tests.rs`, UI metadata tests, policy and budget tests because their behavior is still host-owned.

## Deferred dependencies and integration order

- TinyAgents #60 is the delivery/full-source change; it has no phase pin. Integrate its actual public source where needed and review/merge against the latest fork epic tip. Do not assume a phase-only API from #60.
- TinyAgents #63's session-only approved phase is `120410141b9e59896072a657985073b11cb53768`. Its later source work waits on #60; no API beyond that approved phase is assumed.
- TinyAgents #65 plan-v3 is `bc4910f49b98839d16e9dd91f192f02a589e3a8b`; it is not #63 and this work does not depend on its proposed APIs.
- OpenHuman #212 is the future host-adoption leaf. This #64 plan covers TinyAgents only; #212 adapts and deletes the host rows after #64 lands on the senamakel epic branch and the fork gitlink is advanced there.
- Preserve all unrelated #8088 tracker requirements. The issue #64 scope is the rows and behaviors in its body; it does not narrow the epic's other leaves.

## Acceptance and verification

Issue #64 acceptance (TinyAgents repository):

1. Ported host golden outputs are byte-identical when host `CloseCopy`/`StopCopy` is injected: deterministic checkpoint, final summary, breaker halt, tool/no-tool output, no-reply/quoted/unverified close repairs, output-budget exhaustion and iteration cap.
2. Preserve all generic close state-machine assertions in `close_tests.rs` (tool-less requests, verifier behavior, one repair, deterministic fallback, timeout/cancellation, usage callbacks, request properties, required-output placement) and stop serialization/classification assertions in `turn_stop_tests.rs`.
3. Preserve supplemental trace event order, cache charge clearing, usage/enrichment, run correlation and cancellation/failure terminal event behavior. Retain the existing cost budget test unchanged. No external service calls in tests.
4. Source audit confirms TinyAgents contains no OpenHuman type/dependency, new ports use only harness and inference contracts, no duplicated existing journal/close/cost algorithm was added, and all requested generic source rows have an upstream implementation or documented existing implementation.

OpenHuman row removal, adapter compile, storage integration and all retained host policy tests are acceptance for future OpenHuman #212, not this PR.

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
