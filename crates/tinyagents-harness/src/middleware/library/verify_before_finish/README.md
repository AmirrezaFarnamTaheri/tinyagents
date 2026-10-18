# Verify before finish

`VerifyBeforeFinishMiddleware` can hold an eligible draft final answer for one
model check. It appends the check as a user turn, preserving the conversation's
prefix. The follow-up answer may call tools; the check fires at most once per
run. Hosts opt in and choose a tool-round threshold or a custom trigger.

`mod.rs` exposes the small public surface. `types.rs` defines activity, trigger,
and per-run state. `middleware.rs` contains configuration and lifecycle hooks;
`middleware_tests.rs` exercises those hooks and the run loop;
`middleware_execution_tests.rs` covers actual execution evidence.

## Requested and executed activity

`FinishActivity.tool_rounds` and `tools_called` describe model requests.
`called(name)` retains that request-based meaning. They can include a refused
call, so they do not prove that a tool ran.

`tools_executed` / `executed(name)` describe concrete executors that started.
The middleware receives this information through the synchronous
`Middleware::observe_tool_execution` hook inside the terminal tool future,
after gates and wrap middleware. A packed `use_skill` invocation contributes
the bound inner name; a disclosure or missing target contributes only
`use_skill`. A wrapper that never calls `next` contributes nothing. Retried
attempts are observed individually but these activity sets contain unique names.
Invalid named requests retain conservative dispatcher permissions; they never
identify an inner mutator as executed.

`tools_with_side_effects` / `executed_with_side_effects(name)` additionally
require the actual target to declare an argument-aware permission above
`ReadOnly` or an external effect. For a tool that supports previews, literal
`dry_run: true` can therefore count as execution without counting as a write.
The host can opt into a consequential-action check without charging previews:

```rust
use tinyagents_harness::middleware::VerifyBeforeFinishMiddleware;

let middleware = VerifyBeforeFinishMiddleware::new("Check the final files")
    .with_trigger(|activity| {
        activity.tool_rounds >= 5 || activity.executed_with_side_effects("ast_patch")
    });
```

These sets record attempted execution, including attempts that later fail,
time out, or are cancelled. They do not assert that a mutation succeeded.
The model's arguments and tool-result metadata cannot declare execution names.
Outer transcript names and call ids remain unchanged.

Existing `FinishActivity` struct literals should add `..Default::default()`
unless they intentionally specify the execution sets. Both fields default to
empty when reading older serialized state; requested calls are never promoted
to execution evidence during migration.

## Budgets and deferred runs

The check is skipped for tool-bearing, empty, truncated, or already continued
responses and when call or wall-clock budget is too small. Successful and
failed runs release activity through lifecycle hooks. Deferred runs put their
tool activity and check status in `DeferredToolRequests::resume_metadata`.
Hosts constructing `DeferredToolResults` manually must copy it with
`with_resume_metadata(&requests)`; `approve_all()` copies it automatically.
On resume the middleware restores that state even if compaction removed the
tool calls or the check message. Legacy results without metadata recover only
requested activity visible in the transcript, not execution or side effects.
Interrupted runs are pruned once their
contexts have been dropped; active runs retain state beyond 1,024 entries.
