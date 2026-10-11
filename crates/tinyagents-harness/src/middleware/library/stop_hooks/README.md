# Stop hooks

The harness evaluates `StopHook<Metrics>` policies after completed model calls.
Hosts retain budget and goal rules, pricing and result presentation.
`StopHookMetrics` supplies completed-response accounting; the middleware never
invents prices or assumes unreported spend is free.

## Public API

The types are exported from `tinyagents_harness::middleware::library`:

- `StopHook<Metrics>`: a named asynchronous policy or accounting observer.
- `TurnState<Metrics>`: completed-call count, model-call cap, model label and a
  borrowed snapshot of the host's cumulative metrics.
- `StopDecision`: continue, or stop with a policy-supplied reason.
- `StopHookMetrics`: default, cloneable per-run accounting with `record_model`.
  `()` supports policies that require no cost or usage tally.
- `StopHookMiddleware<Metrics>`: ordering, accounting isolation and pause delivery.
- `StopReport`: the first stopping policy, delivered through the optional
  `with_stop_observer` callback for host events or final-result copy.

Implement the metrics trait on the host accounting type, then implement
`StopHook<ThatType>` for budget and goal policies. Pass the hooks to
`StopHookMiddleware::new(model_label, hooks)` and install it on the harness.
Attach a run-specific steering handle permitting `Pause` to each run context
when the hook list is nonempty. Missing steering fails explicitly before
policies observe a call.

## Execution and lifetime

The completed response is recorded before evaluation. Every hook sees the same
snapshot in registration order, including hooks after a stopping policy.
The first stopping reason wins. The host callback and pause are delivered once;
subsequent responses do not re-evaluate the stopped chain.

The pause is processed at the next loop checkpoint. Tools already requested
by the completed response keep their normal execution path. A stop prevents
another model call rather than cancelling an admitted tool round.

State is keyed by `RunContext::instance_id`, so equal durable run ids cannot
share counters, metrics or pause latches. Success and error lifecycle hooks
retire state. A later context admission reclaims accounting left by a dropped
context that bypassed terminal hooks. Callbacks and asynchronous checks do not hold the accounting mutex.

## Verification

`stop_hooks_tests.rs` covers ordered observation, first-reason selection,
once-only delivery, context isolation, success/error cleanup, empty hook lists
and missing steering, live cap narrowing and abandoned-context cleanup. A native-loop test confirms a stopping hook prevents a
second provider call while retaining the completed call's pending tool round.
