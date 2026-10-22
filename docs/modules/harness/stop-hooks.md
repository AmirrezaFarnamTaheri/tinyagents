# After-model stop hooks

`StopHookMiddleware<Metrics>` owns reusable policy execution: completed-call
tallies, ordered observation, first-reason selection and once-only pause
delivery. Hosts supply budget and goal policies and `StopHookMetrics` accounting;
no product price catalog, goal store or credential dependency enters the harness.

Each context supplies its steering handle. A stopping policy prevents the next
model call without cancelling the completed response's pending tool round.
Successful and failed terminal hooks retire per-context accounting.

See the [module contract and lifetime documentation](../../../crates/tinyagents-harness/src/middleware/library/stop_hooks/README.md)
and [focused tests](../../../crates/tinyagents-harness/src/middleware/library/stop_hooks_tests.rs).
