# After-model stop hooks

`StopHookMiddleware<Metrics>` owns reusable policy execution: completed-call
tallies, ordered observation, first-reason selection and once-only pause
delivery. Hosts supply budget and goal policies and `StopHookMetrics` accounting;
no product price catalog, goal store or credential dependency enters the harness.

Each context supplies its steering handle. A stopping policy prevents the next
model call without cancelling the completed response's pending tool round.
Successful and failed terminal hooks retire per-context accounting.

Halt summaries can use the shared `tinyagents_harness::turn_stop::CopyProvider`
contract. A host may override selected typed `CopyRequest` variants and let the
library provide neutral English for the rest. `tinyagents_hooks::stop::halt_summary`
returns a `HaltSummary` with a stable machine class alongside the rendered
copy; callers must keep those values separate. The terminal inference
classifier only considers TinyInference billing/configuration failures after a
known delegation envelope, with billing taking precedence. Halt detail is
bounded to 600 Unicode characters before a host copy provider sees it.

`TurnStop::user_copy` maps its typed class to one of the stable `turn_stop.*`
keys. It does not parse stop prose, and wind-down and iteration-cap stops use
the generic key. The user-facing copy remains separate from serialization and
content-free status messages.

See the [module contract and lifetime documentation](../../../crates/tinyagents-harness/src/middleware/library/stop_hooks/README.md)
and [focused tests](../../../crates/tinyagents-harness/src/middleware/library/stop_hooks_tests.rs).
