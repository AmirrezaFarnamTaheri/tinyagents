# turn_driver

A generic turn loop for a [`Session`](../session.rs). Nothing here knows a host's
types: the host supplies a `TurnPlanner` and per-turn `TurnControls`.

| Piece | Role |
| --- | --- |
| `TurnDriver` | `SessionDriver` that plans, assembles one `AgentHarness`, runs it and fills a `TurnReport` |
| `TurnPlanner` / `TurnPlan` | per-turn recipe: models, run policy, tools, context middleware, tool policy, listeners, completion sink |
| `ToolSetBuilder` | registers an admitted subset of shared tool sets (fail-closed allowlist, spawn-tool refusal for children, thread-scoped tools, session-deferred tools, typed dispatch, execution scope) |
| `TurnControls` | model-call cap and pause-at-cap, early-exit tools, deferred completion, scope, steering feed, registrar |
| `RunQueueFeed` | `SteeringFeed` over a harness `RunQueue`: pre-run drain, poll loop, requeue of unapplied steers on drop |

## Order of effects

1. Planner runs; `max_iterations` overrides the policy cap (`0` is unset).
2. Models, `context` middleware (outermost first), then the tool-policy gate.
3. Tools register through `CanonicalSharedToolAdapter`; early-exit tools carry the hook,
   so it records the raw result before any `after_tool` middleware rewrites it.
4. The declared tools must equal the session's frozen `ToolSnapshot`.
5. `pause_at_cap` subscribes the harness `CapPauser`; a child's handle goes to the registrar.
6. The run executes; the report is published to `TurnControls::report`, even on failure.
7. A root turn fires the completion sink unless deferred; a child never does.

Tests: `driver_tests.rs` scripts a model through each control; `tool_set_tests.rs` covers admission.
