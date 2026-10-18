# Hooks

`tinyagents-hooks` owns reusable policy and observation hooks shared by TinyAgents
hosts. It depends on the harness contracts, and the runtime depends on hooks to
install the capabilities without making the harness depend on product policy.

## Tool hooks

`tool::ToolHook<Ctx>` receives a typed `ToolHookContext` before and after each
execution. A before-hook returns the shared command/tool `HookOutput` decision,
which can allow, deny, request approval, replace arguments, and add context in
the same result. `ToolHooksMiddleware` applies argument changes in registration
order; an unresolved approval request is refused because the middleware has no
interactive approval channel. After-hook context is appended to the result for
the model. `HookIdentity` lets a host provide session, agent, and working-path
identity. `sanitize_tool_output` produces a content-free summary for learning
records.

The command engine is available at `tinyagents_hooks::command`. The runtime
re-exports it from `tinyagents_runtime::command_hooks` for one release.

## Stop and lifecycle hooks

Stop policy traits and middleware live under `tinyagents_hooks::stop`.
The middleware checks policies after each completed model call, records the
first stop reason, and pauses through the run's steering handle. The stock
`BudgetStopHook` caps known USD spend. `GoalBudgetStopHook` is available with
the `goals` feature and checks an active stored goal's token budget.
`StopHookMiddleware::with_halt_summary` records the first stop reason in a
shared summary slot.

The host-oriented middleware contracts for memory packs, packed-tool routing,
and budget-parity observation live under `tinyagents_hooks::lifecycle`.

## Hook scope

`HookScope` is explicit per-turn data. A host passes the scope to the session
it builds; it is not a process-global registry. Child runs can inherit the same
registered callback lists while each scope remains independently constructible.
