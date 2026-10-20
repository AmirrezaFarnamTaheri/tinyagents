# Plan review

`RequestPlanReviewTool` formats the model-facing review request and preserves
the established result text. `PlanReviewHost` supplies the interactive
surface, thread identity, run mode, and tool-call id, then routes an interactive
request into the host's scope-aware `PlanReviewStore`. A missing surface uses
the existing non-interactive auto-approval response; calls outside plan mode
return the not-applicable response.

`PlanReviewStore<S>` treats `S` as an opaque scope key. It keeps a snapshot and
waiter for each pending request, indexes the newest request per scoped thread,
and rejects after its ten-minute default TTL. A timed out request is reported
as expired; a cancelled waiter or withdrawn sender is rejected and reported as
cancelled. `PlanReviewNotifier` receives requested and terminal decisions so
the host can publish its product events without putting product policy in the
generic store.

The store is in-memory because a parked turn cannot resume after process exit.
Hosts own identity resolution, persistence, and event payload mapping. See
[`plan_exit`](../plan_exit.rs) and [`run_mode`](../run_mode.rs) for the paired
mode transition contracts.
