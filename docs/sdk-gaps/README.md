# TinyAgents SDK Gaps

> **Internal migration backlog.** This is a working document tracking an
> internal OpenHuman-to-TinyAgents migration effort, not a general public
> roadmap or API reference. See [`ROADMAP.md`](../../ROADMAP.md) for the
> project's public-facing roadmap.

This document lists TinyAgents SDK features that are missing or only partially
available from the perspective of migrating OpenHuman's Rust agent core onto
TinyAgents.

Scope: source baseline is the local TinyAgents checkout at `6f898fb`;
OpenHuman evidence is `src/openhuman/{tinyagents,agent,cost,tokenjuice}/*`.
This is not the OpenHuman migration plan (that is
`docs/tinyagents-migration-spec.md`); items here are upstream TinyAgents
implementation candidates, with tests last once API and storage surfaces settle.

## Executive Summary

TinyAgents has strong primitives for harness runs, middleware, event streams,
model profiles, usage/cost accounting, and sub-agent orchestration. The graph
runtime, graph checkpointers, and graph fanout APIs were removed in TinyAgents
#61. Remaining gaps include durable orchestration stores, richer streaming
events, and SDK-owned adapters for lifecycle controls OpenHuman implements.

OpenHuman can migrate more of `src/openhuman/agent/` if TinyAgents grows:

- First-class reasoning and tool-call argument streaming events (tool
  metadata, unknown-tool recovery, deferred approvals, queued steering, and
  tool context/rich returns have since shipped).
- Durable `TaskStore` and event/status stores with replay, lineage, cursors,
  redaction, and cancellation semantics.
- Storage compatibility options for SQLite users that already depend on a
  different `rusqlite` / `libsqlite3-sys` version.
- Higher-level map/reduce and parallel-agent orchestration helpers.
- Budget enforcement and provider/model catalog metadata that can drive
  preflight, fallback, and reconciliation.
- Conformance suites for providers, tools, middleware, sessions, and stores.

## Backlog By Topic

The full backlog (17 items) is split by topic into focused files:

- [`tools.md`](tools.md) — items 1, 2, 9, 14, 16: rich tool policy metadata,
  recoverable unknown tool calls, dynamic tool exposure and allowlist policy,
  deferred tool calls (A2), and tool execution context parity/rich returns
  (B1/B2).
- [`streaming.md`](streaming.md) — items 3, 6: reasoning and tool-argument
  streaming, production event and status journals.
- [`durability.md`](durability.md) — items 4, 5, 17: durable orchestration
  task store, SQLite storage compatibility, and storage/graph conformance.
- [`orchestration.md`](orchestration.md) — items 10, 11, 12, 13: graph fanout
  and parallel agent ergonomics, sub-agent steering/waiting/reuse, workspace
  isolation and sandbox hooks, and middleware control outcomes.
- [`cost-and-model-catalog.md`](cost-and-model-catalog.md) — items 7, 8, 15:
  cost/usage/budget enforcement, model catalog and provider resolution, and
  registry diagnostics and introspection.

## Implementation Order

1. Define API contracts for tool policy, unknown-tool handling, streaming delta
   channels, durable task storage, storage adapters, and control outcomes.
2. Implement the lowest-level data types and traits behind non-breaking
   defaults.
3. Add in-memory implementations first.
4. Add durable stores and compatibility adapters second.
5. Add middleware helpers and high-level graph helpers.
6. Migrate OpenHuman adapters to the new SDK surfaces.
7. Remove OpenHuman-specific compatibility shims once SDK behavior matches.
8. Implement conformance and regression tests last.
