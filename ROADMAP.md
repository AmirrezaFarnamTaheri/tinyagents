# Roadmap

TinyAgents is at v2.1.2. The roadmap favors small, well-tested modules that
build toward a production-grade Rust agent runtime.

## Shipped Foundation

- typed harness model calls, tools, middleware, structured output, streaming,
  usage/cost tracking, retry/limits, cache, memory/embeddings, sub-agents,
  and steering (`harness/`)
- per-thread `ThreadGoal` and `TodoList` productivity primitives, exposed as
  harness tools
- named capability registry (models, tools, agents, stores,
  middleware, policy) bound by name (`registry/`)
- an embedded Langfuse client for observability

## Near-Term Work

- continue splitting any module or doc that grows past the 500-line limit
  into focused files
- expand live (network-gated) provider contract tests as new
  OpenAI-compatible endpoints are added
- track and close the internal SDK feature-parity backlog in
  [`docs/sdk-gaps/README.md`](docs/sdk-gaps/README.md)
- execute the phased plan in
  [`docs/runtime-comparison/plan.md`](docs/runtime-comparison/plan.md), which
  ranks the correctness fixes and feature gaps found by comparing TinyAgents
  with LangGraph, Pydantic AI and pi

## Sub-Agents

Shipped: child contexts, shared caches with explicit isolation policy,
child event namespaces, parent/child run ids, policy-controlled detached
work, and durable child task lifecycle through `tinyagents-orchestration`.

## Stability

The public API is versioned via semver starting at 1.0. Breaking changes are
documented in release notes, tested, and shaped by real examples rather than
speculative abstraction.
