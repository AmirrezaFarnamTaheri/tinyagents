# Harness Streaming Feature

The harness supports caller-consumable run events and provider model streams.

## Responsibilities

- Stream model token/message deltas.
- Stream tool progress.
- Stream usage and cost updates.
- Stream cache hit/miss events.
- Stream summary events.
- Stream final outputs.
- Forward events into the registry event bus.
- Merge provider chunks into a final assistant message.
- Preserve streamed tool-call chunks where providers support them.
- Support cancellation and backpressure.
- Support stream replay from event stores.

## Source Inspiration

LangChain exposes model streams, runnable event streams, callback streaming, and
tracer streams:

- chat model stream:
  <https://github.com/langchain-ai/langchain/blob/master/libs/core/langchain_core/language_models/chat_model_stream.py>
- runnable event streams:
  <https://github.com/langchain-ai/langchain/tree/master/libs/core/langchain_core/runnables>
- streaming tracers:
  <https://github.com/langchain-ai/langchain/tree/master/libs/core/langchain_core/tracers>
- provider streaming adapters such as OpenAI:
  <https://github.com/langchain-ai/langchain/blob/master/libs/partners/openai/langchain_openai/chat_models/base.py>

## Stream Modes

- `messages`: model deltas and final messages
- `tools`: tool lifecycle and progress
- `usage`: token updates
- `cost`: price updates
- `events`: all harness events
- `final`: final result only

Every stream item should carry run ids and component ids so web UIs can identify
the run that emitted it.

## Stream Items

```rust
pub enum HarnessStreamItem {
    Event(HarnessEvent),
    MessageDelta(MessageDelta),
    ToolCallDelta(ToolCallDelta),
    ToolProgress(ToolProgress),
    Usage(UsageRecord),
    Cost(CostRecord),
    Final(AgentRun),
}
```

A stream consumer should be able to subscribe to a subset of modes without
changing execution. Dropping a consumer must not cancel the run unless the
consumer owns the run cancellation token.

## Chunk Merging

Streaming adapters must merge chunks deterministically:

- text chunks preserve order
- reasoning/thinking chunks preserve order on a side channel
- content block indexes are respected
- tool-call chunks are correlated by id or index
- cumulative usage is converted into deltas or clearly marked cumulative
- final message equals the merged stream
- invalid partial tool calls are surfaced as repairable parse errors

## Implementation status (runtime-comparison Phase 3, C1/C2)

The design above names the target shape; here is what exists in code today,
one layer down at `tinyinference_llm::model::ModelStreamItem` and the
`agent_loop::stream` caller API.

**Block-indexed streaming (C1, vendor `tinyinference-llm`).** Content block
indexes are respected via new `ModelStreamItem` variants: `BlockStart {
index, kind: BlockKind::{Text, Thinking, ToolCall { id, name }} }`,
`BlockDelta { index, delta: BlockDelta::{Text, Thinking, ToolArgs} }`, and
`BlockEnd { index, block: ContentBlock }`. The pre-existing flat
`MessageDelta`/`ToolCallDelta` channel is still emitted alongside them
(`model::block_delta_to_message_delta` is the shared derivation), so nothing
that only understood the old shape breaks. `ToolDelta::content_index` carries
the wire block index on the flat channel too. The Anthropic adapter maps
`content_block_start`/`_delta`/`_stop` 1:1 onto the new items. The OpenAI
chat-completions adapter derives the same items from its delta shape: it
tracks which block (text, reasoning, or a given tool call's wire index) is
currently open, emits `BlockStart` the first time a new one is seen (a tool
call opens as soon as its id or name arrives, even before its first argument
fragment), `BlockDelta` per fragment, and `BlockEnd` when the open block
switches or `finish_reason` arrives — all sharing one dense index space, so
`ToolDelta::content_index` lines up with the terminal message's `content`
ordering the same way it does for Anthropic. The OpenAI Responses API has no
true incremental SSE path in this crate yet (`stream()` simulates one with a
single unary call replayed as `Started`/one `MessageDelta`/`Completed`), so
there are no block boundaries to derive there (tracked in
`docs/sdk-gaps/streaming.md` §3). `ModelStreamItem::{Failed, ProviderFailed}` —
specifically `ProviderError` — now carries `partial_message:
Option<AssistantMessage>` and `stop_reason: Option<String>` on both
adapters, so a mid-stream failure does not discard whatever content had
already arrived.
