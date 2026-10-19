# `state_block`: the run-state block

`StateBlockMiddleware` adds one short block to every model request: the
thread's plan (from the `todo` store) and facts the harness derived from the
run's own tool calls. It never writes to the transcript.

## Why

Between `todo` calls the model sees its plan only in an old tool result that
moves further back every round, and what failed calls taught it is spread over
raw output. On Terminal-Bench captures of DeepSeek V4 Flash (207 attempts), a
median of 14 model calls passed between plan updates, and half the attempts
that wrote a plan ended with steps still open. The block puts the current plan
and the hard facts at the tail of every request.

## What it shows

| Section | Source | Rule |
|---|---|---|
| Plan | `todos::store` for the run's thread | Every step with its mark (`[x]` `[>]` `[ ]` `[-]`), the step in progress and the tool rounds it has run. At most 20 steps. |
| Recent failures | `after_tool` results with `is_error` | The last 3, each with its age, tool, command (or argument summary) and the exit code plus first `[stderr]` line. Dropped after 12 rounds, or when the same command later succeeds. |
| Reported not found | Shell wording in error output (`X: command not found`, `sh: 1: X: not found`, `command not found: X`) | Cleared when a later successful command runs `X`. |
| Files written this run | Path argument of tools whose name contains write/edit/patch/create/replace | The last 6, distinct. |

No section is ever a model's interpretation, so nothing in the block can be
wrong in a way the tool results are not. With neither a plan nor a fact, no
block is added.

## Placement and caching

The block is added in `before_model_dispatch` through
`push_ephemeral_instruction`, and re-added in `after_model_rewrite` if a
request rewrite removed it. It therefore follows the model profile's
placement rules (a tail system message, or a `[harness note]` on the last tool
result or user turn for providers that hoist system messages), stays out of the
admission estimate, and is never persisted: the next request is rebuilt from
the transcript, so the previous block is simply absent and the provider's
cached prefix is untouched.

## Wiring

```rust
harness.push_middleware(Arc::new(StateBlockMiddleware::new(todo_store)));
```

Pass the same store the `todo` tool writes. Install it on the turns that have
the `todo` tool; it is an observer, so another middleware's control outcome
never hides a call from its ledger. Per-run state is keyed by the run
instance and dropped in `after_agent` / `on_error`.
