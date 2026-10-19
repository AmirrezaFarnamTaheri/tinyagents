//! The **run-state block**: a short, current view of the run's plan and of the
//! facts its tool calls established, added to every model request.
//!
//! Between `todo` calls a model sees its plan only in an old tool result,
//! further back with every round, and what its failed calls taught it is
//! scattered across raw output. [`StateBlockMiddleware`] rebuilds one block
//! before each model call from two sources that cannot be wrong:
//!
//! - **the plan**: the thread's todo list from the store the `todo` tool
//!   writes ([`crate::todos`]), with the step in progress and how many tool
//!   rounds it has run;
//! - **the ledger**: facts derived from the run's own tool calls with no model
//!   involved ([`ledger`]): the last few failed calls with their exit code and
//!   first stderr line, executables a shell reported missing, and files
//!   written by write/edit tools. Facts correct themselves: a later success of
//!   the same command clears its failure, and a later successful command that
//!   runs a missing executable clears it.
//!
//! ```text
//! [run state] Rebuilt by the harness for this call from your plan and your tool results; …
//! Plan: step 2 of 3 in progress (4 tool rounds on it).
//!   [x] 1. Dump the schema
//!   [>] 2. Write the migration
//!   [ ] 3. Run the tests
//! Recent failures (oldest first):
//!   - 2 rounds ago, shell `python3 migrate.py`: exit code 127; stderr: python3: not found
//! Reported not found: python3
//! Files written this run: /app/migrate.py
//! ```
//!
//! **Request-only.** The block goes in through
//! [`push_ephemeral_instruction`](tinyagents_harness::middleware::push_ephemeral_instruction)
//! at `before_model_dispatch` (re-applied after request rewrites), so it is
//! never written to the transcript: the provider's cached prefix is untouched,
//! the history holds no stale copies, and the model sees exactly one block,
//! always the current one, always at the tail. Nothing is added while there is
//! neither a plan nor a fact.

mod ledger;
mod middleware;
mod render;
mod types;

pub use render::STATE_BLOCK_HEADER;
pub use types::StateBlockMiddleware;
