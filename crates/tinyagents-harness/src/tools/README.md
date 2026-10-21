# Harness tool implementations

Default tool implementations live in `tinyagents-harness-tools`. This crate
keeps the generic harness infrastructure they use: tool declaration and
selection, dispatch, middleware hooks, deferral, timeout handling, and
per-call binding.
