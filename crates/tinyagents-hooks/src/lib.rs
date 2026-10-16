//! Hook contracts and reusable hook engines for TinyAgents.
//!
//! Hooks are capabilities supplied by a host. This crate owns their shared
//! contracts and stock dispatch machinery; it does not choose host policy.

pub mod command;
pub mod tool;
