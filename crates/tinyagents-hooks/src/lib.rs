//! Hook contracts and reusable hook engines for TinyAgents.
//!
//! Hooks are capabilities supplied by a host. This crate owns their shared
//! contracts and stock dispatch machinery; it does not choose host policy.

pub mod command;
pub mod lifecycle;
pub mod stop;
pub mod scope;
pub mod tool;
