//! Hook contracts and reusable hook engines for TinyAgents.
//!
//! Hooks are capabilities supplied by a host. This crate owns their shared
//! contracts and stock dispatch machinery; it does not choose host policy.

pub mod command;
pub mod lifecycle;
pub mod scope;
pub mod session;
pub mod stop;
pub mod tool;
pub mod turn;

pub use session::{FnSessionHooks, SessionHookFuture, SessionHookTypes};
