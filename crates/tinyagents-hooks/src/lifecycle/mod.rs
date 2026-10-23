//! Lifecycle hooks a host fills in with policy: the memory pack delivered to
//! every model request, the router for bare names of withheld packed tools, and
//! the token-accounting parity observer.
//!
//! Each is a generic middleware over a small host trait, so the host keeps only
//! the trait implementation (where the pack lives, which pack owns a tool,
//! whether the tool may run).

mod budget_parity;
mod memory_pack;
mod packed_tool_route;
mod types;

pub use budget_parity::BudgetParityObserver;
pub use memory_pack::MemoryPackMiddleware;
pub use packed_tool_route::PackedToolRouteMiddleware;
pub use types::{MemoryPackSource, PackedToolRouter};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
