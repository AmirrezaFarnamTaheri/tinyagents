//! Optional structural tools behind the host's filesystem gate.
//!
//! [`code_tools`] builds six deferred tools sharing a bounded parser cache and
//! write lock. Only the literal JSON boolean `dry_run: true` previews edits;
//! real writes require exact content revisions and publish atomically. See the
//! module README for the argument and receipt contracts.

mod arguments;
mod chunks;
mod declaration;
mod inspect;
mod io;
mod mutate;
mod publish;
mod response;
mod types;

use std::sync::Arc;

use parking_lot::Mutex;
use tinytools::Tool;
use tinytools_code::{ChunkCatalog, CodeEngine};

use crate::fs::filesystem::FsGate;

pub use types::CodeToolOptions;
use types::{CodeTool, Kind, Shared};

/// Build the six deferred structural tools with their default resource bounds.
///
/// # Errors
///
/// Returns an error if the bundled catalog or parser configuration is invalid.
pub fn code_tools(gate: Arc<dyn FsGate>) -> anyhow::Result<Vec<Box<dyn Tool>>> {
    code_tools_with_options(gate, CodeToolOptions::default())
}

/// Build the structural tools with host-selected parser and I/O budgets.
///
/// All returned tools share the same parser cache and mutation lock. They do
/// not install dependencies, execute generated code, or grant filesystem access.
///
/// # Errors
///
/// Returns an error for invalid resource bounds or an invalid bundled catalog.
pub fn code_tools_with_options(
    gate: Arc<dyn FsGate>,
    options: CodeToolOptions,
) -> anyhow::Result<Vec<Box<dyn Tool>>> {
    options.validate()?;
    let shared = Arc::new(Shared {
        engine: Mutex::new(CodeEngine::new(options.limits)?),
        catalog: ChunkCatalog::builtin()?,
        write_lock: tokio::sync::Mutex::new(()),
        options,
    });
    Ok(Kind::ALL
        .into_iter()
        .map(move |kind| {
            Box::new(CodeTool {
                kind,
                gate: Arc::clone(&gate),
                shared: Arc::clone(&shared),
            }) as Box<dyn Tool>
        })
        .collect())
}

#[cfg(test)]
#[path = "chunk_tests.rs"]
mod chunk_tests;
#[cfg(test)]
#[path = "contract_tests.rs"]
mod contract_tests;
#[cfg(test)]
#[path = "inspection_tests.rs"]
mod inspection_tests;
#[cfg(test)]
#[path = "mutation_tests.rs"]
mod mutation_tests;
#[cfg(test)]
#[path = "publication_tests.rs"]
mod publication_tests;
#[cfg(test)]
mod test_support;
