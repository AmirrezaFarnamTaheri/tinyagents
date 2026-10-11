//! Definition provenance supplied by loaders or host catalogues.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Origin of a definition. Provenance is runtime metadata and is omitted from
/// serialized [`crate::AgentDefinition`] values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "kind", content = "path")]
pub enum DefinitionSource {
    /// Definition bundled by its host.
    #[default]
    Builtin,
    /// Definition loaded from the supplied TOML path.
    File(PathBuf),
    /// Definition projected from a host-managed configuration registry.
    CustomRegistry,
}
