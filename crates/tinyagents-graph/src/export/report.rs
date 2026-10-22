//! Rendered topology reports for debug / inspection UIs.

use super::{GraphTopology, to_json, to_mermaid};

/// A rendered topology for one named graph: Mermaid and JSON renderings plus
/// the structural validation outcome. Behaviour-free; never closure bodies.
#[derive(Debug, Clone)]
pub struct GraphTopologyReport {
    /// Stable graph label (e.g. `"agent_teams:member"`).
    pub name: &'static str,
    /// Mermaid `flowchart TD` rendering.
    pub mermaid: String,
    /// Pretty-printed JSON of the full topology.
    pub json: String,
    /// `true` when the structural validation found no errors.
    pub ok: bool,
    /// Structural defects (missing nodes, unreachable routes, ...).
    pub errors: Vec<String>,
    /// Non-fatal observations.
    pub warnings: Vec<String>,
}

impl GraphTopologyReport {
    /// Render `topology` under the stable label `name`.
    pub fn describe(name: &'static str, topology: &GraphTopology) -> Self {
        Self {
            name,
            mermaid: to_mermaid(topology),
            json: to_json(topology),
            ok: topology.validation.ok,
            errors: topology.validation.errors.clone(),
            warnings: topology.validation.warnings.clone(),
        }
    }
}

#[cfg(test)]
#[path = "report_tests.rs"]
mod tests;
