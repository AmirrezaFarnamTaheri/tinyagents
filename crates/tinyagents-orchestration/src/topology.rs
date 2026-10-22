//! Topology export for the orchestration graphs, for debug / inspection.
//!
//! Each orchestration graph exposes a `*_topology()` builder that constructs
//! its structure with no-op stub closures and returns a behaviour-free
//! [`GraphTopology`](tinyagents_graph::GraphTopology). [`all_graph_topologies`]
//! collects them so a UI or debug endpoint can render them as JSON or Mermaid
//! and surface structural defects.

use tinyagents_graph::GraphTopologyReport;

/// Structure-only topologies of every fixed-shape orchestration graph.
///
/// Graphs that fail to build are skipped. Item-count-driven `map_reduce`
/// fan-outs are not exported: their node set is decided per run.
pub fn all_graph_topologies() -> Vec<GraphTopologyReport> {
    let mut out = Vec::new();

    if let Ok(t) = crate::teams::member_graph_topology() {
        out.push(GraphTopologyReport::describe("agent_teams:member", &t));
    }
    if let Ok(t) = tinyagents_graph::delegation::delegation_graph_topology() {
        out.push(GraphTopologyReport::describe("delegation", &t));
    }
    if let Ok(t) = crate::workflow::scheduler_topology_preview() {
        out.push(GraphTopologyReport::describe(
            "workflow_runs:scheduler_preview",
            &t,
        ));
    }

    out
}

#[cfg(test)]
#[path = "topology_tests.rs"]
mod tests;
