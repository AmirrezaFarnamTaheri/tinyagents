//! Projection of a turn's tools, agents and graphs into a
//! [`CapabilityRegistry`] so health diagnostics see what the turn can use.

use std::collections::HashSet;
use std::sync::Arc;

use tinyagents_harness::runtime::AgentHarness;
use tinyagents_registry::{CapabilityRegistry, ComponentKind};
use tinytools::Tool;

use crate::tool_registration::{plan_registration, RegistrationPlan, ToolAdmission, ToolRegistration};

/// Like [`register_admitted`](crate::register_admitted), and also mirrors every
/// registered tool into `registry`. `build` returns the registration plus the
/// plain `Tool` view of it that the registry should hold.
pub fn register_admitted_projected<State, C, I, S>(
    harness: &mut AgentHarness<State, C>,
    registry: &mut CapabilityRegistry<State>,
    names: I,
    admission: &ToolAdmission,
    mut build: impl FnMut(&str) -> Option<(Arc<dyn Tool>, ToolRegistration<State, C>)>,
) -> (usize, RegistrationPlan)
where
    State: Send + Sync + 'static,
    C: Send + Sync + 'static,
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let plan = plan_registration(names, admission);
    let mut count = 0;
    for name in &plan.admitted {
        let Some((view, registration)) = build(name) else {
            continue;
        };
        registry.replace_tool(view);
        match registration {
            ToolRegistration::Tool(tool) => {
                harness.register_tool(tool);
            }
            ToolRegistration::Dispatch(dispatch) => {
                harness.register_tool_dispatch(dispatch);
            }
        }
        count += 1;
    }
    (count, plan)
}

/// Registers name-only descriptors of `kind` for each distinct name, skipping
/// names already present. Returns how many were newly registered.
pub fn project_descriptors<State, I, S>(
    registry: &mut CapabilityRegistry<State>,
    kind: ComponentKind,
    names: I,
) -> usize
where
    State: Send + Sync + 'static,
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut seen = HashSet::new();
    let mut added = 0;
    for name in names {
        let name = name.into();
        if !seen.insert(name.clone()) {
            continue;
        }
        if registry.register_descriptor(kind, name).is_ok() {
            added += 1;
        }
    }
    added
}

#[cfg(test)]
#[path = "capability_projection_tests.rs"]
mod tests;
