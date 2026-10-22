//! Spawn-hierarchy tier ([`AgentTier`]) and the single tier-transition rule
//! shared by the loader's static check and the runtime spawn gate.

use serde::{Deserialize, Serialize};

/// Delegation tier declared by an agent definition.
///
/// Workers are leaves. Chat and reasoning agents may delegate to a different
/// tier, including a reasoning-to-chat handoff. Hosts enforce depth limits
/// independently through their runtime spawn policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentTier {
    /// User-facing coordinator.
    Chat,
    /// Coordinator for reasoning and decomposition.
    Reasoning,
    /// Leaf executor, forbidden from delegating to another agent.
    #[serde(alias = "worker_leaf")]
    #[default]
    Worker,
}

impl AgentTier {
    /// Human-readable tier name used in error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Reasoning => "reasoning",
            Self::Worker => "worker",
        }
    }
}

impl std::fmt::Display for AgentTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Checks a declared delegation edge.
///
/// Worker parents and same-tier chat or reasoning edges return a tier-only
/// explanation. All other edges are allowed; this does not enforce depth,
/// delegate existence, or host authorization.
pub fn validate_tier_transition(parent: AgentTier, child: AgentTier) -> Result<(), String> {
    match (parent, child) {
        (AgentTier::Worker, _) => Err(format!(
            "a `worker` tier agent must not spawn `{}` — workers are leaf executors",
            child.as_str()
        )),
        (AgentTier::Chat, AgentTier::Chat) => Err(
            "the chat tier is a leaf in its own dimension — hand off to a `reasoning` or \
             `worker` agent instead"
                .to_string(),
        ),
        (AgentTier::Reasoning, AgentTier::Reasoning) => {
            Err("reasoning agents compose downward into workers, not into each other".to_string())
        }
        _ => Ok(()),
    }
}
