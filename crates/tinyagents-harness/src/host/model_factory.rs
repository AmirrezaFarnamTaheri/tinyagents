//! Host-facing model construction API, split into request types, turn assembly,
//! and provider-specific subprocess adapters.

mod assembly;
mod types;

#[cfg(feature = "claude-code")]
mod subprocess;

pub use assembly::{TurnModelSet, TurnModelSetBuilder};
pub use types::{BuildOpts, ModelBuildGate, ModelBuildRequest, ModelFactory, ModelPurpose};

#[cfg(feature = "claude-code")]
pub use subprocess::{
    SubprocessProviderContext, build_claude_subprocess_model, build_claude_subprocess_model_with,
};
