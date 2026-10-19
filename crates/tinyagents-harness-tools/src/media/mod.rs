//! Host-neutral wrappers for tools that generate media artifacts.
mod artifact_filing;
mod artifact_tool;
pub use artifact_filing::{ArtifactFiling, FiledArtifact, GeneratedArtifact, MediaArtifactKind};
pub use artifact_tool::{
    MediaArtifactTool, MediaJobProgress, MediaProgressFn, ToolCallIdExtractor,
};
