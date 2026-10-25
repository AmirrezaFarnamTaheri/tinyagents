use async_trait::async_trait;
use std::path::PathBuf;
use tinytools::ToolRunContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaArtifactKind {
    Image,
    Video,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedArtifact {
    pub path: PathBuf,
    pub artifact_kind: MediaArtifactKind,
    pub kind: String,
    pub media_type: Option<String>,
    pub bytes: Option<u64>,
    pub title: String,
    pub tool_call_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiledArtifact {
    pub id: String,
    pub path: String,
}

#[async_trait]
pub trait ArtifactFiling: Send + Sync {
    async fn file(
        &self,
        generated: GeneratedArtifact,
        context: Option<&dyn ToolRunContext>,
    ) -> Result<FiledArtifact, String>;
}
