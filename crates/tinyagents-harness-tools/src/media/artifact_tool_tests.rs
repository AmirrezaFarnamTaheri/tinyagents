use super::*;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tinytools::{Tool, ToolContent, ToolResult, ToolRunContext};

struct Inner;
#[async_trait]
impl Tool for Inner {
    fn name(&self) -> &str {
        "inner"
    }
    fn description(&self) -> &str {
        "stable"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
    fn max_result_size_chars(&self)->Option<usize>{Some(1234)}
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success_with_markdown(
            json!({"artifacts":[{"type":"image","path":"blob:key","bytes":4}]}),
            "saved blob:key",
        ))
    }
}
#[derive(Default)]
struct Filer(Mutex<Vec<GeneratedArtifact>>);
#[async_trait]
impl ArtifactFiling for Filer {
    async fn file(
        &self,
        g: GeneratedArtifact,
        _: Option<&dyn ToolRunContext>,
    ) -> Result<FiledArtifact, String> {
        self.0.lock().unwrap().push(g);
        Ok(FiledArtifact {
            id: "id1".into(),
            path: "/files/image.png".into(),
        })
    }
}

#[tokio::test]
async fn wrapper_keeps_metadata_and_files_each_artifact() {
    let filer = Arc::new(Filer::default());
    let tool = MediaArtifactTool::new(Inner, MediaArtifactKind::Video, filer.clone());
    assert_eq!(tool.name(), "inner");
    assert_eq!(tool.description(), "stable");
    assert_eq!(tool.parameters_schema(),json!({"type":"object"}));
    assert_eq!(tool.max_result_size_chars(),Some(1234));
    let result = tool.execute(json!({"prompt":" a title "})).await.unwrap();
    let ToolContent::Json { data } = &result.content[0] else {
        panic!("expected JSON content")
    };
    assert_eq!(data["artifacts"][0]["path"], "/files/image.png");
    assert_eq!(data["artifacts"][0]["artifact_id"], "id1");
    assert_eq!(result.markdown_formatted.as_deref(),Some("saved /files/image.png"));
    let filed = filer.0.lock().unwrap();
    assert_eq!(filed[0].artifact_kind, MediaArtifactKind::Video);
    assert_eq!(filed[0].title, "a title");
    assert_eq!(filed[0].path, PathBuf::from("blob:key"));
}
