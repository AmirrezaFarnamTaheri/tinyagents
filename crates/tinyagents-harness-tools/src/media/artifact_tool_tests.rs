use super::*;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tinytools::{Tool, ToolCallOptions, ToolContent, ToolResult, ToolRunContext};

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
    fn max_result_size_chars(&self) -> Option<usize> {
        Some(1234)
    }
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

struct CallIdentity(&'static str);
struct Context(CallIdentity);
impl ToolRunContext for Context {
    fn host_extension(&self) -> Option<&(dyn std::any::Any + Send + Sync)> {
        Some(&self.0)
    }
}

struct RoutingFiler;
#[async_trait]
impl ArtifactFiling for RoutingFiler {
    async fn file(
        &self,
        _: GeneratedArtifact,
        context: Option<&dyn ToolRunContext>,
    ) -> Result<FiledArtifact, String> {
        let identity = context
            .and_then(|context| context.host_extension())
            .and_then(|extension| extension.downcast_ref::<CallIdentity>())
            .ok_or("missing call identity")?;
        Ok(FiledArtifact {
            id: identity.0.to_owned(),
            path: format!("/files/{}.png", identity.0),
        })
    }
}

#[tokio::test]
async fn wrapper_keeps_metadata_and_files_each_artifact() {
    let filer = Arc::new(Filer::default());
    let tool = MediaArtifactTool::new(Inner, MediaArtifactKind::Video, filer.clone());
    assert_eq!(tool.name(), "inner");
    assert_eq!(tool.description(), "stable");
    assert_eq!(tool.parameters_schema(), json!({"type":"object"}));
    assert_eq!(tool.max_result_size_chars(), Some(1234));
    let result = tool.execute(json!({"prompt":" a title "})).await.unwrap();
    let ToolContent::Json { data } = &result.content[0] else {
        panic!("expected JSON content")
    };
    assert_eq!(data["artifacts"][0]["path"], "/files/image.png");
    assert_eq!(data["artifacts"][0]["artifact_id"], "id1");
    assert_eq!(
        result.markdown_formatted.as_deref(),
        Some("saved /files/image.png")
    );
    let filed = filer.0.lock().unwrap();
    assert_eq!(filed[0].artifact_kind, MediaArtifactKind::Video);
    assert_eq!(filed[0].title, "a title");
    assert_eq!(filed[0].path, PathBuf::from("blob:key"));
}

#[tokio::test]
async fn concurrent_calls_keep_each_invocations_context_and_rewrite_separate_results() {
    let tool = MediaArtifactTool::new(Inner, MediaArtifactKind::Image, Arc::new(RoutingFiler));
    let first = Context(CallIdentity("first"));
    let second = Context(CallIdentity("second"));
    let (left, right) = tokio::join!(
        tool.execute_with_context(
            json!({"prompt":"first"}),
            ToolCallOptions::default(),
            Some(&first)
        ),
        tool.execute_with_context(
            json!({"prompt":"second"}),
            ToolCallOptions::default(),
            Some(&second)
        ),
    );
    for (result, identity) in [(left.unwrap(), "first"), (right.unwrap(), "second")] {
        let ToolContent::Json { data } = &result.content[0] else {
            panic!("expected JSON result")
        };
        assert_eq!(data["artifacts"][0]["artifact_id"], identity);
        assert_eq!(
            data["artifacts"][0]["path"],
            format!("/files/{identity}.png")
        );
    }
}

#[test]
fn configured_media_kinds_keep_their_exact_heartbeat_labels() {
    assert_eq!(progress_label(&MediaArtifactKind::Image), "Generating image");
    assert_eq!(progress_label(&MediaArtifactKind::Video), "Generating video");
}
