use super::*;
use crate::media::FiledArtifact;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tinytools::{Tool, ToolCallOptions, ToolContent, ToolResult, ToolRunContext};
use tinytools::{ToolAccess, ToolPolicy, ToolScope, ToolSideEffects, WorkspaceAccess};

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
struct Context {
    identity: CallIdentity,
    progress: Mutex<Vec<String>>,
}
impl ToolRunContext for Context {
    fn host_extension(&self) -> Option<&(dyn std::any::Any + Send + Sync)> {
        Some(&self.identity)
    }
    fn report_progress(&self, update: tinytools::ToolProgress) {
        self.progress
            .lock()
            .unwrap()
            .push(update.message.unwrap_or_default());
    }
}

struct RoutingFiler;
#[async_trait]
impl ArtifactFiling for RoutingFiler {
    async fn file(
        &self,
        generated: GeneratedArtifact,
        context: Option<&dyn ToolRunContext>,
    ) -> Result<FiledArtifact, String> {
        let identity = context
            .and_then(|context| context.host_extension())
            .and_then(|extension| extension.downcast_ref::<CallIdentity>())
            .ok_or("missing call identity")?;
        assert_eq!(generated.tool_call_id, Some(identity.0.to_owned()));
        Ok(FiledArtifact {
            id: identity.0.to_owned(),
            path: format!("/files/{}.png", identity.0),
        })
    }
}

struct OneFails;
#[async_trait]
impl ArtifactFiling for OneFails {
    async fn file(
        &self,
        generated: GeneratedArtifact,
        _: Option<&dyn ToolRunContext>,
    ) -> Result<FiledArtifact, String> {
        if generated.path == std::path::Path::new("blob:bad") {
            return Err("filing failed".into());
        }
        Ok(FiledArtifact {
            id: "good-id".into(),
            path: "/files/good.png".into(),
        })
    }
}

struct MultiInner;
#[async_trait]
impl Tool for MultiInner {
    fn name(&self) -> &str {
        "multi"
    }
    fn description(&self) -> &str {
        "multi description"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{"prompt":{"type":"string"}}})
    }
    fn category(&self) -> ToolCategory {
        ToolCategory::Workflow
    }
    fn external_effect(&self) -> bool {
        true
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::Dangerous
    }
    fn timeout_policy(&self, _: &Value) -> ToolTimeout {
        ToolTimeout::Unbounded
    }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success_with_markdown(
            json!({"artifacts":[
                {"type":"image","path":"blob:good","bytes":3},
                {"type":"image","path":"blob:bad","bytes":4},
                {"type":"image","path":"blob:last","bytes":5}
            ],"other":"retained"}),
            "good blob:good; bad blob:bad; last blob:last",
        ))
    }
}

#[derive(Clone, Copy)]
struct MetadataInner {
    temporary: bool,
}
impl MetadataInner {
    fn text(&self, retained: &'static str, temporary: &'static str) -> &'static str {
        if self.temporary { temporary } else { retained }
    }
}
#[async_trait]
impl Tool for MetadataInner {
    fn name(&self) -> &str {
        self.text("retained_media", "temporary_media")
    }
    fn description(&self) -> &str {
        self.text("retained description", "temporary description")
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{"source":{"const":self.text("retained","temporary")}}})
    }
    fn policy(&self) -> ToolPolicy {
        ToolPolicy::classified()
            .with_side_effects(ToolSideEffects {
                network: !self.temporary,
                writes_files: self.temporary,
                ..ToolSideEffects::default()
            })
            .with_access(ToolAccess {
                workspace: WorkspaceAccess::Scoped,
                trusted_roots: vec![self.text("/retained", "/temporary").into()],
                approval_required: !self.temporary,
                ..ToolAccess::default()
            })
    }
    fn permission_level(&self) -> PermissionLevel {
        if self.temporary {
            PermissionLevel::Write
        } else {
            PermissionLevel::ReadOnly
        }
    }
    fn permission_level_with_args(&self, args: &Value) -> PermissionLevel {
        if args["danger"] == true {
            PermissionLevel::Dangerous
        } else {
            self.permission_level()
        }
    }
    fn scope(&self) -> ToolScope {
        if self.temporary {
            ToolScope::AgentOnly
        } else {
            ToolScope::CliRpcOnly
        }
    }
    fn category(&self) -> ToolCategory {
        if self.temporary {
            ToolCategory::System
        } else {
            ToolCategory::Workflow
        }
    }
    fn external_effect(&self) -> bool {
        self.temporary
    }
    fn external_effect_with_args(&self, args: &Value) -> bool {
        args["send"] == true
    }
    fn max_result_size_chars(&self) -> Option<usize> {
        Some(if self.temporary { 22 } else { 11 })
    }
    fn supports_markdown(&self) -> bool {
        !self.temporary
    }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success_with_markdown(
            json!({"artifacts":[
                {"type":"image","path":"blob:first"},
                {"type":"video","path":"blob:second"}
            ]}),
            "blob:first then blob:second",
        ))
    }
}

async fn assert_retained_metadata(factory: bool) {
    let retained = MetadataInner { temporary: false };
    let filer = Arc::new(Filer::default());
    let mut tool = MediaArtifactTool::new(retained, MediaArtifactKind::Image, filer.clone());
    if factory {
        tool = tool.with_per_call_inner(Arc::new(|_, _| MetadataInner { temporary: true }));
    }
    let assert_metadata = |tool: &MediaArtifactTool<MetadataInner>| {
        assert_eq!(tool.name(), "retained_media");
        assert_eq!(tool.description(), "retained description");
        assert_eq!(
            tool.parameters_schema(),
            json!({"type":"object","properties":{"source":{"const":"retained"}}})
        );
        assert_eq!(tool.policy(), retained.policy());
        assert_eq!(tool.scope(), ToolScope::CliRpcOnly);
        assert_eq!(tool.category(), ToolCategory::Workflow);
        assert_eq!(tool.permission_level(), PermissionLevel::ReadOnly);
        assert_eq!(
            tool.permission_level_with_args(&json!({"danger":true})),
            PermissionLevel::Dangerous
        );
        assert!(!tool.external_effect());
        assert!(tool.external_effect_with_args(&json!({"send":true})));
        assert_eq!(tool.max_result_size_chars(), Some(11));
        assert!(tool.supports_markdown());
    };
    assert_metadata(&tool);
    let long_prompt = format!("{}", "x".repeat(70));
    tool.execute(json!({"prompt":long_prompt})).await.unwrap();
    assert_metadata(&tool);
    let filed = filer.0.lock().unwrap();
    assert_eq!(filed.len(), 2);
    assert_eq!(filed[0].title, format!("{} (1/2)", "x".repeat(60)));
    assert_eq!(filed[1].title, format!("{} (2/2)", "x".repeat(60)));
    assert_eq!(filed[0].artifact_kind, MediaArtifactKind::Image);
    assert_eq!(filed[1].artifact_kind, MediaArtifactKind::Image);
}

#[tokio::test]
async fn retained_metadata_survives_execution_with_and_without_per_call_factory() {
    assert_retained_metadata(false).await;
    assert_retained_metadata(true).await;
}

struct ConcurrentInner {
    identity: &'static str,
    progress: MediaProgressFn,
    barrier: Arc<tokio::sync::Barrier>,
}
#[async_trait]
impl Tool for ConcurrentInner {
    fn name(&self) -> &str {
        "concurrent"
    }
    fn description(&self) -> &str {
        "per-call concurrent tool"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        (self.progress)(&MediaJobProgress {
            id: self.identity.into(),
            state: format!("{}-job", self.identity),
            outputs: 2,
            output_indices: vec![0, 1],
            cost_usd: None,
            error: None,
        });
        self.barrier.wait().await;
        tokio::time::sleep(Duration::from_secs(20)).await;
        Ok(ToolResult::success_with_markdown(
            json!({"artifacts":[
                {"type":"image","path":format!("blob:{}-one", self.identity)},
                {"type":"image","path":format!("blob:{}-two", self.identity)}
            ]}),
            format!("blob:{}-one and blob:{}-two", self.identity, self.identity),
        ))
    }
}

struct ProgressInner(MediaProgressFn);
#[async_trait]
impl Tool for ProgressInner {
    fn name(&self) -> &str {
        "progress"
    }
    fn description(&self) -> &str {
        "progress"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        (self.0)(&MediaJobProgress {
            id: "job-1".into(),
            state: "in_progress".into(),
            outputs: 0,
            output_indices: Vec::new(),
            cost_usd: None,
            error: None,
        });
        tokio::time::sleep(Duration::from_secs(20)).await;
        Ok(ToolResult::success("done"))
    }
}

struct ProgressArtifactInner(MediaProgressFn);
#[async_trait]
impl Tool for ProgressArtifactInner {
    fn name(&self) -> &str {
        "progress_artifact"
    }
    fn description(&self) -> &str {
        "progress artifact"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        (self.0)(&MediaJobProgress {
            id: "job-kind".into(),
            state: "running".into(),
            outputs: 1,
            output_indices: vec![0],
            cost_usd: None,
            error: None,
        });
        tokio::time::sleep(Duration::from_secs(20)).await;
        Ok(ToolResult::success_with_markdown(
            json!({"artifacts":[{"type":"media","path":"blob:kind"}]}),
            "blob:kind",
        ))
    }
}

struct ErrorInner;
#[async_trait]
impl Tool for ErrorInner {
    fn name(&self) -> &str {
        "error"
    }
    fn description(&self) -> &str {
        "error"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::error("provider failed"))
    }
}

struct UnknownShapeInner;
#[async_trait]
impl Tool for UnknownShapeInner {
    fn name(&self) -> &str {
        "unknown_shape"
    }
    fn description(&self) -> &str {
        "unknown"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success_with_markdown(
            json!({"result":"provider-specific","items":[{"url":"https://example.test/media"}]}),
            "provider-specific markdown",
        ))
    }
}

#[derive(Default)]
struct ProgressRecorder(Mutex<Vec<String>>);
impl ToolRunContext for ProgressRecorder {
    fn report_progress(&self, update: tinytools::ToolProgress) {
        self.0
            .lock()
            .unwrap()
            .push(update.message.unwrap_or_default());
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

#[tokio::test(start_paused = true)]
async fn concurrent_calls_keep_each_invocations_context_and_rewrite_separate_results() {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let factory_barrier = Arc::clone(&barrier);
    let tool = MediaArtifactTool::new(
        ConcurrentInner {
            identity: "unused",
            progress: Arc::new(|_| {}),
            barrier: Arc::clone(&barrier),
        },
        MediaArtifactKind::Image,
        Arc::new(RoutingFiler),
    )
    .with_per_call_inner(Arc::new(move |progress, context| {
        let identity = context
            .and_then(|context| context.host_extension())
            .and_then(|extension| extension.downcast_ref::<CallIdentity>())
            .expect("factory receives each invocation context")
            .0;
        ConcurrentInner {
            identity,
            progress,
            barrier: Arc::clone(&factory_barrier),
        }
    }))
    .with_tool_call_id(Arc::new(|context| {
        context
            .and_then(|context| context.host_extension())
            .and_then(|extension| extension.downcast_ref::<CallIdentity>())
            .map(|identity| identity.0.to_owned())
    }));
    let first = Context {
        identity: CallIdentity("first"),
        progress: Mutex::new(Vec::new()),
    };
    let second = Context {
        identity: CallIdentity("second"),
        progress: Mutex::new(Vec::new()),
    };
    let (left, right) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(
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
        )
    })
    .await
    .expect("both factory calls must reach the overlap barrier");
    for (result, identity) in [(left.unwrap(), "first"), (right.unwrap(), "second")] {
        let ToolContent::Json { data } = &result.content[0] else {
            panic!("expected JSON result")
        };
        assert_eq!(data["artifacts"][0]["artifact_id"], identity);
        assert_eq!(
            data["artifacts"][0]["path"],
            format!("/files/{identity}.png")
        );
        assert_eq!(data["artifacts"][1]["artifact_id"], identity);
        assert_eq!(
            data["artifacts"][1]["path"],
            format!("/files/{identity}.png")
        );
        assert_eq!(
            result.markdown_formatted.as_deref(),
            Some(format!("/files/{identity}.png and /files/{identity}.png").as_str())
        );
    }
    assert_eq!(
        *first.progress.lock().unwrap(),
        vec!["Generating image — 15s elapsed (job first-job)"]
    );
    assert_eq!(
        *second.progress.lock().unwrap(),
        vec!["Generating image — 15s elapsed (job second-job)"]
    );
}

#[tokio::test]
async fn generated_artifact_uses_canonical_harness_call_id_when_context_has_one() {
    use tinyagents_harness::{
        context::{RunConfig, RunContext},
        ids::CallId,
        tool::ToolExecutionContext,
    };
    let context = ToolExecutionContext::from_run_context(
        &RunContext::new(RunConfig::new("media-test"), ()),
        CallId::new("provider-call-42"),
    );
    let filer = Arc::new(Filer::default());
    let tool = MediaArtifactTool::new(Inner, MediaArtifactKind::Image, filer.clone());
    tool.execute_with_context(
        json!({"prompt":"call id"}),
        ToolCallOptions::default(),
        Some(&context),
    )
    .await
    .unwrap();
    assert_eq!(
        filer.0.lock().unwrap()[0].tool_call_id.as_deref(),
        Some("provider-call-42")
    );
}

#[tokio::test]
async fn per_call_inner_preserves_stable_metadata_and_files_multiple_entries_independently() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let factory_calls = Arc::clone(&calls);
    let tool = MediaArtifactTool::new(MultiInner, MediaArtifactKind::Image, Arc::new(OneFails))
        .with_per_call_inner(Arc::new(move |_progress, _context| {
            factory_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            MultiInner
        }));
    assert_eq!(tool.name(), "multi");
    assert_eq!(tool.description(), "multi description");
    assert_eq!(tool.category(), ToolCategory::Workflow);
    assert_eq!(tool.permission_level(), PermissionLevel::Dangerous);
    assert!(tool.external_effect());
    assert_eq!(tool.timeout_policy(&json!({})), ToolTimeout::Unbounded);
    let result = tool
        .execute(json!({"prompt":format!(" {} ", "x".repeat(70))}))
        .await
        .unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let ToolContent::Json { data } = &result.content[0] else {
        panic!("expected JSON")
    };
    assert_eq!(data["other"], "retained");
    assert_eq!(data["artifacts"][0]["path"], "/files/good.png");
    assert_eq!(data["artifacts"][0]["artifact_id"], "good-id");
    assert_eq!(data["artifacts"][1]["path"], "blob:bad");
    assert_eq!(data["artifacts"][1]["artifact_error"], "filing failed");
    assert_eq!(data["artifacts"][2]["path"], "/files/good.png");
    assert_eq!(data["artifacts"][2]["artifact_id"], "good-id");
    assert_eq!(
        result.markdown_formatted.as_deref(),
        Some("good /files/good.png; bad blob:bad; last /files/good.png")
    );
}

#[test]
fn configured_media_kinds_keep_their_exact_heartbeat_labels() {
    assert_eq!(
        progress_label(&MediaArtifactKind::Image),
        "Generating image"
    );
    assert_eq!(
        progress_label(&MediaArtifactKind::Video),
        "Generating video"
    );
}

#[test]
fn heartbeat_keeps_elapsed_duration_when_job_state_is_present() {
    assert_eq!(
        heartbeat_message(
            "Generating video",
            std::time::Duration::from_secs(17),
            Some("processing")
        ),
        "Generating video — 17s elapsed (job processing)"
    );
    assert_eq!(
        heartbeat_message(
            "Generating image",
            std::time::Duration::from_secs(125),
            None
        ),
        "Generating image — 2m 05s elapsed"
    );
}

#[tokio::test(start_paused = true)]
async fn media_heartbeat_reports_per_call_job_state() {
    let recorder = ProgressRecorder::default();
    let tool = MediaArtifactTool::new(
        ProgressInner(Arc::new(|_| {})),
        MediaArtifactKind::Video,
        Arc::new(Filer::default()),
    )
    .with_per_call_inner(Arc::new(|progress, _context| ProgressInner(progress)));
    let result = tool
        .execute_with_context(json!({}), ToolCallOptions::default(), Some(&recorder))
        .await
        .unwrap();
    assert_eq!(result.text(), "done");
    assert_eq!(
        *recorder.0.lock().unwrap(),
        vec!["Generating video — 15s elapsed (job in_progress)"],
    );
}

#[tokio::test(start_paused = true)]
async fn image_and_video_factory_calls_keep_configured_kind_and_heartbeat() {
    for (kind, label) in [
        (MediaArtifactKind::Image, "Generating image"),
        (MediaArtifactKind::Video, "Generating video"),
    ] {
        let recorder = ProgressRecorder::default();
        let filer = Arc::new(Filer::default());
        let tool = MediaArtifactTool::new(
            ProgressArtifactInner(Arc::new(|_| {})),
            kind.clone(),
            filer.clone(),
        )
        .with_per_call_inner(Arc::new(|progress, _context| {
            ProgressArtifactInner(progress)
        }));
        let result = tool
            .execute_with_context(json!({}), ToolCallOptions::default(), Some(&recorder))
            .await
            .unwrap();
        let ToolContent::Json { data } = &result.content[0] else {
            panic!("expected JSON result")
        };
        assert_eq!(data["artifacts"][0]["path"], "/files/image.png");
        assert_eq!(
            *recorder.0.lock().unwrap(),
            vec![format!("{label} — 15s elapsed (job running)")]
        );
        assert_eq!(filer.0.lock().unwrap()[0].artifact_kind, kind);
    }
}

#[tokio::test]
async fn errors_and_unknown_output_shapes_are_preserved_without_filing() {
    let filer = Arc::new(Filer::default());
    let error_tool = MediaArtifactTool::new(ErrorInner, MediaArtifactKind::Image, filer.clone());
    let error = error_tool.execute(json!({})).await.unwrap();
    assert!(error.is_error);
    assert_eq!(error.text(), "provider failed");
    let unknown_tool =
        MediaArtifactTool::new(UnknownShapeInner, MediaArtifactKind::Image, filer.clone());
    let unknown = unknown_tool.execute(json!({})).await.unwrap();
    let ToolContent::Json { data } = &unknown.content[0] else {
        panic!("expected JSON")
    };
    assert_eq!(
        data,
        &json!({"result":"provider-specific","items":[{"url":"https://example.test/media"}]})
    );
    assert_eq!(
        unknown.markdown_formatted.as_deref(),
        Some("provider-specific markdown")
    );
    assert!(filer.0.lock().unwrap().is_empty());
}
