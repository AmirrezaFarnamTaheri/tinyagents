use super::*;
use std::{
    collections::HashMap,
    io::Write,
    sync::Mutex,
    sync::atomic::{AtomicUsize, Ordering},
};

use futures::StreamExt;
use tinyinference_llm::{
    message::UserMessage,
    model::{Modalities, ModelStreamItem},
};

#[derive(Default)]
struct CaptureModel {
    requests: Mutex<Vec<ModelRequest>>,
    profile: ModelProfile,
    native_images: bool,
}

#[async_trait]
impl ChatModel<String> for CaptureModel {
    fn profile(&self) -> Option<&ModelProfile> {
        Some(&self.profile)
    }

    fn supports_input(&self, modality: InputModality, _mime: &str, source: InputSource) -> bool {
        modality == InputModality::Image && self.native_images && source == InputSource::Base64
    }

    fn cache_identity(&self) -> Option<String> {
        Some("capture-model".into())
    }

    async fn invoke(
        &self,
        _state: &String,
        request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        self.requests.lock().unwrap().push(request);
        Ok(ModelResponse::assistant(""))
    }
}

impl CaptureModel {
    fn new(native_images: bool) -> Self {
        let mut profile = ModelProfile::default();
        profile.modalities = Modalities {
            image_in: native_images,
            ..Modalities::default()
        };
        Self {
            requests: Mutex::default(),
            profile,
            native_images,
        }
    }
}

#[derive(Default)]
struct Resolver {
    sources: HashMap<String, ResolvedAttachment>,
    calls: AtomicUsize,
    scopes: Mutex<Vec<String>>,
}

#[derive(Default)]
struct PendingResolver {
    entered: tokio::sync::Notify,
}

#[async_trait]
impl MediaSourceResolver<String> for PendingResolver {
    async fn resolve(
        &self,
        _scope: &String,
        _modality: InputModality,
        _source: &str,
        _max_bytes: usize,
    ) -> std::result::Result<ResolvedAttachment, String> {
        self.entered.notify_one();
        std::future::pending().await
    }
}

#[async_trait]
impl MediaSourceResolver<String> for Resolver {
    async fn resolve(
        &self,
        scope: &String,
        _modality: InputModality,
        source: &str,
        max_bytes: usize,
    ) -> std::result::Result<ResolvedAttachment, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.scopes.lock().unwrap().push(scope.clone());
        let attachment = self
            .sources
            .get(source)
            .cloned()
            .ok_or_else(|| format!("missing test source {source}"))?;
        if attachment.bytes.len() > max_bytes {
            return Err("source exceeds test limit".into());
        }
        Ok(attachment)
    }
}

#[derive(Default)]
struct Extractor {
    inputs: Mutex<Vec<(String, Vec<u8>)>>,
}

#[async_trait]
impl TextExtractor for Extractor {
    fn handles(&self, mime: &str) -> bool {
        mime == "application/pdf"
    }

    async fn extract(&self, mime: &str, bytes: &[u8]) -> std::result::Result<String, String> {
        self.inputs
            .lock()
            .unwrap()
            .push((mime.to_string(), bytes.to_vec()));
        Ok("PDF text layer from fake parser".into())
    }
}

struct Readout;

#[async_trait]
impl MediaImageReadout<String> for Readout {
    async fn readout(
        &self,
        _scope: &String,
        _image: &ResolvedAttachment,
    ) -> std::result::Result<String, String> {
        Ok("a small red square".into())
    }
}

struct SafeHistory;

impl MediaHistoryFallback<String> for SafeHistory {
    fn omitted(&self, _scope: &String, _modality: InputModality, _source: &str) -> String {
        "[Earlier attachment omitted by history budget]".into()
    }

    fn unavailable(
        &self,
        _scope: &String,
        _modality: InputModality,
        _source: &str,
        error: &str,
    ) -> Option<String> {
        error
            .contains("missing test source")
            .then(|| "[Earlier attachment unavailable at workspace path: safe-hint]".into())
    }
}

fn attachment(name: &str, mime: &str, bytes: &[u8]) -> ResolvedAttachment {
    ResolvedAttachment {
        bytes: bytes.to_vec(),
        name: name.into(),
        mime: mime.into(),
        size_bytes: bytes.len(),
    }
}

fn request(text: &str) -> ModelRequest {
    ModelRequest {
        messages: vec![Message::User(UserMessage {
            content: vec![ContentBlock::Text(text.into())],
        })],
        ..ModelRequest::default()
    }
}

fn wrapper(
    model: Arc<CaptureModel>,
    config: MediaRequestConfig,
    resolver: Arc<Resolver>,
    extractor: Arc<Extractor>,
) -> MediaRequestModel<String, String> {
    MediaRequestModel::new(
        model,
        config,
        |state: &String, _request| Ok(state.clone()),
        resolver,
        extractor,
    )
}

#[tokio::test]
async fn hard_zero_rejects_before_any_source_read() {
    let model = Arc::new(CaptureModel::new(false));
    let resolver = Arc::new(Resolver::default());
    let disabled = MediaRequestConfig {
        files: FileLimits {
            max_files: 0,
            ..FileLimits::default()
        },
        ..MediaRequestConfig::default()
    };
    let wrapped = wrapper(model.clone(), disabled, resolver.clone(), Arc::default());

    let error = wrapped
        .invoke(&"tenant-a".into(), request("see [IMAGE:secret]"))
        .await
        .expect_err("hard-zero disables all media reads");

    assert!(error.to_string().contains("attachments are disabled"));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn hard_zero_also_blocks_media_only_present_in_history() {
    let model = Arc::new(CaptureModel::new(false));
    let resolver = Arc::new(Resolver::default());
    let disabled = MediaRequestConfig {
        files: FileLimits {
            max_files: 0,
            ..FileLimits::default()
        },
        ..MediaRequestConfig::default()
    };
    let wrapped = wrapper(model.clone(), disabled, resolver.clone(), Arc::default());
    let mut request = request("latest text only");
    request.messages.insert(
        0,
        Message::User(UserMessage {
            content: vec![ContentBlock::Text("earlier [IMAGE:secret]".into())],
        }),
    );

    wrapped.invoke(&"tenant-a".into(), request).await.unwrap();

    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    let captured = model.requests.lock().unwrap();
    let Message::User(user) = &captured[0].messages[0] else {
        panic!("historical user message retained")
    };
    assert!(matches!(
        &user.content[0],
        ContentBlock::Text(text) if text.contains("Earlier attachment omitted")
    ));
}

#[tokio::test]
async fn ordinary_count_limit_is_checked_before_resolving_any_marker() {
    let model = Arc::new(CaptureModel::new(true));
    let resolver = Arc::new(Resolver::default());
    let config = MediaRequestConfig {
        images: ImageLimits {
            max_images: 1,
            ..ImageLimits::default()
        },
        ..MediaRequestConfig::default()
    };
    let wrapped = wrapper(model, config, resolver.clone(), Arc::default());

    let error = wrapped
        .invoke(&"tenant-a".into(), request("[IMAGE:first] [IMAGE:second]"))
        .await
        .expect_err("two images exceed the configured cap");

    assert!(error.to_string().contains("max=1, found=2"));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invoke_and_stream_prepare_disposable_requests_without_changing_history() {
    let model = Arc::new(CaptureModel::new(true));
    let mut sources = HashMap::new();
    sources.insert(
        "image".into(),
        attachment("tiny.png", "image/png", b"\x89PNG\r\n\x1a\n"),
    );
    let resolver = Arc::new(Resolver {
        sources,
        ..Resolver::default()
    });
    let wrapped = wrapper(
        model.clone(),
        MediaRequestConfig::default(),
        resolver,
        Arc::default(),
    );
    let original = request("caption [IMAGE:image]");
    let canonical = original.clone();

    wrapped
        .invoke(&"tenant-a".into(), original.clone())
        .await
        .unwrap();
    let stream = wrapped
        .stream(&"tenant-a".into(), original.clone())
        .await
        .unwrap();
    let items: Vec<ModelStreamItem> = stream.collect().await;

    assert!(matches!(items.first(), Some(ModelStreamItem::Started)));
    assert_eq!(
        serde_json::to_value(&original).unwrap(),
        serde_json::to_value(&canonical).unwrap(),
        "the caller's request remains canonical"
    );
    let calls = model.requests.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for prepared in calls.iter() {
        let Message::User(user) = &prepared.messages[0] else {
            panic!("user request retained")
        };
        assert!(
            user.content
                .iter()
                .any(|block| matches!(block, ContentBlock::Image(_)))
        );
        assert!(!user.content.iter().any(|block| {
            matches!(block, ContentBlock::Text(text) if text.contains("[IMAGE:"))
        }));
    }
    assert_eq!(wrapped.cache_identity().as_deref(), Some("capture-model"));
    assert!(wrapped.profile().is_some());
}

#[tokio::test]
async fn resolution_error_and_cancellation_leave_the_canonical_request_unchanged() {
    let model = Arc::new(CaptureModel::new(true));
    let missing_resolver = Arc::new(Resolver::default());
    let wrapped = wrapper(
        model.clone(),
        MediaRequestConfig::default(),
        missing_resolver,
        Arc::default(),
    );
    let original = request("[IMAGE:unavailable]");
    let canonical = serde_json::to_value(&original).unwrap();
    assert!(
        wrapped
            .invoke(&"tenant-a".into(), original.clone())
            .await
            .is_err()
    );
    assert_eq!(serde_json::to_value(&original).unwrap(), canonical);
    assert!(model.requests.lock().unwrap().is_empty());

    let pending = Arc::new(PendingResolver::default());
    let cancel_model = Arc::new(CaptureModel::new(true));
    let canceled_wrapper = MediaRequestModel::new(
        cancel_model,
        MediaRequestConfig::default(),
        |state: &String, _request| Ok(state.clone()),
        pending.clone(),
        Arc::default(),
    );
    let canonical_after_cancel = serde_json::to_value(&original).unwrap();
    let call = {
        let wrapped = canceled_wrapper;
        let request = original.clone();
        tokio::spawn(async move { wrapped.invoke(&"tenant-a".into(), request).await })
    };
    pending.entered.notified().await;
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert_eq!(serde_json::to_value(&original).unwrap(), canonical_after_cancel);
}

#[tokio::test]
async fn concurrent_calls_keep_their_immutable_scope_separate() {
    let model = Arc::new(CaptureModel::new(true));
    let mut sources = HashMap::new();
    sources.insert(
        "image".into(),
        attachment("img.png", "image/png", b"\x89PNG\r\n\x1a\n"),
    );
    let resolver = Arc::new(Resolver {
        sources,
        ..Resolver::default()
    });
    let wrapped = wrapper(
        model,
        MediaRequestConfig::default(),
        resolver.clone(),
        Arc::default(),
    );

    let scope_a = "scope-a".to_string();
    let scope_b = "scope-b".to_string();
    let (a, b) = tokio::join!(
        wrapped.invoke(&scope_a, request("[IMAGE:image]")),
        wrapped.invoke(&scope_b, request("[IMAGE:image]")),
    );

    a.unwrap();
    b.unwrap();
    let mut scopes = resolver.scopes.lock().unwrap().clone();
    scopes.sort();
    assert_eq!(scopes, vec!["scope-a", "scope-b"]);
}

#[tokio::test]
async fn request_metadata_scope_is_consumed_per_concurrent_call() {
    let model = Arc::new(CaptureModel::new(true));
    let mut sources = HashMap::new();
    sources.insert("image".into(), attachment("img.png", "image/png", b"\x89PNG\r\n\x1a\n"));
    let resolver = Arc::new(Resolver { sources, ..Resolver::default() });
    let wrapped = MediaRequestModel::new(
        model.clone(),
        MediaRequestConfig::default(),
        |_state: &String, request: &mut ModelRequest| {
            request
                .metadata
                .as_object_mut()
                .and_then(|metadata| metadata.remove("trusted_scope"))
                .and_then(|scope| scope.as_str().map(str::to_string))
                .ok_or_else(|| "missing trusted test scope".to_string())
        },
        resolver.clone(),
        Arc::default(),
    );
    let operator = "same-operator".to_string();
    let mut first = request("[IMAGE:image]");
    first.metadata = serde_json::json!({"trusted_scope":"workspace-a"});
    let mut second = request("[IMAGE:image]");
    second.metadata = serde_json::json!({"trusted_scope":"workspace-b"});

    let (a, b) = tokio::join!(wrapped.invoke(&operator, first), wrapped.invoke(&operator, second));

    a.unwrap();
    b.unwrap();
    let mut scopes = resolver.scopes.lock().unwrap().clone();
    scopes.sort();
    assert_eq!(scopes, vec!["workspace-a", "workspace-b"]);
    for request in model.requests.lock().unwrap().iter() {
        assert!(request.metadata.get("trusted_scope").is_none());
    }
}

#[tokio::test]
async fn historical_media_budget_bounds_reads_and_replaces_older_items() {
    let model = Arc::new(CaptureModel::new(true));
    let mut sources = HashMap::new();
    for source in ["oldest", "newest"] {
        sources.insert(source.into(), attachment("img.png", "image/png", b"\x89PNG\r\n\x1a\n"));
    }
    let resolver = Arc::new(Resolver { sources, ..Resolver::default() });
    let config = MediaRequestConfig {
        max_historical_media_blocks: 1,
        ..MediaRequestConfig::default()
    };
    let wrapped = wrapper(model.clone(), config, resolver.clone(), Arc::default());
    let mut request = ModelRequest::default();
    request.messages = vec![
        Message::User(UserMessage {
            content: vec![ContentBlock::Image(ImageRef {
                url: "oldest".into(),
                mime_type: Some("image/png".into()),
            })],
        }),
        Message::User(UserMessage {
            content: vec![ContentBlock::Image(ImageRef {
                url: "newest".into(),
                mime_type: Some("image/png".into()),
            })],
        }),
        Message::User(UserMessage {
            content: vec![ContentBlock::Text("latest".into())],
        }),
    ];

    wrapped.invoke(&"tenant-a".into(), request).await.unwrap();

    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.scopes.lock().unwrap().len(), 1);
    let calls = model.requests.lock().unwrap();
    assert!(matches!(
        &calls[0].messages[0],
        Message::User(user) if matches!(&user.content[0], ContentBlock::Text(text) if text.contains("omitted by history budget"))
    ));
    assert!(matches!(
        &calls[0].messages[1],
        Message::User(user) if matches!(&user.content[0], ContentBlock::Image(_))
    ));
}

#[tokio::test]
async fn missing_history_media_degrades_safely_but_missing_latest_media_fails() {
    let model = Arc::new(CaptureModel::new(true));
    let resolver = Arc::new(Resolver::default());
    let wrapped = wrapper(model.clone(), MediaRequestConfig::default(), resolver, Arc::default())
        .with_history_fallback(Arc::new(SafeHistory));
    let mut history = ModelRequest::default();
    history.messages = vec![
        Message::User(UserMessage {
            content: vec![ContentBlock::Image(ImageRef {
                url: "/private-value/also-private".into(),
                mime_type: Some("image/png".into()),
            })],
        }),
        Message::User(UserMessage {
            content: vec![ContentBlock::Text("latest text".into())],
        }),
    ];

    wrapped.invoke(&"tenant-a".into(), history).await.unwrap();
    let calls = model.requests.lock().unwrap();
    let Message::User(old_user) = &calls[0].messages[0] else {
        panic!("historical message retained")
    };
    let ContentBlock::Text(placeholder) = &old_user.content[0] else {
        panic!("missing historical media becomes text")
    };
    assert!(placeholder.contains("safe-hint"));
    assert!(!placeholder.contains("private-value"));
    drop(calls);

    let error = wrapped
        .invoke(&"tenant-a".into(), request("[IMAGE:latest-missing]"))
        .await
        .expect_err("latest attachment failures remain fatal");
    assert!(error.to_string().contains("missing test source latest-missing"));
}

#[tokio::test]
async fn nonvision_fallback_reads_images_and_uses_the_pdf_extractor() {
    let model = Arc::new(CaptureModel::new(false));
    let pdf = b"%PDF-1.7 a meaningful fake PDF body";
    let mut sources = HashMap::new();
    sources.insert(
        "image".into(),
        attachment("red.png", "image/png", b"\x89PNG\r\n\x1a\n"),
    );
    sources.insert(
        "pdf".into(),
        attachment("brief.pdf", "application/pdf", pdf),
    );
    let resolver = Arc::new(Resolver {
        sources,
        ..Resolver::default()
    });
    let extractor = Arc::new(Extractor::default());
    let wrapped = wrapper(
        model.clone(),
        MediaRequestConfig::default(),
        resolver,
        extractor.clone(),
    )
    .with_image_readout(Arc::new(Readout));

    wrapped
        .invoke(&"tenant-a".into(), request("[IMAGE:image] [FILE:pdf]"))
        .await
        .unwrap();

    let calls = model.requests.lock().unwrap();
    let rendered = match &calls[0].messages[0] {
        Message::User(user) => user
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => panic!("prepared user message retained"),
    };
    assert!(rendered.contains("a small red square"));
    assert!(rendered.contains("PDF text layer from fake parser"));
    assert_eq!(
        extractor.inputs.lock().unwrap().as_slice(),
        &[("application/pdf".into(), pdf.to_vec())]
    );
}

#[tokio::test]
async fn nonvision_archive_fallback_lists_entries_without_extracting_them() {
    let mut bytes = std::io::Cursor::new(Vec::new());
    {
        let mut zip = zip::ZipWriter::new(&mut bytes);
        zip.start_file("notes/readme.txt", zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(b"do not extract this archive").unwrap();
        zip.finish().unwrap();
    }
    let model = Arc::new(CaptureModel::new(false));
    let mut sources = HashMap::new();
    sources.insert(
        "archive".into(),
        attachment("bundle.zip", "application/zip", bytes.get_ref()),
    );
    let resolver = Arc::new(Resolver {
        sources,
        ..Resolver::default()
    });
    let wrapped = wrapper(
        model.clone(),
        MediaRequestConfig::default(),
        resolver,
        Arc::default(),
    );

    wrapped
        .invoke(&"tenant-a".into(), request("[FILE:archive]"))
        .await
        .unwrap();

    let captured = model.requests.lock().unwrap();
    let Message::User(user) = &captured[0].messages[0] else {
        panic!("prepared user message retained")
    };
    let rendered = user
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("Archive listing"));
    assert!(rendered.contains("notes/readme.txt"));
    assert!(rendered.contains("original was not extracted"));
}
