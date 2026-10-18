use super::super::NoTextExtractor;
use super::*;
use std::{
    collections::HashMap,
    io::Write,
    sync::Mutex,
    sync::atomic::{AtomicUsize, Ordering},
};

use futures::StreamExt;
use tinyinference_llm::{
    message::{ImageRef, MediaRef, UserMessage},
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
        let profile = ModelProfile {
            modalities: Modalities {
                image_in: native_images,
                ..Modalities::default()
            },
            ..ModelProfile::default()
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
    annotations: HashMap<String, Vec<String>>,
    calls: AtomicUsize,
    scopes: Mutex<Vec<String>>,
    resolved_sources: Mutex<Vec<String>>,
    mime_hints: Mutex<Vec<Option<String>>>,
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
    ) -> std::result::Result<ResolvedMedia, String> {
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
    ) -> std::result::Result<ResolvedMedia, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.scopes.lock().unwrap().push(scope.clone());
        self.resolved_sources
            .lock()
            .unwrap()
            .push(source.to_string());
        let attachment = self
            .sources
            .get(source)
            .cloned()
            .ok_or_else(|| format!("missing test source {source}"))?;
        if attachment.bytes.len() > max_bytes {
            return Err("source exceeds test limit".into());
        }
        let mut resolved = ResolvedMedia::new(attachment);
        for annotation in self.annotations.get(source).into_iter().flatten() {
            resolved = resolved.with_annotation(annotation.clone());
        }
        Ok(resolved)
    }

    async fn resolve_with_mime_hint(
        &self,
        scope: &String,
        modality: InputModality,
        source: &str,
        max_bytes: usize,
        mime_hint: Option<&str>,
    ) -> std::result::Result<ResolvedMedia, String> {
        self.mime_hints
            .lock()
            .unwrap()
            .push(mime_hint.map(str::to_owned));
        self.resolve(scope, modality, source, max_bytes).await
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

struct LongUnicodeReadout;

#[async_trait]
impl MediaImageReadout<String> for LongUnicodeReadout {
    async fn readout(
        &self,
        _scope: &String,
        _image: &ResolvedAttachment,
    ) -> std::result::Result<String, String> {
        Ok("🧪".repeat(2_000))
    }
}

#[derive(Default)]
struct RecordingFallback {
    calls: Mutex<Vec<(String, InputModality, String)>>,
    fail: bool,
    return_none: bool,
}

#[async_trait]
impl MediaFallbackRenderer<String> for RecordingFallback {
    async fn render(
        &self,
        scope: &String,
        modality: InputModality,
        attachment: &ResolvedAttachment,
    ) -> std::result::Result<Option<String>, String> {
        self.calls
            .lock()
            .unwrap()
            .push((scope.clone(), modality, attachment.name.clone()));
        if self.fail {
            return Err("host rendering failed".into());
        }
        if self.return_none {
            return Ok(None);
        }
        Ok(Some(format!("host fallback for {}", attachment.name)))
    }
}

#[derive(Default)]
struct RecordingFallbackCache {
    gets: AtomicUsize,
    hits: AtomicUsize,
    puts: Mutex<Vec<(InputModality, String)>>,
    entries: Mutex<HashMap<String, String>>,
}

#[async_trait]
impl MediaFallbackCache<String> for RecordingFallbackCache {
    async fn get(
        &self,
        scope: &String,
        modality: InputModality,
        attachment: &ResolvedAttachment,
    ) -> Option<String> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let key = format!("{scope}:{modality:?}:{}", attachment.name);
        let value = self.entries.lock().unwrap().get(&key).cloned();
        if value.is_some() {
            self.hits.fetch_add(1, Ordering::SeqCst);
        }
        value
    }

    async fn put(
        &self,
        scope: &String,
        modality: InputModality,
        attachment: &ResolvedAttachment,
        text: &str,
    ) {
        self.puts
            .lock()
            .unwrap()
            .push((modality, attachment.name.clone()));
        let key = format!("{scope}:{modality:?}:{}", attachment.name);
        self.entries.lock().unwrap().insert(key, text.to_owned());
    }
}

fn valid_png() -> Vec<u8> {
    STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==")
        .unwrap()
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

#[derive(Default)]
struct RecordingHistory {
    unavailable: Mutex<Vec<(InputModality, String, String)>>,
}

impl MediaHistoryFallback<String> for RecordingHistory {
    fn omitted(&self, _scope: &String, _modality: InputModality, _source: &str) -> String {
        "[Earlier attachment omitted by history budget]".into()
    }

    fn unavailable(
        &self,
        _scope: &String,
        modality: InputModality,
        source: &str,
        error: &str,
    ) -> Option<String> {
        self.unavailable
            .lock()
            .unwrap()
            .push((modality, source.to_string(), error.to_string()));
        Some("[Earlier attachment unavailable: safe-hint]".into())
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
        resolver.clone(),
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
async fn typed_mime_hints_are_forwarded_per_block_but_markers_have_none() {
    let model = Arc::new(CaptureModel::new(false));
    let resolver = Arc::new(Resolver::default());
    resolver.sources.insert(
        "shared.bin".into(),
        attachment("shared.bin", "text/plain", b"content"),
    );
    let wrapped = wrapper(
        model,
        MediaRequestConfig::default(),
        resolver.clone(),
        Arc::default(),
    );
    let typed = ModelRequest::new(vec![Message::User(UserMessage {
        content: vec![
            ContentBlock::Document(MediaRef::Path {
                path: "shared.bin".into(),
                media_type: Some("application/pdf".into()),
            }),
            ContentBlock::Document(MediaRef::Path {
                path: "shared.bin".into(),
                media_type: Some("text/markdown".into()),
            }),
        ],
    })]);

    wrapped.invoke(&"tenant-a".into(), typed).await.unwrap();
    let marker = wrapper(
        Arc::new(CaptureModel::new(false)),
        MediaRequestConfig::default(),
        resolver.clone(),
        Arc::default(),
    );
    let stream = marker
        .stream(&"tenant-a".into(), request("[FILE:shared.bin]"))
        .await
        .unwrap();
    let _: Vec<ModelStreamItem> = stream.collect().await;

    assert_eq!(
        *resolver.mime_hints.lock().unwrap(),
        vec![
            Some("application/pdf".into()),
            Some("text/markdown".into()),
            None,
        ]
    );
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
async fn fallback_renderer_handles_markers_and_typed_media_for_invoke_and_stream() {
    let model = Arc::new(CaptureModel::new(false));
    let resolver = Arc::new(Resolver {
        sources: HashMap::from([
            (
                "image".into(),
                attachment("image.png", "image/png", &valid_png()),
            ),
            (
                "document".into(),
                attachment("document.pdf", "application/pdf", b"pdf"),
            ),
            (
                "notes".into(),
                attachment("notes.txt", "text/plain", b"notes"),
            ),
            ("audio".into(), attachment("voice.wav", "audio/wav", b"wav")),
        ]),
        annotations: HashMap::from([
            ("image".into(), vec!["image note".into()]),
            ("notes".into(), vec!["file note".into()]),
            ("document".into(), vec!["document note".into()]),
        ]),
        ..Resolver::default()
    });
    let renderer = Arc::new(RecordingFallback::default());
    let wrapped = wrapper(
        model.clone(),
        MediaRequestConfig::default(),
        resolver,
        Arc::default(),
    )
    .with_fallback_renderer(renderer.clone());

    wrapped
        .invoke(
            &"tenant-a".into(),
            request("caption [IMAGE:image] [FILE:notes]"),
        )
        .await
        .unwrap();
    let typed = ModelRequest::new(vec![Message::User(UserMessage {
        content: vec![ContentBlock::Document(MediaRef::Path {
            path: "document".into(),
            media_type: Some("application/pdf".into()),
        })],
    })]);
    let stream = wrapped.stream(&"tenant-b".into(), typed).await.unwrap();
    let _: Vec<ModelStreamItem> = stream.collect().await;

    let calls = renderer.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert_eq!(
        calls[0],
        ("tenant-a".into(), InputModality::Image, "image.png".into())
    );
    assert_eq!(
        calls[1],
        (
            "tenant-a".into(),
            InputModality::Document,
            "notes.txt".into()
        )
    );
    assert_eq!(
        calls[2],
        (
            "tenant-b".into(),
            InputModality::Document,
            "document.pdf".into()
        )
    );
    let requests = model.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .all(|request| request.messages[0].text().contains("host fallback"))
    );
    assert_eq!(
        requests[0].messages[0].text().matches("image note").count(),
        1
    );
    assert_eq!(
        requests[0].messages[0].text().matches("file note").count(),
        1
    );
    assert_eq!(
        requests[1].messages[0]
            .text()
            .matches("document note")
            .count(),
        1
    );
}

#[tokio::test]
async fn typed_image_fallback_keeps_annotations_once_for_custom_and_default_renderers() {
    let resolver = Arc::new(Resolver {
        sources: HashMap::from([(
            "image".into(),
            attachment("image.png", "image/png", &valid_png()),
        )]),
        annotations: HashMap::from([("image".into(), vec!["safe image source".into()])]),
        ..Resolver::default()
    });
    let typed_image = || {
        ModelRequest::new(vec![Message::User(UserMessage {
            content: vec![ContentBlock::Image(ImageRef {
                url: "image".into(),
                mime_type: Some("image/png".into()),
            })],
        })])
    };
    let custom_model = Arc::new(CaptureModel::new(false));
    let custom = wrapper(
        custom_model.clone(),
        MediaRequestConfig::default(),
        resolver.clone(),
        Arc::default(),
    )
    .with_fallback_renderer(Arc::new(RecordingFallback::default()));
    custom
        .invoke(&"tenant-a".into(), typed_image())
        .await
        .unwrap();
    assert_eq!(
        custom_model.requests.lock().unwrap()[0].messages[0]
            .text()
            .matches("safe image source")
            .count(),
        1
    );

    let default_model = Arc::new(CaptureModel::new(false));
    let default = wrapper(
        default_model.clone(),
        MediaRequestConfig::default(),
        resolver,
        Arc::default(),
    )
    .with_fallback_renderer(Arc::new(RecordingFallback {
        calls: Mutex::default(),
        fail: false,
        return_none: true,
    }));
    default
        .invoke(&"tenant-a".into(), typed_image())
        .await
        .unwrap();
    assert_eq!(
        default_model.requests.lock().unwrap()[0].messages[0]
            .text()
            .matches("safe image source")
            .count(),
        1
    );
}

#[tokio::test]
async fn fallback_renderer_runs_only_after_authorization_limits_and_native_check() {
    let model = Arc::new(CaptureModel::new(true));
    let resolver = Arc::new(Resolver {
        sources: HashMap::from([(
            "image".into(),
            attachment("image.png", "image/png", &valid_png()),
        )]),
        ..Resolver::default()
    });
    let renderer = Arc::new(RecordingFallback::default());
    let wrapped = wrapper(
        model,
        MediaRequestConfig::default(),
        resolver.clone(),
        Arc::default(),
    )
    .with_fallback_renderer(renderer.clone());
    wrapped
        .invoke(&"tenant-a".into(), request("[IMAGE:image]"))
        .await
        .unwrap();
    assert!(renderer.calls.lock().unwrap().is_empty());

    let no_remote = wrapper(
        Arc::new(CaptureModel::new(false)),
        MediaRequestConfig::default(),
        resolver.clone(),
        Arc::default(),
    )
    .with_fallback_renderer(renderer.clone());
    assert!(
        no_remote
            .invoke(
                &"tenant-a".into(),
                request("[IMAGE:https://files.invalid/image.png]"),
            )
            .await
            .is_err()
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert!(renderer.calls.lock().unwrap().is_empty());

    let oversized_resolver = Arc::new(Resolver {
        sources: HashMap::from([(
            "large".into(),
            attachment("large.png", "image/png", &vec![0; 1024 * 1024 + 1]),
        )]),
        ..Resolver::default()
    });
    let oversized = wrapper(
        Arc::new(CaptureModel::new(false)),
        MediaRequestConfig {
            images: ImageLimits {
                max_image_size_mb: 1,
                ..ImageLimits::default()
            },
            ..MediaRequestConfig::default()
        },
        oversized_resolver.clone(),
        Arc::default(),
    )
    .with_fallback_renderer(renderer.clone());
    assert!(
        oversized
            .invoke(&"tenant-a".into(), request("[IMAGE:large]"))
            .await
            .is_err()
    );
    assert_eq!(oversized_resolver.calls.load(Ordering::SeqCst), 1);
    assert!(renderer.calls.lock().unwrap().is_empty());

    let invalid_resolver = Arc::new(Resolver {
        sources: HashMap::from([(
            "invalid".into(),
            attachment("invalid.png", "image/x-invalid", b"not an image"),
        )]),
        ..Resolver::default()
    });
    let invalid = wrapper(
        Arc::new(CaptureModel::new(false)),
        MediaRequestConfig::default(),
        invalid_resolver,
        Arc::default(),
    )
    .with_fallback_renderer(renderer.clone());
    assert!(
        invalid
            .invoke(&"tenant-a".into(), request("[IMAGE:invalid]"))
            .await
            .is_err()
    );
    assert!(renderer.calls.lock().unwrap().is_empty());

    let missing_resolver = Arc::new(Resolver::default());
    let missing = wrapper(
        Arc::new(CaptureModel::new(false)),
        MediaRequestConfig::default(),
        missing_resolver.clone(),
        Arc::default(),
    )
    .with_fallback_renderer(renderer.clone());
    assert!(
        missing
            .invoke(&"tenant-a".into(), request("[IMAGE:missing]"))
            .await
            .is_err()
    );
    assert_eq!(missing_resolver.calls.load(Ordering::SeqCst), 1);
    assert!(renderer.calls.lock().unwrap().is_empty());

    let denied = wrapper(
        Arc::new(CaptureModel::new(false)),
        MediaRequestConfig {
            images: ImageLimits {
                max_images: 1,
                ..ImageLimits::default()
            },
            ..MediaRequestConfig::default()
        },
        resolver,
        Arc::default(),
    )
    .with_fallback_renderer(renderer.clone());
    assert!(
        denied
            .invoke(&"tenant-a".into(), request("[IMAGE:image] [IMAGE:image]"))
            .await
            .is_err()
    );
    assert!(renderer.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn fallback_renderer_errors_propagate_and_default_fallback_stays_available() {
    let resolver = Arc::new(Resolver {
        sources: HashMap::from([(
            "image".into(),
            attachment("image.png", "image/png", &valid_png()),
        )]),
        ..Resolver::default()
    });
    let failing = wrapper(
        Arc::new(CaptureModel::new(false)),
        MediaRequestConfig::default(),
        resolver.clone(),
        Arc::default(),
    )
    .with_fallback_renderer(Arc::new(RecordingFallback {
        calls: Mutex::default(),
        fail: true,
        return_none: false,
    }));
    assert!(
        failing
            .invoke(&"tenant-a".into(), request("[IMAGE:image]"))
            .await
            .unwrap_err()
            .to_string()
            .contains("host rendering failed")
    );

    let default = wrapper(
        Arc::new(CaptureModel::new(false)),
        MediaRequestConfig::default(),
        resolver.clone(),
        Arc::default(),
    );
    default
        .invoke(&"tenant-a".into(), request("[IMAGE:image]"))
        .await
        .unwrap();

    let none_renderer = wrapper(
        Arc::new(CaptureModel::new(false)),
        MediaRequestConfig::default(),
        resolver.clone(),
        Arc::default(),
    )
    .with_fallback_renderer(Arc::new(RecordingFallback {
        calls: Mutex::default(),
        fail: false,
        return_none: true,
    }));
    none_renderer
        .invoke(&"tenant-a".into(), request("[IMAGE:image]"))
        .await
        .unwrap();
}

#[tokio::test]
async fn default_fallback_uses_host_cache_and_skips_failed_archive_output() {
    let resolver = Arc::new(Resolver {
        sources: HashMap::from([
            (
                "archive".into(),
                attachment("bad.zip", "application/zip", b"not an archive"),
            ),
            (
                "text".into(),
                attachment("notes.txt", "text/plain", b"notes"),
            ),
        ]),
        annotations: HashMap::from([("text".into(), vec!["cache label".into()])]),
        ..Resolver::default()
    });
    let cache = Arc::new(RecordingFallbackCache::default());
    let model = Arc::new(CaptureModel::new(false));
    let wrapped = wrapper(
        model.clone(),
        MediaRequestConfig {
            files: FileLimits {
                max_extracted_text_chars: 1_000,
                ..FileLimits::default()
            },
            ..MediaRequestConfig::default()
        },
        resolver,
        Arc::default(),
    )
    .with_fallback_cache(cache.clone());
    wrapped
        .invoke(&"tenant-a".into(), request("[FILE:archive] [FILE:text]"))
        .await
        .unwrap();
    wrapped
        .invoke(&"tenant-a".into(), request("[FILE:text]"))
        .await
        .unwrap();
    wrapped
        .invoke(&"tenant-b".into(), request("[FILE:text]"))
        .await
        .unwrap();
    cache
        .entries
        .lock()
        .unwrap()
        .insert("tenant-c:Document:notes.txt".into(), "🧪".repeat(1_100));
    wrapped
        .invoke(&"tenant-c".into(), request("[FILE:text]"))
        .await
        .unwrap();

    let puts = cache.puts.lock().unwrap();
    assert_eq!(puts.len(), 2);
    assert_eq!(puts[0], (InputModality::Document, "notes.txt".into()));
    assert_eq!(cache.gets.load(Ordering::SeqCst), 5);
    assert_eq!(cache.hits.load(Ordering::SeqCst), 2);
    let requests = model.requests.lock().unwrap();
    assert!(requests[1].messages[0].text().contains("notes"));
    assert_eq!(
        requests[1].messages[0]
            .text()
            .matches("cache label")
            .count(),
        1
    );
    assert!(
        requests[3].messages[0]
            .text()
            .contains("Attachment context truncated")
    );
    assert_eq!(
        requests[3].messages[0]
            .text()
            .matches("cache label")
            .count(),
        1
    );
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
        Arc::new(NoTextExtractor),
    );
    let canonical_after_cancel = serde_json::to_value(&original).unwrap();
    let call = {
        let wrapped = canceled_wrapper;
        let request = original.clone();
        tokio::spawn(async move {
            let tenant = "tenant-a".to_string();
            wrapped.invoke(&tenant, request).await
        })
    };
    pending.entered.notified().await;
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert_eq!(
        serde_json::to_value(&original).unwrap(),
        canonical_after_cancel
    );
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
    sources.insert(
        "image".into(),
        attachment("img.png", "image/png", b"\x89PNG\r\n\x1a\n"),
    );
    let resolver = Arc::new(Resolver {
        sources,
        ..Resolver::default()
    });
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
        Arc::new(NoTextExtractor),
    );
    let operator = "same-operator".to_string();
    let mut first = request("[IMAGE:image]");
    first.metadata = serde_json::json!({"trusted_scope":"workspace-a"});
    let mut second = request("[IMAGE:image]");
    second.metadata = serde_json::json!({"trusted_scope":"workspace-b"});

    let (a, b) = tokio::join!(
        wrapped.invoke(&operator, first),
        wrapped.invoke(&operator, second)
    );

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
        sources.insert(
            source.into(),
            attachment("img.png", "image/png", b"\x89PNG\r\n\x1a\n"),
        );
    }
    let resolver = Arc::new(Resolver {
        sources,
        ..Resolver::default()
    });
    let config = MediaRequestConfig {
        max_historical_media_items: 1,
        ..MediaRequestConfig::default()
    };
    let wrapped = wrapper(model.clone(), config, resolver.clone(), Arc::default());
    let historical_request = ModelRequest {
        messages: vec![
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
        ],
        ..ModelRequest::default()
    };

    wrapped
        .invoke(&"tenant-a".into(), historical_request)
        .await
        .unwrap();

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
async fn historical_marker_budget_selects_only_the_newest_references_in_a_text_block() {
    let model = Arc::new(CaptureModel::new(true));
    let mut sources = HashMap::new();
    for source in ["older", "newer"] {
        sources.insert(
            source.into(),
            attachment("img.png", "image/png", b"\x89PNG\r\n\x1a\n"),
        );
    }
    let resolver = Arc::new(Resolver {
        sources,
        ..Resolver::default()
    });
    let wrapped = wrapper(
        model.clone(),
        MediaRequestConfig {
            max_historical_media_items: 1,
            ..MediaRequestConfig::default()
        },
        resolver.clone(),
        Arc::default(),
    );
    let historical_request = ModelRequest {
        messages: vec![
            Message::User(UserMessage {
                content: vec![ContentBlock::Text("[IMAGE:older] [IMAGE:newer]".into())],
            }),
            Message::User(UserMessage {
                content: vec![ContentBlock::Text("latest".into())],
            }),
        ],
        ..ModelRequest::default()
    };

    wrapped
        .invoke(&"tenant-a".into(), historical_request)
        .await
        .unwrap();

    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        resolver.resolved_sources.lock().unwrap().as_slice(),
        ["newer"]
    );
    let captured = model.requests.lock().unwrap();
    let Message::User(old_user) = &captured[0].messages[0] else {
        panic!("historical message retained")
    };
    assert!(matches!(
        &old_user.content[0],
        ContentBlock::Text(text) if text.contains("omitted by history budget")
    ));
    assert!(
        old_user
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Image(_)))
    );
}

#[tokio::test]
async fn trusted_preparer_normalization_is_counted_before_resolution() {
    let model = Arc::new(CaptureModel::new(true));
    let resolver = Arc::new(Resolver::default());
    let config = MediaRequestConfig {
        images: ImageLimits {
            max_images: 1,
            ..ImageLimits::default()
        },
        ..MediaRequestConfig::default()
    };
    let wrapped = MediaRequestModel::new(
        model,
        config,
        |_state: &String, request: &mut ModelRequest| {
            if let Some(metadata) = request.metadata.as_object_mut() {
                metadata.remove("trusted_scope");
            }
            if let Some(Message::User(user)) = request.messages.last_mut() {
                user.content
                    .push(ContentBlock::Text("[IMAGE:one] [IMAGE:two]".into()));
            }
            Ok("trusted-workspace".to_string())
        },
        resolver.clone(),
        Arc::new(NoTextExtractor),
    );
    let mut original = request("caption");
    original.metadata = serde_json::json!({"trusted_scope":"workspace-a"});
    let canonical = original.clone();

    let error = wrapped
        .invoke(&"tenant-a".into(), original.clone())
        .await
        .expect_err("normalized markers count toward the current-turn limit");

    assert!(error.to_string().contains("max=1, found=2"));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    assert_eq!(original.messages, canonical.messages);
    assert_eq!(original.metadata, canonical.metadata);
}

#[tokio::test]
async fn trusted_preparer_consumes_carrier_when_request_has_no_user_message() {
    let model = Arc::new(CaptureModel::new(false));
    let resolver = Arc::new(Resolver::default());
    let wrapped = MediaRequestModel::new(
        model.clone(),
        MediaRequestConfig::default(),
        |_state: &String, request: &mut ModelRequest| {
            if let Some(metadata) = request.metadata.as_object_mut() {
                metadata.remove("trusted_scope");
            }
            Ok("trusted-workspace".to_string())
        },
        resolver,
        Arc::new(NoTextExtractor),
    );
    let original = ModelRequest {
        metadata: serde_json::json!({"trusted_scope":"workspace-a"}),
        ..ModelRequest::default()
    };

    wrapped.invoke(&"tenant-a".into(), original).await.unwrap();

    assert!(
        model.requests.lock().unwrap()[0]
            .metadata
            .get("trusted_scope")
            .is_none()
    );
}

#[tokio::test]
async fn resolver_annotations_survive_next_to_native_media_only_after_success() {
    let model = Arc::new(CaptureModel::new(true));
    let resolver = Arc::new(Resolver {
        sources: HashMap::from([(
            "recovered-image".into(),
            attachment("image.png", "image/png", b"\x89PNG\r\n\x1a\n"),
        )]),
        annotations: HashMap::from([(
            "recovered-image".into(),
            vec!["[Recovered attachment: image/png; workspace path: image.png]".into()],
        )]),
        ..Resolver::default()
    });
    let wrapped = wrapper(
        model.clone(),
        MediaRequestConfig::default(),
        resolver.clone(),
        Arc::default(),
    );

    wrapped
        .invoke(&"tenant-a".into(), request("[IMAGE:recovered-image]"))
        .await
        .unwrap();

    {
        let captured = model.requests.lock().unwrap();
        let Message::User(user) = &captured[0].messages[0] else {
            panic!("prepared user message retained")
        };
        assert!(matches!(
            &user.content[0],
            ContentBlock::Text(text) if text.contains("Recovered attachment")
        ));
        assert!(matches!(
            &user.content[1],
            ContentBlock::Image(image) if image.url.starts_with("data:image/png;base64,")
        ));
    }

    let missing_model = Arc::new(CaptureModel::new(true));
    let missing_resolver = Arc::new(Resolver {
        annotations: HashMap::from([(
            "missing-image".into(),
            vec!["[Recovered attachment: fake label]".into()],
        )]),
        ..Resolver::default()
    });
    let missing_wrapper = wrapper(
        missing_model.clone(),
        MediaRequestConfig::default(),
        missing_resolver,
        Arc::default(),
    );
    assert!(
        missing_wrapper
            .invoke(&"tenant-a".into(), request("[IMAGE:missing-image]"))
            .await
            .is_err()
    );
    assert!(missing_model.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_history_media_degrades_safely_but_missing_latest_media_fails() {
    let model = Arc::new(CaptureModel::new(true));
    let resolver = Arc::new(Resolver::default());
    let wrapped = wrapper(
        model.clone(),
        MediaRequestConfig::default(),
        resolver,
        Arc::default(),
    )
    .with_history_fallback(Arc::new(SafeHistory));
    let history = ModelRequest {
        messages: vec![
            Message::User(UserMessage {
                content: vec![ContentBlock::Image(ImageRef {
                    url: "/private-value/also-private".into(),
                    mime_type: Some("image/png".into()),
                })],
            }),
            Message::User(UserMessage {
                content: vec![ContentBlock::Text("latest text".into())],
            }),
        ],
        ..ModelRequest::default()
    };

    wrapped.invoke(&"tenant-a".into(), history).await.unwrap();
    {
        let calls = model.requests.lock().unwrap();
        let Message::User(old_user) = &calls[0].messages[0] else {
            panic!("historical message retained")
        };
        let ContentBlock::Text(placeholder) = &old_user.content[0] else {
            panic!("missing historical media becomes text")
        };
        assert!(placeholder.contains("safe-hint"));
        assert!(!placeholder.contains("private-value"));
    }

    let error = wrapped
        .invoke(&"tenant-a".into(), request("[IMAGE:latest-missing]"))
        .await
        .expect_err("latest attachment failures remain fatal");
    assert!(
        error
            .to_string()
            .contains("missing test source latest-missing")
    );
}

#[tokio::test]
async fn historical_text_recovers_each_failed_marker_without_losing_prose_or_valid_media() {
    let model = Arc::new(CaptureModel::new(true));
    let mut sources = HashMap::new();
    sources.insert(
        "good-image".into(),
        attachment("good.png", "image/png", b"\x89PNG\r\n\x1a\n"),
    );
    let resolver = Arc::new(Resolver {
        sources,
        ..Resolver::default()
    });
    let history = Arc::new(RecordingHistory::default());
    let wrapped = wrapper(
        model.clone(),
        MediaRequestConfig {
            images: ImageLimits {
                allow_remote_fetch: false,
                ..ImageLimits::default()
            },
            ..MediaRequestConfig::default()
        },
        resolver,
        Arc::default(),
    )
    .with_history_fallback(history.clone());
    let historical_request = ModelRequest {
        messages: vec![
            Message::User(UserMessage {
                content: vec![ContentBlock::Text(
                    "Caption [IMAGE:good-image] then [IMAGE:https://example.test/photo?token=secret]"
                        .into(),
                )],
            }),
            Message::User(UserMessage {
                content: vec![ContentBlock::Text("latest text".into())],
            }),
        ],
        ..ModelRequest::default()
    };

    wrapped
        .invoke(&"tenant-a".into(), historical_request)
        .await
        .unwrap();

    {
        let captured = model.requests.lock().unwrap();
        let Message::User(old_user) = &captured[0].messages[0] else {
            panic!("historical message retained")
        };
        let rendered = old_user
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains("Caption"));
        assert!(rendered.contains("safe-hint"));
        assert!(!rendered.contains("token=secret"));
        assert!(
            old_user
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::Image(_)))
        );
    }
    assert_eq!(
        history.unavailable.lock().unwrap().as_slice(),
        &[(
            InputModality::Image,
            "https://example.test/photo?token=secret".into(),
            "model error: remote attachment fetch is disabled".into(),
        )]
    );

    let error = wrapped
        .invoke(
            &"tenant-a".into(),
            request("[IMAGE:https://example.test/photo?token=secret]"),
        )
        .await
        .expect_err("the same source failure remains fatal in the latest user turn");
    assert!(
        error
            .to_string()
            .contains("remote attachment fetch is disabled")
    );
    assert_eq!(history.unavailable.lock().unwrap().len(), 1);
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
#[cfg(feature = "multimodal")]
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

#[tokio::test]
async fn image_and_archive_fallback_context_is_unicode_safe_and_bounded() {
    let mut bytes = std::io::Cursor::new(Vec::new());
    {
        let mut zip = zip::ZipWriter::new(&mut bytes);
        for index in 0..40 {
            let name = format!("entries/{index:02}-{}.txt", "long-name".repeat(12));
            zip.start_file(name, zip::write::SimpleFileOptions::default())
                .unwrap();
            zip.write_all(b"entry bytes are not extracted").unwrap();
        }
        zip.finish().unwrap();
    }
    let model = Arc::new(CaptureModel::new(false));
    let mut sources = HashMap::new();
    sources.insert(
        "image".into(),
        attachment("photo.png", "image/png", b"\x89PNG\r\n\x1a\n"),
    );
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
        MediaRequestConfig {
            files: FileLimits {
                max_extracted_text_chars: 1_000,
                ..FileLimits::default()
            },
            ..MediaRequestConfig::default()
        },
        resolver,
        Arc::default(),
    )
    .with_image_readout(Arc::new(LongUnicodeReadout));
    let req = ModelRequest {
        messages: vec![Message::User(UserMessage {
            content: vec![
                ContentBlock::Image(ImageRef {
                    url: "image".into(),
                    mime_type: Some("image/png".into()),
                }),
                ContentBlock::Document(MediaRef::path("archive")),
            ],
        })],
        ..ModelRequest::default()
    };

    wrapped.invoke(&"tenant-a".into(), req).await.unwrap();

    let captured = model.requests.lock().unwrap();
    let Message::User(user) = &captured[0].messages[0] else {
        panic!("prepared user message retained")
    };
    let contexts = user
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(text),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(contexts.len(), 2);
    assert!(
        contexts
            .iter()
            .all(|text| text.contains("Attachment context truncated"))
    );
    assert!(
        contexts
            .iter()
            .all(|text| text.chars().count() <= 1_000 + 31)
    );
    assert!(contexts.iter().any(|text| text.contains("🧪")));
    assert!(
        contexts
            .iter()
            .any(|text| text.contains("Archive listing (names are untrusted"))
    );
}

#[tokio::test]
async fn file_fallback_limit_applies_per_attachment_not_across_the_turn() {
    let model = Arc::new(CaptureModel::new(false));
    let mut sources = HashMap::new();
    sources.insert(
        "first".into(),
        attachment("first.txt", "text/plain", "FIRST ".repeat(150).as_bytes()),
    );
    sources.insert(
        "second".into(),
        attachment("second.txt", "text/plain", "SECOND ".repeat(120).as_bytes()),
    );
    let resolver = Arc::new(Resolver {
        sources,
        ..Resolver::default()
    });
    let wrapped = wrapper(
        model.clone(),
        MediaRequestConfig {
            files: FileLimits {
                max_extracted_text_chars: 1_000,
                ..FileLimits::default()
            },
            ..MediaRequestConfig::default()
        },
        resolver,
        Arc::default(),
    );

    wrapped
        .invoke(&"tenant-a".into(), request("[FILE:first] [FILE:second]"))
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
    assert!(rendered.contains("first.txt"));
    assert!(rendered.contains("second.txt"));
    assert!(rendered.contains("FIRST"));
    assert!(rendered.contains("SECOND"));
    assert!(rendered.chars().count() > 1_000);
}
