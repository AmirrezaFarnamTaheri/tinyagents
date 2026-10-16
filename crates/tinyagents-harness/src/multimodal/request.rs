//! Prepare a disposable model request by resolving its user media references.
//!
//! The wrapper owns generic marker counting, bounded history, capability
//! checks, payload rendering, document extraction hooks, and non-vision text
//! fallbacks. Hosts provide the scope and source resolver because only a host
//! can authorize a path, fetch a remote source, or decide which credentials
//! apply. Scope comes from model state or trusted host request metadata and is
//! never inferred from message text or a media reference.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use tinyinference_llm::{
    message::{ContentBlock, ImageRef, MediaRef, Message},
    model::{
        ChatModel, InputModality, InputSource, ModelProfile, ModelRequest, ModelResponse,
        ModelStream,
    },
};

use super::{
    ArchiveFormat, FileLimits, FilePayload, ImageLimits, ResolvedAttachment, TextExtractor,
    archive::{ArchiveLimits, inspect_archive},
    data_uri::encode_data_uri,
    markers::{parse_file_markers, parse_image_markers},
    mime::{detect_image_mime, is_extractable_text_mime},
    payload::compose_multimodal_message,
};

/// Per-turn attachment limits copied from host configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaRequestConfig {
    /// Image count, size, and remote-fetch limits.
    pub images: ImageLimits,
    /// File count, size, extraction, and remote-fetch limits.
    pub files: FileLimits,
    /// Maximum historical media items to resolve, newest first. Each media
    /// marker and typed media block counts as one item.
    pub max_historical_media_items: usize,
}

impl Default for MediaRequestConfig {
    fn default() -> Self {
        Self {
            images: ImageLimits::default(),
            files: FileLimits::default(),
            max_historical_media_items: 8,
        }
    }
}

/// Resolve one media source after the host has applied its access policy.
///
/// Implementations own all path authorization and source access. `max_bytes`
/// is an additional upper bound the resolver must honor before allocating or
/// returning the body. `scope` is produced once for this invocation from the
/// immutable model state and is shared only by that invocation.
#[async_trait]
pub trait MediaSourceResolver<Scope>: Send + Sync {
    /// Resolve a source into bounded bytes and display metadata.
    async fn resolve(
        &self,
        scope: &Scope,
        modality: InputModality,
        source: &str,
        max_bytes: usize,
    ) -> std::result::Result<ResolvedMedia, String>;
}

/// A resolved attachment plus host-authored context to preserve beside it.
///
/// Hosts can use `annotations` for safe workspace hints or recovered-source
/// labels. The wrapper emits them before native media and with text fallbacks;
/// failed resolutions cannot produce annotations. The host must not put
/// credentials, signed URLs, or resolver errors in this context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMedia {
    /// The bounded attachment bytes and display metadata.
    pub attachment: ResolvedAttachment,
    /// Safe host-authored labels to keep alongside the attachment.
    pub annotations: Vec<String>,
}

impl ResolvedMedia {
    /// Wrap an attachment without additional host context.
    pub fn new(attachment: ResolvedAttachment) -> Self {
        Self {
            attachment,
            annotations: Vec::new(),
        }
    }

    /// Add one host-authored context line.
    pub fn with_annotation(mut self, annotation: impl Into<String>) -> Self {
        self.annotations.push(annotation.into());
        self
    }
}

/// Produce a text description for an image when the selected model cannot
/// accept image input. Model selection and any credentials remain host-owned.
#[async_trait]
pub trait MediaImageReadout<Scope>: Send + Sync {
    /// Return a faithful image description for a host-authorized image.
    async fn readout(
        &self,
        scope: &Scope,
        image: &ResolvedAttachment,
    ) -> std::result::Result<String, String>;
}

/// Host-specific safe text for media omitted from or unavailable in history.
///
/// An implementation chooses which old failures are recoverable and may
/// render a workspace-relative hint. It should not echo remote URLs, signed
/// query tokens, or arbitrary resolver errors. Returning `None` keeps the
/// historical failure fatal.
pub trait MediaHistoryFallback<Scope>: Send + Sync {
    /// Render a historical item outside the bounded history budget.
    fn omitted(&self, scope: &Scope, modality: InputModality, source: &str) -> String;

    /// Render a recoverable historical resolution failure, or return `None`.
    fn unavailable(
        &self,
        scope: &Scope,
        modality: InputModality,
        source: &str,
        error: &str,
    ) -> Option<String>;
}

/// A `ChatModel` decorator that resolves media on a request copy.
pub struct MediaRequestModel<State, Scope> {
    inner: Arc<dyn ChatModel<State>>,
    config: MediaRequestConfig,
    scope_for_request:
        Arc<dyn Fn(&State, &mut ModelRequest) -> std::result::Result<Scope, String> + Send + Sync>,
    resolver: Arc<dyn MediaSourceResolver<Scope>>,
    extractor: Arc<dyn TextExtractor>,
    image_readout: Option<Arc<dyn MediaImageReadout<Scope>>>,
    history_fallback: Option<Arc<dyn MediaHistoryFallback<Scope>>>,
    profile: Option<ModelProfile>,
}

impl<State, Scope> MediaRequestModel<State, Scope>
where
    State: Send + Sync,
    Scope: Send + Sync,
{
    /// Wrap `inner` with generic request preparation.
    ///
    /// The callback returns the caller's already-authorized scope from model
    /// state or trusted host metadata. It receives only the disposable request
    /// copy, so it may consume a host carrier or normalize host-owned markers
    /// without changing the canonical transcript. It runs before media
    /// counting, must not perform source I/O, and is called separately for each
    /// invoke or stream (including requests with no user messages).
    pub fn new(
        inner: Arc<dyn ChatModel<State>>,
        config: MediaRequestConfig,
        scope_for_request: impl Fn(&State, &mut ModelRequest) -> std::result::Result<Scope, String>
        + Send
        + Sync
        + 'static,
        resolver: Arc<dyn MediaSourceResolver<Scope>>,
        extractor: Arc<dyn TextExtractor>,
    ) -> Self {
        let profile = inner.profile().cloned();
        Self {
            inner,
            config,
            scope_for_request: Arc::new(scope_for_request),
            resolver,
            extractor,
            image_readout: None,
            history_fallback: None,
            profile,
        }
    }

    /// Install the host's image-to-text fallback for models without vision.
    pub fn with_image_readout(mut self, readout: Arc<dyn MediaImageReadout<Scope>>) -> Self {
        self.image_readout = Some(readout);
        self
    }

    /// Set selected-model facts while retaining transport support checks.
    pub fn with_profile(mut self, profile: ModelProfile) -> Self {
        self.profile = Some(profile);
        self
    }

    /// Install host policy for bounded historical-media omission and recovery.
    pub fn with_history_fallback(mut self, fallback: Arc<dyn MediaHistoryFallback<Scope>>) -> Self {
        self.history_fallback = Some(fallback);
        self
    }

    async fn prepare(
        &self,
        state: &State,
        mut request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelRequest> {
        // This trusted, synchronous callback may consume per-request metadata
        // or normalize host markers on the disposable copy. Run it before
        // deriving limits and history positions so those checks see the exact
        // request that will reach the transport. It must not perform reads.
        let scope =
            (self.scope_for_request)(state, &mut request).map_err(|error| model_error(&error))?;
        let Some(latest_user) = request
            .messages
            .iter()
            .rposition(|message| matches!(message, Message::User(_)))
        else {
            return Ok(request);
        };

        // Count the host-normalized latest-turn media before calling any
        // source resolver. This keeps configured limits meaningful for reads.
        let (images, files) = count_user_media(&request.messages[latest_user]);
        let (max_images, _) = self.config.images.effective();
        let (max_files, _, _) = self.config.files.effective();
        if self.config.files.files_disabled() && images + files > 0 {
            return Err(model_error("attachments are disabled for this request"));
        }
        if images > max_images {
            return Err(model_error(&format!(
                "image attachment count exceeds configured limit: max={max_images}, found={images}"
            )));
        }
        if files > max_files {
            return Err(model_error(&format!(
                "file attachment count exceeds configured limit: max={max_files}, found={files}"
            )));
        }

        let selected_history = select_history_media(
            &request.messages,
            latest_user,
            self.config.max_historical_media_items,
        );
        for (message_index, message) in request.messages.iter_mut().enumerate() {
            let Message::User(user) = message else {
                continue;
            };
            let mut prepared = Vec::with_capacity(user.content.len());
            for (block_index, block) in std::mem::take(&mut user.content).into_iter().enumerate() {
                let historical = message_index != latest_user && is_media_block(&block);
                let selection = selected_history.get(&(message_index, block_index));
                if historical && self.config.files.files_disabled() {
                    prepared.extend(self.omitted_history(&scope, block));
                    continue;
                }
                if historical
                    && matches!(block, ContentBlock::Text(_))
                    && selection.is_none_or(|selection| {
                        selection.images.iter().all(|selected| !selected)
                            && selection.files.iter().all(|selected| !selected)
                    })
                {
                    prepared.extend(self.omitted_history(&scope, block));
                    continue;
                }
                if historical
                    && !matches!(block, ContentBlock::Text(_))
                    && !selection.is_some_and(|selection| selection.typed)
                {
                    prepared.extend(self.omitted_history(&scope, block));
                    continue;
                }
                let recovery = historical.then(|| media_source_hint(&block)).flatten();
                match block {
                    ContentBlock::Text(text) => {
                        prepared.extend(
                            self.prepare_text(
                                &scope,
                                &text,
                                historical,
                                selection.map(|selection| selection.images.as_slice()),
                                selection.map(|selection| selection.files.as_slice()),
                            )
                            .await?,
                        );
                    }
                    block @ (ContentBlock::Image(_)
                    | ContentBlock::Audio(_)
                    | ContentBlock::Video(_)
                    | ContentBlock::Document(_)) => {
                        match self.prepare_block(&scope, block.clone()).await {
                            Ok(blocks) => prepared.extend(blocks),
                            Err(error) => {
                                if let Some((modality, source)) = recovery.as_ref()
                                    && let Some(placeholder) =
                                        self.recover_history(&scope, *modality, source, &error)
                                {
                                    prepared.push(ContentBlock::Text(placeholder));
                                } else {
                                    return Err(error);
                                }
                            }
                        }
                    }
                    other => prepared.push(other),
                }
            }
            user.content = prepared;
        }
        Ok(request)
    }

    async fn prepare_text(
        &self,
        scope: &Scope,
        text: &str,
        historical: bool,
        selected_images: Option<&[bool]>,
        selected_files: Option<&[bool]>,
    ) -> tinyinference_llm::Result<Vec<ContentBlock>> {
        let (without_images, images) = parse_image_markers(text);
        let (clean_text, files) = parse_file_markers(&without_images);
        if images.is_empty() && files.is_empty() {
            return Ok(vec![ContentBlock::Text(text.to_string())]);
        }

        let mut native_images = Vec::new();
        let mut native_annotations = Vec::new();
        let mut image_fallback = Vec::new();
        for (index, source) in images.into_iter().enumerate() {
            if historical && !selected_images.is_some_and(|selected| selected[index]) {
                image_fallback.push(self.omission(scope, InputModality::Image, &source));
                continue;
            }
            let resolved = match self
                .resolve(
                    scope,
                    InputModality::Image,
                    &source,
                    self.config.images.max_image_bytes(),
                    false,
                )
                .await
            {
                Ok(attachment) => attachment,
                Err(error) if historical => {
                    if let Some(placeholder) =
                        self.recover_history(scope, InputModality::Image, &source, &error)
                    {
                        image_fallback.push(placeholder);
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            let ResolvedMedia {
                attachment,
                annotations,
            } = resolved;
            if self.native(InputModality::Image, &attachment.mime) {
                native_annotations.extend(annotations);
                let mime = validated_image_mime(&attachment)?;
                native_images.push(ContentBlock::Image(ImageRef {
                    url: encode_data_uri(&mime, &attachment.bytes),
                    mime_type: Some(mime),
                }));
            } else {
                image_fallback.push(self.image_fallback(scope, &attachment, &annotations).await?);
            }
        }

        let mut file_payloads = Vec::new();
        let mut file_fallback_text = Vec::new();
        for (index, source) in files.into_iter().enumerate() {
            if historical && !selected_files.is_some_and(|selected| selected[index]) {
                file_fallback_text.push(self.omission(scope, InputModality::Document, &source));
                continue;
            }
            let resolved = match self
                .resolve(
                    scope,
                    InputModality::Document,
                    &source,
                    self.config.files.max_file_bytes(),
                    true,
                )
                .await
            {
                Ok(attachment) => attachment,
                Err(error) if historical => {
                    if let Some(placeholder) =
                        self.recover_history(scope, InputModality::Document, &source, &error)
                    {
                        file_fallback_text.push(placeholder);
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            let ResolvedMedia {
                attachment,
                annotations,
            } = resolved;
            if self.native(InputModality::Document, &attachment.mime) {
                native_annotations.extend(annotations);
                native_images.push(ContentBlock::Document(MediaRef::base64(
                    STANDARD.encode(&attachment.bytes),
                    attachment.mime,
                )));
            } else {
                let (payload, archive_text) = self.file_fallback(&attachment).await;
                file_payloads.push(payload);
                file_fallback_text.extend(annotations);
                if let Some(text) = archive_text {
                    file_fallback_text.push(self.limit_context(text));
                }
            }
        }

        let mut output = Vec::new();
        let mut text_output = compose_multimodal_message(&clean_text, &[], &[]);
        let mut file_context = compose_multimodal_message("", &[], &file_payloads);
        for fallback in file_fallback_text {
            if !file_context.is_empty() {
                file_context.push_str("\n\n");
            }
            file_context.push_str(&fallback);
        }
        if !file_context.is_empty() {
            if !text_output.is_empty() {
                text_output.push_str("\n\n");
            }
            text_output.push_str(&self.limit_context(file_context));
        }
        for fallback in image_fallback {
            if !text_output.is_empty() {
                text_output.push_str("\n\n");
            }
            text_output.push_str(&fallback);
        }
        if !text_output.is_empty() {
            output.push(ContentBlock::Text(text_output));
        }
        output.extend(
            native_annotations
                .into_iter()
                .map(ContentBlock::Text),
        );
        output.extend(native_images);
        Ok(output)
    }

    async fn prepare_block(
        &self,
        scope: &Scope,
        block: ContentBlock,
    ) -> tinyinference_llm::Result<Vec<ContentBlock>> {
        let (modality, source, hinted_mime) = match block {
            ContentBlock::Image(image) => (InputModality::Image, image.url, image.mime_type),
            ContentBlock::Audio(media) => media_source(InputModality::Audio, media)?,
            ContentBlock::Video(media) => media_source(InputModality::Video, media)?,
            ContentBlock::Document(media) => media_source(InputModality::Document, media)?,
            _ => unreachable!("prepare_block only receives media blocks"),
        };
        let max_bytes = match modality {
            InputModality::Image => self.config.images.max_image_bytes(),
            _ => self.config.files.max_file_bytes(),
        };
        let resolved = self
            .resolve(scope, modality, &source, max_bytes, false)
            .await?;
        let ResolvedMedia {
            mut attachment,
            annotations,
        } = resolved;
        if attachment.mime == "application/octet-stream"
            && let Some(mime) = hinted_mime
        {
            attachment.mime = mime;
        }
        if self.native(modality, &attachment.mime) {
            let mut output: Vec<_> = annotations.into_iter().map(ContentBlock::Text).collect();
            output.push(native_block(modality, attachment)?);
            return Ok(output);
        }
        let fallback = match modality {
            InputModality::Image => {
                self.image_fallback(scope, &attachment, &annotations).await?
            }
            _ => {
                let (payload, archive_text) = self.file_fallback(&attachment).await;
                let rendered = compose_multimodal_message("", &[], &[payload]);
                match archive_text {
                    Some(archive) if !rendered.is_empty() => {
                        self.limit_context(format!("{rendered}\n\n{archive}"))
                    }
                    Some(archive) => archive,
                    None => self.limit_context(rendered),
                }
            }
        };
        let fallback = join_annotations(&annotations, &fallback);
        Ok(vec![ContentBlock::Text(self.limit_context(fallback))])
    }

    async fn resolve(
        &self,
        scope: &Scope,
        modality: InputModality,
        source: &str,
        max_bytes: usize,
        require_file_mime: bool,
    ) -> tinyinference_llm::Result<ResolvedMedia> {
        let remote = source.starts_with("http://") || source.starts_with("https://");
        let remote_allowed = match modality {
            InputModality::Image => self.config.images.allow_remote_fetch,
            InputModality::Audio | InputModality::Video | InputModality::Document => {
                self.config.files.allow_remote_fetch
            }
        };
        if remote && !remote_allowed {
            return Err(model_error("remote attachment fetch is disabled"));
        }
        let attachment = self
            .resolver
            .resolve(scope, modality, source, max_bytes)
            .await
            .map_err(|error| model_error(&error))?;
        if attachment.attachment.bytes.len() > max_bytes {
            return Err(model_error("attachment exceeds configured byte limit"));
        }
        if modality == InputModality::Image && attachment.attachment.bytes.is_empty() {
            return Err(model_error("image reference has no bytes"));
        }
        if require_file_mime && !self.config.files.is_mime_allowed(&attachment.attachment.mime) {
            return Err(model_error(&format!(
                "file MIME type '{}' is not allowed; supported: {}",
                attachment.attachment.mime,
                self.config.files.supported_rendered()
            )));
        }
        Ok(attachment)
    }

    fn native(&self, modality: InputModality, mime: &str) -> bool {
        let advertised = self.profile.as_ref().is_some_and(|profile| match modality {
            InputModality::Image => profile.modalities.image_in,
            InputModality::Audio => profile.modalities.audio_in,
            InputModality::Video => profile.modalities.video_in,
            InputModality::Document => profile.modalities.document_in,
        });
        advertised
            && self
                .inner
                .supports_input(modality, mime, InputSource::Base64)
    }

    async fn image_fallback(
        &self,
        scope: &Scope,
        image: &ResolvedAttachment,
        annotations: &[String],
    ) -> tinyinference_llm::Result<String> {
        let mime = validated_image_mime(image)?;
        let details = match &self.image_readout {
            Some(readout) => readout
                .readout(scope, image)
                .await
                .map_err(|error| model_error(&error))?,
            None => "Image contents are unavailable to this text-only model.".to_string(),
        };
        Ok(self.limit_context(join_annotations(annotations, &format!(
            "[Image attachment: {} bytes; mime={mime}]\nImage readout (untrusted):\n{details}",
            image.bytes.len()
        ))))
    }

    async fn file_fallback(
        &self,
        attachment: &ResolvedAttachment,
    ) -> (FilePayload, Option<String>) {
        if let Some(format) =
            ArchiveFormat::detect(&attachment.name, &attachment.mime, &attachment.bytes)
        {
            let text = match inspect_archive(&attachment.bytes, format, &ArchiveLimits::default()) {
                Ok(listing) => {
                    let mut output = String::from(
                        "Archive listing (names are untrusted; original was not extracted):",
                    );
                    for entry in listing.entries {
                        output.push_str(&format!(
                            "\n{:?} {} bytes {}",
                            entry.kind,
                            entry.declared_size,
                            serde_json::to_string(&entry.name).unwrap_or_default()
                        ));
                    }
                    if let Some(truncation) = listing.truncation {
                        output.push_str(&format!("\nListing truncated: {truncation:?}"));
                    }
                    output
                }
                Err(error) => format!("Archive inspection unavailable: {error}"),
            };
            return (
                FilePayload::from_resolved(
                    &attachment.bytes,
                    attachment.name.clone(),
                    attachment.mime.clone(),
                    None,
                    self.config.files.effective().2,
                ),
                Some(self.limit_context(text)),
            );
        }

        let extracted = if is_extractable_text_mime(&attachment.mime)
            || !self.extractor.handles(&attachment.mime)
        {
            None
        } else {
            self.extractor
                .extract(&attachment.mime, &attachment.bytes)
                .await
                .ok()
        };
        (
            FilePayload::from_resolved(
                &attachment.bytes,
                attachment.name.clone(),
                attachment.mime.clone(),
                extracted,
                self.config.files.effective().2,
            ),
            None,
        )
    }

    fn omitted_history(&self, scope: &Scope, block: ContentBlock) -> Vec<ContentBlock> {
        match block {
            ContentBlock::Text(text) => {
                let (without_images, images) = parse_image_markers(&text);
                let (clean, files) = parse_file_markers(&without_images);
                if images.is_empty() && files.is_empty() {
                    return vec![ContentBlock::Text(text)];
                }
                let mut output = clean;
                for source in images {
                    output.push_str("\n\n");
                    output.push_str(&self.omission(scope, InputModality::Image, &source));
                }
                for source in files {
                    output.push_str("\n\n");
                    output.push_str(&self.omission(scope, InputModality::Document, &source));
                }
                vec![ContentBlock::Text(output)]
            }
            other => {
                let Some((modality, source)) = media_source_hint(&other) else {
                    return vec![other];
                };
                vec![ContentBlock::Text(self.omission(scope, modality, &source))]
            }
        }
    }

    fn omission(&self, scope: &Scope, modality: InputModality, source: &str) -> String {
        self.history_fallback.as_ref().map_or_else(
            || "[Earlier attachment omitted by history budget]".to_string(),
            |fallback| fallback.omitted(scope, modality, source),
        )
    }

    fn recover_history(
        &self,
        scope: &Scope,
        modality: InputModality,
        source: &str,
        error: &tinyinference_llm::Error,
    ) -> Option<String> {
        self.history_fallback
            .as_ref()?
            .unavailable(scope, modality, source, &error.to_string())
    }

    fn limit_context(&self, mut text: String) -> String {
        let limit = self.config.files.effective().2;
        if text.chars().count() > limit {
            text = text.chars().take(limit).collect();
            text.push_str("\n[Attachment context truncated]");
        }
        text
    }
}

#[async_trait]
impl<State, Scope> ChatModel<State> for MediaRequestModel<State, Scope>
where
    State: Send + Sync,
    Scope: Send + Sync,
{
    fn profile(&self) -> Option<&ModelProfile> {
        self.profile.as_ref()
    }

    fn supports_input(&self, modality: InputModality, mime: &str, source: InputSource) -> bool {
        self.inner.supports_input(modality, mime, source)
    }

    fn cache_identity(&self) -> Option<String> {
        self.inner.cache_identity()
    }

    async fn invoke(
        &self,
        state: &State,
        request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        self.inner
            .invoke(state, self.prepare(state, request).await?)
            .await
    }

    async fn stream(
        &self,
        state: &State,
        request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelStream> {
        self.inner
            .stream(state, self.prepare(state, request).await?)
            .await
    }

    async fn fetch_deferred(
        &self,
        handle: &tinyinference_llm::model::DeferredHandle,
    ) -> tinyinference_llm::Result<tinyinference_llm::model::DeferredStatus> {
        self.inner.fetch_deferred(handle).await
    }
}

fn is_media_block(block: &ContentBlock) -> bool {
    match block {
        ContentBlock::Text(text) => {
            let (_, images) = parse_image_markers(text);
            let (_, files) = parse_file_markers(text);
            !images.is_empty() || !files.is_empty()
        }
        ContentBlock::Image(_)
        | ContentBlock::Audio(_)
        | ContentBlock::Video(_)
        | ContentBlock::Document(_) => true,
        _ => false,
    }
}

#[derive(Default)]
struct HistorySelection {
    images: Vec<bool>,
    files: Vec<bool>,
    typed: bool,
}

fn select_history_media(
    messages: &[Message],
    latest_user: usize,
    budget: usize,
) -> HashMap<(usize, usize), HistorySelection> {
    let mut selected = HashMap::new();
    let mut remaining = budget;
    for (message_index, message) in messages.iter().enumerate().rev() {
        if message_index == latest_user {
            continue;
        }
        let Message::User(user) = message else {
            continue;
        };
        for (block_index, block) in user.content.iter().enumerate().rev() {
            if !is_media_block(block) {
                continue;
            }
            let selection = match block {
                ContentBlock::Text(text) => {
                    let (_, images) = parse_image_markers(text);
                    let (_, files) = parse_file_markers(text);
                    let mut selection = HistorySelection {
                        images: vec![false; images.len()],
                        files: vec![false; files.len()],
                        typed: false,
                    };
                    for selected in selection.files.iter_mut().rev() {
                        if remaining == 0 {
                            break;
                        }
                        *selected = true;
                        remaining -= 1;
                    }
                    for selected in selection.images.iter_mut().rev() {
                        if remaining == 0 {
                            break;
                        }
                        *selected = true;
                        remaining -= 1;
                    }
                    selection
                }
                _ => HistorySelection {
                    typed: remaining > 0,
                    ..HistorySelection::default()
                },
            };
            if selection.typed || !selection.images.is_empty() || !selection.files.is_empty() {
                if selection.typed {
                    remaining -= 1;
                }
                selected.insert((message_index, block_index), selection);
            }
        }
    }
    selected
}

fn media_source_hint(block: &ContentBlock) -> Option<(InputModality, String)> {
    match block {
        ContentBlock::Text(text) => {
            let (_, images) = parse_image_markers(text);
            if let Some(source) = images.into_iter().next() {
                return Some((InputModality::Image, source));
            }
            let (_, files) = parse_file_markers(text);
            files
                .into_iter()
                .next()
                .map(|source| (InputModality::Document, source))
        }
        ContentBlock::Image(image) => Some((InputModality::Image, image.url.clone())),
        ContentBlock::Audio(media) => media_source_hint_ref(InputModality::Audio, media),
        ContentBlock::Video(media) => media_source_hint_ref(InputModality::Video, media),
        ContentBlock::Document(media) => media_source_hint_ref(InputModality::Document, media),
        _ => None,
    }
}

fn media_source_hint_ref(
    modality: InputModality,
    media: &MediaRef,
) -> Option<(InputModality, String)> {
    let source = match media {
        MediaRef::Path { path, .. } => path,
        MediaRef::Url { url, .. } => url,
        MediaRef::Base64 { data, media_type } => {
            return Some((modality, format!("data:{media_type};base64,{data}")));
        }
    };
    Some((modality, source.clone()))
}

fn count_user_media(message: &Message) -> (usize, usize) {
    let Message::User(user) = message else {
        return (0, 0);
    };
    user.content.iter().fold((0, 0), |(images, files), block| {
        let (image_count, file_count) = match block {
            ContentBlock::Text(text) => {
                let (_, images) = parse_image_markers(text);
                let (_, files) = parse_file_markers(text);
                (images.len(), files.len())
            }
            ContentBlock::Image(_) => (1, 0),
            ContentBlock::Audio(_) | ContentBlock::Video(_) | ContentBlock::Document(_) => (0, 1),
            _ => (0, 0),
        };
        (images + image_count, files + file_count)
    })
}

fn media_source(
    modality: InputModality,
    media: MediaRef,
) -> tinyinference_llm::Result<(InputModality, String, Option<String>)> {
    Ok(match media {
        MediaRef::Path { path, media_type } => (modality, path, media_type),
        MediaRef::Url { url, media_type } => (modality, url, media_type),
        MediaRef::Base64 { data, media_type } => (
            modality,
            format!("data:{media_type};base64,{data}"),
            Some(media_type),
        ),
    })
}

fn native_block(
    modality: InputModality,
    attachment: ResolvedAttachment,
) -> tinyinference_llm::Result<ContentBlock> {
    let mime = if modality == InputModality::Image {
        validated_image_mime(&attachment)?
    } else {
        attachment.mime
    };
    let data = STANDARD.encode(&attachment.bytes);
    Ok(match modality {
        InputModality::Image => ContentBlock::Image(ImageRef {
            url: encode_data_uri(&mime, &attachment.bytes),
            mime_type: Some(mime),
        }),
        InputModality::Audio => ContentBlock::Audio(MediaRef::base64(data, mime)),
        InputModality::Video => ContentBlock::Video(MediaRef::base64(data, mime)),
        InputModality::Document => ContentBlock::Document(MediaRef::base64(data, mime)),
    })
}

fn validated_image_mime(attachment: &ResolvedAttachment) -> tinyinference_llm::Result<String> {
    let mime = detect_image_mime(None, &attachment.bytes, Some(&attachment.mime))
        .ok_or_else(|| model_error("image attachment type cannot be determined"))?;
    if !super::ALLOWED_IMAGE_MIME_TYPES.contains(&mime.as_str()) {
        return Err(model_error("image attachment MIME type is not supported"));
    }
    Ok(mime)
}

fn model_error(message: &str) -> tinyinference_llm::Error {
    tinyinference_llm::Error::Model(message.to_string())
}

#[cfg(test)]
#[path = "request_tests.rs"]
mod tests;
