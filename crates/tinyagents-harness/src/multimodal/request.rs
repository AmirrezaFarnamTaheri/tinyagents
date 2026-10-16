//! Prepare a disposable model request by resolving its user media references.
//!
//! The wrapper owns generic marker counting, capability checks, payload
//! rendering, document extraction hooks, and non-vision text fallbacks. Hosts
//! provide the scope and source resolver because only a host can authorize a
//! path, fetch a remote source, or decide which credentials apply. Scope is
//! obtained from the immutable `ChatModel` state for each call; it is never
//! inferred from message text or a media reference.

use std::{collections::HashSet, sync::Arc};

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
    /// Maximum media-bearing historical blocks to resolve, newest first.
    pub max_historical_media_blocks: usize,
}

impl Default for MediaRequestConfig {
    fn default() -> Self {
        Self {
            images: ImageLimits::default(),
            files: FileLimits::default(),
            max_historical_media_blocks: 8,
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
    ) -> std::result::Result<ResolvedAttachment, String>;
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
    /// copy, so it may consume a host carrier without changing the canonical
    /// transcript. It is called separately for each invoke or stream.
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
    pub fn with_history_fallback(
        mut self,
        fallback: Arc<dyn MediaHistoryFallback<Scope>>,
    ) -> Self {
        self.history_fallback = Some(fallback);
        self
    }

    async fn prepare(
        &self,
        state: &State,
        mut request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelRequest> {
        let Some(latest_user) = request
            .messages
            .iter()
            .rposition(|message| matches!(message, Message::User(_)))
        else {
            return Ok(request);
        };

        // Count the raw latest-turn media before deriving scope or calling any
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

        let scope = (self.scope_for_request)(state, &mut request)
            .map_err(|error| model_error(&error))?;
        let selected_history = selected_history_blocks(
            &request.messages,
            latest_user,
            self.config.max_historical_media_blocks,
        );
        for (message_index, message) in request.messages.iter_mut().enumerate() {
            let Message::User(user) = message else {
                continue;
            };
            let mut prepared = Vec::with_capacity(user.content.len());
            for (block_index, block) in std::mem::take(&mut user.content).into_iter().enumerate() {
                let historical = message_index != latest_user && is_media_block(&block);
                if historical && self.config.files.files_disabled() {
                    prepared.extend(self.omitted_history(&scope, block));
                    continue;
                }
                if historical && !selected_history.contains(&(message_index, block_index)) {
                    prepared.extend(self.omitted_history(&scope, block));
                    continue;
                }
                let recovery = historical.then(|| media_source_hint(&block)).flatten();
                match block {
                    ContentBlock::Text(text) => {
                        match self.prepare_text(&scope, &text).await {
                            Ok(blocks) => prepared.extend(blocks),
                            Err(error) => {
                                if let Some((modality, source)) = recovery.as_ref()
                                    && let Some(placeholder) = self.recover_history(
                                        &scope,
                                        *modality,
                                        source,
                                        &error,
                                    )
                                {
                                    prepared.push(ContentBlock::Text(placeholder));
                                } else {
                                    return Err(error);
                                }
                            }
                        }
                    }
                    block @ (ContentBlock::Image(_)
                    | ContentBlock::Audio(_)
                    | ContentBlock::Video(_)
                    | ContentBlock::Document(_)) => {
                        match self.prepare_block(&scope, block.clone()).await {
                            Ok(blocks) => prepared.extend(blocks),
                            Err(error) => {
                                if let Some((modality, source)) = recovery.as_ref()
                                    && let Some(placeholder) = self.recover_history(
                                        &scope,
                                        *modality,
                                        source,
                                        &error,
                                    )
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
    ) -> tinyinference_llm::Result<Vec<ContentBlock>> {
        let (without_images, images) = parse_image_markers(text);
        let (clean_text, files) = parse_file_markers(&without_images);
        if images.is_empty() && files.is_empty() {
            return Ok(vec![ContentBlock::Text(text.to_string())]);
        }

        let mut native_images = Vec::new();
        let mut image_fallback = Vec::new();
        for source in images {
            let attachment = self
                .resolve(
                    scope,
                    InputModality::Image,
                    &source,
                    self.config.images.max_image_bytes(),
                    false,
                )
                .await?;
            if self.native(InputModality::Image, &attachment.mime) {
                let mime = validated_image_mime(&attachment)?;
                native_images.push(ContentBlock::Image(ImageRef {
                    url: encode_data_uri(&mime, &attachment.bytes),
                    mime_type: Some(mime),
                }));
            } else {
                image_fallback.push(self.image_fallback(scope, &attachment).await?);
            }
        }

        let mut file_payloads = Vec::new();
        let mut file_fallback_text = Vec::new();
        for source in files {
            let attachment = self
                .resolve(
                    scope,
                    InputModality::Document,
                    &source,
                    self.config.files.max_file_bytes(),
                    true,
                )
                .await?;
            if self.native(InputModality::Document, &attachment.mime) {
                native_images.push(ContentBlock::Document(MediaRef::base64(
                    STANDARD.encode(&attachment.bytes),
                    attachment.mime,
                )));
            } else {
                let (payload, archive_text) = self.file_fallback(&attachment).await;
                file_payloads.push(payload);
                if let Some(text) = archive_text {
                    file_fallback_text.push(text);
                }
            }
        }

        let composed = compose_multimodal_message(&clean_text, &[], &file_payloads);
        let mut output = Vec::new();
        let mut text_output = composed;
        for fallback in image_fallback.into_iter().chain(file_fallback_text) {
            if !text_output.is_empty() {
                text_output.push_str("\n\n");
            }
            text_output.push_str(&fallback);
        }
        if !text_output.is_empty() {
            output.push(ContentBlock::Text(text_output));
        }
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
        let mut attachment = self
            .resolve(scope, modality, &source, max_bytes, false)
            .await?;
        if attachment.mime == "application/octet-stream"
            && let Some(mime) = hinted_mime
        {
            attachment.mime = mime;
        }
        if self.native(modality, &attachment.mime) {
            return Ok(vec![native_block(modality, attachment)?]);
        }
        let fallback = match modality {
            InputModality::Image => self.image_fallback(scope, &attachment).await?,
            _ => {
                let (payload, archive_text) = self.file_fallback(&attachment).await;
                let rendered = compose_multimodal_message("", &[], &[payload]);
                match archive_text {
                    Some(archive) if !rendered.is_empty() => format!("{rendered}\n\n{archive}"),
                    Some(archive) => archive,
                    None => rendered,
                }
            }
        };
        Ok(vec![ContentBlock::Text(fallback)])
    }

    async fn resolve(
        &self,
        scope: &Scope,
        modality: InputModality,
        source: &str,
        max_bytes: usize,
        require_file_mime: bool,
    ) -> tinyinference_llm::Result<ResolvedAttachment> {
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
        if attachment.bytes.len() > max_bytes {
            return Err(model_error("attachment exceeds configured byte limit"));
        }
        if modality == InputModality::Image && attachment.bytes.is_empty() {
            return Err(model_error("image reference has no bytes"));
        }
        if require_file_mime && !self.config.files.is_mime_allowed(&attachment.mime) {
            return Err(model_error(&format!(
                "file MIME type '{}' is not allowed; supported: {}",
                attachment.mime,
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
    ) -> tinyinference_llm::Result<String> {
        let mime = validated_image_mime(image)?;
        let details = match &self.image_readout {
            Some(readout) => readout
                .readout(scope, image)
                .await
                .map_err(|error| model_error(&error))?,
            None => "Image contents are unavailable to this text-only model.".to_string(),
        };
        Ok(format!(
            "[Image attachment: {} bytes; mime={mime}]\nImage readout (untrusted):\n{details}",
            image.bytes.len()
        ))
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
                Some(text),
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
        self.history_fallback.as_ref()?.unavailable(
            scope,
            modality,
            source,
            &error.to_string(),
        )
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

fn selected_history_blocks(
    messages: &[Message],
    latest_user: usize,
    budget: usize,
) -> HashSet<(usize, usize)> {
    let mut selected = HashSet::new();
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
            let count = match block {
                ContentBlock::Text(text) => {
                    let (_, images) = parse_image_markers(text);
                    let (_, files) = parse_file_markers(text);
                    images.len() + files.len()
                }
                _ => 1,
            };
            if count <= remaining {
                selected.insert((message_index, block_index));
                remaining -= count;
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

fn media_source_hint_ref(modality: InputModality, media: &MediaRef) -> Option<(InputModality, String)> {
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
