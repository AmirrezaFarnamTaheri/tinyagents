//! Prepare a disposable model request by resolving its user media references.
//!
//! The wrapper owns generic marker counting, capability checks, payload
//! rendering, document extraction hooks, and non-vision text fallbacks. Hosts
//! provide the scope and source resolver because only a host can authorize a
//! path, fetch a remote source, or decide which credentials apply. Scope is
//! obtained from the immutable `ChatModel` state for each call; it is never
//! inferred from message text or a media reference.

use std::sync::Arc;

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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MediaRequestConfig {
    /// Image count, size, and remote-fetch limits.
    pub images: ImageLimits,
    /// File count, size, extraction, and remote-fetch limits.
    pub files: FileLimits,
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

/// A `ChatModel` decorator that resolves media on a request copy.
pub struct MediaRequestModel<State, Scope> {
    inner: Arc<dyn ChatModel<State>>,
    config: MediaRequestConfig,
    scope_for_state: Arc<dyn Fn(&State) -> std::result::Result<Scope, String> + Send + Sync>,
    resolver: Arc<dyn MediaSourceResolver<Scope>>,
    extractor: Arc<dyn TextExtractor>,
    image_readout: Option<Arc<dyn MediaImageReadout<Scope>>>,
}

impl<State, Scope> MediaRequestModel<State, Scope>
where
    State: Send + Sync,
    Scope: Send + Sync,
{
    /// Wrap `inner` with generic request preparation.
    ///
    /// The scope callback must return the caller's already-authorized scope
    /// from immutable model state. It is called separately for each invoke or
    /// stream, so concurrent calls cannot overwrite one another's scope.
    pub fn new(
        inner: Arc<dyn ChatModel<State>>,
        config: MediaRequestConfig,
        scope_for_state: impl Fn(&State) -> std::result::Result<Scope, String> + Send + Sync + 'static,
        resolver: Arc<dyn MediaSourceResolver<Scope>>,
        extractor: Arc<dyn TextExtractor>,
    ) -> Self {
        Self {
            inner,
            config,
            scope_for_state: Arc::new(scope_for_state),
            resolver,
            extractor,
            image_readout: None,
        }
    }

    /// Install the host's image-to-text fallback for models without vision.
    pub fn with_image_readout(mut self, readout: Arc<dyn MediaImageReadout<Scope>>) -> Self {
        self.image_readout = Some(readout);
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
        let request_has_media = request.messages.iter().any(|message| {
            let (images, files) = count_user_media(message);
            images + files > 0
        });
        if self.config.files.files_disabled() && request_has_media {
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

        let scope = (self.scope_for_state)(state).map_err(|error| model_error(&error))?;
        for message in &mut request.messages {
            let Message::User(user) = message else {
                continue;
            };
            let mut prepared = Vec::with_capacity(user.content.len());
            for block in std::mem::take(&mut user.content) {
                match block {
                    ContentBlock::Text(text) => {
                        prepared.extend(self.prepare_text(&scope, &text).await?);
                    }
                    block @ (ContentBlock::Image(_)
                    | ContentBlock::Audio(_)
                    | ContentBlock::Video(_)
                    | ContentBlock::Document(_)) => {
                        prepared.extend(self.prepare_block(&scope, block).await?);
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
        let mut attachment = self.resolve(scope, modality, &source, max_bytes).await?;
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
        Ok(attachment)
    }

    fn native(&self, modality: InputModality, mime: &str) -> bool {
        let advertised = self.inner.profile().is_some_and(|profile| match modality {
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
}

#[async_trait]
impl<State, Scope> ChatModel<State> for MediaRequestModel<State, Scope>
where
    State: Send + Sync,
    Scope: Send + Sync,
{
    fn profile(&self) -> Option<&ModelProfile> {
        self.inner.profile()
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
