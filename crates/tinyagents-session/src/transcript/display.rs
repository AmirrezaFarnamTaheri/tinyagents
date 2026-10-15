//! Display records built from the neutral transcript types.
//!
//! The JSONL reader turns each file line into a [`DisplayMessage`]. A store
//! that does not keep lines (the driver-backed one) holds [`TranscriptMessage`]
//! rows instead; these helpers give it the same display shape by running each
//! row through the same line codec the file writer uses, so a projection reads
//! the two identically.
//!
//! Out-of-band delivery provenance ([`BackgroundOrigin`]) is a line field in
//! JSONL. A [`TranscriptMessage`] carries it in its `extra_metadata` under
//! [`BACKGROUND_METADATA_KEY`] — the JSONL reader lifts the line field in
//! there and the line codec writes it back out as the line field — so a row
//! keeps its provenance through every read and rewrite on either backend;
//! [`display_message_of`] lifts it back out for display.

use super::history::TranscriptPartial;
use super::jsonl::{build_message_line, display_message_from_line, partial_line};
use super::types::{
    BackgroundOrigin, DisplayMessage, DisplayRecord, DisplaySessionTranscript, SessionTranscript,
    TranscriptMessage,
};

/// The `extra_metadata` key a row-keeping store records a background
/// delivery's provenance under.
pub const BACKGROUND_METADATA_KEY: &str = "tinyagents_background";

/// The display form of one stored row, exactly as the file reader would
/// produce it for the line the file writer makes of that row.
pub(crate) fn display_message_of(message: &TranscriptMessage) -> DisplayMessage {
    let mut message = message.clone();
    let background = take_background(&mut message);
    let mut line = build_message_line(
        &message,
        message.turn_usage.as_ref(),
        message.request_id.as_deref(),
        message.interrupted,
    );
    line.background = background;
    display_message_from_line(line)
}

/// The display form of an interrupted turn's partial answer.
pub(crate) fn partial_display_message(
    partial: &TranscriptPartial,
    request_id: Option<&str>,
) -> DisplayMessage {
    display_message_from_line(partial_line(partial, request_id))
}

/// A display projection of `session`'s model context: one message record per
/// row. For a store that keeps no more than the reduced context.
pub(crate) fn display_from_session(session: SessionTranscript) -> DisplaySessionTranscript {
    DisplaySessionTranscript {
        records: session
            .messages
            .iter()
            .map(|message| DisplayRecord::Message(Box::new(display_message_of(message))))
            .collect(),
        meta: session.meta,
    }
}

/// `message` marked as delivered out of band by `origin`, for a store that
/// keeps rows rather than lines.
pub(crate) fn with_background(
    mut message: TranscriptMessage,
    origin: &BackgroundOrigin,
) -> TranscriptMessage {
    message.extra_metadata = metadata_with_background(message.extra_metadata.take(), origin);
    message
}

/// `metadata` with `origin` recorded under [`BACKGROUND_METADATA_KEY`].
pub(crate) fn metadata_with_background(
    metadata: Option<serde_json::Value>,
    origin: &BackgroundOrigin,
) -> Option<serde_json::Value> {
    let mut metadata = match metadata {
        Some(serde_json::Value::Object(map)) => map,
        Some(other) => {
            let mut map = serde_json::Map::new();
            map.insert("value".into(), other);
            map
        }
        None => serde_json::Map::new(),
    };
    metadata.insert(
        BACKGROUND_METADATA_KEY.into(),
        serde_json::to_value(origin).unwrap_or(serde_json::Value::Null),
    );
    Some(serde_json::Value::Object(metadata))
}

/// Splits the background provenance recorded under
/// [`BACKGROUND_METADATA_KEY`] out of `metadata`: the line codec writes it as
/// the line's own `background` field instead.
pub(crate) fn split_background(
    metadata: Option<serde_json::Value>,
) -> (Option<serde_json::Value>, Option<BackgroundOrigin>) {
    let mut message = TranscriptMessage::assistant("");
    message.extra_metadata = metadata;
    let origin = take_background(&mut message);
    (message.extra_metadata, origin)
}

/// The background provenance recorded on a stored row, if any.
fn background_of(message: &TranscriptMessage) -> Option<BackgroundOrigin> {
    message
        .extra_metadata
        .as_ref()
        .and_then(|metadata| metadata.get(BACKGROUND_METADATA_KEY))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
}

/// Removes the background provenance from `message`'s metadata, returning it.
fn take_background(message: &mut TranscriptMessage) -> Option<BackgroundOrigin> {
    let origin = background_of(message)?;
    if let Some(serde_json::Value::Object(map)) = message.extra_metadata.as_mut() {
        map.remove(BACKGROUND_METADATA_KEY);
        if map.is_empty() {
            message.extra_metadata = None;
        }
    }
    Some(origin)
}

#[cfg(test)]
#[path = "display_tests.rs"]
mod tests;
