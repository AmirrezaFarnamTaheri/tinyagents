//! Serde adapters for transcript rows stored in the original `{role, content}`
//! shape.
//!
//! These adapters keep the legacy wire format at call sites that still embed
//! transcript rows in host-owned documents. New transcript storage should use
//! [`super::TranscriptMessage`] directly. Legacy content is normalized on
//! read, and rebuilt on write so typed tool calls and media retain the string
//! representation those documents expect.

use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::TranscriptMessage;

#[derive(Serialize)]
struct WireRow<'a> {
    role: &'a str,
    content: &'a str,
}

fn wire_content(row: &TranscriptMessage) -> String {
    if row.parts.is_some() {
        row.display_content()
    } else {
        row.legacy_content()
    }
}

fn serialize_rows<S: Serializer>(
    rows: &[TranscriptMessage],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut seq = serializer.serialize_seq(Some(rows.len()))?;
    for row in rows {
        let content = wire_content(row);
        seq.serialize_element(&WireRow {
            role: &row.role,
            content: &content,
        })?;
    }
    seq.end()
}

fn lift(row: TranscriptMessage) -> TranscriptMessage {
    row.normalized()
}

/// Serde adapter for `Vec<TranscriptMessage>` fields stored as flat
/// `{ "role", "content" }` pairs.
///
/// Use with `#[serde(with = "tinyagents_session::transcript::legacy::history_wire")]`.
pub mod history_wire {
    use super::*;

    /// Serialize rows using only their legacy `role` and `content` fields.
    pub fn serialize<S: Serializer>(
        rows: &[TranscriptMessage],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serialize_rows(rows, serializer)
    }

    /// Read legacy role/content rows and normalize any encoded tool or media
    /// structure into the typed transcript fields.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<TranscriptMessage>, D::Error> {
        Ok(Vec::<TranscriptMessage>::deserialize(deserializer)?
            .into_iter()
            .map(lift)
            .collect())
    }

    /// The corresponding adapter for `Option<Vec<TranscriptMessage>>` fields.
    pub mod option {
        use super::*;

        /// Serialize an optional row list using only legacy role/content pairs.
        pub fn serialize<S: Serializer>(
            rows: &Option<Vec<TranscriptMessage>>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            match rows {
                Some(rows) => serialize_rows(rows, serializer),
                None => serializer.serialize_none(),
            }
        }

        /// Read and normalize an optional legacy row list.
        pub fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<Vec<TranscriptMessage>>, D::Error> {
            Ok(Option::<Vec<TranscriptMessage>>::deserialize(deserializer)?
                .map(|rows| rows.into_iter().map(lift).collect()))
        }
    }
}

#[cfg(test)]
#[path = "legacy_tests.rs"]
mod tests;
