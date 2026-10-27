//! Strict argument packets; malformed preview flags never become writes.

use anyhow::{Context, ensure};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;
use tinytools_code::{ChunkBindings, Language, PatchOperation};

/// Stop serialization at the bound instead of copying an oversized value.
pub(super) fn check_size(args: &Value, maximum: usize) -> anyhow::Result<()> {
    serde_json::to_writer(ArgumentBudget { remaining: maximum }, args)
        .context("structural tool arguments exceed the byte budget")
}

struct ArgumentBudget {
    remaining: usize,
}

impl std::io::Write for ArgumentBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.remaining = self
            .remaining
            .checked_sub(bytes.len())
            .ok_or_else(|| std::io::Error::other("argument byte budget exhausted"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn decode<T: DeserializeOwned>(args: Value) -> anyhow::Result<T> {
    serde_json::from_value(args).context("invalid structural tool arguments")
}

pub(super) fn language(path: &str, explicit: Option<Language>) -> anyhow::Result<Language> {
    explicit
        .or_else(|| Language::from_path(path))
        .context("unknown filename extension; supply an explicit supported language")
}

pub(super) fn literal_preview(args: &Value) -> bool {
    args.get("dry_run") == Some(&Value::Bool(true))
}

pub(super) fn page_size(requested: Option<usize>, maximum: usize) -> anyhow::Result<usize> {
    let size = requested.unwrap_or(maximum);
    ensure!(
        size > 0 && size <= maximum,
        "page size must be between 1 and {maximum}"
    );
    Ok(size)
}

pub(super) fn require_revision(value: &str) -> anyhow::Result<()> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "expected revision must contain 64 lowercase hexadecimal digits"
    );
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Outline {
    pub(super) path: String,
    pub(super) language: Option<Language>,
    #[serde(default = "outline_level")]
    pub(super) level: u8,
    #[serde(default)]
    pub(super) offset: usize,
    pub(super) limit: Option<usize>,
    #[serde(default = "first_line")]
    pub(super) start_line: usize,
    pub(super) line_count: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Symbol {
    pub(super) path: String,
    pub(super) language: Option<Language>,
    pub(super) selector: String,
    #[serde(default = "first_line")]
    pub(super) start_line: usize,
    pub(super) line_count: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Patch {
    pub(super) path: String,
    pub(super) language: Option<Language>,
    pub(super) expected_revision: String,
    pub(super) selector: Option<String>,
    pub(super) operation: PatchOperation,
    pub(super) code: String,
    #[serde(default)]
    pub(super) dry_run: bool,
    #[serde(default = "first_line")]
    pub(super) start_line: usize,
    pub(super) line_count: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Search {
    #[serde(default)]
    pub(super) query: String,
    pub(super) language: Option<Language>,
    #[serde(default)]
    pub(super) offset: usize,
    pub(super) limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Inspect {
    pub(super) id: Option<String>,
    pub(super) manifest_path: Option<String>,
    #[serde(default)]
    pub(super) offset: usize,
    pub(super) limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Apply {
    pub(super) path: String,
    pub(super) id: Option<String>,
    pub(super) manifest_path: Option<String>,
    pub(super) chunk_revision: String,
    #[serde(default)]
    pub(super) bindings: ChunkBindings,
    pub(super) selector: Option<String>,
    pub(super) expected_revision: Option<String>,
    #[serde(default)]
    pub(super) create_new: bool,
    #[serde(default)]
    pub(super) dry_run: bool,
    #[serde(default = "first_line")]
    pub(super) start_line: usize,
    pub(super) line_count: Option<usize>,
}

const fn first_line() -> usize {
    1
}

const fn outline_level() -> u8 {
    1
}
