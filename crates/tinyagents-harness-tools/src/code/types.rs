//! Host-configured budgets and the shared native adapter state.

use std::sync::Arc;

use anyhow::ensure;
use parking_lot::Mutex;
use tinytools_code::{ChunkCatalog, CodeEngine, CodeLimits};

use crate::fs::filesystem::FsGate;

/// Resource limits for the optional structural tools.
///
/// Source and parser-cache limits live in [`CodeLimits`]. The remaining limits
/// cover native argument decoding, metadata, templates and model-visible output.
#[derive(Debug, Clone, Copy)]
pub struct CodeToolOptions {
    /// Pure parser, source, cache and record bounds.
    pub limits: CodeLimits,
    /// Maximum serialized argument bytes accepted by one invocation.
    pub max_argument_bytes: usize,
    /// Maximum bytes read from a custom manifest.
    pub max_manifest_bytes: usize,
    /// Maximum bytes read from a custom template.
    pub max_template_bytes: usize,
    /// Maximum UTF-8 bytes in the model-visible JSON or error text.
    pub max_output_bytes: usize,
    /// Maximum records returned per list page; also the default page size.
    pub max_page_items: usize,
    /// Maximum source lines returned per page; also the default page size.
    pub max_page_lines: usize,
}

impl Default for CodeToolOptions {
    fn default() -> Self {
        Self {
            limits: CodeLimits::default(),
            max_argument_bytes: 1_048_576,
            max_manifest_bytes: 65_536,
            max_template_bytes: 262_144,
            max_output_bytes: 65_536,
            max_page_items: 32,
            max_page_lines: 80,
        }
    }
}

impl CodeToolOptions {
    pub(super) fn validate(self) -> anyhow::Result<()> {
        for (name, value, maximum) in [
            ("argument", self.max_argument_bytes, 16_777_216),
            ("manifest", self.max_manifest_bytes, 1_048_576),
            ("template", self.max_template_bytes, 16_777_216),
            ("output", self.max_output_bytes, 16_777_216),
            ("page item", self.max_page_items, 1024),
            ("page line", self.max_page_lines, 4096),
        ] {
            ensure!(value > 0 && value <= maximum, "invalid {name} budget");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Outline,
    Symbol,
    Patch,
    Search,
    Inspect,
    Apply,
}

impl Kind {
    pub(super) const ALL: [Self; 6] = [
        Self::Outline,
        Self::Symbol,
        Self::Patch,
        Self::Search,
        Self::Inspect,
        Self::Apply,
    ];

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Outline => "ast_outline",
            Self::Symbol => "ast_symbol",
            Self::Patch => "ast_patch",
            Self::Search => "chunk_search",
            Self::Inspect => "chunk_inspect",
            Self::Apply => "chunk_apply",
        }
    }

    pub(super) fn mutates(self) -> bool {
        matches!(self, Self::Patch | Self::Apply)
    }
}

pub(super) struct Shared {
    pub(super) engine: Mutex<CodeEngine>,
    pub(super) catalog: ChunkCatalog,
    pub(super) write_lock: tokio::sync::Mutex<()>,
    pub(super) options: CodeToolOptions,
}

pub(super) struct CodeTool {
    pub(super) kind: Kind,
    pub(super) gate: Arc<dyn FsGate>,
    pub(super) shared: Arc<Shared>,
}
