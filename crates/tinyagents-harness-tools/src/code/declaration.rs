//! Tool metadata, strict dispatch and the host's per-call workspace scope.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinytools::{
    PermissionLevel, Tool, ToolAccess, ToolCallOptions, ToolExposure, ToolPolicy, ToolResult,
    ToolRunContext, ToolRuntime, ToolSideEffects, WorkspaceAccess,
};

use super::{
    arguments, response,
    types::{CodeTool, Kind},
};
use crate::fs::filesystem::FsGate;

#[async_trait]
impl Tool for CodeTool {
    fn name(&self) -> &str {
        self.kind.name()
    }

    fn description(&self) -> &str {
        match self.kind {
            Kind::Outline => {
                "Inspect a bounded lexical outline and folded source with content revisions and paging. This does not resolve semantic types."
            }
            Kind::Symbol => {
                "Read one unambiguous lexical symbol by revision-bound ID, qualified name or local name; source is paginated."
            }
            Kind::Patch => {
                "Apply one syntax-checked structural edit to an exact expected revision. Set dry_run to the literal boolean true for a read-only preview."
            }
            Kind::Search => {
                "Search bundled typed code patterns by metadata. Dependencies are informational and are never installed."
            }
            Kind::Inspect => {
                "Inspect a bundled pattern or gated custom manifest. Returns the content-bound chunk_revision required by chunk_apply."
            }
            Kind::Apply => {
                "Render a typed pattern into a revision-checked file or selected container. Requires chunk_revision; use create_new for no-clobber creation, or expected_revision for edits. Literal dry_run true previews without writing."
            }
        }
    }

    fn parameters_schema(&self) -> Value {
        schema(self.kind, self.shared.options.max_page_items)
    }

    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::ReadOnly
    }

    fn permission_level_with_args(&self, args: &Value) -> PermissionLevel {
        if self.kind.mutates() && !arguments::literal_preview(args) {
            PermissionLevel::Write
        } else {
            PermissionLevel::ReadOnly
        }
    }

    fn exposure(&self) -> ToolExposure {
        ToolExposure::Deferred
    }

    fn family(&self) -> Option<&str> {
        Some("code")
    }

    fn tags(&self) -> Vec<String> {
        ["code", "structural", "pack:code"]
            .map(str::to_owned)
            .to_vec()
    }

    fn is_concurrency_safe(&self, args: &Value) -> bool {
        !self.kind.mutates() || arguments::literal_preview(args)
    }

    fn external_effect(&self) -> bool {
        self.kind.mutates() && self.gate.write_needs_approval()
    }

    fn external_effect_with_args(&self, args: &Value) -> bool {
        self.external_effect() && !arguments::literal_preview(args)
    }

    fn policy(&self) -> ToolPolicy {
        let writes = self.kind.mutates();
        ToolPolicy::classified()
            .with_side_effects(ToolSideEffects {
                read_only: !writes,
                writes_files: writes,
                ..ToolSideEffects::default()
            })
            .with_runtime(ToolRuntime {
                idempotent: !writes,
                cancelable: true,
                max_result_bytes: Some(self.shared.options.max_output_bytes),
                ..ToolRuntime::default()
            })
            .with_access(ToolAccess {
                workspace: WorkspaceAccess::Scoped,
                trusted_roots: vec![self.gate.action_dir().display().to_string()],
                background_safe: !writes,
                ..ToolAccess::default()
            })
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        self.execute_with_context(args, ToolCallOptions::default(), None)
            .await
    }

    async fn execute_with_context(
        &self,
        args: Value,
        _options: ToolCallOptions,
        context: Option<&dyn ToolRunContext>,
    ) -> anyhow::Result<ToolResult> {
        let gate = context.and_then(ToolRunContext::workspace).map_or_else(
            || Arc::clone(&self.gate),
            |workspace| self.gate.scoped_to_workspace(&workspace.root),
        );
        Ok(match self.dispatch(args, &gate).await {
            Ok(result) => result,
            Err(error) => {
                response::bounded_error(&format!("{error:#}"), self.shared.options.max_output_bytes)
            }
        })
    }
}

impl CodeTool {
    async fn dispatch(&self, args: Value, gate: &Arc<dyn FsGate>) -> anyhow::Result<ToolResult> {
        arguments::check_size(&args, self.shared.options.max_argument_bytes)?;
        match self.kind {
            Kind::Outline => self.outline(arguments::decode(args)?, gate).await,
            Kind::Symbol => self.symbol(arguments::decode(args)?, gate).await,
            Kind::Patch => self.patch(arguments::decode(args)?, gate).await,
            Kind::Search => self.search(&arguments::decode(args)?),
            Kind::Inspect => self.inspect(arguments::decode(args)?, gate).await,
            Kind::Apply => self.apply(arguments::decode(args)?, gate).await,
        }
    }
}

fn schema(kind: Kind, max_page_items: usize) -> Value {
    let mut properties = serde_json::Map::new();
    let required = match kind {
        Kind::Outline | Kind::Symbol | Kind::Patch => {
            properties.insert("path".into(), json!({"type":"string"}));
            properties.insert("language".into(), json!({"type":"string","enum":["rust","typescript","tsx","javascript","python","go","json","toml"]}));
            if kind == Kind::Outline {
                properties.insert(
                    "level".into(),
                    json!({"type":"integer","enum":[0,1],"default":1,"description":"0 returns metadata only; 1 also returns the folded source page"}),
                );
                add_record_page(&mut properties);
                vec!["path"]
            } else {
                properties.insert("selector".into(), json!({"type":"string"}));
                if kind == Kind::Symbol {
                    vec!["path", "selector"]
                } else {
                    properties.insert("expected_revision".into(), revision_schema());
                    properties.insert("operation".into(), json!({"type":"string","enum":["replace_body","replace_symbol","insert_import","append_member","wrap"]}));
                    properties.insert("code".into(), json!({"type":"string"}));
                    properties.insert("dry_run".into(), json!({"type":"boolean","default":false}));
                    vec!["path", "expected_revision", "operation", "code"]
                }
            }
        }
        Kind::Search => {
            properties.insert("query".into(), json!({"type":"string","default":""}));
            properties.insert("language".into(), json!({"type":"string","enum":["rust","typescript","tsx","javascript","python","go","json","toml"]}));
            add_record_page(&mut properties);
            properties.insert(
                "limit".into(),
                json!({"type":"integer","minimum":1,"maximum":max_page_items,"default":max_page_items.min(5)}),
            );
            vec![]
        }
        Kind::Inspect | Kind::Apply => {
            properties.insert("id".into(), json!({"type":"string","description":"Bundled chunk ID; mutually exclusive with manifest_path"}));
            properties.insert("manifest_path".into(), json!({"type":"string","description":"Custom manifest file permitted by FsGate; mutually exclusive with id"}));
            if kind == Kind::Inspect {
                add_record_page(&mut properties);
                vec![]
            } else {
                properties.insert("path".into(), json!({"type":"string"}));
                properties.insert("chunk_revision".into(), revision_schema());
                properties.insert("expected_revision".into(), revision_schema());
                properties.insert("selector".into(), json!({"type":"string"}));
                properties.insert(
                    "create_new".into(),
                    json!({"type":"boolean","default":false}),
                );
                properties.insert("dry_run".into(), json!({"type":"boolean","default":false}));
                properties.insert("bindings".into(), json!({"type":"object","properties":{"parameters":{"type":"object"},"slots":{"type":"object","additionalProperties":{"type":"string"}}},"additionalProperties":false}));
                vec!["path", "chunk_revision"]
            }
        }
    };
    if matches!(
        kind,
        Kind::Outline | Kind::Symbol | Kind::Patch | Kind::Apply
    ) {
        properties.insert(
            "start_line".into(),
            json!({"type":"integer","minimum":1,"default":1}),
        );
        properties.insert("line_count".into(), json!({"type":"integer","minimum":1}));
    }
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}

fn add_record_page(properties: &mut serde_json::Map<String, Value>) {
    properties.insert(
        "offset".into(),
        json!({"type":"integer","minimum":0,"default":0}),
    );
    properties.insert("limit".into(), json!({"type":"integer","minimum":1}));
}

fn revision_schema() -> Value {
    json!({"type":"string","pattern":"^[0-9a-f]{64}$"})
}
