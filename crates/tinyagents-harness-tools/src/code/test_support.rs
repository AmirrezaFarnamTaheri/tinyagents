//! A deterministic host gate and public-tool fixtures for native regressions.

#![allow(clippy::unwrap_used)]

use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tinytools::{Tool, ToolRunContext, WorkspaceDescriptor};

use super::*;

#[derive(Debug)]
pub(super) enum Action {
    Write(PathBuf, String),
    Revoke,
    Remove(PathBuf),
}

#[derive(Debug, Default)]
pub(super) struct State {
    pub(super) read_only: AtomicBool,
    pub(super) denied: AtomicBool,
    pub(super) used: AtomicUsize,
    pub(super) refuse_record: AtomicBool,
    pub(super) action: Mutex<Option<Action>>,
    pub(super) parent_calls: AtomicUsize,
    pub(super) deny_recheck: AtomicBool,
    pub(super) revoke_at_recheck: AtomicBool,
    pub(super) pause_recheck: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

#[derive(Debug)]
pub(super) struct TestGate {
    pub(super) root: PathBuf,
    pub(super) state: Arc<State>,
    pub(super) max_actions: usize,
}

impl TestGate {
    pub(super) fn new(root: &Path) -> Arc<Self> {
        Arc::new(Self {
            root: root.to_path_buf(),
            state: Arc::new(State::default()),
            max_actions: 100,
        })
    }

    fn candidate(&self, path: &str) -> Result<PathBuf, String> {
        if self.state.denied.load(Ordering::SeqCst) || !self.is_path_string_allowed(path) {
            return Err("[policy-blocked] test host denied path".into());
        }
        let path = Path::new(path);
        Ok(if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        })
    }

    fn inside(&self, path: PathBuf) -> Result<PathBuf, String> {
        let root = self
            .root
            .canonicalize()
            .map_err(|error| error.to_string())?;
        if !path.starts_with(root) {
            return Err("[policy-blocked] outside test workspace".into());
        }
        Ok(path)
    }
}

#[async_trait]
impl FsGate for TestGate {
    fn can_act(&self) -> bool {
        !self.state.read_only.load(Ordering::SeqCst)
    }
    fn is_read_only(&self) -> bool {
        !self.can_act()
    }
    fn is_rate_limited(&self) -> bool {
        self.state.used.load(Ordering::SeqCst) >= self.max_actions
    }
    fn record_action(&self) -> bool {
        if self.state.refuse_record.load(Ordering::SeqCst) {
            return false;
        }
        let permitted = self.state.used.fetch_add(1, Ordering::SeqCst) < self.max_actions;
        if let Some(action) = self.state.action.lock().take() {
            match action {
                Action::Write(path, source) => std::fs::write(path, source).unwrap(),
                Action::Revoke => self.state.read_only.store(true, Ordering::SeqCst),
                Action::Remove(path) => std::fs::remove_file(path).unwrap(),
            }
        }
        permitted
    }
    fn write_needs_approval(&self) -> bool {
        true
    }
    fn action_dir(&self) -> &Path {
        &self.root
    }
    fn is_path_string_allowed(&self, path: &str) -> bool {
        !path.contains('\0')
            && !Path::new(path)
                .components()
                .any(|part| part == Component::ParentDir)
    }
    async fn validate_path(&self, path: &str) -> Result<PathBuf, String> {
        let full = self.candidate(path)?;
        self.inside(
            tokio::fs::canonicalize(full)
                .await
                .map_err(|error| error.to_string())?,
        )
    }
    async fn validate_parent_path(&self, path: &str) -> Result<PathBuf, String> {
        let calls = self.state.parent_calls.fetch_add(1, Ordering::SeqCst);
        if calls > 0 {
            if self.state.deny_recheck.load(Ordering::SeqCst) {
                return Err("[policy-blocked] original path revoked".into());
            }
            if let Some((entered, released)) = &self.state.pause_recheck {
                entered.notify_one();
                released.notified().await;
            }
            if self.state.revoke_at_recheck.load(Ordering::SeqCst) {
                self.state.read_only.store(true, Ordering::SeqCst);
            }
        }
        let full = self.candidate(path)?;
        let mut existing = full.parent().ok_or("missing parent")?.to_path_buf();
        while !existing.exists() {
            existing = existing.parent().ok_or("missing ancestor")?.to_path_buf();
        }
        let canonical = tokio::fs::canonicalize(&existing)
            .await
            .map_err(|error| error.to_string())?;
        self.inside(
            canonical.join(
                full.strip_prefix(existing)
                    .map_err(|error| error.to_string())?,
            ),
        )
    }
    fn scoped_to_workspace(&self, root: &Path) -> Arc<dyn FsGate> {
        Arc::new(Self {
            root: root.to_path_buf(),
            state: Arc::clone(&self.state),
            max_actions: self.max_actions,
        })
    }
}

pub(super) struct Workspace(pub(super) WorkspaceDescriptor);
impl ToolRunContext for Workspace {
    fn workspace(&self) -> Option<&WorkspaceDescriptor> {
        Some(&self.0)
    }
}

pub(super) fn tool<'a>(tools: &'a [Box<dyn Tool>], name: &str) -> &'a dyn Tool {
    tools
        .iter()
        .find(|tool| tool.name() == name)
        .unwrap()
        .as_ref()
}

pub(super) async fn success(tool: &dyn Tool, args: Value) -> Value {
    let result = tool.execute(args).await.unwrap();
    assert!(!result.is_error, "{}: {}", tool.name(), result.output());
    serde_json::from_str(&result.output()).unwrap()
}

pub(super) async fn failure(tool: &dyn Tool, args: Value) -> String {
    let result = tool.execute(args).await.unwrap();
    assert!(result.is_error, "unexpected success: {}", result.output());
    result.output()
}

pub(super) fn patch_args(source: &str) -> Value {
    json!({"path":"source.rs","expected_revision":tinytools_code::revision(source),
        "selector":"value","operation":"replace_body","code":"2"})
}

pub(super) fn custom_manifest(root: &Path, target: &str, template: &str) {
    std::fs::write(root.join("template.rs.txt"), template).unwrap();
    std::fs::write(
        root.join("manifest.json"),
        serde_json::to_vec(&json!({
            "id":"custom.pattern.v1", "summary":"Custom pattern", "language":"rust",
            "target":target, "template_path":"template.rs.txt", "parameters":[], "slots":[]
        }))
        .unwrap(),
    )
    .unwrap();
}
