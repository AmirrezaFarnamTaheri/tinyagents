//! A small, self-contained [`FsGate`] for the filesystem tools' unit tests.
//!
//! It models just enough of a host policy to exercise the tools: an autonomy
//! tier, an action budget, an approval flag, and a set of allowed roots that
//! paths must resolve into. It is *not* a copy of any host's policy; the
//! host's own semantics are tested in the host, through its adapter.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;

use super::gate::FsGate;

/// Prefix the fake puts on refusals a real host would mark as policy-blocked.
pub const POLICY_BLOCKED_MARKER: &str = "[policy-blocked]";

/// Autonomy tier of the fake gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AutonomyLevel {
    /// Nothing that changes state may run.
    ReadOnly,
    /// Writes ask for approval.
    Supervised,
    /// Writes run unprompted.
    Full,
}

/// A fake host policy.
#[derive(Debug)]
pub struct TestGate {
    autonomy: AutonomyLevel,
    action_dir: PathBuf,
    roots: Vec<PathBuf>,
    max_actions: usize,
    used: AtomicUsize,
}

impl TestGate {
    /// Supervised gate whose only root is `dir`.
    pub fn supervised(dir: PathBuf) -> Arc<Self> {
        Self::with(dir, AutonomyLevel::Supervised, 1_000_000)
    }

    /// Gate over `dir` with an explicit tier and action budget.
    pub fn with(dir: PathBuf, autonomy: AutonomyLevel, max_actions: usize) -> Arc<Self> {
        Arc::new(Self {
            autonomy,
            roots: vec![dir.clone()],
            action_dir: dir,
            max_actions,
            used: AtomicUsize::new(0),
        })
    }

    /// Supervised gate resolving relative paths against `action_dir`, with
    /// `extra_roots` also readable and writable.
    pub fn split(action_dir: PathBuf, extra_roots: Vec<PathBuf>) -> Arc<Self> {
        let mut roots = vec![action_dir.clone()];
        roots.extend(extra_roots);
        Arc::new(Self {
            autonomy: AutonomyLevel::Supervised,
            roots,
            action_dir,
            max_actions: 1_000_000,
            used: AtomicUsize::new(0),
        })
    }

    fn full_path(&self, path: &str) -> PathBuf {
        if Path::new(path).is_absolute() {
            PathBuf::from(path)
        } else {
            self.action_dir.join(path)
        }
    }

    /// Canonicalize the deepest existing ancestor of `full` and test it
    /// against the roots, so a symlink out of a root is caught.
    fn resolved_inside_roots(&self, full: &Path) -> bool {
        let mut existing = full.to_path_buf();
        while !existing.exists() {
            match existing.parent() {
                Some(parent) => existing = parent.to_path_buf(),
                None => return false,
            }
        }
        let Ok(canonical) = existing.canonicalize() else {
            return false;
        };
        self.roots.iter().any(|root| {
            root.canonicalize()
                .is_ok_and(|root| canonical.starts_with(root))
        })
    }

    fn blocked(path: &str) -> String {
        format!(
            "{POLICY_BLOCKED_MARKER} Path not allowed by security policy: {path}. Do not retry \
             this path; use an allowed location (the workspace or a granted folder)."
        )
    }
}

#[async_trait]
impl FsGate for TestGate {
    fn can_act(&self) -> bool {
        self.autonomy != AutonomyLevel::ReadOnly
    }

    fn is_read_only(&self) -> bool {
        self.autonomy == AutonomyLevel::ReadOnly
    }

    fn is_rate_limited(&self) -> bool {
        self.used.load(Ordering::SeqCst) >= self.max_actions
    }

    fn record_action(&self) -> bool {
        self.used.fetch_add(1, Ordering::SeqCst) < self.max_actions
    }

    fn write_needs_approval(&self) -> bool {
        self.autonomy == AutonomyLevel::Supervised
    }

    fn action_dir(&self) -> &Path {
        &self.action_dir
    }

    fn is_path_string_allowed(&self, path: &str) -> bool {
        if path.contains('\0') {
            return false;
        }
        let candidate = Path::new(path);
        if candidate
            .components()
            .any(|component| component == Component::ParentDir)
        {
            return false;
        }
        self.resolved_inside_roots(&self.full_path(path))
    }

    async fn validate_path(&self, path: &str) -> Result<PathBuf, String> {
        if !self.is_path_string_allowed(path) {
            return Err(Self::blocked(path));
        }
        tokio::fs::canonicalize(self.full_path(path))
            .await
            .map_err(|e| format!("Failed to resolve path '{path}': {e}"))
    }

    async fn validate_parent_path(&self, path: &str) -> Result<PathBuf, String> {
        if !self.is_path_string_allowed(path) {
            return Err(Self::blocked(path));
        }
        let full = self.full_path(path);
        let parent = full
            .parent()
            .ok_or_else(|| format!("Invalid path (no parent): {path}"))?;
        let file_name = full
            .file_name()
            .ok_or_else(|| format!("Invalid path (no filename): {path}"))?;
        let mut existing = parent.to_path_buf();
        while !existing.exists() {
            match existing.parent() {
                Some(next) => existing = next.to_path_buf(),
                None => break,
            }
        }
        let canonical = tokio::fs::canonicalize(&existing)
            .await
            .map_err(|e| format!("Failed to resolve parent of '{path}': {e}"))?;
        let suffix = parent.strip_prefix(&existing).unwrap_or(Path::new(""));
        Ok(canonical.join(suffix).join(file_name))
    }

    fn scoped_to_workspace(&self, root: &Path) -> Arc<dyn FsGate> {
        let mut roots = self.roots.clone();
        roots.push(root.to_path_buf());
        Arc::new(Self {
            autonomy: self.autonomy,
            action_dir: root.to_path_buf(),
            roots,
            max_actions: self.max_actions,
            used: AtomicUsize::new(self.used.load(Ordering::SeqCst)),
        })
    }
}

/// A run context that carries an isolated workspace, as a harness threads one
/// into a worktree-isolated worker.
#[derive(Debug)]
pub struct WorkspaceContext(pub tinytools::WorkspaceDescriptor);

impl WorkspaceContext {
    /// Context whose workspace is rooted at `root`.
    pub fn at(root: &Path) -> Self {
        Self(
            tinytools::WorkspaceDescriptor::new(root.to_path_buf()).with_policy_id("test-worktree"),
        )
    }
}

impl tinytools::ToolRunContext for WorkspaceContext {
    fn workspace(&self) -> Option<&tinytools::WorkspaceDescriptor> {
        Some(&self.0)
    }
}

/// A gate whose budget check passes but whose `record_action` refuses, the
/// interleaving where another call spends the last unit between the two.
#[derive(Debug)]
pub struct RacyGate(pub Arc<TestGate>);

#[async_trait]
impl FsGate for RacyGate {
    fn can_act(&self) -> bool {
        self.0.can_act()
    }
    fn is_read_only(&self) -> bool {
        self.0.is_read_only()
    }
    fn is_rate_limited(&self) -> bool {
        false
    }
    fn record_action(&self) -> bool {
        false
    }
    fn write_needs_approval(&self) -> bool {
        self.0.write_needs_approval()
    }
    fn action_dir(&self) -> &Path {
        self.0.action_dir()
    }
    fn is_path_string_allowed(&self, path: &str) -> bool {
        self.0.is_path_string_allowed(path)
    }
    async fn validate_path(&self, path: &str) -> Result<PathBuf, String> {
        self.0.validate_path(path).await
    }
    async fn validate_parent_path(&self, path: &str) -> Result<PathBuf, String> {
        self.0.validate_parent_path(path).await
    }
    fn scoped_to_workspace(&self, root: &Path) -> Arc<dyn FsGate> {
        self.0.scoped_to_workspace(root)
    }
}

/// A [`TestGate`] wrapper that can force conditions the plain fake cannot
/// reach: a budget that reports "not limited" yet refuses to record, a path
/// resolver pinned to one answer, or an action directory that does not exist.
#[derive(Debug)]
pub struct WrapGate {
    inner: Arc<TestGate>,
    no_budget: bool,
    fixed_path: Option<PathBuf>,
    action_dir: Option<PathBuf>,
    batch_calls: Arc<AtomicUsize>,
    wrong_length_batch: bool,
}

impl WrapGate {
    /// Wrap `inner` without overriding anything yet.
    pub fn new(inner: Arc<TestGate>) -> Self {
        Self {
            inner,
            no_budget: false,
            fixed_path: None,
            action_dir: None,
            batch_calls: Arc::new(AtomicUsize::new(0)),
            wrong_length_batch: false,
        }
    }

    /// Answer every batch with one verdict too few.
    #[must_use]
    pub fn wrong_length_batch(mut self) -> Self {
        self.wrong_length_batch = true;
        self
    }

    /// Counter of `paths_string_allowed` calls this gate has received.
    pub fn batch_calls(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.batch_calls)
    }

    /// `is_rate_limited` stays `false` but `record_action` returns `false`.
    #[must_use]
    pub fn no_budget(mut self) -> Self {
        self.no_budget = true;
        self
    }

    /// Both validators answer `Ok(path)` regardless of input.
    #[must_use]
    pub fn fixed_path(mut self, path: PathBuf) -> Self {
        self.fixed_path = Some(path);
        self
    }

    /// Report `dir` as the action directory.
    #[must_use]
    pub fn action_dir_override(mut self, dir: PathBuf) -> Self {
        self.action_dir = Some(dir);
        self
    }

    /// Erase into the trait object the tools take.
    pub fn arc(self) -> Arc<dyn FsGate> {
        Arc::new(self)
    }
}

#[async_trait]
impl FsGate for WrapGate {
    fn can_act(&self) -> bool {
        self.inner.can_act()
    }

    fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    fn is_rate_limited(&self) -> bool {
        !self.no_budget && self.inner.is_rate_limited()
    }

    fn record_action(&self) -> bool {
        !self.no_budget && self.inner.record_action()
    }

    fn write_needs_approval(&self) -> bool {
        self.inner.write_needs_approval()
    }

    fn action_dir(&self) -> &Path {
        self.action_dir
            .as_deref()
            .unwrap_or_else(|| self.inner.action_dir())
    }

    fn is_path_string_allowed(&self, path: &str) -> bool {
        self.inner.is_path_string_allowed(path)
    }

    fn paths_string_allowed(&self, paths: &[String]) -> Vec<bool> {
        self.batch_calls.fetch_add(1, Ordering::SeqCst);
        let mut answers: Vec<bool> = paths
            .iter()
            .map(|path| self.inner.is_path_string_allowed(path))
            .collect();
        if self.wrong_length_batch {
            answers.pop();
        }
        answers
    }

    async fn validate_path(&self, path: &str) -> Result<PathBuf, String> {
        match &self.fixed_path {
            Some(fixed) => Ok(fixed.clone()),
            None => self.inner.validate_path(path).await,
        }
    }

    async fn validate_parent_path(&self, path: &str) -> Result<PathBuf, String> {
        match &self.fixed_path {
            Some(fixed) => Ok(fixed.clone()),
            None => self.inner.validate_parent_path(path).await,
        }
    }

    fn scoped_to_workspace(&self, root: &Path) -> Arc<dyn FsGate> {
        self.inner.scoped_to_workspace(root)
    }
}
