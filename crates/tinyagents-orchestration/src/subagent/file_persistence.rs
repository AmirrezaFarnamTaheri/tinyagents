//! Filesystem-backed persistence for subagent pauses and terminal outcomes.

use std::{fs::OpenOptions, path::{Path, PathBuf}};

use async_trait::async_trait;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    PersistedSubagentPause, SubagentError, SubagentOutcome, SubagentPausePersistenceDisposition,
    SubagentPersistence, SubagentResume, SubagentTaskKey,
    SubagentTerminalPersistenceDisposition,
};

type Result<T> = std::result::Result<T, SubagentError>;

/// File-backed subagent persistence with per-key process locks and atomic
/// record replacement.
#[derive(Clone, Debug)]
pub struct FileSubagentPersistence {
    root: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct TerminalRecord {
    key: SubagentTaskKey,
    outcome: SubagentOutcome,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum CompatibleTerminalRecord {
    Current(TerminalRecord),
    Legacy(SubagentOutcome),
}

#[derive(Serialize, Deserialize)]
struct TaskIndex {
    keys: Vec<SubagentTaskKey>,
}

impl FileSubagentPersistence {
    /// Creates a persistence store rooted at a canonical absolute directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let root = std::fs::canonicalize(&root).unwrap_or_else(|_| {
            if root.is_absolute() {
                root
            } else {
                std::env::current_dir()
                    .map(|cwd| cwd.join(&root))
                    .unwrap_or(root)
            }
        });
        Self { root }
    }

    /// The canonical absolute root selected by [`Self::new`].
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns live keys indexed under a task id. A task id may have multiple
    /// scoped lifecycles; callers must resolve ambiguity rather than guessing.
    pub fn keys_for_task(&self, task_id: &str) -> Result<Vec<SubagentTaskKey>, SubagentError> {
        let _lock = self.lock_path(&self.index_lock_path(task_id))?;
        self.read_index(task_id)
    }

    fn directory(&self) -> PathBuf {
        self.root.join("lifecycle")
    }

    fn digest<T: Serialize>(value: &T) -> Result<String, SubagentError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|error| SubagentError::Persistence(error.to_string()))?;
        Ok(format!("sha256-{:x}", Sha256::digest(bytes)))
    }

    fn key_id(key: &SubagentTaskKey) -> Result<String, SubagentError> {
        Self::digest(key)
    }

    fn task_id(&self, task_id: &str) -> Result<String, SubagentError> {
        Self::digest(&task_id)
    }

    fn pause_path(&self, key: &SubagentTaskKey) -> Result<PathBuf, SubagentError> {
        Ok(self.directory().join(format!("{}.pause.json", Self::key_id(key)?)))
    }

    fn terminal_path(&self, key: &SubagentTaskKey) -> Result<PathBuf, SubagentError> {
        Ok(self.directory().join(format!("{}.terminal.json", Self::key_id(key)?)))
    }

    fn index_path(&self, task_id: &str) -> Result<PathBuf, SubagentError> {
        Ok(self
            .directory()
            .join(format!("task-{}.index.json", self.task_id(task_id)?)))
    }

    fn lock_path(&self, path: &Path) -> Result<std::fs::File, SubagentError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(persistence_error)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(persistence_error)?;
        file.lock_exclusive().map_err(persistence_error)?;
        Ok(file)
    }

    fn key_lock(&self, key: &SubagentTaskKey) -> Result<std::fs::File, SubagentError> {
        Ok(self.lock_path(&self.directory().join(format!("{}.lock", Self::key_id(key)?)))?)
    }

    fn index_lock_path(&self, task_id: &str) -> Result<PathBuf, SubagentError> {
        Ok(self.directory().join(format!(
            "task-{}.index.lock",
            self.task_id(task_id)?
        )))
    }

    fn read_bytes(path: &Path) -> Result<Option<Vec<u8>>, SubagentError> {
        match std::fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(persistence_error(error)),
        }
    }

    fn read_pause(&self, key: &SubagentTaskKey) -> Result<Option<PersistedSubagentPause>, SubagentError> {
        Self::read_bytes(&self.pause_path(key)?)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(persistence_error))
            .transpose()
    }

    fn read_terminal(&self, key: &SubagentTaskKey) -> Result<Option<TerminalRecord>, SubagentError> {
        let Some(bytes) = Self::read_bytes(&self.terminal_path(key)?)? else {
            return Ok(None);
        };
        let record = serde_json::from_slice(&bytes).map_err(persistence_error)?;
        Ok(Some(match record {
            CompatibleTerminalRecord::Current(record) => record,
            CompatibleTerminalRecord::Legacy(outcome) => TerminalRecord {
                key: key.clone(),
                outcome,
            },
        }))
    }

    fn read_index(&self, task_id: &str) -> Result<Vec<SubagentTaskKey>, SubagentError> {
        let Some(bytes) = Self::read_bytes(&self.index_path(task_id)?)? else {
            return Ok(Vec::new());
        };
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(persistence_error)?;
        if let Some(keys) = value.get("keys") {
            return serde_json::from_value(keys.clone()).map_err(persistence_error);
        }
        // Read the early single-key index shape and upgrade it on mutation.
        Ok(value
            .get("key")
            .cloned()
            .and_then(|key| serde_json::from_value(key).ok())
            .into_iter()
            .collect())
    }

    fn update_index(
        &self,
        task_id: &str,
        update: impl FnOnce(&mut Vec<SubagentTaskKey>),
    ) -> Result<(), SubagentError> {
        let _lock = self.lock_path(&self.index_lock_path(task_id)?)?;
        let mut keys = self.read_index(task_id)?;
        update(&mut keys);
        let path = self.index_path(task_id)?;
        if keys.is_empty() {
            match std::fs::remove_file(path) {
                Ok(()) => return Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(persistence_error(error)),
            }
        }
        self.replace_file(&path, serde_json::to_vec(&TaskIndex { keys }).map_err(persistence_error)?)
    }

    fn add_index_key(&self, key: &SubagentTaskKey) -> Result<(), SubagentError> {
        self.update_index(&key.task_id, |keys| {
            if !keys.contains(key) {
                keys.push(key.clone());
            }
        })
    }

    fn remove_index_key(&self, key: &SubagentTaskKey) -> Result<(), SubagentError> {
        self.update_index(&key.task_id, |keys| keys.retain(|candidate| candidate != key))
    }

    fn replace_file(&self, path: &Path, bytes: Vec<u8>) -> Result<(), SubagentError> {
        let parent = path.parent().expect("lifecycle state path has a parent");
        std::fs::create_dir_all(parent).map_err(persistence_error)?;
        let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        std::fs::write(&temporary, bytes).map_err(persistence_error)?;
        match std::fs::rename(&temporary, path) {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = std::fs::remove_file(&temporary);
                Err(persistence_error(error))
            }
        }
    }

    fn insert_file(&self, path: &Path, bytes: Vec<u8>) -> Result<bool, SubagentError> {
        let parent = path.parent().expect("lifecycle state path has a parent");
        std::fs::create_dir_all(parent).map_err(persistence_error)?;
        let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        std::fs::write(&temporary, bytes).map_err(persistence_error)?;
        match std::fs::hard_link(&temporary, path) {
            Ok(()) => {
                let _ = std::fs::remove_file(temporary);
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = std::fs::remove_file(temporary);
                Ok(false)
            }
            Err(error) => {
                let _ = std::fs::remove_file(temporary);
                Err(persistence_error(error))
            }
        }
    }
}

fn persistence_error(error: impl std::fmt::Display) -> SubagentError {
    SubagentError::Persistence(error.to_string())
}

#[async_trait]
impl SubagentPersistence for FileSubagentPersistence {
    async fn load_terminal(&self, key: &SubagentTaskKey) -> Result<Option<SubagentOutcome>, SubagentError> {
        let _lock = self.key_lock(key)?;
        Ok(self.read_terminal(key)?.map(|record| record.outcome))
    }

    async fn load(&self, key: &SubagentTaskKey) -> Result<Option<SubagentResume>, SubagentError> {
        let _lock = self.key_lock(key)?;
        if self.read_terminal(key)?.is_some() {
            return Ok(None);
        }
        Ok(self.read_pause(key)?.map(|pause| match pause.outcome.status {
            super::SubagentOutcomeKind::AwaitingInput(pause) => pause.resume,
            _ => SubagentResume::default(),
        }))
    }

    async fn load_pause(&self, key: &SubagentTaskKey) -> Result<Option<SubagentOutcome>, SubagentError> {
        let _lock = self.key_lock(key)?;
        if self.read_terminal(key)?.is_some() {
            return Ok(None);
        }
        Ok(self.read_pause(key)?.map(|pause| pause.outcome))
    }

    async fn save_pause(&self, pause: PersistedSubagentPause) -> Result<SubagentPausePersistenceDisposition, SubagentError> {
        let _lock = self.key_lock(&pause.key)?;
        if self.read_terminal(&pause.key)?.is_some() {
            return Ok(SubagentPausePersistenceDisposition::TerminalExisting);
        }
        let path = self.pause_path(&pause.key)?;
        let previous = self.read_pause(&pause.key)?;
        match (&previous, &pause.replaces) {
            (Some(current), Some(expected))
                if pause_resume(current).is_some_and(|resume| resume == expected) => {}
            (None, None) => {},
            (Some(_), _) => return Ok(SubagentPausePersistenceDisposition::Existing),
            (None, Some(_)) => return Ok(SubagentPausePersistenceDisposition::Existing),
        }
        self.replace_file(&path, serde_json::to_vec(&pause).map_err(persistence_error)?)?;
        self.add_index_key(&pause.key)?;
        Ok(if previous.is_some() {
            SubagentPausePersistenceDisposition::Replaced
        } else {
            SubagentPausePersistenceDisposition::Inserted
        })
    }

    async fn record_terminal(&self, key: &SubagentTaskKey, outcome: &SubagentOutcome, replaces: Option<&SubagentResume>) -> Result<SubagentTerminalPersistenceDisposition, SubagentError> {
        let _lock = self.key_lock(key)?;
        if self.read_terminal(key)?.is_some() {
            return Ok(SubagentTerminalPersistenceDisposition::Existing);
        }
        let current = self.read_pause(key)?;
        let expected_matches = match (&current, replaces) {
            (Some(pause), Some(expected)) => pause_resume(pause).is_some_and(|resume| resume == expected),
            (None, None) => true,
            _ => false,
        };
        if !expected_matches {
            return Ok(SubagentTerminalPersistenceDisposition::PauseExisting);
        }
        let record = TerminalRecord { key: key.clone(), outcome: outcome.clone() };
        let inserted = self.insert_file(
            &self.terminal_path(key)?,
            serde_json::to_vec(&record).map_err(persistence_error)?,
        )?;
        if !inserted {
            return Ok(SubagentTerminalPersistenceDisposition::Existing);
        }
        if current.is_some() {
            let _ = std::fs::remove_file(self.pause_path(key)?);
        }
        if let Err(error) = self.remove_index_key(key) {
            tracing::warn!(task_id = %key.task_id, %error, "[subagent-persistence] terminal committed but index cleanup failed");
        }
        Ok(SubagentTerminalPersistenceDisposition::Inserted)
    }
}

fn pause_resume(pause: &PersistedSubagentPause) -> Option<&SubagentResume> {
    match &pause.outcome.status {
        super::SubagentOutcomeKind::AwaitingInput(state) => Some(&state.resume),
        _ => None,
    }
}

#[cfg(test)]
#[path = "file_persistence_tests.rs"]
mod tests;
