use super::*;

fn key(parent: &str, task: &str) -> SubagentTaskKey {
    SubagentTaskKey {
        root_run_id: "root".into(),
        parent_run_id: parent.into(),
        thread_id: Some("thread".into()),
        task_id: task.into(),
    }
}

fn pause(key: SubagentTaskKey, resume: SubagentResume) -> PersistedSubagentPause {
    let task_id = key.task_id.clone();
    PersistedSubagentPause {
        key,
        outcome: SubagentOutcome {
            status: super::super::SubagentOutcomeKind::AwaitingInput(super::super::SubagentPause {
                reason: "need an answer".into(),
                resume: resume.clone(),
            }),
            output: "partial".into(),
            ..SubagentOutcome::cancelled(task_id)
        },
        replaces: None,
    }
}

#[tokio::test]
async fn file_store_scopes_indexes_and_compares_pause_replacements() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSubagentPersistence::new(dir.path());
    let first = key("parent-a", "same-task");
    let second = key("parent-b", "same-task");
    let resume = SubagentResume {
        checkpoint: Some("checkpoint-a".into()),
        ..SubagentResume::default()
    };

    assert_eq!(
        store
            .save_pause(pause(first.clone(), resume.clone()))
            .await
            .unwrap(),
        SubagentPausePersistenceDisposition::Inserted
    );
    assert_eq!(
        store
            .save_pause(pause(second.clone(), SubagentResume::default()))
            .await
            .unwrap(),
        SubagentPausePersistenceDisposition::Inserted
    );
    assert_eq!(
        store.keys_for_task("same-task").unwrap(),
        vec![first.clone(), second]
    );
    assert_eq!(store.load(&first).await.unwrap(), Some(resume.clone()));
    assert_eq!(
        store
            .record_terminal(
                &first,
                &SubagentOutcome::completed("same-task", "done"),
                None
            )
            .await
            .unwrap(),
        SubagentTerminalPersistenceDisposition::PauseExisting
    );

    let mut replacement = pause(first.clone(), SubagentResume::default());
    replacement.replaces = Some(resume.clone());
    assert_eq!(
        store.save_pause(replacement).await.unwrap(),
        SubagentPausePersistenceDisposition::Replaced
    );
    let current = store.load(&first).await.unwrap().unwrap();
    assert_eq!(
        store
            .record_terminal(
                &first,
                &SubagentOutcome::completed("same-task", "done"),
                Some(&current),
            )
            .await
            .unwrap(),
        SubagentTerminalPersistenceDisposition::Inserted
    );
    assert_eq!(store.load(&first).await.unwrap(), None);
    assert_eq!(
        store.load_terminal(&first).await.unwrap().unwrap().output,
        "done"
    );
    assert_eq!(
        store.keys_for_task("same-task").unwrap(),
        vec![key("parent-b", "same-task")]
    );
}

#[tokio::test]
async fn file_store_reads_legacy_terminal_outcome_records() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSubagentPersistence::new(dir.path());
    let key = key("parent", "legacy");
    let outcome = SubagentOutcome::completed("legacy", "already finished");
    let path = store.terminal_path(&key).unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, serde_json::to_vec(&outcome).unwrap()).unwrap();

    assert_eq!(store.load_terminal(&key).await.unwrap(), Some(outcome));
}

#[test]
fn file_store_canonicalizes_checkpoint_roots() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSubagentPersistence::new(dir.path());
    assert!(store.root().is_absolute());
    assert_eq!(store.root(), std::fs::canonicalize(dir.path()).unwrap());
}
