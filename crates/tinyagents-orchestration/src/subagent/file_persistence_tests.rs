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

#[tokio::test]
async fn terminal_record_host_metadata_survives_neutral_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSubagentPersistence::new(dir.path());
    let task_key = key("parent", "host-metadata");
    let path = store.terminal_path(&task_key).unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let record = serde_json::json!({
        "key": task_key,
        "outcome": SubagentOutcome::completed("host-metadata", "done"),
        "agent_id": "worker",
        "elapsed_ms": 25,
        "skill_filter_override": "docs"
    });
    std::fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();

    let outcome = store
        .load_terminal(&key("parent", "host-metadata"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.extensions["agent_id"], "worker");
    assert_eq!(outcome.extensions["elapsed_ms"], 25);
    assert_eq!(outcome.extensions["skill_filter_override"], "docs");
}

#[tokio::test]
async fn independent_file_store_handles_commit_only_one_pause_for_a_key() {
    let dir = tempfile::tempdir().unwrap();
    let first_store = FileSubagentPersistence::new(dir.path());
    let second_store = FileSubagentPersistence::new(dir.path());
    let key = key("parent", "concurrent");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let runtime = tokio::runtime::Handle::current();
    let first_barrier = barrier.clone();
    let first_runtime = runtime.clone();
    let first_store_thread = first_store.clone();
    let first_key = key.clone();
    let first = std::thread::spawn(move || {
        first_barrier.wait();
        first_runtime
            .block_on(first_store_thread.save_pause(pause(first_key, SubagentResume::default())))
    });
    let second_barrier = barrier.clone();
    let second_runtime = runtime;
    let second_store_thread = second_store.clone();
    let second_key = key.clone();
    let second = std::thread::spawn(move || {
        second_barrier.wait();
        second_runtime
            .block_on(second_store_thread.save_pause(pause(second_key, SubagentResume::default())))
    });
    barrier.wait();
    let first = first.join().expect("first persistence thread panicked");
    let second = second.join().expect("second persistence thread panicked");
    let dispositions = [first.unwrap(), second.unwrap()];
    assert_eq!(
        dispositions
            .iter()
            .filter(|value| **value == SubagentPausePersistenceDisposition::Inserted)
            .count(),
        1
    );
    assert!(dispositions.contains(&SubagentPausePersistenceDisposition::Existing));
    assert_eq!(first_store.keys_for_task("concurrent").unwrap(), vec![key]);
}

#[tokio::test]
async fn concurrent_replacements_compare_and_swap_the_pause_file_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let first_store = FileSubagentPersistence::new(dir.path());
    let second_store = FileSubagentPersistence::new(dir.path());
    let key = key("parent", "replace-race");
    let previous = SubagentResume {
        checkpoint: Some("old".into()),
        ..SubagentResume::default()
    };
    first_store
        .save_pause(pause(key.clone(), previous.clone()))
        .await
        .unwrap();
    let make_replacement = |checkpoint: &'static str| {
        let mut next = pause(
            key.clone(),
            SubagentResume {
                checkpoint: Some(checkpoint.into()),
                ..SubagentResume::default()
            },
        );
        next.replaces = Some(previous.clone());
        next
    };
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let runtime = tokio::runtime::Handle::current();
    let first_barrier = barrier.clone();
    let first_runtime = runtime.clone();
    let first_store_thread = first_store.clone();
    let first_replacement = make_replacement("first");
    let first = std::thread::spawn(move || {
        first_barrier.wait();
        first_runtime.block_on(first_store_thread.save_pause(first_replacement))
    });
    let second_barrier = barrier.clone();
    let second_runtime = runtime;
    let second_store_thread = second_store.clone();
    let second_replacement = make_replacement("second");
    let second = std::thread::spawn(move || {
        second_barrier.wait();
        second_runtime.block_on(second_store_thread.save_pause(second_replacement))
    });
    barrier.wait();
    let first = first.join().expect("first replacement thread panicked");
    let second = second.join().expect("second replacement thread panicked");
    let dispositions = [first.unwrap(), second.unwrap()];
    assert_eq!(
        dispositions
            .iter()
            .filter(|value| **value == SubagentPausePersistenceDisposition::Replaced)
            .count(),
        1
    );
    assert!(dispositions.contains(&SubagentPausePersistenceDisposition::Existing));
    assert!(matches!(
        first_store
            .load(&key)
            .await
            .unwrap()
            .unwrap()
            .checkpoint
            .as_deref(),
        Some("first" | "second")
    ));
}

#[tokio::test]
async fn invalid_non_pause_record_is_not_returned_as_default_resume() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSubagentPersistence::new(dir.path());
    let key = key("parent", "invalid-pause");
    let path = store.pause_path(&key).unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let invalid = PersistedSubagentPause {
        key: key.clone(),
        outcome: SubagentOutcome::completed("invalid-pause", "done"),
        replaces: None,
    };
    assert!(store.save_pause(invalid.clone()).await.is_err());
    std::fs::write(path, serde_json::to_vec(&invalid).unwrap()).unwrap();
    assert_eq!(store.load(&key).await.unwrap(), None);
    assert_eq!(store.load_pause(&key).await.unwrap(), None);
}

#[test]
fn file_store_canonicalizes_checkpoint_roots() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSubagentPersistence::new(dir.path());
    assert!(store.root().is_absolute());
    assert_eq!(store.root(), std::fs::canonicalize(dir.path()).unwrap());
}
