use super::*;

#[test]
fn parent_components_never_pop_the_filesystem_root() {
    assert_eq!(
        absolute_normalized_path(Path::new("/a/../..")),
        PathBuf::from("/")
    );
}

#[test]
fn parent_components_collapse_normal_segments() {
    assert_eq!(
        absolute_normalized_path(Path::new("/a/b/../c/./d")),
        PathBuf::from("/a/c/d")
    );
}

#[test]
fn completed_writer_releases_successor_lock_even_when_descriptor_is_inherited() {
    let workspace = tempfile::tempdir().unwrap();
    let history = FileTranscriptHistory::new(
        workspace.path(),
        "conversation",
        seed_meta_for_discovered("agent"),
    )
    .unwrap();
    let locks = history.acquire_write_lock().unwrap().unwrap();
    // File::try_clone shares the open file description, as a forked child does.
    // The child must not extend a completed parent's advisory lock lifetime.
    let inherited_successor = locks.0.file.try_clone().unwrap();
    drop(locks);
    history
        .append(TranscriptMessage::new("user", "after completed writer"))
        .unwrap();
    assert_eq!(history.messages().unwrap().len(), 1);
    drop(inherited_successor);
}

#[test]
fn failed_lock_attempt_does_not_release_the_existing_owner() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("session_raw/conversation.jsonl");
    let mut owner = generation_lock(&path).unwrap();
    owner.lock_exclusive().unwrap();
    let mut failed = generation_lock(&path).unwrap();
    assert!(failed.try_lock_exclusive().is_err());
    drop(failed);
    let mut next = generation_lock(&path).unwrap();
    assert!(next.try_lock_exclusive().is_err());
    drop(owner);
    next.try_lock_exclusive().unwrap();
}

#[test]
fn committed_generation_releases_reservation_even_when_descriptor_is_inherited() {
    let workspace = tempfile::tempdir().unwrap();
    let history = FileTranscriptHistory::new(
        workspace.path(),
        "conversation.g1",
        seed_meta_for_discovered("agent"),
    )
    .unwrap();
    let mut held = generation_lock(&history.path).unwrap();
    held.lock_exclusive().unwrap();
    let inherited = held.file.try_clone().unwrap();
    *history.generation_reservation.lock().unwrap() =
        Some(GenerationReservation { _successor: held });
    history
        .append(TranscriptMessage::new("user", "first generation write"))
        .unwrap();
    let mut observer = generation_lock(&history.path).unwrap();
    observer.try_lock_exclusive().unwrap();
    drop(observer);
    let peer = FileTranscriptHistory::new(
        workspace.path(),
        "conversation.g1",
        seed_meta_for_discovered("agent"),
    )
    .unwrap();
    peer.append(TranscriptMessage::new(
        "assistant",
        "peer after committed generation",
    ))
    .unwrap();
    assert_eq!(peer.messages().unwrap().len(), 2);
    drop(inherited);
}
