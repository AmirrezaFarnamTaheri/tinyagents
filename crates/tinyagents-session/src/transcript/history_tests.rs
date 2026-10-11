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
