use super::*;

#[test]
fn loads_two_layers_once_in_global_then_local_order_and_honors_gate() {
    let root = tempfile::tempdir().unwrap();
    let global = root.path().join("global");
    let local = root.path().join("project");
    std::fs::create_dir_all(&global).unwrap();
    std::fs::create_dir_all(&local).unwrap();
    std::fs::write(global.join(AGENTS_MD_FILENAME), " global rules \n").unwrap();
    std::fs::write(local.join(AGENTS_MD_FILENAME), "project rules").unwrap();
    let allow = |candidate: &Path, root: &Path| candidate.starts_with(root);
    let loaded = load_agents_md_layers(&global, &local, true, allow);
    assert_eq!(loaded.global.as_deref(), Some("global rules"));
    assert_eq!(loaded.local.as_deref(), Some("project rules"));
    assert!(load_agents_md_layers(&global, &local, false, allow).is_empty());

    let alias = root.path().join("global-alias");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&global, &alias).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(&global, &alias).unwrap();
    let deduped = load_agents_md_layers(&global, &alias, true, |candidate, allowed| {
        candidate.starts_with(allowed) || candidate == alias.join(AGENTS_MD_FILENAME)
    });
    assert_eq!(deduped.global.as_deref(), Some("global rules"));
    assert_eq!(deduped.local, None);
}

#[test]
fn rejects_missing_empty_invalid_paths_and_symlinked_instruction_files() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("project");
    std::fs::create_dir_all(&dir).unwrap();
    let allow = |candidate: &Path, allowed: &Path| candidate.starts_with(allowed);
    assert_eq!(load_agents_md(&dir, allow), None);
    std::fs::write(dir.join(AGENTS_MD_FILENAME), " \n\t ").unwrap();
    assert_eq!(load_agents_md(&dir, allow), None);
    std::fs::write(dir.join(AGENTS_MD_FILENAME), "private").unwrap();
    assert_eq!(load_agents_md(&dir, |_, _| false), None);

    let target = root.path().join("outside");
    std::fs::write(&target, "outside text").unwrap();
    std::fs::remove_file(dir.join(AGENTS_MD_FILENAME)).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&target, dir.join(AGENTS_MD_FILENAME)).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_file(&target, dir.join(AGENTS_MD_FILENAME)).unwrap();
    assert_eq!(load_agents_md(&dir, allow), None);
}

#[test]
fn instruction_read_is_bounded_in_bytes() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join(AGENTS_MD_FILENAME),
        vec![b'x'; MAX_AGENTS_MD_READ_BYTES as usize + 100],
    )
    .unwrap();
    let loaded = load_agents_md(root.path(), |candidate, allowed| {
        candidate.starts_with(allowed)
    })
    .unwrap();
    assert_eq!(loaded.len(), MAX_AGENTS_MD_READ_BYTES as usize);
}
