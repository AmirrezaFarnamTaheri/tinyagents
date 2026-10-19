//! Tests for file-state scoping, tracking, staleness checks, and path locks.

use super::*;

#[tokio::test]
async fn returns_none_outside_scope() {
    assert_eq!(current_file_state_agent_id(), None);
}

#[tokio::test]
async fn installs_and_reads_agent_id() {
    let observed =
        with_file_state_agent_id("agent-1".into(), async { current_file_state_agent_id() }).await;
    assert_eq!(observed, Some("agent-1".to_string()));
}

#[tokio::test]
async fn does_not_leak_across_scopes() {
    with_file_state_agent_id("agent-1".into(), async {
        assert_eq!(current_file_state_agent_id(), Some("agent-1".to_string()));
    })
    .await;
    assert_eq!(current_file_state_agent_id(), None);
}

#[tokio::test]
async fn nested_scope_overrides_outer() {
    with_file_state_agent_id("parent".into(), async {
        assert_eq!(current_file_state_agent_id(), Some("parent".to_string()));
        with_file_state_agent_id("child".into(), async {
            assert_eq!(current_file_state_agent_id(), Some("child".to_string()));
        })
        .await;
        assert_eq!(current_file_state_agent_id(), Some("parent".to_string()));
    })
    .await;
}
// Tests for read/write tracking, staleness checks, and path locks.
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::Mutex;

fn fresh_coordinator() -> Arc<FileStateCoordinator> {
    Arc::new(FileStateCoordinator::new())
}

#[test]
fn record_and_check_no_staleness() {
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/a.txt");
    coord.reads.write().insert(
        ("agent-a".to_string(), path.clone()),
        ReadStamp {
            mtime: SystemTime::now(),
            timestamp: Instant::now(),
            partial: false,
        },
    );
    let partial = coord
        .reads
        .read()
        .get(&("agent-a".to_string(), path.clone()))
        .map(|rs| rs.partial);
    assert_eq!(partial, Some(false));
    assert!(coord.writes.read().get(&path).is_none());
}

#[test]
fn detect_sibling_write_staleness() {
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/b.txt");
    coord.record_read(
        "agent-a",
        path.clone(),
        SystemTime::now(),
        false,
        Instant::now(),
    );
    std::thread::sleep(Duration::from_millis(5));
    coord.record_write("agent-b", path.clone());
    let stale = coord.stale_reads_for_parent("agent-a");
    assert_eq!(stale, vec![path]);
}

#[test]
fn own_write_does_not_trigger_staleness() {
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/c.txt");
    coord.record_read(
        "agent-a",
        path.clone(),
        SystemTime::now(),
        false,
        Instant::now(),
    );
    std::thread::sleep(Duration::from_millis(5));
    coord.record_write("agent-a", path.clone());
    let stale = coord.stale_reads_for_parent("agent-a");
    assert_eq!(stale.len(), 0);
    assert_eq!(coord.check_stale_read("agent-a", &path), None);
}

#[test]
fn partial_read_detected() {
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/d.txt");
    coord.reads.write().insert(
        ("agent-a".to_string(), path.clone()),
        ReadStamp {
            mtime: SystemTime::now(),
            timestamp: Instant::now(),
            partial: true,
        },
    );
    let partial = coord
        .reads
        .read()
        .get(&("agent-a".to_string(), path.clone()))
        .map(|rs| rs.partial);
    assert_eq!(partial, Some(true));
}

#[test]
fn parent_stale_files_detects_child_writes() {
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/e.txt");
    coord.record_read(
        "parent",
        path.clone(),
        SystemTime::now(),
        false,
        Instant::now(),
    );
    std::thread::sleep(Duration::from_millis(5));
    coord.record_write("child-1", path.clone());
    assert_eq!(coord.stale_reads_for_parent("parent"), vec![path.clone()]);
    assert_eq!(
        coord.parent_stale_files("parent", &["child-1".to_string()]),
        vec![path]
    );
    assert_eq!(
        coord
            .parent_stale_files("parent", &["someone-else".to_string()])
            .len(),
        0
    );
}

#[test]
fn paths_written_by_collects_correctly() {
    let coord = fresh_coordinator();
    let p1 = PathBuf::from("/tmp/test/f1.txt");
    let p2 = PathBuf::from("/tmp/test/f2.txt");
    coord.record_write("child-1", p1.clone());
    coord.record_write("child-2", p2);
    let result = coord.paths_written_by(&["child-1".to_string()]);
    assert_eq!(result.len(), 1);
    assert_eq!(result.get("child-1"), Some(&vec![p1]));
}

#[tokio::test]
async fn path_lock_serialises_access() {
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/lock.txt");
    let mutex = {
        let mut locks = coord.path_locks.write();
        locks
            .entry(path.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };

    let guard = mutex.lock().await;
    assert!(mutex.try_lock().is_err());
    drop(guard);
    assert!(mutex.try_lock().is_ok());
}

#[tokio::test]
async fn global_api_tracks_reads_writes_and_locks() -> anyhow::Result<()> {
    use super::{
        acquire_path_lock, check_partial_read, check_stale_read, init_global, parent_stale_files,
        record_read, record_write, try_global,
    };

    init_global(false);
    init_global(true);
    assert!(try_global().is_some());

    let path = PathBuf::from("/tmp/test/global-flow.txt");
    record_read(
        "reader",
        path.clone(),
        SystemTime::now(),
        true,
        Instant::now(),
    );
    assert!(check_partial_read("reader", &path).is_some());
    assert!(check_stale_read("reader", &path).is_none());

    record_read(
        "reader",
        path.clone(),
        SystemTime::now(),
        false,
        Instant::now(),
    );
    assert!(check_partial_read("reader", &path).is_none());

    std::thread::sleep(Duration::from_millis(5));
    record_write("writer", path.clone());
    let msg = check_stale_read("reader", &path)
        .ok_or_else(|| anyhow::anyhow!("expected a stale read after the sibling write"))?;
    assert!(msg.contains("writer"));
    assert_eq!(
        parent_stale_files("reader", &["writer".to_string()]),
        vec![path.clone()]
    );
    assert!(check_stale_read("writer", &path).is_none());

    let guard = acquire_path_lock(&path).await;
    assert!(guard.is_some());
    Ok(())
}

#[test]
fn paths_written_by_keeps_a_path_after_another_agent_overwrites_it() {
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/history-shared.txt");
    coord.record_write("child-1", path.clone());
    coord.record_write("child-2", path.clone());

    let result = coord.paths_written_by(&["child-1".to_string(), "child-2".to_string()]);
    assert_eq!(result.get("child-1"), Some(&vec![path.clone()]));
    assert_eq!(result.get("child-2"), Some(&vec![path]));
}

#[test]
fn sibling_write_during_an_in_flight_read_is_reported_stale() {
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/in-flight.txt");
    let read_started = Instant::now();
    std::thread::sleep(Duration::from_millis(2));
    // The sibling write lands after the reader opened the file but before
    // the reader got round to recording the read.
    coord.record_write("sibling", path.clone());
    std::thread::sleep(Duration::from_millis(2));
    coord.record_read(
        "reader",
        path.clone(),
        SystemTime::now(),
        false,
        read_started,
    );

    assert_eq!(coord.stale_reads_for_parent("reader"), vec![path]);
}

// ── A later writer must not mask an earlier one ─────────────

/// Parent reads `path`, then child-1 and child-2 write it in that order.
fn parent_read_then_two_child_writes(coord: &FileStateCoordinator, path: &Path) {
    coord.record_read(
        "parent",
        path.to_path_buf(),
        SystemTime::now(),
        false,
        Instant::now(),
    );
    std::thread::sleep(Duration::from_millis(2));
    coord.record_write("child-1", path.to_path_buf());
    std::thread::sleep(Duration::from_millis(2));
    coord.record_write("child-2", path.to_path_buf());
}

#[test]
fn parent_stale_files_reports_an_earlier_child_write_masked_by_a_later_one() {
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/masked-child.txt");
    parent_read_then_two_child_writes(&coord, &path);

    assert_eq!(
        coord.parent_stale_files("parent", &["child-1".to_string()]),
        vec![path.clone()]
    );
    assert_eq!(
        coord.parent_stale_files("parent", &["child-2".to_string()]),
        vec![path]
    );
}

#[test]
fn stale_reads_for_parent_reports_a_path_written_by_two_children() {
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/two-children.txt");
    parent_read_then_two_child_writes(&coord, &path);

    assert_eq!(coord.stale_reads_for_parent("parent"), vec![path]);
}

#[test]
fn own_later_write_does_not_mask_a_sibling_write_during_an_in_flight_read() {
    // The reader opens the file, a sibling writes it, the reader then writes
    // it through another tool, and only afterwards records the read that
    // started before the sibling's write. The latest writer is the reader
    // itself, but the content it holds may predate the sibling's change.
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/own-write-masks.txt");
    let read_started = Instant::now();
    std::thread::sleep(Duration::from_millis(2));
    coord.record_write("sibling", path.clone());
    std::thread::sleep(Duration::from_millis(2));
    coord.record_write("reader", path.clone());
    coord.record_read(
        "reader",
        path.clone(),
        SystemTime::now(),
        false,
        read_started,
    );

    assert_eq!(coord.stale_reads_for_parent("reader"), vec![path.clone()]);
    let msg = coord.check_stale_read("reader", &path);
    assert!(
        msg.as_deref().is_some_and(|m| m.contains("'sibling'")),
        "got: {msg:?}"
    );
}

#[test]
fn a_write_before_the_read_is_not_stale() {
    let coord = fresh_coordinator();
    let path = PathBuf::from("/tmp/test/write-then-read.txt");
    coord.record_write("child-1", path.clone());
    std::thread::sleep(Duration::from_millis(2));
    coord.record_read(
        "parent",
        path.clone(),
        SystemTime::now(),
        false,
        Instant::now(),
    );

    assert_eq!(coord.stale_reads_for_parent("parent").len(), 0);
    assert_eq!(
        coord
            .parent_stale_files("parent", &["child-1".to_string()])
            .len(),
        0
    );
    assert_eq!(coord.check_stale_read("parent", &path), None);
}
