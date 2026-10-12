use super::*;
use crate::entry_tree::{EntryTree, ForkPosition, ForkScope};
use crate::transcript::TranscriptMessage;
use tinystoragedrivers_core::{MemoryStorage, StorageBackend};

fn kind(text: &str) -> EntryKind {
    EntryKind::Message(TranscriptMessage::new("user", text))
}

async fn parity(backend: &dyn StorageBackend) {
    let ws = tempfile::tempdir().unwrap();
    let classic = EntryTree::new(ws.path(), "session");
    let scoped = backend.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let native = DriverRunLedger::new(scoped.clone());
    let peer = DriverRunLedger::new(scoped);
    let root = classic.append_to_head(kind("root")).unwrap();
    assert_eq!(
        native
            .entry_append_to_head("session", kind("root"))
            .await
            .unwrap(),
        root
    );
    let main = classic.append_to_head(kind("main")).unwrap();
    assert_eq!(
        peer.entry_append_to_head("session", kind("main"))
            .await
            .unwrap(),
        main
    );
    let branch = classic.append(Some(&root), kind("branch")).unwrap();
    assert_eq!(
        native
            .entry_append("session", Some(&root), kind("branch"))
            .await
            .unwrap(),
        branch
    );
    let label = classic.label(&branch, "named").unwrap();
    assert_eq!(
        native
            .entry_label("session", &branch, "named")
            .await
            .unwrap(),
        label
    );
    let fork = Fork {
        scope: ForkScope::Tree,
        position: ForkPosition::At,
    };
    let copy = classic.fork(&main, fork).unwrap();
    assert_eq!(
        native.entry_fork("session", &main, fork).await.unwrap(),
        copy
    );
    assert_eq!(
        peer.entry_head("session").await.unwrap(),
        classic.head().unwrap()
    );
    assert_eq!(
        native.entry_tips("session").await.unwrap(),
        classic.tips().unwrap()
    );
    assert_eq!(
        native.entry_branches("session").await.unwrap(),
        classic.branches().unwrap()
    );
    classic.rebuild_index().unwrap();
    native.entry_rebuild_index("session").await.unwrap();
    for tip in [&main, &label, &copy] {
        let mut expected = classic.ancestor_chain(tip).unwrap();
        let mut actual = native.entry_ancestor_chain("session", tip).await.unwrap();
        for entry in &mut expected {
            entry.ts.clear();
        }
        for entry in &mut actual {
            entry.ts.clear();
        }
        assert_eq!(actual, expected);
        assert_eq!(
            native.entry_build_context("session", tip).await.unwrap(),
            classic.build_context(tip).unwrap()
        );
    }
    assert!(
        peer.entry_get("other-session", &root)
            .await
            .unwrap()
            .is_none()
    );
    let other = DriverRunLedger::new(backend.for_scope(&Scope::new("bob").unwrap()).unwrap());
    assert!(other.entry_head("session").await.unwrap().is_none());
}

#[tokio::test]
async fn memory_native_entry_tree_matches_classic_branches_forks_and_context() {
    parity(&MemoryStorage::new()).await;
}

#[tokio::test]
async fn sqlite_native_entry_tree_matches_classic_branches_forks_and_context() {
    let ws = tempfile::tempdir().unwrap();
    let backend =
        tinystoragedrivers_sqlite::SqliteStorage::open(ws.path().join("native.sqlite")).unwrap();
    parity(&backend).await;
}
