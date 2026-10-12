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

#[tokio::test]
async fn native_import_and_parent_behavior_matches_actual_classic_store() {
    let ws = tempfile::tempdir().unwrap();
    let classic = EntryTree::new(ws.path(), "s");
    let backend = MemoryStorage::new();
    let native = DriverRunLedger::new(backend.for_scope(&Scope::local()).unwrap());
    let first = classic.append(None, kind("root")).unwrap();
    assert_eq!(
        native.entry_append("s", None, kind("root")).await.unwrap(),
        first
    );
    let second = classic.append(None, kind("another root")).unwrap();
    assert_eq!(
        native
            .entry_append("s", None, kind("another root"))
            .await
            .unwrap(),
        second
    );
    let missing = EntryId::from("missing-parent");
    let dangling = classic.append(Some(&missing), kind("dangling")).unwrap();
    assert_eq!(
        native
            .entry_append("s", Some(&missing), kind("dangling"))
            .await
            .unwrap(),
        dangling
    );
    assert!(classic.ancestor_chain(&dangling).is_err());
    assert!(native.entry_ancestor_chain("s", &dangling).await.is_err());
    let imported = Entry {
        id: EntryId::from("opaque-import-id"),
        parent_id: Some(first.clone()),
        ordinal: 12,
        kind: kind("import"),
        ts: "2025-01-01T00:00:00Z".into(),
    };
    let mut changed = imported.clone();
    changed.kind = kind("ignored on repeated import");
    for input in [vec![imported.clone()], vec![changed]] {
        classic.import_legacy(&input).unwrap();
        native.entry_import_legacy("s", &input).await.unwrap();
    }
    assert_eq!(
        native.entry_get("s", &imported.id).await.unwrap(),
        classic.get(&imported.id).unwrap()
    );
    assert_eq!(
        native
            .entry_append_to_head("s", kind("after gap"))
            .await
            .unwrap(),
        classic.append_to_head(kind("after gap")).unwrap()
    );
    assert_eq!(
        native.entry_head("s").await.unwrap(),
        Some(EntryId::derive("s", 13))
    );
    let relabel = classic.label(&second, "").unwrap();
    assert_eq!(native.entry_label("s", &second, "").await.unwrap(), relabel);
    let relabel2 = classic.label(&first, "").unwrap();
    assert_eq!(native.entry_label("s", &first, "").await.unwrap(), relabel2);
    assert_eq!(
        native.entry_branches("s").await.unwrap(),
        classic.branches().unwrap()
    );
    assert!(
        native
            .entry_fork(
                "s",
                &first,
                Fork {
                    scope: ForkScope::Branch,
                    position: ForkPosition::Before
                }
            )
            .await
            .is_err()
    );
    assert_eq!(
        native
            .entry_fork(
                "s",
                &dangling,
                Fork {
                    scope: ForkScope::Branch,
                    position: ForkPosition::Before
                }
            )
            .await
            .unwrap(),
        missing
    );
}

#[tokio::test]
async fn native_compaction_and_pinned_context_matches_classic_projection() {
    use crate::entry_tree::CompactionEntry;
    use tinyagents_harness::summarization::SummaryPlacement;
    let ws = tempfile::tempdir().unwrap();
    let classic = EntryTree::new(ws.path(), "s");
    let backend = MemoryStorage::new();
    let native = DriverRunLedger::new(backend.for_scope(&Scope::local()).unwrap());
    let mut ids = Vec::new();
    for text in ["old", "pinned", "kept"] {
        let id = classic.append_to_head(kind(text)).unwrap();
        assert_eq!(
            native.entry_append_to_head("s", kind(text)).await.unwrap(),
            id
        );
        ids.push(id);
    }
    for placement in [SummaryPlacement::User, SummaryPlacement::System] {
        let entry = EntryKind::Compaction(CompactionEntry {
            summary: "summary".into(),
            placement,
            first_kept_entry_id: ids[2].clone(),
            tokens_before: 99,
            usage: None,
            details: serde_json::json!({"pinnedEntryId": ids[1]}),
        });
        let tip = classic.append_to_head(entry.clone()).unwrap();
        assert_eq!(native.entry_append_to_head("s", entry).await.unwrap(), tip);
        assert_eq!(
            native.entry_build_context("s", &tip).await.unwrap(),
            classic.build_context(&tip).unwrap()
        );
    }
}

#[tokio::test]
async fn retired_generation_cannot_read_or_append_fresh_entry_tree() {
    use tinystoragedrivers_core::{ErrorKind, PurgeMode, PurgeRequest};
    let backend = MemoryStorage::new();
    let scope = Scope::new("alice").unwrap();
    let scoped = backend.for_scope(&scope).unwrap();
    let old = DriverRunLedger::new(scoped.clone());
    old.entry_append_to_head("s", kind("old")).await.unwrap();
    let receipt = scoped
        .purge(&PurgeRequest {
            operation_id: "reset-tree".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap();
    let fresh = DriverRunLedger::new(
        backend
            .for_scope_generation(&scope, receipt.generation)
            .unwrap(),
    );
    let head = fresh
        .entry_append_to_head("s", kind("fresh"))
        .await
        .unwrap();
    for result in [
        old.entry_head("s").await.map(|_| ()),
        old.entry_append_to_head("s", kind("stale"))
            .await
            .map(|_| ()),
    ] {
        assert!(
            matches!(result.unwrap_err(), super::super::LedgerError::Driver(native) if native.kind() == ErrorKind::Fenced)
        );
    }
    assert_eq!(fresh.entry_head("s").await.unwrap(), Some(head));
}
