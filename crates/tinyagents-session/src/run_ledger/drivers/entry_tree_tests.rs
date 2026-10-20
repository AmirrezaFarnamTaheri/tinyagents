use super::*;
use crate::entry_tree::{ForkPosition, ForkScope};
use crate::testkit::MemoryStorage;
use crate::transcript::TranscriptMessage;
use tinyagents_storage_kit::ScopedStorage;
use tinystoragedrivers_api::StorageBackend;

fn scoped_for(backend: &dyn StorageBackend, label: &str) -> ScopedStorage {
    let scope = Scope::new(label).unwrap();
    ScopedStorage::new(backend.for_scope(&scope).unwrap(), scope)
}

fn kind(text: &str) -> EntryKind {
    EntryKind::Message(TranscriptMessage::new("user", text))
}

/// A tree grown through append, label and fork keeps the ids, tips, labels and
/// context projection the classic entry tree defined: ids derive from
/// `(session, ordinal)`, a tip is an entry nobody parents, a label is an entry
/// plus a name, and a tree fork copies the ancestor path.
async fn branches_forks_and_context(backend: &dyn StorageBackend) {
    let scoped = scoped_for(backend, "alice");
    let native = DriverRunLedger::new(scoped.clone());
    let peer = DriverRunLedger::new(scoped);
    let at = |ordinal| EntryId::derive("session", ordinal);
    let (root, main, branch_id, label, copy) = (at(0), at(1), at(2), at(3), at(5));

    assert_eq!(
        native
            .entry_append_to_head("session", kind("root"))
            .await
            .unwrap(),
        root
    );
    assert_eq!(
        peer.entry_append_to_head("session", kind("main"))
            .await
            .unwrap(),
        main,
        "a second handle on the same scope continues the same allocator"
    );
    assert_eq!(
        native
            .entry_append("session", Some(&root), kind("branch"))
            .await
            .unwrap(),
        branch_id
    );
    assert_eq!(
        native
            .entry_label("session", &branch_id, "named")
            .await
            .unwrap(),
        label
    );
    let fork = Fork {
        scope: ForkScope::Tree,
        position: ForkPosition::At,
    };
    assert_eq!(
        native.entry_fork("session", &main, fork).await.unwrap(),
        copy,
        "a tree fork copies root and main as entries 4 and 5"
    );
    assert_eq!(
        peer.entry_head("session").await.unwrap(),
        Some(copy.clone())
    );
    assert_eq!(
        native.entry_tips("session").await.unwrap(),
        vec![main.clone(), label.clone(), copy.clone()]
    );
    assert_eq!(
        native.entry_branches("session").await.unwrap(),
        vec![Branch {
            name: "named".into(),
            tip_id: label.clone()
        }]
    );
    native.entry_rebuild_index("session").await.unwrap();
    let texts = |chain: Vec<Entry>| -> Vec<String> {
        chain
            .into_iter()
            .map(|entry| match entry.kind {
                EntryKind::Message(m) => m.content,
                EntryKind::Label(l) => format!("label:{}", l.name),
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    };
    assert_eq!(
        texts(native.entry_ancestor_chain("session", &main).await.unwrap()),
        ["root", "main"]
    );
    assert_eq!(
        texts(
            native
                .entry_ancestor_chain("session", &label)
                .await
                .unwrap()
        ),
        ["root", "branch", "label:named"]
    );
    assert_eq!(
        texts(native.entry_ancestor_chain("session", &copy).await.unwrap()),
        ["root", "main"]
    );
    // The projection skips bookkeeping entries and keeps conversation order.
    for (tip, expected) in [
        (&main, vec!["root", "main"]),
        (&label, vec!["root", "branch"]),
        (&copy, vec!["root", "main"]),
    ] {
        assert_eq!(
            native.entry_build_context("session", tip).await.unwrap(),
            expected.into_iter().map(Message::user).collect::<Vec<_>>()
        );
    }
    assert!(
        peer.entry_get("other-session", &root)
            .await
            .unwrap()
            .is_none()
    );
    let other = DriverRunLedger::new(scoped_for(backend, "bob"));
    assert!(other.entry_head("session").await.unwrap().is_none());
}

#[tokio::test]
async fn memory_entry_tree_branches_forks_and_context() {
    branches_forks_and_context(&MemoryStorage::new()).await;
}

#[tokio::test]
async fn sqlite_entry_tree_branches_forks_and_context() {
    let ws = tempfile::tempdir().unwrap();
    let backend = MemoryStorage::new();
    branches_forks_and_context(&backend).await;
}

#[tokio::test]
async fn import_and_parent_behavior_follow_the_classic_entry_tree_rules() {
    let backend = MemoryStorage::new();
    let native = DriverRunLedger::new(backend.for_scope(&Scope::new("local").unwrap()).unwrap());
    let at = |ordinal| EntryId::derive("s", ordinal);
    // A second root is allowed: `None` is not restricted to the first entry.
    let first = native.entry_append("s", None, kind("root")).await.unwrap();
    assert_eq!(first, at(0));
    let second = native
        .entry_append("s", None, kind("another root"))
        .await
        .unwrap();
    assert_eq!(second, at(1));
    // A parent that does not exist is accepted at write time and refused when
    // the chain is walked, exactly as the classic store did.
    let missing = EntryId::from("missing-parent");
    let dangling = native
        .entry_append("s", Some(&missing), kind("dangling"))
        .await
        .unwrap();
    assert_eq!(dangling, at(2));
    assert!(native.entry_ancestor_chain("s", &dangling).await.is_err());

    // An imported entry keeps its opaque id and ordinal; re-importing the same
    // id is a no-op even when the second copy differs.
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
        native.entry_import_legacy("s", &input).await.unwrap();
    }
    assert_eq!(
        native.entry_get("s", &imported.id).await.unwrap(),
        Some(imported)
    );
    // Allocation continues past the highest imported ordinal, leaving a gap.
    assert_eq!(
        native
            .entry_append_to_head("s", kind("after gap"))
            .await
            .unwrap(),
        at(13)
    );
    assert_eq!(native.entry_head("s").await.unwrap(), Some(at(13)));
    // Empty label names are accepted; a name labels one tip, the newest wins.
    assert_eq!(native.entry_label("s", &second, "").await.unwrap(), at(14));
    assert_eq!(native.entry_label("s", &first, "").await.unwrap(), at(15));
    assert_eq!(
        native.entry_branches("s").await.unwrap(),
        vec![Branch {
            name: String::new(),
            tip_id: at(15)
        }]
    );
    // Forking before a root has nowhere to go; before a dangling entry it
    // lands on the (missing) parent id.
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
async fn compaction_and_pinned_context_project_summary_pinned_then_kept_rows() {
    use crate::entry_tree::CompactionEntry;
    use tinyagents_harness::summarization::SummaryPlacement;
    let backend = MemoryStorage::new();
    let native = DriverRunLedger::new(backend.for_scope(&Scope::new("local").unwrap()).unwrap());
    let mut ids = Vec::new();
    for text in ["old", "pinned", "kept"] {
        ids.push(native.entry_append_to_head("s", kind(text)).await.unwrap());
    }
    for placement in [SummaryPlacement::User, SummaryPlacement::System] {
        let entry = EntryKind::Compaction(CompactionEntry {
            summary: "summary".into(),
            placement,
            first_kept_entry_id: ids[2].clone(),
            tokens_before: 99,
            usage: None,
            details: serde_json::json!({"pinned_entry_id": ids[1]}),
        });
        let tip = native.entry_append_to_head("s", entry).await.unwrap();
        let context = native.entry_build_context("s", &tip).await.unwrap();
        // The summary replaces everything folded; the pinned user row follows
        // it verbatim, then the rows kept after the boundary.
        assert_eq!(context.len(), 3);
        match placement {
            SummaryPlacement::User => assert_eq!(context[0], Message::user("summary")),
            SummaryPlacement::System => assert!(matches!(context[0], Message::System(_))),
        }
        assert_eq!(context[1], Message::user("pinned"));
        assert_eq!(context[2], Message::user("kept"));
    }
}

use serde_json::Value;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tinyagents_storage_kit::{
    CollectionSpec, DocumentStore, Page, Precondition, ReadCheck, Version, Versioned, WriteOp,
    WriteResult,
};
type NativeResult<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
struct TreeBoundary {
    inner: Arc<dyn DocumentStore>,
    barrier: Option<tokio::sync::Barrier>,
    calls: AtomicUsize,
    refuse: bool,
    reject_post_commit_reads: bool,
    committed: AtomicBool,
}
#[async_trait::async_trait]
impl DocumentStore for TreeBoundary {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        self.inner.capabilities()
    }
    async fn ensure_collection(&self, spec: &CollectionSpec) -> NativeResult<()> {
        self.inner.ensure_collection(spec).await
    }
    async fn get(&self, collection: &str, id: &str) -> NativeResult<Option<Versioned<Value>>> {
        if self.reject_post_commit_reads && self.committed.load(Ordering::SeqCst) {
            return Err(StorageError::unavailable("postcommit read refused"));
        }
        self.inner.get(collection, id).await
    }
    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> NativeResult<Version> {
        self.inner.put(collection, id, doc, precondition).await
    }

    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: Precondition,
    ) -> NativeResult<bool> {
        self.inner.delete(collection, id, precondition).await
    }
    async fn query(&self, collection: &str, query: &Query) -> NativeResult<Page<Versioned<Value>>> {
        if self.reject_post_commit_reads && self.committed.load(Ordering::SeqCst) {
            return Err(StorageError::unavailable("postcommit query refused"));
        }
        self.inner.query(collection, query).await
    }
    async fn count(&self, collection: &str, filter: &Filter) -> NativeResult<u64> {
        self.inner.count(collection, filter).await
    }
    async fn delete_where(&self, collection: &str, filter: &Filter) -> NativeResult<u64> {
        self.inner.delete_where(collection, filter).await
    }
    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> NativeResult<Option<Versioned<Value>>> {
        self.inner.claim(collection, filter, sort, patch).await
    }
    async fn drop_collection(&self, collection: &str) -> NativeResult<()> {
        self.inner.drop_collection(collection).await
    }
    async fn atomic_batch_checked(
        &self,
        checks: Vec<ReadCheck>,
        writes: Vec<WriteOp>,
    ) -> NativeResult<Vec<WriteResult>> {
        let changes = writes.iter().any(|write| matches!(write, WriteOp::Put { collection, .. } if collection == entry_tree::HEADS));
        if changes {
            if self.refuse {
                return Err(StorageError::unavailable("tree commit refused")
                    .with_source(std::io::Error::other("native tree source")));
            }
            if let Some(barrier) = &self.barrier
                && self.calls.fetch_add(1, Ordering::SeqCst) < 2
            {
                barrier.wait().await;
            }
        }
        let result = self.inner.atomic_batch_checked(checks, writes).await?;
        if changes {
            self.committed.store(true, Ordering::SeqCst);
        }
        Ok(result)
    }
}

fn boundary(
    native: &ScopedStorage,
    barrier: bool,
    refuse: bool,
    reject_post_commit_reads: bool,
) -> ScopedStorage {
    crate::testkit::with_documents(
        native,
        Arc::new(TreeBoundary {
            inner: native.documents().clone(),
            barrier: barrier.then(|| tokio::sync::Barrier::new(2)),
            calls: AtomicUsize::new(0),
            refuse,
            reject_post_commit_reads,
            committed: AtomicBool::new(false),
        }),
    )
}

#[tokio::test]
async fn competing_entry_allocators_retry_without_losing_parent_or_ordinal() {
    let backend = MemoryStorage::new();
    let native = backend.for_scope(&Scope::new("local").unwrap()).unwrap();
    let wrapped = boundary(&native, true, false, false);
    let a = DriverRunLedger::new(wrapped.clone());
    let b = DriverRunLedger::new(wrapped);
    a.declare().await.unwrap();
    b.declare().await.unwrap();
    let (a, b) = tokio::join!(
        a.entry_append_to_head("s", kind("a")),
        b.entry_append_to_head("s", kind("b"))
    );
    let mut ids = vec![a.unwrap(), b.unwrap()];
    ids.sort();
    assert_eq!(ids, vec![EntryId::derive("s", 0), EntryId::derive("s", 1)]);
    let reader = DriverRunLedger::new(native);
    let head = reader.entry_head("s").await.unwrap().unwrap();
    let chain = reader.entry_ancestor_chain("s", &head).await.unwrap();
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[1].parent_id, Some(chain[0].id.clone()));
}

#[tokio::test]
async fn tree_batch_refusal_preserves_entries_labels_head_and_native_source() {
    use tinystoragedrivers_types::ErrorKind;
    let backend = MemoryStorage::new();
    let native = backend.for_scope(&Scope::new("local").unwrap()).unwrap();
    let reader = DriverRunLedger::new(native.clone());
    let root = reader
        .entry_append_to_head("s", kind("root"))
        .await
        .unwrap();
    let denied = DriverRunLedger::new(boundary(&native, false, true, false));
    for result in [
        denied.entry_label("s", &root, "label").await,
        denied
            .entry_fork(
                "s",
                &root,
                Fork {
                    scope: ForkScope::Tree,
                    position: ForkPosition::At,
                },
            )
            .await,
    ] {
        let error = result.unwrap_err();
        assert!(
            matches!(&error, super::super::LedgerError::Driver(native) if native.kind() == ErrorKind::Unavailable)
        );
        assert_eq!(
            std::error::Error::source(&error).unwrap().to_string(),
            "native tree source"
        );
    }
    assert_eq!(reader.entry_head("s").await.unwrap(), Some(root.clone()));
    assert_eq!(reader.entry_tips("s").await.unwrap(), vec![root]);
    assert!(reader.entry_branches("s").await.unwrap().is_empty());
}

#[tokio::test]
async fn successful_tree_commit_never_performs_fallible_postcommit_reads() {
    let backend = MemoryStorage::new();
    let native = backend.for_scope(&Scope::new("local").unwrap()).unwrap();
    let bound = DriverRunLedger::new(boundary(&native, false, false, true));
    let id = bound
        .entry_append_to_head("s", kind("committed"))
        .await
        .unwrap();
    let reader = DriverRunLedger::new(native);
    assert_eq!(reader.entry_head("s").await.unwrap(), Some(id));
}

#[tokio::test]
async fn import_keeps_insertion_order_for_entries_sharing_an_ordinal() {
    let backend = MemoryStorage::new();
    let native = DriverRunLedger::new(backend.for_scope(&Scope::new("local").unwrap()).unwrap());
    let entries: Vec<_> = ["z-first", "a-last"]
        .into_iter()
        .map(|id| Entry {
            id: EntryId::from(id),
            parent_id: None,
            ordinal: 7,
            kind: kind(id),
            ts: "2025-01-01T00:00:00Z".into(),
        })
        .collect();
    native.entry_import_legacy("s", &entries).await.unwrap();
    // Ties on the ordinal fall back to insertion order, not to the id.
    assert_eq!(
        native.entry_tips("s").await.unwrap(),
        vec![EntryId::from("z-first"), EntryId::from("a-last")]
    );
    // The later insertion at an equal ordinal is the head (`rowid DESC`).
    assert_eq!(
        native.entry_head("s").await.unwrap(),
        Some(EntryId::from("a-last"))
    );
}

#[tokio::test]
async fn imported_cycle_and_native_ordinal_exhaustion_refuse_without_partial_entries() {
    use tinystoragedrivers_types::ErrorKind;
    let backend = MemoryStorage::new();
    let native = DriverRunLedger::new(backend.for_scope(&Scope::new("local").unwrap()).unwrap());
    let cycle = Entry {
        id: EntryId::from("cycle"),
        parent_id: Some(EntryId::from("cycle")),
        ordinal: 0,
        kind: kind("cycle"),
        ts: "2025-01-01T00:00:00Z".into(),
    };
    native
        .entry_import_legacy("cycle-session", std::slice::from_ref(&cycle))
        .await
        .unwrap();
    assert!(
        matches!(native.entry_ancestor_chain("cycle-session", &cycle.id).await.unwrap_err(), super::super::LedgerError::Driver(error) if error.kind() == ErrorKind::Serialization)
    );
    let last = Entry {
        id: EntryId::from("last"),
        parent_id: None,
        ordinal: i64::MAX as u64,
        kind: kind("last"),
        ts: cycle.ts,
    };
    native
        .entry_import_legacy("full", std::slice::from_ref(&last))
        .await
        .unwrap();
    assert!(
        matches!(native.entry_append_to_head("full", kind("overflow")).await.unwrap_err(), super::super::LedgerError::Driver(error) if error.kind() == ErrorKind::InvalidInput)
    );
    assert_eq!(
        native.entry_get("full", &last.id).await.unwrap(),
        Some(last.clone())
    );
    assert_eq!(native.entry_head("full").await.unwrap(), Some(last.id));
}

#[tokio::test]
async fn terminal_entry_allocators_remain_representable_in_native_bson() {
    let backend = MemoryStorage::new();
    let native =
        super::event_bson_tests::wrap(backend.for_scope(&Scope::new("local").unwrap()).unwrap());
    native.declare().await.unwrap();
    native
        .scoped
        .documents()
        .put(
            super::entry_tree::HEADS,
            &super::storage::key(&["append"]).unwrap(),
            serde_json::json!({
                "session_id": "append", "next_ordinal": i64::MAX,
                "next_insertion": 0, "tip": null
            }),
            tinystoragedrivers_types::Precondition::Absent,
        )
        .await
        .unwrap();
    let last = native
        .entry_append_to_head("append", kind("last"))
        .await
        .unwrap();
    assert_eq!(
        native
            .entry_get("append", &last)
            .await
            .unwrap()
            .unwrap()
            .ordinal,
        i64::MAX as u64
    );
    let imported = Entry {
        id: EntryId::from("legacy-last"),
        parent_id: None,
        ordinal: i64::MAX as u64,
        kind: kind("imported"),
        ts: "2025-01-01T00:00:00Z".into(),
    };
    native
        .entry_import_legacy("import", std::slice::from_ref(&imported))
        .await
        .unwrap();
    for session in ["append", "import"] {
        let row = native
            .scoped
            .documents()
            .get(
                super::entry_tree::HEADS,
                &super::storage::key(&[session]).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(row.doc["next_ordinal"].is_null());
        assert!(
            matches!(native.entry_append_to_head(session, kind("overflow")).await.unwrap_err(), super::super::LedgerError::Driver(error) if error.kind() == tinystoragedrivers_types::ErrorKind::InvalidInput)
        );
    }
    assert_eq!(
        native.entry_get("import", &imported.id).await.unwrap(),
        Some(imported)
    );
}
