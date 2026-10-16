use super::*;
use serde_json::{Value, json};
use tinystoragedrivers_core::{
    Filter, MemoryStorage, MigrateOptions, Precondition, Registry, Scope, ScopedStorage,
    StorageBackend, WriteOptions,
};

fn scoped() -> ScopedStorage {
    MemoryStorage::new().for_scope(&Scope::local()).unwrap()
}

async fn put_v1(storage: &ScopedStorage, collection: &str, id: &str, body: Value) {
    storage
        .documents()
        .put_with(
            collection,
            id,
            body,
            Precondition::None,
            WriteOptions::new().created_ms(1_700_000_000_000),
        )
        .await
        .unwrap();
}

async fn migrate(storage: &ScopedStorage) -> tinystoragedrivers_core::MigrationReport {
    let registry = Registry::compose([storage_schema()]).unwrap();
    // `prepare` needs a backend; the scoped handle's documents are enough here.
    for spec in tinystoragedrivers_core::primitives::collection_specs() {
        storage.documents().ensure_collection(&spec).await.unwrap();
    }
    for item in storage_schema().items {
        if let tinystoragedrivers_core::Item::Collection { spec, .. } = item {
            storage.documents().ensure_collection(&spec).await.unwrap();
        }
    }
    registry
        .migrate(storage, &MigrateOptions::new())
        .await
        .unwrap()
}

#[test]
fn the_schema_composes_and_names_everything_the_harness_owns() {
    let schema = storage_schema();
    assert_eq!(schema.owner.as_str(), "harness");
    let names: Vec<&str> = schema.items.iter().map(|item| item.name()).collect();
    assert_eq!(
        names,
        [
            "harness.run_status",
            "harness.embedding_cache",
            "harness.attachment_stash",
            "harness/attachments/",
            "harness/tool_results/",
            "harness/outputs/",
            "harness/workspace/",
            "harness/generated_media/",
        ]
    );
    assert!(
        schema
            .items
            .iter()
            .all(|item| item.scope() == tinystoragedrivers_core::ScopeClass::Agent)
    );
    Registry::compose([schema]).unwrap();
}

#[tokio::test]
async fn run_status_moves_to_a_bare_snake_case_document_under_its_run_id() {
    let storage = scoped();
    put_v1(
        &storage,
        "harness_run_status",
        "8f3a-hash",
        json!({
            "runId": "run.abc", "rootRunId": "run.root", "threadId": "t-1", "active": true,
            "record": {
                "run_id": "run.abc", "parent_run_id": "run.root", "root_run_id": "run.root",
                "thread_id": "t-1", "component": "model", "status": "running",
                "current_phase": "model", "model_calls": 2, "tool_calls": 1,
                "active_tool_calls": [],
                "usage": {}, "cost": {},
                "started_at": 1_700_000_000_u64, "updated_at": 1_700_000_010_u64,
                "metadata": {"k": "v"}
            }
        }),
    )
    .await;
    // A record from an incompatible build is dropped, not fatal.
    put_v1(
        &storage,
        "harness_run_status",
        "bad",
        json!({"runId": "x", "record": {"nope": 1}}),
    )
    .await;

    let report = migrate(&storage).await;
    assert!(report.ran.iter().any(|step| step.step == "run_status_v2"));

    let row = storage
        .documents()
        .get("harness.run_status", "run.abc")
        .await
        .unwrap()
        .expect("the id is the run id");
    assert_eq!(row.doc["run_id"], "run.abc");
    assert_eq!(row.doc["root_run_id"], "run.root");
    assert_eq!(row.doc["parent_run_id"], "run.root");
    assert_eq!(row.doc["thread_id"], "t-1");
    assert_eq!(row.doc["active"], true);
    assert_eq!(row.doc["started_ms"], 1_700_000_000_000_u64);
    assert_eq!(row.doc["updated_ms"], 1_700_000_010_000_u64);
    assert!(row.doc.get("record").is_none());
    assert!(row.doc.get("runId").is_none());
    assert_eq!(row.meta.created_ms, 1_700_000_000_000, "history keeps its time");
    assert_eq!(row.meta.schema, 2);
    let count = storage
        .documents()
        .count("harness.run_status", &Filter::default())
        .await
        .unwrap();
    assert_eq!(count, 1, "the undecodable v1 record is dropped");
    let left = storage
        .documents()
        .count("harness_run_status", &Filter::default())
        .await
        .unwrap();
    assert_eq!(left, 0, "the v1 rows are removed");
}

#[tokio::test]
async fn embedding_vectors_move_to_a_key_of_signature_and_hex() {
    let storage = scoped();
    put_v1(
        &storage,
        "harness_embedding_cache",
        "sha256ofparts",
        json!({"sig": "provider=bag;dims=3", "key": "00000000000000ab", "vector": [1.0, 2.0, 3.0]}),
    )
    .await;
    migrate(&storage).await;
    let row = storage
        .documents()
        .get("harness.embedding_cache", "provider=bag;dims=3/00000000000000ab")
        .await
        .unwrap()
        .expect("Key::of([sig, key])");
    assert_eq!(row.doc["vector"], json!([1.0, 2.0, 3.0]));
    assert_eq!(row.meta.schema, 2);
}

#[tokio::test]
async fn attachment_index_rows_gain_snake_case_fields_and_the_new_blob_key() {
    let storage = scoped();
    put_v1(
        &storage,
        "attachment_stash_index",
        "abc123",
        json!({"id": "abc123", "key": "attachments/abc123", "mime": "image/png", "len": 68,
               "touchedAtMs": 1_700_000_500_000_u64}),
    )
    .await;
    migrate(&storage).await;
    let row = storage
        .documents()
        .get("harness.attachment_stash", "abc123")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.doc,
        json!({"id": "abc123", "key": "harness/attachments/abc123", "mime": "image/png",
               "len": 68, "touched_ms": 1_700_000_500_000_u64})
    );
}

#[tokio::test]
async fn blobs_move_under_harness_with_their_bytes_and_content_type() {
    let storage = scoped();
    let blobs = storage.blobs();
    for (key, bytes) in [
        ("attachments/abc", &b"png"[..]),
        ("tool-results/s/shell/c.txt", b"out"),
        ("outputs/report.md", b"# r"),
        ("workspace/notes.md", b"n"),
        ("generated-media/clip.png", b"img"),
        ("elsewhere/keep.txt", b"keep"),
    ] {
        blobs
            .put(key, bytes.to_vec(), Some("application/test"))
            .await
            .unwrap();
    }
    migrate(&storage).await;
    for (key, bytes) in [
        ("harness/attachments/abc", &b"png"[..]),
        ("harness/tool_results/s/shell/c.txt", b"out"),
        ("harness/outputs/report.md", b"# r"),
        ("harness/workspace/notes.md", b"n"),
        ("harness/generated_media/clip.png", b"img"),
    ] {
        let blob = blobs.get(key).await.unwrap().unwrap_or_else(|| panic!("{key}"));
        assert_eq!(blob.bytes, bytes, "{key}");
        assert_eq!(blob.meta.content_type.as_deref(), Some("application/test"));
    }
    for old in [
        "attachments/abc",
        "tool-results/s/shell/c.txt",
        "outputs/report.md",
        "workspace/notes.md",
        "generated-media/clip.png",
    ] {
        assert!(blobs.head(old).await.unwrap().is_none(), "{old} is moved");
    }
    assert!(blobs.head("elsewhere/keep.txt").await.unwrap().is_some());
}

#[tokio::test]
async fn migrating_twice_changes_nothing() {
    let storage = scoped();
    put_v1(
        &storage,
        "attachment_stash_index",
        "a",
        json!({"id": "a", "key": "attachments/a", "mime": "image/png", "len": 1, "touchedAtMs": 5}),
    )
    .await;
    migrate(&storage).await;
    let again = migrate(&storage).await;
    assert!(again.ran.is_empty());
    assert_eq!(again.already_done, storage_schema().steps.len());
}
