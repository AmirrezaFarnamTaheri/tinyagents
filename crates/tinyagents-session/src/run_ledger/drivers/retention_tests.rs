use super::*;
use chrono::Duration;
use tinystoragedrivers_core::{MemoryStorage, Precondition, StorageBackend};

async fn seed(ledger: &DriverRunLedger, id: &str, at: DateTime<Utc>) {
    let mut row = ledger
        .upsert_run_telemetry(RunTelemetryUpsert {
            run_id: id.into(),
            input_tokens: Some(17),
            ..Default::default()
        })
        .await
        .unwrap();
    row.updated_at = at;
    ledger
        .scoped
        .documents()
        .put(
            TELEMETRY,
            &storage::key(&[id]),
            storage::encode(&row).unwrap(),
            Precondition::None,
        )
        .await
        .unwrap();
}

async fn prunes_only_old_telemetry(backend: &dyn StorageBackend) {
    let a = DriverRunLedger::new(backend.for_scope(&Scope::new("a").unwrap()).unwrap());
    let peer = DriverRunLedger::new(backend.for_scope(&Scope::new("a").unwrap()).unwrap());
    let sibling = DriverRunLedger::new(backend.for_scope(&Scope::new("b").unwrap()).unwrap());
    let cutoff = DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    seed(&a, "old", cutoff - Duration::seconds(1)).await;
    seed(&a, "boundary", cutoff).await;
    seed(&a, "new", cutoff + Duration::seconds(1)).await;
    seed(&sibling, "old", cutoff - Duration::seconds(1)).await;
    assert_eq!(a.prune_run_telemetry_before(cutoff).await.unwrap(), 1);
    assert!(peer.get_run_telemetry("old").await.unwrap().is_none());
    assert_eq!(
        peer.get_run_telemetry("boundary")
            .await
            .unwrap()
            .unwrap()
            .input_tokens,
        17
    );
    assert!(peer.get_run_telemetry("new").await.unwrap().is_some());
    assert!(sibling.get_run_telemetry("old").await.unwrap().is_some());
    assert_eq!(peer.prune_run_telemetry_before(cutoff).await.unwrap(), 0);
}

#[tokio::test]
async fn memory_telemetry_retention_preserves_cutoff_and_other_scopes() {
    prunes_only_old_telemetry(&MemoryStorage::new()).await;
}

#[tokio::test]
async fn sqlite_telemetry_retention_preserves_cutoff_and_other_scopes() {
    let root = tempfile::tempdir().unwrap();
    let backend =
        tinystoragedrivers_sqlite::SqliteStorage::open(root.path().join("ledger.sqlite")).unwrap();
    prunes_only_old_telemetry(&backend).await;
}
