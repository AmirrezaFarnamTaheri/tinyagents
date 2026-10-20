//! Host-bound settings persistence and legacy import tests.

use serde::Serialize;

use super::*;
use tinyagents_storage_kit::{Scope, StorageBackend};
use tinystoragedrivers_testkit::MemoryBackend;

fn settings_store() -> (tinyagents_storage_kit::ScopedStorage, Kv) {
    let scoped = MemoryBackend::new()
        .for_scope(&Scope::new("claude-code-settings-test").unwrap())
        .unwrap();
    let kv = store(scoped.documents()).unwrap();
    (scoped, kv)
}

#[tokio::test]
async fn missing_settings_return_the_safe_default() {
    let (_scoped, kv) = settings_store();
    assert!(!load(&kv).await.unwrap().full_access);
}

#[tokio::test]
async fn save_then_load_roundtrips_settings() {
    let (_scoped, kv) = settings_store();
    save(&kv, &ClaudeCodeSettings { full_access: true })
        .await
        .unwrap();
    assert!(load(&kv).await.unwrap().full_access);
    save(&kv, &ClaudeCodeSettings { full_access: false })
        .await
        .unwrap();
    assert!(!load(&kv).await.unwrap().full_access);
}

#[tokio::test]
async fn legacy_settings_import_only_fills_an_empty_target() {
    let (_scoped, kv) = settings_store();
    // The old writer used serde_json::to_vec_pretty for this DTO. Keep its
    // serialized output as a fixture from the released file format.
    #[derive(Serialize)]
    struct ReleasedSettings {
        full_access: bool,
    }
    let serialized_by_released_writer =
        serde_json::to_vec_pretty(&ReleasedSettings { full_access: true }).unwrap();
    let old_release_fixture = include_bytes!("fixtures/claude_code_settings.json");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&serialized_by_released_writer).unwrap(),
        serde_json::from_slice::<serde_json::Value>(old_release_fixture).unwrap()
    );
    assert!(import_legacy(&kv, old_release_fixture).await.unwrap());
    assert!(load(&kv).await.unwrap().full_access);
    assert!(
        !import_legacy(&kv, br#"{"full_access":false}"#)
            .await
            .unwrap()
    );
    assert!(load(&kv).await.unwrap().full_access);
}

#[tokio::test]
async fn malformed_legacy_settings_are_rejected_without_changing_state() {
    let (_scoped, kv) = settings_store();
    assert!(import_legacy(&kv, b"{broken").await.is_err());
    assert!(!load(&kv).await.unwrap().full_access);
}
