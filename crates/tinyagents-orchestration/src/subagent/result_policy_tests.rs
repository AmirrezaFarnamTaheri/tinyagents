use std::sync::Mutex;

use serde_json::json;

use super::*;

#[test]
fn truncate_keeps_head_and_tail_and_stays_within_the_cap_including_the_marker() {
    let text = "0123456789".repeat(10);
    let (out, omitted) = truncate_head_tail(&text, 60);
    assert!(out.chars().count() <= 60, "{out}");
    assert!(out.starts_with("0123") && out.ends_with("6789"));
    assert!(
        out.contains(&format!("[… {omitted} chars omitted …]")),
        "{out}"
    );
    assert!(omitted > 40);
}

#[test]
fn truncate_is_a_noop_within_the_cap_and_counts_chars_not_bytes() {
    assert_eq!(truncate_head_tail("héllo", 5), ("héllo".to_owned(), 0));
    let (out, omitted) = truncate_head_tail(&"é".repeat(100), 60);
    assert!(omitted > 0 && out.chars().count() <= 60);
    assert!(out.starts_with("éé") && out.ends_with("éé"));
}

#[test]
fn a_cap_smaller_than_the_marker_hard_cuts_within_the_cap() {
    let (out, omitted) = truncate_head_tail("0123456789", 4);
    assert_eq!(out, "0123");
    assert_eq!(omitted, 6);
    let (out, omitted) = truncate_head_tail("héllo wörld", 0);
    assert_eq!((out.as_str(), omitted), ("", 11));
}

#[test]
fn task_ids_are_safe_single_filename_components() {
    for safe in ["sub-123", "subsess_ab12", "short_test"] {
        assert!(is_safe_task_id(safe), "{safe}");
    }
    let too_long = "x".repeat(129);
    for unsafe_id in ["", "../escape", "a/b", "a\\b", ".", "naïve", &too_long] {
        assert!(!is_safe_task_id(unsafe_id), "{unsafe_id}");
    }
}

#[test]
fn tool_call_only_payloads_are_not_accepted_as_results() {
    assert!(is_unexecuted_tool_call_stub(
        r#"{"tool_calls":[{"id":"1","type":"function","function":{"name":"search","arguments":"{}"}}]}"#
    ));
    assert!(!is_unexecuted_tool_call_stub(
        "I searched and found three useful pages."
    ));
    assert!(frame_unexecuted_outcome("delegate", "timed out").contains("did not complete"));
}

#[tokio::test]
async fn the_default_policy_changes_nothing() {
    let applied = ResultPolicy::default()
        .apply("t", "x".repeat(10_000).as_str(), None)
        .await;
    assert_eq!(applied.text.len(), 10_000);
    assert_eq!(applied.omitted_chars, 0);
    assert!(applied.artifact.is_none() && applied.schema_error.is_none());
}

#[tokio::test]
async fn truncate_overflow_caps_the_visible_text() {
    let policy = ResultPolicy::new().with_max_chars(60);
    let applied = policy.apply("t", &"x".repeat(200), None).await;
    assert!(applied.omitted_chars > 100);
    assert!(applied.text.chars().count() <= 60);
    assert!(applied.text.contains("omitted"));
    assert!(applied.artifact.is_none());
}

struct MemoryStore(Mutex<Vec<(String, String)>>);

#[async_trait::async_trait]
impl ArtifactStore for MemoryStore {
    async fn store(&self, task_id: &str, content: &str) -> Result<ArtifactReference, String> {
        let mut stored = self.0.lock().unwrap();
        stored.push((task_id.to_owned(), content.to_owned()));
        Ok(ArtifactReference {
            id: format!("artifact-{}", stored.len()),
            media_type: Some("text/plain".into()),
            ..ArtifactReference::default()
        })
    }
}

#[tokio::test]
async fn byte_overflow_stores_without_a_character_cap_and_keeps_the_full_text() {
    let store = Arc::new(MemoryStore(Mutex::new(Vec::new())));
    let full = "éclair";
    let applied = ResultPolicy::new()
        .with_max_bytes(full.len() - 1)
        .with_overflow(ResultOverflow::Artifact)
        .with_artifact_store(store.clone())
        .apply("task-bytes", full, None)
        .await;

    assert_eq!(applied.text, full);
    assert_eq!(applied.omitted_chars, 0);
    assert_eq!(applied.artifact.as_ref().unwrap().id, "artifact-1");
    assert_eq!(
        store.0.lock().unwrap().as_slice(),
        &[("task-bytes".to_owned(), full.to_owned())]
    );
}

#[tokio::test]
async fn byte_overflow_uses_strict_utf8_byte_boundaries() {
    let store = Arc::new(MemoryStore(Mutex::new(Vec::new())));
    let policy = ResultPolicy::new()
        .with_max_bytes("é".len())
        .with_overflow(ResultOverflow::Artifact)
        .with_artifact_store(store.clone());
    let at_boundary = policy.apply("t", "é", None).await;
    assert!(at_boundary.artifact.is_none());
    assert!(store.0.lock().unwrap().is_empty());

    let over_boundary = policy.apply("t", "éx", None).await;
    assert!(over_boundary.artifact.is_some());
    assert_eq!(store.0.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn character_overflow_stores_once_when_byte_limit_is_not_exceeded() {
    let full = "é0123456789";
    for max_bytes in [1, 100] {
        let store = Arc::new(MemoryStore(Mutex::new(Vec::new())));
        let applied = ResultPolicy::new()
            .with_max_bytes(max_bytes)
            .with_max_chars(6)
            .with_overflow(ResultOverflow::Artifact)
            .with_artifact_store(store.clone())
            .apply("task-both", full, None)
            .await;

        assert_eq!(applied.artifact.as_ref().unwrap().id, "artifact-1");
        assert_eq!(
            applied.omitted_chars,
            full.chars().count() - applied.text.chars().count()
        );
        assert!(applied.text.chars().count() <= 6);
        assert_eq!(
            store.0.lock().unwrap().as_slice(),
            &[("task-both".to_owned(), full.to_owned())]
        );
    }
}

#[tokio::test]
async fn byte_overflow_without_a_store_keeps_text_and_reports_the_store_error() {
    let full = "0123456789";
    let applied = ResultPolicy::new()
        .with_max_bytes(2)
        .with_overflow(ResultOverflow::Artifact)
        .apply("t", full, None)
        .await;

    assert_eq!(applied.text, full);
    assert_eq!(applied.omitted_chars, 0);
    assert!(applied.artifact.is_none());
    assert!(
        applied
            .artifact_error
            .unwrap()
            .contains("no artifact store")
    );
}

struct FailingStore;

#[async_trait::async_trait]
impl ArtifactStore for FailingStore {
    async fn store(&self, _task_id: &str, _content: &str) -> Result<ArtifactReference, String> {
        Err("disk unavailable".to_owned())
    }
}

#[tokio::test]
async fn byte_overflow_store_failure_preserves_full_or_character_capped_text() {
    let store = Arc::new(FailingStore);
    let full = "0123456789";
    let byte_only = ResultPolicy::new()
        .with_max_bytes(2)
        .with_overflow(ResultOverflow::Artifact)
        .with_artifact_store(store.clone())
        .apply("t", full, None)
        .await;
    assert_eq!(byte_only.text, full);
    assert_eq!(byte_only.omitted_chars, 0);
    assert!(byte_only.artifact.is_none());
    assert!(
        byte_only
            .artifact_error
            .unwrap()
            .contains("disk unavailable")
    );

    let capped = ResultPolicy::new()
        .with_max_bytes(2)
        .with_max_chars(4)
        .with_overflow(ResultOverflow::Artifact)
        .with_artifact_store(store)
        .apply("t", full, None)
        .await;
    assert!(capped.text.chars().count() <= 4);
    assert!(capped.omitted_chars > 0);
    assert!(capped.artifact.is_none());
    assert!(capped.artifact_error.unwrap().contains("disk unavailable"));
}

#[tokio::test]
async fn zero_byte_threshold_is_strict_and_active_only_for_artifact_overflow() {
    let store = Arc::new(MemoryStore(Mutex::new(Vec::new())));
    let policy = ResultPolicy::new()
        .with_max_bytes(0)
        .with_overflow(ResultOverflow::Artifact)
        .with_artifact_store(store.clone());
    assert!(policy.is_active());
    let empty = policy.apply("t", "", None).await;
    assert!(empty.artifact.is_none());
    assert!(store.0.lock().unwrap().is_empty());

    let nonempty = policy.apply("t", "x", None).await;
    assert_eq!(nonempty.artifact.as_ref().unwrap().id, "artifact-1");
    assert_eq!(nonempty.text, "x");
    assert_eq!(store.0.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn byte_threshold_does_not_truncate_without_artifact_overflow() {
    let policy = ResultPolicy::new().with_max_bytes(0);
    assert!(!policy.is_active());
    let empty = policy.apply("t", "", None).await;
    assert_eq!(empty.text, "");
    assert!(empty.artifact.is_none() && empty.artifact_error.is_none());

    let output = policy.apply("t", "é", None).await;
    assert_eq!(output.text, "é");
    assert_eq!(output.omitted_chars, 0);
    assert!(output.artifact.is_none() && output.artifact_error.is_none());
}

#[tokio::test]
async fn artifact_overflow_stores_the_full_text_and_returns_a_preview() {
    let store = Arc::new(MemoryStore(Mutex::new(Vec::new())));
    let policy = ResultPolicy::new()
        .with_max_chars(6)
        .with_overflow(ResultOverflow::Artifact)
        .with_artifact_store(store.clone());
    let applied = policy.apply("task-1", "0123456789", None).await;
    assert_eq!(applied.artifact.as_ref().unwrap().id, "artifact-1");
    assert!(applied.omitted_chars > 0 && applied.text.chars().count() <= 6);
    assert_eq!(
        store.0.lock().unwrap()[0],
        ("task-1".to_owned(), "0123456789".to_owned()),
        "the full, untruncated output is what is stored"
    );
}

#[tokio::test]
async fn artifact_within_the_cap_is_not_stored() {
    let store = Arc::new(MemoryStore(Mutex::new(Vec::new())));
    let policy = ResultPolicy::new()
        .with_max_chars(100)
        .with_overflow(ResultOverflow::Artifact)
        .with_artifact_store(store.clone());
    let applied = policy.apply("t", "short", None).await;
    assert!(applied.artifact.is_none());
    assert!(store.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn artifact_without_a_store_falls_back_to_truncation_and_says_so() {
    let policy = ResultPolicy::new()
        .with_max_chars(4)
        .with_overflow(ResultOverflow::Artifact);
    let applied = policy.apply("t", "0123456789", None).await;
    assert!(applied.artifact.is_none());
    assert!(applied.omitted_chars > 0 && applied.text.chars().count() <= 4);
    assert!(
        applied
            .artifact_error
            .unwrap()
            .contains("no artifact store")
    );
}

#[tokio::test]
async fn schema_validates_the_final_output_as_json() {
    let schema = json!({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]});
    let policy = ResultPolicy::new().with_schema(schema);
    assert!(
        policy
            .apply("t", r#"{"n": 3}"#, None)
            .await
            .schema_error
            .is_none()
    );
    let wrong = policy.apply("t", r#"{"n": "x"}"#, None).await;
    assert!(wrong.schema_error.unwrap().contains("integer"));
    let not_json = policy.apply("t", "plain prose", None).await;
    assert!(not_json.schema_error.unwrap().contains("not valid JSON"));
}

#[tokio::test]
async fn schema_prefers_the_runs_structured_value() {
    let policy = ResultPolicy::new().with_schema(json!({"type":"object","required":["k"]}));
    let applied = policy.apply("t", "prose", Some(&json!({"k": 1}))).await;
    assert!(applied.schema_error.is_none());
}
