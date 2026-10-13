use super::*;

#[test]
fn legacy_label_spellings_collapse_into_current_buckets_without_duplicates() {
    let labels = vec![
        "work".to_string(),
        "general".to_string(),
        "worker".to_string(),
        "agent-task".to_string(),
        "custom".to_string(),
    ];
    assert_eq!(normalize_labels(labels), vec!["general", "tasks", "custom"]);
}

#[test]
fn default_labels_follow_the_thread_id_namespace() {
    assert_eq!(infer_labels("proactive:morning_briefing"), vec!["briefing"]);
    assert_eq!(infer_labels("proactive:other"), vec!["notification"]);
    assert_eq!(infer_labels("thread-1"), vec!["general"]);
}
