//! Label canonicalisation shared by the thread stores.

/// Default labels for a thread whose upsert carried none, inferred from its
/// id namespace (proactive briefings/notifications vs ordinary chats).
pub(super) fn infer_labels(thread_id: &str) -> Vec<String> {
    if thread_id == "proactive:morning_briefing" {
        vec!["briefing".to_string()]
    } else if thread_id.starts_with("proactive:") {
        vec!["notification".to_string()]
    } else {
        vec!["general".to_string()]
    }
}

/// Canonicalise legacy label spellings into their current buckets and dedupe.
pub(super) fn normalize_labels(labels: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::with_capacity(labels.len());
    for label in labels {
        let next = match label.as_str() {
            "work" => "general".to_string(),
            // Labels written by the retired background-reasoning engine. Its
            // threads are ordinary conversations now.
            "from_reflection" | "subconscious_tick" | "subconscious" => "general".to_string(),
            "agent-task" | "worker" => "tasks".to_string(),
            _ => label,
        };
        if !normalized.contains(&next) {
            normalized.push(next);
        }
    }
    normalized
}

#[cfg(test)]
#[path = "labels_tests.rs"]
mod tests;
