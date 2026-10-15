use super::*;
use crate::transcript::{TranscriptMessage, TranscriptMeta};

fn meta() -> TranscriptMeta {
    TranscriptMeta {
        agent_name: "orchestrator".into(),
        agent_id: Some("orchestrator".into()),
        agent_type: None,
        dispatcher: "native".into(),
        provider: None,
        model: None,
        created: "2026-01-01T00:00:00Z".into(),
        updated: "2026-01-01T00:00:00Z".into(),
        turn_count: 1,
        prefix_message_count: None,
        input_tokens: 0,
        output_tokens: 0,
        cached_input_tokens: 0,
        charged_amount_usd: 0.0,
        thread_id: None,
        task_id: None,
        session_id: None,
        parent_session_id: None,
    }
}

fn write(path: &Path, system: &str) {
    write_transcript(
        path,
        &[
            TranscriptMessage::system(system),
            TranscriptMessage::user("hi"),
        ],
        &meta(),
        None,
    )
    .unwrap();
}

#[test]
fn rewrites_flat_and_dated_transcripts_and_counts_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let raw = raw_dir(dir.path());
    write(&raw.join("flat.jsonl"), "keep ### SECRET");
    write(&raw.join("01012026").join("dated.jsonl"), "clean");
    std::fs::write(raw.join("broken.jsonl"), b"not json\n").unwrap();
    std::fs::write(raw.join("notes.md"), b"ignored").unwrap();

    let report = rewrite_legacy_transcripts(dir.path(), |session| {
        let first = &mut session.messages[0];
        if !first.content.contains("### SECRET") {
            return false;
        }
        first.content = first.content.replace(" ### SECRET", "");
        true
    });

    assert_eq!(
        report,
        LegacyRewriteReport {
            scanned: 3,
            rewritten: 1,
            unchanged: 1,
            errors: 1,
        }
    );
    let flat = read_transcript(&raw.join("flat.jsonl")).unwrap();
    assert_eq!(flat.messages[0].content, "keep");
    assert_eq!(flat.messages[1].content, "hi");
    let dated = read_transcript(&raw.join("01012026").join("dated.jsonl")).unwrap();
    assert_eq!(dated.messages[0].content, "clean");
}

#[test]
fn a_workspace_without_transcripts_is_an_empty_report() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        rewrite_legacy_transcripts(dir.path(), |_| true),
        LegacyRewriteReport::default()
    );
}
