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

#[test]
fn a_rewrite_keeps_the_recorded_tool_declarations() {
    let dir = tempfile::tempdir().unwrap();
    let path = raw_dir(dir.path()).join("tools.jsonl");
    write(&path, "keep ### SECRET");
    let tools = serde_json::json!([{"name": "search"}]);
    crate::transcript::append_tools_record(&path, &tools).unwrap();

    let report = rewrite_legacy_transcripts(dir.path(), |session| {
        session.messages[0].content = "keep".into();
        true
    });

    assert_eq!(report.rewritten, 1);
    let rewritten = read_transcript(&path).unwrap();
    assert_eq!(rewritten.messages[0].content, "keep");
    assert_eq!(rewritten.tools, Some(tools));
}

#[test]
fn an_unreadable_session_raw_is_an_error_not_an_empty_scan() {
    let dir = tempfile::tempdir().unwrap();
    // `session_raw` exists but is not a directory: `read_dir` fails with
    // something other than `NotFound`.
    std::fs::write(raw_dir(dir.path()), b"not a directory").unwrap();

    let report = rewrite_legacy_transcripts(dir.path(), |_| true);

    assert_eq!(report.scanned, 0);
    assert_eq!(report.errors, 1, "the failed scan is reported");
}

#[cfg(unix)]
#[test]
fn an_unreadable_dated_directory_is_counted() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let raw = raw_dir(dir.path());
    write(&raw.join("flat.jsonl"), "flat");
    let dated = raw.join("01012026");
    write(&dated.join("dated.jsonl"), "dated");
    std::fs::set_permissions(&dated, std::fs::Permissions::from_mode(0o000)).unwrap();
    // A privileged runner (root) can read it anyway; nothing to observe then.
    let readable = std::fs::read_dir(&dated).is_ok();

    let report = rewrite_legacy_transcripts(dir.path(), |_| false);
    std::fs::set_permissions(&dated, std::fs::Permissions::from_mode(0o755)).unwrap();

    if !readable {
        assert_eq!(report.scanned, 1);
        assert_eq!(report.errors, 1, "the unreadable child directory is reported");
    }
}

#[cfg(unix)]
#[test]
fn symlinks_are_neither_followed_nor_rewritten() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let raw = raw_dir(dir.path());
    write(&raw.join("inside.jsonl"), "inside");
    let target_file = outside.path().join("outside.jsonl");
    write(&target_file, "outside");
    let target_dir = outside.path().join("01012026");
    write(&target_dir.join("nested.jsonl"), "nested");
    symlink(&target_file, raw.join("linked.jsonl")).unwrap();
    symlink(&target_dir, raw.join("02012026")).unwrap();

    let report = rewrite_legacy_transcripts(dir.path(), |session| {
        session.messages[0].content = "rewritten".into();
        true
    });

    assert_eq!(report.scanned, 1, "only the real file is scanned");
    assert_eq!(report.rewritten, 1);
    assert_eq!(
        read_transcript(&target_file).unwrap().messages[0].content,
        "outside"
    );
    assert_eq!(
        read_transcript(&target_dir.join("nested.jsonl"))
            .unwrap()
            .messages[0]
            .content,
        "nested"
    );
}
