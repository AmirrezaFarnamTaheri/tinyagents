use super::*;
use crate::port::drivers::transcripts::tests::meta;
use crate::transcript::{TranscriptMessage, TranscriptTurn};
use tinystoragedrivers_core::{Blocking, DocumentStore, MemoryStorage, Scope, StorageBackend};

fn locator() -> DriverTranscriptLocator {
    let storage = MemoryStorage::new();
    let docs: Arc<dyn DocumentStore> =
        Arc::clone(storage.for_scope(&Scope::local()).unwrap().documents());
    DriverTranscriptLocator::new(docs, Blocking::new().unwrap(), "memory://display")
}

fn write(locator: &DriverTranscriptLocator, stem: &str, thread: &str, rows: &[&str]) {
    let rows: Vec<TranscriptMessage> = rows.iter().map(|r| TranscriptMessage::user(*r)).collect();
    let meta = meta(thread);
    locator
        .open_stem(stem, meta.clone())
        .unwrap()
        .append_turn(TranscriptTurn {
            prev: &[],
            next: &rows,
            meta: &meta,
            turn_usage: None,
            request_id: None,
            tools: None,
        })
        .unwrap();
}

#[test]
fn a_replace_after_the_first_write_is_a_compaction_and_a_clear_an_empty_one() {
    let locator = locator();
    let history = locator.open_stem("s", meta("t")).unwrap();
    history.replace(&[TranscriptMessage::user("a")]).unwrap();
    history.replace(&[TranscriptMessage::user("b")]).unwrap();
    history.clear().unwrap();
    let records = history.read_display().unwrap().unwrap().records;
    assert!(matches!(&records[0], DisplayRecord::Message(m) if m.message.content == "a"));
    assert!(matches!(&records[1], DisplayRecord::Compaction(c) if c.replacement.len() == 1));
    assert!(matches!(&records[2], DisplayRecord::Compaction(c) if c.replacement.is_empty()));
}

#[test]
fn an_unwritten_stem_has_no_display() {
    let locator = locator();
    let history = locator.open_stem("never", meta("t")).unwrap();
    assert!(history.read_display().unwrap().is_none());
}

#[test]
fn legacy_roots_are_dropped_once_a_session_root_exists() {
    let locator = locator();
    // A pre-identity root of the thread (no session id in its meta).
    write(&locator, "1700000000_planner", "t", &["old"]);
    let listed = locator.thread_transcripts("t", None).unwrap();
    assert_eq!(listed.roots.len(), 1, "alone, a legacy root is listed");

    let session = SessionRef::scoped("t", "planner");
    let mut seed = meta("t");
    seed.session_id = Some(session.session_id());
    locator
        .open_session(&session, seed.clone())
        .unwrap()
        .append_turn(TranscriptTurn {
            prev: &[],
            next: &[TranscriptMessage::user("new")],
            meta: &seed,
            turn_usage: None,
            request_id: None,
            tools: None,
        })
        .unwrap();
    let stems: Vec<String> = locator
        .thread_transcripts("t", None)
        .unwrap()
        .roots
        .into_iter()
        .map(|root| root.stem)
        .collect();
    assert_eq!(stems, vec![session_stem(&session)]);
}

#[test]
fn subagents_are_found_by_stem_prefix_and_unscoped_by_thread() {
    let locator = locator();
    write(&locator, "root", "t", &["q"]);
    write(&locator, "root__1_child", "other-thread", &["by prefix"]);
    write(&locator, "elsewhere__2_child", "t", &["by thread"]);
    write(
        &locator,
        "elsewhere__3_child",
        "other-thread",
        &["unrelated"],
    );
    let subs = |agent: Option<&str>| -> Vec<String> {
        locator
            .thread_transcripts("t", agent)
            .unwrap()
            .subagents
            .into_iter()
            .map(|sub| sub.stem)
            .collect()
    };
    assert_eq!(subs(None), vec!["elsewhere__2_child", "root__1_child"]);
    assert_eq!(subs(Some("planner")), vec!["root__1_child"]);
    assert!(locator.thread_transcripts("  ", None).is_none());
}
