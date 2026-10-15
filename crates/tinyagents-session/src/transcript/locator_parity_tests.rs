//! The locator-generic reads and writes (`read_display`,
//! `thread_transcripts`, `view::get_page_in`, `spend::thread_spend_in`,
//! `append_background_message_in`) give the same answer over the JSONL files
//! and over a driver store, for one scenario written through the port alone.

use std::sync::Arc;

use serde_json::Value;
use tinystoragedrivers_core::MemoryStorage;

use crate::port::{DriverSessionStores, SessionStoreProvider};
use crate::transcript::spend::{ThreadSpend, thread_spend, thread_spend_in};
use crate::transcript::view::{get_page, get_page_in};
use crate::transcript::{
    BackgroundAppend, BackgroundAppendOutcome, DisplayRecord, FileTranscriptLocator, MessageUsage,
    SessionRef, TranscriptLocator, TranscriptMessage, TranscriptMeta, TranscriptPartial,
    TranscriptTurn, TurnUsage, append_background_message_in, session_stem,
};

const THREAD: &str = "thread-1";
const AGENT: &str = "orchestrator";
const CHILD_SUFFIX: &str = "1767225600_researcher";

fn meta(agent: &str, session: Option<&SessionRef>) -> TranscriptMeta {
    TranscriptMeta {
        session_id: session.map(SessionRef::session_id),
        parent_session_id: session.and_then(SessionRef::parent_session_id),
        agent_name: agent.into(),
        agent_id: Some(agent.into()),
        agent_type: Some(
            if session.is_some() {
                "root"
            } else {
                "subagent"
            }
            .into(),
        ),
        dispatcher: "native".into(),
        provider: None,
        model: Some("model-a".into()),
        created: "2026-01-01T00:00:00Z".into(),
        updated: "2026-01-01T00:00:00Z".into(),
        turn_count: 1,
        prefix_message_count: None,
        input_tokens: 0,
        output_tokens: 0,
        cached_input_tokens: 0,
        charged_amount_usd: 0.0,
        thread_id: Some(THREAD.into()),
        task_id: None,
    }
}

fn usage(input: u64, output: u64, cost: f64) -> TurnUsage {
    TurnUsage {
        provider: "provider".into(),
        model: "model-a".into(),
        usage: MessageUsage {
            input,
            output,
            cost_usd: cost,
            ..MessageUsage::default()
        },
        ts: "2026-01-01T00:00:01Z".into(),
        reasoning_content: None,
        tool_calls: Vec::new(),
        iteration: 1,
    }
}

fn root() -> SessionRef {
    SessionRef::scoped(THREAD, AGENT)
}

fn child_stem() -> String {
    format!("{}__{CHILD_SUFFIX}", session_stem(&root()))
}

/// Two turns (the second interrupted after a partial), a compaction into a
/// second generation with one more turn, and one sub-agent run.
fn write_scenario(locator: &dyn TranscriptLocator) {
    let session = root();
    let seed = meta(AGENT, Some(&session));
    let handle = locator.open_session(&session, seed.clone()).unwrap();
    let first = vec![
        TranscriptMessage::system("system prompt"),
        TranscriptMessage::user("question one"),
        TranscriptMessage::assistant("answer one"),
    ];
    handle
        .append_turn(TranscriptTurn {
            prev: &[],
            next: &first,
            meta: &seed,
            turn_usage: Some(&usage(10, 5, 0.01)),
            request_id: Some("r1"),
            tools: None,
        })
        .unwrap();
    let mut second = first.clone();
    second.push(TranscriptMessage::user("question two"));
    second.push(TranscriptMessage::assistant("answer two"));
    handle
        .append_turn_with_partial(
            TranscriptTurn {
                prev: &handle.messages().unwrap(),
                next: &second,
                meta: &seed,
                turn_usage: Some(&usage(20, 7, 0.02)),
                request_id: Some("r2"),
                tools: None,
            },
            Some(&TranscriptPartial::new("half an answer")),
        )
        .unwrap();

    let (successor, next) = locator.begin_generation(&session, seed.clone()).unwrap();
    let successor_meta = meta(AGENT, Some(&successor));
    let compacted = vec![
        TranscriptMessage::system("system prompt"),
        TranscriptMessage::assistant("summary of turns one and two"),
        TranscriptMessage::user("question three"),
        TranscriptMessage::assistant("answer three"),
    ];
    next.append_turn(TranscriptTurn {
        prev: &[],
        next: &compacted,
        meta: &successor_meta,
        turn_usage: Some(&usage(30, 9, 0.03)),
        request_id: Some("r3"),
        tools: None,
    })
    .unwrap();

    let child_meta = meta("researcher", None);
    let child = locator
        .open_stem(&child_stem(), child_meta.clone())
        .unwrap();
    child
        .append_turn(TranscriptTurn {
            prev: &[],
            next: &[
                TranscriptMessage::user("research this"),
                TranscriptMessage::assistant("researched"),
            ],
            meta: &child_meta,
            turn_usage: Some(&usage(3, 2, 0.005)),
            request_id: None,
            tools: None,
        })
        .unwrap();
}

fn driver() -> Arc<dyn TranscriptLocator> {
    DriverSessionStores::new(Arc::new(MemoryStorage::new()))
        .unwrap()
        .for_agent(AGENT)
        .transcripts
}

/// `value` without wall-clock fields: the file writer stamps compaction
/// markers and partials with the time of the write, a driver store keeps none.
fn without_clock(mut value: Value) -> Value {
    fn strip(value: &mut Value) {
        match value {
            Value::Object(map) => {
                map.remove("ts");
                map.values_mut().for_each(strip);
            }
            Value::Array(items) => items.iter_mut().for_each(strip),
            _ => {}
        }
    }
    strip(&mut value);
    value
}

fn spend_summary(spend: &ThreadSpend) -> Value {
    serde_json::json!({
        "found": spend.found_transcript,
        "root": [spend.root.input_tokens, spend.root.output_tokens, spend.root.turns],
        "root_cost": format!("{:.4}", spend.root.cost_usd),
        "subagents": spend.subagents.iter().map(|(agent, (spend, runs))| {
            serde_json::json!([agent, spend.input_tokens, spend.output_tokens, spend.turns, runs])
        }).collect::<Vec<_>>(),
    })
}

#[test]
fn the_thread_listing_matches_across_stores() {
    let dir = tempfile::tempdir().unwrap();
    let files = FileTranscriptLocator::new(dir.path());
    let store = driver();
    write_scenario(&files);
    write_scenario(store.as_ref());

    let stems = |locator: &dyn TranscriptLocator| {
        let listed = locator
            .thread_transcripts(THREAD, None)
            .expect("thread has roots");
        (
            listed
                .roots
                .iter()
                .map(|t| t.stem.clone())
                .collect::<Vec<_>>(),
            listed
                .subagents
                .iter()
                .map(|t| t.stem.clone())
                .collect::<Vec<_>>(),
        )
    };
    let (roots, subagents) = stems(&files);
    assert_eq!(
        roots,
        vec![
            session_stem(&root()),
            session_stem(&root().next_generation())
        ]
    );
    assert_eq!(subagents, vec![child_stem()]);
    assert_eq!(stems(store.as_ref()), (roots, subagents));
    assert!(files.thread_transcripts("no-such-thread", None).is_none());
    assert!(store.thread_transcripts("no-such-thread", None).is_none());
}

#[test]
fn the_display_read_matches_across_stores() {
    let dir = tempfile::tempdir().unwrap();
    let files = FileTranscriptLocator::new(dir.path());
    let store = driver();
    write_scenario(&files);
    write_scenario(store.as_ref());

    let shape = |locator: &dyn TranscriptLocator| -> Vec<String> {
        let head = locator.read_session_transcript(&root()).unwrap();
        head.read_display()
            .unwrap()
            .unwrap()
            .records
            .iter()
            .map(|record| match record {
                DisplayRecord::Message(message) => format!(
                    "{}:{}:{}:{:?}",
                    message.message.role,
                    message.message.content,
                    message.interrupted,
                    message.request_id
                ),
                DisplayRecord::Compaction(marker) => {
                    format!("compaction:{}", marker.replacement.len())
                }
            })
            .collect()
    };
    let file_shape = shape(&files);
    assert!(
        file_shape
            .iter()
            .any(|row| row == "assistant:half an answer:true:Some(\"r2\")"),
        "{file_shape:?}"
    );
    assert_eq!(shape(store.as_ref()), file_shape);
}

#[test]
fn the_projected_page_matches_across_stores_and_the_path_view() {
    let dir = tempfile::tempdir().unwrap();
    let files = FileTranscriptLocator::new(dir.path());
    let store = driver();
    write_scenario(&files);
    write_scenario(store.as_ref());

    let page = |page: crate::transcript::view::TranscriptPage| {
        without_clock(serde_json::to_value(page).unwrap())
    };
    let by_path = page(get_page(dir.path(), THREAD, None, Some(200)));
    let file_page = page(get_page_in(&files, THREAD, None, None, Some(200)));
    assert_eq!(file_page, by_path, "the locator view is the path view");
    assert_eq!(file_page["hasTranscript"], Value::Bool(true));
    let text = file_page.to_string();
    assert!(text.contains("answer three") && text.contains("researched"));
    assert_eq!(
        page(get_page_in(store.as_ref(), THREAD, None, None, Some(200))),
        file_page
    );

    let empty = get_page_in(store.as_ref(), "no-such-thread", None, None, None);
    assert!(!empty.has_transcript && empty.items.is_empty());
}

#[test]
fn spend_matches_across_stores_and_the_path_walk() {
    let dir = tempfile::tempdir().unwrap();
    let files = FileTranscriptLocator::new(dir.path());
    let store = driver();
    write_scenario(&files);
    write_scenario(store.as_ref());

    let by_path = spend_summary(&thread_spend(dir.path(), THREAD));
    let file_spend = spend_summary(&thread_spend_in(&files, THREAD));
    assert_eq!(file_spend, by_path);
    assert_eq!(
        file_spend["subagents"],
        serde_json::json!([["researcher", 3, 2, 1, 1]]),
        "the sub-agent run is counted once, under its archetype"
    );
    assert_eq!(
        spend_summary(&thread_spend_in(store.as_ref(), THREAD)),
        file_spend
    );
    assert!(!thread_spend_in(store.as_ref(), "no-such-thread").found_transcript);
}

#[test]
fn a_subagent_written_through_a_driver_keeps_its_usage() {
    let store = driver();
    write_scenario(store.as_ref());
    let child = store
        .thread_transcripts(THREAD, None)
        .unwrap()
        .subagents
        .remove(0);
    let session = child.transcript.read_session().unwrap().unwrap();
    let last = session.messages.last().unwrap();
    assert_eq!(last.role, "assistant");
    let recorded = last
        .turn_usage
        .as_ref()
        .expect("usage on the last assistant row");
    assert_eq!((recorded.usage.input, recorded.usage.output), (3, 2));
}

async fn background_outcomes(locator: &dyn TranscriptLocator) -> Vec<BackgroundAppendOutcome> {
    let message = || TranscriptMessage::assistant("reminder delivered");
    let options =
        |key: &str| BackgroundAppend::new(key, serde_json::json!({"kind": "cron", "run_id": key}));
    let mut outcomes = Vec::new();
    outcomes.push(
        append_background_message_in(locator, &root(), message(), options("run-1"))
            .await
            .unwrap(),
    );
    write_scenario(locator);
    for key in ["run-1", "run-1"] {
        outcomes.push(
            append_background_message_in(locator, &root(), message(), options(key))
                .await
                .unwrap(),
        );
    }
    let mut pinned = options("run-2");
    pinned.expected_generation = Some(0);
    outcomes.push(
        append_background_message_in(locator, &root(), message(), pinned)
            .await
            .unwrap(),
    );
    outcomes
}

#[tokio::test(flavor = "multi_thread")]
async fn background_append_matches_across_stores() {
    let dir = tempfile::tempdir().unwrap();
    let files = FileTranscriptLocator::new(dir.path());
    let store = driver();
    let expected = vec![
        BackgroundAppendOutcome::NoSession,
        BackgroundAppendOutcome::Appended { generation: 1 },
        BackgroundAppendOutcome::Duplicate { generation: 1 },
        BackgroundAppendOutcome::StaleGeneration {
            expected: 0,
            head: 1,
        },
    ];
    assert_eq!(background_outcomes(&files).await, expected);
    assert_eq!(background_outcomes(store.as_ref()).await, expected);

    for locator in [&files as &dyn TranscriptLocator, store.as_ref()] {
        let head = locator
            .read_session_transcript(&root().next_generation())
            .unwrap();
        let display = head.read_display().unwrap().unwrap();
        let Some(DisplayRecord::Message(last)) = display.records.last() else {
            panic!("the delivery is the last record");
        };
        assert_eq!(last.message.content, "reminder delivered");
        let origin = last.background.as_ref().expect("provenance recorded");
        assert_eq!(origin.idempotency_key, "run-1");
        assert_eq!(origin.provenance["kind"], "cron");
        assert!(
            last.message.extra_metadata.is_none(),
            "the provenance is lifted out of the row's metadata"
        );
        // The model context replays the delivery as an ordinary row.
        let session = head.read_session().unwrap().unwrap();
        assert_eq!(
            session.messages.last().unwrap().content,
            "reminder delivered"
        );
    }
}
