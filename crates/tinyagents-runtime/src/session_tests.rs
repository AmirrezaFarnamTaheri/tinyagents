use super::*;
use crate::test_store::{memory, read, seed};
use crate::{DriverFailure, DriverOutcome, SessionBuilder, TranscriptCodec};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tinyagents_session::transcript::{
    BackgroundAppend, BackgroundAppendOutcome, SessionRef, SessionTranscript, TranscriptMessage,
    TranscriptMeta, session_stem,
};
use tinyinference_llm::message::Message;
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

fn meta() -> TranscriptMeta {
    TranscriptMeta {
        session_id: None,
        parent_session_id: None,
        agent_name: "agent".into(),
        agent_id: Some("agent".into()),
        agent_type: None,
        dispatcher: "test".into(),
        provider: None,
        model: None,
        created: "then".into(),
        updated: "then".into(),
        turn_count: 0,
        prefix_message_count: None,
        prefix_reset: false,
        input_tokens: 0,
        output_tokens: 0,
        cached_input_tokens: 0,
        charged_amount_usd: 0.0,
        thread_id: None,
        task_id: None,
    }
}

/// Maps user/assistant rows one-to-one, keeping prior raw rows verbatim.
struct RoleCodec;

impl TranscriptCodec for RoleCodec {
    fn decode_history(&self, transcript: &SessionTranscript) -> Result<Vec<Message>, RuntimeError> {
        Ok(transcript
            .messages
            .iter()
            .map(|row| match row.role.as_str() {
                "system" => Message::system(&row.content),
                "user" => Message::user(&row.content),
                _ => Message::assistant(&row.content),
            })
            .collect())
    }

    fn reconcile(
        &self,
        prior: &[TranscriptMessage],
        previous: &[Message],
        next: &[Message],
        _: &crate::TranscriptTurnOptions,
    ) -> Result<Vec<TranscriptMessage>, RuntimeError> {
        let mut rows = prior.to_vec();
        let prior_system_count = prior.iter().take_while(|row| row.role == "system").count();
        let next_system_count = next
            .iter()
            .take_while(|message| matches!(message, Message::System(_)))
            .count();
        if prior_system_count == 0 && next_system_count > 0 {
            rows.splice(
                0..0,
                next[..next_system_count]
                    .iter()
                    .map(|message| TranscriptMessage::new("system", message.text())),
            );
        }
        rows.extend(
            next[next_system_count
                .saturating_add(previous.len())
                .min(next.len())..]
                .iter()
                .map(|message| match message {
                    Message::System(_) => TranscriptMessage::new("system", message.text()),
                    Message::User(_) => TranscriptMessage::user(message.text()),
                    _ => TranscriptMessage::assistant(message.text()),
                }),
        );
        Ok(rows)
    }
}

struct CapturingDriver(Arc<Mutex<Vec<Vec<Message>>>>);

#[async_trait]
impl SessionDriver for CapturingDriver {
    async fn execute(&self, request: DriverRequest) -> Result<DriverOutcome, DriverFailure> {
        self.0.lock().unwrap().push(request.history.clone());
        let mut history = request.history;
        history.push(Message::assistant("reply"));
        Ok(DriverOutcome {
            outcome: None,
            history,
            output: Some("reply".into()),
            partial: None,
            interrupted: false,
        })
    }
}

struct PrefixWhenUnfrozen(&'static str);

#[async_trait]
impl SessionHooks for PrefixWhenUnfrozen {
    async fn before_turn(
        &self,
        _: &mut SessionTurnRequest,
        _: &mut TurnOptions,
        state: SessionStateView<'_>,
    ) -> Result<TurnPreparation, RuntimeError> {
        Ok(TurnPreparation {
            prefix: (!state.prefix_is_frozen)
                .then(|| PrefixSnapshot::new(vec![Message::system(self.0)])),
            ..TurnPreparation::default()
        })
    }

    async fn on_terminal(&self, _: SessionTerminal) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn before_commit(
        &self,
        _: &SessionTurnOutcome,
        _: &TranscriptTurnOptions,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[tokio::test]
async fn reset_history_seals_old_turns_and_starts_a_fresh_resumable_context() {
    let locator = memory();
    let session_ref = SessionRef::scoped("reset-thread", "agent-id");
    let first_requests = Arc::new(Mutex::new(Vec::new()));
    let mut session = SessionBuilder::new(Arc::new(CapturingDriver(first_requests)))
        .codec(Arc::new(RoleCodec))
        .hooks(Arc::new(PrefixWhenUnfrozen("original prompt")))
        .session(locator.clone(), session_ref.clone(), meta())
        .build()
        .unwrap();
    session
        .turn(
            SessionTurnRequest::new(Message::user("before reset")),
            TurnOptions {
                resume: ResumeMode::Session,
                session: Some(session_ref.clone()),
                ..TurnOptions::default()
            },
        )
        .await
        .unwrap();

    let old_head = locator.head_generation(&session_ref);
    let old_before = locator
        .read_session_transcript(&old_head)
        .unwrap()
        .read_session()
        .unwrap()
        .unwrap();
    assert_eq!(old_before.messages[0].content, "original prompt");
    assert!(session.reset_history().await.unwrap());

    let new_head = locator.head_generation(&session_ref);
    assert_eq!(new_head.generation, old_head.generation + 1);
    assert_eq!(new_head.parent_session_id(), Some(old_head.session_id()));
    let old_after = locator
        .read_session_transcript(&old_head)
        .unwrap()
        .read_session()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value((&old_after.meta, &old_after.messages, &old_after.tools,)).unwrap(),
        serde_json::to_value((&old_before.meta, &old_before.messages, &old_before.tools,)).unwrap(),
    );
    assert!(
        locator
            .read_session_transcript(&new_head)
            .unwrap()
            .read_session()
            .unwrap()
            .unwrap()
            .messages
            .is_empty()
    );

    assert!(!session.reset_history().await.unwrap());
    assert_eq!(locator.head_generation(&session_ref), new_head);

    let reset_requests = Arc::new(Mutex::new(Vec::new()));
    let mut after_reset = SessionBuilder::new(Arc::new(CapturingDriver(reset_requests.clone())))
        .codec(Arc::new(RoleCodec))
        .hooks(Arc::new(PrefixWhenUnfrozen("fresh prompt")))
        .session(locator.clone(), session_ref.clone(), meta())
        .build()
        .unwrap();
    after_reset
        .turn(
            SessionTurnRequest::new(Message::user("after reset")),
            TurnOptions {
                resume: ResumeMode::Session,
                session: Some(session_ref.clone()),
                ..TurnOptions::default()
            },
        )
        .await
        .unwrap();
    let reset_request = reset_requests.lock().unwrap()[0].clone();
    assert_eq!(reset_request[0].text(), "fresh prompt");
    assert_eq!(reset_request[1].text(), "after reset");
    assert!(
        !reset_request
            .iter()
            .any(|message| message.text() == "before reset")
    );

    let resumed_requests = Arc::new(Mutex::new(Vec::new()));
    let mut resumed = SessionBuilder::new(Arc::new(CapturingDriver(resumed_requests.clone())))
        .codec(Arc::new(RoleCodec))
        .hooks(Arc::new(PrefixWhenUnfrozen("replacement prompt")))
        .session(locator.clone(), session_ref.clone(), meta())
        .build()
        .unwrap();
    resumed
        .turn(
            SessionTurnRequest::new(Message::user("cold resume")),
            TurnOptions {
                resume: ResumeMode::Session,
                session: Some(session_ref.clone()),
                ..TurnOptions::default()
            },
        )
        .await
        .unwrap();
    let cold_request = resumed_requests.lock().unwrap()[0].clone();
    assert_eq!(cold_request[0].text(), "fresh prompt");
    assert!(
        !cold_request
            .iter()
            .any(|message| message.text() == "replacement prompt")
    );
    assert!(
        !cold_request
            .iter()
            .any(|message| message.text() == "before reset")
    );
}

#[tokio::test]
async fn reset_history_advances_an_explicit_frozen_empty_prefix() {
    let locator = memory();
    let session_ref = SessionRef::scoped("empty-prefix-reset", "agent-id");
    let mut empty_prefix_meta = meta();
    empty_prefix_meta.prefix_message_count = Some(0);
    let empty_head = locator
        .open_stem(&session_stem(&session_ref), empty_prefix_meta)
        .unwrap();
    empty_head.replace(&[]).unwrap();
    let mut session =
        SessionBuilder::new(Arc::new(CapturingDriver(Arc::new(Mutex::new(Vec::new())))))
            .codec(Arc::new(RoleCodec))
            .session(locator.clone(), session_ref.clone(), meta())
            .build()
            .unwrap();

    assert!(session.reset_history().await.unwrap());
    let reset_head = locator.head_generation(&session_ref);
    assert_eq!(reset_head.generation, 1);
    assert_eq!(
        locator
            .read_session_transcript(&reset_head)
            .unwrap()
            .read_session()
            .unwrap()
            .unwrap()
            .meta
            .prefix_message_count,
        None,
    );
    assert!(
        locator
            .read_session_transcript(&reset_head)
            .unwrap()
            .read_session()
            .unwrap()
            .unwrap()
            .meta
            .prefix_reset
    );

    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut cold = SessionBuilder::new(Arc::new(CapturingDriver(requests.clone())))
        .codec(Arc::new(RoleCodec))
        .hooks(Arc::new(PrefixWhenUnfrozen("after empty reset")))
        .session(locator, session_ref.clone(), meta())
        .build()
        .unwrap();
    cold.turn(
        SessionTurnRequest::new(Message::user("new turn")),
        TurnOptions {
            resume: ResumeMode::Session,
            session: Some(session_ref),
            ..TurnOptions::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(requests.lock().unwrap()[0][0].text(), "after empty reset");
}

#[tokio::test]
async fn an_empty_compaction_without_reset_marker_recovers_explicit_parent_boundary() {
    let locator = memory();
    let session_ref = SessionRef::scoped("empty-compaction", "agent-id");
    let mut root_meta = meta();
    root_meta.prefix_message_count = Some(0);
    let root = locator
        .open_stem(&session_stem(&session_ref), root_meta)
        .unwrap();
    root.replace(&[]).unwrap();
    let (successor, empty_head) = locator
        .begin_generation_from_baseline(&session_ref, meta(), &[])
        .unwrap();
    empty_head.replace(&[]).unwrap();

    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut resumed = SessionBuilder::new(Arc::new(CapturingDriver(requests.clone())))
        .codec(Arc::new(RoleCodec))
        .hooks(Arc::new(PrefixWhenUnfrozen(
            "must not replace frozen empty",
        )))
        .session(locator, session_ref.clone(), meta())
        .build()
        .unwrap();
    resumed
        .turn(
            SessionTurnRequest::new(Message::user("after compaction")),
            TurnOptions {
                resume: ResumeMode::Session,
                session: Some(session_ref),
                ..TurnOptions::default()
            },
        )
        .await
        .unwrap();
    let request = requests.lock().unwrap()[0].clone();
    assert_eq!(request.len(), 1);
    assert_eq!(request[0].text(), "after compaction");
    assert_eq!(successor.generation, 1);
}

#[tokio::test]
async fn reset_waits_for_an_in_flight_append_before_sealing_the_head() {
    let locator = memory();
    let session_ref = SessionRef::scoped("reset-race", "agent-id");
    seed(
        locator.as_ref(),
        &session_stem(&session_ref),
        &[
            TranscriptMessage::user("before concurrent turn"),
            TranscriptMessage::assistant("old answer"),
        ],
        &meta(),
    );
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut turning = SessionBuilder::new(Arc::new(GatedDriver {
        started: started.clone(),
        release: release.clone(),
    }))
    .codec(Arc::new(RoleCodec))
    .session(locator.clone(), session_ref.clone(), meta())
    .build()
    .unwrap();
    let turn_ref = session_ref.clone();
    let turn = tokio::spawn(async move {
        turning
            .turn(
                SessionTurnRequest::new(Message::user("commits before reset")),
                TurnOptions {
                    resume: ResumeMode::Session,
                    session: Some(turn_ref),
                    ..TurnOptions::default()
                },
            )
            .await
    });
    started.notified().await;

    let reset_ref = SessionRef::scoped("reset-race", "agent-id");
    let mut resetting =
        SessionBuilder::new(Arc::new(CapturingDriver(Arc::new(Mutex::new(Vec::new())))))
            .codec(Arc::new(RoleCodec))
            .session(locator.clone(), reset_ref, meta())
            .build()
            .unwrap();
    let reset = tokio::spawn(async move { resetting.reset_history().await });
    tokio::task::yield_now().await;
    assert!(
        !reset.is_finished(),
        "reset bypassed the active session turn lock"
    );

    release.notify_one();
    turn.await.unwrap().unwrap();
    assert!(reset.await.unwrap().unwrap());
    let old = locator
        .read_session_transcript(&session_ref)
        .unwrap()
        .read_session()
        .unwrap()
        .unwrap();
    assert!(
        old.messages
            .iter()
            .any(|message| message.content == "commits before reset")
    );
    assert_eq!(locator.head_generation(&session_ref).generation, 1);
}

#[tokio::test]
async fn reset_does_not_create_an_absent_session_or_cross_agent_boundary() {
    let locator = memory();
    let absent_ref = SessionRef::scoped("absent-reset", "agent-a");
    let mut absent =
        SessionBuilder::new(Arc::new(CapturingDriver(Arc::new(Mutex::new(Vec::new())))))
            .codec(Arc::new(RoleCodec))
            .session(locator.clone(), absent_ref.clone(), meta())
            .build()
            .unwrap();
    assert!(!absent.reset_history().await.unwrap());
    assert!(locator.read_session_transcript(&absent_ref).is_none());

    let first_ref = SessionRef::scoped("same-thread", "agent-a");
    let other_agent_ref = SessionRef::scoped("same-thread", "agent-b");
    seed(
        locator.as_ref(),
        &session_stem(&first_ref),
        &[
            TranscriptMessage::user("first agent"),
            TranscriptMessage::assistant("answer"),
        ],
        &meta(),
    );
    seed(
        locator.as_ref(),
        &session_stem(&other_agent_ref),
        &[
            TranscriptMessage::user("other agent"),
            TranscriptMessage::assistant("answer"),
        ],
        &meta(),
    );
    let mut first =
        SessionBuilder::new(Arc::new(CapturingDriver(Arc::new(Mutex::new(Vec::new())))))
            .codec(Arc::new(RoleCodec))
            .session(locator.clone(), first_ref.clone(), meta())
            .build()
            .unwrap();
    assert!(first.reset_history().await.unwrap());
    assert_eq!(locator.head_generation(&first_ref).generation, 1);
    assert_eq!(locator.head_generation(&other_agent_ref).generation, 0);
    assert!(
        locator
            .read_session_transcript(&other_agent_ref)
            .unwrap()
            .read_session()
            .unwrap()
            .unwrap()
            .messages
            .iter()
            .any(|message| message.content == "other agent")
    );
}

/// An atomic import may create a durable session before its first model turn.
/// The resumed history must still receive the host's initial prefix, while a
/// later process must reuse that committed prefix rather than its new prompt.
#[tokio::test]
async fn imported_history_gets_initial_prefix_before_the_first_turn() {
    let locator = memory();
    let session_ref = SessionRef::scoped("relay-import", "agent");
    let prior = vec![
        TranscriptMessage::user("legacy question"),
        TranscriptMessage::assistant("legacy answer"),
    ];
    assert!(
        locator
            .adopt_messages_if_absent(&session_ref, meta(), &prior)
            .unwrap()
    );

    let first_requests = Arc::new(Mutex::new(Vec::new()));
    let mut first = SessionBuilder::new(Arc::new(CapturingDriver(first_requests.clone())))
        .codec(Arc::new(RoleCodec))
        .hooks(Arc::new(PrefixWhenUnfrozen("configured initial prompt")))
        .session(locator.clone(), session_ref.clone(), meta())
        .build()
        .unwrap();
    first
        .turn(
            SessionTurnRequest::new(Message::user("current question")),
            TurnOptions {
                resume: ResumeMode::Session,
                session: Some(session_ref.clone()),
                thread_id: Some("relay-import".into()),
                ..TurnOptions::default()
            },
        )
        .await
        .unwrap();

    let first_history = first_requests.lock().unwrap()[0].clone();
    assert!(matches!(&first_history[0], Message::System(_)));
    assert_eq!(first_history[0].text(), "configured initial prompt");
    assert!(matches!(&first_history[1], Message::User(_)));
    assert_eq!(first_history[1].text(), "legacy question");
    assert!(matches!(&first_history[2], Message::Assistant(_)));
    assert_eq!(first_history[2].text(), "legacy answer");
    assert!(matches!(&first_history[3], Message::User(_)));
    assert_eq!(first_history[3].text(), "current question");
    let head = locator.head_generation(&session_ref);
    let persisted = locator
        .read_session_transcript(&head)
        .unwrap()
        .read_session()
        .unwrap()
        .unwrap();
    assert_eq!(persisted.messages[0].role, "system");
    assert_eq!(persisted.messages[0].content, "configured initial prompt");

    let second_requests = Arc::new(Mutex::new(Vec::new()));
    let mut second = SessionBuilder::new(Arc::new(CapturingDriver(second_requests.clone())))
        .codec(Arc::new(RoleCodec))
        .hooks(Arc::new(PrefixWhenUnfrozen("replacement prompt")))
        .session(locator, session_ref.clone(), meta())
        .build()
        .unwrap();
    second
        .turn(
            SessionTurnRequest::new(Message::user("after restart")),
            TurnOptions {
                resume: ResumeMode::Session,
                session: Some(session_ref),
                thread_id: Some("relay-import".into()),
                ..TurnOptions::default()
            },
        )
        .await
        .unwrap();
    let second_history = second_requests.lock().unwrap()[0].clone();
    assert!(matches!(&second_history[0], Message::System(_)));
    assert_eq!(second_history[0].text(), "configured initial prompt");
    assert!(matches!(&second_history[1], Message::User(_)));
    assert_eq!(second_history[1].text(), "legacy question");
    assert!(matches!(&second_history[2], Message::Assistant(_)));
    assert_eq!(second_history[2].text(), "legacy answer");
    assert!(
        matches!(&second_history[3], Message::User(_)),
        "resumed request history: {:?}",
        second_history.iter().map(Message::text).collect::<Vec<_>>()
    );
    assert_eq!(second_history[3].text(), "current question");
    assert!(matches!(second_history.last(), Some(Message::User(_))));
    assert_eq!(second_history.last().unwrap().text(), "after restart");
}

#[tokio::test]
async fn an_explicit_empty_frozen_prefix_stays_empty() {
    let locator = memory();
    let session_ref = SessionRef::scoped("empty-prefix", "agent");
    let mut explicit_empty_prefix = meta();
    explicit_empty_prefix.prefix_message_count = Some(0);
    seed(
        locator.as_ref(),
        &session_stem(&session_ref),
        &[TranscriptMessage::user("existing message")],
        &explicit_empty_prefix,
    );

    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut session = SessionBuilder::new(Arc::new(CapturingDriver(requests.clone())))
        .codec(Arc::new(RoleCodec))
        .hooks(Arc::new(PrefixWhenUnfrozen("must remain unused")))
        .session(locator, session_ref.clone(), explicit_empty_prefix)
        .build()
        .unwrap();
    session
        .turn(
            SessionTurnRequest::new(Message::user("next message")),
            TurnOptions {
                resume: ResumeMode::Session,
                session: Some(session_ref),
                thread_id: Some("empty-prefix".into()),
                ..TurnOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(
        requests.lock().unwrap()[0]
            .iter()
            .all(|message| !matches!(message, Message::System(_)))
    );
}

/// Answers each turn with `reply`, but only after the test releases it.
struct GatedDriver {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl SessionDriver for GatedDriver {
    async fn execute(&self, request: DriverRequest) -> Result<DriverOutcome, DriverFailure> {
        self.started.notify_one();
        self.release.notified().await;
        let mut history = request.history;
        history.push(Message::assistant("Sunny."));
        Ok(DriverOutcome {
            outcome: None,
            history,
            output: Some("Sunny.".into()),
            partial: None,
            interrupted: false,
        })
    }
}

/// A background delivery issued while a turn is running waits for that turn
/// to persist, then lands after it — including when resume selects an absent
/// session and the write falls back to the target's existing session.
#[tokio::test]
async fn a_running_turn_holds_off_a_background_append_into_its_session() {
    let locator = memory();
    let session_ref = SessionRef::scoped("thread-1", "agent");
    let selected_session = SessionRef::scoped("thread-2", "agent");
    seed(
        locator.as_ref(),
        &session_stem(&session_ref),
        &[
            TranscriptMessage::user("remind me at 5"),
            TranscriptMessage::assistant("Scheduled."),
        ],
        &meta(),
    );
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut session = SessionBuilder::new(Arc::new(GatedDriver {
        started: started.clone(),
        release: release.clone(),
    }))
    .codec(Arc::new(RoleCodec))
    .session(locator.clone(), session_ref.clone(), meta())
    .build()
    .unwrap();
    session
        .seed_history(
            vec![
                Message::user("remind me at 5"),
                Message::assistant("Scheduled."),
            ],
            vec![
                TranscriptMessage::user("remind me at 5"),
                TranscriptMessage::assistant("Scheduled."),
            ],
        )
        .unwrap();

    let turn = tokio::spawn(async move {
        session
            .turn(
                SessionTurnRequest::new(Message::user("weather?")),
                TurnOptions {
                    resume: ResumeMode::Session,
                    session: Some(selected_session),
                    thread_id: Some("thread-2".into()),
                    ..TurnOptions::default()
                },
            )
            .await
    });
    started.notified().await;

    let delivery_locator = locator.clone();
    let delivery_session = session_ref.clone();
    let mut delivery = tokio::spawn(async move {
        tinyagents_session::transcript::append_background_message_in(
            delivery_locator.as_ref(),
            &delivery_session,
            TranscriptMessage::assistant("Time to stretch!"),
            BackgroundAppend::new("run-1", serde_json::json!({"kind": "cron"})),
        )
        .await
    });
    // Single-threaded runtime: yielding lets the delivery run up to the turn
    // lock it must wait on.
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert!(
        timeout(Duration::from_secs(1), &mut delivery)
            .await
            .is_err(),
        "delivery must wait for the turn"
    );

    release.notify_one();
    turn.await.unwrap().unwrap();
    assert_eq!(
        delivery.await.unwrap().unwrap(),
        BackgroundAppendOutcome::Appended { generation: 1 }
    );
    let contents: Vec<String> = read(
        locator.as_ref(),
        &session_stem(&locator.head_generation(&session_ref)),
    )
    .messages
    .into_iter()
    .map(|message| message.content)
    .collect();
    assert_eq!(
        contents,
        [
            "remind me at 5",
            "Scheduled.",
            "weather?",
            "Sunny.",
            "Time to stretch!"
        ]
    );
}
