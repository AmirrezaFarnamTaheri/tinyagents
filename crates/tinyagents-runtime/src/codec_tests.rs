use super::*;
use tinyagents_session::transcript::TranscriptMessage;
use tinyinference_llm::message::Message;

struct Codec;

impl TranscriptCodec for Codec {
    fn decode_history(
        &self,
        _: &tinyagents_session::transcript::SessionTranscript,
    ) -> Result<Vec<Message>, RuntimeError> {
        Ok(Vec::new())
    }
}

#[test]
fn default_reconcile_matches_duplicate_messages_to_unused_rows_and_keeps_metadata() {
    let mut first = TranscriptMessage::assistant("same");
    first.extra_metadata = Some(serde_json::json!({"row": 1}));
    let mut second = first.clone();
    second.extra_metadata = Some(serde_json::json!({"row": 2}));
    let previous = vec![Message::assistant("same"), Message::assistant("same")];
    let next = vec![Message::assistant("same"), Message::assistant("same")];
    let rows = Codec
        .reconcile(
            &[first, second],
            &previous,
            &next,
            &TranscriptTurnOptions {
                request_id: Some("current".into()),
                thread_id: None,
                stream: false,
                resume: ResumeMode::Never,
                context: (),
            },
        )
        .unwrap();

    assert_eq!(rows[0].extra_metadata, Some(serde_json::json!({"row": 1})));
    assert_eq!(rows[1].extra_metadata, Some(serde_json::json!({"row": 2})));
    assert!(rows.iter().all(|row| row.request_id.is_none()));
}

#[test]
fn default_reconcile_stamps_only_new_rows() {
    let mut retained = TranscriptMessage::assistant("old");
    retained.request_id = Some("old-request".into());
    let previous = vec![Message::assistant("old")];
    let next = vec![Message::assistant("old"), Message::user("new")];
    let rows = Codec
        .reconcile(
            &[retained],
            &previous,
            &next,
            &TranscriptTurnOptions {
                request_id: Some("current".into()),
                thread_id: None,
                stream: false,
                resume: ResumeMode::Never,
                context: (),
            },
        )
        .unwrap();

    assert_eq!(rows[0].request_id.as_deref(), Some("old-request"));
    assert_eq!(rows[1].request_id.as_deref(), Some("current"));
}

#[test]
fn default_reconcile_matches_reordered_rows_to_their_original_metadata() {
    let mut alpha = TranscriptMessage::user("alpha");
    alpha.extra_metadata = Some(serde_json::json!({"source": "alpha"}));
    let mut beta = TranscriptMessage::system("beta");
    beta.extra_metadata = Some(serde_json::json!({"source": "beta"}));
    let rows = Codec
        .reconcile(
            &[alpha, beta],
            &[Message::user("alpha"), Message::system("beta")],
            &[Message::system("beta"), Message::user("alpha")],
            &TranscriptTurnOptions {
                request_id: Some("new-turn".into()),
                thread_id: None,
                stream: false,
                resume: ResumeMode::Never,
                context: (),
            },
        )
        .unwrap();
    assert_eq!(
        rows[0].extra_metadata,
        Some(serde_json::json!({"source": "beta"}))
    );
    assert_eq!(
        rows[1].extra_metadata,
        Some(serde_json::json!({"source": "alpha"}))
    );
}
