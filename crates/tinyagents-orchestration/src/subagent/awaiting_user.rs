//! Typed pause envelope exchanged between a paused subagent and its parent.

/// A subagent pause that needs a user answer before it can resume.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubagentAwaitingUser {
    /// Durable task id used for the child operation.
    pub task_id: String,
    /// Stable session selector used to respawn/reuse the paused child.
    pub task_key: String,
    /// Host-resolved agent identity accepted by the continuation tool.
    pub agent_id: String,
    /// Worker conversation id, when the host uses a separate worker thread.
    pub worker_thread_id: Option<String>,
    /// The question to surface to the user.
    pub question: String,
    /// Whether the pause was durably checkpointed.
    pub checkpointed: bool,
}

impl SubagentAwaitingUser {
    /// Renders the stable pause marker and the instructions needed to resume.
    ///
    /// Free-form values are JSON encoded on individual lines so a question or
    /// identifier cannot inject a second field or terminate the marker.
    pub fn render(&self) -> String {
        let task_id = encode(&self.task_id);
        let task_key = encode(&self.task_key);
        let agent_id = encode(&self.agent_id);
        let worker_thread_id = encode(self.worker_thread_id.as_deref().unwrap_or("(none)"));
        let question = encode(&self.question);
        let resume_caveat = if self.checkpointed {
            ""
        } else {
            " NOTE: this pause could NOT be saved to disk, so reusing the task_key may fail. If spawn_subagent reports no checkpoint and no durable session, tell the user the sub-agent's progress was lost instead of retrying."
        };
        format!(
            "[SUBAGENT_AWAITING_USER]\n\
             task_id: {task_id}\n\
             task_key: {task_key}\n\
             agent_id: {agent_id}\n\
             worker_thread_id: {worker_thread_id}\n\
             question: {question}\n\
             checkpointed: {}\n\
             [/SUBAGENT_AWAITING_USER]\n\n\
             The sub-agent needs clarification before it can continue. Surface the above question to the user. When the user responds, call continue_subagent with the task_id, agent_id, and the user's answer as the message parameter. Do NOT re-spawn or re-delegate the sub-agent — that restarts it from scratch and loses its progress.{resume_caveat}",
            self.checkpointed
        )
    }

    /// Parses the marked block from a rendered tool result.
    pub fn parse(rendered: &str) -> Result<Self, AwaitingUserParseError> {
        let block = rendered
            .split_once("[SUBAGENT_AWAITING_USER]\n")
            .ok_or(AwaitingUserParseError::MissingMarker)?
            .1
            .split_once("\n[/SUBAGENT_AWAITING_USER]")
            .ok_or(AwaitingUserParseError::MissingTerminator)?
            .0;
        let mut fields = std::collections::BTreeMap::new();
        for line in block.lines() {
            let (key, value) = line
                .split_once(": ")
                .ok_or(AwaitingUserParseError::MalformedField)?;
            if fields.insert(key, value).is_some() {
                return Err(AwaitingUserParseError::DuplicateField(key.to_owned()));
            }
        }
        let task_id = decode(take_field(&mut fields, "task_id")?)?;
        let task_key = decode(take_field(&mut fields, "task_key")?)?;
        let agent_id = decode(take_field(&mut fields, "agent_id")?)?;
        let worker = decode(take_field(&mut fields, "worker_thread_id")?)?;
        let question = decode(take_field(&mut fields, "question")?)?;
        let checkpointed = match take_field(&mut fields, "checkpointed")? {
            "true" => true,
            "false" => false,
            _ => return Err(AwaitingUserParseError::InvalidCheckpointFlag),
        };
        if let Some(key) = fields.keys().next() {
            return Err(AwaitingUserParseError::UnexpectedField((*key).to_owned()));
        }
        Ok(Self {
            task_id,
            task_key,
            agent_id,
            worker_thread_id: (worker != "(none)").then_some(worker),
            question,
            checkpointed,
        })
    }
}

fn take_field<'a>(
    fields: &mut std::collections::BTreeMap<&'a str, &'a str>,
    key: &'static str,
) -> Result<&'a str, AwaitingUserParseError> {
    fields
        .remove(key)
        .ok_or(AwaitingUserParseError::MissingField(key))
}

fn encode(value: &str) -> String {
    serde_json::to_string(value).expect("serializing a string cannot fail")
}

fn decode(value: &str) -> Result<String, AwaitingUserParseError> {
    serde_json::from_str(value).map_err(|_| AwaitingUserParseError::InvalidJsonValue)
}

/// Why a subagent awaiting-user envelope could not be parsed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AwaitingUserParseError {
    /// The opening marker is absent.
    #[error("missing awaiting-user marker")]
    MissingMarker,
    /// The closing marker is absent.
    #[error("missing awaiting-user terminator")]
    MissingTerminator,
    /// A line in the marked block is not a key/value field.
    #[error("malformed awaiting-user field")]
    MalformedField,
    /// A required field is absent.
    #[error("missing awaiting-user field `{0}`")]
    MissingField(&'static str),
    /// A field appears more than once.
    #[error("duplicate awaiting-user field `{0}`")]
    DuplicateField(String),
    /// A field not defined by this contract appears.
    #[error("unexpected awaiting-user field `{0}`")]
    UnexpectedField(String),
    /// A quoted field does not contain a JSON string.
    #[error("invalid JSON string in awaiting-user field")]
    InvalidJsonValue,
    /// Checkpoint field is not `true` or `false`.
    #[error("invalid awaiting-user checkpoint flag")]
    InvalidCheckpointFlag,
}

#[cfg(test)]
#[path = "awaiting_user_tests.rs"]
mod tests;
