//! Live transcript snapshots for callers that need completed rounds after a
//! run fails. The snapshot mirrors only provider requests the harness actually
//! dispatches and marks the prefix a provider answered.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tinyinference_llm::message::Message;
use tinyinference_llm::model::{ModelRequest, ModelResponse, ResolvedModelRoute};

use crate::context::RunContext;
use crate::error::Result;
use crate::middleware::{Middleware, ToolInvocationIdentity};
use crate::turn_outcome::{ToolCallOutcome, ToolOutcomeSink};
use tinytools::ToolResult;

use super::tool_outcome_capture::ToolCallCapture;

/// Snapshot of transcript state observed while a run is in progress.
#[derive(Clone, Debug, Default)]
pub struct TranscriptSnapshot {
    /// Most recent provider request, followed by its response and subsequent
    /// tool results that have not yet been accepted by another request.
    pub messages: Vec<Message>,
    /// Length of the last provider request that received a response.
    pub accepted_len: usize,
    /// Number of caller-seeded messages before this run began.
    pub request_base_len: usize,
    /// Accumulated usage from answered, non-cache model calls.
    pub input_tokens: u64,
    /// Accumulated output tokens from answered, non-cache model calls.
    pub output_tokens: u64,
    /// Accumulated cached input tokens from answered, non-cache model calls.
    pub cached_input_tokens: u64,
    /// Input tokens of the latest answered model call, including cache replay.
    pub last_call_input_tokens: u64,
    /// Output tokens of the latest answered model call, including cache replay.
    pub last_call_output_tokens: u64,
    /// Route reported by the latest answered model call when available.
    pub resolved_route: Option<ResolvedModelRoute>,
    /// Number of model calls answered during this run.
    pub model_calls: u32,
    /// Completed tool-call outcomes observed before a later failure.
    pub tool_outcomes: Vec<ToolCallOutcome>,
}

impl TranscriptSnapshot {
    /// End of the provider-accepted prefix, clamped to the current messages and
    /// never before the caller-seeded transcript.
    pub fn accepted_end(&self) -> usize {
        let len = self.messages.len();
        self.accepted_len.clamp(self.request_base_len.min(len), len)
    }
}

/// Shared snapshot buffer written by [`TranscriptSnapshotMiddleware`].
pub type TranscriptSnapshotSink = Arc<Mutex<TranscriptSnapshot>>;

/// Creates an empty, shareable transcript snapshot.
pub fn new_transcript_snapshot_sink() -> TranscriptSnapshotSink {
    Arc::default()
}

/// Observation middleware that preserves completed transcript rounds on errors.
/// Register it outside transformations whose final tool result should appear
/// in the snapshot. The snapshot records the concrete arguments and duration
/// while the tool call is still available, then the final result after inner
/// `after_tool` transformations have run.
pub struct TranscriptSnapshotMiddleware {
    sink: TranscriptSnapshotSink,
    outcome_sink: Option<ToolOutcomeSink>,
    capture: ToolCallCapture,
}

impl TranscriptSnapshotMiddleware {
    /// Creates a snapshot observer for a shared sink.
    pub fn new(sink: TranscriptSnapshotSink) -> Self {
        Self {
            sink,
            outcome_sink: None,
            capture: ToolCallCapture::default(),
        }
    }

    /// Also appends each captured outcome to the standard turn-outcome sink,
    /// including when callers persist the transcript snapshot separately.
    pub fn with_tool_outcome_sink(mut self, sink: ToolOutcomeSink) -> Self {
        self.outcome_sink = Some(sink);
        self
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> Middleware<State, Ctx> for TranscriptSnapshotMiddleware {
    fn name(&self) -> &str {
        "transcript_snapshot"
    }

    async fn before_tool(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &State,
        call: &mut tinyinference_llm::tool::ToolCall,
    ) -> Result<()> {
        self.capture.start(ctx, call);
        Ok(())
    }

    async fn after_tool(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &State,
        invocation: &ToolInvocationIdentity,
        result: &mut ToolResult,
    ) -> Result<()> {
        let outcome = self.capture.finish(ctx, invocation, result);
        if let Ok(mut snapshot) = self.sink.lock() {
            let content = outcome.content.clone();
            snapshot
                .messages
                .push(Message::tool(outcome.call_id.clone(), content));
            snapshot.tool_outcomes.push(outcome.clone());
        }
        if let Some(sink) = &self.outcome_sink
            && let Ok(mut sink) = sink.lock()
        {
            sink.push(outcome);
        }
        Ok(())
    }

    async fn before_model_dispatch(
        &self,
        _ctx: &mut RunContext<Ctx>,
        _state: &State,
        request: &mut ModelRequest,
    ) -> Result<()> {
        if let Ok(mut snapshot) = self.sink.lock() {
            snapshot.messages.clone_from(&request.messages);
        }
        Ok(())
    }

    async fn after_model(
        &self,
        _ctx: &mut RunContext<Ctx>,
        _state: &State,
        response: &mut ModelResponse,
    ) -> Result<()> {
        let route = response.resolved_route.clone();
        if let Ok(mut snapshot) = self.sink.lock() {
            snapshot.accepted_len = snapshot.messages.len();
            snapshot.model_calls = snapshot.model_calls.saturating_add(1);
            snapshot
                .messages
                .push(Message::Assistant(response.message.clone()));
            if let Some(usage) = &response.usage {
                snapshot.last_call_input_tokens = usage.input_tokens;
                snapshot.last_call_output_tokens = usage.output_tokens;
                if !response.served_from_cache {
                    snapshot.input_tokens =
                        snapshot.input_tokens.saturating_add(usage.input_tokens);
                    snapshot.output_tokens =
                        snapshot.output_tokens.saturating_add(usage.output_tokens);
                    snapshot.cached_input_tokens = snapshot
                        .cached_input_tokens
                        .saturating_add(usage.cache_read_tokens);
                }
            }
            if route.is_some() {
                snapshot.resolved_route = route;
            }
        }
        Ok(())
    }

    async fn after_agent(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &State,
        _run: &mut crate::middleware::AgentRun,
    ) -> Result<()> {
        self.capture.forget_run(ctx.instance_id());
        Ok(())
    }

    async fn on_error(
        &self,
        ctx: &mut RunContext<Ctx>,
        _error: &crate::error::TinyAgentsError,
    ) -> Result<()> {
        self.capture.forget_run(ctx.instance_id());
        Ok(())
    }
}

/// Renders messages from an unanswered provider request as bounded plain text.
/// Returning text rather than replayable messages avoids persisting a malformed
/// assistant/tool sequence rejected by the provider.
pub fn render_unanswered_steps(messages: &[Message], max_chars: usize) -> Option<String> {
    if messages.is_empty() {
        return None;
    }
    let clip = |text: &str| {
        let text = text.trim();
        let mut chars = text.chars();
        let clipped: String = chars.by_ref().take(max_chars).collect();
        if chars.next().is_some() {
            format!("{}...", clipped.trim_end())
        } else {
            clipped
        }
    };
    let mut out =
        String::from("The request that failed also carried these steps, recorded here as text:\n");
    for message in messages {
        match message {
            Message::Assistant(assistant) if !assistant.tool_calls.is_empty() => {
                for call in &assistant.tool_calls {
                    let args = serde_json::to_string(&call.arguments).unwrap_or_default();
                    out.push_str(&format!("- called `{}` with {}\n", call.name, clip(&args)));
                }
            }
            Message::Assistant(_) => {
                out.push_str(&format!("- assistant: {}\n", clip(&message.text())));
            }
            Message::Tool(_) => {
                out.push_str(&format!("- tool result: {}\n", clip(&message.text())))
            }
            Message::User(_) | Message::System(_) => {
                out.push_str(&format!("- message: {}\n", clip(&message.text())));
            }
            Message::Custom(_) => {}
        }
    }
    Some(out)
}

#[cfg(test)]
#[path = "transcript_snapshot_tests.rs"]
mod tests;
