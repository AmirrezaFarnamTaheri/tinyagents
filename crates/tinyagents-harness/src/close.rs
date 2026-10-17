//! Host-configured, tool-disabled turn closing and checkpoint primitives.
//!
//! The harness owns bounded result rendering, quote detection, verdict
//! parsing, and the one-repair state machine. Hosts supply every user-facing
//! instruction so product policy and prompt wording remain outside the neutral
//! runtime.
//!
//! Additive `*_with_options` entry points let hosts set an output cap and
//! observe completed responses while preserving the legacy entry points and
//! configuration structs.

use tinyinference_llm::message::Message;
use tinyinference_llm::model::{
    ChatModel, ModelRequest, ModelResponse, ModelStreamItem, ToolChoice,
};
use tinyinference_llm::usage::Usage;
use tinytools_agent::dialect::{DialectResponse, ToolDialect};

/// One completed tool call captured by the host before transcript compaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseToolResult {
    /// Stable tool name.
    pub name: String,
    /// Whether the call succeeded.
    pub success: bool,
    /// Captured output, already bounded by the caller if desired.
    pub content: String,
}

/// Copy the captured outcome boundary into the data shape used by checkpoint
/// and grounded-close rendering, truncating each result by Unicode characters.
pub fn results_from_tool_outcomes(
    outcomes: &[crate::turn_outcome::ToolCallOutcome],
    per_result_budget: usize,
) -> Vec<CloseToolResult> {
    outcomes
        .iter()
        .map(|outcome| CloseToolResult {
            name: outcome.name.clone(),
            success: outcome.success,
            content: truncate_chars(&outcome.content, per_result_budget),
        })
        .collect()
}

/// Copy tool outcomes into close results using the host's truncation policy.
pub fn results_from_tool_outcomes_with_options(
    outcomes: &[crate::turn_outcome::ToolCallOutcome],
    per_result_cap: usize,
    truncation_marker: &str,
) -> Vec<CloseToolResult> {
    outcomes
        .iter()
        .map(|outcome| CloseToolResult {
            name: outcome.name.clone(),
            success: outcome.success,
            content: truncate_chars_with_marker(
                &outcome.content,
                per_result_cap,
                truncation_marker,
            ),
        })
        .collect()
}

/// Trim surrounding whitespace and truncate on a Unicode character boundary.
pub fn truncate_chars(text: &str, max: usize) -> String {
    truncate_chars_with_marker(text, max, "… [truncated]")
}

fn truncate_chars_with_marker(text: &str, max: usize, marker: &str) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= max {
        return trimmed.to_owned();
    }
    let prefix = trimmed.chars().take(max).collect::<String>();
    format!("{prefix}{marker}")
}

/// Close result and the provider usage for its completion and verification
/// calls. Usage is returned even when a later repair fails.
#[derive(Debug, Clone, PartialEq)]
pub struct CloseOutput {
    /// Accepted model text, or the deterministic fallback.
    pub text: String,
    /// Usage reported by completed model requests, in call order.
    pub usage: Vec<Usage>,
}

/// Result of repairing a required-output contract.
#[derive(Debug, Clone, PartialEq)]
pub struct RequiredOutputRepair {
    /// Final response, including a deterministic structured block if needed.
    pub text: String,
    /// Whether the model produced a valid replacement block.
    pub model_repaired: bool,
    /// Usage reported by the repair request, if it completed.
    pub usage: Vec<Usage>,
}

/// Why a close response was rejected and needs its single repair attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseViolation {
    /// The model returned no usable text.
    NoReply,
    /// The model repeated harness instruction text.
    QuotedInstruction,
    /// The separate verifier did not accept the answer.
    Unverified,
}

/// Result of parsing the verifier's response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseVerdict {
    /// The response is grounded in the supplied evidence.
    Accept,
    /// The response contradicts or exceeds the evidence.
    Reject,
    /// The response did not contain a recognized verdict.
    Unclear,
}

/// Parse the last standalone `ACCEPT` or `REJECT` token; all other output is
/// unclear and therefore fails closed.
pub fn parse_close_verdict(text: &str) -> CloseVerdict {
    text.split(|c: char| !c.is_ascii_alphabetic())
        .filter_map(|token| match token.to_ascii_uppercase().as_str() {
            "ACCEPT" => Some(CloseVerdict::Accept),
            "REJECT" => Some(CloseVerdict::Reject),
            _ => None,
        })
        .next_back()
        .unwrap_or(CloseVerdict::Unclear)
}

/// Render tool outputs under a total character budget, spending the budget on
/// the newest results first because those are most likely to be unread.
pub fn render_tool_results(results: &[CloseToolResult], total_budget: usize) -> String {
    let mut rendered = Vec::new();
    let mut remaining = total_budget;
    for result in results.iter().rev() {
        if remaining == 0 {
            break;
        }
        if !rendered.is_empty() {
            if remaining <= 1 {
                break;
            }
            remaining -= 1;
        }
        let status = if result.success { "ok" } else { "failed" };
        let prefix = format!("- `{}` ({status}): ", result.name);
        if prefix.chars().count() >= remaining {
            rendered.push(prefix.chars().take(remaining).collect::<String>());
            break;
        }
        remaining -= prefix.chars().count();
        let source = result.content.trim();
        let source_chars = source.chars().count();
        let truncated = source_chars > remaining;
        let content_limit = if truncated {
            remaining.saturating_sub(1)
        } else {
            remaining
        };
        let content: String = source.chars().take(content_limit).collect();
        let suffix = if truncated { "…" } else { "" };
        remaining = remaining.saturating_sub(content.chars().count() + suffix.chars().count());
        rendered.push(format!("{prefix}{content}{suffix}"));
    }
    rendered.reverse();
    rendered.join("\n")
}

/// Host-configurable per-result trimming and newest-first block selection.
///
/// `render_block` returns one complete output block, including any separators
/// it needs. Its exact character count is charged against `total_budget`.
/// `omitted_notice` receives the number of earlier results omitted and its
/// returned string is prefixed verbatim after selection.
pub struct CloseResultRenderOptions<'a, Block, Omitted> {
    /// Per-result Unicode character cap before calling `render_block`.
    pub per_result_cap: usize,
    /// Marker appended after capped result content.
    pub truncation_marker: &'a str,
    /// Total Unicode character budget for rendered blocks.
    pub total_budget: usize,
    /// Render one complete host-formatted tool result block.
    pub render_block: Block,
    /// Render the host-formatted notice for omitted older results.
    pub omitted_notice: Omitted,
}

/// Render host-formatted results, selecting newest entries under a budget.
///
/// Each result's content is trimmed and capped first. The newest result is
/// always retained, even when its rendered block exceeds the total budget;
/// older results are added while their exact rendered block lengths fit.
/// Selected blocks retain their original order and the omission notice is
/// prepended verbatim.
pub fn render_tool_results_with_options<Block, Omitted>(
    results: &[CloseToolResult],
    options: CloseResultRenderOptions<'_, Block, Omitted>,
) -> String
where
    Block: Fn(&CloseToolResult) -> String,
    Omitted: Fn(usize) -> String,
{
    let mut selected = Vec::new();
    let mut used = 0usize;
    for result in results.iter().rev() {
        let bounded = CloseToolResult {
            name: result.name.clone(),
            success: result.success,
            content: truncate_chars_with_marker(
                &result.content,
                options.per_result_cap,
                options.truncation_marker,
            ),
        };
        let block = (options.render_block)(&bounded);
        let block_chars = block.chars().count();
        if !selected.is_empty() && used.saturating_add(block_chars) > options.total_budget {
            break;
        }
        used = used.saturating_add(block_chars);
        selected.push(block);
    }
    let omitted = results.len().saturating_sub(selected.len());
    selected.reverse();
    format!("{}{}", (options.omitted_notice)(omitted), selected.concat())
}

/// Build a deterministic checkpoint from captured tool output. The host owns
/// the prose template; `{steps}` and `{results}` are the only substitutions.
pub fn deterministic_checkpoint(
    template: &str,
    steps: usize,
    results: &[CloseToolResult],
    total_budget: usize,
) -> String {
    template
        .replace("{steps}", &steps.to_string())
        .replace("{results}", &render_tool_results(results, total_budget))
}

/// Build the deterministic summary used when a normal close is rejected or
/// unavailable. Hosts configure the prose and may insert the stop label;
/// captured outputs are rendered under the supplied total budget.
/// Build the deterministic summary used when a normal close is rejected or
/// unavailable. Hosts configure the prose and may insert the stop label;
/// captured outputs are rendered under the supplied total budget.
pub fn deterministic_final_summary(
    template: &str,
    results: &[CloseToolResult],
    stop_reason: Option<&str>,
    total_budget: usize,
) -> String {
    template
        .replace("{stop_reason}", stop_reason.unwrap_or(""))
        .replace("{results}", &render_tool_results(results, total_budget))
}

/// Whether a candidate repeats any harness-only phrase supplied by the host.
/// Matching normalizes case and whitespace and requires at least 24 chars, so
/// short common phrases do not spuriously reject a reply.
pub fn quotes_harness_instruction(candidate: &str, phrases: &[&str]) -> bool {
    let candidate = normalize(candidate);
    phrases.iter().any(|phrase| {
        let phrase = normalize(phrase);
        phrase.chars().count() >= 24 && candidate.contains(&phrase)
    })
}

/// Render a close-repair prompt from host-provided template and reason text.
pub fn close_repair_instruction(template: &str, reason: &str) -> String {
    template.replace("{repair_reason}", reason)
}

/// Host-provided prompts and policies for one grounded close sequence.
pub struct GroundedCloseConfig<'a, V, R, F> {
    /// Conversation history to close.
    pub history: &'a [Message],
    /// Initial caller-configured close prompt.
    pub initial_prompt: &'a str,
    /// Build a separate verifier prompt for a candidate reply.
    pub verification_prompt: V,
    /// Build a repair prompt for the detected violation.
    pub repair_instruction: R,
    /// Caller-provided harness phrases the candidate must not quote.
    pub harness_phrases: &'a [&'a str],
    /// Total time budget shared by generation, verification and repair.
    pub budget: Option<std::time::Duration>,
    /// Deterministic fallback when the model cannot produce an accepted close.
    pub deterministic_fallback: F,
}

/// Inputs for repairing a reply that omitted required structured output.
pub struct RequiredOutputRepairConfig<'a> {
    /// Conversation history used for the repair request.
    pub history: &'a [Message],
    /// Reply that needs repair.
    pub reply: &'a str,
    /// Required output contract to validate.
    pub contract: &'a crate::config::RequiredOutput,
    /// Caller-configured repair prompt.
    pub repair_instruction: &'a str,
    /// Whether the original reply was already streamed to the caller.
    pub reply_already_streamed: bool,
}

/// Per-request options shared by grounded close and required-output repair.
/// Kind of close request whose terminal result is being observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseRequestKind {
    /// Candidate close response, including a retry after validation failure.
    Generation,
    /// Separate verification request for a candidate.
    Verification,
    /// Required-output contract repair request.
    RequiredOutputRepair,
}

/// Per-request host settings shared by grounded close and required repair.
///
/// The `*_with_options` entry points use streaming calls; legacy entry points
/// continue to use `ChatModel::invoke`.
pub struct CloseRequestOptions<'a> {
    /// Model id or registry alias to set on each request.
    pub model: Option<&'a str>,
    /// Sampling temperature to set on each request.
    pub temperature: Option<f64>,
    /// Maximum number of output tokens for every request in the operation.
    pub max_output_tokens: Option<u32>,
    /// Observe each request's terminal completion before validation or retry.
    /// `Some(response)` means the model completed; `None` means the request
    /// failed or its stream ended without a completed response. The callback
    /// is not called for a timeout, a dropped request, or a zero time budget.
    pub on_call_completed:
        Option<&'a mut (dyn FnMut(CloseRequestKind, Option<&ModelResponse>) + Send)>,
}

impl Default for CloseRequestOptions<'_> {
    fn default() -> Self {
        Self {
            model: None,
            temperature: None,
            max_output_tokens: None,
            on_call_completed: None,
        }
    }
}

/// Ask, screen, separately verify, and repair at most once. Every call uses a
/// request with no tool schemas and `ToolChoice::None`; `ToolDialect` rejects
/// textual tool syntax even if a provider ignores that constraint.
pub async fn grounded_close<State, M, V, R, F>(
    model: &M,
    state: &State,
    dialect: &dyn ToolDialect,
    config: GroundedCloseConfig<'_, V, R, F>,
) -> CloseOutput
where
    M: ChatModel<State> + ?Sized,
    State: Send + Sync,
    V: for<'reply> Fn(&'reply str) -> String,
    R: Fn(CloseViolation) -> String,
    F: FnOnce() -> String,
{
    grounded_close_impl(
        model,
        state,
        dialect,
        config,
        CloseRequestOptions::default(),
        false,
    )
    .await
}

/// Ask, screen, verify, and repair with per-request host options.
///
/// This options entry point uses `ChatModel::stream`, retaining buffered text
/// deltas when a completed response has empty text. The callback runs once on
/// terminal completion or failure, before validation; it is not called when a
/// request times out or is dropped.
pub async fn grounded_close_with_options<State, M, V, R, F>(
    model: &M,
    state: &State,
    dialect: &dyn ToolDialect,
    config: GroundedCloseConfig<'_, V, R, F>,
    options: CloseRequestOptions<'_>,
) -> CloseOutput
where
    M: ChatModel<State> + ?Sized,
    State: Send + Sync,
    V: for<'reply> Fn(&'reply str) -> String,
    R: Fn(CloseViolation) -> String,
    F: FnOnce() -> String,
{
    grounded_close_impl(model, state, dialect, config, options, true).await
}

async fn grounded_close_impl<State, M, V, R, F>(
    model: &M,
    state: &State,
    dialect: &dyn ToolDialect,
    config: GroundedCloseConfig<'_, V, R, F>,
    mut options: CloseRequestOptions<'_>,
    stream: bool,
) -> CloseOutput
where
    M: ChatModel<State> + ?Sized,
    State: Send + Sync,
    V: for<'reply> Fn(&'reply str) -> String,
    R: Fn(CloseViolation) -> String,
    F: FnOnce() -> String,
{
    let mut usage = Vec::new();
    let mut prompt = config.initial_prompt.to_owned();
    let started = tokio::time::Instant::now();
    for attempt in 0..2 {
        let remaining = config
            .budget
            .map(|limit| limit.saturating_sub(started.elapsed()));
        let Some((candidate, call_usage, has_tool_call)) = completion(
            model,
            state,
            dialect,
            config.history,
            &prompt,
            remaining,
            &mut options,
            CloseRequestKind::Generation,
            stream,
        )
        .await
        else {
            if attempt == 1 {
                break;
            }
            prompt = (config.repair_instruction)(CloseViolation::NoReply);
            continue;
        };
        usage.extend(call_usage);
        let candidate = candidate.trim().to_owned();
        let violation = if candidate.is_empty() || has_tool_call {
            Some(CloseViolation::NoReply)
        } else if quotes_harness_instruction(&candidate, config.harness_phrases) {
            Some(CloseViolation::QuotedInstruction)
        } else {
            let verify_prompt = (config.verification_prompt)(&candidate);
            let remaining = config
                .budget
                .map(|limit| limit.saturating_sub(started.elapsed()));
            match completion(
                model,
                state,
                dialect,
                &[],
                &verify_prompt,
                remaining,
                &mut options,
                CloseRequestKind::Verification,
                stream,
            )
            .await
            {
                Some((verdict, call_usage, has_tool_call)) => {
                    usage.extend(call_usage);
                    if !has_tool_call && parse_close_verdict(&verdict) == CloseVerdict::Accept {
                        return CloseOutput {
                            text: candidate,
                            usage,
                        };
                    }
                    Some(CloseViolation::Unverified)
                }
                None => Some(CloseViolation::Unverified),
            }
        };
        if let Some(violation) = violation {
            if attempt == 1 {
                break;
            }
            prompt = (config.repair_instruction)(violation);
        }
    }
    CloseOutput {
        text: (config.deterministic_fallback)(),
        usage,
    }
}

/// Repair a reply that omits a required structured-output block.
///
/// The instruction is caller-provided. If the model fails, returns tool
/// syntax, or returns an invalid block, the harness appends the contract's
/// deterministic synthesized block so the required-output invariant holds.
pub async fn repair_required_output<State, M>(
    model: &M,
    state: &State,
    dialect: &dyn ToolDialect,
    config: RequiredOutputRepairConfig<'_>,
) -> Option<RequiredOutputRepair>
where
    M: ChatModel<State> + ?Sized,
    State: Send + Sync,
{
    repair_required_output_impl(
        model,
        state,
        dialect,
        config,
        CloseRequestOptions::default(),
        false,
    )
    .await
}

/// Repair required output using host-supplied per-request options.
///
/// This options entry point uses `ChatModel::stream`, retaining buffered text
/// deltas when a completed response has empty text.
pub async fn repair_required_output_with_options<State, M>(
    model: &M,
    state: &State,
    dialect: &dyn ToolDialect,
    config: RequiredOutputRepairConfig<'_>,
    options: CloseRequestOptions<'_>,
) -> Option<RequiredOutputRepair>
where
    M: ChatModel<State> + ?Sized,
    State: Send + Sync,
{
    repair_required_output_impl(model, state, dialect, config, options, true).await
}

async fn repair_required_output_impl<State, M>(
    model: &M,
    state: &State,
    dialect: &dyn ToolDialect,
    config: RequiredOutputRepairConfig<'_>,
    mut options: CloseRequestOptions<'_>,
    stream: bool,
) -> Option<RequiredOutputRepair>
where
    M: ChatModel<State> + ?Sized,
    State: Send + Sync,
{
    use crate::config::{find_required_block, output_satisfies_contract, synthesize_block};

    if output_satisfies_contract(config.reply, config.contract) {
        return None;
    }
    let attempted = completion(
        model,
        state,
        dialect,
        config.history,
        config.repair_instruction,
        None,
        &mut options,
        CloseRequestKind::RequiredOutputRepair,
        stream,
    )
    .await;
    let (candidate, usage, has_tool_call) = attempted.unwrap_or_default();
    let usable = !has_tool_call && output_satisfies_contract(&candidate, config.contract);
    let text = if config.reply_already_streamed {
        let block = if usable {
            find_required_block(&candidate, config.contract)
                .and_then(|block| serde_json::to_string(&block).ok())
                .unwrap_or_else(|| synthesize_block(config.contract))
        } else {
            synthesize_block(config.contract)
        };
        format!("{}\n\n{block}", config.reply)
    } else if usable {
        candidate
    } else {
        format!("{}\n\n{}", config.reply, synthesize_block(config.contract))
    };
    Some(RequiredOutputRepair {
        text,
        model_repaired: usable,
        usage,
    })
}

async fn completion<State, M>(
    model: &M,
    state: &State,
    dialect: &dyn ToolDialect,
    history: &[Message],
    prompt: &str,
    timeout: Option<std::time::Duration>,
    options: &mut CloseRequestOptions<'_>,
    kind: CloseRequestKind,
    stream_mode: bool,
) -> Option<(String, Vec<Usage>, bool)>
where
    M: ChatModel<State> + ?Sized,
    State: Send + Sync,
{
    use futures::StreamExt;

    if timeout.is_some_and(|limit| limit.is_zero()) {
        return None;
    }
    let mut messages = history.to_vec();
    messages.push(Message::user(prompt));
    let mut request = ModelRequest::new(messages)
        .with_tools(Vec::new())
        .with_tool_choice(ToolChoice::None);
    request.model = options.model.map(str::to_owned);
    request.temperature = options.temperature;
    request.max_tokens = options.max_output_tokens;
    let (response, buffered_text) = if stream_mode {
        let consume_stream = async {
            let mut stream = model.stream(state, request).await.ok()?;
            let mut buffered_text = String::new();
            loop {
                match stream.next().await {
                    Some(ModelStreamItem::MessageDelta(delta)) => {
                        buffered_text.push_str(&delta.text)
                    }
                    Some(ModelStreamItem::Completed(response)) => {
                        return Some((response, buffered_text));
                    }
                    Some(
                        ModelStreamItem::Failed(_)
                        | ModelStreamItem::ProviderFailed(_)
                        | ModelStreamItem::Deferred(_),
                    )
                    | None => return None,
                    Some(_) => {}
                }
            }
        };
        let result = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, consume_stream).await.ok(),
            None => Some(consume_stream.await),
        };
        match result {
            Some(Some((response, buffered_text))) => {
                if let Some(on_call_completed) = options.on_call_completed.as_mut() {
                    on_call_completed(kind, Some(&response));
                }
                (response, buffered_text)
            }
            Some(None) => {
                if let Some(on_call_completed) = options.on_call_completed.as_mut() {
                    on_call_completed(kind, None);
                }
                return None;
            }
            None => return None,
        }
    } else {
        let response = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, model.invoke(state, request))
                .await
                .ok()?
                .ok()?,
            None => model.invoke(state, request).await.ok()?,
        };
        (response, String::new())
    };
    let response_text = response.text();
    let text = if stream_mode && response_text.trim().is_empty() {
        buffered_text
    } else {
        response_text.to_owned()
    };
    let has_tool_call = !response.message.tool_calls.is_empty()
        || !dialect
            .parse_response(&DialectResponse {
                text: Some(text.clone()),
                tool_calls: Vec::new(),
            })
            .1
            .is_empty();
    let usage = response.usage.into_iter().collect();
    Some((text, usage, has_tool_call))
}

fn normalize(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

#[cfg(test)]
#[path = "close_tests.rs"]
mod tests;
