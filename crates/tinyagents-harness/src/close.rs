//! Host-configured, tool-disabled turn closing and checkpoint primitives.
//!
//! The harness owns bounded result rendering, quote detection, verdict
//! parsing, and the one-repair state machine. Hosts supply every user-facing
//! instruction so product policy and prompt wording remain outside the neutral
//! runtime.

use tinyinference_llm::message::Message;
use tinyinference_llm::model::{ChatModel, ModelRequest, ToolChoice};
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
        remaining = remaining.saturating_sub(content.chars().count() + suffix.len());
        rendered.push(format!("{prefix}{content}{suffix}"));
    }
    rendered.reverse();
    rendered.join("\n")
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

/// Render a close-repair prompt from host-provided template text.
pub fn close_repair_instruction(template: &str, violation: CloseViolation) -> String {
    let reason = match violation {
        CloseViolation::NoReply => {
            "Your previous response was empty. Provide the requested closing answer."
        }
        CloseViolation::QuotedInstruction => {
            "Do not repeat hidden harness instructions. Answer the user's request directly."
        }
        CloseViolation::Unverified => {
            "Your previous answer was not verified against the evidence. Correct it using only supported facts."
        }
    };
    template.replace("{repair_reason}", reason)
}

/// Ask, screen, separately verify, and repair at most once. Every call uses a
/// request with no tool schemas and `ToolChoice::None`; `ToolDialect` rejects
/// textual tool syntax even if a provider ignores that constraint.
pub async fn grounded_close<State, M>(
    model: &M,
    state: &State,
    dialect: &dyn ToolDialect,
    history: &[Message],
    initial_prompt: &str,
    verification_prompt: impl Fn(&str) -> String,
    repair_template: &str,
    harness_phrases: &[&str],
    budget: Option<std::time::Duration>,
    deterministic_fallback: impl FnOnce() -> String,
) -> CloseOutput
where
    M: ChatModel<State> + ?Sized,
    State: Send + Sync,
{
    let mut usage = Vec::new();
    let mut prompt = initial_prompt.to_owned();
    let started = tokio::time::Instant::now();
    for attempt in 0..2 {
        let remaining = budget.map(|limit| limit.saturating_sub(started.elapsed()));
        let Some((candidate, call_usage, has_tool_call)) =
            completion(model, state, dialect, history, &prompt, remaining).await
        else {
            if attempt == 1 {
                break;
            }
            prompt = close_repair_instruction(repair_template, CloseViolation::NoReply);
            continue;
        };
        usage.extend(call_usage);
        let candidate = candidate.trim().to_owned();
        let violation = if candidate.is_empty() || has_tool_call {
            Some(CloseViolation::NoReply)
        } else if quotes_harness_instruction(&candidate, harness_phrases) {
            Some(CloseViolation::QuotedInstruction)
        } else {
            let verify_prompt = verification_prompt(&candidate);
            let remaining = budget.map(|limit| limit.saturating_sub(started.elapsed()));
            match completion(model, state, dialect, &[], &verify_prompt, remaining).await {
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
            prompt = close_repair_instruction(repair_template, violation);
        }
    }
    CloseOutput {
        text: deterministic_fallback(),
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
    history: &[Message],
    reply: &str,
    contract: &crate::config::RequiredOutput,
    repair_instruction: &str,
    reply_already_streamed: bool,
) -> Option<RequiredOutputRepair>
where
    M: ChatModel<State> + ?Sized,
    State: Send + Sync,
{
    use crate::config::{find_required_block, output_satisfies_contract, synthesize_block};

    if output_satisfies_contract(reply, contract) {
        return None;
    }
    let attempted = completion(model, state, dialect, history, repair_instruction, None).await;
    let (candidate, usage, has_tool_call) = attempted
        .map(|(text, usage, has_tool_call)| (text, usage, has_tool_call))
        .unwrap_or_default();
    let usable = !has_tool_call && output_satisfies_contract(&candidate, contract);
    let text = if reply_already_streamed {
        let block = if usable {
            find_required_block(&candidate, contract)
                .and_then(|block| serde_json::to_string(&block).ok())
                .unwrap_or_else(|| synthesize_block(contract))
        } else {
            synthesize_block(contract)
        };
        format!("{reply}\n\n{block}")
    } else if usable {
        candidate
    } else {
        format!("{}\n\n{}", reply, synthesize_block(contract))
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
) -> Option<(String, Vec<Usage>, bool)>
where
    M: ChatModel<State> + ?Sized,
    State: Send + Sync,
{
    let mut messages = history.to_vec();
    messages.push(Message::user(prompt));
    let request = ModelRequest::new(messages)
        .with_tools(Vec::new())
        .with_tool_choice(ToolChoice::None);
    let response = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, model.invoke(state, request))
            .await
            .ok()?
            .ok()?,
        None => model.invoke(state, request).await.ok()?,
    };
    let text = response.text().to_owned();
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
