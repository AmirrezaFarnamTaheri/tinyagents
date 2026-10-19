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
    ChatModel, ModelRequest, ModelResponse, ModelStream, ModelStreamItem, ToolChoice,
};
use tinyinference_llm::usage::Usage;
use tinytools_agent::dialect::{DialectResponse, ToolDialect};

/// Host copy used by deterministic close and repair paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseCopy {
    pub checkpoint_template: &'static str,
    pub final_summary_template: &'static str,
    pub empty_checkpoint_results: &'static str,
    pub empty_final_summary: &'static str,
    pub repair_template: &'static str,
    pub max_iteration_instruction: &'static str,
    pub final_write_instruction: &'static str,
    pub final_answer_instruction: &'static str,
    pub truncated_answer_instruction: &'static str,
    pub stop_note_preamble: &'static str,
    pub no_tool_calls: &'static str,
    pub repair_no_reply: &'static str,
    pub repair_quoted_instruction: &'static str,
    pub repair_unverified: &'static str,
    pub instruction_needles: &'static [&'static str],
    pub minimum_quoted_words: usize,
    pub verification_prompt_template: &'static str,
}

const ENGLISH_FINAL_ANSWER_NEEDLE: &str = "You have finished using tools for this turn but have not yet written a reply to the user. Tools are no longer available and nothing more will run this turn, so do not call any tools and do not describe steps you are about to take. Write a self-contained final message that reports what actually happened: what you found, changed or established, grounded in the tool results above and the tool records below. If the request was not completed, say so and give the reason from the failing tool's own error message, keeping any link it includes. Do not state anything the tool records contradict. If nothing conclusive resulted, say so plainly.\n\nThese directions are addressed to you and are not part of the conversation. Do not quote or restate them, in whole or in part. Do not list or describe the tools available to you. Do not narrate your deliberation: no thinking aloud, no correcting yourself mid-reply, no weighing what to do. Write only the message the user will read.";
const ENGLISH_TRUNCATED_ANSWER_NEEDLE: &str = "Your last reply ran out of output tokens while you were still reasoning, so this turn ended before you made your next tool call or wrote a reply to the user. The work is unfinished: say so plainly and name what is still left to do. Tools are no longer available and nothing more will run this turn, so do not call any tools and do not describe steps you are about to take. Write a self-contained final message that reports what actually happened: what you found, changed or established, grounded in the tool results above and the tool records below. If the request was not completed, say so and give the reason from the failing tool's own error message, keeping any link it includes. Do not state anything the tool records contradict. If nothing conclusive resulted, say so plainly.\n\nThese directions are addressed to you and are not part of the conversation. Do not quote or restate them, in whole or in part. Do not list or describe the tools available to you. Do not narrate your deliberation: no thinking aloud, no correcting yourself mid-reply, no weighing what to do. Write only the message the user will read.";
const ENGLISH_ITERATION_NEEDLE: &str = "You have reached the maximum number of tool calls allowed for this single turn, so you cannot call any more tools right now. Do not attempt another tool call.\n\nFirst, report the substance of what this turn produced, grounded in the tool results above: what you found, what you changed, or what you established — whichever this task was. Be concrete about it. If you were gathering information, give the information itself rather than saying that you gathered it; if you were changing something, say what is now different. Include your most recent tool calls: they completed, and this is your only opportunity to use them.\n\nThen close with a brief **Still to do** line naming what remains, so the user can ask you to carry on.\n\nLet the length follow the substance — neither a one-line status nor a raw dump of tool output. If the results support no conclusion yet, say that plainly and name what is missing.";
const ENGLISH_FINAL_WRITE_NEEDLE: &str = "This is the last call on which you can use a tool, and the only tools left are the ones that write files. Gathering is over — anything you have not found by now will not be found in this turn.\n\nIf this task asked you to produce a file (a report, a guide, a document) and you have not written it yet, write it now with file_write, from what is already in the results above, and mark inside it any part you could not confirm.\n\nIf this task is a code change, apply as much of the change as you can right now as real edits to the source files: apply_patch for targeted edits, or file_write to rewrite a file whose full content you have. A partial set of real edits is worth more than any description of them. Do not write notes, a plan, a summary, a TODO list or any other document into the project in place of the change. If you cannot edit a file exactly, leave it untouched and name it in your reply.\n\nIf there is nothing to write — the task asked only for an answer, or the work is already written — then do not call a tool. Answer instead, and you will be asked to conclude next.";

/// English copy preserving OpenHuman's close and checkpoint wording.
pub const ENGLISH_CLOSE_COPY: CloseCopy = CloseCopy {
    checkpoint_template: "I reached the tool-call limit for this turn ({steps} steps), so I paused here before I could write up what I found.\n\n**Raw results from this turn**\n{results}\n**Next steps:** I'll continue from here — just reply (e.g. \"continue\") and I'll pick up where I left off.",
    final_summary_template: "I finished this turn without writing up a result. Here is what each tool call returned:\n{results}\nTell me how you'd like to proceed.",
    empty_checkpoint_results: "\n- (no tools completed yet)\n",
    empty_final_summary: "I finished this turn but produced no result to report.",
    repair_template: "{repair_reason} Write the message again.\n\n{instruction}",
    max_iteration_instruction: "You have reached the maximum number of tool calls allowed for this single turn, so you cannot call any more tools right now. Do not attempt another tool call.\n\nFirst, report the substance of what this turn produced, grounded in the tool results above: what you found, what you changed, or what you established — whichever this task was. Be concrete about it. If you were gathering information, give the information itself rather than saying that you gathered it; if you were changing something, say what is now different. Include your most recent tool calls: they completed, and this is your only opportunity to use them.\n\nThen close with a brief **Still to do** line naming what remains, so the user can ask you to carry on.\n\nLet the length follow the substance — neither a one-line status nor a raw dump of tool output. If the results support no conclusion yet, say that plainly and name what is missing.",
    final_write_instruction: "This is the last call on which you can use a tool, and the only tools left are the ones that write files. Gathering is over — anything you have not found by now will not be found in this turn.\n\nIf this task asked you to produce a file (a report, a guide, a document) and you have not written it yet, write it now with file_write, from what is already in the results above, and mark inside it any part you could not confirm.\n\nIf this task is a code change, apply as much of the change as you can right now as real edits to the source files: apply_patch for targeted edits, or file_write to rewrite a file whose full content you have. A partial set of real edits is worth more than any description of them. Do not write notes, a plan, a summary, a TODO list or any other document into the project in place of the change. If you cannot edit a file exactly, leave it untouched and name it in your reply.\n\nIf there is nothing to write — the task asked only for an answer, or the work is already written — then do not call a tool. Answer instead, and you will be asked to conclude next.",
    final_answer_instruction: "You have finished using tools for this turn but have not yet written a reply to the user. Tools are no longer available and nothing more will run this turn, so do not call any tools and do not describe steps you are about to take. Write a self-contained final message that reports what actually happened: what you found, changed or established, grounded in the tool results above and the tool records below. If the request was not completed, say so and give the reason from the failing tool's own error message, keeping any link it includes. Do not state anything the tool records contradict. If nothing conclusive resulted, say so plainly.\n\nThese directions are addressed to you and are not part of the conversation. Do not quote or restate them, in whole or in part. Do not list or describe the tools available to you. Do not narrate your deliberation: no thinking aloud, no correcting yourself mid-reply, no weighing what to do. Write only the message the user will read.",
    truncated_answer_instruction: "Your last reply ran out of output tokens while you were still reasoning, so this turn ended before you made your next tool call or wrote a reply to the user. The work is unfinished: say so plainly and name what is still left to do. Tools are no longer available and nothing more will run this turn, so do not call any tools and do not describe steps you are about to take. Write a self-contained final message that reports what actually happened: what you found, changed or established, grounded in the tool results above and the tool records below. If the request was not completed, say so and give the reason from the failing tool's own error message, keeping any link it includes. Do not state anything the tool records contradict. If nothing conclusive resulted, say so plainly.\n\nThese directions are addressed to you and are not part of the conversation. Do not quote or restate them, in whole or in part. Do not list or describe the tools available to you. Do not narrate your deliberation: no thinking aloud, no correcting yourself mid-reply, no weighing what to do. Write only the message the user will read.",
    stop_note_preamble: "The harness stopped this turn early because its tool calls stopped making progress: they kept failing, or kept repeating the same step. Its stop note is written for you, not for the user, so explain it in your own words rather than repeating it, and describe each call as the tool records show it:",
    no_tool_calls: "(no tool calls completed)",
    repair_no_reply: "Your previous reply was empty or tried to call a tool. No tools will run: write the message itself.",
    repair_quoted_instruction: "Your previous reply repeated these directions, or the stop note, back to the user. That text is addressed to you alone and must never appear in the message. Do not reproduce any part of it, do not refer to it, and do not explain that you were given it.",
    repair_unverified: "Your previous reply did not pass the check that it reports what actually happened, grounded in the records below.",
    instruction_needles: &[
        ENGLISH_FINAL_ANSWER_NEEDLE,
        ENGLISH_TRUNCATED_ANSWER_NEEDLE,
        ENGLISH_ITERATION_NEEDLE,
        ENGLISH_FINAL_WRITE_NEEDLE,
        "The harness stopped this turn early because its tool calls stopped making progress: they kept failing, or kept repeating the same step. Its stop note is written for you, not for the user, so explain it in your own words rather than repeating it, and describe each call as the tool records show it:",
    ],
    minimum_quoted_words: 10,
    verification_prompt_template: "You are checking a reply before it is shown to a user. Below are the user's request, the records of the tool calls made while handling it, and the reply.\n\nAnswer REJECT if any of these is true:\n1. The reply only says what the assistant will do or is about to do, instead of reporting what happened.\n2. The reply states something the tool records contradict, for example that something does not exist or did not work when a record shows it succeeded, or that something succeeded when its record shows it failed.\n3. The request was not completed, a failed record gives the reason, and the reply does not pass that reason on.\n4. The reply narrates the assistant's own deliberation — thinking aloud, correcting itself part way through, or weighing what to do — rather than stating the outcome. Reporting what a tool call returned is not deliberation.\n5. The reply recites or enumerates the tools the assistant has, instead of reporting what this turn's tool calls produced. Naming a tool the reply actually used, or answering a request that asked what the assistant can do, is not a violation.\n\nOtherwise answer ACCEPT. Reply with the single word ACCEPT or REJECT.\n\n<user_request>\n{user_request}\n</user_request>\n\n<tool_records>\n{records}\n</tool_records>\n\n<reply>\n{reply}\n</reply>",
};

/// Substitute only tokens present in the template; inserted content stays literal.
pub(crate) fn interpolate_once(template: &str, fields: &[(&str, &str)]) -> String {
    let mut output = String::with_capacity(template.len());
    let mut remaining = template;
    while let Some((index, key, value)) = fields
        .iter()
        .filter_map(|(key, value)| remaining.find(key).map(|index| (index, *key, *value)))
        .min_by_key(|(index, _, _)| *index)
    {
        output.push_str(&remaining[..index]);
        output.push_str(value);
        remaining = &remaining[index + key.len()..];
    }
    output.push_str(remaining);
    output
}

/// Build the checkpoint prose around already-rendered results.
pub fn checkpoint_with_copy(
    copy: &CloseCopy,
    steps: usize,
    rendered_results: &str,
    has_results: bool,
) -> String {
    interpolate_once(
        copy.checkpoint_template,
        &[
            ("{steps}", &steps.to_string()),
            (
                "{results}",
                if has_results {
                    rendered_results
                } else {
                    copy.empty_checkpoint_results
                },
            ),
        ],
    )
}

/// Build the fallback close prose around already-rendered results.
pub fn final_summary_with_copy(
    copy: &CloseCopy,
    rendered_results: &str,
    has_results: bool,
) -> String {
    if !has_results {
        return copy.empty_final_summary.to_owned();
    }
    interpolate_once(
        copy.final_summary_template,
        &[("{results}", rendered_results)],
    )
}

/// Build the evidence-grounded closing instruction using host copy.
pub fn final_answer_instruction(
    copy: &CloseCopy,
    stop_reason: Option<&str>,
    truncated: bool,
    records: &str,
) -> String {
    use crate::middleware::wrap_harness_instruction;
    let mut directive = String::new();
    if let Some(reason) = stop_reason {
        directive.push_str(copy.stop_note_preamble);
        directive.push_str("\n<stop_note>\n");
        directive.push_str(reason.trim());
        directive.push_str("\n</stop_note>\n\n");
    }
    directive.push_str(if truncated && stop_reason.is_none() {
        copy.truncated_answer_instruction
    } else {
        copy.final_answer_instruction
    });
    let mut out = wrap_harness_instruction(&directive);
    out.push_str("\n\n<tool_records>\n");
    out.push_str(if records.trim().is_empty() {
        copy.no_tool_calls
    } else {
        records.trim()
    });
    out.push_str("\n</tool_records>");
    out
}

/// Build the verifier prompt without rescanning inserted user or tool content.
pub fn verification_prompt(
    copy: &CloseCopy,
    request: &str,
    records: &str,
    reply: &str,
    request_char_cap: usize,
) -> String {
    let records = if records.trim().is_empty() {
        copy.no_tool_calls
    } else {
        records.trim()
    };
    interpolate_once(
        copy.verification_prompt_template,
        &[
            ("{user_request}", &truncate_chars(request, request_char_cap)),
            ("{records}", records),
            ("{reply}", reply.trim()),
        ],
    )
}

/// Derive normalized quote-detection phrases from a copy bundle.
pub fn instruction_needles(copy: &CloseCopy, stop_reason: Option<&str>) -> Vec<String> {
    let mut sources = copy.instruction_needles.to_vec();
    if let Some(reason) = stop_reason {
        sources.push(reason);
    }
    harness_instruction_needles(&sources, copy.minimum_quoted_words)
}

/// Build the single corrective prompt for a rejected close response.
pub fn repair_with_copy(copy: &CloseCopy, instruction: &str, violation: CloseViolation) -> String {
    let reason = match violation {
        CloseViolation::NoReply => copy.repair_no_reply,
        CloseViolation::QuotedInstruction => copy.repair_quoted_instruction,
        CloseViolation::Unverified => copy.repair_unverified,
    };
    interpolate_once(
        copy.repair_template,
        &[("{repair_reason}", reason), ("{instruction}", instruction)],
    )
}

/// Host-provided model builder for evidence-grounded close calls.
pub trait CloseModelSource: Send + Sync {
    /// Build the selected model for one close or repair request.
    fn build_summarizer(
        &self,
        model: &str,
        temperature: f64,
        thread_id: Option<&str>,
    ) -> Result<std::sync::Arc<dyn ChatModel<()>>, String>;
}

/// Close model that resolves its host model source for each request.
pub struct CloseSourceModel<'a> {
    pub source: &'a dyn CloseModelSource,
    pub model: &'a str,
    pub temperature: f64,
    pub thread_id: Option<&'a str>,
    pub trace: Option<&'a std::sync::Arc<crate::observability::SupplementalCallTrace>>,
}

#[async_trait::async_trait]
impl ChatModel<()> for CloseSourceModel<'_> {
    async fn invoke(
        &self,
        state: &(),
        mut request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        let model = self
            .source
            .build_summarizer(self.model, self.temperature, self.thread_id)
            .map_err(tinyinference_llm::Error::Model)?;
        let mut call = self.trace.map(|trace| {
            crate::observability::SupplementalCallGuard::start(trace, self.model, &mut request)
        });
        match model.invoke(state, request).await {
            Ok(response) => {
                if let Some(call) = &mut call {
                    call.complete(&response);
                }
                Ok(response)
            }
            Err(error) => Err(error),
        }
    }

    async fn stream(
        &self,
        state: &(),
        mut request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelStream> {
        let model = self
            .source
            .build_summarizer(self.model, self.temperature, self.thread_id)
            .map_err(tinyinference_llm::Error::Model)?;
        let call = self.trace.map(|trace| {
            crate::observability::SupplementalCallGuard::start(trace, self.model, &mut request)
        });
        let stream = model.stream(state, request).await?;
        let mut call = call;
        Ok(stream.map_items(move |item| {
            match &item {
                ModelStreamItem::Completed(response) => {
                    if let Some(call) = &mut call {
                        call.complete(response);
                    }
                }
                ModelStreamItem::Failed(_) | ModelStreamItem::ProviderFailed(_) => {
                    call.take();
                }
                _ => {}
            }
            item
        }))
    }
}

/// Usage and context-window accounting for supplemental close requests.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CloseUsage {
    pub model_calls: usize,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub last_call_input_tokens: u64,
    pub last_call_output_tokens: u64,
    pub cost: crate::cost::CostTally,
    pub pricing_model: String,
}

impl CloseUsage {
    /// Empty accounting for supplemental calls made on `model`.
    pub fn for_model(model: &str) -> Self {
        Self {
            pricing_model: model.to_owned(),
            ..Self::default()
        }
    }

    /// Record one attempted request and its provider usage when available.
    pub fn record(
        &mut self,
        _kind: CloseRequestKind,
        usage: Option<&Usage>,
        measures_context: bool,
    ) {
        self.model_calls += 1;
        if let Some(usage) = usage {
            if measures_context {
                self.last_call_input_tokens = usage.input_tokens;
                self.last_call_output_tokens = usage.output_tokens;
            }
            self.input_tokens += usage.input_tokens;
            self.output_tokens += usage.output_tokens;
            self.cached_input_tokens += usage.cache_read_tokens;
        }
    }

    /// Record a close response, excluding verifier calls from final context.
    pub fn record_close_call(&mut self, kind: CloseRequestKind, response: Option<&ModelResponse>) {
        self.record(
            kind,
            response
                .and_then(|response| response.usage.or(response.message.usage))
                .as_ref(),
            kind != CloseRequestKind::Verification,
        );
    }

    /// Record a close request and let the host attach pricing from its billing
    /// and catalog adapters without importing host types into the harness.
    pub fn record_close_call_with_cost(
        &mut self,
        kind: CloseRequestKind,
        response: Option<&ModelResponse>,
        cost: impl FnOnce(Option<&ModelResponse>) -> Option<(String, crate::cost::CallCost)>,
    ) {
        self.record_close_call(kind, response);
        if let Some((pricing_model, call_cost)) = cost(response) {
            self.pricing_model = pricing_model;
            self.cost.add(call_cost);
        }
    }
}

/// Invoke a close builder only when the supplied stop reason is classified.
pub fn classified_halt_close<T>(
    stop_reason: Option<&str>,
    is_classified_halt: impl FnOnce(&str) -> bool,
    build_close: impl FnOnce() -> T,
) -> Option<T> {
    let reason = stop_reason?;
    is_classified_halt(reason).then(build_close)
}

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
    let steps = steps.to_string();
    let rendered = render_tool_results(results, total_budget);
    interpolate_once(template, &[("{steps}", &steps), ("{results}", &rendered)])
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
    let rendered = render_tool_results(results, total_budget);
    interpolate_once(
        template,
        &[
            ("{stop_reason}", stop_reason.unwrap_or("")),
            ("{results}", &rendered),
        ],
    )
}

/// Whether a candidate repeats any harness-only phrase supplied by the host.
/// Matching normalizes case and whitespace and requires at least 24 chars, so
/// short common phrases do not spuriously reject a reply.
pub fn quotes_harness_instruction(candidate: &str, phrases: &[&str]) -> bool {
    quotes_harness_instruction_with_minimum(candidate, phrases, 24)
}

/// Normalize host-provided directive sources and return spans with enough
/// words to serve as quote-detection needles.
///
/// Sources are split on sentence periods and newlines, lowercased, and
/// whitespace-collapsed before filtering. Hosts own the directive text and
/// select the minimum word count appropriate to their policy.
pub fn harness_instruction_needles(sources: &[&str], minimum_words: usize) -> Vec<String> {
    sources
        .iter()
        .flat_map(|source| source.split(['.', '\n']))
        .map(normalize)
        .filter(|span| span.split_whitespace().count() >= minimum_words)
        .collect()
}

fn quotes_harness_instruction_with_minimum(
    candidate: &str,
    phrases: &[&str],
    minimum_chars: usize,
) -> bool {
    let candidate = normalize(candidate);
    phrases.iter().any(|phrase| {
        let phrase = normalize(phrase);
        phrase.chars().count() >= minimum_chars && candidate.contains(&phrase)
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

/// Callback receiving the terminal outcome of one close-related model call.
pub type CloseCallObserver<'a> =
    &'a mut (dyn FnMut(CloseRequestKind, Option<&ModelResponse>) + Send);

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
    /// Minimum normalized phrase length checked by grounded close.
    /// Defaults to 24 Unicode characters for compatibility with the legacy
    /// quote guard. Hosts that prefilter needles by word count may lower it.
    pub minimum_harness_phrase_chars: usize,
    /// Observe each request's terminal completion before validation or retry.
    /// `Some(response)` means the model completed; `None` means the request
    /// failed or its stream ended without a completed response. The callback
    /// is not called for a timeout, a dropped request, or a zero time budget.
    pub on_call_completed: Option<CloseCallObserver<'a>>,
}

impl Default for CloseRequestOptions<'_> {
    fn default() -> Self {
        Self {
            model: None,
            temperature: None,
            max_output_tokens: None,
            minimum_harness_phrase_chars: 24,
            on_call_completed: None,
        }
    }
}

/// Placement of a deterministic required-output block after an unusable
/// repair response.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RequiredOutputFallbackPlacement {
    /// Preserve the TinyAgents legacy order: reply, then synthesized block.
    #[default]
    Append,
    /// Put the block before an unstreamed reply, as some hosts require.
    /// Already-streamed replies always receive the block as a suffix.
    Prepend,
}

/// Request settings and fallback placement for required-output repair.
pub struct RequiredOutputRepairOptions<'a> {
    /// Model-call settings and completion observer.
    pub request: CloseRequestOptions<'a>,
    /// Where to place a deterministic block when the model repair is unusable.
    pub fallback_placement: RequiredOutputFallbackPlacement,
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
            CompletionCall {
                history: config.history,
                prompt: &prompt,
                timeout: remaining,
                options: &mut options,
                kind: CloseRequestKind::Generation,
                stream_mode: stream,
            },
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
        } else if quotes_harness_instruction_with_minimum(
            &candidate,
            config.harness_phrases,
            options.minimum_harness_phrase_chars,
        ) {
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
                CompletionCall {
                    history: &[],
                    prompt: &verify_prompt,
                    timeout: remaining,
                    options: &mut options,
                    kind: CloseRequestKind::Verification,
                    stream_mode: stream,
                },
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
        RequiredOutputFallbackPlacement::Append,
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
    repair_required_output_with_repair_options(
        model,
        state,
        dialect,
        config,
        RequiredOutputRepairOptions {
            request: options,
            fallback_placement: RequiredOutputFallbackPlacement::Append,
        },
    )
    .await
}

/// Repair required output with request and fallback-placement options.
pub async fn repair_required_output_with_repair_options<State, M>(
    model: &M,
    state: &State,
    dialect: &dyn ToolDialect,
    config: RequiredOutputRepairConfig<'_>,
    options: RequiredOutputRepairOptions<'_>,
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
        options.request,
        options.fallback_placement,
        true,
    )
    .await
}

async fn repair_required_output_impl<State, M>(
    model: &M,
    state: &State,
    dialect: &dyn ToolDialect,
    config: RequiredOutputRepairConfig<'_>,
    mut options: CloseRequestOptions<'_>,
    fallback_placement: RequiredOutputFallbackPlacement,
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
        CompletionCall {
            history: config.history,
            prompt: config.repair_instruction,
            timeout: None,
            options: &mut options,
            kind: CloseRequestKind::RequiredOutputRepair,
            stream_mode: stream,
        },
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
    } else if fallback_placement == RequiredOutputFallbackPlacement::Prepend {
        format!("{}\n\n{}", synthesize_block(config.contract), config.reply)
    } else {
        format!("{}\n\n{}", config.reply, synthesize_block(config.contract))
    };
    Some(RequiredOutputRepair {
        text,
        model_repaired: usable,
        usage,
    })
}

struct CompletionCall<'call, 'options> {
    history: &'call [Message],
    prompt: &'call str,
    timeout: Option<std::time::Duration>,
    options: &'call mut CloseRequestOptions<'options>,
    kind: CloseRequestKind,
    stream_mode: bool,
}

async fn completion<State, M>(
    model: &M,
    state: &State,
    dialect: &dyn ToolDialect,
    call: CompletionCall<'_, '_>,
) -> Option<(String, Vec<Usage>, bool)>
where
    M: ChatModel<State> + ?Sized,
    State: Send + Sync,
{
    use futures::StreamExt;

    let CompletionCall {
        history,
        prompt,
        timeout,
        options,
        kind,
        stream_mode,
    } = call;

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
