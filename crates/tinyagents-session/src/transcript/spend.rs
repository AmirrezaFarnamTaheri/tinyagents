//! Token/cost spend totals read back from persisted session transcripts.
//!
//! Every transcript records only what the agent that owns it spent, so walking a
//! thread's root transcripts and their `{root_stem}__{child}` descendants counts
//! each token exactly once. Pricing (re-auditing cost at current rates) is the
//! host's concern and stays out of here.

use std::collections::{BTreeMap, HashSet};

use super::{
    DisplayRecord, DisplaySessionTranscript, SessionTranscript, ThreadTranscript,
    TranscriptLocator, TranscriptMessage, TranscriptMeta, TurnUsage, UsageCostSource,
};

/// One transcript's own spend, summed from the per-turn `turn_usage` records
/// the codec attaches to its assistant rows.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TranscriptSpend {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub cost_usd: f64,
    /// Turns that recorded usage. The codec emits one record per durable
    /// append, and omits an all-zero one, so this is "turns that spent".
    pub turns: usize,
    /// The newest record's model and window, for the caller's last-turn view.
    /// `last_input_tokens` / `last_output_tokens` are that turn's spend, summed
    /// over every call it made; they are not a context size.
    pub last_input_tokens: u64,
    pub last_output_tokens: u64,
    /// Tokens the context held after the newest turn's final call (its input
    /// plus its reply): the numerator of a context-window gauge. See
    /// [`context_tokens_of`] for records written before the per-call fields.
    pub last_context_tokens: u64,
    /// Final call's input and output are kept separately for readers that need
    /// to exclude generated output from conversation-history estimates.
    pub last_context_input_tokens: u64,
    pub last_context_output_tokens: u64,
    pub model: Option<String>,
    pub context_window: u64,
    /// `cost_usd` split by how much of it is known; see [`CostSplit`].
    pub cost_split: CostSplit,
}

/// A transcript's recorded cost, split into the part whose source is stated
/// and the turns whose cost is not known.
///
/// `cost_usd` sums every record, including records written before
/// [`MessageUsage::cost_source`](super::MessageUsage::cost_source) existed,
/// whose cost may be a host's guessed rate. A reader that presents spend
/// should use `priced_cost_usd` and re-price (or decline to report) the
/// `unpriced_*` tokens instead.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CostSplit {
    /// Sum of `cost_usd` over records stating a `Charged` or `Estimated` source.
    pub priced_cost_usd: f64,
    /// Least certain of those records' sources; `None` when there were none.
    pub priced_source: Option<UsageCostSource>,
    /// Turns whose cost is not known: no stated source, or `Unknown`.
    pub unpriced_turns: usize,
    /// Token counts of those turns, for a reader that can price them.
    pub unpriced_input_tokens: u64,
    pub unpriced_output_tokens: u64,
    pub unpriced_cached_input_tokens: u64,
}

impl CostSplit {
    /// Fold one turn's record in.
    fn add(&mut self, usage: &TurnUsage) {
        match usage.usage.cost_source {
            Some(source @ (UsageCostSource::Charged | UsageCostSource::Estimated)) => {
                self.priced_cost_usd += usage.usage.cost_usd;
                self.priced_source = Some(self.priced_source.map_or(source, |s| s.max(source)));
            }
            Some(UsageCostSource::Unknown) | None => {
                self.unpriced_turns += 1;
                self.unpriced_input_tokens =
                    self.unpriced_input_tokens.saturating_add(usage.usage.input);
                self.unpriced_output_tokens = self
                    .unpriced_output_tokens
                    .saturating_add(usage.usage.output);
                self.unpriced_cached_input_tokens = self
                    .unpriced_cached_input_tokens
                    .saturating_add(usage.usage.cached_input);
            }
        }
    }

    /// Fold another transcript's split in.
    pub fn merge(&mut self, other: &CostSplit) {
        self.priced_cost_usd += other.priced_cost_usd;
        self.priced_source = match (self.priced_source, other.priced_source) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        self.unpriced_turns += other.unpriced_turns;
        self.unpriced_input_tokens = self
            .unpriced_input_tokens
            .saturating_add(other.unpriced_input_tokens);
        self.unpriced_output_tokens = self
            .unpriced_output_tokens
            .saturating_add(other.unpriced_output_tokens);
        self.unpriced_cached_input_tokens = self
            .unpriced_cached_input_tokens
            .saturating_add(other.unpriced_cached_input_tokens);
    }
}

/// Context occupancy recorded by one turn's usage: the final call's input plus
/// its reply.
///
/// A record written before `last_call_input` existed carries only the turn's
/// summed spend. A single-call turn's sum *is* its call, so it is exact; a
/// multi-call turn's is divided by its call count (`iteration`), the mean
/// request size. That undercounts the final, largest request but stays inside
/// the window, where the raw sum reported a 72-call turn at 5.7M tokens of a
/// 1M window.
pub fn context_tokens_of(usage: &TurnUsage) -> u64 {
    let record = &usage.usage;
    if record.last_call_input > 0 || record.last_call_output > 0 {
        return record
            .last_call_input
            .saturating_add(record.last_call_output);
    }
    let calls = u64::from(usage.iteration.max(1));
    record.input.saturating_add(record.output) / calls
}

/// Input tokens of the final provider call, with an averaged legacy fallback.
pub fn context_input_tokens_of(usage: &TurnUsage) -> u64 {
    let record = &usage.usage;
    if record.last_call_input > 0 || record.last_call_output > 0 {
        return record.last_call_input;
    }
    let calls = u64::from(usage.iteration.max(1));
    record.input / calls
}

/// Output tokens of the final provider call, with an averaged legacy fallback.
pub fn context_output_tokens_of(usage: &TurnUsage) -> u64 {
    let record = &usage.usage;
    if record.last_call_input > 0 || record.last_call_output > 0 {
        return record.last_call_output;
    }
    let calls = u64::from(usage.iteration.max(1));
    record.output / calls
}

/// Total one transcript's own recorded spend over its logical message set.
///
/// The `_meta` header carries denormalised rollups for the same figures, but
/// nothing has written them on the root path since the TinyAgents runtime
/// cutover — every root transcript since reads `input_tokens: 0`
/// while its per-turn records hold the real numbers (#6460). The per-turn
/// records are the authoritative copy and the only one that is written on every
/// path, so this reads those and ignores the header.
///
/// This sums only what `transcript.messages` still holds: after a compaction
/// that is the reduced context, so it undercounts a compacted session. The
/// thread aggregate ([`thread_spend_in`]) reads the full append history instead.
pub fn transcript_spend(transcript: &SessionTranscript) -> TranscriptSpend {
    spend_from_usages(
        transcript
            .messages
            .iter()
            .filter_map(|message| message.turn_usage.as_ref()),
        transcript.meta.model.as_ref(),
    )
}

/// Sum a sequence of per-turn usage records, oldest first.
fn spend_from_usages<'a>(
    usages: impl IntoIterator<Item = &'a TurnUsage>,
    header_model: Option<&String>,
) -> TranscriptSpend {
    let mut spend = TranscriptSpend::default();
    for usage in usages {
        // A text-dialect tool round's issuing row carries a provenance-only
        // record (its calls, zero spend); the turn's spend is on its final row.
        // It is not a turn that spent, and must not become the "last" one.
        if !usage.tool_calls.is_empty()
            && usage.usage.input == 0
            && usage.usage.output == 0
            && usage.usage.cached_input == 0
            && usage.usage.cost_usd == 0.0
        {
            continue;
        }
        spend.input_tokens = spend.input_tokens.saturating_add(usage.usage.input);
        spend.output_tokens = spend.output_tokens.saturating_add(usage.usage.output);
        spend.cached_input_tokens = spend
            .cached_input_tokens
            .saturating_add(usage.usage.cached_input);
        spend.cost_usd += usage.usage.cost_usd;
        spend.cost_split.add(usage);
        spend.turns += 1;
        spend.last_input_tokens = usage.usage.input;
        spend.last_output_tokens = usage.usage.output;
        spend.last_context_tokens = context_tokens_of(usage);
        spend.last_context_input_tokens = context_input_tokens_of(usage);
        spend.last_context_output_tokens = context_output_tokens_of(usage);
        if usage.usage.context_window > 0 {
            spend.context_window = usage.usage.context_window;
        }
        if !usage.model.is_empty() {
            spend.model = Some(usage.model.clone());
        }
    }
    // A transcript whose model never landed on a usage record still has one in
    // its header; prefer the record, fall back to the header.
    if spend.model.is_none() {
        spend.model = header_model.cloned();
    }
    spend
}

/// Spend of one transcript file over its whole append history.
///
/// Compaction never deletes lines: the earlier rows (and their usage) stay in
/// the file, and the compaction record carries the reduced context. Reading
/// the model-context view would drop every usage record a compaction summarised
/// away, so this walks the whole log instead:
///
/// - every appended message line is a distinct durable append and always
///   counts (interrupted partials carry no spend and are skipped);
/// - a compaction replacement mixes kept history with the rows of the turn
///   that wrote it. Kept rows were read back from the transcript and keep
///   their original `request_id`; the turn's own rows take the compaction's
///   `request_id`. So a replacement row's usage counts only when its
///   `request_id` is the compaction's and no appended line already counted
///   spend under that `request_id`. A compaction written without a
///   `request_id` falls back to the usage on its final assistant row.
///
/// `earlier_requests` holds the `request_id`s that earlier generation files of
/// the same conversation already counted. A successor generation starts by
/// writing the rows it retained from its parent, and those rows keep their
/// original `request_id`, so a row whose `request_id` is in that set is
/// carried history, not new spend. The ids this file counts are added to it.
fn display_spend(
    display: DisplaySessionTranscript,
    earlier_requests: &mut HashSet<String>,
) -> (TranscriptSpend, TranscriptMeta) {
    let mut usages: Vec<&TurnUsage> = Vec::new();
    let mut counted_requests: HashSet<&str> = HashSet::new();
    // The logical context as of the previous record: what a compaction's kept
    // rows are carried over from.
    let mut context: Vec<&TranscriptMessage> = Vec::new();
    for record in &display.records {
        match record {
            DisplayRecord::Message(message) if !message.interrupted => {
                let carried = message
                    .request_id
                    .as_deref()
                    .is_some_and(|request_id| earlier_requests.contains(request_id));
                if !carried && let Some(usage) = message.message.turn_usage.as_ref() {
                    usages.push(usage);
                    if let Some(request_id) = message.request_id.as_deref() {
                        counted_requests.insert(request_id);
                    }
                }
                context.push(&message.message);
            }
            DisplayRecord::Message(_) => {}
            DisplayRecord::Compaction(marker) => {
                match marker.request_id.as_deref() {
                    Some(turn)
                        if !counted_requests.contains(turn) && !earlier_requests.contains(turn) =>
                    {
                        let fresh: Vec<&TurnUsage> = marker
                            .replacement
                            .iter()
                            .filter(|row| row.request_id.as_deref() == Some(turn))
                            .filter_map(|row| row.message.turn_usage.as_ref())
                            .collect();
                        if !fresh.is_empty() {
                            counted_requests.insert(turn);
                        }
                        usages.extend(fresh);
                    }
                    Some(_) => {}
                    None => {
                        // A compaction written without a `request_id` has no
                        // turn identity. Its writing turn's spend sits on the
                        // replacement's final assistant row; that row is fresh
                        // unless it was carried over from the context this
                        // compaction replaced (same position-free row:
                        // role, content and usage).
                        if let Some(row) = marker
                            .replacement
                            .iter()
                            .rev()
                            .find(|row| row.message.role == "assistant")
                            && let Some(usage) = row.message.turn_usage.as_ref()
                            && !context
                                .iter()
                                .any(|prior| carried_from(prior, &row.message))
                        {
                            usages.push(usage);
                        }
                    }
                }
                context = marker.replacement.iter().map(|row| &row.message).collect();
            }
        }
    }
    let spend = spend_from_usages(usages, display.meta.model.as_ref());
    earlier_requests.extend(counted_requests.into_iter().map(str::to_owned));
    (spend, display.meta)
}

/// Whether `row` in a compaction replacement is `prior` carried over: same
/// role, content, typed structure and usage record.
fn carried_from(prior: &TranscriptMessage, row: &TranscriptMessage) -> bool {
    prior.role == row.role
        && prior.content == row.content
        && prior.tool_calls == row.tool_calls
        && prior.tool_call_id == row.tool_call_id
        && prior.parts == row.parts
        && prior.turn_usage == row.turn_usage
}

/// One thread's spend, split into the orchestrator's own and each sub-agent
/// archetype's, each counted exactly once.
#[derive(Debug, Clone, Default)]
pub struct ThreadSpend {
    pub root: TranscriptSpend,
    /// Keyed by archetype (`_meta.agent`), with a run count.
    pub subagents: BTreeMap<String, (TranscriptSpend, usize)>,
    pub updated: Option<String>,
    pub found_transcript: bool,
}

/// A thread's spend over a [`TranscriptLocator`]: the thread's roots and
/// sub-agent transcripts come from
/// [`TranscriptLocator::thread_transcripts`], each counted once — the roots
/// in projection order with one shared set of counted requests, every listed
/// sub-agent into its archetype. Unlike the file walk, a sub-agent the
/// listing finds by its `_meta.thread_id` (not only by stem) is counted too.
pub fn thread_spend_in(locator: &dyn TranscriptLocator, thread_id: &str) -> ThreadSpend {
    let mut out = ThreadSpend::default();
    let Some(listed) = locator.thread_transcripts(thread_id, None) else {
        return out;
    };
    let mut root_requests = HashSet::new();
    for root in &listed.roots {
        out.found_transcript = true;
        if let Some(display) = read_listed(root) {
            let (spend, meta) = display_spend(display, &mut root_requests);
            out.add_root(spend, meta);
        }
    }
    let children = listed
        .subagents
        .iter()
        .map(|child| (child.stem.clone(), child));
    for chain in subagent_chains(children) {
        // As in the file walk: one run and one set of counted requests per
        // compaction chain.
        let mut requests = HashSet::new();
        let mut new_run = true;
        for child in chain {
            if let Some(display) = read_listed(child) {
                let (spend, meta) = display_spend(display, &mut requests);
                out.add_subagent(spend, &meta, std::mem::take(&mut new_run));
            }
        }
    }
    out
}

/// Groups sub-agent transcripts into compaction chains: a compaction seals
/// `{stem}` and opens `{stem}.g1`, `{stem}.g2`, … Chains come out ordered by
/// their base stem, each one's generations oldest first.
fn subagent_chains<T>(items: impl IntoIterator<Item = (String, T)>) -> Vec<Vec<T>> {
    let mut chains: BTreeMap<String, Vec<(u32, String, T)>> = BTreeMap::new();
    for (stem, item) in items {
        let (base, generation) = match stem.rsplit_once(".g") {
            Some((base, digits))
                if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) =>
            {
                (base.to_string(), digits.parse().unwrap_or(u32::MAX))
            }
            _ => (stem.clone(), 0),
        };
        chains
            .entry(base)
            .or_default()
            .push((generation, stem, item));
    }
    chains
        .into_values()
        .map(|mut chain| {
            chain.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            chain.into_iter().map(|(_, _, item)| item).collect()
        })
        .collect()
}

/// A listed transcript's display projection; unreadable or missing ones are
/// skipped with a warning, as the file walk skips an unreadable file.
fn read_listed(listed: &ThreadTranscript) -> Option<DisplaySessionTranscript> {
    match listed.transcript.read_display() {
        Ok(found) => found,
        Err(err) => {
            tracing::warn!(
                "[transcript:spend] skipping unreadable transcript {}: {err}",
                listed.stem
            );
            None
        }
    }
}

impl ThreadSpend {
    /// Adds one root transcript's own spend; roots arrive oldest first, so
    /// the last one to report a turn owns the last-turn view.
    fn add_root(&mut self, spend: TranscriptSpend, meta: TranscriptMeta) {
        self.root.input_tokens = self.root.input_tokens.saturating_add(spend.input_tokens);
        self.root.output_tokens = self.root.output_tokens.saturating_add(spend.output_tokens);
        self.root.cached_input_tokens = self
            .root
            .cached_input_tokens
            .saturating_add(spend.cached_input_tokens);
        self.root.cost_usd += spend.cost_usd;
        self.root.cost_split.merge(&spend.cost_split);
        self.root.turns += spend.turns;
        if spend.turns > 0 {
            self.root.last_input_tokens = spend.last_input_tokens;
            self.root.last_output_tokens = spend.last_output_tokens;
            self.root.last_context_tokens = spend.last_context_tokens;
            self.root.last_context_input_tokens = spend.last_context_input_tokens;
            self.root.last_context_output_tokens = spend.last_context_output_tokens;
        }
        if spend.context_window > 0 {
            self.root.context_window = spend.context_window;
        }
        if spend.model.is_some() {
            self.root.model = spend.model;
        }
        self.updated = Some(meta.updated);
    }

    /// Adds one sub-agent transcript's own spend to its archetype
    /// (`_meta.agent`); `new_run` is false for a later generation of a run
    /// already counted.
    fn add_subagent(&mut self, spend: TranscriptSpend, meta: &TranscriptMeta, new_run: bool) {
        self.found_transcript = true;
        let entry = self.subagents.entry(meta.agent_name.clone()).or_default();
        entry.0.input_tokens = entry.0.input_tokens.saturating_add(spend.input_tokens);
        entry.0.output_tokens = entry.0.output_tokens.saturating_add(spend.output_tokens);
        entry.0.cached_input_tokens = entry
            .0
            .cached_input_tokens
            .saturating_add(spend.cached_input_tokens);
        entry.0.cost_usd += spend.cost_usd;
        entry.0.cost_split.merge(&spend.cost_split);
        entry.0.turns += spend.turns;
        if entry.0.model.is_none() {
            entry.0.model = spend.model;
        }
        if new_run {
            entry.1 += 1;
        }
    }
}

#[cfg(test)]
#[path = "spend_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "spend_cost_tests.rs"]
mod cost_tests;
