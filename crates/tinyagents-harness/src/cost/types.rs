//! Cost accounting types.
//!
//! [`CostTotals`] is the additive value that lets cost roll up across a
//! recursive run tree (model call → run → parent run).

use super::CostTally;
use serde::{Deserialize, Serialize};

/// Per-token pricing for a model.
///
/// Every field is optional: `None` means the price is unknown or does not
/// apply, rather than that the token class is free.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ModelPricing {
    /// Price per input token.
    #[serde(default)]
    pub input_per_token: Option<f64>,
    /// Price per output token.
    #[serde(default)]
    pub output_per_token: Option<f64>,
    /// Discounted price per cached input token.
    #[serde(default)]
    pub cache_read_input_per_token: Option<f64>,
    /// Price per input token written to a prompt cache.
    #[serde(default)]
    pub cache_creation_input_per_token: Option<f64>,
    /// Price per audio input token.
    #[serde(default)]
    pub input_audio_per_token: Option<f64>,
    /// Price per reasoning output token.
    #[serde(default)]
    pub output_reasoning_per_token: Option<f64>,
    /// Context-size-tiered pricing (some providers charge more once a call's
    /// context crosses a threshold, e.g. Gemini's price step above 200K
    /// input tokens). Empty by default: when set, [`crate::cost::estimate_cost`]
    /// selects the tier matching the call's input-token count and uses its
    /// rates in place of the flat fields above (falling back to the flat
    /// fields for any rate a matched tier leaves `None`). Tiers do not need
    /// to be pre-sorted; the matching tier is the one with the smallest
    /// [`PriceTier::up_to_tokens`] that is still `>=` the call's input-token
    /// count, or the tier with no `up_to_tokens` (unlimited) when the count
    /// exceeds every capped tier.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tiers: Vec<PriceTier>,
}

/// One context-size pricing tier. See [`ModelPricing::tiers`].
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PriceTier {
    /// Upper bound (inclusive) on input tokens this tier applies to. `None`
    /// means "no upper bound" — the tier for any call whose input-token
    /// count exceeds every other tier's `up_to_tokens`.
    #[serde(default)]
    pub up_to_tokens: Option<u64>,
    /// Price per input token in this tier. Falls back to
    /// [`ModelPricing::input_per_token`] when `None`.
    #[serde(default)]
    pub input: Option<f64>,
    /// Price per output token in this tier. Falls back to
    /// [`ModelPricing::output_per_token`] when `None`.
    #[serde(default)]
    pub output: Option<f64>,
    /// Price per cached input token in this tier. Falls back to
    /// [`ModelPricing::cache_read_input_per_token`] when `None`.
    #[serde(default)]
    pub cache_read: Option<f64>,
    /// Price per input token written to a prompt cache in this tier. Falls
    /// back to [`ModelPricing::cache_creation_input_per_token`] when `None`.
    #[serde(default)]
    pub cache_write: Option<f64>,
}

/// A breakdown of estimated cost for one or more model calls, in the pricing
/// table's currency (typically USD). All values are accumulating sums.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CostTotals {
    /// Cost attributed to (non-cached) input tokens.
    #[serde(default)]
    pub input_cost: f64,
    /// Cost attributed to output tokens.
    #[serde(default)]
    pub output_cost: f64,
    /// Cost attributed to cache read and cache creation tokens.
    #[serde(default)]
    pub cache_cost: f64,
    /// Cost attributed to reasoning tokens.
    #[serde(default)]
    pub reasoning_cost: f64,
    /// Sum of all component costs.
    #[serde(default)]
    pub total_cost: f64,
}

/// Token and cost usage attributed to a completed delegated run.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SubagentUsage {
    /// Stable run identity used to ignore replayed completion entries.
    pub run_id: String,
    /// Task identity. A task may have multiple distinct runs.
    pub task_id: String,
    /// Input tokens consumed by this run itself.
    pub input_tokens: u64,
    /// Output tokens consumed by this run itself.
    pub output_tokens: u64,
    /// Cached input tokens, a subset of this run's `input_tokens`.
    pub cached_input_tokens: u64,
    /// This run's cost with its certainty source retained.
    pub cost: CostTally,
    /// Descendant runs, each carrying only its own spend.
    pub children: Vec<SubagentUsage>,
}

/// Aggregate usage for a root run and its unique delegated run tree.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UsageRollup {
    /// Input tokens including child runs.
    pub input_tokens: u64,
    /// Output tokens including child runs.
    pub output_tokens: u64,
    /// Cached input tokens including child runs.
    pub cached_input_tokens: u64,
    /// Cost including child runs; unknown source is preserved.
    pub cost: CostTally,
    /// Flattened child details in first-seen order, with replayed run ids omitted.
    pub subagents: Vec<SubagentUsage>,
}

/// Rolls child usage into one run while ignoring duplicate/replayed completion
/// rows by run identity. Each child carries only its own spend; descendants
/// stay nested so an aggregate child plus a separate grandchild row cannot be
/// double-counted. Distinct runs of the same task remain distinct. Cached
/// tokens remain a subset and are not added twice.
pub fn roll_up_subagent_usage(
    input_tokens: u64,
    output_tokens: u64,
    cached_input_tokens: u64,
    cost: CostTally,
    children: impl IntoIterator<Item = SubagentUsage>,
) -> UsageRollup {
    use std::collections::HashSet;

    let mut result = UsageRollup {
        input_tokens,
        output_tokens,
        cached_input_tokens,
        cost,
        subagents: Vec::new(),
    };
    fn visit(result: &mut UsageRollup, seen: &mut HashSet<String>, child: SubagentUsage) {
        if !seen.insert(child.run_id.clone()) {
            return;
        }
        result.input_tokens = result.input_tokens.saturating_add(child.input_tokens);
        result.output_tokens = result.output_tokens.saturating_add(child.output_tokens);
        result.cached_input_tokens = result
            .cached_input_tokens
            .saturating_add(child.cached_input_tokens);
        result.cost.merge(child.cost);
        let descendants = child.children.clone();
        result.subagents.push(SubagentUsage {
            children: Vec::new(),
            ..child
        });
        for descendant in descendants {
            visit(result, seen, descendant);
        }
    }
    let mut seen = HashSet::new();
    for child in children {
        visit(&mut result, &mut seen, child);
    }
    result
}
