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

/// Token and cost usage for a run whose delegated usage is already included.
///
/// This totals-only value is for hosts that record finalized, inclusive
/// sub-agent entries without exposing run identity or a child tree. Each
/// contribution passed to [`roll_up_inclusive_subagent_usage`] is counted
/// once as supplied; callers must not pass both an inclusive parent and its
/// descendants as separate contributions.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct UsageContribution {
    /// Input tokens consumed by the run, including its descendants.
    pub input_tokens: u64,
    /// Output tokens consumed by the run, including its descendants.
    pub output_tokens: u64,
    /// Cached input tokens, a subset of `input_tokens`.
    pub cached_input_tokens: u64,
    /// Cost for the run, including its descendants, with certainty retained.
    pub cost: CostTally,
}

/// Rolls already-inclusive child usage into a root contribution.
///
/// Every supplied child contribution is added exactly once. This function
/// does not deduplicate, recurse, or infer relationships between entries;
/// each child's totals already include any descendants. Token sums saturate
/// at `u64::MAX`, cached tokens remain a subset, and [`CostTally::merge`]
/// preserves known amounts and the least certain cost source.
pub fn roll_up_inclusive_subagent_usage(
    root: UsageContribution,
    children: impl IntoIterator<Item = UsageContribution>,
) -> UsageContribution {
    let mut result = root;
    for child in children {
        merge_usage_totals(
            &mut result.input_tokens,
            &mut result.output_tokens,
            &mut result.cached_input_tokens,
            &mut result.cost,
            child,
        );
    }
    result
}

fn merge_usage_totals(
    input_tokens: &mut u64,
    output_tokens: &mut u64,
    cached_input_tokens: &mut u64,
    cost: &mut CostTally,
    contribution: UsageContribution,
) {
    *input_tokens = input_tokens.saturating_add(contribution.input_tokens);
    *output_tokens = output_tokens.saturating_add(contribution.output_tokens);
    *cached_input_tokens = cached_input_tokens.saturating_add(contribution.cached_input_tokens);
    cost.merge(contribution.cost);
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
        merge_usage_totals(
            &mut result.input_tokens,
            &mut result.output_tokens,
            &mut result.cached_input_tokens,
            &mut result.cost,
            UsageContribution {
                input_tokens: child.input_tokens,
                output_tokens: child.output_tokens,
                cached_input_tokens: child.cached_input_tokens,
                cost: child.cost,
            },
        );
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
