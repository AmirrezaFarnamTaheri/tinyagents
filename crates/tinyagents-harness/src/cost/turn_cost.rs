//! Per-call and per-turn cost accounting for an agent's tool-call loop.
//!
//! A call's cost is one of three things, never a guess:
//!
//! - **Charged**: the amount the provider actually billed (may be zero for a
//!   free route; that is still a known cost).
//! - **Estimated**: no charge was reported, but the caller supplied list-rate
//!   [`ModelPricing`] for the model, which prices the token counts.
//! - **Unknown**: no charge and no pricing. A turn with any unknown call has
//!   an unknown total.
//!
//! There is deliberately no default rate for an unrecognised model. Pricing
//! lookup is host policy: callers pass the `Option<&ModelPricing>` they
//! resolved for the call.

use tinyinference_llm::usage::Usage;

use super::{ModelPricing, estimate_cost};

/// One provider call's usage plus what the provider said it billed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CallUsage {
    /// Vendor token counts for the call.
    pub usage: Usage,
    /// Amount billed in USD. Zero when unavailable.
    pub charged_amount_usd: f64,
    /// `charged_amount_usd` is a local estimate, not a provider charge.
    pub cost_is_estimate: bool,
    /// The provider reported what it billed, so `charged_amount_usd` is known
    /// even when zero. `false` means zero reads as "unknown", not free.
    pub charge_reported: bool,
}

impl CallUsage {
    /// Input tokens served from the provider cache.
    pub fn cached_input_tokens(&self) -> u64 {
        self.usage.cache_read_tokens
    }
}

/// Price a call from `pricing` when no provider charge was reported.
pub fn estimate_call_cost_usd(pricing: Option<&ModelPricing>, usage: &CallUsage) -> Option<f64> {
    let pricing = pricing?;
    let usage = Usage {
        input_tokens: usage.usage.input_tokens,
        output_tokens: usage.usage.output_tokens,
        cache_read_tokens: usage.cached_input_tokens().min(usage.usage.input_tokens),
        ..Default::default()
    };
    Some(estimate_cost(pricing, &usage).total_cost)
}

/// Where a cost figure came from. Ordered from most to least certain, so the
/// cost of several calls takes the least certain of their sources.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CostSource {
    /// Every call reported the amount the provider billed.
    #[default]
    Charged,
    /// At least one call was priced from list rates.
    Estimated,
    /// At least one call had neither a charge nor a price.
    Unknown,
}

/// The cost of one provider call.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CallCost {
    /// Billed by the provider (may be zero).
    Charged(f64),
    /// List-price estimate.
    Estimated(f64),
    /// Neither available.
    Unknown,
}

impl CallCost {
    /// The USD figure, or `None` when unknown.
    pub fn usd(self) -> Option<f64> {
        match self {
            Self::Charged(usd) | Self::Estimated(usd) => Some(usd),
            Self::Unknown => None,
        }
    }

    /// The source this cost contributes to a total.
    pub fn source(self) -> CostSource {
        match self {
            Self::Charged(_) => CostSource::Charged,
            Self::Estimated(_) => CostSource::Estimated,
            Self::Unknown => CostSource::Unknown,
        }
    }
}

/// The cost of one call: the provider's charge when it reported one, else the
/// estimate from `pricing`, else unknown.
pub fn call_cost(pricing: Option<&ModelPricing>, usage: &CallUsage) -> CallCost {
    if usage.charge_reported && !usage.cost_is_estimate {
        return CallCost::Charged(usage.charged_amount_usd);
    }
    match estimate_call_cost_usd(pricing, usage) {
        Some(usd) => CallCost::Estimated(usd),
        None => CallCost::Unknown,
    }
}

/// A sum of call costs that remembers how certain it is.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CostTally {
    /// Sum of the known call costs. Read it through [`CostTally::usd`].
    pub known_usd: f64,
    /// The least certain source among the calls added.
    pub source: CostSource,
}

impl CostTally {
    /// Fold one call in.
    pub fn add(&mut self, cost: CallCost) {
        self.known_usd += cost.usd().unwrap_or(0.0);
        self.source = self.source.max(cost.source());
    }

    /// Fold another tally in (a sub-step's or a child's).
    pub fn merge(&mut self, other: CostTally) {
        self.known_usd += other.known_usd;
        self.source = self.source.max(other.source);
    }

    /// The total, or `None` when any call's cost was unknown.
    pub fn usd(&self) -> Option<f64> {
        (self.source != CostSource::Unknown).then_some(self.known_usd)
    }
}

/// A host's list-rate lookup: the pricing for a model label, or `None` when
/// the model is not priced (its calls are then `Unknown` unless the provider
/// reports a charge).
pub type PricingLookup = std::sync::Arc<dyn Fn(&str) -> Option<ModelPricing> + Send + Sync>;

/// Running cost / token tally across every provider call inside one turn.
#[derive(Clone, Default)]
pub struct TurnCost {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub cost: CostTally,
    pub call_count: u32,
    /// Pricing the stop-hook accounting ([`StopHookMetrics`](crate::middleware::StopHookMetrics)) resolves each
    /// recorded response's model against. `None` prices nothing, so a call
    /// without a provider charge is `Unknown`.
    pub pricing: Option<PricingLookup>,
}

impl std::fmt::Debug for TurnCost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnCost")
            .field("input_tokens", &self.input_tokens)
            .field("output_tokens", &self.output_tokens)
            .field("cached_input_tokens", &self.cached_input_tokens)
            .field("cost", &self.cost)
            .field("call_count", &self.call_count)
            .field("pricing", &self.pricing.as_ref().map(|_| "set"))
            .finish()
    }
}

impl TurnCost {
    /// New empty accumulator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one provider call's usage into the running totals.
    pub fn add_call(&mut self, pricing: Option<&ModelPricing>, usage: &CallUsage) {
        self.input_tokens = self.input_tokens.saturating_add(usage.usage.input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(usage.usage.output_tokens);
        self.cached_input_tokens = self
            .cached_input_tokens
            .saturating_add(usage.cached_input_tokens());
        self.cost.add(call_cost(pricing, usage));
        self.call_count = self.call_count.saturating_add(1);
    }

    /// An empty accumulator that prices recorded responses through `pricing`.
    pub fn with_pricing(pricing: PricingLookup) -> Self {
        Self {
            pricing: Some(pricing),
            ..Self::default()
        }
    }

    /// [`add_call`](Self::add_call) with the pricing resolved for `model`
    /// through the installed [`PricingLookup`].
    pub fn add_priced_call(&mut self, model: &str, usage: &CallUsage) {
        let pricing = self.pricing.as_ref().and_then(|lookup| lookup(model));
        self.add_call(pricing.as_ref(), usage);
    }

    /// The turn's cost so far, or `None` when any call's cost is unknown.
    pub fn total_usd(&self) -> Option<f64> {
        self.cost.usd()
    }
}

/// Folds every completed model response into the tally: the provider's
/// reported charge when the response carries one, else the list-rate estimate
/// from the installed [`PricingLookup`], else unknown.
impl crate::middleware::StopHookMetrics for TurnCost {
    fn record_model(&mut self, model: &str, response: &tinyinference_llm::model::ModelResponse) {
        let Some(recovered) = tinyinference_llm::usage::recover_usage(response) else {
            return;
        };
        let usage = CallUsage {
            usage: recovered.usage,
            charged_amount_usd: recovered.charged_amount_usd.unwrap_or(0.0),
            cost_is_estimate: false,
            charge_reported: recovered.charged_amount_usd.is_some(),
        };
        self.add_priced_call(model, &usage);
    }
}

#[cfg(test)]
#[path = "turn_cost_tests.rs"]
mod tests;
