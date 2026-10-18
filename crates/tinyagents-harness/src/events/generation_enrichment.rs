//! Content-free host metadata attached to one completed or in-flight generation.

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _, ser::Error as _};

/// Provider and pricing metadata the generic harness cannot derive itself.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GenerationEnrichment {
    /// Stable provider identifier, when known.
    pub provider_id: Option<String>,
    /// Provider pricing rates in USD per million tokens, when known.
    pub pricing: Option<GenerationPricing>,
    /// Host-side estimated cost in USD, when available.
    pub estimated_cost_usd: Option<f64>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationEnrichmentWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pricing: Option<GenerationPricing>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    estimated_cost_usd: Option<f64>,
}

impl Serialize for GenerationEnrichment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(S::Error::custom)?;
        GenerationEnrichmentWire {
            provider_id: self.provider_id.clone(),
            pricing: self.pricing.clone(),
            estimated_cost_usd: self.estimated_cost_usd,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GenerationEnrichment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GenerationEnrichmentWire::deserialize(deserializer)?;
        let enrichment = Self {
            provider_id: wire.provider_id,
            pricing: wire.pricing,
            estimated_cost_usd: wire.estimated_cost_usd,
        };
        enrichment.validate().map_err(D::Error::custom)?;
        Ok(enrichment)
    }
}

impl GenerationEnrichment {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self
            .provider_id
            .as_deref()
            .is_some_and(|id| !is_valid_provider_id(id))
        {
            return Err("provider_id must be a bounded machine identifier");
        }
        if self
            .estimated_cost_usd
            .is_some_and(|amount| !valid_usd(amount))
        {
            return Err("estimated_cost_usd must be finite and non-negative");
        }
        if let Some(pricing) = &self.pricing {
            pricing.validate()?;
        }
        Ok(())
    }
}

/// Host pricing basis attached to a generation span.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GenerationPricing {
    /// Uncached input price in USD per million tokens.
    pub input_per_mtok_usd: Option<f64>,
    /// Cached input price in USD per million tokens.
    pub cached_input_per_mtok_usd: Option<f64>,
    /// Output price in USD per million tokens.
    pub output_per_mtok_usd: Option<f64>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationPricingWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input_per_mtok_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cached_input_per_mtok_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    output_per_mtok_usd: Option<f64>,
}

impl Serialize for GenerationPricing {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(S::Error::custom)?;
        GenerationPricingWire {
            input_per_mtok_usd: self.input_per_mtok_usd,
            cached_input_per_mtok_usd: self.cached_input_per_mtok_usd,
            output_per_mtok_usd: self.output_per_mtok_usd,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GenerationPricing {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GenerationPricingWire::deserialize(deserializer)?;
        let pricing = Self {
            input_per_mtok_usd: wire.input_per_mtok_usd,
            cached_input_per_mtok_usd: wire.cached_input_per_mtok_usd,
            output_per_mtok_usd: wire.output_per_mtok_usd,
        };
        pricing.validate().map_err(D::Error::custom)?;
        Ok(pricing)
    }
}

impl GenerationPricing {
    fn validate(&self) -> Result<(), &'static str> {
        if [
            self.input_per_mtok_usd,
            self.cached_input_per_mtok_usd,
            self.output_per_mtok_usd,
        ]
        .into_iter()
        .flatten()
        .any(|amount| !valid_usd(amount))
        {
            return Err("pricing rates must be finite and non-negative");
        }
        Ok(())
    }
}

fn valid_usd(amount: f64) -> bool {
    amount.is_finite() && amount >= 0.0
}

pub(crate) fn is_valid_provider_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

#[cfg(test)]
#[path = "generation_enrichment_tests.rs"]
mod tests;
