//! Offline published model prices, context windows and decorated-id lookup.
//!
//! Values preserve the July 2026 consumer snapshot; estimates remain best effort.
//! This module performs no I/O. Consumers own billing authority, runtime capability
//! overlays and product configuration.

/// Month the published values below were last verified. Bump when refreshing.
pub const PRICING_AS_OF: &str = "2026-07";

mod data;
mod types;

pub use data::KNOWN_MODEL_PRICING;
pub use types::ModelPrice;

/// Normalise a model string for matching: lower-case, trim, drop a trailing
/// `:tag` / `@date` decoration and a `[...]` suffix.
fn normalize(model: &str) -> String {
    let mut s = model.trim().to_ascii_lowercase();
    // Strip a `[1m]`-style context-window suffix.
    if let Some(idx) = s.find('[') {
        s.truncate(idx);
    }
    // Strip `:tag` (e.g. ollama-style) and `@date` (Vertex-style) decorations.
    for sep in [':', '@'] {
        if let Some(idx) = s.find(sep) {
            s.truncate(idx);
        }
    }
    s.trim().to_string()
}

/// Resolve a concrete model string to its catalogued row, if known.
///
/// Match order: exact canonical id → id with a leading `vendor/` segment
/// stripped → longest canonical id that is a substring of the normalised
/// request (handles dated/suffixed ids). Returns `None` for unknown models —
/// callers should fall back to a tier estimate.
///
/// The inherited substring fallback can also match unlisted hyphenated variants
/// to a base model (for example `gpt-4.1-nano` to `gpt-4.1`). This compatibility
/// behavior is preserved during migration and is not authoritative provider-id
/// validation. The snapshot also retains historical cached-rate assumptions
/// and models no long-context estimate tiers except MiniMax M3.
pub fn lookup(model: &str) -> Option<&'static ModelPrice> {
    let norm = normalize(model);
    if norm.is_empty() {
        return None;
    }
    if let Some(p) = KNOWN_MODEL_PRICING.iter().find(|p| p.model_id == norm) {
        return Some(p);
    }
    let bare = norm.rsplit('/').next().unwrap_or(norm.as_str());
    if let Some(p) = KNOWN_MODEL_PRICING.iter().find(|p| p.model_id == bare) {
        return Some(p);
    }
    KNOWN_MODEL_PRICING
        .iter()
        .filter(|p| {
            contains_at_boundary(&norm, p.model_id) || contains_at_boundary(bare, p.model_id)
        })
        .max_by_key(|p| p.model_id.len())
}

/// Whether `needle` occurs in `haystack` at a token boundary — i.e. the
/// characters immediately flanking the match are non-alphanumeric (or the string
/// ends). This keeps the longest-substring fallback matching dated/suffixed ids
/// (`gpt-5.4-mini-2026-05-01` → `gpt-5.4-mini`, flanked by `-`/string end) while
/// refusing spurious mid-token collisions for short canonical ids
/// (`proto3-chat` must NOT match the `o3` row, `solo1-7b` must NOT match `o1`).
/// A naive `contains` overmatches those and, since context-window lookup
/// now routes windows through this catalog, that leaked a bogus 200K window
/// (issue #4249 regression guard).
fn contains_at_boundary(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    let bytes = haystack.as_bytes();
    let mut search_from = 0;
    while let Some(rel) = haystack[search_from..].find(needle) {
        let start = search_from + rel;
        let end = start + needle.len();
        let before_ok = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
        let after_ok = end == haystack.len() || !bytes[end].is_ascii_alphanumeric();
        if before_ok && after_ok {
            return true;
        }
        search_from = start + 1;
    }
    false
}

/// Estimate the USD cost of a single model call from catalogued per-MTok rates.
///
/// Prices the standard (cache-miss) input tokens, the cached-prefix input
/// tokens, and the output tokens separately. `cached_input_tokens` are billed at
/// the (usually cheaper) cached rate and are assumed to be a subset of
/// `input_tokens`, so the standard-rate portion is `input − cached`. Returns
/// `0.0` when the model is not catalogued (caller should treat as "unknown, not
/// free" — this is a best-effort estimate used when the provider does not report
/// a charged amount).
pub fn estimate_cost_usd(
    model: &str,
    input_tokens: u64,
    output_tokens: u64,
    cached_input_tokens: u64,
) -> f64 {
    let Some(p) = lookup(model) else {
        return 0.0;
    };
    let cached = cached_input_tokens.min(input_tokens);
    let standard_input = input_tokens.saturating_sub(cached);
    if p.model_id == "minimax-m3" && input_tokens <= 512_000 {
        return (standard_input as f64) * 0.30 / 1_000_000.0
            + (cached as f64) * 0.06 / 1_000_000.0
            + (output_tokens as f64) * 1.20 / 1_000_000.0;
    }
    let per_tok = |mtok_rate: f64| mtok_rate / 1_000_000.0;
    (standard_input as f64) * per_tok(p.input_per_mtok_usd)
        + (cached as f64) * per_tok(p.cached_input_per_mtok_usd)
        + (output_tokens as f64) * per_tok(p.output_per_mtok_usd)
}

fn per_token(rate_per_mtok: f64) -> Option<f64> {
    (rate_per_mtok > 0.0).then_some(rate_per_mtok / 1_000_000.0)
}

const VISION_INPUT_MODELS: &[&str] = &["minimax-m3"];

/// Whether the published vision allowlist includes this decorated model id.
pub fn model_accepts_image_input(model_id: &str) -> bool {
    let normalized = normalize(model_id);
    let bare = normalized.rsplit('/').next().unwrap_or(normalized.as_str());
    VISION_INPUT_MODELS.contains(&normalized.as_str()) || VISION_INPUT_MODELS.contains(&bare)
}

/// Project one published catalog row as flat metadata (not a tier-aware estimate) into a TinyAgents model-catalog entry.
///
/// This is intentionally a pricing/window projection only. Consumers still
/// derives live runtime capability flags (tools, vision, streaming) from the
/// provider adapter at construction time, because the static cost catalog does
/// not yet encode those fields authoritatively for every provider/model.
///
/// This preserves the migrated flat metadata projection. For MiniMax M3,
/// `estimate_cost_usd` applies a lower rate through 512K input tokens, while
/// this projection uses the row rates and intentionally contains no tiers.
/// Consumers must not treat these two paths as equivalent call estimates.
pub fn flat_catalog_entry(price: &ModelPrice, source: &str) -> crate::ModelCatalogEntry {
    crate::ModelCatalogEntry {
        provider: price.provider.to_string(),
        model_id: price.model_id.to_string(),
        aliases: Vec::new(),
        mode: "chat".to_string(),
        max_input_tokens: Some(u64::from(price.context_window)),
        max_output_tokens: None,
        deprecation_date: None,
        release_date: None,
        pricing: tinyagents_harness::cost::ModelPricing {
            input_per_token: per_token(price.input_per_mtok_usd),
            output_per_token: per_token(price.output_per_mtok_usd),
            cache_read_input_per_token: per_token(price.cached_input_per_mtok_usd),
            cache_creation_input_per_token: None,
            input_audio_per_token: None,
            output_reasoning_per_token: None,
            // The local price table is flat; context-size tiers are not modeled.
            tiers: Vec::new(),
        },
        capabilities: crate::ModelCapabilities {
            prompt_caching: price.cached_input_per_mtok_usd > 0.0,
            vision: model_accepts_image_input(price.model_id),
            ..crate::ModelCapabilities::default()
        },
        source: source.to_string(),
        source_url: None,
        raw: serde_json::json!({ "pricing_as_of": PRICING_AS_OF }),
    }
}

#[cfg(test)]
#[path = "pricing_tests.rs"]
mod tests;
