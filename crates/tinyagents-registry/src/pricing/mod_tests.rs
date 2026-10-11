use super::*;

#[test]
fn exact_lookup_resolves_canonical_ids() {
    let p = lookup("claude-opus-4-8").expect("anthropic row");
    assert_eq!(p.provider, "anthropic");
    assert_eq!(p.input_per_mtok_usd, 5.00);
    assert_eq!(p.output_per_mtok_usd, 25.00);
    assert_eq!(p.cached_input_per_mtok_usd, 0.50);
    assert_eq!(p.context_window, 1_000_000);
}

#[test]
fn lookup_is_case_insensitive() {
    assert_eq!(lookup("GPT-4.1").unwrap().model_id, "gpt-4.1");
}

#[test]
fn lookup_strips_vendor_prefix_openrouter_style() {
    assert_eq!(
        lookup("anthropic/claude-sonnet-4-6").unwrap().model_id,
        "claude-sonnet-4-6"
    );
    assert_eq!(
        lookup("deepseek/deepseek-chat").unwrap().model_id,
        "deepseek-chat"
    );
    assert_eq!(lookup("qwen/qwen3-max").unwrap().model_id, "qwen3-max");
}

#[test]
fn lookup_strips_context_and_tag_decorations() {
    assert_eq!(
        lookup("claude-opus-4-8[1m]").unwrap().model_id,
        "claude-opus-4-8"
    );
    assert_eq!(lookup("kimi-k2.6:turbo").unwrap().model_id, "kimi-k2.6");
    assert_eq!(
        lookup("claude-opus-4-5@20251101").unwrap().model_id,
        "claude-opus-4-5"
    );
}

#[test]
fn lookup_longest_substring_wins_for_suffixed_ids() {
    // A dated/suffixed id should resolve to the most specific row.
    assert_eq!(
        lookup("gpt-5.4-mini-2026-05-01").unwrap().model_id,
        "gpt-5.4-mini"
    );
}

#[test]
fn lookup_returns_none_for_unknown() {
    assert!(lookup("totally-made-up-model").is_none());
    assert!(lookup("").is_none());
    assert!(
        lookup("agentic-v1").is_none(),
        "abstract tiers aren't vendor models"
    );
}

#[test]
fn every_row_has_sane_values() {
    for p in KNOWN_MODEL_PRICING {
        assert!(p.input_per_mtok_usd > 0.0, "{}", p.model_id);
        assert!(p.output_per_mtok_usd > 0.0, "{}", p.model_id);
        assert!(p.context_window > 0, "{}", p.model_id);
        assert!(
            p.cached_input_per_mtok_usd <= p.input_per_mtok_usd,
            "{} cached should not exceed input",
            p.model_id
        );
    }
}

fn approx(a: f64, b: f64) {
    assert!((a - b).abs() < 1e-9, "expected {b}, got {a}");
}

#[test]
fn estimate_prices_standard_input_and_output() {
    // opus-4-8: $5/$25 per MTok in/out. 1M in + 1M out, no cache.
    approx(
        estimate_cost_usd("claude-opus-4-8", 1_000_000, 1_000_000, 0),
        30.00,
    );
}

#[test]
fn estimate_bills_cached_prefix_at_the_cheaper_rate() {
    // Fully cached input (cached == input) → cached rate only ($0.50/MTok).
    approx(
        estimate_cost_usd("claude-opus-4-8", 1_000_000, 0, 1_000_000),
        0.50,
    );
    // Half cached: 0.5M standard @ $5 + 0.5M cached @ $0.50 = 2.50 + 0.25.
    approx(
        estimate_cost_usd("claude-opus-4-8", 1_000_000, 0, 500_000),
        2.75,
    );
}

#[test]
fn estimate_clamps_cached_to_input() {
    // cached_input_tokens > input_tokens must not underflow or overcharge:
    // it is clamped to input, so this is billed as fully cached.
    approx(
        estimate_cost_usd("claude-opus-4-8", 1_000_000, 0, 5_000_000),
        0.50,
    );
}

#[test]
fn estimate_returns_zero_for_uncatalogued_models() {
    // "unknown, not free" — the caller treats 0.0 as no estimate available.
    assert_eq!(
        estimate_cost_usd("totally-made-up-model", 1_000_000, 1_000_000, 0),
        0.0
    );
}

#[test]
fn estimate_resolves_decorated_model_ids() {
    // The catalog lookup normalizes tags/suffixes, so a decorated id
    // (e.g. the runtime "[1m]" window tag) still prices correctly.
    approx(
        estimate_cost_usd("claude-opus-4-8[1m]", 1_000_000, 0, 0),
        5.00,
    );
}

#[test]
fn minimax_m3_uses_standard_tier_through_512k() {
    approx(estimate_cost_usd("minimax-m3", 512_000, 0, 0), 0.1536);
    approx(estimate_cost_usd("minimax-m3", 512_001, 0, 0), 0.3072006);
}

#[test]
fn short_ids_do_not_match_inside_unrelated_tokens() {
    assert!(lookup("proto3-chat").is_none());
    assert!(lookup("solo1-7b").is_none());
    assert_eq!(
        lookup("gpt-5.4-mini-2026-05-01").unwrap().model_id,
        "gpt-5.4-mini"
    );
}
