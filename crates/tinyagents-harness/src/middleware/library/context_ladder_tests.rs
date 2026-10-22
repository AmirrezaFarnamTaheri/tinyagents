//! Tests for the context ladder ordering and its sizing rules.

use super::*;
use crate::runtime::AgentHarness;

#[test]
fn policy_is_absent_without_a_window_or_override() {
    assert!(compression_policy(None, None).is_none());
    assert!(compression_policy(Some(0), None).is_none());
}

#[test]
fn policy_pins_the_turn_message_and_sizes_the_tail() {
    let policy = compression_policy(Some(100_000), None).unwrap();
    assert!(policy.pin_turn_user_message);
    let keep = policy.keep_recent_tokens.unwrap();
    assert!(keep <= policy.trigger_budget() / 2);
    assert!(keep <= 30_000, "30% of the window at most");
}

#[test]
fn trigger_override_enables_compaction_without_a_window() {
    let policy = compression_policy(None, Some(10_000)).unwrap();
    assert_eq!(policy.trigger_budget(), 10_000);
    assert_eq!(policy.keep_recent_tokens, Some(3_000));
}

#[test]
fn microcompact_without_a_window_uses_the_fallback_budget() {
    let mw = microcompact_for_window(5, "[cleared]", None);
    assert_eq!(
        mw.token_budget(),
        Some(legacy_max_input_tokens(UNKNOWN_WINDOW_FALLBACK_TOKENS).max(1))
    );
    let known = microcompact_for_window(5, "[cleared]", Some(200_000));
    assert_eq!(known.token_budget(), Some(legacy_max_input_tokens(200_000)));
    assert_eq!(known.keep_recent(), 5);
}

#[test]
fn install_registers_steps_in_firing_order() {
    let ladder = ContextLadder {
        compression: None,
        microcompact: Some(microcompact_for_window(3, "[cleared]", Some(100_000))),
        wrap_up: None,
        artifact_toc: Some(ArtifactIndexTocMiddleware::new(0, "idx")),
        trim: Some(ImageAwareMessageTrimMiddleware::for_context_window(100_000)),
    };
    let mut harness: AgentHarness<(), ()> = AgentHarness::new();
    let (compression, wrap_up) = ladder.install(&mut harness);
    assert!(compression.is_none() && wrap_up.is_none());
    let names: Vec<&str> = harness
        .middleware()
        .middlewares
        .iter()
        .map(|m| m.name())
        .collect();
    assert_eq!(names.len(), 3);
    let pos = |needle: &str| names.iter().position(|n| n.contains(needle)).unwrap();
    assert!(pos("microcompact") < pos("artifact"));
    assert!(pos("artifact") < pos("trim"));
}

#[test]
fn empty_ladder_installs_nothing() {
    let mut harness: AgentHarness<(), ()> = AgentHarness::new();
    ContextLadder::default().install(&mut harness);
    assert!(harness.middleware().middlewares.is_empty());
}
