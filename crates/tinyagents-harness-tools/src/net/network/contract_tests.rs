//! Wire-contract fixtures: the name, description, permission level, exposure
//! and JSON Schema each network tool advertises to the model.
//!
//! These are what a model is prompted with, and what existing transcripts were
//! produced against, so they are pinned against literal fixture files rather
//! than re-derived from the code under test. A change here is a change to the
//! tool's public contract and must be made on purpose.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::{Value, json};
use tinytools::Tool;

use super::test_support::{AutonomyLevel, DEFAULT_LIMITS, TestHtml, TestNetGate};
use super::*;

fn tools() -> Vec<(&'static str, Box<dyn Tool>)> {
    let gate = TestNetGate::supervised();
    vec![
        (
            "curl",
            Box::new(CurlTool::new(
                gate.clone(),
                vec![],
                PathBuf::from("."),
                "downloads".into(),
                1024,
                30,
            )) as Box<dyn Tool>,
        ),
        (
            "http_request",
            Box::new(HttpRequestTool::new(
                gate.clone(),
                vec![],
                1024,
                30,
                DEFAULT_LIMITS,
            )),
        ),
        (
            "pushover",
            Box::new(PushoverTool::new(gate.clone(), PathBuf::from("."))),
        ),
        (
            "web_fetch",
            Box::new(WebFetchTool::new(
                gate,
                vec![],
                None,
                None,
                DEFAULT_LIMITS,
                Arc::new(TestHtml),
            )),
        ),
    ]
}

fn describe(tool: &dyn Tool) -> Value {
    json!({
        "name": tool.name(),
        "description": tool.description(),
        "permission_level": format!("{:?}", tool.permission_level()),
        "exposure": format!("{:?}", tool.exposure()),
        "schema": tool.parameters_schema(),
    })
}

fn fixture(name: &str) -> Value {
    let raw = match name {
        "curl" => include_str!("fixtures/curl.json"),
        "http_request" => include_str!("fixtures/http_request.json"),
        "pushover" => include_str!("fixtures/pushover.json"),
        "web_fetch" => include_str!("fixtures/web_fetch.json"),
        other => panic!("no fixture for {other}"),
    };
    serde_json::from_str(raw).expect("fixture is valid JSON")
}

#[test]
fn every_tool_advertises_exactly_its_pinned_contract() {
    for (fixture_name, tool) in tools() {
        assert_eq!(
            describe(tool.as_ref()),
            fixture(fixture_name),
            "contract drift in `{fixture_name}`"
        );
    }
}

#[test]
fn web_fetch_with_a_host_property_extends_only_the_properties() {
    let tool = WebFetchTool::new(
        TestNetGate::supervised(),
        vec![],
        None,
        None,
        DEFAULT_LIMITS,
        Arc::new(TestHtml),
    )
    .with_schema_property("summary_focus", json!({"type": "string"}));
    let mut expected = fixture("web_fetch");
    expected["schema"]["properties"]["summary_focus"] = json!({"type": "string"});
    assert_eq!(describe(&tool), expected);
}

#[test]
fn only_the_approval_gated_tools_route_through_approval() {
    let args = json!({"url": "https://example.com"});
    for (autonomy, prompts) in [
        (AutonomyLevel::Supervised, true),
        (AutonomyLevel::Full, false),
        (AutonomyLevel::ReadOnly, false),
    ] {
        let gate = TestNetGate::with(autonomy, 100);
        let curl = CurlTool::new(gate.clone(), vec![], ".".into(), "d".into(), 1, 1);
        let http = HttpRequestTool::new(gate.clone(), vec![], 1, 1, DEFAULT_LIMITS);
        let fetch = WebFetchTool::new(gate, vec![], None, None, DEFAULT_LIMITS, Arc::new(TestHtml));
        assert_eq!(curl.external_effect_with_args(&args), prompts);
        assert_eq!(http.external_effect_with_args(&args), prompts);
        assert!(!fetch.external_effect_with_args(&args));
    }
}
