//! Tests for the memory-pack, packed-tool-route and budget-parity hooks.

use super::*;
use crate::context::{RunConfig, RunContext};
use crate::middleware::Middleware;
use serde_json::json;
use tinyinference_llm::model::ModelRequest;
use tinyinference_llm::tool::ToolCall;
use crate::middleware::library::BudgetTracker;
use tinyinference_llm::message::Message;

struct Pack(Option<&'static str>);

impl MemoryPackSource<()> for Pack {
    fn injection(&self, _: &()) -> Option<String> {
        self.0.map(str::to_string)
    }
}

#[tokio::test]
async fn memory_pack_is_added_to_the_request_when_present() {
    let mw = MemoryPackMiddleware::<(), _>::new(Pack(Some("recalled facts")));
    let mut ctx = RunContext::new(RunConfig::new("r"), ());
    let mut request = ModelRequest {
        messages: vec![Message::user("hi")],
        ..Default::default()
    };
    mw.before_model(&mut ctx, &(), &mut request).await.unwrap();
    assert!(request.messages.len() > 1 || format!("{:?}", request.messages).contains("recalled"));
}

#[tokio::test]
async fn memory_pack_is_a_no_op_without_a_pack() {
    let mw = MemoryPackMiddleware::<(), _>::new(Pack(None));
    let mut ctx = RunContext::new(RunConfig::new("r"), ());
    let mut request = ModelRequest {
        messages: vec![Message::user("hi")],
        ..Default::default()
    };
    mw.before_model(&mut ctx, &(), &mut request).await.unwrap();
    assert_eq!(request.messages.len(), 1);
}

struct Router(&'static str);

impl PackedToolRouter<()> for Router {
    fn pack_for(&self, _: &(), tool: &str) -> Option<String> {
        (tool == self.0).then(|| "pack-a".to_string())
    }
}

fn route_mw(registered: &[&str]) -> PackedToolRouteMiddleware<(), Router> {
    PackedToolRouteMiddleware::new(
        registered.iter().map(|s| s.to_string()),
        "use_skill",
        Router("hidden_tool"),
    )
}

async fn run(mw: &PackedToolRouteMiddleware<(), Router>, call: &mut ToolCall) {
    let mut ctx = RunContext::new(RunConfig::new("r"), ());
    mw.before_tool(&mut ctx, &(), call).await.unwrap();
}

#[tokio::test]
async fn bare_packed_tool_is_routed_through_the_router_tool() {
    let mw = route_mw(&["use_skill"]);
    let mut call = ToolCall::new("c1", "hidden_tool", json!({"x": 1}));
    run(&mw, &mut call).await;
    assert_eq!(call.name, "use_skill");
    assert_eq!(
        call.arguments,
        json!({"skill": "pack-a", "tool": "hidden_tool", "args": {"x": 1}})
    );
}

#[tokio::test]
async fn null_arguments_become_an_empty_object() {
    let mw = route_mw(&["use_skill"]);
    let mut call = ToolCall::new("c1", "hidden_tool", serde_json::Value::Null);
    run(&mw, &mut call).await;
    assert_eq!(call.arguments["args"], json!({}));
}

#[tokio::test]
async fn calls_are_left_alone_when_routing_does_not_apply() {
    // Registered name, no router tool registered, declined name, non-object args.
    for (registered, name, args) in [
        (vec!["use_skill", "hidden_tool"], "hidden_tool", json!({})),
        (vec![], "hidden_tool", json!({})),
        (vec!["use_skill"], "unknown", json!({})),
        (vec!["use_skill"], "hidden_tool", json!([1])),
    ] {
        let mw = route_mw(&registered);
        let mut call = ToolCall::new("c", name, args.clone());
        run(&mw, &mut call).await;
        assert_eq!(call.name, name);
        assert_eq!(call.arguments, args);
    }
}

#[test]
fn parity_observer_matches_when_both_are_empty() {
    let observer = BudgetParityObserver::new(BudgetTracker::default());
    let run = crate::middleware::AgentRun::default();
    assert!(observer.matches(&run));
}
