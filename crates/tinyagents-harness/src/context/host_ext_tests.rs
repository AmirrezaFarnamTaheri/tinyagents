use std::sync::Arc;

use super::*;
use crate::context::{RunConfig, RunContext};
use crate::ids::CallId;
use crate::tool::ToolExecutionContext;

#[derive(Debug, PartialEq)]
struct Origin(&'static str);

#[derive(Debug, PartialEq)]
struct Sink(u32);

#[test]
fn get_returns_none_until_a_value_of_that_type_is_installed() {
    let ext = HostExtensions::new();
    assert!(ext.is_empty());
    assert!(ext.get::<Origin>().is_none());
    let ext = ext.with(Origin("web"));
    assert_eq!(ext.get::<Origin>(), Some(&Origin("web")));
    assert!(ext.get::<Sink>().is_none());
}

#[test]
fn installing_leaves_the_receiver_untouched_and_replaces_same_type() {
    let base = HostExtensions::new().with(Origin("a"));
    let next = base.with(Origin("b")).with(Sink(1));
    assert_eq!(base.get::<Origin>(), Some(&Origin("a")));
    assert_eq!(next.get::<Origin>(), Some(&Origin("b")));
    assert_eq!(next.len(), 2);
    assert!(next.without::<Sink>().get::<Sink>().is_none());
}

#[test]
fn get_arc_shares_the_installed_value() {
    let shared = Arc::new(Sink(7));
    let ext = HostExtensions::new().with_arc(Arc::clone(&shared));
    let got = ext.get_arc::<Sink>().unwrap();
    assert!(Arc::ptr_eq(&shared, &got));
}

#[test]
fn run_context_host_is_inherited_by_forks_and_overridable_per_child() {
    let parent: RunContext = RunContext::new(RunConfig::new("p"), ())
        .with_host(Origin("web"))
        .with_host(Sink(1));
    assert_eq!(parent.host::<Origin>(), Some(&Origin("web")));

    let inherited = parent.child(RunConfig::new("c1"), ()).unwrap();
    assert_eq!(inherited.host::<Origin>(), Some(&Origin("web")));
    assert_eq!(inherited.host::<Sink>(), Some(&Sink(1)));

    let retyped = parent.child_with_data(RunConfig::new("c2"), 5u8).unwrap();
    assert_eq!(retyped.host::<Origin>(), Some(&Origin("web")));

    let overridden = inherited.with_host(Origin("cron"));
    assert_eq!(overridden.host::<Origin>(), Some(&Origin("cron")));
    // The parent and the earlier fork's source are unaffected.
    assert_eq!(parent.host::<Origin>(), Some(&Origin("web")));
}

#[test]
fn tools_read_the_run_hosts_through_the_execution_context() {
    let ctx: RunContext = RunContext::new(RunConfig::new("r"), ()).with_host(Origin("tool"));
    let tool_ctx = ToolExecutionContext::from_run_context(&ctx, CallId::new("c"));
    assert_eq!(tool_ctx.host::<Origin>(), Some(&Origin("tool")));
    assert!(tool_ctx.host::<Sink>().is_none());
}
