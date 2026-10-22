use super::*;
use crate::context::RunConfig;

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| (*s).to_owned()).collect()
}

#[tokio::test]
async fn narrowed_set_agrees_with_selection() {
    let allowed: HashSet<String> = names(&["a", "b"]).into_iter().collect();
    let mw = ToolExposureShadowMiddleware::new(&names(&["a", "b", "c"]), Some(&allowed), vec![]);
    let mut ctx = RunContext::new(RunConfig::new("t"), ());
    let divergence = mw.evaluate(&mut ctx, None).await.unwrap();
    assert!(divergence.is_parity(), "{divergence:?}");
}

#[tokio::test]
async fn empty_allowed_set_denies_all_and_still_agrees() {
    let allowed = HashSet::new();
    let mw = ToolExposureShadowMiddleware::new(&names(&["a", "b"]), Some(&allowed), vec![]);
    let mut ctx = RunContext::new(RunConfig::new("t"), ());
    assert!(mw.evaluate(&mut ctx, None).await.unwrap().is_parity());
}

#[tokio::test]
async fn live_request_tools_are_never_touched() {
    let mw = ToolExposureShadowMiddleware::new(&names(&["a", "b"]), None, vec!["x".into()]);
    let mut ctx = RunContext::new(RunConfig::new("t"), ());
    let mut request = ModelRequest {
        tools: vec![ToolSchema::new("a", "", serde_json::json!({}))],
        ..Default::default()
    };
    Middleware::<(), ()>::before_model(&mw, &mut ctx, &(), &mut request)
        .await
        .unwrap();
    assert_eq!(request.tools.len(), 1);
    assert!(mw.ran.load(Ordering::SeqCst));
}
