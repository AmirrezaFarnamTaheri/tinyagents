//! The live driver projects its configured depth instead of an ambient parent.
use super::*;
use crate::context::{RunDepth, current_run_depth, with_run_depth};
use crate::middleware::Middleware;
use async_trait::async_trait;
use tinyinference_llm::providers::MockModel;

struct RecordDepth(Arc<std::sync::Mutex<Vec<RunDepth>>>);

#[async_trait]
impl Middleware<(), ()> for RecordDepth {
    fn name(&self) -> &str {
        "record-depth"
    }

    async fn before_model(
        &self,
        ctx: &mut RunContext<()>,
        _state: &(),
        _request: &mut ModelRequest,
    ) -> Result<()> {
        let depth = current_run_depth().expect("driver has scoped its depth");
        assert_eq!(depth, RunDepth::from(&ctx.config));
        self.0.lock().unwrap().push(depth);
        Ok(())
    }
}

/// Unary and streaming entry points override and restore ambient depth.
#[tokio::test]
async fn driver_scopes_the_real_configured_depth_for_unary_and_streaming_runs() {
    let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.with_model_resolver(Arc::new(crate::host::FixedModelResolver::named(
        "mock",
        Arc::new(MockModel::constant("done")),
    )));
    harness.push_middleware(Arc::new(RecordDepth(recorded.clone())));
    let incoming = RunDepth {
        depth: 1,
        max_depth: 8,
    };
    with_run_depth(incoming, async {
        for streaming in [false, true] {
            let config = RunConfig::new("native-child")
                .with_depth(3)
                .with_max_depth(3);
            let ctx = RunContext::new(config, ());
            let input = vec![Message::user("go")];
            if streaming {
                harness
                    .invoke_streaming_in_context(&(), ctx, input)
                    .await
                    .unwrap();
            } else {
                harness.invoke_in_context(&(), ctx, input).await.unwrap();
            }
            assert_eq!(current_run_depth(), Some(incoming));
        }
    })
    .await;
    assert_eq!(
        *recorded.lock().unwrap(),
        vec![
            RunDepth {
                depth: 3,
                max_depth: 3
            };
            2
        ]
    );
    assert_eq!(current_run_depth(), None);
}

struct RecordErrorDepth(Arc<std::sync::Mutex<Vec<Option<RunDepth>>>>);

#[async_trait]
impl Middleware<(), ()> for RecordErrorDepth {
    fn name(&self) -> &str {
        "record-error-depth"
    }

    async fn on_error(&self, _ctx: &mut RunContext<()>, _error: &TinyAgentsError) -> Result<()> {
        self.0.lock().unwrap().push(current_run_depth());
        Ok(())
    }
}

/// A driver failure keeps its own depth visible while middleware finalizes it.
#[tokio::test]
async fn error_middleware_keeps_the_failed_runs_configured_depth() {
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.push_middleware(Arc::new(RecordErrorDepth(observed.clone())));
    let configured = RunDepth {
        depth: 3,
        max_depth: 3,
    };
    let parent = RunDepth {
        depth: 1,
        max_depth: 8,
    };
    with_run_depth(parent, async {
        let config = RunConfig::new("missing-model")
            .with_depth(3)
            .with_max_depth(3);
        assert!(
            harness
                .invoke_in_context(&(), RunContext::new(config, ()), vec![Message::user("go")])
                .await
                .is_err()
        );
        assert_eq!(current_run_depth(), Some(parent));
    })
    .await;
    assert_eq!(*observed.lock().unwrap(), vec![Some(configured)]);
    assert_eq!(current_run_depth(), None);
}
