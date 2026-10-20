use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use serde_json::{Value, json};
use tinytools::{Tool, ToolPolicy, ToolResult};

struct BindingTool {
    binding: ToolCallBinding,
}

#[async_trait]
impl Tool for BindingTool {
    fn name(&self) -> &str {
        "outer"
    }
    fn description(&self) -> &str {
        "outer declaration"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success("outer"))
    }
    fn host_extension(&self) -> Option<&(dyn std::any::Any + Send + Sync)> {
        Some(&self.binding)
    }
}

struct Target;

#[async_trait]
impl Tool for Target {
    fn name(&self) -> &str {
        "selected"
    }
    fn description(&self) -> &str {
        "selected target"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success("selected"))
    }
}

struct BoundProbe {
    outer: Arc<dyn Tool>,
    target: Arc<dyn Tool>,
    args: Value,
}

impl BoundToolCall for BoundProbe {
    fn tool(&self) -> Arc<dyn Tool> {
        self.outer.clone()
    }
    fn admission_target(&self) -> Option<(&dyn Tool, &Value)> {
        Some((self.target.as_ref(), &self.args))
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[test]
fn canonical_adapter_forwards_tool_extension_to_the_call_binding_contract() {
    let bindings = Arc::new(AtomicUsize::new(0));
    let count = bindings.clone();
    let binding = ToolCallBinding::new(move |outer, args| {
        count.fetch_add(1, Ordering::SeqCst);
        Arc::new(BoundProbe {
            outer,
            target: Arc::new(Target),
            args: args.clone(),
        })
    });
    let outer: Arc<dyn Tool> = Arc::new(BindingTool { binding });
    let requiring_approval = Arc::new(|policy: ToolPolicy| policy.requiring_approval());
    let wrapped: Arc<dyn Tool> = Arc::new(
        toolset::OverrideTool::new(outer.clone()).with_policy_transform(requiring_approval),
    );
    let mut registry = ToolRegistry::<(), ()>::new();
    registry.register(wrapped);

    let dispatch = registry.model_dispatch("outer").expect("registered tool");
    let args = json!({"target": "selected", "value": 7});
    let bound = dispatch
        .bind_call(&args)
        .unwrap()
        .expect("binding captured");

    assert_eq!(bindings.load(Ordering::SeqCst), 1);
    assert_eq!(bound.tool().name(), "outer");
    assert!(bound.tool().policy().access.approval_required);
    let (target, effective_args) = bound.admission_target().unwrap();
    assert_eq!(target.name(), "selected");
    assert_eq!(effective_args, &args);
}

#[test]
fn binding_exposes_typed_registration_metadata_without_changing_call_binding() {
    let binding = ToolCallBinding::new(|outer, args| {
        Arc::new(BoundProbe {
            outer: outer.clone(),
            target: Arc::new(Target),
            args: args.clone(),
        })
    })
    .with_metadata(String::from("registry metadata"));

    assert_eq!(binding.metadata::<String>().map(String::as_str), Some("registry metadata"));
    assert!(binding.metadata::<usize>().is_none());

    let outer: Arc<dyn Tool> = Arc::new(Target);
    let arguments = json!({"value": 7});
    let bound = binding.bind(outer.clone(), &arguments);
    let (target, effective_args) = bound.admission_target().unwrap();
    assert_eq!(target.name(), "selected");
    assert_eq!(effective_args, &arguments);
}
