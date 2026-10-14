use super::*;

struct Scoped {
    name: &'static str,
    scope: ToolScope,
}

#[async_trait]
impl Tool for Scoped {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        "scoped"
    }
    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({ "type": "object", "properties": {} })
    }
    fn scope(&self) -> ToolScope {
        self.scope
    }
    async fn execute(&self, _arguments: serde_json::Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success("ok"))
    }
}

fn guard() -> CliRpcOnlyMiddleware {
    let set: Vec<Box<dyn Tool>> = vec![
        Box::new(Scoped { name: "phone_call", scope: ToolScope::CliRpcOnly }),
        Box::new(Scoped { name: "file_read", scope: ToolScope::All }),
    ];
    CliRpcOnlyMiddleware::new(vec![Arc::new(set)])
}

#[test]
fn only_cli_rpc_scoped_registered_tools_are_refused() {
    let guard = guard();
    assert!(guard.is_cli_rpc_only("phone_call"));
    assert!(!guard.is_cli_rpc_only("file_read"));
    assert!(!guard.is_cli_rpc_only("unregistered"));
}
