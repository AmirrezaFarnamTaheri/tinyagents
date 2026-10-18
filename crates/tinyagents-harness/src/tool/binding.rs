//! Generic infrastructure for tools that bind indirect targets per call.

use std::any::Any;
use std::sync::Arc;

use serde_json::Value;
use tinytools::Tool;

/// A call-scoped binding supplied by a tool implementation through its host extension.
///
/// Harness infrastructure invokes this once after argument preparation and uses the
/// returned target for both admission and execution. The harness does not know the
/// implementation-specific routing model.
pub struct ToolCallBinding {
    bind: Arc<dyn Fn(Arc<dyn Tool>, &Value) -> BoundToolCall + Send + Sync>,
}

impl ToolCallBinding {
    /// Creates a generic binder for a tool implementation.
    pub fn new(
        bind: impl Fn(Arc<dyn Tool>, &Value) -> BoundToolCall + Send + Sync + 'static,
    ) -> Self {
        Self { bind: Arc::new(bind) }
    }

    /// Captures the indirect target selected for these arguments.
    pub fn bind(&self, outer: Arc<dyn Tool>, arguments: &Value) -> BoundToolCall {
        (self.bind)(outer, arguments)
    }
}

/// The immutable tool target captured for one admitted call.
pub struct BoundToolCall {
    tool: Arc<dyn Tool>,
    admission_tool: Option<Arc<dyn Tool>>,
    admission_arguments: Value,
    payload: Option<Arc<dyn Any + Send + Sync>>,
}

impl BoundToolCall {
    /// Creates a binding with its execution tool, optional policy target, and implementation payload.
    pub fn new<T: Any + Send + Sync>(
        tool: Arc<dyn Tool>,
        admission_target: Option<(Arc<dyn Tool>, Value)>,
        payload: T,
    ) -> Self {
        let (admission_tool, admission_arguments) = admission_target
            .map(|(tool, arguments)| (Some(tool), arguments))
            .unwrap_or((None, Value::Null));
        Self {
            tool,
            admission_tool,
            admission_arguments,
            payload: Some(Arc::new(payload)),
        }
    }

    /// The executor to run for the admitted call.
    pub fn tool(&self) -> Arc<dyn Tool> {
        Arc::clone(&self.tool)
    }

    /// The selected policy target and effective arguments, when the binder has one.
    pub fn admission_target(&self) -> Option<(&dyn Tool, &Value)> {
        Some((self.admission_tool.as_deref()?, &self.admission_arguments))
    }

    /// Reads implementation-specific metadata captured with this binding.
    pub fn payload<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.payload.as_ref()?.downcast_ref()
    }
}
