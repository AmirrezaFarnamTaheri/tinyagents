//! Generic infrastructure for binding indirect tool targets per call.

use std::any::Any;
use std::sync::Arc;

use serde_json::Value;
use tinytools::Tool;

/// A call-scoped target captured by a tool implementation.
pub trait BoundToolCall: Send + Sync {
    /// The executor to run for the admitted call.
    fn tool(&self) -> Arc<dyn Tool>;

    /// The selected policy target and effective arguments, when available.
    fn admission_target(&self) -> Option<(&dyn Tool, &Value)>;

    /// Exposes the implementation payload to the implementation's own dispatcher.
    fn as_any(&self) -> &dyn Any;
}

/// Generic binding contract exposed by a tool through its erased host extension.
///
/// Harness infrastructure invokes this once after argument preparation and uses
/// the returned target for both admission and execution. It does not know the
/// implementation-specific routing model.
pub struct ToolCallBinding {
    bind: Arc<dyn Fn(Arc<dyn Tool>, &Value) -> Arc<dyn BoundToolCall> + Send + Sync>,
}

impl ToolCallBinding {
    /// Creates a binder for a tool implementation.
    pub fn new(
        bind: impl Fn(Arc<dyn Tool>, &Value) -> Arc<dyn BoundToolCall> + Send + Sync + 'static,
    ) -> Self {
        Self { bind: Arc::new(bind) }
    }

    /// Captures the indirect target selected for these arguments.
    pub fn bind(&self, outer: Arc<dyn Tool>, arguments: &Value) -> Arc<dyn BoundToolCall> {
        (self.bind)(outer, arguments)
    }
}
