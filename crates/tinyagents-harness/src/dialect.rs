//! Mechanical bridge between harness dispatch policy and TinyTools dialects.

use crate::config::ToolDispatcher;
pub use tinytools_agent::dialect::ToolCallFormat;

/// Resolve the harness policy using the shared dialect selection rule.
///
/// An unknown provider capability retains the harness's native-call default.
/// Explicit dispatcher choices are unaffected by capability information.
#[must_use]
pub fn resolve_tool_format(
    dispatcher: ToolDispatcher,
    native_support: Option<bool>,
) -> ToolCallFormat {
    let choice = match dispatcher {
        ToolDispatcher::Auto => "auto",
        ToolDispatcher::Native => "native",
        ToolDispatcher::Xml => "xml",
        ToolDispatcher::Pformat => "pformat",
        ToolDispatcher::Python => "python",
        ToolDispatcher::Typescript => "typescript",
    };
    ToolCallFormat::resolve(choice, native_support.unwrap_or(true))
}

/// Convert a selected prompt format into harness policy.
///
/// Native format uses automatic dispatch so a provider reporting unsupported
/// native calls can still fall back. Other formats explicitly select their
/// matching text grammar.
#[must_use]
pub fn dispatcher_for_format(format: ToolCallFormat) -> ToolDispatcher {
    match format {
        ToolCallFormat::Native => ToolDispatcher::Auto,
        ToolCallFormat::Json => ToolDispatcher::Xml,
        ToolCallFormat::PFormat => ToolDispatcher::Pformat,
        ToolCallFormat::Python => ToolDispatcher::Python,
        ToolCallFormat::TypeScript => ToolDispatcher::Typescript,
    }
}

#[cfg(test)]
#[path = "dialect_tests.rs"]
mod tests;
