use super::*;

#[test]
fn explicit_dispatchers_keep_their_grammar_and_auto_retains_the_unknown_default() {
    for (dispatcher, format) in [
        (ToolDispatcher::Native, ToolCallFormat::Native),
        (ToolDispatcher::Xml, ToolCallFormat::Json),
        (ToolDispatcher::Pformat, ToolCallFormat::PFormat),
        (ToolDispatcher::Python, ToolCallFormat::Python),
        (ToolDispatcher::Typescript, ToolCallFormat::TypeScript),
    ] {
        for support in [None, Some(false), Some(true)] {
            assert_eq!(resolve_tool_format(dispatcher, support), format);
        }
    }
    assert_eq!(
        resolve_tool_format(ToolDispatcher::Auto, None),
        ToolCallFormat::Native
    );
    assert_eq!(
        resolve_tool_format(ToolDispatcher::Auto, Some(true)),
        ToolCallFormat::Native
    );
    assert_eq!(
        resolve_tool_format(ToolDispatcher::Auto, Some(false)),
        ToolCallFormat::Json
    );
}

#[test]
fn prompt_formats_use_matching_dispatchers_with_native_fallback() {
    for (format, dispatcher) in [
        (ToolCallFormat::Native, ToolDispatcher::Auto),
        (ToolCallFormat::Json, ToolDispatcher::Xml),
        (ToolCallFormat::PFormat, ToolDispatcher::Pformat),
        (ToolCallFormat::Python, ToolDispatcher::Python),
        (ToolCallFormat::TypeScript, ToolDispatcher::Typescript),
    ] {
        assert_eq!(dispatcher_for_format(format), dispatcher);
    }
}
