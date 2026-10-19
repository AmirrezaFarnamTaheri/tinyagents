use std::{collections::BTreeMap, ffi::OsString, time::Duration};

use tinytools::{ToolContent, ToolResult};

use super::ExecOutput;

pub(super) const MAX_OUTPUT_BYTES: usize = 1_048_576;
pub(super) const COMMAND_TIMEOUT_EXIT_CODE: i32 = 124;
pub(super) const COMMAND_TIMEOUT_NOTE: &str = "[exit 124: the command's own `timeout` limit expired; the exec tool's timeout_secs did not fire]";
const OUTPUT_TRUNCATED: &str = "\n... [output truncated at 1MB]";
const STDERR_TRUNCATED: &str = "\n... [stderr truncated at 1MB]";

pub(super) fn tool_timeout_message(secs: u64) -> String {
    format!(
        "Command timed out after {secs}s and was killed: the exec tool's timeout_secs limit fired."
    )
}

pub(super) const PYTHON_UTF8_DEFAULTS: &[(&str, &str)] =
    &[("PYTHONUTF8", "1"), ("PYTHONIOENCODING", "utf-8")];

pub(super) fn python_utf8_env(
    lookup: impl Fn(&str) -> Option<OsString>,
) -> Vec<(&'static str, OsString)> {
    PYTHON_UTF8_DEFAULTS
        .iter()
        .map(|(name, default)| (*name, lookup(name).unwrap_or_else(|| (*default).into())))
        .collect()
}

pub(super) fn command_failure(code: Option<i32>, stdout: &str, stderr: &str) -> ToolResult {
    let mut failure = tinytools::command_failure(code, stdout, stderr);
    if code == Some(COMMAND_TIMEOUT_EXIT_CODE) {
        tracing::debug!("[exec] exit 124: attributing it to the command's own timeout");
        failure.content.push(ToolContent::Text {
            text: COMMAND_TIMEOUT_NOTE.to_string(),
        });
    }
    failure
}

fn bounded_stream(bytes: &[u8], marker: &str) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.len() <= MAX_OUTPUT_BYTES {
        return text.into_owned();
    }
    let mut end = MAX_OUTPUT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{marker}", &text[..end])
}

pub(super) fn exec_result(output: ExecOutput, explicit_timeout: Option<Duration>) -> ToolResult {
    let stdout = bounded_stream(&output.stdout, OUTPUT_TRUNCATED);
    let stderr = bounded_stream(&output.stderr, STDERR_TRUNCATED);
    if output.timed_out {
        let secs = explicit_timeout.map_or(0, |timeout| timeout.as_secs());
        let mut message = tool_timeout_message(secs);
        if !stdout.is_empty() {
            message.push_str("\n[stdout]\n");
            message.push_str(&stdout);
        }
        if !stderr.is_empty() {
            message.push_str("\n[stderr]\n");
            message.push_str(&stderr);
        }
        return ToolResult::error(message);
    }
    if output.exit_code == Some(0) {
        if stderr.is_empty() {
            ToolResult::success(stdout)
        } else {
            ToolResult::success(format!("{stdout}\n[stderr]\n{stderr}"))
        }
    } else {
        command_failure(output.exit_code, &stdout, &stderr)
    }
}

pub(super) fn python_environment(
    mut environment: BTreeMap<OsString, OsString>,
) -> BTreeMap<OsString, OsString> {
    for (name, value) in
        python_utf8_env(|name| environment.get(std::ffi::OsStr::new(name)).cloned())
    {
        environment.insert(name.into(), value);
    }
    environment
}

#[cfg(test)]
#[path = "shell_support_tests.rs"]
mod tests;
