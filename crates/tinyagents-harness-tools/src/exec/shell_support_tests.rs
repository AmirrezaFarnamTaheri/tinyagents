use super::*;
use crate::exec::ExecOutput;
use std::ffi::OsString;

fn output(stdout: Vec<u8>, stderr: Vec<u8>, exit_code: Option<i32>, timed_out: bool) -> ExecOutput {
    ExecOutput { stdout, stderr, exit_code, timed_out }
}

#[test]
fn explicit_timeout_preserves_captured_streams() {
    let result = exec_result(
        output(b"partial stdout".to_vec(), b"partial stderr".to_vec(), None, true),
        Some(Duration::from_secs(17)),
    );
    assert!(result.is_error);
    let text = result.text();
    assert!(text.contains("Command timed out after 17s"));
    assert!(text.contains("[stdout]\npartial stdout"));
    assert!(text.contains("[stderr]\npartial stderr"));
}

#[test]
fn successful_stderr_is_preserved() {
    let result = exec_result(output(b"out".to_vec(), b"warning".to_vec(), Some(0), false), None);
    assert!(!result.is_error);
    assert_eq!(result.text(), "out\n[stderr]\nwarning");
}

#[test]
fn command_failure_preserves_exit_and_both_streams() {
    let result = exec_result(output(b"out".to_vec(), b"err".to_vec(), Some(7), false), None);
    assert!(result.is_error);
    let text = result.text();
    assert!(text.contains("exit code 7"));
    assert!(text.contains("out"));
    assert!(text.contains("err"));
}

#[test]
fn exit_124_identifies_the_command_timeout() {
    let result = exec_result(output(b"done".to_vec(), Vec::new(), Some(124), false), None);
    assert!(result.is_error);
    assert!(result.text().contains(COMMAND_TIMEOUT_NOTE));
}

#[test]
fn each_stream_is_truncated_at_a_utf8_boundary() {
    let mut stdout = vec![b'a'; MAX_OUTPUT_BYTES - 1];
    stdout.extend_from_slice("é".as_bytes());
    let mut stderr = stdout.clone();
    stderr.extend_from_slice(b"extra");
    let result = exec_result(output(stdout, stderr, Some(0), false), None);
    assert!(!result.is_error);
    let text = result.text();
    assert!(text.contains(OUTPUT_TRUNCATED));
    assert!(text.contains(STDERR_TRUNCATED));
    assert!(!text.contains('�'));
}

#[test]
fn python_utf8_defaults_keep_explicit_values() {
    let env = python_environment(BTreeMap::from([
        (OsString::from("PYTHONUTF8"), OsString::from("0")),
        (OsString::from("KEEP"), OsString::from("value")),
    ]));
    assert_eq!(env.get(std::ffi::OsStr::new("PYTHONUTF8")), Some(&OsString::from("0")));
    assert_eq!(env.get(std::ffi::OsStr::new("PYTHONIOENCODING")), Some(&OsString::from("utf-8")));
    assert_eq!(env.get(std::ffi::OsStr::new("KEEP")), Some(&OsString::from("value")));
}

#[test]
fn python_utf8_lookup_fills_only_missing_defaults() {
    let env = python_utf8_env(|name| (name == "PYTHONUTF8").then(|| OsString::from("0")));
    assert_eq!(env, vec![
        ("PYTHONUTF8", OsString::from("0")),
        ("PYTHONIOENCODING", OsString::from("utf-8")),
    ]);
}
