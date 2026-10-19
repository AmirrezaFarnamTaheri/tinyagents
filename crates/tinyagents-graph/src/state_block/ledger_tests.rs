use super::*;

/// OpenHuman's shell tool, verbatim from a Terminal-Bench capture.
const SHELL_127: &str = "Command failed (exit code 127 \u{2014} command not found: a required \
executable or dependency is missing or not on PATH. Install/declare it, use an available \
alternative, or report the blocker \u{2014} do NOT re-run the same command)\n[stdout]\nexit=127\n\
0\n[stderr]\n/usr/bin/bash: line 2: exec: python3: not found\n";

fn call(name: &str, arguments: Value) -> ToolCall {
    ToolCall::new("c1", name, arguments)
}

fn shell(command: &str) -> CallRecord {
    call_record(&call("shell", serde_json::json!({ "command": command })))
}

#[test]
fn a_record_keeps_the_command_and_only_a_write_tools_path() {
    let record = shell("python3   migrate.py\n--dry-run");
    assert_eq!(record.summary, "python3 migrate.py --dry-run");
    assert_eq!(
        record.command.as_deref(),
        Some("python3   migrate.py\n--dry-run")
    );
    assert_eq!(record.written_path, None);

    let write = call_record(&call(
        "file_write",
        serde_json::json!({ "path": "/app/a.py", "content": "x" }),
    ));
    assert_eq!(write.written_path.as_deref(), Some("/app/a.py"));
    assert_eq!(write.command, None);
    assert!(write.summary.contains("/app/a.py"), "{}", write.summary);

    let read = call_record(&call(
        "file_read",
        serde_json::json!({ "path": "/app/a.py" }),
    ));
    assert_eq!(read.written_path, None, "a read does not write");
}

#[test]
fn the_error_line_is_the_exit_code_and_first_stderr_line() {
    assert_eq!(
        error_line(SHELL_127),
        "exit code 127; stderr: /usr/bin/bash: line 2: exec: python3: not found"
    );
    assert_eq!(
        error_line("\n  Tool `x` refused: path outside the workspace\nmore"),
        "Tool `x` refused: path outside the workspace"
    );
    assert!(error_line(&"e".repeat(500)).chars().count() <= 160);
}

#[test]
fn missing_executables_follow_the_shells_wording() {
    assert_eq!(missing_executables(SHELL_127), vec!["python3"]);
    assert_eq!(
        missing_executables("bash: jq: command not found\nzsh: command not found: rg"),
        vec!["jq", "rg"]
    );
    assert_eq!(
        missing_executables("sh: 1: /usr/bin/node: not found"),
        vec!["node"]
    );
    // Prose is not a name.
    assert!(missing_executables("Error: not found").is_empty());
    assert!(missing_executables("command not found: a required executable").is_empty());
    assert!(missing_executables("line 3: 404: not found").is_empty());
}

#[test]
fn a_success_of_the_same_command_clears_its_failure() {
    let mut ledger = RunLedger::default();
    record_result(&mut ledger, shell("make test"), true, "exit code 2");
    assert_eq!(ledger.failures.len(), 1);
    record_result(&mut ledger, shell("make"), false, "ok");
    assert_eq!(ledger.failures.len(), 1, "a different command leaves it");
    record_result(&mut ledger, shell("make test"), false, "ok");
    assert!(ledger.failures.is_empty());
}

#[test]
fn a_missing_executable_clears_once_a_command_runs_it() {
    let mut ledger = RunLedger::default();
    record_result(&mut ledger, shell("python3 x.py"), true, SHELL_127);
    assert_eq!(ledger.missing, vec![("python3".to_string(), 0)]);
    record_result(
        &mut ledger,
        shell("apt-get install -y python3"),
        false,
        "done",
    );
    assert!(ledger.missing.is_empty());
}

#[test]
fn failures_and_files_are_bounded_and_files_stay_distinct() {
    let mut ledger = RunLedger::default();
    for n in 0..5 {
        record_result(&mut ledger, shell(&format!("cmd{n}")), true, "exit code 1");
    }
    let kept: Vec<&str> = ledger.failures.iter().map(|f| f.summary.as_str()).collect();
    assert_eq!(kept, vec!["cmd2", "cmd3", "cmd4"]);

    for n in 0..8 {
        let write = call_record(&call(
            "edit_file",
            serde_json::json!({ "file_path": format!("/f{n}") }),
        ));
        record_result(&mut ledger, write, false, "ok");
    }
    let again = call_record(&call(
        "edit_file",
        serde_json::json!({ "file_path": "/f3" }),
    ));
    record_result(&mut ledger, again, false, "ok");
    assert_eq!(ledger.files, vec!["/f2", "/f4", "/f5", "/f6", "/f7", "/f3"]);
}

#[test]
fn old_failures_age_out() {
    let mut ledger = RunLedger::default();
    record_result(&mut ledger, shell("a"), true, "exit code 1");
    ledger.round = FAILURE_HORIZON_ROUNDS;
    age_out(&mut ledger);
    assert_eq!(ledger.failures.len(), 1);
    ledger.round += 1;
    age_out(&mut ledger);
    assert!(ledger.failures.is_empty());
}
