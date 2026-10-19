use super::*;

use crate::state_block::types::Failure;

fn item(content: &str, status: TodoStatus) -> TodoItem {
    TodoItem::with_status(content, status)
}

#[test]
fn nothing_to_show_renders_nothing() {
    assert_eq!(render(&[], &RunLedger::default()), None);
}

#[test]
fn the_plan_marks_each_step_and_counts_rounds_on_the_active_one() {
    let mut cancelled = item("Benchmark", TodoStatus::Cancelled);
    cancelled.reason = Some("not asked for".to_string());
    let items = vec![
        item("Dump the schema", TodoStatus::Completed),
        item("Write the migration", TodoStatus::InProgress),
        item("Run the tests", TodoStatus::Pending),
        cancelled,
    ];
    let ledger = RunLedger {
        round: 7,
        step_since: 3,
        ..RunLedger::default()
    };
    let block = render(&items, &ledger).unwrap();
    assert!(block.starts_with(STATE_BLOCK_HEADER), "{block}");
    assert!(
        block.contains("Plan: step 2 of 4 in progress (4 tool rounds on it).\n  [x] 1. Dump the schema\n  [>] 2. Write the migration\n  [ ] 3. Run the tests\n  [-] 4. Benchmark (cancelled: not asked for)"),
        "{block}"
    );
}

#[test]
fn the_plan_header_covers_no_active_step_and_a_closed_plan() {
    let ledger = RunLedger::default();
    let open = [
        item("A", TodoStatus::Completed),
        item("B", TodoStatus::Pending),
    ];
    assert!(
        render(&open, &ledger)
            .unwrap()
            .contains("Plan: no step in progress; step 2 of 2 is next.")
    );
    let closed = [item("A", TodoStatus::Completed)];
    assert!(
        render(&closed, &ledger)
            .unwrap()
            .contains("Plan: all 1 steps closed.")
    );
}

#[test]
fn a_long_plan_is_cut_with_a_count() {
    let items: Vec<TodoItem> = (0..25)
        .map(|n| item(&format!("step {n}"), TodoStatus::Pending))
        .collect();
    let block = render(&items, &RunLedger::default()).unwrap();
    assert!(block.contains("  [ ] 20. step 19\n  … 5 more"), "{block}");
    assert!(!block.contains("step 20"), "{block}");
}

#[test]
fn facts_render_without_a_plan() {
    let mut ledger = RunLedger {
        round: 4,
        ..RunLedger::default()
    };
    ledger.failures.push_back(Failure {
        round: 4,
        tool: "shell".to_string(),
        summary: "python3 x.py".to_string(),
        error: "exit code 127".to_string(),
    });
    ledger.failures.push_back(Failure {
        round: 2,
        tool: "shell".to_string(),
        summary: "make".to_string(),
        error: "exit code 2".to_string(),
    });
    ledger.missing.push(("python3".to_string(), 4));
    ledger.files.push("/app/x.py".to_string());
    let block = render(&[], &ledger).unwrap();
    assert!(!block.contains("Plan:"), "{block}");
    assert!(
        block.contains(
            "Recent failures (oldest first):\n  - last round, shell `python3 x.py`: exit code 127\n  - 3 rounds ago, shell `make`: exit code 2\nReported not found: python3\nFiles written this run: /app/x.py"
        ),
        "{block}"
    );
}
