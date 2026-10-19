//! Tests for the markdown renderers: literal JSON in, literal markdown out.

#![allow(clippy::unwrap_used)]

use super::render::{render_branch_markdown, render_log_markdown, render_status_markdown};
use serde_json::{Value, json};

fn status(value: &Value) -> String {
    render_status_markdown(value.as_object().unwrap())
}

#[test]
fn clean_status_stops_after_the_branch_line() {
    let md =
        status(&json!({"branch": "main", "clean": true, "staged": [{"path": "a", "status": "M"}]}));
    assert_eq!(md, "**branch**: `main`\n_Working tree clean._\n");
}

#[test]
fn dirty_status_lists_each_section_with_counts() {
    let md = status(&json!({
        "branch": "feat",
        "clean": false,
        "staged": [{"path": "a.rs", "status": "M"}, {"path": "b.rs", "status": "A"}],
        "unstaged": [{"path": "c.rs", "status": "D"}],
        "untracked": ["new.txt"],
    }));
    assert_eq!(
        md,
        "**branch**: `feat`\n\
         \n**staged** (2)\n- `M` a.rs\n- `A` b.rs\n\
         \n**unstaged** (1)\n- `D` c.rs\n\
         \n**untracked** (1)\n- new.txt\n"
    );
}

#[test]
fn status_skips_empty_sections_and_malformed_entries() {
    let md = status(&json!({
        "staged": [],
        "unstaged": [{"path": "only-path"}, {"status": "M"}],
        "untracked": [42],
    }));
    assert_eq!(md, "\n**unstaged** (2)\n\n**untracked** (1)\n");
}

#[test]
fn status_without_a_branch_or_clean_flag_renders_nothing() {
    assert_eq!(status(&json!({})), "");
}

#[test]
fn log_renders_short_hashes_and_defaults_missing_fields() {
    assert_eq!(render_log_markdown(&[]), "_No commits._");
    let md = render_log_markdown(&[
        json!({"hash": "0123456789abcdef", "author": "Ann", "date": "2026-01-02", "message": "fix"}),
        json!({"hash": "abc"}),
        json!({}),
    ]);
    assert_eq!(
        md,
        "# Commits (3)\n\
         - `01234567` fix _(by Ann, 2026-01-02)_\n\
         - `abc`  _(by , )_\n\
         - ``  _(by , )_\n"
    );
}

#[test]
fn branches_mark_the_current_one() {
    let md = render_branch_markdown(
        "main",
        &[
            json!({"name": "main", "current": true}),
            json!({"name": "dev"}),
            json!({}),
        ],
    );
    assert_eq!(
        md,
        "**current**: `main`\n\n## Branches\n- **main** ← current\n- dev\n- \n"
    );
}
