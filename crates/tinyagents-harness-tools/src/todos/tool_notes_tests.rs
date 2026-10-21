use super::*;

use tinyagents_harness::store::InMemoryStore;
use tinytools::ToolContent;

use crate::scratchpad::store as scratchpad_store;

struct ThreadContext;

impl ToolRunContext for ThreadContext {
    fn thread_id(&self) -> Option<&str> {
        Some("t")
    }
}

async fn run(tool: &TodoTool, args: Value) -> ToolResult {
    tool.execute_with_context(args, Default::default(), Some(&ThreadContext))
        .await
        .unwrap()
}

fn payload(result: &ToolResult) -> &Value {
    result
        .content
        .iter()
        .find_map(|block| match block {
            ToolContent::Json { data } => Some(data),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected a JSON payload: {}", result.output()))
}

fn tool() -> (TodoTool, Arc<dyn Store>) {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::default());
    (TodoTool::new(store.clone()), store)
}

#[tokio::test]
async fn notes_alone_are_saved_and_read_the_list() {
    let (tool, store) = tool();
    let result = run(
        &tool,
        json!({"facts": "the table is auth_users", "dead_ends": ["pip: no network"], "approach": "dump the schema"}),
    )
    .await;
    assert!(!result.is_error, "{}", result.output());
    let payload = payload(&result);
    assert_eq!(payload["todos"], json!([]), "a read of the (empty) list");
    assert_eq!(
        payload["scratchpad"],
        json!({"facts": ["the table is auth_users"], "deadEnds": ["pip: no network"], "approach": "dump the schema"})
    );
    let pad = scratchpad_store::get(&store, "t").await.unwrap().unwrap();
    assert_eq!(pad.facts, ["the table is auth_users"]);
}

#[tokio::test]
async fn notes_ride_along_with_every_plan_mode() {
    let (tool, store) = tool();
    let written = run(
        &tool,
        json!({"todos": [{"content": "a"}, {"content": "b"}], "approach": "a then b"}),
    )
    .await;
    assert!(!written.is_error, "{}", written.output());
    assert_eq!(payload(&written)["todos"].as_array().unwrap().len(), 2);

    let completed = run(&tool, json!({"complete": 1, "facts": ["a works"]})).await;
    assert!(!completed.is_error, "{}", completed.output());
    assert_eq!(
        payload(&completed)["scratchpad"]["facts"],
        json!(["a works"])
    );

    let cancelled = run(
        &tool,
        json!({"cancel": 2, "reason": "covered by a", "dead_ends": ["b is redundant"]}),
    )
    .await;
    assert!(!cancelled.is_error, "{}", cancelled.output());
    let pad = scratchpad_store::get(&store, "t").await.unwrap().unwrap();
    assert_eq!(pad.dead_ends, ["b is redundant"]);
    assert_eq!(pad.approach.as_deref(), Some("a then b"));
}

#[tokio::test]
async fn a_plain_plan_call_carries_no_scratchpad() {
    let (tool, _store) = tool();
    let result = run(&tool, json!({"todos": [{"content": "a"}]})).await;
    assert!(payload(&result).get("scratchpad").is_none());
}

#[tokio::test]
async fn a_refused_plan_call_still_saves_its_notes_and_says_so() {
    let (tool, store) = tool();
    let result = run(&tool, json!({"complete": 3, "facts": ["learned anyway"]})).await;
    assert!(result.is_error);
    assert!(
        result.output().contains("your notes were saved"),
        "{}",
        result.output()
    );
    let pad = scratchpad_store::get(&store, "t").await.unwrap().unwrap();
    assert_eq!(pad.facts, ["learned anyway"]);
}

#[tokio::test]
async fn malformed_notes_are_refused_before_anything_is_written() {
    let (tool, store) = tool();
    for args in [
        json!({"facts": 3}),
        json!({"dead_ends": [1, 2]}),
        json!({"approach": ["x"], "todos": [{"content": "a"}]}),
    ] {
        let result = run(&tool, args.clone()).await;
        assert!(result.is_error, "{args} should be refused");
    }
    assert_eq!(scratchpad_store::get(&store, "t").await.unwrap(), None);
    assert_eq!(
        crate::todos::store::get(&store, "t").await.unwrap(),
        None,
        "the plan was not written either"
    );
}

#[tokio::test]
async fn unknown_arguments_next_to_notes_are_still_refused() {
    let (tool, _store) = tool();
    let result = run(&tool, json!({"complete": 1, "facts": ["x"], "priority": 2})).await;
    assert!(result.is_error);
    assert!(result.output().contains("priority"), "{}", result.output());
}

#[test]
fn the_schema_and_description_advertise_the_notes() {
    let schema = parameters_schema();
    for name in NOTE_ARGS {
        assert!(
            schema["properties"].get(name).is_some(),
            "{name} in the schema"
        );
        assert!(
            TODO_DESCRIPTION.contains(&format!("`{name}`")),
            "{name} described"
        );
    }
}
