use super::*;

use tinyagents_harness::context::RunConfig;
use tinyagents_harness::store::InMemoryStore;
use tinyinference_llm::message::Message;

use crate::state_block::STATE_BLOCK_HEADER;

const THREAD: &str = "t";

fn ctx() -> RunContext<()> {
    RunContext::new(RunConfig::new("state-block").with_thread(THREAD), ())
}

fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![Message::system("persona"), Message::user("do the task")],
        ..Default::default()
    }
}

/// The block on a fresh request, if any.
async fn block(mw: &StateBlockMiddleware, ctx: &mut RunContext<()>) -> Option<String> {
    let mut request = request();
    Middleware::<(), ()>::before_model_dispatch(mw, ctx, &(), &mut request)
        .await
        .unwrap();
    let base = self::request().messages.len();
    assert!(request.messages.len() <= base + 1, "at most one block");
    request.messages.get(base).map(Message::text)
}

/// One tool round: the model asks for `name` with `arguments`, and it returns
/// `output` (an error when `is_error`).
async fn round(
    mw: &StateBlockMiddleware,
    ctx: &mut RunContext<()>,
    name: &str,
    arguments: serde_json::Value,
    is_error: bool,
    output: &str,
) {
    let mut call = ToolCall::new("c", name, arguments);
    let mut response = ModelResponse::assistant(String::new());
    response.message.tool_calls = vec![call.clone()];
    Middleware::<(), ()>::after_model(mw, ctx, &(), &mut response)
        .await
        .unwrap();
    Middleware::<(), ()>::before_tool(mw, ctx, &(), &mut call)
        .await
        .unwrap();
    let mut result = if is_error {
        ToolResult::error(output)
    } else {
        ToolResult::success(output)
    };
    let invocation = ToolInvocationIdentity::new("c", name);
    Middleware::<(), ()>::after_tool(mw, ctx, &(), &invocation, &mut result)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_run_with_no_plan_and_no_facts_gets_no_block() {
    let mw = StateBlockMiddleware::new(Arc::new(InMemoryStore::default()));
    let mut ctx = ctx();
    assert_eq!(block(&mw, &mut ctx).await, None);
    round(
        &mw,
        &mut ctx,
        "shell",
        serde_json::json!({"command": "ls"}),
        false,
        "a b",
    )
    .await;
    assert_eq!(block(&mw, &mut ctx).await, None);
}

#[tokio::test]
async fn the_block_follows_the_plan_and_the_tool_results() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::default());
    todo_store::replace(
        &store,
        THREAD,
        vec![
            TodoItem::new("Write the migration"),
            TodoItem::new("Run the tests"),
        ],
    )
    .await
    .unwrap();
    let mw = StateBlockMiddleware::new(store.clone());
    let mut ctx = ctx();

    let first = block(&mw, &mut ctx).await.expect("a plan shows");
    assert!(first.starts_with(STATE_BLOCK_HEADER), "{first}");
    assert!(
        first.contains("step 1 of 2 in progress (0 tool rounds on it)"),
        "{first}"
    );

    round(
        &mw,
        &mut ctx,
        "shell",
        serde_json::json!({"command": "python3 migrate.py"}),
        true,
        "Command failed (exit code 127)\n[stderr]\nbash: python3: command not found",
    )
    .await;
    let failed = block(&mw, &mut ctx).await.unwrap();
    assert!(failed.contains("(1 tool round on it)"), "{failed}");
    assert!(
        failed.contains("  - last round, shell `python3 migrate.py`: exit code 127; stderr: bash: python3: command not found"),
        "{failed}"
    );
    assert!(failed.contains("Reported not found: python3"), "{failed}");

    // `todo` calls are not ledger facts, but a tick moves the plan on.
    round(
        &mw,
        &mut ctx,
        "todo",
        serde_json::json!({"complete": 1}),
        false,
        "{}",
    )
    .await;
    todo_store::complete(&store, THREAD, 1).await.unwrap();
    // The loop renders before every model call; that is when the new step is seen.
    block(&mw, &mut ctx).await.unwrap();
    round(
        &mw,
        &mut ctx,
        "shell",
        serde_json::json!({"command": "python3 migrate.py"}),
        false,
        "migrated",
    )
    .await;
    let fixed = block(&mw, &mut ctx).await.unwrap();
    assert!(fixed.contains("[x] 1. Write the migration"), "{fixed}");
    assert!(
        fixed.contains("step 2 of 2 in progress (1 tool round on it)"),
        "{fixed}"
    );
    assert!(
        !fixed.contains("Recent failures"),
        "the success cleared it: {fixed}"
    );
    assert!(!fixed.contains("not found"), "python3 ran: {fixed}");
}

#[tokio::test]
async fn a_rewritten_request_gets_the_block_back_once() {
    let mw = StateBlockMiddleware::new(Arc::new(InMemoryStore::default()));
    let mut ctx = ctx();
    round(
        &mw,
        &mut ctx,
        "file_write",
        serde_json::json!({"path": "/a"}),
        false,
        "ok",
    )
    .await;
    let mut request = request();
    Middleware::<(), ()>::before_model_dispatch(&mw, &mut ctx, &(), &mut request)
        .await
        .unwrap();
    let with_block = request.messages.len();

    // Already there: left alone.
    Middleware::<(), ()>::after_model_rewrite(&mw, &mut ctx, &(), &mut request)
        .await
        .unwrap();
    assert_eq!(request.messages.len(), with_block);

    // A rewrite dropped it: added back.
    request.messages.pop();
    Middleware::<(), ()>::after_model_rewrite(&mw, &mut ctx, &(), &mut request)
        .await
        .unwrap();
    assert_eq!(request.messages.len(), with_block);
    assert!(
        request
            .messages
            .last()
            .unwrap()
            .text()
            .contains("Files written this run: /a")
    );
}

#[tokio::test]
async fn the_ledger_ends_with_the_run() {
    let mw = StateBlockMiddleware::new(Arc::new(InMemoryStore::default()));
    let mut ctx = ctx();
    round(
        &mw,
        &mut ctx,
        "shell",
        serde_json::json!({"command": "x"}),
        true,
        "exit code 1",
    )
    .await;
    assert!(block(&mw, &mut ctx).await.is_some());
    let mut run = AgentRun::default();
    Middleware::<(), ()>::after_agent(&mw, &mut ctx, &(), &mut run)
        .await
        .unwrap();
    assert_eq!(block(&mw, &mut ctx).await, None);
}
