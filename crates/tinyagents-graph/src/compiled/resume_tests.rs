//! Resume honours completion markers: a pending task that the checkpoint's
//! write ledger says already ran is skipped, unless every pending task is
//! marked, in which case all of them re-run rather than strand the thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;

use crate::builder::{GraphBuilder, NodeContext};
use crate::checkpoint::{CheckpointConfig, Checkpointer, InMemoryCheckpointer, PendingWrite};
use crate::command::{Command, Interrupt, NodeResult};
use crate::reducer::ClosureStateReducer;

/// `p` and `q` both pause on their first run; on resume each adds its value.
/// Returns the graph, its checkpointer and the per-node run counters.
fn two_pausing_nodes() -> (
    crate::CompiledGraph<i32, i32>,
    Arc<InMemoryCheckpointer<i32>>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let cp = Arc::new(InMemoryCheckpointer::<i32>::new());
    let p_runs = Arc::new(AtomicUsize::new(0));
    let q_runs = Arc::new(AtomicUsize::new(0));
    let (p, q) = (p_runs.clone(), q_runs.clone());
    let graph = GraphBuilder::<i32, i32>::new()
        .with_parallel(true)
        .set_reducer(ClosureStateReducer::new(|s: i32, u: i32| Ok(s + u)))
        .add_node("split", |_s: i32, _c: NodeContext| async move {
            Ok(NodeResult::Command(
                Command::default().with_goto(["p", "q"]),
            ))
        })
        .add_node("p", move |_s: i32, c: NodeContext| {
            let runs = p.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                match c.resume {
                    Some(_) => Ok(NodeResult::Update(1)),
                    None => Ok(NodeResult::Interrupt(Interrupt::new("p", json!({})))),
                }
            }
        })
        .add_node("q", move |_s: i32, c: NodeContext| {
            let runs = q.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                match c.resume {
                    Some(_) => Ok(NodeResult::Update(10)),
                    None => Ok(NodeResult::Interrupt(Interrupt::new("q", json!({})))),
                }
            }
        })
        .set_entry("split")
        .mark_command_routing("split")
        .set_finish("p")
        .set_finish("q")
        .compile()
        .unwrap()
        .with_checkpointer(cp.clone());
    (graph, cp, p_runs, q_runs)
}

/// Pauses on thread `t` and records a completion marker for each of `marked`.
async fn pause_and_mark(
    graph: &crate::CompiledGraph<i32, i32>,
    cp: &InMemoryCheckpointer<i32>,
    marked: &[&str],
) {
    let paused = graph.run_with_thread("t", 0).await.unwrap();
    assert!(paused.is_interrupted());
    let checkpoint = cp.get("t", None).await.unwrap().unwrap();
    assert_eq!(checkpoint.tasks.len(), 2, "both p and q are pending");
    let writes: Vec<PendingWrite> = checkpoint
        .tasks
        .iter()
        .filter(|task| marked.contains(&task.node.as_str()))
        .map(|task| PendingWrite::completion_marker(task.node.clone(), task.task_id.clone()))
        .collect();
    let config = CheckpointConfig {
        thread_id: "t".into(),
        checkpoint_id: Some(checkpoint.checkpoint_id.clone()),
        namespace: Vec::new(),
    };
    cp.put_writes(&config, &writes).await.unwrap();
}

#[tokio::test]
async fn resume_skips_a_pending_task_with_a_completion_marker() {
    let (graph, cp, p_runs, q_runs) = two_pausing_nodes();
    pause_and_mark(&graph, &cp, &["p"]).await;

    let done = graph
        .resume("t", Command::resume(json!(null)))
        .await
        .unwrap();
    assert_eq!(
        p_runs.load(Ordering::SeqCst),
        1,
        "p is marked done: not re-run"
    );
    assert_eq!(q_runs.load(Ordering::SeqCst), 2, "q pauses, then resumes");
    assert_eq!(done.state, 10);
}

#[tokio::test]
async fn resume_reruns_everything_when_every_pending_task_is_marked() {
    let (graph, cp, p_runs, q_runs) = two_pausing_nodes();
    pause_and_mark(&graph, &cp, &["p", "q"]).await;

    let done = graph
        .resume("t", Command::resume(json!(null)))
        .await
        .unwrap();
    assert_eq!(p_runs.load(Ordering::SeqCst), 2);
    assert_eq!(q_runs.load(Ordering::SeqCst), 2);
    assert_eq!(done.state, 11);
}
