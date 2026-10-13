use super::*;
use std::sync::Mutex;
use tinyagents_harness::CancellationToken;

#[derive(Clone, Debug)]
struct P;

struct Policy;
impl AdmissionPolicy<P> for Policy {
    fn admit(&self, _i: usize, _t: &Task<P>) -> AdmissionVerdict {
        AdmissionVerdict::ReadOnly
    }
}

fn workers(ids: &[&str]) -> Vec<Worker<P>> {
    Plan::new(
        ids.iter()
            .map(|id| Task {
                id: (*id).into(),
                prompt: "p".into(),
                ownership: None,
                payload: P,
            })
            .collect(),
    )
    .stage(&Policy)
    .into_iter()
    .map(|p| match p {
        Preflight::Prepared(w) => w,
        Preflight::Rejected(_) => panic!("rejected"),
    })
    .collect()
}

#[derive(Default)]
struct Rec {
    deny: Option<&'static str>,
    started: Mutex<Vec<usize>>,
    finished: Mutex<Vec<String>>,
}
impl RunHooks<P, String> for Rec {
    fn approve(&self, _i: usize, w: &Worker<P>) -> Result<(), String> {
        match self.deny {
            Some(id) if id == w.id => Err("no".into()),
            _ => Ok(()),
        }
    }
    fn on_start(&self, i: usize, _w: &Worker<P>) {
        self.started.lock().unwrap().push(i);
    }
    fn on_finish(&self, _i: usize, r: &String) {
        self.finished.lock().unwrap().push(r.clone());
    }
}

#[test]
fn concurrency_is_clamped_to_batch_size() {
    assert_eq!(resolve_concurrency(0, 5), 5);
    assert_eq!(resolve_concurrency(8, 3), 3);
    assert_eq!(resolve_concurrency(2, 5), 2);
    assert_eq!(resolve_concurrency(0, 0), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn denied_worker_is_skipped_but_keeps_its_slot_and_progress_fires() {
    let hooks = Rec {
        deny: Some("b"),
        ..Default::default()
    };
    let out = run_with_hooks(
        workers(&["a", "b", "c"]),
        2,
        CancellationToken::new(),
        &hooks,
        |_| (),
        |w, _| async move { format!("ran:{}", w.id) },
        |w, reason| format!("denied:{}:{reason}", w.id),
    )
    .await
    .unwrap();
    assert_eq!(out, vec!["ran:a", "denied:b:no", "ran:c"]);
    let mut started = hooks.started.lock().unwrap().clone();
    started.sort();
    assert_eq!(started, vec![0, 2]);
    assert_eq!(*hooks.finished.lock().unwrap(), out);
}
