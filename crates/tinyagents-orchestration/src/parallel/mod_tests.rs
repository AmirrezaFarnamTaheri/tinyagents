use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tinyagents_harness::CancellationToken;

#[derive(Clone, Debug)]
struct P {
    kind: &'static str,
    paths: Vec<&'static str>,
}

struct Policy;
impl AdmissionPolicy<P> for Policy {
    fn admit(&self, _i: usize, t: &Task<P>) -> AdmissionVerdict {
        match t.payload.kind {
            "read" => AdmissionVerdict::ReadOnly,
            "iso" => AdmissionVerdict::Isolated,
            "write" => AdmissionVerdict::Writes(t.payload.paths.iter().map(Into::into).collect()),
            _ => AdmissionVerdict::Reject("nope".into()),
        }
    }
}

fn task(id: &str, kind: &'static str, paths: Vec<&'static str>) -> Task<P> {
    Task {
        id: id.into(),
        prompt: format!("do {id}"),
        ownership: None,
        payload: P { kind, paths },
    }
}

fn prepared(p: Vec<Preflight<P>>) -> Vec<Worker<P>> {
    p.into_iter()
        .map(|x| match x {
            Preflight::Prepared(w) => w,
            Preflight::Rejected(r) => panic!("rejected {:?}", r.kind),
        })
        .collect()
}

#[test]
fn policy_rejection_keeps_input_position() {
    let out = Plan::new(vec![
        task("a", "read", vec![]),
        task("b", "bad", vec![]),
        task("c", "iso", vec![]),
    ])
    .stage(&Policy);
    assert!(matches!(out[0], Preflight::Prepared(_)));
    assert!(
        matches!(&out[1], Preflight::Rejected(r) if r.kind == RejectionKind::Policy("nope".into()))
    );
    assert!(matches!(out[2], Preflight::Prepared(_)));
}

#[test]
fn disjoint_writers_serialize_and_overlapping_writers_conflict() {
    let out = Plan::new(vec![
        task("a", "write", vec!["x.rs"]),
        task("b", "write", vec!["y.rs"]),
    ])
    .stage(&Policy);
    assert!(out.iter().all(|p| matches!(p, Preflight::Prepared(w) if w.dispatch_mode == WorkerDispatchMode::SerialSharedWorkspaceWrite)));

    let out = Plan::new(vec![
        task("a", "write", vec!["x.rs"]),
        task("b", "write", vec!["x.rs"]),
    ])
    .stage(&Policy);
    assert!(matches!(out[0], Preflight::Prepared(_)));
    assert!(
        matches!(&out[1], Preflight::Rejected(r) if matches!(r.kind, RejectionKind::Conflict(_)))
    );
}

#[test]
fn ownership_boundary_is_folded_into_prompt() {
    let mut t = task("a", "read", vec![]);
    t.ownership = Some("files: a.rs".into());
    let w = prepared(Plan::new(vec![t]).stage(&Policy));
    assert!(w[0].prompt.starts_with("[Ownership Boundary]\nfiles: a.rs"));
    assert_eq!(with_ownership_boundary("x", Some("  ")), "x");
}

#[tokio::test(flavor = "multi_thread")]
async fn results_are_in_input_order_with_interleaved_completion() {
    let ws = prepared(
        Plan::new(vec![
            task("a", "read", vec![]),
            task("b", "read", vec![]),
            task("c", "read", vec![]),
        ])
        .stage(&Policy),
    );
    // Earlier workers finish later.
    let out = run(
        ws,
        0,
        CancellationToken::new(),
        |i| i,
        |w, i| async move {
            tokio::time::sleep(Duration::from_millis(60 - 25 * i as u64)).await;
            (w.id, i)
        },
    )
    .await
    .unwrap();
    assert_eq!(
        out,
        vec![
            ("a".to_string(), 0),
            ("b".to_string(), 1),
            ("c".to_string(), 2)
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn each_branch_gets_its_own_context() {
    let ws = prepared(
        Plan::new(vec![task("a", "iso", vec![]), task("b", "iso", vec![])]).stage(&Policy),
    );
    let made = Arc::new(AtomicUsize::new(0));
    let m = made.clone();
    let out = run(
        ws,
        2,
        CancellationToken::new(),
        move |_| Arc::new(AtomicUsize::new(m.fetch_add(1, Ordering::SeqCst))),
        |_w, ctx| async move { ctx.fetch_add(100, Ordering::SeqCst) },
    )
    .await
    .unwrap();
    assert_eq!(made.load(Ordering::SeqCst), 2);
    assert_eq!(out, vec![0, 1]);
}

#[tokio::test]
async fn serial_path_runs_one_at_a_time_and_honours_cancel() {
    let ws = prepared(
        Plan::new(vec![
            task("a", "write", vec!["x"]),
            task("b", "write", vec!["y"]),
        ])
        .stage(&Policy),
    );
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (a, p) = (active.clone(), peak.clone());
    run(
        ws.clone(),
        4,
        CancellationToken::new(),
        |_| (),
        move |_w, _| {
            let (a, p) = (a.clone(), p.clone());
            async move {
                let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                p.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                a.fetch_sub(1, Ordering::SeqCst);
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(peak.load(Ordering::SeqCst), 1);

    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    let ran = Arc::new(AtomicUsize::new(0));
    let r = ran.clone();
    let err = run(
        ws,
        4,
        cancel,
        |_| (),
        move |_w, _| {
            let (c, r) = (c2.clone(), r.clone());
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                c.cancel();
            }
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        tinyagents_harness::TinyAgentsError::Cancelled
    ));
    assert_eq!(ran.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn async_preflight_prepares_each_task_before_admission_staging() {
    let out = Plan::new(vec![task("a", "read", vec![]), task("b", "read", vec![])])
        .stage_with_preflight(&Policy, |index, mut task| async move {
            tokio::task::yield_now().await;
            if index == 1 {
                return Err("workspace isolation failed".to_owned());
            }
            task.payload.kind = "iso";
            Ok(task)
        })
        .await;

    assert!(
        matches!(out[0], Preflight::Prepared(ref w) if w.dispatch_mode == WorkerDispatchMode::Parallel)
    );
    assert!(
        matches!(&out[1], Preflight::Rejected(r) if r.kind == RejectionKind::Policy("workspace isolation failed".into()))
    );
}

struct R {
    id: &'static str,
    ok: bool,
    files: Vec<String>,
}
impl BranchReport for R {
    fn branch_id(&self) -> &str {
        self.id
    }
    fn succeeded(&self) -> bool {
        self.ok
    }
    fn changed_files(&self) -> &[String] {
        &self.files
    }
}

#[test]
fn collected_counts_failures_and_reports_overlaps() {
    let c = Collected::new(vec![
        R {
            id: "a",
            ok: true,
            files: vec!["x.rs".into(), "y.rs".into()],
        },
        R {
            id: "b",
            ok: false,
            files: vec!["x.rs".into()],
        },
        R {
            id: "c",
            ok: true,
            files: vec![],
        },
    ]);
    assert_eq!((c.total(), c.succeeded(), c.failures), (3, 2, 1));
    assert_eq!(
        c.overlap_warnings,
        vec![OverlapWarning {
            file: "x.rs".into(),
            workers: vec!["a".into(), "b".into()]
        }]
    );
}

#[test]
fn duplicate_files_from_one_branch_do_not_create_a_cross_branch_overlap() {
    let warnings = overlap_warnings(&[R {
        id: "same-worker",
        ok: true,
        files: vec!["x.rs".into(), "x.rs".into()],
    }]);

    assert!(warnings.is_empty(), "one branch cannot overlap with itself");
}
