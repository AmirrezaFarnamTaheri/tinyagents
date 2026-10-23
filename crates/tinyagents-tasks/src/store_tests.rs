use super::*;

#[test]
fn an_end_after_a_cancel_request_settles_cancelled() {
    let store = InMemoryTaskStore::new();
    type End = fn(&InMemoryTaskStore, &TaskId) -> Result<OrchestrationTaskRecord>;
    let ends: [(&str, End); 3] = [
        ("complete", |store, id| {
            store.complete(id, OrchestrationTaskResult::text("late"))
        }),
        ("fail", |store, id| store.fail(id, "late".into())),
        ("timeout", |store, id| store.timeout(id, "late".into())),
    ];
    for (name, end) in ends {
        let id = TaskId::new(name);
        store
            .insert(OrchestrationTaskSpec::new(
                name,
                OrchestrationTaskKind::Graph {
                    graph_id: "child".into(),
                },
            ))
            .unwrap();
        store.mark_running(&id).unwrap();
        store.request_cancel(&id).unwrap();
        let record = end(&store, &id).expect(name);
        assert_eq!(record.status, OrchestrationTaskStatus::Cancelled, "{name}");
        assert!(record.result.is_none(), "{name}: {:?}", record.result);
    }
}
