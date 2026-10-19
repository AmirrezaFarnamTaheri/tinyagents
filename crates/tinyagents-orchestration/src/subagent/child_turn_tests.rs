use super::*;
use tinyagents_harness::cost::{CallCost, CostSource};
use tinyinference_llm::usage::{ChargedAmount, Usage};

#[derive(Default)]
struct FakeHost {
    events: Vec<&'static str>,
    run: Option<Result<ChildTurnRun<String>, ChildTurnFailure<String, &'static str>>>,
    summary: Option<ChildTurnCapSummary>,
    persisted: Option<ChildTurnResult<String>>,
    failure_history: Option<Vec<String>>,
    fail_persist: bool,
}

#[async_trait::async_trait]
impl ChildTurnHost for FakeHost {
    type Message = String;
    type Error = &'static str;

    async fn run_turn(
        &mut self,
    ) -> Result<ChildTurnRun<String>, ChildTurnFailure<String, &'static str>> {
        self.events.push("run");
        self.run.take().unwrap()
    }

    async fn summarize_cap(
        &mut self,
        _: &ChildTurnRun<String>,
    ) -> Result<ChildTurnCapSummary, &'static str> {
        self.events.push("summary");
        Ok(self.summary.take().unwrap())
    }

    fn append_final_message(
        &mut self,
        history: &mut Vec<String>,
        text: &str,
    ) -> Result<(), &'static str> {
        self.events.push("append-final");
        history.push(format!("native:{text}"));
        Ok(())
    }

    async fn persist_success(
        &mut self,
        result: &ChildTurnResult<String>,
    ) -> Result<(), &'static str> {
        self.events.push("persist-success");
        if self.fail_persist {
            return Err("persist failed");
        }
        self.persisted = Some(result.clone());
        Ok(())
    }

    async fn persist_failure(
        &mut self,
        history: &[String],
        _: &ChildTurnAccounting,
    ) -> Result<(), &'static str> {
        self.events.push("persist-failure");
        self.failure_history = Some(history.to_vec());
        Ok(())
    }

    async fn after_commit(
        &mut self,
        _: &ChildTurnResult<String>,
    ) -> Result<(), &'static str> {
        self.events.push("after-commit");
        Ok(())
    }
}

fn totals(input: u64, output: u64, cache_read: u64, cache_create: u64, reasoning: u64) -> UsageTotals {
    let mut totals = UsageTotals::default();
    totals.record(Usage {
        input_tokens: input,
        output_tokens: output,
        total_tokens: input + output,
        cache_read_tokens: cache_read,
        cache_creation_tokens: cache_create,
        reasoning_tokens: reasoning,
        charged_amount: Some(ChargedAmount::usd_micros(250)),
        context_window_tokens: Some(32_000),
    });
    totals
}

#[tokio::test]
async fn cap_summary_folds_complete_usage_and_runs_after_transcript_commit() {
    let accounting = ChildTurnAccounting {
        usage: totals(10, 4, 3, 2, 1),
        cost: {
            let mut cost = CostTally::default();
            cost.add(CallCost::Charged(1.25));
            cost
        },
        model_calls: 5,
    };
    let mut host = FakeHost {
        run: Some(Ok(ChildTurnRun {
            accepted_history: vec!["accepted-native-assistant-tool-call".into()],
            output: "partial".into(),
            model_calls: 5,
            accounting,
            hit_cap: true,
            early_exit: false,
        })),
        summary: Some(ChildTurnCapSummary {
            text: "resume from checkpoint".into(),
            usage: totals(7, 5, 2, 1, 3),
            cost: CallCost::Estimated(2.25),
            model_calls: 1,
        }),
        ..FakeHost::default()
    };

    let result = drive_child_turn(&mut host).await.unwrap();

    assert_eq!(
        host.events,
        ["run", "summary", "append-final", "persist-success", "after-commit"]
    );
    assert_eq!(
        result.accepted_history,
        [
            "accepted-native-assistant-tool-call",
            "native:resume from checkpoint"
        ]
    );
    assert_eq!(result.output, "resume from checkpoint");
    assert_eq!(result.accounting.model_calls, 6);
    assert_eq!(result.accounting.usage.calls, 2);
    assert_eq!(result.accounting.usage.usage.input_tokens, 17);
    assert_eq!(result.accounting.usage.usage.output_tokens, 9);
    assert_eq!(result.accounting.usage.usage.total_tokens, 26);
    assert_eq!(result.accounting.usage.usage.cache_read_tokens, 5);
    assert_eq!(result.accounting.usage.usage.cache_creation_tokens, 3);
    assert_eq!(result.accounting.usage.usage.reasoning_tokens, 4);
    assert_eq!(
        result.accounting.usage.usage.charged_amount,
        Some(ChargedAmount::usd_micros(500))
    );
    assert_eq!(result.accounting.cost.known_usd, 3.5);
    assert_eq!(result.accounting.cost.source, CostSource::Estimated);
    assert_eq!(host.persisted, Some(result));
}

#[tokio::test]
async fn failed_turn_persists_only_accepted_history_before_returning_error() {
    let mut host = FakeHost {
        run: Some(Err(ChildTurnFailure {
            error: "provider failed",
            accepted_history: vec!["accepted-user".into(), "accepted-assistant".into()],
            accounting: ChildTurnAccounting {
                model_calls: 1,
                usage: totals(3, 2, 0, 0, 0),
                ..ChildTurnAccounting::default()
            },
        })),
        ..FakeHost::default()
    };

    assert_eq!(drive_child_turn(&mut host).await, Err("provider failed"));
    assert_eq!(host.events, ["run", "persist-failure"]);
    assert_eq!(
        host.failure_history,
        Some(vec!["accepted-user".into(), "accepted-assistant".into()])
    );
    assert!(host.persisted.is_none());
}

#[tokio::test]
async fn failed_transcript_commit_never_runs_deferred_terminal_effects() {
    let mut host = FakeHost {
        run: Some(Ok(ChildTurnRun {
            accepted_history: vec!["accepted-xml-suffix".into()],
            output: "done".into(),
            model_calls: 2,
            accounting: ChildTurnAccounting::default(),
            hit_cap: false,
            early_exit: false,
        })),
        fail_persist: true,
        ..FakeHost::default()
    };

    assert_eq!(drive_child_turn(&mut host).await, Err("persist failed"));
    assert_eq!(host.events, ["run", "persist-success"]);
    assert!(host.persisted.is_none());
}
