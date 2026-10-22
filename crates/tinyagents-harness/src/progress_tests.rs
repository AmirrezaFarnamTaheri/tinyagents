use crate::progress::AgentProgress;

#[test]
fn context_compacting_is_a_progress_event() {
    let event = AgentProgress::ContextCompacting;

    assert!(matches!(event, AgentProgress::ContextCompacting));
    assert_ne!(
        std::mem::discriminant(&AgentProgress::ContextCompacting),
        std::mem::discriminant(&AgentProgress::TurnStarted),
    );
}
