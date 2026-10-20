//! Print the authoritative Rust JSON Schema for persisted session UI records.

use schemars::JsonSchema;
use tinyagents_session::{
    run_ledger::types::{AgentRun, RunEvent, RunTelemetry},
    turn_state::types::TurnState,
};

#[derive(JsonSchema)]
#[schemars(rename_all = "camelCase")]
#[allow(dead_code)]
struct SessionUiWireTypes {
    turn_state: TurnState,
    agent_run: AgentRun,
    run_event: RunEvent,
    run_telemetry: RunTelemetry,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        serde_json::to_string_pretty(&schemars::schema_for!(SessionUiWireTypes))?
    );
    Ok(())
}
