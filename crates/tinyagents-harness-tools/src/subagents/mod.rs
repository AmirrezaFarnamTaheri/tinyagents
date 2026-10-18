//! Generic subagent tool shells.

mod driver_tool;
mod jobs;

pub use driver_tool::{DriverSubAgentTool, SubagentDriverToolHost};
pub use tinyagents_orchestration::subagent::SubAgentJobRegistry;
#[cfg(test)]
#[path = "driver_tool_tests.rs"]
mod driver_tool_tests;
pub use jobs::{
    SubAgentJobAction, SubAgentJobsTool, SubAgentMessageTool, register_subagent_job_tools,
};

#[cfg(test)]
#[path = "jobs_tests.rs"]
mod jobs_tests;

#[cfg(test)]
mod test_helpers;
