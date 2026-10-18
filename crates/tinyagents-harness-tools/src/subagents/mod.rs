//! Generic subagent tool shells.

mod driver_tool;
mod jobs;

use tinyagents_orchestration::subagent::SubAgentJobRegistry;
pub use driver_tool::{DriverSubAgentTool, SubagentDriverToolHost};
#[cfg(test)]
#[path = "driver_tool_tests.rs"]
mod driver_tool_tests;
pub use jobs::{
    SubAgentJobAction, SubAgentJobsTool, SubAgentMessageTool, register_subagent_job_tools,
};

