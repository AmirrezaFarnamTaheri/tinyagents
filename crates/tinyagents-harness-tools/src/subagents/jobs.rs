//! Model-facing shells for asynchronous subagent job control.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinyagents_harness::context::RunContext;
use tinyagents_harness::error::TinyAgentsError;
use tinyagents_harness::ids::RunId;
use tinyagents_harness::run_queue::QueueLane;
use tinyagents_harness::tool::{ToolDispatch, ToolRegistry};
use tinyagents_orchestration::subagent::{
    DetachedSubagentStatus, DurableSubagentSessionSummary, DurableSubagentStatus, SubAgentJobError,
    SubAgentJobRegistry, SubagentAwaitingUser, SubagentSessionStore, WaitError, WaitOutcome,
    list_subagent_records, record_agent_id, record_subagent_session_id, record_to_wait_outcome,
    subagent_record_for_task, task_id_for_session_in_records,
};
use tinyagents_tasks::{OrchestrationTaskFilter, TaskStore};
use tinytools::{PermissionLevel, Tool, ToolCategory, ToolResult, ToolScope, ToolTimeout};

type SessionOverlay = (
    Arc<dyn SubagentSessionStore>,
    Arc<dyn Fn(u64) -> String + Send + Sync>,
);
type TaskOverlay = (Arc<dyn TaskStore>, Arc<dyn Fn(u64) -> RunId + Send + Sync>);

const SELECTOR_RETRY_HINT: &str = "The selected subagent job or session is unavailable to this run. Check the identifier and try again.";

/// A fixed operation for a host-named alias of [`SubAgentJobsTool`].
///
/// The default tool continues to read its `action` argument. Hosts that keep
/// separate model-facing operations can bind one action while supplying the
/// existing tool name and schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubAgentJobAction {
    /// Read one job or list jobs when no identifier is supplied.
    Query,
    /// Cancel one owned live job.
    Cancel,
    /// Close one owned durable session.
    Close,
    /// Wait for one job, or for all jobs when no identifier is supplied.
    Await,
}

impl SubAgentJobAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Cancel => "cancel",
            Self::Close => "close",
            Self::Await => "await",
        }
    }
}

#[derive(Clone)]
struct JobArgumentNames {
    action: String,
    identifier: String,
    fallback_identifier: Option<String>,
}

impl JobArgumentNames {
    fn identifier<'a>(&self, object: &'a serde_json::Map<String, Value>) -> Option<&'a str> {
        object
            .get(&self.identifier)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                self.fallback_identifier.as_ref().and_then(|field| {
                    object
                        .get(field)
                        .and_then(Value::as_str)
                        .filter(|value| !value.is_empty())
                })
            })
    }
}

#[derive(Clone)]
struct AwaitTimeout {
    field: String,
    default_secs: u64,
    max_secs: u64,
}

impl AwaitTimeout {
    fn requested_secs(&self, object: &serde_json::Map<String, Value>) -> u64 {
        object
            .get(&self.field)
            .and_then(Value::as_u64)
            .unwrap_or(self.default_secs)
            .clamp(1, self.max_secs)
    }
}

impl Default for JobArgumentNames {
    fn default() -> Self {
        Self {
            action: "action".into(),
            identifier: "job_id".into(),
            fallback_identifier: None,
        }
    }
}

#[derive(Clone)]
struct MessageArgumentNames {
    job_id: String,
    session_id: String,
    message: String,
    request_id: String,
}

impl Default for MessageArgumentNames {
    fn default() -> Self {
        Self {
            job_id: "job_id".into(),
            session_id: "session_id".into(),
            message: "message".into(),
            request_id: "request_id".into(),
        }
    }
}

fn selector_retry(enabled: bool, fallback: &'static str) -> anyhow::Error {
    if enabled {
        TinyAgentsError::ModelRetry(SELECTOR_RETRY_HINT.to_owned()).into()
    } else {
        anyhow::anyhow!(fallback)
    }
}

fn selector_error(error: SubAgentJobError, recover: bool) -> anyhow::Error {
    if recover && is_recoverable_selector_denial(&error) {
        TinyAgentsError::ModelRetry(SELECTOR_RETRY_HINT.to_owned()).into()
    } else {
        error.into()
    }
}

pub(super) fn is_recoverable_selector_denial(error: &SubAgentJobError) -> bool {
    matches!(
        error,
        SubAgentJobError::NotFound(_)
            | SubAgentJobError::Terminal { .. }
            | SubAgentJobError::Cancelling(_)
    )
}

/// Typed host tool that queries a job or lists jobs owned by the requesting run.
#[derive(Clone)]
pub struct SubAgentJobsTool {
    jobs: SubAgentJobRegistry,
    sessions: Option<SessionOverlay>,
    tasks: Option<TaskOverlay>,
    tool_name: String,
    description: String,
    parameters: Option<Value>,
    fixed_action: Option<SubAgentJobAction>,
    argument_names: JobArgumentNames,
    await_timeout: Option<AwaitTimeout>,
    require_identifier: bool,
    owner_scope: OwnerScope,
    permission: PermissionLevel,
    scope: ToolScope,
    category: ToolCategory,
    timeout: ToolTimeout,
    task_thread_scope: Option<Arc<dyn Fn(u64) -> Option<String> + Send + Sync>>,
    recover_selector_errors: bool,
}

impl SubAgentJobsTool {
    /// Creates the query tool over `jobs`.
    pub fn new(jobs: SubAgentJobRegistry) -> Self {
        Self {
            jobs,
            sessions: None,
            tasks: None,
            tool_name: "subagent_jobs".into(),
            description: "Query an asynchronous subagent job by id, list all subagent jobs, or cancel one job with action \"cancel\".".into(),
            parameters: None,
            fixed_action: None,
            argument_names: JobArgumentNames::default(),
            await_timeout: None,
            require_identifier: false,
            owner_scope: Arc::new(|owner| owner.to_string()),
            permission: PermissionLevel::ReadOnly,
            scope: ToolScope::All,
            category: ToolCategory::System,
            timeout: ToolTimeout::Inherit,
            task_thread_scope: None,
            recover_selector_errors: false,
        }
    }

    /// Returns selector misses as model-correctable errors instead of fatal
    /// tool dispatch failures. Storage and registry failures remain fatal.
    pub fn recover_selector_errors(mut self) -> Self {
        self.recover_selector_errors = true;
        self
    }

    /// Uses a host-facing name while retaining this tool's typed job logic.
    pub fn with_tool_name(mut self, name: impl Into<String>) -> Self {
        self.tool_name = name.into();
        self
    }

    /// Uses a host-facing description for a named operation alias.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Uses a host-facing schema for a named operation alias.
    pub fn with_parameters(mut self, parameters: Value) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Binds this alias to one operation, so its schema need not expose the
    /// generic `action` field.
    pub fn with_fixed_action(mut self, action: SubAgentJobAction) -> Self {
        self.fixed_action = Some(action);
        self
    }

    /// Maps a host schema identifier field to the generic job/session id.
    pub fn with_identifier_field(mut self, identifier: impl Into<String>) -> Self {
        self.argument_names.identifier = identifier.into();
        self.argument_names.fallback_identifier = None;
        self
    }

    /// Uses a primary job id and falls back to a durable session id when the
    /// primary field is missing, empty, or not a string.
    pub fn with_identifier_fields(
        mut self,
        primary: impl Into<String>,
        fallback: impl Into<String>,
    ) -> Self {
        self.argument_names.identifier = primary.into();
        self.argument_names.fallback_identifier = Some(fallback.into());
        self
    }

    /// Bounds a named await alias using the supplied integer argument field.
    /// Missing or invalid values use `default_secs`; valid values clamp to
    /// `1..=max_secs`.
    pub fn with_await_timeout_field(
        mut self,
        field: impl Into<String>,
        default_secs: u64,
        max_secs: u64,
    ) -> Self {
        let max_secs = max_secs.max(1);
        self.await_timeout = Some(AwaitTimeout {
            field: field.into(),
            default_secs: default_secs.clamp(1, max_secs),
            max_secs,
        });
        self
    }

    /// Resolves a per-turn run owner to a stable host-owned authorization key.
    /// Use the same resolver for spawn, job, and message aliases when run
    /// contexts are recreated between turns.
    pub fn with_owner_scope(mut self, resolve: OwnerScope) -> Self {
        self.owner_scope = resolve;
        self
    }

    /// Requires a job/session identifier for this named alias.
    pub fn with_required_identifier(mut self) -> Self {
        self.require_identifier = true;
        self
    }

    /// Preserves host execution metadata when a product alias uses this
    /// generic dispatch implementation.
    pub fn with_metadata(
        mut self,
        permission: PermissionLevel,
        scope: ToolScope,
        category: ToolCategory,
        timeout: ToolTimeout,
    ) -> Self {
        self.permission = permission;
        self.scope = scope;
        self.category = category;
        self.timeout = timeout;
        self
    }

    /// Overlays durable sessions in list/query and enables durable close.
    /// `parent_key` maps the live run owner to the host's durable parent scope.
    pub fn with_sessions(
        mut self,
        sessions: Arc<dyn SubagentSessionStore>,
        parent_key: impl Fn(u64) -> String + Send + Sync + 'static,
    ) -> Self {
        self.sessions = Some((sessions, Arc::new(parent_key)));
        self
    }

    /// Overlays host-managed task records owned by the requesting parent run.
    pub fn with_task_store(
        mut self,
        tasks: Arc<dyn TaskStore>,
        parent_run_id: impl Fn(u64) -> RunId + Send + Sync + 'static,
    ) -> Self {
        self.tasks = Some((tasks, Arc::new(parent_run_id)));
        self
    }

    /// Restricts task overlays to the authenticated parent thread.
    ///
    /// This is applied to both task listings and durable wait recovery. A
    /// missing thread is an exact `None` scope, not permission to read tasks
    /// from every thread owned by the parent session.
    pub fn with_task_thread_scope(
        mut self,
        thread_id: impl Fn(u64) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        self.task_thread_scope = Some(Arc::new(thread_id));
        self
    }
}

fn durable_wait_snapshot(record: tinyagents_tasks::OrchestrationTaskRecord) -> Value {
    let task_id = record.spec.task_id.as_str().to_owned();
    let session_id = record_subagent_session_id(&record).map(str::to_owned);
    let agent_id = record_agent_id(&record);
    let (status, output, error, iterations) = match record_to_wait_outcome(record) {
        WaitOutcome::Terminal(DetachedSubagentStatus::Running) => {
            ("running", String::new(), None, 0)
        }
        WaitOutcome::Terminal(DetachedSubagentStatus::Completed { output, iterations }) => {
            ("completed", output, None, iterations)
        }
        WaitOutcome::Terminal(DetachedSubagentStatus::AwaitingUser { question }) => {
            ("awaiting_user", question, None, 0)
        }
        WaitOutcome::Terminal(DetachedSubagentStatus::Failed { error }) => {
            ("failed", String::new(), Some(error), 0)
        }
        WaitOutcome::TimedOut(DetachedSubagentStatus::Running) => {
            ("running", String::new(), None, 0)
        }
        WaitOutcome::TimedOut(DetachedSubagentStatus::Completed { output, iterations }) => {
            ("completed", output, None, iterations)
        }
        WaitOutcome::TimedOut(DetachedSubagentStatus::AwaitingUser { question }) => {
            ("awaiting_user", question, None, 0)
        }
        WaitOutcome::TimedOut(DetachedSubagentStatus::Failed { error }) => {
            ("failed", String::new(), Some(error), 0)
        }
    };
    json!({
        "task_id": task_id,
        "session_id": session_id,
        "agent": agent_id,
        "status": status,
        "output": output,
        "error": error,
        "iterations": iterations,
        "extensions": {
            "task_id": task_id,
            "agent_id": agent_id,
            "iterations": iterations,
        }
    })
}

#[async_trait]
impl Tool for SubAgentJobsTool {
    fn name(&self) -> &str {
        &self.tool_name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters_schema(&self) -> Value {
        self.parameters.clone().unwrap_or_else(|| {
            let mut properties = json!({
                self.argument_names.identifier.clone(): { "type": "string" },
                "action": {
                    "type": "string",
                    "enum": ["query", "cancel", "close", "await"],
                    "description": "`query` reads a job/session or lists work; `cancel` stops a live job; `close` prevents a durable session from being reused; `await` waits for the named live job."
                }
            });
            if let Some(fallback) = &self.argument_names.fallback_identifier {
                properties[fallback] = json!({ "type": "string" });
            }
            json!({"type":"object","properties":properties})
        })
    }

    fn permission_level(&self) -> PermissionLevel {
        self.permission
    }

    fn scope(&self) -> ToolScope {
        self.scope
    }

    fn category(&self) -> ToolCategory {
        self.category
    }

    fn timeout_policy(&self, args: &Value) -> ToolTimeout {
        if self.timeout != ToolTimeout::Inherit {
            return self.timeout;
        }
        let Some(timeout) = self.await_timeout.as_ref() else {
            return ToolTimeout::Inherit;
        };
        let empty = serde_json::Map::new();
        ToolTimeout::Millis(
            timeout
                .requested_secs(args.as_object().unwrap_or(&empty))
                .saturating_mul(1000),
        )
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        let _ = args;
        anyhow::bail!("subagent_jobs requires typed-parent dispatch")
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> ToolDispatch<State, Ctx> for SubAgentJobsTool {
    fn tool(&self) -> Arc<dyn Tool> {
        Arc::new(self.clone())
    }

    async fn execute(
        &self,
        _state: &State,
        _call_id: tinyagents_harness::ids::CallId,
        args: Value,
        _options: tinytools::ToolCallOptions,
        parent: &RunContext<Ctx>,
    ) -> anyhow::Result<ToolResult> {
        let object = args
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("arguments must be an object"))?;
        let owner = (self.owner_scope)(parent.instance_id());
        let identifier = self.argument_names.identifier(object);
        let action = match self.fixed_action {
            Some(action) => Some(Some(action.as_str())),
            None => object
                .get(&self.argument_names.action)
                .filter(|value| !value.is_null())
                .map(Value::as_str),
        };
        if self.require_identifier && identifier.is_none() {
            if self.recover_selector_errors {
                return Err(TinyAgentsError::ModelRetry(SELECTOR_RETRY_HINT.to_owned()).into());
            }
            anyhow::bail!("a job or session identifier is required");
        }
        match action {
            None | Some(Some("query")) => {}
            Some(Some("cancel")) => {
                let Some(job_id) = identifier else {
                    return Err(selector_retry(
                        self.recover_selector_errors,
                        "job_id must be a string for action `cancel`",
                    ));
                };
                let job = match self.jobs.cancel_owned(job_id, &owner) {
                    Ok(job) => job,
                    Err(error) => return Err(selector_error(error, self.recover_selector_errors)),
                };
                return Ok(ToolResult::json(serde_json::to_value(job)?));
            }
            Some(Some("close")) => {
                let Some(id) = identifier else {
                    return Err(selector_retry(
                        self.recover_selector_errors,
                        "job_id must be a string for action `close`",
                    ));
                };
                let (sessions, key) = self
                    .sessions
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("durable sessions are not configured"))?;
                let Some(session) = sessions.get(id).await? else {
                    return Err(selector_retry(
                        self.recover_selector_errors,
                        "durable subagent session not found",
                    ));
                };
                if session.parent_session != key(parent.instance_id()) {
                    return Err(selector_retry(
                        self.recover_selector_errors,
                        "durable subagent session not found",
                    ));
                }
                return Ok(ToolResult::json(
                    json!({"closed": sessions.close(id).await?}),
                ));
            }
            Some(Some("await")) => {
                let Some(id) = identifier else {
                    let mut tombstones = Vec::new();
                    for job in self.jobs.try_list_owned(&owner)? {
                        let job = self.jobs.wait_owned(job.id.as_str(), &owner).await?;
                        if job.status.is_terminal() {
                            tombstones.push(serde_json::to_value(job)?);
                        }
                    }
                    let awaiting_user = if let Some((sessions, key)) = &self.sessions {
                        sessions.list_for_parent(&key(parent.instance_id()), None).await?.into_iter()
                            .filter(|session| session.status == DurableSubagentStatus::AwaitingUser)
                        .map(|session| {
                            let question = session.latest_history.as_ref()
                                .and_then(|history| history.last())
                                .map(tinyinference_llm::message::Message::text)
                                .unwrap_or_default();
                            let envelope = SubagentAwaitingUser {
                                task_id: session.current_task_id.clone().unwrap_or_default(),
                                agent_id: session.agent_id.clone(),
                                worker_thread_id: session.worker_thread_id.clone(),
                                question,
                                checkpointed: true,
                            };
                            json!({"session_id":session.subagent_session_id,"task_id":session.current_task_id,"status":"awaiting_user","history":session.latest_history,"awaiting_user_envelope":envelope.render()})
                        })
                            .collect()
                    } else {
                        Vec::<Value>::new()
                    };
                    return Ok(ToolResult::json(
                        json!({"collected_tombstones":tombstones,"awaiting_user":awaiting_user}),
                    ));
                };
                let bridged_id = if self.jobs.try_get_owned(id, &owner)?.is_some() {
                    Some(id.to_owned())
                } else if let Some((sessions, key)) = &self.sessions {
                    let session = sessions
                        .get(id)
                        .await?
                        .filter(|session| session.parent_session == key(parent.instance_id()));
                    if let Some(session) = session {
                        if let Some(job_id) = session
                            .lifecycle_key
                            .as_ref()
                            .and_then(|key| self.jobs.driver_job_for_key(key, &owner))
                        {
                            Some(job_id.as_str().to_owned())
                        } else if let Some(task_id) = session.current_task_id {
                            self.jobs.try_get_owned(&task_id, &owner)?.map(|_| task_id)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(job_id) = bridged_id {
                    let job = if let Some(timeout) = &self.await_timeout {
                        let duration =
                            std::time::Duration::from_secs(timeout.requested_secs(object));
                        match tokio::time::timeout(duration, self.jobs.wait_owned(&job_id, &owner))
                            .await
                        {
                            Ok(Err(SubAgentJobError::NotFound(_)))
                                if self.recover_selector_errors =>
                            {
                                return Err(TinyAgentsError::ModelRetry(
                                    SELECTOR_RETRY_HINT.to_owned(),
                                )
                                .into());
                            }
                            Ok(result) => result?,
                            Err(_) => match self.jobs.try_get_owned(&job_id, &owner)? {
                                Some(job) => job,
                                None if self.recover_selector_errors => {
                                    return Err(TinyAgentsError::ModelRetry(
                                        SELECTOR_RETRY_HINT.to_owned(),
                                    )
                                    .into());
                                }
                                None => {
                                    return Err(SubAgentJobError::NotFound(job_id.clone()).into());
                                }
                            },
                        }
                    } else {
                        match self.jobs.wait_owned(&job_id, &owner).await {
                            Ok(job) => job,
                            Err(SubAgentJobError::NotFound(_)) if self.recover_selector_errors => {
                                return Err(TinyAgentsError::ModelRetry(
                                    SELECTOR_RETRY_HINT.to_owned(),
                                )
                                .into());
                            }
                            Err(error) => return Err(error.into()),
                        }
                    };
                    return Ok(ToolResult::json(serde_json::to_value(job)?));
                }
                if let Some((tasks, parent_run_id)) = &self.tasks {
                    let parent_run_id = parent_run_id(parent.instance_id());
                    let thread_id = self
                        .task_thread_scope
                        .as_ref()
                        .map(|scope| scope(parent.instance_id()));
                    let records = list_subagent_records(tasks.as_ref())
                        .into_iter()
                        .filter(|record| {
                            record.spec.parent_run_id.as_ref() == Some(&parent_run_id)
                                && thread_id.as_ref().is_none_or(|thread| {
                                    record
                                        .spec
                                        .thread_id
                                        .as_ref()
                                        .map(ToString::to_string)
                                        .as_ref()
                                        == thread.as_ref()
                                })
                        })
                        .collect::<Vec<_>>();
                    let task_id = match subagent_record_for_task(
                        tasks.as_ref(),
                        id,
                        parent_run_id.as_str(),
                    ) {
                        Ok(record)
                            if record.spec.parent_run_id.as_ref() == Some(&parent_run_id)
                                && thread_id.as_ref().is_none_or(|thread| {
                                    record
                                        .spec
                                        .thread_id
                                        .as_ref()
                                        .map(ToString::to_string)
                                        .as_ref()
                                        == thread.as_ref()
                                }) =>
                        {
                            Some(record.spec.task_id.as_str().to_owned())
                        }
                        Err(WaitError::Unknown) => task_id_for_session_in_records(
                            records.clone(),
                            id,
                            parent_run_id.as_str(),
                        )
                        .ok(),
                        Err(_) => None,
                        Ok(_) => None,
                    };
                    if let Some(task_id) = task_id
                        && let Some(record) = records
                            .into_iter()
                            .find(|record| record.spec.task_id.as_str() == task_id)
                    {
                        return Ok(ToolResult::json(durable_wait_snapshot(record)));
                    }
                }
                if let Some((sessions, key)) = &self.sessions
                    && let Some(session) = sessions
                        .get(id)
                        .await?
                        .filter(|session| session.parent_session == key(parent.instance_id()))
                {
                    let awaiting_user = session.status == DurableSubagentStatus::AwaitingUser;
                    let question = session
                        .latest_history
                        .as_ref()
                        .and_then(|history| history.last())
                        .map(tinyinference_llm::message::Message::text)
                        .unwrap_or_default();
                    let envelope = SubagentAwaitingUser {
                        task_id: session.current_task_id.clone().unwrap_or_default(),
                        agent_id: session.agent_id.clone(),
                        worker_thread_id: session.worker_thread_id.clone(),
                        question,
                        checkpointed: true,
                    };
                    return Ok(ToolResult::json(json!({
                        "session_id": session.subagent_session_id,
                        "task_id": session.current_task_id,
                        "status": session.status,
                        "awaiting_user_envelope": awaiting_user.then_some(envelope.render()),
                        "latest_error": session.latest_error,
                    })));
                }
                if self.recover_selector_errors {
                    return Err(TinyAgentsError::ModelRetry(SELECTOR_RETRY_HINT.to_owned()).into());
                }
                return Err(SubAgentJobError::NotFound(id.to_owned()).into());
            }
            Some(_) => anyhow::bail!("action must be `query`, `cancel`, `close`, or `await`"),
        }
        if let Some(job_id) = identifier {
            let job = self.jobs.try_get_owned(job_id, &owner)?;
            if let Some(job) = job {
                return Ok(ToolResult::json(serde_json::to_value(job)?));
            }
            let Some((sessions, key)) = self.sessions.as_ref() else {
                if self.recover_selector_errors {
                    return Err(TinyAgentsError::ModelRetry(SELECTOR_RETRY_HINT.to_owned()).into());
                }
                return Err(SubAgentJobError::NotFound(job_id.to_owned()).into());
            };
            let Some(session) = sessions
                .get(job_id)
                .await?
                .filter(|session| session.parent_session == key(parent.instance_id()))
            else {
                if self.recover_selector_errors {
                    return Err(TinyAgentsError::ModelRetry(SELECTOR_RETRY_HINT.to_owned()).into());
                }
                return Err(SubAgentJobError::NotFound(job_id.to_owned()).into());
            };
            Ok(ToolResult::json(serde_json::to_value(
                DurableSubagentSessionSummary::from(&session),
            )?))
        } else {
            let mut rows = self
                .jobs
                .try_list_owned(&owner)?
                .into_iter()
                .map(|job| {
                    serde_json::to_value(job).map(|mut v| {
                        v["kind"] = Value::String("job".into());
                        v
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if let Some((sessions, key)) = &self.sessions {
                let parent_key = key(parent.instance_id());
                rows.extend(
                    sessions
                        .list_for_parent(&parent_key, None)
                        .await?
                        .iter()
                        .map(|session| {
                            let mut value =
                                serde_json::to_value(DurableSubagentSessionSummary::from(session))
                                    .unwrap_or(Value::Null);
                            value["kind"] = Value::String("session".into());
                            value
                        }),
                );
            }
            if let Some((tasks, parent_run_id)) = &self.tasks {
                let thread_id = self
                    .task_thread_scope
                    .as_ref()
                    .map(|scope| scope(parent.instance_id()));
                let filter = OrchestrationTaskFilter {
                    parent_run_id: Some(parent_run_id(parent.instance_id())),
                    ..OrchestrationTaskFilter::default()
                };
                rows.extend(
                    tasks
                        .list(filter)
                        .into_iter()
                        .filter(|task| {
                            thread_id.as_ref().is_none_or(|thread| {
                                task.spec
                                    .thread_id
                                    .as_ref()
                                    .map(ToString::to_string)
                                    .as_ref()
                                    == thread.as_ref()
                            })
                        })
                        .map(|task| {
                            let mut value = serde_json::to_value(task).unwrap_or(Value::Null);
                            value["kind"] = Value::String("task".into());
                            value
                        }),
                );
            }
            Ok(ToolResult::json(Value::Array(rows)))
        }
    }
}

/// Host tool that sends a message to a queued or running subagent job.
#[derive(Clone)]
pub struct SubAgentMessageTool {
    jobs: SubAgentJobRegistry,
    sessions: Option<SessionOverlay>,
    tool_name: String,
    description: String,
    parameters: Option<Value>,
    argument_names: MessageArgumentNames,
    lane: Option<QueueLane>,
    lane_field: Option<String>,
    owner_scope: OwnerScope,
    permission: PermissionLevel,
    scope: ToolScope,
    category: ToolCategory,
    timeout: ToolTimeout,
    recover_selector_errors: bool,
}

impl SubAgentMessageTool {
    /// Creates the message tool over `jobs`.
    pub fn new(jobs: SubAgentJobRegistry) -> Self {
        Self {
            jobs,
            sessions: None,
            tool_name: "subagent_message".into(),
            description: "Send a message to a queued or running asynchronous subagent job.".into(),
            parameters: None,
            argument_names: MessageArgumentNames::default(),
            lane: None,
            lane_field: None,
            owner_scope: Arc::new(|owner| owner.to_string()),
            permission: PermissionLevel::ReadOnly,
            scope: ToolScope::All,
            category: ToolCategory::System,
            timeout: ToolTimeout::Inherit,
            recover_selector_errors: false,
        }
    }

    /// Returns selector misses as model-correctable errors instead of fatal
    /// tool dispatch failures. Storage and registry failures remain fatal.
    pub fn recover_selector_errors(mut self) -> Self {
        self.recover_selector_errors = true;
        self
    }

    /// Uses a host-facing name while retaining this tool's typed message logic.
    pub fn with_tool_name(mut self, name: impl Into<String>) -> Self {
        self.tool_name = name.into();
        self
    }

    /// Uses a host-facing description for a named steering alias.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Resolves a per-turn run id to the stable owner key used by the matching
    /// spawn and Jobs aliases.
    pub fn with_owner_scope(mut self, resolve: OwnerScope) -> Self {
        self.owner_scope = resolve;
        self
    }

    /// Uses a host-facing schema for a named steering alias.
    pub fn with_parameters(mut self, parameters: Value) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Maps host schema fields to the generic job/session and message fields.
    pub fn with_argument_fields(
        mut self,
        job_id: impl Into<String>,
        session_id: impl Into<String>,
        message: impl Into<String>,
        request_id: impl Into<String>,
    ) -> Self {
        self.argument_names = MessageArgumentNames {
            job_id: job_id.into(),
            session_id: session_id.into(),
            message: message.into(),
            request_id: request_id.into(),
        };
        self
    }

    /// Binds a named message operation to one real job queue lane.
    ///
    /// The generic `subagent_message` tool leaves this unset and preserves its
    /// historical unframed steer behavior.
    pub fn with_lane(mut self, lane: QueueLane) -> Self {
        self.lane = Some(lane);
        self
    }

    /// Reads a lane field from an alias schema. `"collect"` routes to the
    /// collect lane; missing or unknown values preserve the host's default
    /// steer behavior.
    pub fn with_lane_field(mut self, field: impl Into<String>) -> Self {
        self.lane_field = Some(field.into());
        self
    }

    /// Preserves host execution metadata when a product alias uses this
    /// generic dispatch implementation.
    pub fn with_metadata(
        mut self,
        permission: PermissionLevel,
        scope: ToolScope,
        category: ToolCategory,
        timeout: ToolTimeout,
    ) -> Self {
        self.permission = permission;
        self.scope = scope;
        self.category = category;
        self.timeout = timeout;
        self
    }

    /// Enables steering a live durable session by its session id.
    pub fn with_sessions(
        mut self,
        sessions: Arc<dyn SubagentSessionStore>,
        parent_key: impl Fn(u64) -> String + Send + Sync + 'static,
    ) -> Self {
        self.sessions = Some((sessions, Arc::new(parent_key)));
        self
    }
}

#[async_trait]
impl Tool for SubAgentMessageTool {
    fn name(&self) -> &str {
        &self.tool_name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters_schema(&self) -> Value {
        self.parameters.clone().unwrap_or_else(|| json!({
            "type": "object",
            "properties": {
                "job_id": { "type": "string", "description": "Live job id. Use session_id for a durable reusable session." },
                "session_id": { "type": "string", "description": "Durable reusable session id." },
                "message": { "type": "string" },
                "request_id": {
                    "type": "string",
                    "description": "Optional idempotency key: resending the same request_id to the same job does not queue the message again while it is among the most recent 64 request ids remembered for that job (older ids are evicted); ids over 128 bytes are rejected."
                }
            },
            "required": ["message"],
            "oneOf": [{"required":["job_id"]},{"required":["session_id"]}]
        }))
    }

    fn permission_level(&self) -> PermissionLevel {
        self.permission
    }

    fn scope(&self) -> ToolScope {
        self.scope
    }

    fn category(&self) -> ToolCategory {
        self.category
    }

    fn timeout_policy(&self, _args: &Value) -> ToolTimeout {
        self.timeout
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        let _ = args;
        anyhow::bail!("subagent_message requires typed-parent dispatch")
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> ToolDispatch<State, Ctx> for SubAgentMessageTool {
    fn tool(&self) -> Arc<dyn Tool> {
        Arc::new(self.clone())
    }

    async fn execute(
        &self,
        _state: &State,
        _call_id: tinyagents_harness::ids::CallId,
        args: Value,
        _options: tinytools::ToolCallOptions,
        parent: &RunContext<Ctx>,
    ) -> anyhow::Result<ToolResult> {
        let owner = (self.owner_scope)(parent.instance_id());
        let job_id = if let Some(session_id) = args
            .get(&self.argument_names.session_id)
            .and_then(Value::as_str)
        {
            let (sessions, key) = self
                .sessions
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("durable sessions are not configured"))?;
            let Some(session) = sessions
                .get(session_id)
                .await?
                .filter(|session| session.parent_session == key(parent.instance_id()))
            else {
                return Err(selector_retry(
                    self.recover_selector_errors,
                    "durable subagent session not found",
                ));
            };
            let Some(task_id) = session.current_task_id else {
                if self.recover_selector_errors {
                    return Err(TinyAgentsError::ModelRetry(SELECTOR_RETRY_HINT.to_owned()).into());
                }
                return Err(anyhow::anyhow!(
                    "durable subagent session has no running task"
                ));
            };
            let driver_job = session
                .lifecycle_key
                .as_ref()
                .and_then(|key| self.jobs.driver_job_for_key(key, &owner))
                .map(|job_id| job_id.as_str().to_owned());
            if let Some(job_id) = driver_job {
                job_id
            } else {
                let _ = self.jobs.try_get_owned(&task_id, &owner)?;
                task_id
            }
        } else {
            let Some(job_id) = args
                .get(&self.argument_names.job_id)
                .and_then(Value::as_str)
            else {
                return Err(selector_retry(
                    self.recover_selector_errors,
                    "job_id or session_id must be a string",
                ));
            };
            job_id.to_owned()
        };
        let message = args
            .get(&self.argument_names.message)
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("message must be a string"))?;
        let request_id = match args.get(&self.argument_names.request_id) {
            None | Some(Value::Null) => None,
            Some(value) => {
                let id = value
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("request_id must be a string when provided"))?;
                Some(id)
            }
        };
        let lane = self.lane.or_else(|| {
            self.lane_field.as_ref().map(|field| {
                if args.get(field).and_then(Value::as_str) == Some("collect") {
                    QueueLane::Collect
                } else {
                    QueueLane::Steer
                }
            })
        });
        let duplicate = match self
            .jobs
            .send_message_with_lane_and_request_id(&job_id, &owner, message, request_id, lane)
        {
            Ok(duplicate) => duplicate,
            Err(error) => return Err(selector_error(error, self.recover_selector_errors)),
        };
        let mut payload = json!({
            "job_id": job_id,
            "status": "message_queued"
        });
        if duplicate {
            payload["duplicate"] = Value::Bool(true);
        }
        Ok(ToolResult::json(payload))
    }
}

/// Registers the standard run-scoped query and message tools in a harness registry.
pub fn register_subagent_job_tools<State: Send + Sync, Ctx: Send + Sync>(
    registry: &mut ToolRegistry<State, Ctx>,
    jobs: SubAgentJobRegistry,
) -> &mut ToolRegistry<State, Ctx> {
    registry
        .register_dispatch(Arc::new(SubAgentJobsTool::new(jobs.clone())))
        .register_dispatch(Arc::new(SubAgentMessageTool::new(jobs)))
}
type OwnerScope = Arc<dyn Fn(u64) -> String + Send + Sync>;

#[cfg(test)]
#[path = "jobs_tests.rs"]
mod tests;
