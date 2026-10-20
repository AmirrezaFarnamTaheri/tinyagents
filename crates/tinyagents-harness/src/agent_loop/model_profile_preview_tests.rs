//! The agent loop previews the target model's profile onto
//! [`RunContext::model_profile`] before `before_model` middleware runs, so
//! middleware can shape what it adds for that model (#6962).

use super::*;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::middleware::Middleware;
use crate::testkit::ScriptedModel;
use tinyinference_llm::model::ModelProfile;

/// Records the profile each `before_model` call saw.
struct SeenProfiles(Arc<Mutex<Vec<Option<ModelProfile>>>>);

struct SelectModel(&'static str);

#[async_trait]
impl Middleware<()> for SelectModel {
    fn name(&self) -> &str {
        "select-model"
    }

    async fn before_model(
        &self,
        _ctx: &mut RunContext<()>,
        _state: &(),
        request: &mut ModelRequest,
    ) -> Result<()> {
        request.model = Some(self.0.into());
        Ok(())
    }
}

#[async_trait]
impl Middleware<()> for SeenProfiles {
    fn name(&self) -> &str {
        "seen-profiles"
    }

    async fn before_model(
        &self,
        ctx: &mut RunContext<()>,
        _state: &(),
        _request: &mut ModelRequest,
    ) -> Result<()> {
        self.0.lock().unwrap().push(ctx.model_profile.clone());
        Ok(())
    }
}

#[tokio::test]
async fn before_model_sees_the_target_model_profile() {
    let profile = ModelProfile {
        model: Some("deepseek-chat".into()),
        hoists_system_messages: true,
        ..ModelProfile::default()
    };
    let model = Arc::new(ScriptedModel::replies(vec!["done"]).with_profile(profile.clone()));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.with_model_resolver(Arc::new(crate::host::FixedModelResolver::named("mock", model)));
    harness.push_middleware(Arc::new(SeenProfiles(seen.clone())));

    harness
        .invoke_default(&(), vec![Message::user("go")])
        .await
        .expect("run succeeds");

    let seen = seen.lock().unwrap();
    assert_eq!(seen.as_slice(), [Some(profile)]);
}

#[tokio::test]
async fn later_middleware_sees_model_selected_by_earlier_middleware() {
    let first_profile = ModelProfile {
        model: Some("first".into()),
        hoists_system_messages: true,
        ..ModelProfile::default()
    };
    let second_profile = ModelProfile {
        model: Some("second".into()),
        hoists_system_messages: false,
        ..ModelProfile::default()
    };
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.with_model_resolver(Arc::new(crate::host::FixedModelResolver::named("first", Arc::new(ScriptedModel::replies(vec!["wrong"]).with_profile(first_profile)),)));
    harness.with_model_resolver(Arc::new(crate::host::FixedModelResolver::named("second", Arc::new(ScriptedModel::replies(vec!["done"]).with_profile(second_profile.clone())),)));
    harness.push_middleware(Arc::new(SelectModel("second")));
    harness.push_middleware(Arc::new(SeenProfiles(seen.clone())));

    harness
        .invoke_default(&(), vec![Message::user("go")])
        .await
        .expect("run succeeds");

    assert_eq!(seen.lock().unwrap().as_slice(), [Some(second_profile)]);
}

#[test]
fn profile_preview_uses_the_eligible_default_for_an_ineligible_pin() {
    use crate::host::{ModelResolver, PrebuiltModelResolver};
    use tinyinference_llm::model::{CapabilitySet, ModelRequest};

    let primary: Arc<dyn ChatModel<()>> = Arc::new(ScriptedModel::replies(vec!["primary"]).with_profile(ModelProfile {
        model: Some("primary".into()),
        tool_calling: true,
        ..ModelProfile::default()
    }));
    let pinned: Arc<dyn ChatModel<()>> = Arc::new(ScriptedModel::replies(vec!["pinned"]).with_profile(ModelProfile {
        model: Some("pinned".into()),
        tool_calling: false,
        ..ModelProfile::default()
    }));
    let resolver: Arc<dyn ModelResolver<()>> = Arc::new(
        PrebuiltModelResolver::new(primary, [("pinned".to_owned(), pinned)])
            .with_primary_name("primary"),
    );
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.with_model_resolver(resolver);

    let request = ModelRequest::default()
        .with_model("pinned")
        .with_required_capabilities(CapabilitySet { tool_calling: true, ..CapabilitySet::default() });
    let profile = harness.preview_model_profile_for_request(&request)
        .expect("eligible primary profile is previewed");
    assert_eq!(profile.model.as_deref(), Some("primary"));
}
