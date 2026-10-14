//! [`ToolExposureShadowMiddleware`]: drive the crate-native tool-selection
//! layer over the broad candidate set and log any divergence from the set a
//! host actually registered. Never enforced.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tinyinference_llm::model::ModelRequest;
use tinyinference_llm::tool::ToolSchema;

use crate::context::RunContext;
use crate::error::Result;
use crate::middleware::{ContextualToolSelectionMiddleware, Middleware, ToolAllowlistMiddleware};

/// How the shadow selection differs from what the host registered. All three
/// lists are empty when the two agree.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExposureDivergence {
    /// Registered as callable, but the shadow selection would not expose it.
    pub missing_from_shadow: Vec<String>,
    /// Exposed by the shadow selection, but not registered.
    pub extra_in_shadow: Vec<String>,
    /// Exposed by the shadow selection but refused by the static allow guard.
    pub allowlist_disagree: Vec<String>,
}

impl ExposureDivergence {
    /// Whether the shadow selection and the host's registration agree.
    #[must_use]
    pub fn is_parity(&self) -> bool {
        self.missing_from_shadow.is_empty()
            && self.extra_in_shadow.is_empty()
            && self.allowlist_disagree.is_empty()
    }
}

/// SHADOW tool-exposure middleware.
///
/// A host collapses its exposure policy (allow/deny lists, sub-agent scope,
/// MCP visibility, channel ceiling) into a visible set and registers only
/// that. This middleware expresses the same policy as the crate's composed
/// selection layer: a [`ToolAllowlistMiddleware`] for the static guard and a
/// [`ContextualToolSelectionMiddleware::inheriting`] so a delegated child can
/// only narrow its parent's exposure.
///
/// On the first model call it drives that selection over a scratch
/// [`ModelRequest`] built from the **broad candidate set** (not the live
/// request, whose tools the host already narrowed). That makes the exposure
/// decision event-native (the selection emits `ToolsFiltered`) and logs any
/// divergence from the registered set under `[tool-exposure]`. It never
/// mutates the live `ModelRequest::tools`, so what the model can call is
/// unchanged.
pub struct ToolExposureShadowMiddleware {
    allowlist: ToolAllowlistMiddleware,
    selection: ContextualToolSelectionMiddleware,
    candidates: Vec<ToolSchema>,
    registered: HashSet<String>,
    tags: Vec<String>,
    ran: AtomicBool,
}

impl ToolExposureShadowMiddleware {
    /// Build the shadow layer from the same inputs the host feeds its runner:
    /// the broad `candidate_names` and the narrowed `allowed` visible set.
    ///
    /// Fail-closed: `None` means no filter was supplied, so every candidate is
    /// visible; `Some(set)` means exactly the named candidates, so
    /// `Some(empty)` denies all. `tags` (agent id, task kind, tier, channel)
    /// ride on the scratch request for the divergence log.
    #[must_use]
    pub fn new(
        candidate_names: &[String],
        allowed: Option<&HashSet<String>>,
        tags: Vec<String>,
    ) -> Self {
        let registered: HashSet<String> = match allowed {
            None => candidate_names.iter().cloned().collect(),
            Some(set) => candidate_names
                .iter()
                .filter(|name| set.contains(*name))
                .cloned()
                .collect(),
        };
        let excluded: Vec<String> = candidate_names
            .iter()
            .filter(|name| !registered.contains(*name))
            .cloned()
            .collect();
        // Parent ceiling = the broad candidates, child = the visible set, so the
        // effective allow is `candidates ∩ registered` and can never widen.
        let selection = ContextualToolSelectionMiddleware::inheriting(
            Some(candidate_names.to_vec()),
            Vec::<String>::new(),
            Some(registered.iter().cloned().collect::<Vec<_>>()),
            excluded,
        );
        let allowlist = ToolAllowlistMiddleware::new(registered.iter().cloned());
        let candidates = candidate_names
            .iter()
            .map(|name| ToolSchema::new(name.clone(), String::new(), serde_json::json!({})))
            .collect();
        Self {
            allowlist,
            selection,
            candidates,
            registered,
            tags,
            ran: AtomicBool::new(false),
        }
    }

    /// Run the shadow selection once for `model` and report how it differs
    /// from the registered set.
    ///
    /// # Errors
    ///
    /// The selection layer's error.
    pub async fn evaluate<Ctx: Send + Sync>(
        &self,
        ctx: &mut RunContext<Ctx>,
        model: Option<String>,
    ) -> Result<ExposureDivergence> {
        let mut scratch = ModelRequest {
            tools: self.candidates.clone(),
            model,
            tags: self.tags.clone(),
            ..Default::default()
        };
        Middleware::<(), Ctx>::before_model(&self.selection, ctx, &(), &mut scratch).await?;
        let shadow_exposed: HashSet<String> =
            scratch.tools.iter().map(|s| s.name.clone()).collect();
        let mut divergence = ExposureDivergence {
            missing_from_shadow: self
                .registered
                .iter()
                .filter(|name| !shadow_exposed.contains(*name))
                .cloned()
                .collect(),
            extra_in_shadow: shadow_exposed
                .iter()
                .filter(|name| !self.registered.contains(*name))
                .cloned()
                .collect(),
            allowlist_disagree: shadow_exposed
                .iter()
                .filter(|name| !self.allowlist.allows(name))
                .cloned()
                .collect(),
        };
        divergence.missing_from_shadow.sort();
        divergence.extra_in_shadow.sort();
        divergence.allowlist_disagree.sort();
        Ok(divergence)
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> Middleware<State, Ctx> for ToolExposureShadowMiddleware {
    fn name(&self) -> &str {
        "tool_exposure_shadow"
    }

    async fn before_model(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &State,
        request: &mut ModelRequest,
    ) -> Result<()> {
        // Once per run: the exposure decision is stable for the turn.
        if self.ran.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let divergence = self.evaluate(ctx, request.model.clone()).await?;
        if divergence.is_parity() {
            tracing::debug!(
                candidates = self.candidates.len(),
                registered = self.registered.len(),
                tags = ?self.tags,
                "[tool-exposure] shadow crate selection agrees with the host's registration (parity)"
            );
        } else {
            tracing::warn!(
                missing_from_shadow = ?divergence.missing_from_shadow,
                extra_in_shadow = ?divergence.extra_in_shadow,
                allowlist_disagree = ?divergence.allowlist_disagree,
                registered = self.registered.len(),
                candidates = self.candidates.len(),
                tags = ?self.tags,
                "[tool-exposure] DIVERGENCE: shadow crate selection differs from the host's registration — NOT enforced"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tool_exposure_shadow_tests.rs"]
mod tests;
