//! Memory-pack injection middleware.

use std::marker::PhantomData;

use async_trait::async_trait;

use tinyagents_harness::context::RunContext;
use tinyagents_harness::error::Result as TaResult;
use tinyagents_harness::middleware::Middleware;
use tinyinference_llm::model::ModelRequest;

use super::MemoryPackSource;
use tinyagents_harness::middleware::library::push_ephemeral_instruction;

/// Adds the turn's memory pack to every model request of the turn.
///
/// Uses [`push_ephemeral_instruction`]: the request is built from a copy of the
/// working transcript, so the pack is never committed and the persisted history
/// and the provider's cached prefix stay as they were.
pub struct MemoryPackMiddleware<Ctx, Src> {
    source: Src,
    _ctx: PhantomData<fn() -> Ctx>,
}

impl<Ctx, Src> MemoryPackMiddleware<Ctx, Src> {
    /// Build the middleware over the host's `source`.
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _ctx: PhantomData,
        }
    }
}

#[async_trait]
impl<Ctx, Src> Middleware<(), Ctx> for MemoryPackMiddleware<Ctx, Src>
where
    Ctx: Send + Sync + 'static,
    Src: MemoryPackSource<Ctx>,
{
    fn name(&self) -> &str {
        "memory_pack"
    }

    fn is_observer(&self) -> bool {
        true
    }

    async fn before_model(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &(),
        request: &mut ModelRequest,
    ) -> TaResult<()> {
        let Some(injection) = self.source.injection(&ctx.data) else {
            return Ok(());
        };
        tracing::trace!("[lifecycle_hooks] memory pack added to the model request");
        push_ephemeral_instruction(request, injection, ctx.model_profile.as_ref());
        Ok(())
    }
}
