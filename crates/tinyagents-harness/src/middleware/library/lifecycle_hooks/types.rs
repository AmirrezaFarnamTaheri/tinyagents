//! Host traits the lifecycle hooks are generic over.

/// Where a turn's memory pack lives on the host's run context.
pub trait MemoryPackSource<Ctx>: Send + Sync {
    /// The text to add to each model request, or `None` when the turn has no
    /// pack.
    fn injection(&self, data: &Ctx) -> Option<String>;
}

/// Host decision for [`PackedToolRouteMiddleware`](super::PackedToolRouteMiddleware): which pack owns a tool name
/// and whether this turn may run it.
pub trait PackedToolRouter<Ctx>: Send + Sync {
    /// The pack id to route `tool` through, or `None` to leave the call alone
    /// (no pack owns it, the pack is not withheld, or the session blocks it).
    fn pack_for(&self, data: &Ctx, tool: &str) -> Option<String>;
}
