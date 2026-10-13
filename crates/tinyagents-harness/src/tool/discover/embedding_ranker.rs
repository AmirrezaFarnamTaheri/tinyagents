//! [`EmbeddingToolRanker`]: the semantic retriever for `tool_search`.
//!
//! A lexical retriever loses every paraphrase — "ping alex" never overlaps
//! `SLACK_SEND_MESSAGE send a message` — and a decision model can only pick
//! from what the retriever hands it. Measured on the orchestrator's catalogue
//! plus nine Composio toolkits (1,215 tools), BM25 recall@20 was 70%, and
//! that ceiling capped Jev at 67% top-3. This ranker embeds each tool's
//! summary once with the host's embedding model, embeds the intent per search, and ranks by cosine
//! similarity, so recall follows meaning rather than shared words. It is the
//! `retriever` inside `tinytools_jev::JevRanker` for every catalogue larger
//! than one Jev `Choice`.
//!
//! Catalogue embeddings are cached in memory by content hash and optionally
//! through a host cache port ([`EmbeddingCache`], with
//! [`DriverEmbeddingCache`](super::DriverEmbeddingCache) over the storage
//! drivers), keyed by embedding space, so a cold process pays for the catalogue
//! once and every later search embeds only the intent (one provider call).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tinyinference_embeddings::EmbeddingModel;
use tinytools::{RankCandidate, RankContext, RankError, RankHit, ToolRanker};

/// Texts per embedding request. The managed provider accepts far more, but a
/// bounded batch keeps one request under any body cap and lets a partial
/// failure cost one batch, not the catalogue.
const EMBED_BATCH: usize = 64;

/// Persistence for rebuildable catalogue embeddings.
///
/// A host binds the handle to its tenant and credential namespace. The ranker
/// supplies its embedding-space signature and stable summary/family hashes;
/// cache implementations must keep signatures separate and merge stored deltas
/// rather than replace unrelated entries. They must not cache intent vectors.
///
/// Errors are best-effort: the ranker recomputes cache misses and keeps vectors
/// in memory when persistence fails. Error messages should not include secrets.
#[async_trait::async_trait]
pub trait EmbeddingCache: Send + Sync + std::fmt::Debug {
    /// Load any available vectors for the requested hashes in this signature.
    ///
    /// Missing hashes are ordinary misses. The ranker rejects non-finite vectors
    /// and vectors whose dimensions differ from the embedding model.
    async fn load(&self, signature: &str, keys: &[u64]) -> crate::Result<HashMap<u64, Vec<f32>>>;

    /// Merge newly computed vectors into this signature's cache.
    ///
    /// Calls can repeat the same keys and must be idempotent. Implementations
    /// should coordinate concurrent writers so disjoint deltas are preserved.
    async fn store(&self, signature: &str, values: &[(u64, Vec<f32>)]) -> crate::Result<()>;
}

/// Ranks tools by cosine similarity between the intent and each tool's
/// summary, with catalogue embeddings cached.
pub struct EmbeddingToolRanker {
    provider: Arc<dyn EmbeddingModel>,
    /// Identifies the embedding space (provider, model, dimensions) so a cached
    /// vector is never compared against one from a different model.
    signature: String,
    cache: RwLock<HashMap<u64, Vec<f32>>>,
    persistent_cache: Option<Arc<dyn EmbeddingCache>>,
}

impl std::fmt::Debug for EmbeddingToolRanker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingToolRanker")
            .field("provider", &self.provider.name())
            .field("model", &self.provider.model_id())
            .field("persistent_cache", &self.persistent_cache.is_some())
            .finish_non_exhaustive()
    }
}

impl EmbeddingToolRanker {
    /// The stable [`ToolRanker::kind`] of this ranker.
    pub const KIND: &'static str = "embedding";

    /// A ranker over `provider` with an in-memory cache only. `signature`
    /// identifies the provider's embedding space (for example
    /// `provider=voyage;model=voyage-3;dims=1024`); the disk cache is valid
    /// only for the signature it was written under.
    pub fn new(provider: Arc<dyn EmbeddingModel>, signature: impl Into<String>) -> Self {
        Self {
            provider,
            signature: signature.into(),
            cache: RwLock::new(HashMap::new()),
            persistent_cache: None,
        }
    }

    /// Persist catalogue embeddings through a host-supplied cache port.
    ///
    /// Vectors already held in memory are cleared. A failed port never falls
    /// back to a filesystem write. Hosts should create a new ranker when its tenant or credential namespace
    /// changes; its in-memory cache is bound to this handle.
    pub fn with_cache(mut self, cache: Arc<dyn EmbeddingCache>) -> Self {
        self.cache
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        self.persistent_cache = Some(cache);
        self
    }

    fn key(candidate: &RankCandidate) -> u64 {
        // Persisted cache keys must not depend on the standard library's
        // unspecified DefaultHasher algorithm or seed.
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in candidate
            .summary
            .bytes()
            .chain([0xff])
            .chain(candidate.family.as_deref().unwrap_or_default().bytes())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    fn text(candidate: &RankCandidate) -> String {
        match &candidate.family {
            Some(family) => format!("{} ({family})", candidate.summary),
            None => candidate.summary.clone(),
        }
    }

    /// Embeds every candidate not already cached, in batches.
    async fn ensure_cached(&self, candidates: &[RankCandidate]) -> Result<(), RankError> {
        let mut missing: Vec<(u64, String)> = {
            let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
            let mut seen = std::collections::HashSet::new();
            candidates
                .iter()
                .map(|c| (Self::key(c), c))
                .filter(|(k, _)| !cache.contains_key(k) && seen.insert(*k))
                .map(|(k, c)| (k, Self::text(c)))
                .collect()
        };
        if missing.is_empty() {
            return Ok(());
        }
        if let Some(persistent) = &self.persistent_cache {
            let keys: Vec<u64> = missing.iter().map(|(key, _)| *key).collect();
            match persistent.load(&self.signature, &keys).await {
                Ok(found) => {
                    let mut cache = self.cache.write().unwrap_or_else(|p| p.into_inner());
                    for key in keys {
                        if let Some(vector) = found.get(&key).filter(|vector| {
                            vector.len() == self.provider.dimensions()
                                && vector.iter().all(|value| value.is_finite())
                        }) {
                            cache.insert(key, vector.clone());
                        }
                    }
                    missing.retain(|(key, _)| !cache.contains_key(key));
                }
                Err(error) => tracing::warn!(
                    error = %error,
                    "[tool-search] could not load embedding cache"
                ),
            }
            if missing.is_empty() {
                return Ok(());
            }
        }
        tracing::info!(
            missing = missing.len(),
            provider = self.provider.name(),
            "[tool-search] embedding catalogue entries"
        );
        for batch in missing.chunks(EMBED_BATCH) {
            let texts: Vec<String> = batch.iter().map(|(_, t)| t.clone()).collect();
            let vectors =
                self.provider
                    .embed(&texts)
                    .await
                    .map_err(|error| RankError::Backend {
                        reason: format!("embedding failed: {error:#}"),
                    })?;
            if vectors.len() != batch.len() {
                return Err(RankError::Backend {
                    reason: format!(
                        "embedding returned {} vectors for {} texts",
                        vectors.len(),
                        batch.len()
                    ),
                });
            }
            let delta: Vec<(u64, Vec<f32>)> = batch
                .iter()
                .zip(vectors)
                .map(|((key, _), vector)| (*key, vector))
                .collect();
            {
                let mut cache = self.cache.write().unwrap_or_else(|p| p.into_inner());
                for (key, vector) in &delta {
                    cache.insert(*key, vector.clone());
                }
            }
            if let Some(persistent) = &self.persistent_cache
                && let Err(error) = persistent.store(&self.signature, &delta).await
            {
                tracing::warn!(
                    error = %error,
                    "[tool-search] could not persist embedding cache"
                );
            }
        }
        Ok(())
    }
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let (mut dot, mut na, mut nb) = (0.0_f64, 0.0_f64, 0.0_f64);
    for (x, y) in a.iter().zip(b) {
        dot += f64::from(*x) * f64::from(*y);
        na += f64::from(*x) * f64::from(*x);
        nb += f64::from(*y) * f64::from(*y);
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

#[async_trait::async_trait]
impl ToolRanker for EmbeddingToolRanker {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    async fn rank(
        &self,
        intent: &str,
        _context: &RankContext,
        candidates: &[RankCandidate],
        limit: usize,
    ) -> Result<Vec<RankHit>, RankError> {
        let intent = intent.trim();
        if intent.is_empty() {
            return Err(RankError::InvalidInput {
                reason: "intent is empty".to_owned(),
            });
        }
        if candidates.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        self.ensure_cached(candidates).await?;
        let query = self
            .provider
            .embed(&[intent.to_owned()])
            .await
            .map_err(|error| RankError::Backend {
                reason: format!("embedding failed: {error:#}"),
            })?
            .into_iter()
            .next()
            .ok_or_else(|| RankError::Backend {
                reason: "embedding returned no vector for the intent".to_owned(),
            })?;
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        let mut scored: Vec<RankHit> = candidates
            .iter()
            .filter_map(|c| {
                cache
                    .get(&Self::key(c))
                    .map(|v| RankHit::new(c.key.clone(), cosine(&query, v)))
            })
            .collect();
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });
        scored.truncate(limit);
        Ok(scored)
    }
}

#[cfg(test)]
#[path = "embedding_ranker_tests.rs"]
mod test;
