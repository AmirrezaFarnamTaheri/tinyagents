use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tinytools::{RankCandidate, RankContext, ToolRanker};

use tinyinference_embeddings::{EmbeddingModel, Result as EmbedResult};

/// Embeds a text as a bag of three hand-picked words, so similarity is
/// deterministic and readable.
struct BagEmbedder {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl EmbeddingModel for BagEmbedder {
    fn name(&self) -> &str {
        "bag"
    }
    fn model_id(&self) -> &str {
        "bag-v1"
    }
    fn dimensions(&self) -> usize {
        3
    }
    async fn embed(&self, texts: &[String]) -> EmbedResult<Vec<Vec<f32>>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(texts
            .iter()
            .map(|t| {
                let t = t.to_ascii_lowercase();
                vec![
                    f32::from(u8::from(t.contains("message") || t.contains("ping"))),
                    f32::from(u8::from(t.contains("email") || t.contains("mail"))),
                    f32::from(u8::from(t.contains("file"))),
                ]
            })
            .collect())
    }
}

const SIGNATURE: &str = "provider=bag;model=bag-v1;dims=3";

fn candidates() -> Vec<RankCandidate> {
    vec![
        RankCandidate::new("SLACK_SEND_MESSAGE", "send a message to a channel")
            .with_family("slack"),
        RankCandidate::new("GMAIL_SEND_EMAIL", "send an email").with_family("gmail"),
        RankCandidate::new("file_read", "read a file"),
    ]
}

#[tokio::test]
async fn ranks_by_cosine_and_embeds_the_catalogue_once() {
    let embedder = Arc::new(BagEmbedder {
        calls: AtomicUsize::new(0),
    });
    let ranker = EmbeddingToolRanker::new(embedder.clone(), SIGNATURE);
    assert_eq!(ranker.kind(), "embedding");

    let hits = ranker
        .rank("ping alex", &RankContext::empty(), &candidates(), 2)
        .await
        .unwrap();
    assert_eq!(hits[0].key, "SLACK_SEND_MESSAGE");
    assert!(hits[0].confidence.is_none());
    assert_eq!(hits.len(), 2);
    // One batch for the catalogue plus one for the intent.
    assert_eq!(embedder.calls.load(Ordering::SeqCst), 2);

    let hits = ranker
        .rank("mail the report", &RankContext::empty(), &candidates(), 1)
        .await
        .unwrap();
    assert_eq!(hits[0].key, "GMAIL_SEND_EMAIL");
    // Only the intent was embedded this time.
    assert_eq!(embedder.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn empty_intent_is_rejected() {
    let ranker = EmbeddingToolRanker::new(
        Arc::new(BagEmbedder {
            calls: AtomicUsize::new(0),
        }),
        SIGNATURE,
    );
    assert!(
        ranker
            .rank("  ", &RankContext::empty(), &candidates(), 1)
            .await
            .is_err()
    );
}

/// A tool that appears later — a newly connected toolkit's actions, a
/// rewritten description — is embedded on its own; the rest is a cache hit.
#[tokio::test]
async fn a_new_or_changed_tool_is_embedded_incrementally() {
    let embedder = Arc::new(BagEmbedder {
        calls: AtomicUsize::new(0),
    });
    let ranker = EmbeddingToolRanker::new(embedder.clone(), SIGNATURE);
    ranker
        .rank("ping", &RankContext::empty(), &candidates(), 1)
        .await
        .unwrap();
    assert_eq!(
        embedder.calls.load(Ordering::SeqCst),
        2,
        "catalogue + intent"
    );

    let mut grown = candidates();
    grown.push(RankCandidate::new("NOTION_CREATE_PAGE", "create a page").with_family("notion"));
    grown[2] = RankCandidate::new("file_read", "read a file from disk");
    ranker
        .rank("ping", &RankContext::empty(), &grown, 1)
        .await
        .unwrap();
    // One batch for the two unseen texts (the new tool and the changed one),
    // plus the intent — never the whole catalogue again.
    assert_eq!(embedder.calls.load(Ordering::SeqCst), 4);
    assert_eq!(
        ranker.cache.read().unwrap().len(),
        5,
        "old and new descriptions both cached; a stale entry is harmless"
    );
}

/// A port fake that persists deltas, with independently controllable failures.
#[derive(Debug, Default)]
struct MemoryCache {
    entries: std::sync::Mutex<HashMap<(String, u64), Vec<f32>>>,
    fail_load: std::sync::atomic::AtomicBool,
    fail_store: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl EmbeddingCache for MemoryCache {
    async fn load(&self, signature: &str, keys: &[u64]) -> crate::Result<HashMap<u64, Vec<f32>>> {
        if self.fail_load.load(Ordering::SeqCst) {
            return Err(crate::TinyAgentsError::Storage("cache unavailable".into()));
        }
        let entries = self.entries.lock().unwrap();
        Ok(keys
            .iter()
            .filter_map(|key| {
                entries
                    .get(&(signature.to_owned(), *key))
                    .map(|vector| (*key, vector.clone()))
            })
            .collect())
    }

    async fn store(&self, signature: &str, values: &[(u64, Vec<f32>)]) -> crate::Result<()> {
        if self.fail_store.load(Ordering::SeqCst) {
            return Err(crate::TinyAgentsError::Storage("cache unavailable".into()));
        }
        let mut entries = self.entries.lock().unwrap();
        for (key, vector) in values {
            entries.insert((signature.to_owned(), *key), vector.clone());
        }
        Ok(())
    }
}

fn bag() -> Arc<BagEmbedder> {
    Arc::new(BagEmbedder {
        calls: AtomicUsize::new(0),
    })
}

#[tokio::test]
async fn pluggable_cache_warms_recreated_rankers_and_separates_embedding_spaces() {
    let cache = Arc::new(MemoryCache::default());
    let first = bag();
    EmbeddingToolRanker::new(first.clone(), SIGNATURE)
        .with_cache(cache.clone())
        .rank("ping", &RankContext::empty(), &candidates(), 1)
        .await
        .unwrap();
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
    let warm = bag();
    let hits = EmbeddingToolRanker::new(warm.clone(), SIGNATURE)
        .with_cache(cache.clone())
        .rank("ping", &RankContext::empty(), &candidates(), 1)
        .await
        .unwrap();
    assert_eq!(hits[0].key, "SLACK_SEND_MESSAGE");
    assert_eq!(
        warm.calls.load(Ordering::SeqCst),
        1,
        "persistent vectors avoid catalogue embeddings"
    );
    let other = bag();
    EmbeddingToolRanker::new(other.clone(), "different-space")
        .with_cache(cache)
        .rank("ping", &RankContext::empty(), &candidates(), 1)
        .await
        .unwrap();
    assert_eq!(other.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn pluggable_cache_load_and_store_failures_still_rank_without_files() {
    for fail_load in [true, false] {
        let cache = Arc::new(MemoryCache::default());
        cache.fail_load.store(fail_load, Ordering::SeqCst);
        cache.fail_store.store(!fail_load, Ordering::SeqCst);
        let embedder = bag();
        let ranker = EmbeddingToolRanker::new(embedder.clone(), SIGNATURE).with_cache(cache);
        let hits = ranker
            .rank("ping", &RankContext::empty(), &candidates(), 1)
            .await
            .unwrap();
        assert_eq!(hits[0].key, "SLACK_SEND_MESSAGE");
        ranker
            .rank("mail", &RankContext::empty(), &candidates(), 1)
            .await
            .unwrap();
        assert_eq!(
            embedder.calls.load(Ordering::SeqCst),
            3,
            "in-memory vectors survive failed persistence"
        );
    }
}

#[tokio::test]
async fn pluggable_cache_reembeds_invalid_vectors_and_partial_misses() {
    let cache = Arc::new(MemoryCache::default());
    let tools = candidates();
    {
        let mut entries = cache.entries.lock().unwrap();
        entries.insert(
            (SIGNATURE.into(), EmbeddingToolRanker::key(&tools[0])),
            vec![1.0, 0.0],
        );
        entries.insert(
            (SIGNATURE.into(), EmbeddingToolRanker::key(&tools[1])),
            vec![f32::NAN, 0.0, 0.0],
        );
        entries.insert(
            (SIGNATURE.into(), EmbeddingToolRanker::key(&tools[2])),
            vec![0.0, 0.0, 1.0],
        );
    }
    let embedder = bag();
    let hits = EmbeddingToolRanker::new(embedder.clone(), SIGNATURE)
        .with_cache(cache.clone())
        .rank("ping", &RankContext::empty(), &tools, 1)
        .await
        .unwrap();
    assert_eq!(hits[0].key, "SLACK_SEND_MESSAGE");
    assert_eq!(embedder.calls.load(Ordering::SeqCst), 2);
    let entries = cache.entries.lock().unwrap();
    assert!(
        entries
            .values()
            .all(|vector| vector.len() == 3 && vector.iter().all(|value| value.is_finite()))
    );
}

#[tokio::test]
async fn pluggable_cache_deltas_preserve_other_rankers_catalogues() {
    let cache = Arc::new(MemoryCache::default());
    let left = EmbeddingToolRanker::new(bag(), SIGNATURE).with_cache(cache.clone());
    let right = EmbeddingToolRanker::new(bag(), SIGNATURE).with_cache(cache.clone());
    let tools = candidates();
    let context = RankContext::empty();
    let (a, b) = tokio::join!(
        left.rank("ping", &context, &tools[..1], 1),
        right.rank("mail", &context, &tools[1..], 1)
    );
    a.unwrap();
    b.unwrap();
    let warm = bag();
    EmbeddingToolRanker::new(warm.clone(), SIGNATURE)
        .with_cache(cache)
        .rank("ping", &context, &tools, 1)
        .await
        .unwrap();
    assert_eq!(
        warm.calls.load(Ordering::SeqCst),
        1,
        "both disjoint deltas are retained"
    );
}
