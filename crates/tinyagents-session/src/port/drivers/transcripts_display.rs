//! The display read and the thread listing of driver-backed transcripts.
//!
//! - **Display.** [`TranscriptRead::read_display`] replays a stem's whole
//!   entry log, not the reduced context: `extend` rows are message records, a
//!   `set` after the first write is a compaction marker carrying the new
//!   context (the JSONL writer records exactly that), a `clear` is an empty
//!   compaction, and a `partial` is an interrupted record at the position it
//!   was written. Entries keep no wall clock, so a marker's `ts` is `None` and
//!   intermediate tool-call rows carry no per-step timestamp.
//! - **Thread listing.** [`TranscriptLocator::thread_transcripts`] reads the
//!   index: the thread's written roots, ordered like the file view (session
//!   chains oldest first, pre-identity roots dropped once a session root
//!   exists), and the sub-agent transcripts that descend from a root stem or
//!   (unscoped) name the thread.

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Value;
use tinystoragedrivers_core::{DocumentStoreExt, Filter, Query, Sort, StorageError};

use super::{
    DriverTranscriptHistory, DriverTranscriptLocator, ENTRIES, Entry, HistoryInner, INDEX,
    discovered_seed, normalized, run_on,
};
use crate::transcript::{
    CompactionMarker, DisplayRecord, DisplaySessionTranscript, SessionRef, ThreadTranscript,
    ThreadTranscripts, TranscriptLocator, TranscriptPartial, TranscriptRead, display_message_of,
    partial_display_message, session_stem,
};

impl HistoryInner {
    /// The display projection of this stem's whole log; `None` when nothing
    /// was ever written to it.
    async fn display(&self) -> Result<Option<DisplaySessionTranscript>, StorageError> {
        self.declared.ensure(&self.docs).await?;
        let query = Query::filter(Filter::eq("stem", self.stem.as_str())).sort(Sort::asc("seq"));
        let mut meta = None;
        let mut records: Vec<DisplayRecord> = Vec::new();
        let mut written = false;
        for stored in self.docs.query_all(ENTRIES, &query).await? {
            let entry: Entry = serde_json::from_value(stored.doc).map_err(|error| {
                StorageError::serialization(format!(
                    "transcript entry {} is unreadable: {error}",
                    stored.id
                ))
            })?;
            let was_written = written;
            written |= entry.written;
            if let Some(entry_meta) = entry.meta {
                meta = Some(entry_meta);
            }
            if let Some(set) = entry.set {
                let rows: Vec<_> = normalized(set).iter().map(display_message_of).collect();
                if was_written {
                    records.push(DisplayRecord::Compaction(CompactionMarker {
                        replacement: rows,
                        ts: None,
                        request_id: entry.request_id.clone(),
                    }));
                } else {
                    records.extend(
                        rows.into_iter()
                            .map(|row| DisplayRecord::Message(Box::new(row))),
                    );
                }
            }
            if let Some(extend) = entry.extend {
                records.extend(
                    normalized(extend)
                        .iter()
                        .map(|row| DisplayRecord::Message(Box::new(display_message_of(row)))),
                );
            }
            if let Some(partial) = entry.partial {
                let partial = TranscriptPartial::from(partial);
                records.push(DisplayRecord::Message(Box::new(partial_display_message(
                    &partial,
                    entry.request_id.as_deref(),
                ))));
            }
            if entry.clear {
                records.push(DisplayRecord::Compaction(CompactionMarker {
                    replacement: Vec::new(),
                    ts: None,
                    request_id: None,
                }));
            }
        }
        Ok(written.then(|| DisplaySessionTranscript {
            meta: meta.unwrap_or_else(|| self.seed.clone()),
            records,
        }))
    }
}

impl DriverTranscriptHistory {
    pub(super) fn display(&self) -> anyhow::Result<Option<DisplaySessionTranscript>> {
        let inner = Arc::clone(&self.inner);
        Ok(run_on(&self.bridge, async move { inner.display().await })?)
    }
}

/// One index document's lookup fields.
struct Indexed {
    stem: String,
    agent_id: Option<String>,
    thread_id: Option<String>,
}

impl Indexed {
    fn of(doc: &Value) -> Option<Self> {
        let field = |key: &str| doc.get(key).and_then(Value::as_str).map(str::to_string);
        Some(Self {
            stem: field("stem")?,
            agent_id: field("agent_id"),
            thread_id: field("thread_id"),
        })
    }
}

impl DriverTranscriptLocator {
    /// The written index documents matching `filter`, oldest first.
    fn indexed(&self, filter: Filter, what: &'static str) -> Vec<Indexed> {
        let found = self.run(|docs, declared| async move {
            declared.ensure(&docs).await?;
            let query = Query::filter(filter.and(Filter::eq("written", true)))
                .sort(Sort::asc("created_at"));
            docs.query_all(INDEX, &query).await
        });
        match found {
            Ok(items) => items
                .iter()
                .filter_map(|item| Indexed::of(&item.doc))
                .collect(),
            Err(error) => {
                tracing::warn!(
                    target: "tinyagents_session::port::drivers",
                    locator = %self.label,
                    lookup = what,
                    %error,
                    "[session-store] transcript listing failed"
                );
                Vec::new()
            }
        }
    }

    fn listed(&self, stem: String) -> ThreadTranscript {
        let transcript: Arc<dyn TranscriptRead> =
            Arc::new(self.handle(&stem, discovered_seed(&stem)));
        ThreadTranscript { stem, transcript }
    }

    /// Whether the transcript at `stem` records a session identity (a
    /// pre-identity root does not).
    fn has_session_id(&self, stem: &str) -> bool {
        self.handle(stem, discovered_seed(stem))
            .read_session()
            .ok()
            .flatten()
            .is_some_and(|session| session.meta.session_id.is_some())
    }

    /// The thread's roots in projection order; see the module docs.
    fn ordered_roots(&self, thread_id: &str, roots: Vec<Indexed>) -> Vec<String> {
        let mut chains: Vec<Vec<String>> = Vec::new();
        let mut agents: HashSet<String> = HashSet::new();
        for root in &roots {
            if let Some(agent_id) = &root.agent_id
                && agents.insert(agent_id.clone())
            {
                let chain: Vec<String> = self
                    .session_chain(&SessionRef::scoped(thread_id, agent_id.as_str()))
                    .iter()
                    .map(session_stem)
                    .collect();
                if !chain.is_empty() {
                    chains.push(chain);
                }
            }
        }
        if chains.is_empty() {
            return roots.into_iter().map(|root| root.stem).collect();
        }
        let mut seen: HashSet<String> = HashSet::new();
        let mut ordered = Vec::new();
        let mut dropped_legacy = 0usize;
        for root in roots {
            if seen.contains(&root.stem) {
                continue;
            }
            if let Some(chain) = chains.iter().find(|chain| chain.contains(&root.stem)) {
                for link in chain {
                    if seen.insert(link.clone()) {
                        ordered.push(link.clone());
                    }
                }
            } else if self.has_session_id(&root.stem) {
                seen.insert(root.stem.clone());
                ordered.push(root.stem);
            } else {
                dropped_legacy += 1;
            }
        }
        if dropped_legacy > 0 {
            tracing::debug!(
                target: "tinyagents_session::port::drivers",
                thread_id,
                dropped_legacy,
                "[session-store] dropped adopted legacy roots in favour of the session chain"
            );
        }
        ordered
    }

    pub(super) fn list_thread(
        &self,
        thread_id: &str,
        agent_id: Option<&str>,
    ) -> Option<ThreadTranscripts> {
        let thread_id = thread_id.trim();
        if thread_id.is_empty() {
            return None;
        }
        let mut filter = Filter::eq("thread_id", thread_id).and(Filter::eq("subagent", false));
        if let Some(agent_id) = agent_id {
            filter = filter.and(Filter::eq("agent_id", agent_id));
        }
        let roots = self.indexed(filter, "thread_roots");
        if roots.is_empty() {
            return None;
        }
        let prefixes: Vec<String> = roots
            .iter()
            .map(|root| format!("{}__", root.stem))
            .collect();
        let roots = self.ordered_roots(thread_id, roots);
        let mut subagents: Vec<String> = self
            .indexed(Filter::eq("subagent", true), "thread_subagents")
            .into_iter()
            .filter(|sub| {
                prefixes.iter().any(|prefix| sub.stem.starts_with(prefix))
                    || (agent_id.is_none() && sub.thread_id.as_deref() == Some(thread_id))
            })
            .map(|sub| sub.stem)
            .collect();
        subagents.sort();
        subagents.dedup();
        tracing::debug!(
            target: "tinyagents_session::port::drivers",
            thread_id,
            roots = roots.len(),
            subagents = subagents.len(),
            "[session-store] listed thread transcripts"
        );
        Some(ThreadTranscripts {
            roots: roots.into_iter().map(|stem| self.listed(stem)).collect(),
            subagents: subagents
                .into_iter()
                .map(|stem| self.listed(stem))
                .collect(),
        })
    }
}

#[cfg(test)]
#[path = "transcripts_display_tests.rs"]
mod tests;
