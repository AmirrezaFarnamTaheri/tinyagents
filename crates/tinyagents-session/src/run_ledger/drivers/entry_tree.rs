//! Immutable entry records with per-session allocation and native batch admission.

use super::{DriverRunLedger, storage};
use crate::entry_tree::{
    Branch, Entry, EntryId, EntryKind, Fork, ForkPosition, ForkScope, LabelEntry,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tinystoragedrivers_core::{
    DocumentStoreExt, ErrorKind, Filter, Precondition, Query, ReadCheck, StorageError, Version,
    WriteOp,
};

pub(super) const HEADS: &str = "run_ledger_entry_heads";
pub(super) const ENTRIES: &str = "run_ledger_entries";
pub(super) const LABELS: &str = "run_ledger_entry_labels";
pub(super) const INDEXES: &str = "run_ledger_entry_indexes";
type Result<T> = std::result::Result<T, StorageError>;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Head {
    session_id: String,
    next_ordinal: u64,
    pub(super) tip: Option<EntryId>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Node {
    session_id: String,
    entry: Entry,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Named {
    session_id: String,
    branch: Branch,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Index {
    session_id: String,
    tip: EntryId,
    chain: Vec<EntryId>,
}

pub(super) struct Tree {
    pub(super) head: Head,
    version: Option<Version>,
    pub(super) entries: BTreeMap<EntryId, Entry>,
    pub(super) labels: BTreeMap<String, EntryId>,
    indexes: BTreeMap<EntryId, Vec<EntryId>>,
    added: Vec<Entry>,
    named: BTreeSet<String>,
    old_indexes: Vec<EntryId>,
    reindexed: bool,
}

impl Tree {
    pub(super) fn append(
        &mut self,
        parent_id: Option<EntryId>,
        kind: EntryKind,
    ) -> Result<EntryId> {
        if self.head.next_ordinal > i64::MAX as u64 {
            return Err(StorageError::invalid_input(
                "entry ordinal exceeds native signed integer range",
            ));
        }
        let ordinal = self.head.next_ordinal;
        let id = EntryId::derive(&self.head.session_id, ordinal);
        if self.entries.contains_key(&id) {
            return Err(StorageError::conflict(
                "entry allocator collides with an imported id",
            ));
        }
        let entry = Entry {
            id: id.clone(),
            parent_id,
            ordinal,
            kind,
            ts: chrono::Utc::now().to_rfc3339(),
        };
        self.head.next_ordinal += 1;
        self.head.tip = Some(id.clone());
        self.entries.insert(id.clone(), entry.clone());
        self.added.push(entry);
        Ok(id)
    }

    pub(super) fn chain(&self, tip: &EntryId) -> Result<Vec<Entry>> {
        let mut chain = Vec::new();
        let mut visited = BTreeSet::new();
        let mut current = Some(tip.clone());
        while let Some(id) = current {
            if !visited.insert(id.clone()) {
                return Err(StorageError::serialization("entry tree contains a cycle"));
            }
            let entry = self.entries.get(&id).ok_or_else(|| {
                StorageError::not_found(format!("entry tree dangling reference: {id}"))
            })?;
            current = entry.parent_id.clone();
            chain.push(entry.clone());
        }
        chain.reverse();
        Ok(chain)
    }

    pub(super) fn context_chain(&self, tip: &EntryId) -> Result<Vec<Entry>> {
        let live = self.chain(tip)?;
        // Index data never authorizes a different path. Missing/stale projections
        // fall back to the immutable parent chain, as the classic store does.
        if self
            .indexes
            .get(tip)
            .is_some_and(|ids| ids.iter().eq(live.iter().map(|entry| &entry.id)))
        {
            return Ok(self.indexes[tip]
                .iter()
                .map(|id| self.entries[id].clone())
                .collect());
        }
        Ok(live)
    }

    pub(super) fn tips(&self) -> Vec<EntryId> {
        let parents: BTreeSet<_> = self
            .entries
            .values()
            .filter_map(|entry| entry.parent_id.as_ref())
            .collect();
        let mut tips: Vec<_> = self
            .entries
            .values()
            .filter(|entry| !parents.contains(&entry.id))
            .collect();
        tips.sort_by(|a, b| a.ordinal.cmp(&b.ordinal).then(a.id.cmp(&b.id)));
        tips.into_iter().map(|entry| entry.id.clone()).collect()
    }

    pub(super) fn label(&mut self, tip: &EntryId, name: &str) -> Result<EntryId> {
        // Classic SQLite accepts missing parent ids and empty labels too.
        let id = self.append(
            Some(tip.clone()),
            EntryKind::Label(LabelEntry { name: name.into() }),
        )?;
        self.labels.insert(name.into(), id.clone());
        self.named.insert(name.into());
        Ok(id)
    }

    pub(super) fn fork(&mut self, tip: &EntryId, fork: Fork) -> Result<EntryId> {
        let entry = self
            .entries
            .get(tip)
            .ok_or_else(|| StorageError::not_found("fork: unknown tip"))?;
        let target = match fork.position {
            ForkPosition::At => tip.clone(),
            ForkPosition::Before => entry
                .parent_id
                .clone()
                .ok_or_else(|| StorageError::invalid_input("fork before root"))?,
        };
        match fork.scope {
            ForkScope::Branch => Ok(target),
            ForkScope::Tree => {
                let chain = self.chain(&target)?;
                let mut parent = None;
                for entry in chain {
                    parent = Some(self.append(parent, entry.kind)?);
                }
                parent.ok_or_else(|| StorageError::not_found("fork produced no entries"))
            }
        }
    }

    pub(super) fn import(&mut self, entries: &[Entry]) -> Result<()> {
        for entry in entries {
            if self.entries.contains_key(&entry.id) {
                continue;
            }
            if entry.ordinal > i64::MAX as u64 {
                return Err(StorageError::invalid_input(
                    "imported ordinal exceeds native signed integer range",
                ));
            }
            if entry.ordinal >= self.head.next_ordinal {
                self.head.next_ordinal = entry.ordinal + 1;
                self.head.tip = Some(entry.id.clone());
            }
            self.entries.insert(entry.id.clone(), entry.clone());
            self.added.push(entry.clone());
        }
        Ok(())
    }

    pub(super) fn rebuild_index(&mut self) -> Result<()> {
        let mut tips: BTreeSet<_> = self.tips().into_iter().collect();
        tips.extend(self.labels.values().cloned());
        let mut indexes = BTreeMap::new();
        for tip in tips {
            indexes.insert(
                tip.clone(),
                self.chain(&tip)?
                    .into_iter()
                    .map(|entry| entry.id)
                    .collect(),
            );
        }
        self.indexes = indexes;
        self.reindexed = true;
        Ok(())
    }

    fn condition(&self) -> Precondition {
        self.version
            .map(Precondition::Version)
            .unwrap_or(Precondition::Absent)
    }
    fn check(&self) -> ReadCheck {
        ReadCheck {
            collection: HEADS.into(),
            id: storage::key(&[&self.head.session_id]),
            expected: self
                .version
                .map(tinystoragedrivers_core::ExpectedDocument::Version)
                .unwrap_or(tinystoragedrivers_core::ExpectedDocument::Absent),
        }
    }

    fn writes(&self) -> Result<Vec<WriteOp>> {
        let session = &self.head.session_id;
        let mut writes = Vec::new();
        for entry in &self.added {
            writes.push(WriteOp::Put {
                collection: ENTRIES.into(),
                id: storage::key(&[session, entry.id.as_str()]),
                doc: storage::encode(&Node {
                    session_id: session.clone(),
                    entry: entry.clone(),
                })?,
                precondition: Precondition::Absent,
            });
        }
        for name in &self.named {
            writes.push(WriteOp::Put {
                collection: LABELS.into(),
                id: storage::key(&[session, name]),
                doc: storage::encode(&Named {
                    session_id: session.clone(),
                    branch: Branch {
                        name: name.clone(),
                        tip_id: self.labels[name].clone(),
                    },
                })?,
                precondition: Precondition::None,
            });
        }
        if self.reindexed {
            for tip in &self.old_indexes {
                if !self.indexes.contains_key(tip) {
                    writes.push(WriteOp::Delete {
                        collection: INDEXES.into(),
                        id: storage::key(&[session, tip.as_str()]),
                        precondition: Precondition::None,
                    });
                }
            }
            for (tip, chain) in &self.indexes {
                writes.push(WriteOp::Put {
                    collection: INDEXES.into(),
                    id: storage::key(&[session, tip.as_str()]),
                    doc: storage::encode(&Index {
                        session_id: session.clone(),
                        tip: tip.clone(),
                        chain: chain.clone(),
                    })?,
                    precondition: Precondition::None,
                });
            }
        }
        if !writes.is_empty() || self.reindexed {
            writes.push(WriteOp::Put {
                collection: HEADS.into(),
                id: storage::key(&[session]),
                doc: storage::encode(&self.head)?,
                precondition: self.condition(),
            });
        }
        Ok(writes)
    }
}

impl DriverRunLedger {
    pub(super) async fn tree_snapshot(&self, session: &str) -> Result<Tree> {
        self.declare().await?;
        for _ in 0..storage::CAS_ATTEMPTS {
            let found = self
                .scoped
                .documents()
                .get(HEADS, &storage::key(&[session]))
                .await?;
            let version = found.as_ref().map(|row| row.version);
            let head = match found {
                Some(row) => storage::decode::<Head>(row.doc)?,
                None => Head {
                    session_id: session.into(),
                    next_ordinal: 0,
                    tip: None,
                },
            };
            if head.session_id != session {
                return Err(StorageError::serialization("entry head session mismatch"));
            }
            let query = Query::filter(Filter::eq("record.sessionId", session));
            let mut entries = BTreeMap::new();
            for row in self.scoped.documents().query_all(ENTRIES, &query).await? {
                let node: Node = storage::decode(row.doc)?;
                entries.insert(node.entry.id.clone(), node.entry);
            }
            let mut labels = BTreeMap::new();
            for row in self.scoped.documents().query_all(LABELS, &query).await? {
                let named: Named = storage::decode(row.doc)?;
                labels.insert(named.branch.name, named.branch.tip_id);
            }
            let mut indexes = BTreeMap::new();
            for row in self.scoped.documents().query_all(INDEXES, &query).await? {
                let index: Index = storage::decode(row.doc)?;
                indexes.insert(index.tip, index.chain);
            }
            let old_indexes = indexes.keys().cloned().collect();
            let tree = Tree {
                head,
                version,
                entries,
                labels,
                indexes,
                added: Vec::new(),
                named: BTreeSet::new(),
                old_indexes,
                reindexed: false,
            };
            match self
                .scoped
                .documents()
                .atomic_batch_checked(vec![tree.check()], Vec::new())
                .await
            {
                Ok(_) => return Ok(tree),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
        Err(StorageError::conflict(
            "entry tree snapshot contention budget exhausted",
        ))
    }

    pub(super) async fn tree_mutate<T, F>(&self, session: &str, edit: F) -> Result<T>
    where
        T: Send,
        F: Fn(&mut Tree) -> Result<T> + Send + Sync,
    {
        for _ in 0..storage::CAS_ATTEMPTS {
            let mut tree = self.tree_snapshot(session).await?;
            let result = edit(&mut tree)?;
            let writes = tree.writes()?;
            match self
                .scoped
                .documents()
                .atomic_batch_checked(vec![tree.check()], writes)
                .await
            {
                Ok(_) => return Ok(result),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
        Err(StorageError::conflict(
            "entry tree mutation contention budget exhausted",
        ))
    }
}
