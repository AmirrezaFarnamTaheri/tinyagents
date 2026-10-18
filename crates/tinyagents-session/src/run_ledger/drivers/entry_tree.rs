//! Immutable entry records with per-session allocation and native batch admission.

use super::dto::{Stored, stored_as_is};
use super::storage::{self, key, put_op};
use super::{DriverRunLedger, now};
use crate::entry_tree::{
    Branch, Entry, EntryId, EntryKind, Fork, ForkPosition, ForkScope, LabelEntry,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tinystoragedrivers_core::{
    DocumentStoreExt, ErrorKind, Filter, Precondition, Query, ReadCheck, StorageError, Version,
    WriteOp,
};

pub(super) const HEADS: &str = "ledger.entry_heads";
pub(super) const ENTRIES: &str = "ledger.entries";
pub(super) const LABELS: &str = "ledger.entry_labels";
pub(super) const INDEXES: &str = "ledger.entry_indexes";
type Result<T> = std::result::Result<T, StorageError>;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Head {
    session_id: String,
    next_ordinal: Option<u64>,
    next_insertion: Option<u64>,
    pub(super) tip: Option<EntryId>,
}

/// An entry with the allocation order it was inserted in.
struct Node {
    session_id: String,
    insertion: u64,
    entry: Entry,
}

/// `ledger.entries`: the entry's own fields beside its session and insertion
/// order. `ts_ms` is the entry's RFC 3339 `ts` as epoch milliseconds; an empty
/// or unparseable `ts` is stored as absent.
#[derive(Serialize, Deserialize)]
struct NodeDto {
    session_id: String,
    insertion: u64,
    id: EntryId,
    parent_id: Option<EntryId>,
    ordinal: u64,
    kind: EntryKind,
    ts_ms: Option<i64>,
}

impl Stored for Node {
    type Dto = NodeDto;

    fn to_dto(&self) -> NodeDto {
        NodeDto {
            session_id: self.session_id.clone(),
            insertion: self.insertion,
            id: self.entry.id.clone(),
            parent_id: self.entry.parent_id.clone(),
            ordinal: self.entry.ordinal,
            kind: self.entry.kind.clone(),
            ts_ms: ts_to_ms(&self.entry.ts),
        }
    }

    fn from_dto(dto: NodeDto) -> Result<Self> {
        Ok(Self {
            session_id: dto.session_id,
            insertion: dto.insertion,
            entry: Entry {
                id: dto.id,
                parent_id: dto.parent_id,
                ordinal: dto.ordinal,
                kind: dto.kind,
                ts: ts_from_ms(dto.ts_ms)?,
            },
        })
    }
}

/// The epoch milliseconds of an RFC 3339 entry timestamp.
pub(super) fn ts_to_ms(ts: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|at| at.timestamp_millis())
}

/// An entry timestamp from epoch milliseconds, in RFC 3339 with a `Z` offset.
pub(super) fn ts_from_ms(ms: Option<i64>) -> Result<String> {
    ms.map(|ms| {
        super::dto::from_ms(ms).map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
    })
    .transpose()
    .map(Option::unwrap_or_default)
}

/// A named branch.
struct Named {
    session_id: String,
    branch: Branch,
}

/// `ledger.entry_labels`: the branch's name and tip beside its session.
#[derive(Serialize, Deserialize)]
struct NamedDto {
    session_id: String,
    name: String,
    tip_id: EntryId,
}

impl Stored for Named {
    type Dto = NamedDto;

    fn to_dto(&self) -> NamedDto {
        NamedDto {
            session_id: self.session_id.clone(),
            name: self.branch.name.clone(),
            tip_id: self.branch.tip_id.clone(),
        }
    }

    fn from_dto(dto: NamedDto) -> Result<Self> {
        Ok(Self {
            session_id: dto.session_id,
            branch: Branch {
                name: dto.name,
                tip_id: dto.tip_id,
            },
        })
    }
}

/// `ledger.entry_indexes`: the ancestor chain of one tip.
#[derive(Clone, Serialize, Deserialize)]
struct Index {
    session_id: String,
    tip: EntryId,
    chain: Vec<EntryId>,
}

stored_as_is!(Head, Index);

pub(super) struct Tree {
    pub(super) head: Head,
    version: Option<Version>,
    pub(super) entries: BTreeMap<EntryId, Entry>,
    insertion: BTreeMap<EntryId, u64>,
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
        let ordinal = self
            .head
            .next_ordinal
            .ok_or_else(|| StorageError::invalid_input("entry ordinal exhausted"))?;
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
            ts: now().to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        };
        self.head.next_ordinal = (ordinal < i64::MAX as u64).then_some(ordinal + 1);
        self.head.tip = Some(id.clone());
        self.remember_insertion(&id)?;
        self.entries.insert(id.clone(), entry.clone());
        self.added.push(entry);
        Ok(id)
    }

    fn remember_insertion(&mut self, id: &EntryId) -> Result<()> {
        let insertion = self
            .head
            .next_insertion
            .ok_or_else(|| StorageError::invalid_input("entry insertion sequence exhausted"))?;
        self.insertion.insert(id.clone(), insertion);
        self.head.next_insertion = (insertion < i64::MAX as u64).then_some(insertion + 1);
        Ok(())
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
        tips.sort_by(|a, b| {
            a.ordinal
                .cmp(&b.ordinal)
                .then(self.insertion[&a.id].cmp(&self.insertion[&b.id]))
        });
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

    /// Add `entries` not already present, preserving their ids and ordinals;
    /// returns how many were new.
    pub(super) fn import(&mut self, entries: &[Entry]) -> Result<usize> {
        let mut added = 0;
        for entry in entries {
            if self.entries.contains_key(&entry.id) {
                continue;
            }
            added += 1;
            if entry.ordinal > i64::MAX as u64 {
                return Err(StorageError::invalid_input(
                    "imported ordinal exceeds native signed integer range",
                ));
            }
            if self
                .head
                .next_ordinal
                .is_some_and(|next| entry.ordinal + 1 >= next)
                || (self.head.next_ordinal.is_none() && entry.ordinal == i64::MAX as u64)
            {
                self.head.next_ordinal =
                    (entry.ordinal < i64::MAX as u64).then_some(entry.ordinal + 1);
                self.head.tip = Some(entry.id.clone());
            }
            self.remember_insertion(&entry.id)?;
            self.entries.insert(entry.id.clone(), entry.clone());
            self.added.push(entry.clone());
        }
        Ok(added)
    }

    /// Record a named branch label unless the tree already has one by that name.
    /// Returns whether it was new.
    #[cfg(feature = "legacy-import")]
    pub(super) fn import_label(&mut self, name: &str, tip: &EntryId) -> bool {
        if self.labels.contains_key(name) {
            return false;
        }
        self.labels.insert(name.into(), tip.clone());
        self.named.insert(name.into());
        true
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
    fn check(&self) -> Result<ReadCheck> {
        Ok(ReadCheck {
            collection: HEADS.into(),
            id: key(&[&self.head.session_id])?,
            expected: self
                .version
                .map(tinystoragedrivers_core::ExpectedDocument::Version)
                .unwrap_or(tinystoragedrivers_core::ExpectedDocument::Absent),
        })
    }

    fn writes(&self) -> Result<Vec<WriteOp>> {
        let session = &self.head.session_id;
        let mut writes = Vec::new();
        for entry in &self.added {
            writes.push(put_op(
                ENTRIES,
                key(&[session, entry.id.as_str()])?,
                &Node {
                    session_id: session.clone(),
                    insertion: self.insertion[&entry.id],
                    entry: entry.clone(),
                },
                Precondition::Absent,
            )?);
        }
        for name in &self.named {
            writes.push(put_op(
                LABELS,
                key(&[session, name])?,
                &Named {
                    session_id: session.clone(),
                    branch: Branch {
                        name: name.clone(),
                        tip_id: self.labels[name].clone(),
                    },
                },
                Precondition::None,
            )?);
        }
        if self.reindexed {
            for tip in &self.old_indexes {
                if !self.indexes.contains_key(tip) {
                    writes.push(WriteOp::Delete {
                        collection: INDEXES.into(),
                        id: key(&[session, tip.as_str()])?,
                        precondition: Precondition::None,
                    });
                }
            }
            for (tip, chain) in &self.indexes {
                writes.push(put_op(
                    INDEXES,
                    key(&[session, tip.as_str()])?,
                    &Index {
                        session_id: session.clone(),
                        tip: tip.clone(),
                        chain: chain.clone(),
                    },
                    Precondition::None,
                )?);
            }
        }
        if !writes.is_empty() || self.reindexed {
            writes.push(put_op(
                HEADS,
                key(&[session])?,
                &self.head,
                self.condition(),
            )?);
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
                .get(HEADS, &key(&[session])?)
                .await?;
            let version = found.as_ref().map(|row| row.version);
            let head = match found {
                Some(row) => storage::decode::<Head>(row.doc)?,
                None => Head {
                    session_id: session.into(),
                    next_ordinal: Some(0),
                    next_insertion: Some(0),
                    tip: None,
                },
            };
            if head.next_ordinal.is_some_and(|next| next > i64::MAX as u64)
                || head
                    .next_insertion
                    .is_some_and(|next| next > i64::MAX as u64)
            {
                return Err(StorageError::serialization(
                    "entry head exceeds native signed integer range",
                ));
            }
            if head.session_id != session {
                return Err(StorageError::serialization("entry head session mismatch"));
            }
            let query = Query::filter(Filter::eq("session_id", session));
            let mut entries = BTreeMap::new();
            let mut insertion = BTreeMap::new();
            for row in self.scoped.documents().query_all(ENTRIES, &query).await? {
                let node: Node = storage::decode(row.doc)?;
                insertion.insert(node.entry.id.clone(), node.insertion);
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
                insertion,
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
                .atomic_batch_checked(vec![tree.check()?], Vec::new())
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
                .atomic_batch_checked(vec![tree.check()?], writes)
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
