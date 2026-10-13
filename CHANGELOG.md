# Changelog

## Unreleased: driver stores replace SQLite, file and JSONL backends

Every durable store now runs on `tinystoragedrivers` ports (memory, SQLite,
MongoDB or any other driver the host opens). The bundled SQLite, file and JSONL
implementations are removed and `rusqlite` is no longer a dependency of
session, graph, harness or tasks. Tests may dev-depend on the memory and SQLite
drivers.

### Removed, with the replacement

| Removed | Use instead |
| --- | --- |
| `FileCheckpointer`, `SqliteCheckpointer` (graph) | `DriverCheckpointer` |
| `SqliteTaskCache` (graph) | `DriverTaskCache` |
| `SqliteResponseCache` (harness) | `DriverResponseCache` |
| `FileStore`, `JsonlAppendStore` (harness) | `DriverStore`, `DriverAppendStore` |
| harness disk embedding cache, file status store | `DriverEmbeddingCache`, `DriverStatusStore` |
| `JsonlTaskStore`, `JsonlCompletionStore` (tasks) | `DriverTaskStore`, `DriverCompletionStore` |
| session SQLite functions (`record_session_start`, `search_sessions`, `apply_retention`, `with_connection`, ...), `run_ledger` free functions and `sqlite` ledger | `DriverRunLedger` through `SessionHistoryStore` and `RunLedgerStore` |
| `session::transcript::import` (ledger import) | `session::legacy` |

The `sqlite` cargo features on harness and graph remain as no-ops so existing
manifests keep resolving, and `storage-drivers` is a no-op because the driver
stores are now unconditional.

### Signature changes

- `JsonlSink::new` takes an `Arc<dyn AppendStore>`.
- `SessionTeamLedger::new`, `SessionWorkflowStore::new` and
  `SessionCompactionSink::new` take an `Arc<dyn RunLedgerStore>`.
- `testkit::conformance::run_ledger_conformance` is async and takes an
  `Arc<dyn RunLedgerStore>`.
- Transcript view projection takes an optional `SpawnCallLookup` instead of a
  workspace directory to look spawn calls up in the ledger.

### Added

- `DriverRunLedger`: team-task claim, complete and shutdown, `prune_*`,
  `trim_session_messages`, `apply_retention` and `reindex_fts`, plus the
  `SessionHistoryStore` trait (sessions, messages, tool calls, full-text search).
- Driver implementations of `TaskCache`, `ResponseCache`, `NamespacedStore`,
  `EmbeddingCache`, `ToolEffectLedger`, `TaskStore`, `CompletionStore` and the
  status store; a `BlobAttachmentStash` over `BlobStore`.

### Migrating legacy data

The `legacy` module (feature `legacy-import`, on by default) reads a classic
workspace read-only and writes it into the driver stores. Rows keep their ids,
existing documents are never overwritten, an interrupted import resumes, and
the search index is rebuilt at the end.

```rust
use tinyagents_session::legacy::import_workspace;

let report = import_workspace(&provider, "agent-id", workspace_dir).await?;
```

- `sessions.db` is opened immutable and read-only. It must be quiescent: a
  database with a WAL or journal sidecar is refused with
  `SourceRequiresSnapshot`, so checkpoint or copy it first.
- `session_raw/*.jsonl` transcripts and turn-state JSON are replayed through
  the same only-if-absent path.
- `DriverRunLedger::import_legacy_database` imports one database directly.

### Not yet moved

The path-bound JSONL transcript, turn-state-file and thread stores, and the
artifact and media offload, still use files and are migrated in a follow-up.
