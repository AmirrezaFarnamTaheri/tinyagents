# Changelog

## Unreleased: driver stores replace SQLite, file and JSONL backends

Every durable store now runs on `tinystoragedrivers` ports (memory, SQLite,
MongoDB or any other driver the host opens). The bundled SQLite, file and JSONL
implementations are removed and `rusqlite` is no longer a dependency of
session, graph, harness or tasks. Tests may dev-depend on the memory and SQLite
drivers.

### Chat thread store: JSONL removed

- `ConversationStore`, `FileConversationStore` and the `threads::` free
  functions (`append_message`, `list_threads`, ...) are removed.
  `DriverConversationStore` is the only `ConversationPersistence`
  implementation, and now implements `update_message`, `delete_messages_from`
  and `purge_threads` by rewriting the log onto a fresh incarnation and
  swapping the metadata with a compare-and-swap.
- `DriverConversationStore::in_memory()` is the default for library hosts with
  no storage backend.
- `ConversationPurgeStats` moves to `threads::persistence` (same re-exports);
  `ConversationError::Legacy` and `::Worker` are removed.
- Classic `memory/conversations/` files are no longer read by this crate.

### Artifact offload and generated media: blob store

- `artifacts::ArtifactOffload` writes to a tinystoragedrivers `BlobStore`
  (`ArtifactOffload::new(blobs, agent, task)`; `ArtifactOffload::in_memory`
  is the default for library hosts) instead of the filesystem. Pointers carry
  the blob key (`outputs/<name>`); `ArtifactOffload::read` and `::blobs`
  fetch it back. Removed: `ArtifactOffload::root` and `::with_render_root`,
  `OffloadedArtifact::absolute_path`, `OffloadError::Io` and `::SymlinkEscape`
  (new `OffloadError::Storage`), `relative_to_root`, and
  `resolve_artifact_path` (now `resolve_artifact_key`, with no root argument).
- `media::MediaOutput` stores generated images and clips on a blob store
  (`MediaOutput::with_blobs`, in-memory by default) under `generated-media/`;
  tool results report the blob key as `path`. Local reference inputs still
  resolve against the workspace. `rustix` is no longer a dependency.
- Hosts that let the model open these artifacts by file path must expose the
  store through their read tool or materialise it themselves.

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

### Turn-state files and the directory attachment stash removed

- `TurnStateStore` (the per-turn JSON files under `memory/conversations/turn_states/`)
  is removed. `TurnStates` is implemented by `InMemoryTurnStates` and
  `DriverTurnStates`; a library host with no session provider gets the memory
  store by default.
- `TurnStateMirror::new` takes an `Arc<dyn TurnStates>` instead of a
  `TurnStateStore`. An interrupted turn's partial answer is carried into the
  transcript only when the host attaches a locator with
  `TurnStateMirror::with_transcripts`; otherwise it stays in the snapshot.
- `multimodal::AttachmentStash` (a directory of files) is removed. Use
  `BlobAttachmentStash` over blob and document ports, which is also the
  implementation hosts with no storage URL should use over the memory driver.
  Retrieval is by id (`read`, `read_data_uri`, `build_index`); there is no
  path to hand a file reader, so `managed_path` and path-keyed `write` results
  are gone.
- The classic turn-state layout is still read, never written, by
  `session::legacy::import_workspace`.

### Not yet moved

These still use files and are migrated in follow-ups (tracked in
tinyhumansai/openhuman#7688, #7768 and #7783):

- the JSONL transcript writer, reader and `FileTranscriptLocator`
  (path-bound; `session_raw/` adoption, spend and view helpers read them,
  about 12k lines across `session::transcript`);
- `artifacts::tool_results::ToolResultArtifactStore`, whose pointers are
  action-relative or absolute file paths the host's `file_read` opens, and
  whose pruning relies on file mtimes (a blob has no modification time).
