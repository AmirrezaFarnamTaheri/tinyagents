# `session::threads` — chat thread and message store

A workspace-backed store for the product-facing chat log: the threads a UI
lists (title, labels, parent, personality, message count, last activity) and
the messages it renders inside each one. It also keeps an in-memory inverted
index for substring search across every thread, and a small subscriber that
mirrors channel turns (Slack, Telegram, ...) into the same store.

The classic store is plain files plus `serde`. The optional `storage-drivers`
feature additionally exposes explicitly admitted native scoped ports; neither
path implements a memory engine.

## Where it came from

This module was moved verbatim from TinyMemory's `tinymemory-conversations`
crate. Chat history is session data, not memory, so it lives with the other
session persistence here. Nothing about the format changed:

- the root and metadata file names are the same; short thread IDs retain the
  same per-thread hex naming, while long IDs use bounded hash names;
- every serde attribute (`camelCase` fields, the `type` and `extraMetadata`
  wire keys, the `op`-tagged log entries) is the same;
- every public name is the same except two types, renamed because
  TinyAgents' dependency-boundary guard
  (`tinyagents-integration-tests/tests/dependency_boundary.rs`) reserves
  `ConversationMessage` and `ConversationMessagePatch` as OpenHuman domain
  type names that host-independent crates must not use. Type names never
  reach the disk, so the rename does not affect the format.

A host switches by changing the import path and the two type names:

| Old | New |
| --- | --- |
| `tinymemory_conversations::X` | `tinyagents_session::threads::X` |
| `ConversationMessage` | `ThreadMessage` |
| `ConversationMessagePatch` | `ThreadMessagePatch` |

Every other name (`ConversationStore`, `ConversationThread`,
`CreateConversationThread`, `ConversationPurgeStats`, `CrossThreadHit`, and
the free functions) is unchanged. The `bus` persistence subscriber was
not carried over: hosts keep their own (OpenHuman's lives in
`openhuman-core`'s `threads::store::bus`). A host that wants to keep
its own spelling can import `ThreadMessage as ConversationMessage`.

The store, its wire types, and `CrossThreadHit` are also re-exported from the
crate root (`tinyagents_session::ConversationStore`, ...). The free functions
(`list_threads`, `append_message`, ...) stay under `threads::` so they are
not confused with the SQLite session operations at the root.

Legacy errors stay `Result<_, String>` rather than the crate's `TinyAgentsError`.
That keeps the switch a pure path change for existing callers; converting
the error type is a separate API change.

`store/mod_compat_tests.rs` holds a fixture in the old crate's exact byte
layout and proves it loads, re-encodes byte-for-byte, accepts appends in the
same format, and is searchable.

## Explicit async persistence

The opt-in object-safe ConversationPersistence contract covers thread/message
operations and exact substring search. FileConversationStore::new(workspace)
adapts the existing JSONL implementation through Tokio's blocking pool; it
preserves labels, deterministic/channel message deduplication, tail cuts,
recreation and the local CJK index. It creates no files at construction.
Its reads may repair classic stats, so it is not a read-only import reader.

ConversationError distinguishes Legacy(String), unavailable authoritative
operations, blocking worker failures, and native driver errors under the
storage-drivers feature. SessionStoreProvider::conversations_for_agent returns
a typed optional shared handle without changing existing AgentStores literals.
Providers bind trusted conversation scope; thread IDs and payload metadata
do not grant authority. A configured refusal must not select file persistence.

Turn claim, renew, complete and release currently return typed Unsupported
by default, including in the file adapter. Their contract requires durable
completed-request deduplication and exact owner/epoch/expiry validation; a
process mutex or lease precheck does not establish distributed turn ownership.
With storage-drivers, DriverConversationStore::new(ScopedStorage) explicitly
accepts an already-admitted profile scope. Single-document metadata CAS covers
ensure/title/labels/working directory/tombstone; recreation opens a fresh UUID
incarnation. Bounded backend pages read immutable committed message prefixes and
revalidate metadata before returning. Search rebuilds a temporary owning index
from exhaustive live backend reads, preserving local substring/CJK ranking.
It does not use capped FullText or an authoritative process cache.

### Explicit schema preparation and actor admission

`DriverConversationStore::collection_specs()` publishes the existing bare
`conversation_threads_v1` declaration. Trusted bootstrap calls
`DriverConversationStore::prepare_schema(documents)` on its raw native document
port. Native internal stream/database preparation remains a separate host step.
Restricted native handles refuse this declaration helper.

`new_validate_only(scoped)` captures immutable validation-only behavior and
validates required persisted features on every implemented operation, including
reads and empty searches. Validation success is not cached. Native compatible
supersets are accepted: this is required-feature validation, not exact schema
equality. Native failure kind and original source are retained. The caller must
supply existing-schema native handles with its original actor fence; selecting
this constructor alone does not restrict backend lazy initialization or grant
scope authority. Default turn-claim refusals perform no native operation.

The existing `new(scoped)` remains deliberately compatible and does not declare
or validate schema. It must not be advertised as actor schema safety. Neither
constructor selects a provider automatically, opens host files or enables imports.
Plain append requires the additional preparation below; metadata/stream encoding
remains schema1.

### Native plain append and recovery

`append_collection_specs()` adds `conversation_appends_v1` to the existing
metadata declaration. Explicit trusted `prepare_append_schema(documents)`
prepares both; metadata-only readers retain their previous schema requirements.
Every append validates both declarations without actor DDL. It requires native
checked document batches and conditional stream append. An actual empty
conditional append probes the paired stream before reserving any intent: it
adds no message, but may affect native internal stream bookkeeping. A refusal
never selects file persistence.

Reservation atomically checks the observed thread revision and absent pending
intent. The intent holds an immutable attempt token, incarnation, expected
stream offset and complete serialized message. All envelope fields are strings;
compatible numeric TTL declarations cannot expire these fields. Writer-only
recovery appends or verifies the exact row, then atomically publishes the next
committed prefix and removes that exact intent version. Readers never recover
pending work or expose an uncommitted row. Metadata count/last activity follow
classic semantics, and concurrent unrelated title changes are retained.

Cancellation may leave accepted pending work for a later admitted writer.
An unknown acknowledgment is resolved only through durable ownership evidence:
matching row bytes alone cannot attribute an unobserved ordinary-ID reservation.
`ConversationError::Uncertain` preserves the original native error and source
when ownership remains unresolved; callers must not silently retry such ordinary
UUID appends. Deterministic `agent:` IDs or channel metadata retain classic
existing-ID deduplication and return the original stored message. Ordinary IDs
still append on each separate invocation; this is not a generic exactly-once API.

Message edits, tail cuts, purge and distributed turn claims remain Unsupported.
Plain message append does not authorize distributed agent execution. No importer,
provider selection, host opt-in or legacy source-ownership grant is added.

## File layout

Every legacy file entry point takes the **workspace directory** and derives the root:

```text
{workspace_dir}/memory/conversations/
├── threads.jsonl                      ← append-only thread metadata log
└── threads/
    ├── {hex-or-sha256(thread_id)}.jsonl ← one message log per thread
    └── .conversations-{uuid}.tmp      ← transient, during atomic rewrites
```

- **`threads.jsonl`** is a log, not a table. Each line is one
  `{"op": ...}` entry, folded in order into the current thread state:
  - `upsert` — create or update a thread (`title`, `created_at`,
    `updated_at`, optional `parent_thread_id`, `labels`, `personality_id`).
    `labels: Some` replaces the label set; `None` keeps it, or infers one
    for a new thread (`briefing`, `notification`, or `general`).
  - `delete` — tombstone; the thread disappears from the fold.
  - `message_appended` — bumps the message count by one and sets
    `last_message_at`, so listing threads never reads message files.
  - `stats` — an absolute count/timestamp snapshot. Written to backfill
    threads whose messages predate `message_appended`, and to repair the
    trail after a crash between the two appends.
- **`threads/{hex-or-sha256}.jsonl`** holds one `ThreadMessage` per line
  (`id`, `content`, `type`, `extraMetadata`, `sender`, `createdAt`). The
  filename is the lowercase hex of the thread id's UTF-8 bytes for IDs up to
  124 bytes. Longer IDs use `sha256-` followed by the lowercase hex SHA-256
  digest of those bytes, keeping the filename within common filesystem limits.
- Appends are `O_APPEND` + `fsync`. Edits that must change existing lines
  (`update_message`, `delete_messages_from`) write a sibling temp file,
  `fsync` it, and rename it over the original, so a crash never leaves a
  half-written transcript.
- Readers skip blank and unparseable lines, so one corrupt line never loses
  the rest of a thread.

Message ids with the `agent:` prefix (`run_reply_message_id`) are minted
deterministically and may be presented twice by two writers; `append_message`
returns the stored message instead of writing a duplicate. Channel events use
`{role}:{message_id}` IDs with `extraMetadata.scope` set to `"channel"`;
`append_message` also returns the stored message on redelivery. Other IDs are
UUID-fresh and append without a lookup.

## Concurrency

Each conversation root has one lock set, shared by every `ConversationStore`
handle that points at it (roots are canonicalized first, so relative and
symlinked paths agree). The registry holds `Weak` references, so temporary
workspaces do not leak lock sets.

| Lock | Kind | Guards |
| --- | --- | --- |
| lifecycle | `RwLock` per root | read by every operation; write by `purge_threads` |
| thread | `Mutex` per thread id | that thread's message file |
| metadata | `Mutex` per root | reads and appends of `threads.jsonl` |
| index build | `Mutex` per root | only one cold index build at a time |

**Lock order:** lifecycle → thread → metadata → index cache. No code takes a
thread or metadata lock while holding the global index cache lock. So writers
on different threads append concurrently, and only the short
`threads.jsonl` append is serialized per root.

All of this is in-process. Two processes writing the same workspace are not
coordinated.

## The cross-thread index

`search_cross_thread_messages(query, limit, exclude_thread_id)` answers from
an in-memory inverted index over message content:

- **Tokenizing** (`tokenize.rs`): normalize (lowercase, strip combining
  marks and Latin diacritics, fold full-width ASCII), then index character
  bigrams for runs of CJK text and trigrams for everything else.
- **Querying** (`inverted_index.rs`): intersect the posting lists of each
  query term's n-grams to get candidates, then verify each candidate with an
  exact substring match on the normalized content. Score is
  `matched_terms / total_terms`, newest first on ties. Terms shorter than
  three bytes are dropped. A query whose candidate set is very large returns
  recent hits without the verify pass, to cap latency.
- **Lifetime:** the index is never written to disk. It is built from the
  JSONL files the first time a process searches a root, cached per root for
  the life of the process, and updated in place by appends, edits, deletes,
  and purges. The JSONL files are always the source of truth.
- **Cold build without stalls:** the build snapshots live thread ids under
  the metadata lock (no message I/O), releases it, reads each thread under
  its own thread lock, and journals any append that lands meanwhile. The
  journal is folded in when the built index is published, so no message is
  missed and unrelated writers are never blocked by the scan.

## Public surface

- **Store:** `ConversationStore` (`new`, `ensure_thread`, `list_threads`,
  `get_messages`, `append_message`, `update_thread_title`,
  `update_thread_labels`, `update_message`, `delete_messages_from`,
  `delete_thread`, `purge_threads`, `search_cross_thread_messages`) and
  `ConversationPurgeStats`.
- **Free functions:** the same operations taking a workspace path —
  `ensure_thread`, `list_threads`, `get_messages`, `append_message`,
  `update_thread_title`, `update_thread_labels`, `update_message`,
  `delete_messages_from`, `delete_thread`, `purge_threads`.
- **Types:** `ConversationThread`, `ThreadMessage`,
  `CreateConversationThread`, `ThreadMessagePatch`, `CrossThreadHit`.
- **Deterministic ids:** `DETERMINISTIC_MESSAGE_ID_PREFIX`,
  `run_reply_message_id`, `is_deterministic_message_id`, `reply_run_id`.

A transient failure while helping another writer's pending intent returns the
original native driver error: the caller's own message has not yet been reserved
and can be retried. `Uncertain` describes only the caller's own accepted-attempt
ambiguity. Definitive reconciliation corruption/fencing errors remain typed;
unrelated title or label revisions do not undo an already published append.

## Files

| File | Role |
| --- | --- |
| `mod.rs` | module docs and re-exports |
| `persistence.rs` | explicit async port, typed failures and unsupported turn claims |
| `driver.rs` | admitted native metadata CAS, schema policies and committed-prefix readers |
| `driver/append.rs` | writer-only conditional append and checked publication |
| `driver/pending.rs` | all-string attempt intent and strict pending-record validation |
| `types.rs` | wire types and deterministic message ids |
| `tokenize.rs` | normalization and n-gram tokenizer |
| `inverted_index.rs` | in-memory search index |
| `store/mod.rs` | `ConversationStore`, log entries, JSONL helpers, free functions |
| `store/ops.rs` | public CRUD and search methods |
| `store/index.rs` | log folding, stat repair, cold index build |
| `store/locks.rs` | per-root lock registry |
| `*_tests.rs` | unit tests; `store/mod_compat_tests.rs` is the legacy-format fixture |
