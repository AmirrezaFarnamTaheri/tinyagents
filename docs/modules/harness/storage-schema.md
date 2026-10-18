# Harness storage schema v2


`tinyagents_harness::storage_schema()` declares what the harness stores for the
host's `Registry::compose` (owner `harness`, scope class Agent):

| v1 | v2 |
|---|---|
| `harness_run_status` (`{runId, rootRunId, threadId, active, record}`) | `harness.run_status`, id = run id, snake_case body with `*_ms` times |
| `harness_embedding_cache` | `harness.embedding_cache`, id `Key::of([signature, key])` |
| `attachment_stash_index` (`touchedAtMs`) | `harness.attachment_stash` (`touched_ms`) |
| blobs `attachments/`, `tool-results/`, `outputs/`, `workspace/`, `generated-media/` | blobs `harness/attachments/`, `harness/tool_results/`, `harness/outputs/`, `harness/workspace/`, `harness/generated_media/` |
| stream `16:session_journal/<run>` | stream `harness.journal/<run>`; entries drop `run_id`, `root_run_id` and `event_id` |
| `harness_store`, `harness_namespaced_store`, `harness_response_cache` | `tsd.kv` (owner `harness`; namespaces `namespaced:…` and `cache:…` for the latter two) |

Document and blob steps run with `Registry::migrate`. A step may only write its
own owner's collections, so the key-value stores move with
`tinyagents_harness::storage::import_legacy_kv(storage, owner, legacy_collection,
LegacyKv::{Flat, Namespaced, ResponseCache})`, which the host calls once per
scope (it records completion in `tsd.migrations`). `DriverStore` also serves the
session crate's key-value store under owner `session` (`session_kv`, `LegacyKv::Flat`).
A transcript pointer written before the move (`tool-results/…`, `outputs/…`,
`workspace/…`) still resolves.

## Run journal

`DriverAppendStore::journal(streams)` writes each run to `harness.journal/<run
id>` (the `<len>:` name scheme is gone; `with_prefix` joins the prefix and name
as they are). `StoreEventJournal` stores a `JournalEntry` rather than the
`AgentObservation` wire type:

```json
{"offset": 7, "ts_ms": 1700000000007, "event": {"kind": "tool_started", "...": "..."}}
```

The run id is the stream name and the position is the offset, so neither is
stored. `offset` is the emitting sink's counter, which the event id
`{run id}-evt-{offset}` is built from, so ids are byte-identical to the v1 ones
(it can run ahead of the stream position when a bounded sink dropped events).
`root_run_id` and `event_id` are stored only when they differ from that
derivation (a child run's root; an id minted by a sink with another stream id),
so nothing is lost. A v1 entry that was never migrated still reads.

The family is declared `SealPolicy::new(SealWhen::OnFinish).retaining_ms(30
days)`. The owner seals a finished run with `HarnessEventJournal::seal_run`
(`AppendStore::seal` underneath); `reap_orphaned_runs_and_seal` seals the runs
a restart left behind. A sealed stream reads exactly as before (the driver
keeps it as one compressed blob) and refuses appends; the host's
`Registry::maintain` deletes it once the retention has passed.

The step `run_journal_v2` moves `16:session_journal/<run>` (the session crate's
v1 prefix behind the old length prefix) to `harness.journal/<run>`, rewriting
each entry in place. Journals are never truncated, so the migrated stream
numbers from 0 like the source and every offset and event id is unchanged; a
sealed source gives a sealed target. An entry that is not an observation of its
run is copied unchanged so no later offset moves.
