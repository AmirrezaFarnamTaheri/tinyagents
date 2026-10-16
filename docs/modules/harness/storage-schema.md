# Harness storage schema v2


`tinyagents_harness::storage_schema()` declares what the harness stores for the
host's `Registry::compose` (owner `harness`, scope class Agent):

| v1 | v2 |
|---|---|
| `harness_run_status` (`{runId, rootRunId, threadId, active, record}`) | `harness.run_status`, id = run id, snake_case body with `*_ms` times |
| `harness_embedding_cache` | `harness.embedding_cache`, id `Key::of([signature, key])` |
| `attachment_stash_index` (`touchedAtMs`) | `harness.attachment_stash` (`touched_ms`) |
| blobs `attachments/`, `tool-results/`, `outputs/`, `workspace/`, `generated-media/` | blobs `harness/attachments/`, `harness/tool_results/`, `harness/outputs/`, `harness/workspace/`, `harness/generated_media/` |
| `harness_store`, `harness_namespaced_store`, `harness_response_cache` | `tsd.kv` (owner `harness`; namespaces `namespaced:…` and `cache:…` for the latter two) |

Document and blob steps run with `Registry::migrate`. A step may only write its
own owner's collections, so the key-value stores move with
`tinyagents_harness::storage::import_legacy_kv(storage, owner, legacy_collection,
LegacyKv::{Flat, Namespaced, ResponseCache})`, which the host calls once per
scope (it records completion in `tsd.migrations`). `DriverStore` also serves the
session crate's key-value store under owner `session` (`session_kv`, `LegacyKv::Flat`).
A transcript pointer written before the move (`tool-results/…`, `outputs/…`,
`workspace/…`) still resolves.
