# harness::store

Long-term key-value and append-only stream storage backends for the harness
runtime.

## Why this exists

In the recursive architecture the store is the durable, shared substrate that
outlives any single run: parent and child runs, sub-agents, and
nested executions read and write the same namespaced values, so a
deeply nested call can persist a result a sibling or a later turn picks up.
It is the harness-side persistence layer for runtime data — events, model and
tool call records, message history, artifacts, memory — and is intentionally
separate from graph checkpointing (which belongs to `tinyagents-graph`'s
`checkpoint` module) and from prompt/model context assembly (`prompt`,
`context`).

## Public surface

### Flat key-value: [`Store`]

The original get/put/delete/list trait over a flat `&str` namespace.

- [`InMemoryStore`] — thread-safe, non-durable, for tests and prototyping.
- [`DriverStore`] — durable store over a driver `DocumentStore`; the host's
  backend holds the data. The file-per-key `FileStore` was removed.
- [`StoreRegistry`] — a named bag of `Arc<dyn Store>` backends, always
  carrying a built-in default in-memory store, injected into `RunContext`.

### Append-only streams: [`AppendStore`]

Where `Store` answers "what is the current value?", an `AppendStore` answers
"what happened, in order?" — the durable backbone for event journals.

- [`InMemoryAppendStore`] — thread-safe, non-durable, with an optional
  per-stream retention cap (`with_max_entries_per_stream`); offsets stay
  monotonic across eviction.
- [`DriverAppendStore`] — durable streams over a driver `StreamStore`. Offsets
  are allocated by the backend, so several instances over one database keep
  handing out fresh offsets. The JSONL file store was removed.
- [`StoreRecord`] — a decoded entry (`offset`, `value`, `created_at_ms`).

### Hierarchical store: [`namespaced`]

The richer, TTL-aware, batch-oriented sibling trait — see its own README.

## Files

| File | Role |
| --- | --- |
| `mod.rs` | Implementations of `Store` and `AppendStore` for every backend, plus `StoreRegistry`. |
| `types.rs` | The `Store`/`AppendStore` traits and every backend struct/field. |
| `namespaced/` | `NamespacedStore` — hierarchical namespaces, TTL, filtering, pagination, batching. See `namespaced/README.md`. |
| `mod_tests.rs` | Coverage for every backend: round-tripping, namespace isolation, sanitization, atomicity, offset monotonicity/tailing, retention eviction, and driver round-trips. |

## Operational constraints

- The driver stores take any name: ids are hashed or length-prefixed by the
  driver layer, so a hostile namespace, key or stream name cannot reach a path.
- Backends that lack native TTL are served by the read-side expiry check; see
  `namespaced/README.md`.
- None of the in-memory backends are durable: data is lost when the value is
  dropped.

## Storage-driver backends (`storage-drivers` feature)

`DriverStore` and `DriverAppendStore` adapt the harness `Store` and
`AppendStore` traits onto [tinystoragedrivers](https://github.com/tinyhumansai/tinystoragedrivers)
ports. A host opens one storage backend (SQLite on desktop, MongoDB in the cloud,
memory in tests), takes a scoped `DocumentStore` / `StreamStore` for the tenant,
and wraps it:

```rust,ignore
let scoped = backend.for_scope(&Scope::new(agent_id)?)?;
let kv = DriverStore::new(scoped.documents().clone());
let journal = DriverAppendStore::with_prefix(scoped.streams().clone(), "journal/");
```

- `DriverStore` keeps every namespace in one collection (`harness_store` by
  default) as `{ns, key, value}` documents, so namespaces and keys may hold any
  characters, and `list` is an indexed query.
- `DriverAppendStore` maps each stream to a driver stream (optionally
  prefixed). Driver offsets are already dense and zero-based, matching the
  `AppendStore` contract.
- Driver `InvalidInput` errors surface as `TinyAgentsError::Validation`. Every
  other driver error surfaces as `TinyAgentsError::Storage`.

## Prepared document schemas

With `storage-drivers`, `SchemaAdmission::Declare` keeps native collection
creation as the default. `DriverStore::with_collection_and_schema_admission`
can instead select `ValidateOnly` at construction. Both policies use the exact
`DriverStore::collection_spec`; validation calls the native persisted-schema
validator and never falls back to declaration or repair. Missing, incompatible,
unsupported or retired native handles refuse the operation. This policy does
not grant tenant or lease authority, which remains the host's responsibility.
