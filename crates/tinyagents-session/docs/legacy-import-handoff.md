# Legacy session import ownership handoff

Legacy workspace discovery and parsing now belong to the OpenHuman host. This
crate owns persistence contracts and migrations, not filesystem layout,
SQLite connections, or JSONL rewriting. The former source-reader implementation
was in `src/legacy/` and `src/run_ledger/drivers/{import,legacy_import}.rs`;
it read `session_db/sessions.db`, `session_raw/*.jsonl`, and turn-state JSON.
The host migration is tracked by OpenHuman issue #440.

The host supplies an admitted storage backend to `DriverSessionStores::new`
and uses the returned `AgentStores` ports for ordinary writes. For imported
entry trees, the target-side bulk operation remains
`RunLedgerStore::entry_import_legacy(session_id, entries)`. It accepts typed
`Entry` values and performs only target writes; it takes no path, source
connection, or workspace authority. Schema changes remain owner-local through
`storage_schema()` / `DriverRunLedger::prepare_schema` and the corresponding
transcript and conversation declarations.

OpenHuman #440 should move the previous filesystem and SQLite readers from
`src/legacy/` and `src/run_ledger/drivers/{import,legacy_import}.rs`, along
with JSONL rewrite behavior from `src/legacy/rewrite.rs`. The former public
source entry points were `legacy::import_workspace`,
`DriverRunLedger::import_legacy_database`, and
`DriverRunLedger::import_agent_family`; they are host responsibilities and
are intentionally absent here. The host should decode old rows into the
session crate's typed records, then use the ordinary session ports and
`entry_import_legacy` for writes. It must preserve source immutability,
identity, idempotence, and resumability as host-owned migration behavior.

In-storage migration from previously written session collections remains in
this crate's `storage_schema()` steps. Those steps migrate records already in
the supplied backend and do not inspect the host's former files.
