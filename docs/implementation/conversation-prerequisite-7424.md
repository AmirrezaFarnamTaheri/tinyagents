# Conversation persistence prerequisite (#7424)

This branch adapts retained OpenHuman TinyAgents commit
64fa8dc2bbf1ad65a33a53053bfd9ddd2de7d84f onto canonical base
3983fb5b450f63a29d5dbae8d1c897e5b81025c1 using a normal cherry-pick.
The original retained commit remains intact in the originating branch.

The imported surface is the async conversation contract, compatibility file
adapter, optional provider resolver, and scoped metadata/committed-prefix reader.
Message mutations, authoritative turn claims, and import remain unsupported.
This prerequisite does not complete the conversation migration.

The original mixed commit also contained workflow changes already published
upstream. Current canonical workflow code, including later cancellation fixes
and tests, is preserved. Its historical TSD gitlink is not imported: this branch
retains canonical f6d7591dfae5e1a699c67867257ef13ff020ae31.
No unpublished run-ledger ancestor was needed by the conversation code.

Six accidentally tracked SQLite test databases were excluded before creating
any imported checkpoint. The reopened SQLite fixture now supplies a filesystem
path to the driver's path-based open API, rather than a sqlite: URL.
No generated database bytes are published in this branch history.

The subsequent schema slice is now included as separate granular commits.
collection_specs() publishes the unchanged bare metadata declaration;
prepare_schema(documents) is an explicit trusted bootstrap operation;
new_validate_only(scoped) validates required persisted features on every
implemented operation without caching success or declaring schema. Compatible
native supersets are accepted, not exact declaration equality. Hosts must still
supply native existing-schema ports with their original fence. The existing new
constructor deliberately retains legacy behavior and provides no actor schema
safety. No host routing or dependency consumption is enabled here.

## Verification (2026-10-12)

The baseline explicit constructor delegated to existing behavior. Memory and
SQLite both returned None for an undeclared collection: two genuine behavior
failures, alongside two passing prepared native fence/scope controls. After
validation was implemented, all seven schema controls passed, including repeated
validation, original native cause identity, zero actor declarations, restricted
preparation refusal and compatible native supersets.

- Existing conversation prerequisite: 116 thread tests and 10 provider controls.
- Full session suite: 681 storage-driver tests and 517 default tests passed.
- Workspace: 5,206 all-feature and 4,811 default tests passed; each retained 36
  ignored live tests. These totals include doctests.
- Strict workspace all-target Clippy passed with and without all features.
- Workspace all-target builds passed with and without all features.
- Canonical rustdoc broken-link gate passed. Existing private-item warnings in
  unchanged modules remain; no new conversation documentation warnings occurred.
- Declared Rust 1.88 locked workspace all-target/all-feature check passed.
- JSON insertion-order compatibility: 2,697 harness tests passed.
- Canonical all-feature workspace line coverage: 88.39% (67,377/76,223 lines),
  exceeding 80%; 5,157 instrumented tests passed with 29 ignored live tests.
- Canonical scoped dependency check (cargo machete crates), formatting and diff
  checks passed. No dependencies, workflow code or gitlinks changed.

The remaining native message publication/import/turn-claim protocols are not
implemented by this prerequisite. Schema admission must not be described as a
completed conversation migration.
