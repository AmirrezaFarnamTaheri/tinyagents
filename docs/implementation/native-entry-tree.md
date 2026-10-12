# Native entry-tree slice

## Boundary

Implement the existing RunLedgerStore entry-tree methods through the supplied
ScopedStorage. Preserve the classic SQLite record model and public identifiers;
neither entry ids nor session ids establish profile authority. This branch
preserves PR404 as a dependency and does not change OpenHuman or its gitlinks.

Classic code accepts explicit null and dangling parents, including another root
after the first append. Legacy import skips an existing id without changing its
body. The native implementation preserves these behaviors rather than enforcing
stronger documentation through a new adapter. Missing/dangling chains still
refuse when a caller walks them; cycle detection prevents an unbounded walk.

## Durable layout and transactions

- Immutable version-one entry records bind session id and the existing Entry.
- A per-session allocator/head record serializes ordinal allocation. Preserve
  EntryId::derive(session, ordinal), imported ordinals, and allocation above the
  greatest imported ordinal; refuse native signed-integer exhaustion.
- Named-label records point at the newly appended label entry.
- Chain-index records are rebuildable projections, not authoritative entries.

Every mutation uses a native validated batch with the captured head version.
Entries are absent-only writes. Appending, relabeling, copying a fork path,
importing entries and rebuilding indexes never partially commit. Conflicts
retry from a fresh snapshot; other native errors retain their sources. No
postcommit read can turn a successful commit into a reported rollback.

Snapshot reads capture head, enumerate immutable records, then validate head in
an empty native checked batch. This rejects concurrent tree mutations and
obtains native admission even for an empty tree. The protocol applies within the
supplied scope/generation, not a scope chosen by record data. A retired-generation
handle cannot read or mutate a replacement generation.

## Context

Extract the existing pure entry-chain projection mechanically and invoke it from
both SQLite and native paths. Preserve newest compaction, summary placement,
pinned entry handling, custom records, and the existing legacy message mapping.
Indexes are verified against the live chain and cannot choose a different chain
from immutable parent pointers. This first implementation enumerates session
records and does not claim indexed append performance.

## Evidence and gates

Memory and SQLite classic/native parity regressions initially failed at the
unsupported entry operation. A duplicate-ordinal import regression also failed
before private insertion ranks preserved SQLite ordering. Extend coverage for competing handles,
explicit parents/multiple roots, copy and branch forks, labels, import retries,
scope/session isolation, compaction/pinned context, native refusal and retired
generations. Run enabled/disabled session suites, strict Clippy, docs and owning
coverage. Publication remains a dependency draft until PR404 is canonical and
fresh workspace acceptance passes. No self-merge or host consumption.

Final merged graph: 638 enabled session tests, 509 disabled tests, and full
workspace enabled/disabled suites pass. New native entry coverage is 90.85%;
workspace production coverage is 88.41%. Strict Clippy, build, public
documentation link lint, formatting and diff checks pass. Existing private-link
and HTML documentation warnings remain. These gates do not prove OpenHuman
ProfileHost composition or complete the remaining run-ledger migration.
