# Native conversation append with a recoverable stream outbox

Base: canonical TinyAgents `34b2c853`; preserve schema1 thread metadata and
`MessageRecord` stream records. Scope: plain `append_message` only. Turn claims,
proof-bearing agent execution, message edits, tail deletion, purge and legacy
import remain unsupported. Existing committed-prefix readers stay read-only.

## Native contracts and compatibility

`DocumentStore::atomic_batch_checked` atomically validates document revisions and
applies document writes. `StreamStore::append_batch_if_offset` atomically checks
the persistent next offset and appends. Neither combines documents and streams.
The protocol therefore reserves a durable intent before appending, and exposes
a stream row only after verifying it and publishing metadata.

Do not add pending fields to `ThreadDoc`: older schema1 metadata writers ignore
unknown fields and would erase them during unrelated edits. Use a companion
`conversation_appends_v1` collection, keyed by the thread digest and incarnation.
Prepare it explicitly through raw trusted bootstrap. Existing
`collection_specs`, `prepare_schema`, metadata operations and readers retain
their current requirements. Add append-specific declaration/preparation APIs;
append validates both declarations each time and never performs actor DDL.

The pending document contains only strings: schema tag, immutable attempt token,
thread identity, incarnation, decimal expected offset and the complete serialized
schema1 `MessageRecord`. No nested message object or numeric field remains in the
pending document. Native required-feature validation permits extra TTL fields;
all-string encoding prevents numeric epoch TTL from expiring accepted intent.
Extra uniqueness/search declarations may still refuse with their native error.

## Operation protocol

1. Validate input and append schema; require advertised `ValidatedBatch` and
   `ConditionalStreamAppend`. `StreamStore` has no separate capability method.
2. Load the live thread and its companion pending document. Help a valid pending
   intent before reserving a new one. Never substitute the caller's body for
   the durable pending body. Reject malformed/misbound intent with typed error.
3. For deterministic `agent:` IDs OR `extraMetadata.scope == "channel"`, scan
   the committed prefix and return the original matching message, even if the
   retry body differs. Ordinary UUID repeated calls remain append operations.
4. Probe the actual paired stream with an empty conditional append at the
   observed `visible_end`. This appends no business records, but may touch native
   internal stream metadata. Conflict restarts fresh thread/pending reads;
   unsupported/refused probe leaves no pending intent. Do not claim the probe
   replaces real append/fence validation. Unknown probe acknowledgment has not
   accepted a message and returns the native error.
5. Reserve using a checked document batch: exact live thread version and absent
   companion pending document; put the immutable intent with `Absent`. Do not
   change thread metadata or advertise the future row. Reservation Conflict
   restarts the full observation and deterministic/channel dedup path; it does
   not jump directly to another reserve. Retain the same attempt
   token and original offset across uncertain acknowledgment.
6. Conditional-append the exact stored record at the stored offset. If it
   conflicts or acknowledgment is lost, inspect that exact stream offset.
   Only the exact record, incarnation and contiguous offset can be reconciled;
   never treat another row as a successful append or silently skip an orphan.
7. Reload metadata. Finalize with one checked document batch checking current
   thread version plus exact pending version, putting merged metadata and
   deleting that pending document. While pending remains present, require
   `visible_end == pending.expected_offset`; publish exactly that offset plus
   one, refusing inconsistent later prefixes. Preserve unrelated title/labels/creation
   fields. Advance `visible_end` and message count by one; set last-message time
   to this appended message's timestamp, not the maximum timestamp.
8. On cancellation or unresolvable native error, retain pending intent. A later
   admitted writer reconciles it; readers only expose the committed prefix.
   A delete/recreate produces a new incarnation; no old helper publishes into it.
   Old unreachable intent/stream cleanup is outside this slice.

Successful reservation, or a durable reread of this exact attempt token, proves
ownership of the offset. Then exact row equality under the same incarnation and
committed prefix can resolve finalization after its pending document is gone.
If reservation acknowledgment is lost, the token was never observed, pending is
already gone, and an ordinary-ID row is byte-identical, ownership is ambiguous:
return typed uncertainty/native cause. Do not claim success or invisibly rerun
an ordinary UUID append. Deterministic/channel IDs can use their existing dedup
semantics. No general exactly-once delivery promise is made.

Every mutation uses captured native scoped/fenced ports. A stale original fence
must refuse reservation, real append and finalization; read permission alone is
not proof of current ownership. No ambient backend, local files or actor schema
fallback is involved. Unsupported paired ports refuse before accepting intent.

## RED-first execution

Files: `threads/driver.rs` wiring/public append preparation only;
`threads/driver/append.rs` protocol; sibling `driver_append_tests.rs` and focused
`driver_append_fixtures.rs` for wrappers around real ports. Keep production
unsupported until the first prepared Memory/SQLite append tests fail at runtime.

1. Prepared existing-schema Memory and SQLite: two independent handles, same
   scope reopen, different scope isolation; append visible row/count/timestamp,
   deterministic and channel replay return original body, ordinary ID repeats
   still append. No declaration through actor, no filesystem state.
2. Validate missing append declaration, repeated validation after lifecycle,
   preserved native error/source; document capability masks and counterfeit
   paired stream whose actual conditional append is unsupported. Empty probe
   cancellation leaves no pending or business row; existing stream cursor and
   records remain unchanged.
3. Deterministic native interleavings: two cold helpers race one pending intent;
   concurrent unrelated title update is preserved; differing appends keep a
   contiguous prefix. Verify conflict actually occurred, not only final values.
4. Real native fault wrappers pause after reservation and after actual stream
   commit, then drop owning future: old visible prefix stays coherent; next
   writer finishes original exactly once and appends its own row. Inject lost
   reservation, append and finalize acknowledgments separately. Keep immutable
   token identity and distinguish a successor pending document.
5. Byte-identical competing ordinary-ID write under lost reservation ack must
   not produce falsely attributed success or hidden duplicate retry. Native
   source cause remains accessible when durable rereads cannot resolve it.
6. Delete/recreate while original writer is paused: new incarnation never shows
   old rows. Exact pending/row corruption refuses; stale fences at each native
   commit stage refuse and preserve current owner state. Preprovision numeric
   TTL on each pending field and dotted payload path on real Memory/SQLite;
   pending must not expire because the envelope contains only strings.
7. Keep all existing metadata/reader/schema and classic conversation controls.
   Update the old unsupported-writers test to keep edits/tail/purge/claims
   unsupported; plain append alone becomes supported after its own schema.

Only run Cargo after the coordinated owner window is handed back. Preserve
baseline failure logs and checkpoint tests separately from production changes.

## Execution checkpoint

The pre-implementation baseline was actually executed with:

```sh
cargo test --locked -p tinyagents-session --features storage-drivers threads::driver::append_tests -- --nocapture
```

The cancel-aware command handle `49018` terminated with exit 101 after a
successful build (1m19s). Its fresh executable was
`target/debug/deps/tinyagents_session-8ac47fd04c9e906b`. Both prepared native
Memory and SQLite tests failed on their first append with
`ConversationError::Unsupported { operation: "append_message" }`:
0 passed, 2 failed, 0 ignored, 681 filtered. The full output was read at that
terminal checkpoint; there was no compiler or setup failure.

The original log was `target/debug-logs/conversation-append-baseline-red.log`.
That disposable artifact is no longer present on a later filesystem check.
This checkpoint preserves the observed tool exit and results, rather than
claiming the missing artifact was freshly reread. The new 18-case phase suite
has been authored but has not yet executed; no candidate GREEN is claimed.

At source checkpoint `16d05342`, phase command `42353` successfully built and
executed 18 tests: 17 passed, 1 failed, 0 ignored. The single genuine failure was
`noncanonical_pending_record_refuses_without_appending_or_publishing_prefix`:
recovery accepted a pending record's unknown field and returned a successful
next append. The complete terminal output was read from
`target/debug-logs/conversation-append-phase-baseline.log`.

Commit `068254d0` adds exact serde Value roundtrip validation for **new pending
records only**; old committed-stream reader compatibility remains unchanged.
Fresh command `43414` (`cargo test --locked -j 1 -p tinyagents-session --features
storage-drivers threads::driver::append_tests -- --nocapture`) terminated 0:
18 passed, 0 failed, 0 ignored, 681 filtered. Its successful build took 16.05s,
and the full output was read from
`target/debug-logs/conversation-append-phase-green.log`. This proves the two
original prepared native append failures and the later corruption failure are
fixed, alongside phase/CAS/TTL/fence/uncertainty controls. Broad workspace,
coverage, strict lint, documentation and MSRV gates remain pending.

The strengthened snapshot `2e880359` was freshly verified by command `28065`,
which terminated 0 after a successful 20.68s build: 20 passed, 0 failed,
0 ignored, 681 filtered. The complete output was read from
`target/debug-logs/conversation-append-phase-strengthened-green.log`.
This adds an actual two-helper finalization CAS barrier with observed native
conflict, malformed/foreign pending identities and missing-field normalization,
and native fresh-epoch bound recovery after each original writer is fenced.
The previous 18-case results remain separate chronological checkpoints.

## Final owning gate checkpoint

At frozen source `223c6bee461b55a7422a125805822d11cb27e002`, the
cancel-aware sequential command `25725` terminated 0. Every step in
`.validation/gates-progress.log` finished successfully: formatting, session
storage-driver tests, strict default and all-feature workspace Clippy,
all-targets/all-feature workspace build, default and all-feature workspace
tests, canonical rustdoc broken-link lint, Rust 1.88 all-targets/all-feature
build, and diff check. Session results were 701 unit tests plus 2 doctests.
Default workspace results were 4,811 passed, 0 failed, 36 ignored; all-feature
results were 5,226 passed, 0 failed, 36 ignored, including doctests. These are
separate feature graphs, not summed unique tests; nested child repetitions are
excluded. Full outputs were read. The MSRV build finished in 6m49s. Rustdoc
retains existing private and redundant link warnings outside this change;
its canonical `-D rustdoc::broken_intra_doc_links` gate passed.

Canonical coverage command `65988` also terminated 0 after a successful
instrumented build (4m15s): 5,176 passed, 0 failed, 29 ignored, excluding the
nested child repetition. The exact owning CI selection was:

```sh
cargo llvm-cov --locked -j 1 --all-features --workspace \
  --ignore-filename-regex '(^|/)(tests?|examples)/|/test(_.*)?\.rs$' \
  --fail-under-lines 80
```

The complete 6,674-line output was read and checked for failing results and
errors. Noncompiling JSON export confirmed 67,758 of 76,729 executable lines
covered: **88.3082%**, above the canonical whole-workspace 80% threshold.
There is no per-file floor claim: new `append.rs` is 240/301 (79.7342%) and
`pending.rs` is 61/94 (64.8936%). This instrumented result differs from regular
all-feature/doctest counts and must not be conflated with them. The final
coverage run includes all 20 append phase controls.

The final broad and coverage logs, JSON export, and copied later phase logs
are retained outside disposable build output under `.validation/`. No
production source changed after this tested checkpoint; subsequent evidence
updates are documentation only. Ready upstream publication still requires the
final independent review. No module release, host consumption, native turn
claims, import, or complete conversation migration is asserted.

## Hosted review correction checkpoint

PR416's three review findings were verified against the protocol and reproduced
without modifying production. Regression snapshot `03c8cb26` ran via command
`69288`: successful 9.53s build, 0 passed, 3 failed, 0 ignored, 701 filtered.
The complete `.validation/pr416-review-red.log` shows genuine behavior failures:

- Lost finalization acknowledgment plus a real title CAS between committed
  verification reads returned a failure despite the exact published row.
- An actually finalized row then removed from the native stream was reported
  as `Uncertain`, hiding definitive `Serialization` corruption.
- A helper reported a foreign pending writer's uncertainty as uncertainty
  about its own message, before reserving that message.

Source `894f7b52` corrects only those boundaries: committed verification
rechecks live incarnation and prefix after the exact row, rather than rejecting
unrelated metadata revisions; definitive reconciliation errors remain typed;
foreign-intent uncertainty returns its underlying native error because the
caller's own message is still unreserved. Native error sources are retained.

Fresh command `84087` terminated 0 after a 10.98s build: all 23 append tests
passed, including the unchanged three failure assertions. Then command
`28038` reran the complete owning sequence and terminated 0: formatting,
session tests (704 unit plus 2 doctests), both strict Clippy graphs, all-targets
all-feature build (2m21s), default tests (4,811 passed, 36 ignored), all-feature
tests (5,229 passed, 36 ignored), canonical rustdoc lint, Rust 1.88 build
(2m19s), diff check and canonical coverage. Each feature graph is counted
separately, and nested subprocess repetitions are excluded.

The complete fresh coverage output has 6,453 lines with no failing results or
errors: 5,179 instrumented tests passed, 29 ignored. Noncompiling JSON export
`3208` confirms 67,779/76,744 lines covered (**88.3183%**, canonical floor 80%).
`append.rs` is 254/310 (81.9355%); `pending.rs` remains 61/94 (64.8936%). No
per-file floor is claimed. All fresh logs and the JSON export are preserved as
`.validation/pr416-*` outside build output. The source was frozen throughout;
subsequent chronology/contract wording changes are documentation only.
