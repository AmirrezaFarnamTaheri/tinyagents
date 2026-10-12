# Local Mongo run-ledger proof

These integration tests exercise the native Mongo driver, including its BSON
conversion and transaction-based lease admission. They add no production
dependency and change no host adapter. The standalone workspace at `integration-proofs/run-ledger-mongodb`
enables Mongo only with `mongodb-storage-tests`; its default graph omits Mongo.
The root workspace explicitly excludes it, so ordinary workspace features
cannot unify Mongo into the harness graph.

The standalone tests are ignored by default, including its own all-features
run. The root matrix does not build this excluded fixture. An ignored result proves compilation only. Actual acceptance requires
an explicit unauthenticated, literal-loopback Mongo URL with an explicit port
and `--ignored`. Each test selects its own UUID database. SRV endpoints,
non-loopback hosts and credentials are rejected before connecting.

## Reproduce

Use an owned Mongo 7 replica-set container; Mongo 8 is incompatible with the
kernel on the development host used for this proof.

```sh
docker run -d --rm --name tinyagents-ledger-mongo-7423 \
  --ulimit nofile=65536:65536 -p 127.0.0.1::27017 mongo:7.0 \
  mongod --replSet tinyagents-ledger --bind_ip_all
docker port tinyagents-ledger-mongo-7423 27017
docker exec tinyagents-ledger-mongo-7423 mongosh --quiet --eval \
  'rs.initiate({_id:"tinyagents-ledger",members:[{_id:0,host:"127.0.0.1:27017"}]})'
docker exec tinyagents-ledger-mongo-7423 mongosh --quiet --eval \
  'db.hello().isWritablePrimary'
```

Wait for the last command to return true, substitute the published loopback
port below, and use the repository's cancellation-aware runner when available:

```sh
TINYAGENTS_TEST_MONGO_URL='mongodb://127.0.0.1:<published-port>/?directConnection=true' \
  cargo test --manifest-path integration-proofs/run-ledger-mongodb/Cargo.toml \
  --features mongodb-storage-tests \
  --test native_run_ledger_mongodb -- --ignored --nocapture
docker stop tinyagents-ledger-mongo-7423
```

Stop only the container created for this run, including after a failing run.
No host filesystem mounts or external credentials are needed. The removed
container also removes the isolated test databases.

## What the cases prove

- Import an immutable classic SQLite snapshot ending at signed BSON MAX-1;
  append the terminal event through the actual Mongo driver, then verify typed
  serialization refusal on exhaustion. The source bytes remain unchanged.
- Import a snapshot ending at MAX, independently reconnect, and verify the
  terminal event is readable and the next append is refused.
- Prepare schema under trusted bootstrap, then use existing-schema actors with
  native immutable lease fences. Independently reconnect and read labels,
  head, fork and projected context; another session and sibling scope remain
  separate. Replacing the lease epoch causes retained actor reads and writes to
  return typed Fenced, while the sibling remains usable.

If native fencing is unsupported, the actor case reports typed Unsupported and
explicitly states that positive actor proof was not executed. That output must
not be counted as actor acceptance. The development replica-set run completed
the positive actor proof and printed its completion marker.

This fixture does not prove ScopePurge generation admission, a live host
provider's schema mode, ProfileHost composition, retention, telemetry, import
reapers or host subscription routing. Those remain separate owner/host work.

## Recorded evidence

The final local Mongo 7 replica-set run executed all three tests successfully
with zero ignored cases; both terminal event completion markers and the native
actor completion marker were printed. Earlier fixture builds failed to compile
because of test API mismatches; those were not behavioral regression evidence.
The container was owned by this task and stopped after the run.

## Workspace feature-unification blocker

The first full all-features workspace run exposed six harness failures
(2,682 harness tests passed). The actual dependency graph shows Mongo's
bson 2.15 dependency enables serde_json/preserve_order globally. Existing
tool presentation assertions and the pinned prompt fingerprint assume sorted
JSON object keys. The fingerprint actually changes under that feature;
it is a production serialization contract issue rather than an ignored-test
or external-service failure. An OTLP usage assertion also compares a JSON
string in sorted order.

The first failed gate is retained at
target/mongodb-workspace-all-features.log. The isolated native Mongo target
passed all three actual cases. Those results do not establish full workspace
compatibility. The final fixture instead uses an explicitly excluded standalone workspace.
This isolates that dependency unification from ordinary gates; it does not
establish safe production Mongo/harness feature composition. The ordering
and enum-size findings remain owning follow-up evidence.
No expected fingerprint or behavior assertion was changed to hide the failure.

Strict all-features workspace Clippy independently failed at
MiddlewareToolOutcome and DeferredCallResult: insertion-ordered JSON enlarges
ToolResult and crosses the large-enum-variant threshold. This branch adds
neither public variant boxing nor lint exceptions. The default workspace gate
was run separately and passed 4,792 unit/integration/doc tests (36 ignored).
Fresh gates after isolation must also verify the ordinary all-features graph.

The observed ordering failures were:
- prompt/mod_tests.rs: fingerprint_value_is_pinned_for_cross_process_stability;
- tool/packs/mod_tests.rs: a_skill_alone_renders_guide_and_the_schema_of_a_present_tool;
- tool/prompt_tests.rs: prompt_replay_renders_assistant_calls_before_results;
- tool/signature_tests.rs: renders_arrays_unions_and_nesting and
  renders_flat_objects_with_optional_markers;
- observability/trace_export/otlp_tests.rs:
  otlp_has_one_root_with_readable_io_and_call_only_usage.

The pinned fingerprint changed from
0c8eb74fc9194b5d7845d787735eb2e36a68dc9d2ed91e5e7e07a13035d7d2a6
to 13ceff0b2049a85fe023ffef086bebc8bcc0d884d37fdef3a3563bd21565403c.
These findings establish a feature-composition incompatibility; the isolated
fixture intentionally does not claim to repair or validate that composition.

## Final isolated graph gates

After a normal merge of canonical main 6c1962a5, the root workspace
all-features suite passed 5,153 unit/integration/doc tests (36 ignored), and
strict all-targets/all-features Clippy passed. A final standalone Clippy run
also passed; ordinary standalone execution reported three ignored backend
cases, and its disabled feature discovered zero cases.

One initial default run failed in the unchanged canonical Claude CLI fixture
with Text file busy (OS error 26). The original failure is preserved at
target/mongodb-final-default.log. The entire harness source matched current
canonical main. Exact diagnostic passed one case, and serial diagnostic
passed all 2,522 harness cases. A fresh normal parallel default workspace run
then passed 4,793 unit/integration/doc tests (36 ignored). This records
both the failure and subsequent evidence; it does not establish the cause or
claim the transient fixture failure was fixed.

The actual Mongo replica-set proof was repeated after moving into the isolated
workspace: three cases passed, including positive native actor completion.
No runtime source or dependency changes to that standalone graph followed
that proof; the canonical merge touched only harness code, outside its graph.
Both owned containers used for these runs were stopped and removed.
