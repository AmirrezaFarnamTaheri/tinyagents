# Local Mongo run-ledger proof

These integration tests exercise the native Mongo driver, including its BSON
conversion and transaction-based lease admission. They add no production
dependency and change no host adapter. The private integration-test package
enables Mongo only with `mongodb-storage-tests`; its default graph omits Mongo.

The tests are ignored by default, including in the ordinary all-features
matrix. An ignored result proves compilation only. Actual acceptance requires
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
  cargo test -p tinyagents-integration-tests --features mongodb-storage-tests \
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
compatibility. This fixture must not be promoted as ready until the owning
serialization contract is fixed and the complete affected graph passes, or
a separately verified dependency-isolation design prevents that unification.
No expected fingerprint or behavior assertion was changed to hide the failure.
