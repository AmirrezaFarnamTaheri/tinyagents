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

Schema preparation and explicit immutable validation-only admission are a
subsequent focused slice. The existing constructor does not establish actor
schema safety, and no host routing or dependency consumption is enabled here.
