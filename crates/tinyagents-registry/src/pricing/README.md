# Published pricing

`lookup` resolves decorated provider model ids to the July 2026 published
`ModelPrice` snapshot. `estimate_cost_usd` preserves cache-read discounts and
the MiniMax M3 512K input threshold. Unknown models return zero as an unknown
estimate; consumers must not interpret that as a billed-free response.

`flat_catalog_entry` projects rates, windows and the existing vision allowlist into
a `ModelCatalogEntry`, using the source label supplied by the consumer. Runtime
capability overlays, configuration and billing authority stay with consumers.
This module performs no I/O and does not refresh prices. The seed catalog and
its validation remain independent; this additive API preserves the migrated
consumer snapshot without silently changing existing seed lookups.

Inherited compatibility limitations remain: unlisted hyphenated variants can
match a base id; some cached rates are historical assumptions; long-context
pricing other than the MiniMax M3 estimate branch is unmodeled. The flat
metadata projection deliberately preserves row rates with no tiers, so it is
not equivalent to the tier-aware M3 call estimator. These need separate
behavioral corrections rather than silent changes in this migration.
