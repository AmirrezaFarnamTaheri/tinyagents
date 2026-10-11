# Published pricing

`lookup` resolves decorated provider model ids to the July 2026 published
`ModelPrice` snapshot. `estimate_cost_usd` preserves cache-read discounts and
the MiniMax M3 512K input threshold. Unknown models return zero as an unknown
estimate; consumers must not interpret that as a billed-free response.

`catalog_entry` projects rates, windows and the existing vision allowlist into
a `ModelCatalogEntry`, using the source label supplied by the consumer. Runtime
capability overlays, configuration and billing authority stay with consumers.
This module performs no I/O and does not refresh prices. The seed catalog and
its validation remain independent; this additive API preserves the migrated
consumer snapshot without silently changing existing seed lookups.
