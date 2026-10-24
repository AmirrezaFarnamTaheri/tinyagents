# Published pricing

`lookup` resolves decorated provider model ids to the July 2026 published
`ModelPrice` snapshot. `estimate_cost_usd` preserves cache-read discounts and
the MiniMax M3 512K input threshold. Unknown models return zero as an unknown
estimate; consumers must not interpret that as a billed-free response.

`flat_catalog_entry` projects rates, windows and the existing vision allowlist into
a `ModelCatalogEntry`, using the source label supplied by the consumer. Runtime
capability overlays, configuration and billing authority stay with consumers.
`lookup_pricing` returns this flat estimate projection and treats an explicit
case-insensitive `:free` suffix as zero list price. Call accounting still uses a
provider-reported charge when one exists.
This module performs no I/O and does not refresh prices. The seed catalog and
its validation remain independent; this additive API preserves the migrated
consumer snapshot without silently changing existing seed lookups.

Lookup first matches an exact canonical id, then an id with a leading vendor
segment removed. Its fallback chooses the longest listed id extended only by
date/version fragments or release-channel words (`latest`, `preview`, `exp`),
such as `gpt-5.4-mini-2026-05-01` or
`gemini-2.5-pro-preview-06-05`. Unlisted variants such as `gpt-4.1-nano` and
`o3-pro` return `None`; they do not inherit the rates or context window of a
listed base model.

Some cached rates remain historical assumptions, and long-context pricing other
than the MiniMax M3 estimate branch is unmodeled. The flat metadata projection
preserves row rates with no tiers, so it is not equivalent to the tier-aware M3
call estimator. Those limitations need separate behavioral corrections.
