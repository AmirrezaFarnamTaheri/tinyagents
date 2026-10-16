//! The harness's v2 storage declarations: its `storage_schema()`, the shared
//! `tsd.kv` body helpers and the importer for the key-value stores that moved
//! onto `tsd.kv`.

mod kv;
mod kv_import;

pub(crate) use kv::{CACHE_NS_PREFIX, KV, KvEntry, NAMESPACED_NS_PREFIX, kv_body, kv_id};
pub use kv_import::{LegacyKv, import_legacy_kv};
