//! Storage-port models shared by TinyAgents persistence adapters.
//!
//! The crate owns the small typed helpers used by TinyAgents. Runtime engine
//! selection remains with the host, which supplies handles from
//! `tinystoragedrivers-api`.

pub use tinystoragedrivers_api::{BlobStore, DocumentStore, DocumentWatch, KvStore, Storage, StorageBackend, StreamStore};
pub use tinystoragedrivers_types::*;

pub mod blocking;
pub mod primitives;
