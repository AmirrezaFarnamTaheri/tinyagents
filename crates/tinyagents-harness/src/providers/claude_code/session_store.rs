//! Per-thread Claude Code session state on the host-bound storage ports.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tinyagents_storage_kit::primitives::{Kv, Leases};
use tinyagents_storage_kit::Key;
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_types::StorageError;

/// Maximum delivered-turn fingerprints remembered per thread.
const MAX_DELIVERED_PER_THREAD: usize = 256;
const OWNER: &str = "tinyagents-harness";
const NAMESPACE: &str = "claude_code_sessions";
const CLAIM_OWNER: &str = "tinyagents-harness-claude-code-delivery";
const CLAIM_TTL_MS: i64 = 120_000;
const CLAIM_RENEW_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct SessionRecord {
    session_id: Option<String>,
    #[serde(default)]
    delivered: Vec<String>,
}

/// Serialize async read/reserve and persist/release sequences in this process.
static STORE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A process-local fast path for reservations; storage leases coordinate
/// separate handles and processes that address the same logical scope.
static IN_FLIGHT: Mutex<Vec<(usize, String, String)>> = Mutex::new(Vec::new());

/// Persistent per-thread session state. The host supplies an already scoped
/// document handle; this type never discovers or reads workspace files.
#[derive(Debug, Clone)]
pub struct SessionStore {
    kv: Kv,
    leases: Leases,
    store_identity: usize,
}

impl SessionStore {
    pub fn new(docs: &Arc<dyn DocumentStore>) -> Result<Self, StorageError> {
        Ok(Self {
            kv: Kv::ns(docs, OWNER, NAMESPACE)?,
            leases: Leases::new(docs),
            store_identity: Arc::as_ptr(docs) as *const () as usize,
        })
    }

    pub async fn get(&self, thread_id: &str) -> Result<Option<String>, StorageError> {
        let _guard = STORE_LOCK.lock().await;
        Ok(self
            .record(thread_id)
            .await?
            .and_then(|record| record.session_id))
    }

    #[cfg(test)]
    pub async fn delivered(&self, thread_id: &str) -> Result<HashSet<String>, StorageError> {
        let _guard = STORE_LOCK.lock().await;
        Ok(self
            .record(thread_id)
            .await?
            .map_or_else(HashSet::new, |record| {
                record.delivered.into_iter().collect()
            }))
    }

    pub async fn claim_delivered(
        self: &Arc<Self>,
        thread_id: &str,
        fingerprints: &[String],
    ) -> Result<(HashSet<String>, DeliveryClaim), StorageError> {
        let _guard = STORE_LOCK.lock().await;
        let record = self.record(thread_id).await?.unwrap_or_default();
        let mut already: HashSet<String> = record.delivered.into_iter().collect();
        let in_flight = IN_FLIGHT.lock().unwrap_or_else(|e| e.into_inner());
        already.extend(
            in_flight
                .iter()
                .filter(|(store, thread, _)| *store == self.store_identity && thread == thread_id)
                .map(|(_, _, fingerprint)| fingerprint.clone()),
        );
        drop(in_flight);
        let mut claimed = Vec::new();
        let holder = uuid::Uuid::new_v4().to_string();
        let mut reservations = Vec::new();
        for fingerprint in fingerprints {
            if !already.contains(fingerprint) && !claimed.contains(fingerprint) {
                let key = Key::of([thread_id, fingerprint])?.as_str().to_owned();
                match self
                    .leases
                    .acquire(CLAIM_OWNER, &key, &holder, CLAIM_TTL_MS)
                    .await
                {
                    Ok(Some(lease)) => {
                        claimed.push(fingerprint.clone());
                        reservations.push(LeaseReservation {
                            key,
                            holder: holder.clone(),
                            epoch: lease.epoch,
                        });
                    }
                    Ok(None) => {
                        already.insert(fingerprint.clone());
                    }
                    Err(error) => {
                        release_reservations(&self.leases, &reservations).await;
                        return Err(error);
                    }
                }
            }
        }
        IN_FLIGHT
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(claimed.iter().map(|fingerprint| {
                (
                    self.store_identity,
                    thread_id.to_owned(),
                    fingerprint.clone(),
                )
            }));
        let heartbeat = start_lease_heartbeat(self.leases.clone(), reservations.clone());
        Ok((
            already,
            DeliveryClaim {
                store: Arc::clone(self),
                thread_id: thread_id.to_owned(),
                claimed,
                reservations,
                heartbeat,
            },
        ))
    }

    pub async fn record_delivered(
        &self,
        thread_id: &str,
        fingerprints: &[String],
    ) -> Result<(), StorageError> {
        if fingerprints.is_empty() {
            return Ok(());
        }
        let _guard = STORE_LOCK.lock().await;
        self.kv
            .update(thread_id, |current: Option<SessionRecord>| {
                let mut record = current.unwrap_or_default();
                for fingerprint in fingerprints {
                    if !record.delivered.contains(fingerprint) {
                        record.delivered.push(fingerprint.clone());
                    }
                }
                trim(&mut record.delivered, fingerprints.len());
                Ok(Some(record))
            })
            .await?;
        Ok(())
    }

    pub async fn set(&self, thread_id: &str, session_id: &str) -> Result<(), StorageError> {
        let _guard = STORE_LOCK.lock().await;
        self.kv
            .update(thread_id, |current: Option<SessionRecord>| {
                let mut record = current.unwrap_or_default();
                if record.session_id.as_deref() != Some(session_id) {
                    record.delivered.clear();
                }
                record.session_id = Some(session_id.to_owned());
                Ok(Some(record))
            })
            .await?;
        Ok(())
    }

    pub async fn remove_if(
        &self,
        thread_id: &str,
        expected_session_id: &str,
    ) -> Result<bool, StorageError> {
        let _guard = STORE_LOCK.lock().await;
        let removed = self
            .kv
            .update(thread_id, |current: Option<SessionRecord>| {
                let Some(record) = current else {
                    return Ok(None);
                };
                if record.session_id.as_deref() != Some(expected_session_id) {
                    return Ok(None);
                }
                Ok(Some(SessionRecord::default()))
            })
            .await?;
        Ok(removed.is_some())
    }

    async fn record(&self, thread_id: &str) -> Result<Option<SessionRecord>, StorageError> {
        self.kv.get(thread_id).await
    }

    async fn import_record(
        &self,
        thread_id: &str,
        incoming: SessionRecord,
    ) -> Result<(), StorageError> {
        let _guard = STORE_LOCK.lock().await;
        self.kv
            .update(thread_id, |current: Option<SessionRecord>| {
                let Some(mut current) = current else {
                    return Ok(Some(incoming.clone()));
                };
                if current.session_id == incoming.session_id {
                    for fingerprint in &incoming.delivered {
                        if !current.delivered.contains(fingerprint) {
                            current.delivered.push(fingerprint.clone());
                        }
                    }
                    trim(&mut current.delivered, 0);
                    Ok(Some(current))
                } else {
                    Ok(None)
                }
            })
            .await?;
        Ok(())
    }

    fn release_in_flight(&self, thread_id: &str, fingerprints: &[String]) {
        let mut in_flight = IN_FLIGHT.lock().unwrap_or_else(|e| e.into_inner());
        in_flight.retain(|(store, thread, fingerprint)| {
            !(*store == self.store_identity
                && thread == thread_id
                && fingerprints.contains(fingerprint))
        });
    }
}

fn trim(delivered: &mut Vec<String>, keep_at_least: usize) {
    let limit = MAX_DELIVERED_PER_THREAD.max(keep_at_least);
    if delivered.len() > limit {
        delivered.drain(..delivered.len() - limit);
    }
}

/// Importer input for the old `claude-code-sessions.json` file. The host owns
/// filesystem discovery and supplies the bytes; malformed input is rejected.
#[derive(Debug, Default, Deserialize)]
struct LegacySessionFile {
    #[serde(default)]
    sessions: std::collections::HashMap<String, String>,
    #[serde(default)]
    delivered: std::collections::HashMap<String, Vec<String>>,
}

/// Import old session JSON bytes without accessing the filesystem. Existing
/// session IDs win; matching IDs merge delivered fingerprints, while a
/// different current ID is left untouched. Safe to call repeatedly.
pub async fn import_legacy_sessions(
    store: &SessionStore,
    bytes: &[u8],
) -> Result<usize, StorageError> {
    let legacy: LegacySessionFile = serde_json::from_slice(bytes)
        .map_err(|error| StorageError::serialization(error.to_string()))?;
    let mut imported = 0;
    let thread_ids: std::collections::HashSet<_> = legacy
        .sessions
        .keys()
        .chain(legacy.delivered.keys())
        .cloned()
        .collect();
    for thread_id in thread_ids {
        let record = SessionRecord {
            session_id: legacy.sessions.get(&thread_id).cloned(),
            delivered: legacy
                .delivered
                .get(&thread_id)
                .cloned()
                .unwrap_or_default(),
        };
        let has_legacy_state = record.session_id.is_some() || !record.delivered.is_empty();
        let before = store.record(&thread_id).await?;
        store.import_record(&thread_id, record).await?;
        if before.is_none() && has_legacy_state {
            imported += 1;
        }
    }
    Ok(imported)
}

/// Reservation of fingerprints; dropping releases it, commit ends it after
/// the CLI accepted the user turn and durable state was recorded.
#[derive(Debug)]
pub struct DeliveryClaim {
    store: Arc<SessionStore>,
    thread_id: String,
    claimed: Vec<String>,
    reservations: Vec<LeaseReservation>,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

impl DeliveryClaim {
    pub async fn commit(mut self) {
        self.release_reservations().await;
    }

    pub async fn release(mut self) {
        self.release_reservations().await;
    }

    async fn release_reservations(&mut self) {
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.abort();
        }
        self.store
            .release_in_flight(&self.thread_id, &self.claimed);
        let reservations = std::mem::take(&mut self.reservations);
        release_reservations(&self.store.leases, &reservations).await;
    }
}

impl Drop for DeliveryClaim {
    fn drop(&mut self) {
        self.store.release_in_flight(&self.thread_id, &self.claimed);
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.abort();
        }
        if !self.reservations.is_empty()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let leases = self.store.leases.clone();
            let reservations = std::mem::take(&mut self.reservations);
            handle.spawn(async move {
                for reservation in reservations {
                    if let Err(error) = leases
                        .release(
                            CLAIM_OWNER,
                            &reservation.key,
                            &reservation.holder,
                            reservation.epoch,
                        )
                        .await
                    {
                        tracing::warn!("failed to release Claude Code delivery reservation: {error}");
                    }
                }
            });
        }
    }
}

#[derive(Debug, Clone)]
struct LeaseReservation {
    key: String,
    holder: String,
    epoch: u64,
}

fn start_lease_heartbeat(
    leases: Leases,
    reservations: Vec<LeaseReservation>,
) -> Option<tokio::task::JoinHandle<()>> {
    if reservations.is_empty() {
        return None;
    }
    Some(tokio::spawn(async move {
        loop {
            tokio::time::sleep(CLAIM_RENEW_INTERVAL).await;
            for reservation in &reservations {
                match leases
                    .renew(
                        CLAIM_OWNER,
                        &reservation.key,
                        &reservation.holder,
                        reservation.epoch,
                        CLAIM_TTL_MS,
                    )
                    .await
                {
                    Ok(Some(_)) => {}
                    Ok(None) => tracing::warn!(
                        key = %reservation.key,
                        "Claude Code delivery reservation was lost"
                    ),
                    Err(error) => tracing::warn!(
                        key = %reservation.key,
                        %error,
                        "failed to renew Claude Code delivery reservation"
                    ),
                }
            }
        }
    }))
}

async fn release_reservations(leases: &Leases, reservations: &[LeaseReservation]) {
    for reservation in reservations {
        if let Err(error) = leases
            .release(
                CLAIM_OWNER,
                &reservation.key,
                &reservation.holder,
                reservation.epoch,
            )
            .await
        {
            tracing::warn!("failed to release Claude Code delivery reservation: {error}");
        }
    }
}

/// Random RFC-4122 v4 UUID, formatted lower-case with hyphens.
pub fn generate_uuid_v4() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// CC accepts only RFC-4122 v4. Older stores might carry pre-v4 strings;
/// they are treated as missing and regenerated.
pub fn is_uuid_v4(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        if [8, 13, 18, 23].contains(&index) {
            if *byte != b'-' {
                return false;
            }
        } else if !byte.is_ascii_hexdigit() {
            return false;
        }
    }
    bytes[14] == b'4' && matches!(bytes[19], b'8' | b'9' | b'a' | b'b' | b'A' | b'B')
}

#[cfg(test)]
#[path = "session_store_tests.rs"]
mod tests;
