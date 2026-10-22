//! TSD-backed ownership for one tenant-scoped parent-thread turn.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tinystoragedrivers_core::primitives::{Lease, Leases};
use tinystoragedrivers_core::{Scope, StorageBackend, StorageError};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::DeliveryFence;

/// Factory for durable thread-turn leases stored in `tsd.leases`.
///
/// Every process serving the same database must use the same explicit
/// `coordination_domain`, lease scope, tenant, and thread key. The domain is
/// host configuration, never model input. Different databases must use
/// different domains even when their tenant/thread text matches.
#[derive(Clone)]
pub struct DurableThreadDeliveryLeases {
    leases: Leases,
    lease_scope: Scope,
    coordination_domain: String,
    ttl: Duration,
    renewal_interval: Duration,
}

impl std::fmt::Debug for DurableThreadDeliveryLeases {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableThreadDeliveryLeases")
            .field("lease_scope", &self.lease_scope)
            .field("coordination_domain", &self.coordination_domain)
            .field("ttl", &self.ttl)
            .field("renewal_interval", &self.renewal_interval)
            .finish_non_exhaustive()
    }
}

impl DurableThreadDeliveryLeases {
    /// Opens the durable lease collection over `lease_scope`.
    ///
    /// `lease_scope` is normally the trusted cluster scope shared by every
    /// process serving the database. `coordination_domain` names that shared
    /// deployment namespace and must not be derived from a thread or tenant
    /// payload.
    pub fn new(
        backend: &dyn StorageBackend,
        lease_scope: Scope,
        coordination_domain: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self, StorageError> {
        let coordination_domain = coordination_domain.into();
        if coordination_domain.trim().is_empty() || ttl < Duration::from_millis(3) {
            return Err(StorageError::invalid_input(
                "durable thread lease requires a domain and a TTL of at least 3 ms",
            ));
        }
        let documents = Arc::clone(backend.for_scope(&lease_scope)?.documents());
        let renewal_interval = (ttl / 3).max(Duration::from_millis(1));
        Ok(Self {
            leases: Leases::new(&documents),
            lease_scope,
            coordination_domain,
            ttl,
            renewal_interval,
        })
    }

    /// Attempts the tenant/thread lease. `None` means another live holder owns
    /// it; backend errors remain errors and never fall back to local locking.
    pub async fn try_acquire(
        &self,
        tenant: &Scope,
        thread_key: &str,
    ) -> Result<Option<DurableThreadDeliveryLease>, StorageError> {
        if thread_key.is_empty() {
            return Err(StorageError::invalid_input(
                "durable thread lease key must not be empty",
            ));
        }
        let tenant_scope = Scope::parse(tenant.as_str())?;
        if tenant_scope.principal.is_some()
            || tenant_scope.tenant == tinystoragedrivers_core::Tenant::Cluster
        {
            return Err(StorageError::invalid_input(
                "durable thread lease requires a tenant scope without a principal",
            ));
        }
        let owner = "tinyagents.thread-turn";
        let key = serde_json::to_string(&(
            self.coordination_domain.as_str(),
            tenant.as_str(),
            thread_key,
        ))
        .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        let holder = uuid::Uuid::new_v4().to_string();
        let ttl_ms = duration_ms(self.ttl)?;
        let Some(lease) = self.leases.acquire(owner, &key, &holder, ttl_ms).await? else {
            return Ok(None);
        };
        let epoch = lease.epoch;
        let (admission, receiver) = watch::channel(true);
        let renewer = spawn_renewer(LeaseRenewalContext {
            leases: self.leases.clone(),
            ttl: self.ttl,
            interval: self.renewal_interval,
            owner: owner.to_owned(),
            key: key.clone(),
            holder: holder.clone(),
            epoch: lease.epoch,
            admission: admission.clone(),
        });
        Ok(Some(DurableThreadDeliveryLease {
            leases: self.leases.clone(),
            lease_scope: self.lease_scope.clone(),
            owner: owner.to_owned(),
            key,
            holder,
            lease,
            epoch,
            admission,
            receiver,
            renewer: Some(renewer),
            released: AtomicBool::new(false),
        }))
    }
}

/// One renewable TSD lease tenure. Renewal never reacquires a lost lease; a
/// loss permanently closes admission for this handle.
pub struct DurableThreadDeliveryLease {
    leases: Leases,
    lease_scope: Scope,
    owner: String,
    key: String,
    holder: String,
    lease: Lease,
    epoch: u64,
    admission: watch::Sender<bool>,
    receiver: watch::Receiver<bool>,
    renewer: Option<JoinHandle<()>>,
    released: AtomicBool,
}

impl std::fmt::Debug for DurableThreadDeliveryLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableThreadDeliveryLease")
            .field("epoch", &self.epoch)
            .field("admitted", &self.admitted())
            .finish_non_exhaustive()
    }
}

impl DurableThreadDeliveryLease {
    /// The stable lease epoch used to fence session writes.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Whether renewal still confirms this exact tenure.
    pub fn admitted(&self) -> bool {
        *self.receiver.borrow() && !self.released.load(Ordering::Acquire)
    }

    /// Creates the native epoch fence for storage in `lease_scope`.
    pub fn fence(&self) -> tinystoragedrivers_core::Fence {
        self.lease.fence(&self.lease_scope)
    }

    /// Permanently cancels the supplied token if this exact lease tenure is lost.
    ///
    /// Hosts pass the token through the full turn/runtime operation. Renewal
    /// failure closes the token; it never reacquires a lease or restores
    /// admission on this handle.
    pub fn cancel_on_loss(
        &self,
        cancellation: tinyagents_harness::CancellationToken,
    ) -> JoinHandle<()> {
        let mut receiver = self.receiver.clone();
        tokio::spawn(async move {
            loop {
                if !*receiver.borrow() {
                    cancellation.cancel();
                    return;
                }
                if receiver.changed().await.is_err() {
                    cancellation.cancel();
                    return;
                }
            }
        })
    }

    /// Releases this exact holder/epoch and stops renewal. A later holder's
    /// acquisition advances the epoch and fences every retained old writer.
    pub async fn release(mut self) -> Result<bool, StorageError> {
        self.released.store(true, Ordering::Release);
        let _ = self.admission.send(false);
        if let Some(renewer) = self.renewer.take() {
            renewer.abort();
            let _ = renewer.await;
        }
        self.leases
            .release(&self.owner, &self.key, &self.holder, self.epoch)
            .await
    }
}

impl DeliveryFence for DurableThreadDeliveryLease {
    fn admitted(&self) -> bool {
        self.admitted()
    }

    fn lost(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let mut receiver = self.receiver.clone();
        Box::pin(async move {
            loop {
                if !*receiver.borrow() {
                    return;
                }
                if receiver.changed().await.is_err() {
                    return;
                }
            }
        })
    }
}

impl Drop for DurableThreadDeliveryLease {
    fn drop(&mut self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.admission.send(false);
        if let Some(renewer) = self.renewer.take() {
            renewer.abort();
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let leases = self.leases.clone();
            let owner = self.owner.clone();
            let key = self.key.clone();
            let holder = self.holder.clone();
            let epoch = self.epoch;
            runtime.spawn(async move {
                if let Err(error) = leases.release(&owner, &key, &holder, epoch).await {
                    tracing::warn!(error = %error, "durable thread lease release failed");
                }
            });
        }
    }
}

struct LeaseRenewalContext {
    leases: Leases,
    ttl: Duration,
    interval: Duration,
    owner: String,
    key: String,
    holder: String,
    epoch: u64,
    admission: watch::Sender<bool>,
}

fn spawn_renewer(context: LeaseRenewalContext) -> JoinHandle<()> {
    let LeaseRenewalContext {
        leases,
        ttl,
        interval,
        owner,
        key,
        holder,
        epoch,
        admission,
    } = context;
    tokio::spawn(async move {
        let Ok(ttl_ms) = duration_ms(ttl) else {
            let _ = admission.send(false);
            return;
        };
        loop {
            tokio::time::sleep(interval).await;
            match leases.renew(&owner, &key, &holder, epoch, ttl_ms).await {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => {
                    let _ = admission.send(false);
                    return;
                }
            }
        }
    })
}

fn duration_ms(duration: Duration) -> Result<i64, StorageError> {
    i64::try_from(duration.as_millis())
        .map_err(|_| StorageError::invalid_input("durable thread lease TTL is out of range"))
}
