use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::Instant;
use uuid::Uuid;

pub const MAX_LEASE_MILLIS: u64 = 30_000;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NativeError {
    #[error("native authority targets another process incarnation")]
    Incarnation,
    #[error("native authority revision must be positive and cannot go backwards")]
    Revision,
    #[error("native authority revision cannot be reused with different access")]
    Conflict,
    #[error("native authority lease must be between 1 and 30000 milliseconds")]
    Lease,
    #[error("native client access is closed")]
    Closed,
    #[error("native application failed: {0}")]
    Application(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeAuthority {
    pub incarnation: String,
    pub revision: u64,
    pub enabled: bool,
    pub lease_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeHealth {
    pub live: bool,
    pub ready: bool,
    pub readable: bool,
    pub writable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase", deny_unknown_fields)]
pub enum NativeHealthObservation {
    Observed { health: NativeHealth },
    Unavailable { error: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeObservation {
    pub incarnation: String,
    pub revision: u64,
    pub accepting_clients: bool,
    pub health: NativeHealthObservation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeOperation {
    pub id: String,
    pub request: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeOperationCommand {
    pub authority: NativeAuthority,
    pub operation: NativeOperation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase", deny_unknown_fields)]
pub enum NativeOperationStatus {
    Pending,
    Complete { evidence: serde_json::Value },
}

#[async_trait]
pub trait NativeApplication: Send + Sync {
    async fn observe(&self) -> Result<NativeHealth, NativeError>;

    async fn reconcile(
        &self,
        operation: &NativeOperation,
    ) -> Result<NativeOperationStatus, NativeError>;

    async fn close(&self) -> Result<(), NativeError>;
}

#[derive(Clone, Debug)]
struct Access {
    revision: u64,
    enabled: bool,
    deadline: Instant,
    generation: u64,
}

pub struct NativeGate {
    incarnation: String,
    access: watch::Sender<Access>,
}

impl Default for NativeGate {
    fn default() -> Self {
        Self::new()
    }
}

impl NativeGate {
    pub fn new() -> Self {
        Self {
            incarnation: Uuid::new_v4().to_string(),
            access: watch::channel(Access {
                revision: 0,
                enabled: false,
                deadline: Instant::now(),
                generation: 0,
            })
            .0,
        }
    }

    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }

    pub fn authorize(&mut self, authority: &NativeAuthority) -> Result<(), NativeError> {
        if authority.incarnation != self.incarnation {
            return Err(NativeError::Incarnation);
        }
        if authority.lease_millis == 0 || authority.lease_millis > MAX_LEASE_MILLIS {
            return Err(NativeError::Lease);
        }
        let now = Instant::now();
        let current = self.access.borrow().clone();
        if authority.revision == 0 || authority.revision < current.revision {
            return Err(NativeError::Revision);
        }
        if authority.revision == current.revision && authority.enabled != current.enabled {
            return Err(NativeError::Conflict);
        }
        let changed = authority.revision != current.revision || current.deadline <= now;
        let generation = if changed {
            current
                .generation
                .checked_add(1)
                .ok_or(NativeError::Revision)?
        } else {
            current.generation
        };
        self.access.send_replace(Access {
            revision: authority.revision,
            enabled: authority.enabled,
            deadline: now + Duration::from_millis(authority.lease_millis),
            generation,
        });
        Ok(())
    }

    pub fn close(&mut self) {
        self.access.send_modify(|access| {
            access.deadline = Instant::now();
        });
    }

    pub fn admit(&self) -> Result<NativePermit, NativeError> {
        let access = self.access.subscribe();
        let current = access.borrow().clone();
        if !current.enabled || current.deadline <= Instant::now() {
            return Err(NativeError::Closed);
        }
        Ok(NativePermit {
            access,
            generation: current.generation,
        })
    }

    pub fn observation(&self, health: Result<NativeHealth, NativeError>) -> NativeObservation {
        NativeObservation {
            incarnation: self.incarnation.clone(),
            revision: self.access.borrow().revision,
            accepting_clients: self.admit().is_ok(),
            health: match health {
                Ok(health) => NativeHealthObservation::Observed { health },
                Err(error) => NativeHealthObservation::Unavailable {
                    error: error.to_string(),
                },
            },
        }
    }
}

pub struct NativePermit {
    access: watch::Receiver<Access>,
    generation: u64,
}

impl NativePermit {
    pub async fn revoked(&mut self) {
        loop {
            let current = self.access.borrow_and_update().clone();
            if !current.enabled
                || current.generation != self.generation
                || current.deadline <= Instant::now()
            {
                return;
            }
            tokio::select! {
                result = self.access.changed() => {
                    if result.is_err() {
                        return;
                    }
                }
                () = tokio::time::sleep_until(current.deadline) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority(gate: &NativeGate, revision: u64, enabled: bool) -> NativeAuthority {
        NativeAuthority {
            incarnation: gate.incarnation().into(),
            revision,
            enabled,
            lease_millis: 1000,
        }
    }

    #[tokio::test]
    async fn admission_is_closed_until_exact_authority_and_never_implies_health() {
        let mut gate = NativeGate::new();
        assert!(matches!(gate.admit(), Err(NativeError::Closed)));
        let mut request = authority(&gate, 1, true);
        request.incarnation = NativeGate::new().incarnation().into();
        assert_eq!(gate.authorize(&request), Err(NativeError::Incarnation));
        request.incarnation = gate.incarnation().into();
        gate.authorize(&request).unwrap();
        let health = NativeHealth {
            live: true,
            ready: true,
            readable: true,
            writable: false,
        };
        let observation = gate.observation(Ok(health.clone()));
        assert!(observation.accepting_clients);
        assert_eq!(
            observation.health,
            NativeHealthObservation::Observed { health }
        );
        let value = serde_json::to_value(observation).unwrap();
        for field in ["lsn", "committedLsn", "catchUpComplete", "healingComplete"] {
            assert!(value.get(field).is_none());
        }
    }

    #[tokio::test]
    async fn revisions_fence_in_flight_connections_and_reject_conflicts() {
        let mut gate = NativeGate::new();
        gate.authorize(&authority(&gate, 2, true)).unwrap();
        let mut permit = gate.admit().unwrap();
        assert_eq!(
            gate.authorize(&authority(&gate, 1, false)),
            Err(NativeError::Revision)
        );
        assert_eq!(
            gate.authorize(&authority(&gate, 2, false)),
            Err(NativeError::Conflict)
        );
        gate.authorize(&authority(&gate, 3, false)).unwrap();
        tokio::time::timeout(Duration::from_millis(100), permit.revoked())
            .await
            .unwrap();
        assert!(matches!(gate.admit(), Err(NativeError::Closed)));
        gate.authorize(&authority(&gate, 4, true)).unwrap();
        assert!(gate.admit().is_ok());
    }

    #[tokio::test]
    async fn expiry_and_host_drop_revoke_existing_access() {
        let mut gate = NativeGate::new();
        let mut request = authority(&gate, 1, true);
        request.lease_millis = 10;
        gate.authorize(&request).unwrap();
        let mut permit = gate.admit().unwrap();
        tokio::time::timeout(Duration::from_secs(1), permit.revoked())
            .await
            .unwrap();
        assert!(matches!(gate.admit(), Err(NativeError::Closed)));
        gate.authorize(&request).unwrap();
        let mut reopened = gate.admit().unwrap();
        drop(gate);
        tokio::time::timeout(Duration::from_millis(100), reopened.revoked())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn forced_closure_cannot_resurrect_an_old_permit() {
        let mut gate = NativeGate::new();
        let request = authority(&gate, 1, true);
        gate.authorize(&request).unwrap();
        let mut permit = gate.admit().unwrap();
        gate.close();
        gate.authorize(&request).unwrap();
        assert!(gate.admit().is_ok());
        tokio::time::timeout(Duration::from_millis(100), permit.revoked())
            .await
            .unwrap();
    }

    #[test]
    fn bounds_are_explicit() {
        let mut gate = NativeGate::new();
        for (revision, lease, error) in [
            (0, 1, NativeError::Revision),
            (1, 0, NativeError::Lease),
            (1, MAX_LEASE_MILLIS + 1, NativeError::Lease),
        ] {
            let mut request = authority(&gate, revision, true);
            request.lease_millis = lease;
            assert_eq!(gate.authorize(&request), Err(error));
        }
    }
}
