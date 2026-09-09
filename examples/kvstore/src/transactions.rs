use std::collections::HashMap;
use std::time::Duration;

use kuberic_core::types::Lsn;
use tokio::time::Instant;
use tonic::Status;

use crate::state::{KvMutation, KvState};

pub(crate) const MAX_TRANSACTIONS: usize = 1024;
const MAX_OPERATIONS: usize = 1024;
const MAX_BYTES: usize = 1024 * 1024;
const MAX_LIFETIME: Duration = Duration::from_secs(60);

pub(crate) struct Transaction {
    generation: u64,
    deadline: Instant,
    observed: HashMap<String, (Lsn, Option<String>)>,
    pub mutations: Vec<KvMutation>,
    bytes: usize,
}

impl Transaction {
    pub fn new(state: &KvState, timeout_ms: u64) -> Result<Self, Status> {
        let lifetime = if timeout_ms == 0 {
            MAX_LIFETIME
        } else {
            Duration::from_millis(timeout_ms)
        };
        if lifetime > MAX_LIFETIME {
            return Err(Status::invalid_argument(
                "transaction timeout exceeds 60 seconds",
            ));
        }
        Ok(Self {
            generation: state.generation,
            deadline: Instant::now() + lifetime,
            observed: HashMap::new(),
            mutations: Vec::new(),
            bytes: 0,
        })
    }

    fn active(&self, generation: u64, now: Instant) -> bool {
        self.generation == generation && now < self.deadline
    }

    fn reserve(&mut self, bytes: usize) -> Result<(), Status> {
        if bytes > MAX_BYTES.saturating_sub(self.bytes) {
            return Err(Status::resource_exhausted("transaction exceeds 1 MiB"));
        }
        self.bytes += bytes;
        Ok(())
    }

    pub fn get(&mut self, state: &KvState, key: &str) -> Result<Option<String>, Status> {
        if !self.observed.contains_key(key) {
            if self.observed.len() == MAX_OPERATIONS {
                return Err(Status::resource_exhausted("transaction exceeds 1024 keys"));
            }
            let value = state.data.get(key);
            self.reserve(key.len() + value.map_or(0, String::len))?;
            self.observed
                .insert(key.into(), (state.key_version(key), value.cloned()));
        }
        for mutation in self.mutations.iter().rev() {
            match mutation {
                KvMutation::Put {
                    key: staged_key,
                    value,
                } if staged_key == key => {
                    return Ok(Some(value.clone()));
                }
                KvMutation::Delete { key: staged_key } if staged_key == key => return Ok(None),
                _ => {}
            }
        }
        Ok(self.observed[key].1.clone())
    }

    pub fn stage(&mut self, state: &KvState, mutation: KvMutation) -> Result<(), Status> {
        if self.mutations.len() == MAX_OPERATIONS {
            return Err(Status::resource_exhausted(
                "transaction exceeds 1024 mutations",
            ));
        }
        let (key, bytes) = match &mutation {
            KvMutation::Put { key, value } => (key, key.len() + value.len()),
            KvMutation::Delete { key } => (key, key.len()),
        };
        self.get(state, key)?;
        self.reserve(bytes)?;
        self.mutations.push(mutation);
        Ok(())
    }

    pub fn validate(&self, state: &KvState) -> Result<(), Status> {
        if !self.active(state.generation, Instant::now()) {
            return Err(Status::aborted(
                "transaction expired or primary epoch changed",
            ));
        }
        if self
            .observed
            .iter()
            .any(|(key, (version, _))| state.key_version(key) != *version)
        {
            return Err(Status::aborted(
                "transaction conflict; retry in a new transaction",
            ));
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct Transactions {
    pending: HashMap<String, Transaction>,
}

impl Transactions {
    pub fn cleanup(&mut self, generation: u64, now: Instant) {
        self.pending
            .retain(|_, transaction| transaction.active(generation, now));
    }

    pub fn begin(&mut self, state: &KvState, timeout_ms: u64) -> Result<String, Status> {
        self.cleanup(state.generation, Instant::now());
        if self.pending.len() >= MAX_TRANSACTIONS {
            return Err(Status::resource_exhausted("too many active transactions"));
        }
        let transaction = Transaction::new(state, timeout_ms)?;
        let transaction_id = format!("{:032x}", rand::random::<u128>());
        self.pending.insert(transaction_id.clone(), transaction);
        Ok(transaction_id)
    }

    pub fn get_mut(
        &mut self,
        state: &KvState,
        transaction_id: &str,
    ) -> Result<&mut Transaction, Status> {
        self.cleanup(state.generation, Instant::now());
        self.pending
            .get_mut(transaction_id)
            .ok_or_else(|| Status::not_found("transaction not found, expired, or fenced"))
    }

    pub fn take(&mut self, state: &KvState, transaction_id: &str) -> Result<Transaction, Status> {
        self.cleanup(state.generation, Instant::now());
        self.pending
            .remove(transaction_id)
            .ok_or_else(|| Status::not_found("transaction not found, expired, or fenced"))
    }

    pub fn abort(&mut self, transaction_id: &str) {
        self.pending.remove(transaction_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cleanup_bounds_resources_and_fences_old_epochs() {
        let directory =
            std::env::temp_dir().join(format!("kv-limits-{:032x}", rand::random::<u128>()));
        let mut state = KvState::open(directory.clone()).await.unwrap();
        let mut transactions = Transactions::default();
        for _ in 0..MAX_TRANSACTIONS {
            transactions.begin(&state, 0).unwrap();
        }
        assert_eq!(
            transactions.begin(&state, 0).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        transactions.cleanup(state.generation, Instant::now() + MAX_LIFETIME);
        assert!(transactions.pending.is_empty());
        let transaction_id = transactions.begin(&state, 0).unwrap();
        let transaction = transactions.get_mut(&state, &transaction_id).unwrap();
        assert_eq!(
            transaction
                .stage(
                    &state,
                    KvMutation::Put {
                        key: "large".into(),
                        value: "x".repeat(MAX_BYTES),
                    }
                )
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        for _ in 0..MAX_OPERATIONS {
            transaction
                .stage(
                    &state,
                    KvMutation::Delete {
                        key: "absent".into(),
                    },
                )
                .unwrap();
        }
        assert_eq!(
            transaction
                .stage(
                    &state,
                    KvMutation::Delete {
                        key: "absent".into()
                    }
                )
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        state.generation += 1;
        assert!(transactions.get_mut(&state, &transaction_id).is_err());
        assert!(transactions.pending.is_empty());
        drop(state);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}
