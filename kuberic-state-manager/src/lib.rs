#![doc = include_str!("../README.md")]

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::path::PathBuf;

pub use kuberic_transactional_replicator::{
    CommitVersion, Error, IsolationLevel, Result, TransactionId, TransactionOptions,
};
use kuberic_transactional_replicator::{
    TransactionContext, TransactionalReplicator, TransactionalStateProvider,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub trait ReliableValue: Serialize + DeserializeOwned {
    const TYPE_ID: &'static str;
    const VERSION: u32 = 1;
}

impl ReliableValue for String {
    const TYPE_ID: &'static str = "string/utf8";
}
impl ReliableValue for Vec<u8> {
    const TYPE_ID: &'static str = "bytes";
}
impl ReliableValue for i64 {
    const TYPE_ID: &'static str = "i64";
}
impl ReliableValue for u64 {
    const TYPE_ID: &'static str = "u64";
}
impl ReliableValue for bool {
    const TYPE_ID: &'static str = "bool";
}

#[derive(Clone, Serialize, Deserialize)]
struct Value {
    version: i64,
    bytes: Option<Vec<u8>>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Provider {
    id: u128,
    key_type: String,
    key_version: u32,
    value_type: String,
    value_version: u32,
    revision: i64,
    entries: BTreeMap<Vec<u8>, Value>,
}

#[doc(hidden)]
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Registry {
    revision: i64,
    providers: BTreeMap<String, Provider>,
}

#[doc(hidden)]
#[derive(Clone, Serialize, Deserialize)]
pub enum Change {
    Create {
        name: String,
        id: u128,
        key_type: String,
        key_version: u32,
        value_type: String,
        value_version: u32,
    },
    RemoveProvider {
        name: String,
        id: u128,
    },
    Set {
        name: String,
        id: u128,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    },
    Clear {
        name: String,
        id: u128,
    },
}

#[doc(hidden)]
#[derive(Default)]
pub struct Observations {
    providers: BTreeMap<String, Option<u128>>,
    keys: BTreeMap<(String, Vec<u8>), i64>,
    scans: BTreeMap<String, i64>,
    registry: Option<i64>,
}

fn conflict(message: &str) -> Error {
    Error::Conflict(message.into())
}

impl Registry {
    fn key_version(&self, provider: &str, key: &[u8]) -> i64 {
        self.providers
            .get(provider)
            .and_then(|provider| provider.entries.get(key))
            .map_or(0, |entry| entry.version)
    }

    fn provider_mut(&mut self, name: &str, id: u128) -> Result<&mut Provider> {
        self.providers
            .get_mut(name)
            .filter(|provider| provider.id == id)
            .ok_or_else(|| conflict("provider removed or recreated"))
    }
}

impl TransactionalStateProvider for Registry {
    const FORMAT_ID: &'static str = "kuberic-reliable-dictionary-registry/1";
    type Command = Vec<Change>;
    type Observations = Observations;

    fn validate_snapshot(&self, version: CommitVersion) -> Result<()> {
        if self.providers.len() > 1024 || self.revision < 0 || self.revision > version.0 {
            return Err(Error::Invalid("invalid registry snapshot".into()));
        }
        for (name, provider) in &self.providers {
            if name.is_empty()
                || name.len() > 128
                || provider.key_type.is_empty()
                || provider.key_type.len() > 128
                || provider.value_type.is_empty()
                || provider.value_type.len() > 128
                || provider.revision < 0
                || provider.revision > version.0
                || provider.entries.iter().any(|(key, value)| {
                    key.len() > 64 * 1024 || value.version < 0 || value.version > provider.revision
                })
            {
                return Err(Error::Invalid("invalid dictionary snapshot".into()));
            }
        }
        Ok(())
    }

    fn validate(&self, observed: &Observations, _command: &Vec<Change>) -> Result<()> {
        if observed
            .registry
            .is_some_and(|revision| revision != self.revision)
        {
            return Err(conflict("provider enumeration changed"));
        }
        for (name, identity) in &observed.providers {
            if self.providers.get(name).map(|provider| provider.id) != *identity {
                return Err(conflict("provider registry changed"));
            }
        }
        for ((name, key), version) in &observed.keys {
            if self.key_version(name, key) != *version {
                return Err(conflict("observed key changed"));
            }
        }
        for (name, revision) in &observed.scans {
            if self
                .providers
                .get(name)
                .map_or(0, |provider| provider.revision)
                != *revision
            {
                return Err(conflict("enumerated dictionary changed"));
            }
        }
        Ok(())
    }

    fn apply(&mut self, command: &Vec<Change>, version: CommitVersion) -> Result<()> {
        for change in command {
            match change {
                Change::Create {
                    name,
                    id,
                    key_type,
                    key_version,
                    value_type,
                    value_version,
                } => {
                    if self.providers.contains_key(name) {
                        return Err(conflict("duplicate provider name"));
                    }
                    if self.providers.len() >= 1024 {
                        return Err(Error::ResourceExhausted);
                    }
                    self.providers.insert(
                        name.clone(),
                        Provider {
                            id: *id,
                            key_type: key_type.clone(),
                            key_version: *key_version,
                            value_type: value_type.clone(),
                            value_version: *value_version,
                            revision: version.0,
                            entries: BTreeMap::new(),
                        },
                    );
                    self.revision = version.0;
                }
                Change::RemoveProvider { name, id } => {
                    self.provider_mut(name, *id)?;
                    self.providers.remove(name);
                    self.revision = version.0;
                }
                Change::Set {
                    name,
                    id,
                    key,
                    value,
                } => {
                    let provider = self.provider_mut(name, *id)?;
                    provider.entries.insert(
                        key.clone(),
                        Value {
                            bytes: value.clone(),
                            version: version.0,
                        },
                    );
                    provider.revision = version.0;
                }
                Change::Clear { name, id } => {
                    let provider = self.provider_mut(name, *id)?;
                    for entry in provider.entries.values_mut() {
                        entry.bytes = None;
                        entry.version = version.0;
                    }
                    provider.revision = version.0;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct StateManager {
    identity: u128,
    replicator: TransactionalReplicator<Registry>,
}

impl StateManager {
    pub async fn open(path: PathBuf) -> Result<Self> {
        Ok(Self {
            identity: rand::random(),
            replicator: TransactionalReplicator::open(path).await?,
        })
    }

    pub async fn run(
        self,
        lifecycle: tokio::sync::mpsc::Receiver<kuberic_core::events::LifecycleEvent>,
    ) {
        self.replicator.run(lifecycle).await;
    }

    pub async fn create_transaction(&self) -> Result<Transaction> {
        self.transaction_with_options(TransactionOptions::default())
            .await
    }

    pub async fn transaction_with_options(
        &self,
        options: TransactionOptions,
    ) -> Result<Transaction> {
        let (context, state) = self.replicator.begin(options).await?;
        let transaction = rand::random::<u128>();
        Ok(Transaction {
            identity: TransactionId {
                transaction,
                request: format!("{transaction:032x}"),
            },
            manager: self.identity,
            replicator: self.replicator.clone(),
            context,
            baseline: state.clone(),
            state,
            observed: Observations::default(),
            changes: Vec::new(),
            bytes: 0,
        })
    }

    pub async fn get_or_add_dictionary<Key: ReliableValue, Item: ReliableValue>(
        &self,
        name: &str,
    ) -> Result<ReliableDictionary<Key, Item>> {
        let mut transaction = self.create_transaction().await?;
        let existed = transaction.state.providers.contains_key(name);
        let dictionary = transaction.get_or_add_dictionary(name)?;
        if !existed {
            transaction.commit().await?;
        }
        Ok(dictionary)
    }

    pub async fn committed_result(&self, identity: TransactionId) -> Result<Option<CommitVersion>> {
        self.replicator.committed_result(identity).await
    }
    pub async fn checkpoint(&self) -> Result<()> {
        self.replicator.checkpoint().await
    }
    pub async fn backup(&self, destination: PathBuf) -> Result<()> {
        self.replicator.backup(destination).await
    }
    pub async fn restore_backup(&self, source: PathBuf) -> Result<()> {
        self.replicator.restore_backup(source).await
    }
    pub async fn applied_lsn(&self) -> Result<i64> {
        self.replicator.applied_lsn().await
    }
}

pub struct Transaction {
    identity: TransactionId,
    manager: u128,
    replicator: TransactionalReplicator<Registry>,
    context: TransactionContext,
    baseline: Registry,
    state: Registry,
    observed: Observations,
    changes: Vec<Change>,
    bytes: usize,
}

impl Transaction {
    pub fn id(&self) -> &TransactionId {
        &self.identity
    }

    pub fn with_identity(mut self, identity: TransactionId) -> Result<Self> {
        if identity.request.is_empty() || identity.request.len() > 128 {
            return Err(Error::Invalid("request ID must be 1-128 bytes".into()));
        }
        self.identity = identity;
        Ok(self)
    }

    fn observe_provider(&mut self, name: &str) -> Result<()> {
        self.context.ensure_active()?;
        if name.is_empty() || name.len() > 128 {
            return Err(Error::Invalid("provider name must be 1-128 bytes".into()));
        }
        if self.observed.providers.len() >= 1024 && !self.observed.providers.contains_key(name) {
            return Err(Error::ResourceExhausted);
        }
        self.observed
            .providers
            .entry(name.into())
            .or_insert_with(|| {
                self.baseline
                    .providers
                    .get(name)
                    .map(|provider| provider.id)
            });
        Ok(())
    }

    fn observe_key(&mut self, name: &str, key: &[u8]) -> Result<()> {
        self.observe_provider(name)?;
        if self.observed.keys.len() >= 1024
            && !self
                .observed
                .keys
                .contains_key(&(name.into(), key.to_vec()))
        {
            return Err(Error::ResourceExhausted);
        }
        if key.len() > 64 * 1024 {
            return Err(Error::ResourceExhausted);
        }
        self.observed
            .keys
            .entry((name.into(), key.to_vec()))
            .or_insert_with(|| self.baseline.key_version(name, key));
        Ok(())
    }

    fn scan(&mut self, name: &str) -> Result<()> {
        self.observe_provider(name)?;
        self.observed.scans.entry(name.into()).or_insert_with(|| {
            self.baseline
                .providers
                .get(name)
                .map_or(0, |provider| provider.revision)
        });
        Ok(())
    }

    fn stage(&mut self, change: Change) -> Result<()> {
        self.context.ensure_active()?;
        let size = postcard::to_allocvec(&change)?.len();
        if self.changes.len() >= 1024
            || size
                > (kuberic_transactional_replicator::MAX_TRANSACTION_BYTES / 2)
                    .saturating_sub(self.bytes)
        {
            return Err(Error::ResourceExhausted);
        }
        let command = vec![change.clone()];
        self.state.apply(&command, CommitVersion(0))?;
        self.bytes += size;
        self.changes.push(change);
        Ok(())
    }

    pub fn get_or_add_dictionary<Key: ReliableValue, Item: ReliableValue>(
        &mut self,
        name: &str,
    ) -> Result<ReliableDictionary<Key, Item>> {
        if let Some(dictionary) = self.get_dictionary(name)? {
            return Ok(dictionary);
        }
        if Key::TYPE_ID.is_empty()
            || Item::TYPE_ID.is_empty()
            || Key::TYPE_ID.len() > 128
            || Item::TYPE_ID.len() > 128
        {
            return Err(Error::Invalid("stable type IDs must be 1-128 bytes".into()));
        }
        let id = rand::random();
        self.stage(Change::Create {
            name: name.into(),
            id,
            key_type: Key::TYPE_ID.into(),
            key_version: Key::VERSION,
            value_type: Item::TYPE_ID.into(),
            value_version: Item::VERSION,
        })?;
        Ok(ReliableDictionary {
            manager: self.manager,
            name: name.into(),
            id,
            marker: PhantomData,
        })
    }

    pub fn get_dictionary<Key: ReliableValue, Item: ReliableValue>(
        &mut self,
        name: &str,
    ) -> Result<Option<ReliableDictionary<Key, Item>>> {
        self.observe_provider(name)?;
        let Some(provider) = self.state.providers.get(name) else {
            return Ok(None);
        };
        if provider.key_type != Key::TYPE_ID
            || provider.key_version != Key::VERSION
            || provider.value_type != Item::TYPE_ID
            || provider.value_version != Item::VERSION
        {
            return Err(Error::Invalid(
                "incompatible provider types or versions".into(),
            ));
        }
        Ok(Some(ReliableDictionary {
            manager: self.manager,
            name: name.into(),
            id: provider.id,
            marker: PhantomData,
        }))
    }

    pub fn remove_provider(&mut self, name: &str) -> Result<bool> {
        self.scan(name)?;
        let Some(provider) = self.state.providers.get(name) else {
            return Ok(false);
        };
        self.stage(Change::RemoveProvider {
            name: name.into(),
            id: provider.id,
        })?;
        Ok(true)
    }

    pub fn provider_names(&mut self) -> Result<Vec<String>> {
        self.context.ensure_active()?;
        self.observed.registry = Some(self.baseline.revision);
        Ok(self.state.providers.keys().cloned().collect())
    }

    pub async fn commit(self) -> Result<CommitVersion> {
        self.replicator
            .commit(self.context, self.identity, self.observed, self.changes)
            .await
    }

    pub fn abort(self) {}
}

pub struct ReliableDictionary<Key, Item> {
    manager: u128,
    name: String,
    id: u128,
    marker: PhantomData<fn(Key) -> Item>,
}

impl<Key, Item> Clone for ReliableDictionary<Key, Item> {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager,
            name: self.name.clone(),
            id: self.id,
            marker: PhantomData,
        }
    }
}

impl<Key: ReliableValue, Item: ReliableValue> ReliableDictionary<Key, Item> {
    fn check(&self, transaction: &mut Transaction) -> Result<()> {
        transaction.context.ensure_active()?;
        if self.manager != transaction.manager {
            return Err(Error::Invalid(
                "dictionary belongs to another state manager".into(),
            ));
        }
        transaction.observe_provider(&self.name)?;
        transaction.state.provider_mut(&self.name, self.id)?;
        Ok(())
    }

    pub fn get(&self, transaction: &mut Transaction, key: &Key) -> Result<Option<Item>> {
        self.check(transaction)?;
        let key = postcard::to_allocvec(key)?;
        transaction.observe_key(&self.name, &key)?;
        transaction.state.providers[&self.name]
            .entries
            .get(&key)
            .and_then(|entry| entry.bytes.as_ref())
            .map(|bytes| postcard::from_bytes(bytes).map_err(Error::from))
            .transpose()
    }

    pub fn contains_key(&self, transaction: &mut Transaction, key: &Key) -> Result<bool> {
        Ok(self.get(transaction, key)?.is_some())
    }

    pub fn set(&self, transaction: &mut Transaction, key: &Key, value: &Item) -> Result<()> {
        self.check(transaction)?;
        let key = postcard::to_allocvec(key)?;
        transaction.observe_key(&self.name, &key)?;
        transaction.stage(Change::Set {
            name: self.name.clone(),
            id: self.id,
            key,
            value: Some(postcard::to_allocvec(value)?),
        })
    }

    pub fn insert(&self, transaction: &mut Transaction, key: &Key, value: &Item) -> Result<bool> {
        if self.contains_key(transaction, key)? {
            return Ok(false);
        }
        self.set(transaction, key, value)?;
        Ok(true)
    }

    pub fn update(
        &self,
        transaction: &mut Transaction,
        key: &Key,
        expected: &Item,
        replacement: &Item,
    ) -> Result<bool> {
        let Some(current) = self.get(transaction, key)? else {
            return Ok(false);
        };
        if postcard::to_allocvec(&current)? != postcard::to_allocvec(expected)? {
            return Ok(false);
        }
        self.set(transaction, key, replacement)?;
        Ok(true)
    }

    pub fn remove(&self, transaction: &mut Transaction, key: &Key) -> Result<Option<Item>> {
        let value = self.get(transaction, key)?;
        transaction.stage(Change::Set {
            name: self.name.clone(),
            id: self.id,
            key: postcard::to_allocvec(key)?,
            value: None,
        })?;
        Ok(value)
    }

    pub fn clear(&self, transaction: &mut Transaction) -> Result<()> {
        self.check(transaction)?;
        transaction.scan(&self.name)?;
        transaction.stage(Change::Clear {
            name: self.name.clone(),
            id: self.id,
        })
    }

    pub fn get_or_add(
        &self,
        transaction: &mut Transaction,
        key: &Key,
        value: Item,
    ) -> Result<Item> {
        if let Some(existing) = self.get(transaction, key)? {
            return Ok(existing);
        }
        self.set(transaction, key, &value)?;
        Ok(value)
    }

    pub fn add_or_update(
        &self,
        transaction: &mut Transaction,
        key: &Key,
        update: impl FnOnce(Option<Item>) -> Item,
    ) -> Result<Item> {
        let value = update(self.get(transaction, key)?);
        self.set(transaction, key, &value)?;
        Ok(value)
    }

    pub fn entries(&self, transaction: &mut Transaction) -> Result<Vec<(Key, Item)>> {
        self.check(transaction)?;
        transaction.scan(&self.name)?;
        transaction.state.providers[&self.name]
            .entries
            .iter()
            .filter_map(|(key, entry)| entry.bytes.as_ref().map(|value| (key, value)))
            .map(|(key, value)| Ok((postcard::from_bytes(key)?, postcard::from_bytes(value)?)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_versions_must_not_exceed_their_boundary() {
        let mut registry = Registry::default();
        registry
            .apply(
                &vec![Change::Create {
                    name: "values".into(),
                    id: 1,
                    key_type: "i64".into(),
                    key_version: 1,
                    value_type: "i64".into(),
                    value_version: 1,
                }],
                CommitVersion(2),
            )
            .unwrap();
        assert!(registry.validate_snapshot(CommitVersion(1)).is_err());
        assert!(registry.validate_snapshot(CommitVersion(2)).is_ok());
        registry
            .providers
            .get_mut("values")
            .unwrap()
            .entries
            .insert(
                vec![1],
                Value {
                    version: 3,
                    bytes: Some(vec![7]),
                },
            );
        assert!(registry.validate_snapshot(CommitVersion(3)).is_err());
    }
}
