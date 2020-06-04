//! Distributed single writer key/value store
//!Uses a rolling hash array mapped trie to index key/value data on top of a hypercore.
use crate::discovery_key;
use crate::hypertrie_proto as proto;
use anyhow::anyhow;
use hypercore::{Feed, PublicKey, SecretKey};
use prost::Message as ProtoMessage;
use random_access_storage::RandomAccess;
use std::fmt;
use std::fmt::Debug;

pub mod batch;
pub mod delete;
pub mod diff;
pub mod get;
pub mod history;
pub mod put;

struct MountableHyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug,
{
    feed: Feed<T>,
}

impl<T> MountableHyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug,
{
    pub fn get_feed(&self) -> &Feed<T> {
        &self.feed
    }

    pub fn get_feed_mut(&mut self) -> &mut Feed<T> {
        &mut self.feed
    }

    pub async fn get(&self) {}

    async fn mount(&self) {}

    pub async fn put(&mut self) {}
}

struct HyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug,
{
    /// these are all from the metadata feed
    feed: Feed<T>,
    discovery_key: [u8; 32],
    version: usize,
    /// This is public key of the content feed
    metadata: Option<Vec<u8>>,
}

impl<T> HyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    /// Returns the db public key. You need to pass this to other instances you want to replicate with.
    pub fn key(&self) -> &PublicKey {
        self.feed.public_key()
    }

    /// Returns the db discovery key. Can be used to find other db peers.
    pub fn discovery_key(&self) -> blake2_rfc::blake2b::Blake2bResult {
        discovery_key(self.key())
    }

    /// Returns a new db instance checked out at the version specified.
    async fn checkout(self, version: String) {}

    /// Returns a hypercore replication stream for the db. Pipe this together with another hypertrie instance.
    async fn replicate(self) {}

    /// Same as checkout but just returns the latest version as a checkout.
    async fn snapshot(self) {
        // self.checkout(self.version)
    }

    async fn head(self) {}

    async fn diff(self) {}
    /// Lookup a key. Returns a result node if found or `None` otherwise.
    async fn get(&self) {}

    /// Insert a value.
    async fn put(&mut self) {}

    /// Delete a key from the database.
    async fn delete(&mut self) {}

    async fn ready(&mut self) -> anyhow::Result<()> {
        if self.feed.is_empty() {
            let mut header = proto::Header::default();
            header.r#type = "hypertrie".to_string();
            header.metadata = self.metadata.clone();
        }

        unimplemented!()
    }

    pub async fn get_metadata(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        let data = self
            .feed
            .get(0)
            .await?
            .ok_or_else(|| anyhow!("No hypertrie header present."))?;
        Ok(proto::Header::decode(&*data)?.metadata)
    }
}

// TODO impl watcher: channels or futures::stream::Stream?

#[derive(Debug, Clone)]
pub enum ValueEncoding {
    Json,
    Binary,
}

impl Default for ValueEncoding {
    fn default() -> Self {
        ValueEncoding::Binary
    }
}
