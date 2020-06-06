//! Distributed single writer key/value store
//!Uses a rolling hash array mapped trie to index key/value data on top of a hypercore.
use crate::discovery_key;
use crate::hypertrie_proto as proto;
use crate::trie::node::Node;
use crate::trie::put::PutOptions;
use anyhow::anyhow;
use hypercore::{Feed, PublicKey, SecretKey};
use lru::LruCache;
use prost::Message as ProtoMessage;
use random_access_storage::RandomAccess;
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;

pub mod batch;
pub mod delete;
pub mod diff;
pub mod get;
pub mod history;
pub mod node;
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
    /// cache for seqs
    // TODO what's the value here? `valueBuffer` from a node?
    cache: LruCache<u64, Node>,
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

    async fn diff(self) {}

    /// Lookup a key. Returns a result node if found or `None` otherwise.
    // TODO add options
    pub async fn get(&mut self, key: &str) -> anyhow::Result<Node> {
        unimplemented!()
    }

    pub async fn put_batch(&mut self) {}

    /// Insert a value.
    // TODO add with PutOptions
    pub async fn put(&mut self, key: impl Into<PutOptions>, value: &[u8]) -> anyhow::Result<()> {
        let opts = key.into();

        if let Some(head) = self.head().await? {
        } else {
        }

        unimplemented!()
    }

    async fn put_update(&mut self, head: Option<Node>) {
        if let Some(node) = head {
            for idx in 0..node.len() {
                let check_collision = Node::terminator(idx);
            }
        }
    }

    async fn put_finalize(&mut self) {}

    /// Delete a key from the database.
    async fn delete(&mut self) {}

    async fn ready(&mut self) -> anyhow::Result<()> {
        if self.feed.is_empty() {
            let mut header = proto::Header::default();
            header.r#type = "hypertrie".to_string();
            header.metadata = self.metadata.clone();
        }
        // insert header
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

    pub async fn get_by_seq(&mut self, seq: u64) -> anyhow::Result<Option<Node>> {
        if seq == 0 {
            return Ok(None);
        }
        // TODO lookup cached node
        // TODO clone on cache or cow?

        if let Some(data) = self.feed.get(seq).await? {
            let node = Node::decode(&data, seq)?;
            if !node.has_key() && !node.has_value() {
                // early exit for the key: '' nodes we write to reset the db
                Ok(None)
            } else {
                Ok(Some(node))
            }
        } else {
            Ok(None)
        }
    }

    async fn head(&mut self) -> anyhow::Result<Option<Node>> {
        Ok(self.get_by_seq(self.head_seq()).await?)
    }

    fn head_seq(&self) -> u64 {
        if self.feed.len() < 2 {
            0
        } else {
            self.feed.len() - 1
        }
    }
}

pub struct HypertrieBuilder {
    cache_size: usize,
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

struct Trie(pub Vec<Vec<Option<u64>>>);

impl Trie {
    #[inline]
    pub fn bucket(&self, idx: usize) -> Option<&Vec<Option<u64>>> {
        self.0.get(idx)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(65536);
        let offset = varinteger::encode(self.len() as u64, &mut buf);

        unimplemented!()
    }

    pub fn decode(buf: &[u8]) -> anyhow::Result<Self> {
        let mut len = 0;
        let mut offset = varinteger::decode(buf, &mut len);

        let mut trie = Vec::with_capacity(len as usize);

        while offset < buf.len() {
            let mut idx = 0;
            offset += varinteger::decode_with_offset(buf, offset, &mut idx);

            let mut bitfield = 0;
            offset += varinteger::decode_with_offset(buf, offset, &mut bitfield);

            let mut bucket = Vec::with_capacity((32 - bitfield.leading_zeros()) as usize);

            while bitfield > 0 {
                let bit = bitfield & 1;

                if bit != 0 {
                    let mut val = 0;
                    offset += varinteger::decode_with_offset(buf, offset, &mut val);
                    bucket.push(Some(val));
                } else {
                    bucket.push(None);
                }

                bitfield = (bitfield - bit) / 2;
            }
            trie.push(bucket);
        }

        Ok(Trie(trie))
    }
}
