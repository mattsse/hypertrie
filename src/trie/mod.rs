//! Distributed single writer key/value store
//!Uses a rolling hash array mapped trie to index key/value data on top of a hypercore.
use crate::discovery_key;
use crate::hypertrie_proto as proto;
use crate::trie::node::Node;
use crate::trie::put::PutOptions;
use anyhow::anyhow;
use futures::StreamExt;
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

/// Put impl
impl<T> HyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    /// Insert a value.
    // TODO add with PutOptions
    pub async fn put(&mut self, key: impl Into<PutOptions>, value: &[u8]) -> anyhow::Result<()> {
        let opts = key.into();

        if let Some(head) = self.head().await? {
        } else {
        }

        unimplemented!()
    }

    async fn put_update(&mut self, idx: u64, node: Node, head: Option<Node>) -> anyhow::Result<()> {
        if let Some(head) = head {
            for i in idx..node.len() {
                let check_collision = Node::terminator(i);
                let val = node.path(i);
                let head_val = head.path(i);

                if let Some(bucket) = head.bucket(i as usize) {
                    for j in 0..bucket.len() {
                        if !check_collision && j as u64 == val {
                            continue;
                        }

                        if let Some(seq) = bucket.get(j).cloned().flatten() {
                            if check_collision {
                                // TODO this._push_collidable(i, j, seq)
                            } else {
                                // TODO this._push(i, j, seq)
                            }
                        }
                    }

                    // we copied the head bucket, if this is still the closest node, continue
                    // if no collision is possible
                    if head_val == val && (!check_collision || !node.collides(&head, i)) {
                        continue;
                    }

                    // TODO this._push(i, headVal, head.seq)

                    if check_collision {
                        // TODO return this._updateHeadCollidable(i, bucket, val)
                    }
                    if let Some(seq) = bucket.get(val as usize).cloned().flatten() {
                        // TODO rewrite as non recursive
                        // return Ok(self.update_head(i, seq, node).await?);
                    } else {
                        break;
                    }
                }
            }
        } else {
            // TODO finalize
        }

        // TODO this._finalize(null)
        // TODO this._head = head?
        unimplemented!()
    }

    async fn update_head(&mut self, i: u64, seq: u64, node: Node) -> anyhow::Result<()> {
        let head = self.get_by_seq(seq).await?;
        Ok(self.put_update(i + 1, node, head).await?)
    }

    async fn put_finalize(&mut self, mut node: Node) -> anyhow::Result<()> {
        node.set_seq(self.feed.len());
        Ok(self.feed.append(&node.encode()?).await?)
    }

    async fn update_head_collidable(
        &mut self,
        node: Node,
        i: u64,
        bucket: &Vec<Option<u64>>,
        val: u64,
    ) -> anyhow::Result<()> {
        let mut missing = 1u64;
        for j in val as usize..bucket.len() {
            if let Some(seq) = bucket.get(j).cloned().flatten() {
                missing += 1;
                if let Some(other) = self.get_by_seq(seq).await? {
                    if other.collides(&node, i) {
                        // TODO recursive
                        // self.put_update(i +1,node)
                    }
                } else {
                    // TODO error, can't fix non collided nodes
                }
            } else {
                break;
            }
        }

        // TODO update with latest node retrieved from feed

        unimplemented!()
    }

    async fn push_collidable(
        &mut self,
        i: u64,
        val: u64,
        seq: u64,
        node: &mut Node,
    ) -> anyhow::Result<()> {
        if let Some(other) = self.get_by_seq(seq).await? {
            if other.collides(node, i) {
                self.push(node.trie_mut(), i, val, seq)
            }
            // TODO finalize()?
        }

        Ok(())
    }

    fn push(&mut self, trie: &mut Trie, i: u64, mut val: u64, seq: u64) {
        while val > 5 {
            val -= 5;
        }

        let bucket = trie.bucket_or_insert(i as usize);

        while bucket.len() as u64 > val && bucket.get(val as usize).is_some() {
            val += 5
        }

        if !bucket.contains(&Some(seq)) {
            bucket.insert(val as usize, Some(seq));
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

#[derive(Debug, Clone)]
pub(crate) enum Bucket {
    Vaccant,
    Occupied(Vec<Option<u64>>),
}

// TODO use btreehashmap instead?
#[derive(Debug, Clone, Default)]
pub(crate) struct Trie(pub Vec<Option<Vec<Option<u64>>>>);

impl Trie {
    #[inline]
    pub fn bucket(&self, idx: usize) -> Option<&Vec<Option<u64>>> {
        if let Some(b) = self.0.get(idx) {
            if let Some(b) = b {
                return Some(b);
            }
        }
        None
    }

    #[inline]
    pub fn bucket_mut(&mut self, idx: usize) -> Option<&mut Vec<Option<u64>>> {
        if let Some(b) = self.0.get_mut(idx) {
            if let Some(b) = b {
                return Some(b);
            }
        }
        None
    }

    pub fn bucket_or_insert(&mut self, index: usize) -> &mut Vec<Option<u64>> {
        if self.0[index].is_none() {
            self.0.insert(index, Some(Vec::new()));
        }
        self.0[index].as_mut().unwrap()
    }

    /// # Panics
    ///
    /// Panics if `index > len`.
    pub fn insert_bucket(
        &mut self,
        index: usize,
        bucket: Vec<Option<u64>>,
    ) -> &mut Vec<Option<u64>> {
        self.0.insert(index, Some(bucket));
        self.0[index].as_mut().unwrap()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn encode(&self) -> Vec<u8> {
        // TODO varint implementation that uses `bytes::buf::Buf`
        let mut buf = Vec::with_capacity(65536);
        let mut offset = varinteger::encode(self.len() as u64, &mut buf);

        for i in 0..self.len() {
            if let Some(bucket) = self.bucket(i) {
                offset += varinteger::encode_with_offset(i as u64, &mut buf, offset);

                let mut bit = 1;
                let mut bitfield = 0;

                for j in 0..bucket.len() {
                    if bucket.get(j).cloned().flatten().is_some() {
                        bitfield |= bit;
                    }
                    bit *= 2;
                }

                offset += varinteger::encode_with_offset(bitfield, &mut buf, offset);

                for j in 0..bucket.len() {
                    if let Some(seq) = bucket.get(j).cloned().flatten() {
                        offset += varinteger::encode_with_offset(seq as u64, &mut buf, offset);
                    }
                }
            }
        }
        buf[..offset].to_vec()
    }

    pub fn decode(buf: &[u8]) -> anyhow::Result<Self> {
        let mut len = 0;
        let mut offset = varinteger::decode(buf, &mut len);

        let mut trie = Vec::with_capacity(len as usize);

        // the JS trie starts at trie[offset] with the first bucket
        trie.extend(std::iter::repeat(None).take(offset));

        while offset < buf.len() {
            let mut idx = 0;
            offset += varinteger::decode_with_offset(buf, offset, &mut idx);

            // the JS trie adds the new bucket at index `idx`, so we add empty buckets until idx == trie.len()
            trie.extend(std::iter::repeat(None).take(idx as usize - trie.len()));

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
            trie.push(Some(bucket));
        }

        Ok(Trie(trie))
    }
}
