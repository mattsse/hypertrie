//! Distributed single writer key/value store
//!Uses a rolling hash array mapped trie to index key/value data on top of a hypercore.
use crate::discovery_key;
use crate::hypertrie_proto as proto;
use crate::trie::extension::HypertrieExtension;
use crate::trie::get::{Get, GetOptions};
use crate::trie::node::Node;
use crate::trie::put::{Put, PutOptions};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::StreamExt;
use hypercore::{Feed, PublicKey, SecretKey, Storage, Store};
use lru::LruCache;
use prost::Message as ProtoMessage;
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::fmt;
use std::hash::Hash;
use std::path::PathBuf;

pub mod batch;
pub mod delete;
pub mod diff;
pub mod extension;
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

pub enum Command {
    Get,
    Delete,
    Diff,
    Put,
}

#[async_trait]
pub trait TrieCommand {
    fn process(&mut self, trie: ());
}

pub struct HyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    /// these are all from the metadata feed
    feed: Feed<T>,
    version: usize,
    /// This is public key of the content feed
    metadata: Option<Vec<u8>>,
    /// cache for seqs
    // TODO what's the value here? `valueBuffer` from a node?
    cache: LruCache<u64, Node>,
    /// How to encode/decode the value of nodes
    value_encoding: ValueEncoding,
    extension: Option<HypertrieExtension>,
}

impl<T> HyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
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
    pub async fn get(&mut self, opts: impl Into<GetOptions>) -> anyhow::Result<Option<Node>> {
        Ok(Get::new(opts).execute(self).await?)
    }

    /// Insert a value.
    pub async fn put(&mut self, opts: impl Into<PutOptions>, value: &[u8]) -> anyhow::Result<Node> {
        Ok(Put::new(opts, value.to_vec()).execute(self).await?)
    }

    pub async fn put_batch(&mut self) {}

    /// Delete a key from the database.
    async fn delete(&mut self) {}

    async fn ready(&mut self) -> anyhow::Result<()> {
        if self.feed.is_empty() {
            let mut header = proto::Header::default();
            header.r#type = "hypertrie".to_string();
            header.metadata = self.metadata.clone();
            let mut buf = Vec::with_capacity(header.encoded_len());
            header.encode(&mut buf)?;
            self.feed.append(&buf).await?;
        }
        Ok(())
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

    pub async fn head(&mut self) -> anyhow::Result<Option<Node>> {
        Ok(self.get_by_seq(self.head_seq()).await?)
    }

    pub fn head_seq(&self) -> u64 {
        dbg!(self.feed.len());
        if self.feed.len() < 2 {
            0
        } else {
            self.feed.len() - 1
        }
    }
}

#[derive(Debug, Clone)]
pub struct HyperTrieBuilder {
    cache_size: usize,
    metadata: Option<Vec<u8>>,
    value_encoding: Option<ValueEncoding>,
    extension: Option<HypertrieExtension>,
}

impl HyperTrieBuilder {
    pub fn cache_size(mut self, cache_size: usize) -> Self {
        self.cache_size = cache_size;
        self
    }

    pub fn metadata(mut self, metadata: Vec<u8>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn value_encoding(mut self, value_encoding: ValueEncoding) -> Self {
        self.value_encoding = Some(value_encoding);
        self
    }

    pub fn extension(mut self, extension: HypertrieExtension) -> Self {
        self.extension = Some(extension);
        self
    }

    pub async fn build<T, Cb>(self, create: Cb) -> anyhow::Result<HyperTrie<T>>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
        Cb: Fn(
            Store,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<T>> + Send>>,
    {
        Ok(self.with_storage(Storage::new(create).await?).await?)
    }

    pub async fn with_storage<T>(self, storage: Storage<T>) -> anyhow::Result<HyperTrie<T>>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        let feed = Feed::with_storage(storage).await?;

        let mut trie = HyperTrie {
            feed,
            version: 0,
            metadata: self.metadata,
            cache: LruCache::new(self.cache_size),
            value_encoding: self.value_encoding.unwrap_or_default(),
            extension: self.extension,
        };
        trie.ready().await?;
        Ok(trie)
    }

    pub async fn ram(self) -> anyhow::Result<HyperTrie<RandomAccessMemory>> {
        Ok(self.with_storage(Storage::new_memory().await?).await?)
    }

    pub async fn disk(self, dir: &PathBuf) -> anyhow::Result<HyperTrie<RandomAccessDisk>> {
        Ok(self.with_storage(Storage::new_disk(dir).await?).await?)
    }
}

impl Default for HyperTrieBuilder {
    fn default() -> Self {
        Self {
            cache_size: 256,
            metadata: None,
            value_encoding: None,
            extension: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ValueEncoding {
    Binary,
    // TODO make json a feature
    Json,
    // TODO others?
}

impl Default for ValueEncoding {
    fn default() -> Self {
        ValueEncoding::Binary
    }
}

// TODO impl watcher: channels or futures::stream::Stream?

#[derive(Debug, Clone)]
pub(crate) enum Bucket {
    Vaccant,
    Occupied(Vec<Option<u64>>),
}

// TODO use btreehashmap instead?
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Trie(pub Vec<Option<Vec<Option<u64>>>>);

impl Trie {

    #[inline]
    pub(crate) fn insert_value(index: usize, value: u64, bucket: &mut Vec<Option<u64>>) {
        while index >= bucket.len() {
            bucket.push(None);
        }
        bucket[index] = Some(value)
    }

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

    fn fill_up_to(&mut self, mut index: usize) {
        while index >= self.len() {
            self.0.push(None);
        }
    }

    pub fn bucket_or_insert(&mut self, index: usize) -> &mut Vec<Option<u64>> {
        self.fill_up_to(index);
        if self.0[index].is_none() {
            self.0[index ] = Some(Vec::new());
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
        self.fill_up_to(index);
        self.0[index] = Some(bucket);
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
        let mut buf = BytesMut::with_capacity(self.len());
        varintbuf::encode(self.len() as u64, &mut buf);

        for i in 0..self.len() {
            if let Some(bucket) = self.bucket(i) {
                varintbuf::encode(i as u64, &mut buf);

                let mut bit = 1;
                let mut bitfield = 0;

                for j in 0..bucket.len() {
                    if bucket.get(j).cloned().flatten().is_some() {
                        bitfield |= bit;
                    }
                    bit *= 2;
                }

                varintbuf::encode(bitfield, &mut buf);

                for j in 0..bucket.len() {
                    if let Some(seq) = bucket.get(j).cloned().flatten() {
                        varintbuf::encode(seq as u64, &mut buf);
                    }
                }
            }
        }
        buf.to_vec()
    }

    pub fn decode(mut buf: &[u8]) -> Self {
        if !buf.has_remaining() {
            return Trie(vec![])
        }
        let remaining = buf.remaining();
        let mut len = varintbuf::decode(&mut buf);

        let mut trie = Trie(Vec::with_capacity(len as usize));

        if buf.has_remaining() {
            // the JS implementations starts at trie[offset] with the first bucket
            let offset = remaining - buf.remaining();
            trie.0.extend(std::iter::repeat(None).take(offset));
        }

        while buf.has_remaining() {
            let idx = varintbuf::decode(&mut buf);

            let mut bitfield = varintbuf::decode(&mut buf);
            let mut pos = 0;

            let mut bucket = Vec::with_capacity((32 - (bitfield as u32).leading_zeros()) as usize);

            while bitfield > 0 {
                let bit = bitfield & 1;

                if bit != 0 {
                    let val = varintbuf::decode(&mut buf);
                    Trie::insert_value(pos, val, &mut bucket);
                }

                bitfield = (bitfield - bit) / 2;
                pos+=1;
            }
            trie.insert_bucket(idx as usize, bucket);
        }
        trie
    }
}

impl Default for Trie {
    fn default() -> Self {
        Trie(vec![None])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn basic_put_get() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;

        let put = trie.put("hello", b"world").await?;
        let get = trie.get("hello").await?.unwrap();
        assert_eq!(put, get);

        Ok(())
    }

    #[async_std::test]
    async fn get_on_empty() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;
        let get = trie.get("hello").await?;
        assert!(get.is_none());

        Ok(())
    }

    #[async_std::test]
    async fn ignore_leading_slash() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;

        let put = trie.put("/hello", b"world").await?;
        let get = trie.get("/hello").await?.unwrap();
        assert_eq!(put, get);

        let get = trie.get("hello").await?.unwrap();
        assert_eq!(put, get);

        Ok(())
    }

    #[async_std::test]
    async fn multiple_put_get() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;

        let hello_put = trie.put("/hello", b"world").await?;
        let world_put = trie.put("world", b"hello").await?;

        let hello_get = trie.get("hello").await?.unwrap();
        let world_get = trie.get("world").await?.unwrap();

        assert_eq!(hello_put, hello_get);
        assert_eq!(world_put, world_get);

        Ok(())
    }

    #[test]
    fn encode_decode_trie() {
        let trie = Trie(vec![]);
        let buf = trie.encode();
        assert_eq!(buf, vec![0]);
        let decoded = Trie::decode(&*buf);
        assert_eq!(trie, decoded);

        let trie = Trie(vec![None, Some(vec![None, Some(1)])]);
        let buf = trie.encode();
        assert_eq!(buf.len(), 4);
        assert_eq!(buf, vec![2, 1, 2, 1]);

        let decoded = Trie::decode(&*buf);
        assert_eq!(trie, decoded);
    }
}
