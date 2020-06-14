#![allow(unused)]
#![allow(clippy::len_without_is_empty)]
//! Distributed single writer key/value store
//!Uses a rolling hash array mapped trie to index key/value data on top of a hypercore.
use std::fmt;
use std::hash::Hash;
use std::path::PathBuf;
use std::pin::Pin;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::stream::FuturesOrdered;
use futures::task::{Context, Poll};
use futures::{Future, Stream, StreamExt};
use hypercore::{Feed, PublicKey, SecretKey, Storage, Store};
use lru::LruCache;
use prost::Message as ProtoMessage;
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;

use anyhow::anyhow;

use crate::cmd::delete::{Delete, DeleteOptions};
use crate::cmd::diff::DiffOptions;
use crate::cmd::extension::HypertrieExtension;
use crate::cmd::get::{Get, GetOptions};
use crate::cmd::history::{History, HistoryOpts};
use crate::cmd::put::{Put, PutOptions};
use crate::cmd::TrieCommand;
use crate::hypertrie_proto as proto;
use crate::node::Node;

mod hypertrie_proto {
    include!(concat!(env!("OUT_DIR"), "/hypertrie_pb.rs"));
}

pub mod cmd;
mod hyperdrive;
pub mod node;
mod storage;
mod trie;

pub(crate) const HYPERCORE: &[u8] = b"hypercore";

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

pub struct HyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    /// these are all from the metadata feed
    feed: Feed<T>,
    /// This is public key of the content feed
    metadata: Option<Vec<u8>>,
    /// cache for seqs
    // TODO what's the value here? `valueBuffer` from a node?
    cache: LruCache<u64, Node>,
    /// How to encode/decode the value of nodes
    value_encoding: ValueEncoding,
    extension: Option<HypertrieExtension>,
    checkout: Option<u64>,
}

impl<T> HyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    pub fn len(&self) -> u64 {
        self.feed.len()
    }

    /// Returns the db public key. You need to pass this to other instances you want to replicate with.
    pub fn key(&self) -> &PublicKey {
        self.feed.public_key()
    }

    /// Returns the db discovery key. Can be used to find other db peers.
    pub fn discovery_key(&self) -> blake2_rfc::blake2b::Blake2bResult {
        discovery_key(self.key())
    }

    pub fn version(&self) -> u64 {
        if let Some(checkout) = self.checkout {
            checkout
        } else {
            self.feed.len()
        }
    }

    /// Checked out at the version specified.
    pub fn set_checkout(&mut self, version: u64) -> Option<u64> {
        self.checkout.replace(version)
    }

    /// Remove the checkout.
    pub fn remove_checkout(&mut self) -> Option<u64> {
        self.checkout.take()
    }

    /// Returns a hypercore replication stream for the db. Pipe this together with another hypertrie instance.
    async fn replicate(self) {}

    /// Same as checkout but just sets the latest version as a checkout.
    pub async fn snapshot(&mut self) -> Option<u64> {
        self.set_checkout(self.version())
    }

    pub async fn get_metadata(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        if let Some(data) = self.feed.get(0).await? {
            Ok(proto::Header::decode(&*data)?.metadata)
        } else {
            Ok(None)
        }
    }

    pub async fn diff_checkpoint(&mut self, checkout: impl Into<DiffOptions>) {}

    pub async fn diff_other<S>(
        &mut self,
        checkout: impl Into<DiffOptions>,
        other: &mut HyperTrie<S>,
    ) where
        S: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        unimplemented!()
    }

    pub async fn execute<C: TrieCommand>(&mut self, cmd: C) -> <C as TrieCommand>::Item {
        cmd.execute(self).await
    }

    pub async fn execute_all<C: TrieCommand>(
        &mut self,
        mut cmds: impl Iterator<Item = C>,
    ) -> Vec<<C as TrieCommand>::Item> {
        let mut results = Vec::new();
        for cmd in cmds {
            let res = self.execute(cmd).await;
            results.push(res);
        }
        results
    }

    /// Lookup a key. Returns a result node if found or `None` otherwise.
    pub async fn get(&mut self, opts: impl Into<GetOptions>) -> anyhow::Result<Option<Node>> {
        Ok(Get::new(opts).execute(self).await?)
    }

    /// Insert a value.
    pub async fn put(&mut self, opts: impl Into<PutOptions>, value: &[u8]) -> anyhow::Result<Node> {
        Ok(Put::new(opts, value.to_vec()).execute(self).await?)
    }

    /// Insert a a batch of values.
    pub async fn batch_put(&mut self, puts: Vec<impl Into<Put>>) -> anyhow::Result<Vec<Node>> {
        let mut nodes = Vec::with_capacity(puts.len());
        for put in puts {
            let put = put.into();
            nodes.push(put.execute(self).await?);
        }
        Ok(nodes)
    }

    /// delete a value
    pub async fn delete(&mut self, opts: impl Into<DeleteOptions>) -> anyhow::Result<Option<()>> {
        Ok(Delete::new(opts).execute(self).await?)
    }

    /// Delete a batch of values.
    pub async fn batch_delete(
        &mut self,
        deletes: Vec<impl Into<DeleteOptions>>,
    ) -> anyhow::Result<Vec<Option<()>>> {
        let mut results = Vec::with_capacity(deletes.len());
        for del in deletes {
            results.push(self.delete(del).await?);
        }
        Ok(results)
    }

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

    pub async fn get_by_seq(&mut self, seq: u64) -> anyhow::Result<Option<Node>> {
        if seq == 0 {
            // index 0 is always `Header`
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
        if let Some(checkout) = self.checkout {
            if checkout > 0 {
                return checkout - 1;
            }
        }

        if self.feed.len() < 2 {
            0
        } else {
            self.feed.len() - 1
        }
    }

    pub fn iter<'a>(&'a mut self) -> impl Stream<Item = Node> + 'a {
        HyperTrieStream {
            db: self,
            in_progress_queue: FuturesOrdered::new(),
            seq: 1,
        }
    }

    pub fn history_with_opts(&mut self, opts: impl Into<HistoryOpts>) -> History<T> {
        let mut opts = opts.into();
        let lte = opts.lte.unwrap_or_else(|| self.head_seq());

        History {
            db: self,
            lte,
            gte: opts.gte,
            reverse: opts.reverse,
        }
    }

    pub fn history(&mut self) -> History<T> {
        History {
            lte: self.head_seq(),
            db: self,
            gte: 1,
            reverse: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HyperTrieBuilder {
    cache_size: usize,
    metadata: Option<Vec<u8>>,
    value_encoding: Option<ValueEncoding>,
    extension: Option<HypertrieExtension>,
    checkout: Option<u64>,
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

    pub fn checkout(mut self, checkout: u64) -> Self {
        self.checkout = Some(checkout);
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
            metadata: self.metadata,
            cache: LruCache::new(self.cache_size),
            value_encoding: self.value_encoding.unwrap_or_default(),
            extension: self.extension,
            checkout: self.checkout,
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
            checkout: None,
        }
    }
}

pub struct HyperTrieStream<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    db: &'a mut HyperTrie<T>,
    in_progress_queue: FuturesOrdered<Pin<Box<dyn Future<Output = Node>>>>,
    seq: u64,
}

impl<'a, T> Stream for HyperTrieStream<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    type Item = Node;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use futures::ready;

        while self.in_progress_queue.len() < 5 {
            // TODO check len
        }

        let res = self.in_progress_queue.poll_next_unpin(cx);
        if let Some(val) = ready!(res) {
            return Poll::Ready(Some(val));
        }

        unimplemented!()
    }
}

/// Calculate a hypercore's discovery key (32 bytes) using the BLAKE2b hashing
/// function, keyed with the public key (as 32 bytes, not 64 hexadecimal
/// characters), to hash the word “hypercore”
fn discovery_key(publickey: &PublicKey) -> blake2_rfc::blake2b::Blake2bResult {
    blake2_rfc::blake2b::blake2b(32, publickey.as_ref(), HYPERCORE)
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
        let lorem_put = trie.put("lorem", b"ipsum").await?;

        let hello_get = trie.get("hello").await?.unwrap();
        let world_get = trie.get("world").await?.unwrap();
        let lorem_get = trie.get("lorem").await?.unwrap();

        assert_eq!(hello_put, hello_get);
        assert_eq!(world_put, world_get);
        assert_eq!(lorem_put, lorem_get);

        Ok(())
    }

    #[async_std::test]
    async fn overwrite() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;

        let put = trie.put("/hello", b"world").await?;
        let get = trie.get("hello").await?.unwrap();
        assert_eq!(put, get);

        let put = trie.put("/hello", b"verden").await?;
        let get = trie.get("hello").await?.unwrap();
        assert_eq!(put, get);

        Ok(())
    }

    #[async_std::test]
    async fn put_in_tree() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;

        let root = trie.put("hello", b"a").await?;
        let leaf = trie.put("hello/world", b"b").await?;

        let get = trie.get("hello").await?.unwrap();
        assert_eq!(root, get);

        let get = trie.get("hello/world").await?.unwrap();
        assert_eq!(leaf, get);

        let leaf = trie.put("hello/world/lorem.txt", b"ipsum").await?;
        let get = trie.get("hello/world/lorem.txt").await?.unwrap();
        assert_eq!(leaf, get);

        Ok(())
    }

    #[async_std::test]
    async fn put_in_tree_reverse() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;

        let leaf = trie.put("hello/world", b"b").await?;
        let root = trie.put("hello", b"a").await?;

        let get = trie.get("hello").await?.unwrap();
        assert_eq!(root, get);

        let get = trie.get("hello/world").await?.unwrap();
        assert_eq!(leaf, get);

        Ok(())
    }

    #[async_std::test]
    async fn multiple_put_in_tree() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;

        let leaf_a = trie.put("hello/world", b"b").await?;
        let root = trie.put("hello", b"a").await?;
        let leaf_b = trie.put("hello/verden", b"c").await?;
        let root = trie.put("hello", b"d").await?;

        let get = trie.get("hello").await?.unwrap();
        assert_eq!(root, get);

        let get = trie.get("hello/world").await?.unwrap();
        assert_eq!(leaf_a, get);

        let get = trie.get("hello/verden").await?.unwrap();
        assert_eq!(leaf_b, get);

        Ok(())
    }

    #[async_std::test]
    async fn insert_many() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;

        let num = 100u64;

        for i in 0..num {
            trie.put(format!("#{}", i), format!("#{}", i).as_bytes())
                .await?;
        }

        for i in 0..num {
            let node = trie.get(format!("#{}", i)).await?.unwrap();
            assert_eq!(node.key(), &format!("#{}", i));
            assert_eq!(node.value(), Some(&format!("#{}", i).as_bytes().to_vec()));
        }

        Ok(())
    }

    #[async_std::test]
    async fn siphash_collision() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;

        let a = trie.put("idgcmnmna", b"a").await?;
        let b = trie.put("mpomeiehc", b"b").await?;

        let get = trie.get("idgcmnmna").await?.unwrap();
        assert_eq!(get, a);

        let get = trie.get("mpomeiehc").await?.unwrap();
        assert_eq!(get, b);

        Ok(())
    }

    #[async_std::test]
    async fn batch_put() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;

        let batch = trie
            .batch_put(vec![
                ("hello", b"world"),
                ("world", b"hello"),
                ("lorem", b"ipsum"),
            ])
            .await?;

        let mut nodes = Vec::with_capacity(3);
        nodes.push(trie.get("hello").await?.unwrap());
        nodes.push(trie.get("world").await?.unwrap());
        nodes.push(trie.get("lorem").await?.unwrap());

        assert_eq!(batch, nodes);

        Ok(())
    }

    #[async_std::test]
    async fn batch_delete() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrieBuilder::default().ram().await?;

        let nodes = trie
            .batch_put(vec![
                ("hello", b"world"),
                ("world", b"hello"),
                ("lorem", b"ipsum"),
                ("brown", b"doggo"),
            ])
            .await?;

        let del = trie
            .batch_delete(vec!["world", "hello", "ipsum", "yellow"])
            .await?;
        assert_eq!(del, vec![Some(()), Some(()), None, None]);
        assert!(trie.get("hello").await?.is_none());

        let node = trie.get("lorem").await?.unwrap();
        assert_eq!(node.key(), nodes[2].key());
        assert_eq!(node.value(), nodes[2].value());

        Ok(())
    }
}
