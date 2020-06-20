#![allow(unused)]
#![allow(clippy::len_without_is_empty)]
//! Single writer key/value store
//! Uses a rolling hash array mapped trie to index key/value data.
use std::fmt;
use std::path::Path;

use hypercore::{Feed, PublicKey, Storage, Store};
use lru::LruCache;
use prost::Message as ProtoMessage;
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;

pub use crate::cmd::delete::{Delete, DeleteOptions};
use crate::cmd::diff::DiffOptions;
use crate::cmd::extension::HypertrieExtension;
pub use crate::cmd::get::{Get, GetOptions};
pub use crate::cmd::history::{History, HistoryOpts};
pub use crate::cmd::put::{Put, PutOptions};
pub use crate::cmd::TrieCommand;
use crate::hypertrie_proto as proto;
use crate::iter::{HyperTrieIterator, IteratorOpts};
pub use crate::node::Node;

mod hypertrie_proto {
    include!(concat!(env!("OUT_DIR"), "/hypertrie_pb.rs"));
}

pub mod cmd;
mod hyperdrive;
mod iter;
mod mount;
pub mod node;
mod storage;
mod trie;

pub(crate) const HYPERCORE: &[u8] = b"hypercore";

pub struct HyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    /// these are all from the metadata feed
    feed: Feed<T>,
    /// This is public key of the content feed
    metadata: Option<Vec<u8>>,
    /// cache for seqs
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

    pub fn feed(&self) -> &Feed<T> {
        &self.feed
    }

    pub fn feed_mut(&mut self) -> &mut Feed<T> {
        &mut self.feed
    }

    /// Returns the db discovery key. Can be used to find other db peers.
    pub fn discovery_key(&self) -> blake2_rfc::blake2b::Blake2bResult {
        discovery_key(self.key())
    }

    /// The current version of the trie.
    ///
    /// If no checkout is set, version equals to the amount of nodes in the storage feed.
    pub fn version(&self) -> u64 {
        if let Some(checkout) = self.checkout {
            checkout
        } else {
            self.feed.len()
        }
    }

    pub(crate) fn has_seq_in_bucket(&mut self, bucket: &[Option<u64>], val: u64) -> bool {
        for i in (val as usize..bucket.len()).step_by(5) {
            if let Some(v) = bucket.get(i).cloned().flatten() {
                if self.feed.has(v) {
                    return true;
                }
            }
        }
        false
    }

    /// Check out at the version specified.
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

    /// Return the metadata that was stored in the root `Header` at index `0` in the feed.
    pub async fn get_metadata(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        if let Some(data) = self.feed.get(0).await? {
            Ok(proto::Header::decode(&*data)?.metadata)
        } else {
            Ok(None)
        }
    }

    /// Compute the differences between this trie and a specific checkout of this trie.
    pub async fn diff_checkpoint(&mut self, _checkout: impl Into<DiffOptions>) {
        unimplemented!()
    }

    /// Compute the differences between this trie and another trie.
    pub async fn diff_other<S>(
        &mut self,
        _checkout: impl Into<DiffOptions>,
        _other: &mut HyperTrie<S>,
    ) where
        S: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        unimplemented!()
    }

    /// Execute a single `TrieCommand`
    pub async fn execute<C: TrieCommand>(&mut self, cmd: C) -> <C as TrieCommand>::Item {
        cmd.execute(self).await
    }

    /// Execute a series of commands and return their results as a new `Vec`
    pub async fn execute_all<C: TrieCommand>(
        &mut self,
        cmds: impl Iterator<Item = C>,
    ) -> Vec<<C as TrieCommand>::Item> {
        let mut results = Vec::new();
        for cmd in cmds {
            let res = self.execute(cmd).await;
            results.push(res);
        }
        results
    }

    /// Get the node that matches the options (key).
    ///
    /// # Examples
    ///
    /// Get a node by its key
    ///
    /// ```
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use hypertrie::{HyperTrie, PutOptions};
    /// let mut trie = HyperTrie::ram().await?;
    ///
    /// let put = trie.put("hello", b"world").await?;
    ///
    /// let get = trie.get("hello").await?.unwrap();
    /// assert_eq!(put, get);
    ///
    /// assert_eq!(trie.get("world").await?, None);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Get a hidden node.
    /// ```
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use hypertrie::{HyperTrie, PutOptions, GetOptions};
    /// let mut trie = HyperTrie::ram().await?;
    ///
    /// let node = trie.put(PutOptions::new("hello").hidden(), b"world").await?;
    /// assert_eq!(trie.get("hello").await?, None);
    ///
    /// assert_eq!(trie.get(GetOptions::new("hello").hidden()).await?.unwrap(), node);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(&mut self, opts: impl Into<GetOptions>) -> anyhow::Result<Option<Node>> {
        Ok(Get::new(opts).execute(self).await?)
    }

    /// Insert a new value in the feed.
    ///
    /// # Examples
    ///
    /// Insert a basic node with key `hello` and value `world`.
    /// ```
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut trie = hypertrie::HyperTrie::ram().await?;
    /// let node = trie.put("hello", b"world").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Insert a hidden node.
    ///
    /// ```
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use hypertrie::{HyperTrie, PutOptions};
    /// let mut trie = HyperTrie::ram().await?;
    /// let node = trie.put(PutOptions::new("hello").hidden(), b"world").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn put(&mut self, opts: impl Into<PutOptions>, value: &[u8]) -> anyhow::Result<Node> {
        Ok(Put::new(opts, value.to_vec()).execute(self).await?)
    }

    /// Insert a a series of new values.
    ///
    /// # Examples
    ///
    /// Insert a series of key, value tuples where values are slices.
    /// Byte strings work only if each value is the batch is the same size.
    ///
    /// ```rust
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use hypertrie::{HyperTrie, PutOptions};
    /// let mut trie = HyperTrie::ram().await?;
    /// let batch = trie
    ///     .batch_put(vec![
    ///         ("hello", &b"world"[..]),
    ///         ("world", &b"hello"[..]),
    ///         ("lorem", &b"ipsum"[..]),
    ///         ("dummy", &b""[..]),
    ///     ])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Insert a series of key, value tuples where the value is str
    ///
    /// ```rust
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use hypertrie::{HyperTrie, PutOptions};
    /// let mut trie = HyperTrie::ram().await?;
    /// let batch = trie
    ///     .batch_put(vec![
    ///         ("hello", "world"),
    ///         ("world", "hello"),
    ///         ("lorem", "ipsum"),
    ///         ("dummy", ""),
    ///     ])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_put(&mut self, puts: Vec<impl Into<Put>>) -> anyhow::Result<Vec<Node>> {
        let mut nodes = Vec::with_capacity(puts.len());
        for put in puts {
            let put = put.into();
            nodes.push(put.execute(self).await?);
        }
        Ok(nodes)
    }

    /// Delete a value from the feed.
    ///
    /// # Examples
    ///
    /// Delete a basic node
    /// ```rust
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use hypertrie::{HyperTrie, PutOptions};
    /// let mut trie = HyperTrie::ram().await?;
    ///
    /// trie.put("hello", b"world").await?;
    /// assert_eq!(trie.delete("hello").await?, Some(()));
    /// assert_eq!(trie.get("hello").await?, None);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Delete a hidden node
    /// ```rust
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use hypertrie::{HyperTrie, PutOptions, DeleteOptions, GetOptions};
    /// let mut trie = HyperTrie::ram().await?;
    ///
    /// let node = trie.put(PutOptions::new("hello").hidden(), b"world").await?;
    /// assert_eq!(trie.delete("hello").await?, None);
    /// assert_eq!(trie.get(GetOptions::new("hello").hidden()).await?, Some(node));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&mut self, opts: impl Into<DeleteOptions>) -> anyhow::Result<Option<()>> {
        Ok(Delete::new(opts).execute(self).await?)
    }

    /// Delete a batch of values.
    ///
    /// # Example
    ///
    /// ```rust
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use hypertrie::{HyperTrie, PutOptions};
    /// let mut trie = HyperTrie::ram().await?;
    /// let nodes = trie
    ///     .batch_put(vec![
    ///         ("hello", b"world"),
    ///         ("world", b"hello"),
    ///         ("lorem", b"ipsum"),
    ///         ("brown", b"doggo"),
    ///    ])
    ///     .await?;
    ///
    /// let del = trie
    ///     .batch_delete(vec!["world", "hello", "ipsum", "yellow"])
    ///     .await?;
    /// assert_eq!(del, vec![Some(()), Some(()), None, None]);
    /// assert!(trie.get("hello").await?.is_none());
    /// # Ok(())
    /// # }
    /// ```
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

    ///
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

    /// Return the latest node in the feed.
    pub async fn head(&mut self) -> anyhow::Result<Option<Node>> {
        Ok(self.get_by_seq(self.head_seq()).await?)
    }

    /// The index of the latest node in the feed.
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

    pub fn iter_with_options(&mut self, opts: impl Into<IteratorOpts>) -> HyperTrieIterator<T> {
        HyperTrieIterator::new(opts, self)
    }

    pub fn iter(&mut self) -> HyperTrieIterator<T> {
        HyperTrieIterator::new(IteratorOpts::default(), self)
    }

    /// Same as [`HyperTrie::history`] but with options.
    ///
    /// # Example
    ///
    /// History but in reverse order.
    ///
    /// ```
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut trie = hypertrie::HyperTrie::ram().await?;
    /// let hello = trie.put("hello", b"world").await?;
    /// let world = trie.put("world", b"hello").await?;
    ///
    /// let mut history = trie.history_with_opts(false);
    /// let node = history.next().await.unwrap();
    /// assert_eq!(node.unwrap(), world);
    ///
    /// let node = history.next().await.unwrap();
    /// assert_eq!(node.unwrap(), hello);
    ///
    /// let node = history.next().await;
    /// assert!(node.is_none());
    /// # Ok(()) }
    /// ```
    pub fn history_with_opts(&mut self, opts: impl Into<HistoryOpts>) -> History<T> {
        let opts = opts.into();
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

impl HyperTrie<RandomAccessMemory> {
    pub async fn ram() -> anyhow::Result<HyperTrie<RandomAccessMemory>> {
        Ok(HyperTrieBuilder::default().ram().await?)
    }
}

impl HyperTrie<RandomAccessDisk> {
    pub async fn disk(dir: impl AsRef<Path>) -> anyhow::Result<HyperTrie<RandomAccessDisk>> {
        Ok(HyperTrieBuilder::default().disk(dir).await?)
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

    pub async fn disk(self, dir: impl AsRef<Path>) -> anyhow::Result<HyperTrie<RandomAccessDisk>> {
        let dir = dir.as_ref().to_path_buf();
        Ok(self.with_storage(Storage::new_disk(&dir).await?).await?)
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
        let mut trie = HyperTrie::ram().await?;

        let put = trie.put("hello", b"world").await?;
        let get = trie.get("hello").await?.unwrap();
        assert_eq!(put, get);

        Ok(())
    }

    #[async_std::test]
    async fn get_on_empty() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;
        let get = trie.get("hello").await?;
        assert!(get.is_none());

        Ok(())
    }

    #[async_std::test]
    async fn get_on_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        let node = trie.put("a", b"a").await?;
        assert_eq!(None, trie.get("").await?);

        let get = trie.get(GetOptions::new("").prefix()).await?;
        assert_eq!(Some(node), get);

        Ok(())
    }

    #[async_std::test]
    async fn ignore_leading_slash() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        let put = trie.put("/hello", b"world").await?;
        let get = trie.get("/hello").await?.unwrap();
        assert_eq!(put, get);

        let get = trie.get("hello").await?.unwrap();
        assert_eq!(put, get);

        Ok(())
    }

    #[async_std::test]
    async fn multiple_put_get() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

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
        let mut trie = HyperTrie::ram().await?;

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
        let mut trie = HyperTrie::ram().await?;

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
        let mut trie = HyperTrie::ram().await?;

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
        let mut trie = HyperTrie::ram().await?;

        let leaf_a = trie.put("hello/world", b"b").await?;
        let _ = trie.put("hello", b"a").await?;
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
        let mut trie = HyperTrie::ram().await?;

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
        let mut trie = HyperTrie::ram().await?;

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
        let mut trie = HyperTrie::ram().await?;

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
        let mut trie = HyperTrie::ram().await?;

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

    #[async_std::test]
    async fn hidden_put() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        let put = trie
            .put(PutOptions::new("hello").hidden(), b"world")
            .await?;

        let node = trie.get("hello").await?;
        assert!(node.is_none());

        let node = trie.get(GetOptions::new("hello").hidden()).await?;
        assert_eq!(node.unwrap(), put);

        Ok(())
    }

    #[async_std::test]
    async fn hidden_visible_not_collide() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        let hidden = trie
            .put(PutOptions::new("hello").hidden(), b"hidden")
            .await?;
        let not_hidden = trie.put("hello", b"not hidden").await?;

        let node = trie.get("hello").await?.unwrap();
        assert_eq!(node.key(), not_hidden.key());
        assert_eq!(node.value(), not_hidden.value());

        let node = trie.get(GetOptions::new("hello").hidden()).await?.unwrap();
        assert_eq!(node.key(), hidden.key());
        assert_eq!(node.value(), hidden.value());

        Ok(())
    }

    #[async_std::test]
    async fn hidden_delete() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        let _hidden = trie
            .put(PutOptions::new("hello").hidden(), b"hidden")
            .await?;

        assert!(trie.delete("hello").await?.is_none());
        assert!(trie
            .delete(DeleteOptions::new("hello").hidden())
            .await?
            .is_some());

        Ok(())
    }
}
