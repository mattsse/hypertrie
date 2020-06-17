use crate::{discovery_key, HyperTrie};
use hypercore::{Feed, PublicKey};
use random_access_storage::RandomAccess;
use std::collections::HashMap;
use std::fmt;

mod mount_proto {
    include!(concat!(env!("OUT_DIR"), "/mount_pb.rs"));
}

const MOUNT_FLAG: u64 = 1;

const MOUNT_PREFIX: &str = "/mounts";

/// A Hypertrie wrapper that supports mounting of sub-Hypertries.
pub struct MountableHyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    key: Option<u64>,
    trie: HyperTrie<T>,
    tries: HashMap<u64, u64>,
    checkouts: HashMap<u64, u64>,
}

impl<T> MountableHyperTrie<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    /// Returns the db discovery key. Can be used to find other db peers.
    pub fn discovery_key(&self) -> blake2_rfc::blake2b::Blake2bResult {
        self.trie.discovery_key()
    }

    pub async fn get(&self) {}

    /// - `path` is the mountpoint
    /// - `key` is the key for the `MountableHypertrie` to be mounted at `path`
    async fn mount(&mut self, path: impl Into<MountOps>, key: PublicKey) {}

    /// Remove a mount from a mountpoint
    async fn delete(&mut self, path: &str) {}

    pub async fn put(&mut self) {}
}

#[derive(Debug, Clone)]
pub struct MountableHyperTrieOpts {}

#[derive(Debug, Clone)]
pub struct MountOps {
    path: String,
    version: Option<u64>,
    remote_path: Option<String>,
}

impl<T: Into<String>> From<T> for MountOps {
    fn from(path: T) -> Self {
        Self {
            path: path.into(),
            version: None,
            remote_path: None,
        }
    }
}
