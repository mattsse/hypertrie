use std::fmt;
use std::path::{Path, PathBuf};

use async_std::fs;
use async_std::prelude::*;
use futures::FutureExt;
use hypercore::{generate_keypair, Feed, FeedBuilder, PublicKey, Storage, Store};
use rand::RngCore;
use random_access_disk::RandomAccessDisk;
use random_access_storage::RandomAccess;

use crate::discovery_key;
pub use crate::{HyperTrie, HyperTrieBuilder};

const MASTER_KEY_FILENAME: &'static str = "master_key";

/// Hyperdrive indexes filenames into a Hypercore. To avoid having to scan
/// through this entire Hypercore when you want to find a specific file or
/// folder, filenames are indexed using an append-only hash trie, which are
/// called Hypertrie. The hash trie basically functions as a fast append-only
/// key value store with listable folders.
pub struct Hyperdrive<TConent, TMetadata = TConent>
where
    TConent: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    TMetadata: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    dir: PathBuf,
    content: Feed<TConent>,
    metadata: Feed<TMetadata>,
}

// TODO metadata = `db` in js and has its own feed
//

impl<TContent, TMetadata> Hyperdrive<TContent, TMetadata>
where
    TContent: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    TMetadata: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    /// If the hyperdrive has already been created, wait for the db (metadata
    /// feed) to load. If the metadata feed is writable, we can immediately load
    /// the content feed from its private key. (Otherwise, we need to read the
    /// feed's metadata block first)
    pub async fn restore(keypair: String) {}

    pub async fn write_file(&self) {}

    pub async fn read_file(&self) {}

    async fn mkdir(&self) {}

    pub fn is_writable(&self) -> bool {
        unimplemented!()
    }

    pub async fn update(&self) {}

    fn storage_root(&self) {}

    async fn ready(&mut self) {}

    // TODO API: https://awesome.datproject.org/hyperdrive#
}

impl Hyperdrive<RandomAccessDisk> {
    /// Create a new instance that persists to disk at the location of `dir`.
    pub async fn open(dir: impl AsRef<Path>) -> anyhow::Result<()> {
        let dir = dir.as_ref();
        if !dir.exists() {
            fs::create_dir_all(dir).await?;
        } else {
            assert!(dir.is_dir());
        }

        // write master key: random 32 bytes
        let mut master_key = [0u8; 32];
        fill_random_bytes(&mut master_key);

        let mut master_file = fs::File::create(dir.join(MASTER_KEY_FILENAME)).await?;
        master_file.write_all(&master_key).await?;

        // create metadata and content feed

        Ok(())
    }

    async fn new_feed(dir: impl AsRef<Path>) -> anyhow::Result<Feed<RandomAccessDisk>> {
        let dir = dir.as_ref();
        let keypair = generate_keypair();

        let storage = |storage: Store| {
            let name = match storage {
                Store::Tree => "tree",
                Store::Data => "data",
                Store::Bitfield => "bitfield",
                Store::Signatures => "signatures",
                Store::Keypair => "key",
            };

            let dkey = hex::encode(&discovery_key(&keypair.public));

            // storage root is `<dir>/dkey[0..2]/dkey[2..4]/<dkey>`
            let storage_root = Path::new(&dkey.as_str()[0..2])
                .join(&dkey.as_str()[2..4])
                .join(dkey);

            let file = dir.join(storage_root).join(name);

            RandomAccessDisk::open(file).boxed()
        };

        let mut storage = Storage::new(storage).await?;
        storage.write_public_key(&keypair.public).await?;
        storage.write_secret_key(&keypair.secret).await?;

        Ok(FeedBuilder::new(keypair.public, storage)
            .secret_key(keypair.secret)
            .build()?)
    }
}

#[derive(Debug, Clone)]
pub struct HyperdriveBuilder {
    key: String,
    sparse: bool,
    namespace: Option<String>,
    key_pair: Option<()>,
}

// TODO impl AsyncRead AsyncWrite

struct Feed2 {}

fn fill_random_bytes(dest: &mut [u8]) {
    use rand::rngs::{OsRng, StdRng};
    use rand::SeedableRng;
    let mut rng = StdRng::from_rng(OsRng::default()).unwrap();
    rng.fill_bytes(dest)
}
