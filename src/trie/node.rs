use crate::hypertrie_proto as proto;
use crate::trie::Trie;
use prost::Message;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;

pub(crate) const HIDDEN_FLAG: u64 = 1;

#[derive(Debug, Clone)]
pub struct Node {
    /// index of the node in the Feed
    seq: u64,
    /// hypertrie key
    key: String,
    /// decoded node value from the stored valuebuffer in the node
    value: Option<Vec<u8>>,
    /// decoded trie from the stored triebuffer in the node
    trie: Trie,
    /// hashed key
    hash: Vec<u8>,
    /// flags of the stored node
    flags: Option<u64>,
}

impl Node {
    #[inline]
    pub fn terminator(idx: u64) -> bool {
        idx > 0 && idx & 31 == 0
    }

    #[inline]
    pub fn set_seq(&mut self, seq: u64) -> u64 {
        std::mem::replace(&mut self.seq, seq)
    }

    #[inline]
    pub fn len(&self) -> u64 {
        self.hash.len() as u64 * 4 + 1 + 1
    }

    #[inline]
    pub fn has_key(&self) -> bool {
        self.key.is_empty()
    }

    #[inline]
    pub(crate) fn trie(&self) -> &Trie {
        &self.trie
    }

    #[inline]
    pub(crate) fn trie_mut(&mut self) -> &mut Trie {
        &mut self.trie
    }

    pub(crate) fn bucket(&self, idx: usize) -> Option<&Vec<Option<u64>>> {
        self.trie.bucket(idx)
    }

    #[inline]
    pub fn has_value(&self) -> bool {
        self.value.is_some()
    }

    #[inline]
    pub fn is_hidden(&self) -> bool {
        if let Some(ref f) = self.flags {
            f & HIDDEN_FLAG == HIDDEN_FLAG
        } else {
            false
        }
    }

    #[inline]
    pub fn collides(&self, other: &Node, i: u64) -> bool {
        unimplemented!()
    }

    pub fn path(&self, mut idx: u64) -> u64 {
        if idx == 0 {
            return if self.is_hidden() { 1 } else { 0 };
        }
        idx -= 1;
        if let Some(h) = self.hash.get((idx >> 2) as usize) {
            (*h as u64 >> (2 * (idx & 3))) & 3
        } else {
            4
        }
    }

    pub(crate) fn encode(self) -> anyhow::Result<Vec<u8>> {
        let node = self.into_proto()?;
        let mut buf = Vec::with_capacity(node.encoded_len());
        node.encode(&mut buf)?;
        Ok(buf)
    }

    // TODO add encoding option
    pub(crate) fn into_proto(self) -> anyhow::Result<proto::Node> {
        // TODO shift flags?
        let mut node = proto::Node::default();
        if self.trie.is_empty() {
            node.trie_buffer = Some(self.trie.encode());
        }
        node.key = self.key;
        node.value_buffer = self.value;
        node.flags = self.flags;

        Ok(node)
    }

    pub(crate) fn decode(buf: &[u8], seq: u64) -> anyhow::Result<Self> {
        let node = proto::Node::decode(buf)?;
        // assumed key is correctly normalized before
        let hash = hash(split_key(&node.key));

        let trie = if let Some(trie) = node.trie_buffer {
            Trie::decode(&trie)?
        } else {
            Default::default()
        };

        Ok(Self {
            seq,
            key: node.key,
            value: node.value_buffer,
            trie,
            hash,
            flags: node.flags,
        })
    }
}

#[inline]
fn hash<'a>(keys: impl Iterator<Item = &'a str>) -> Vec<u8> {
    let mut hash = Vec::new();
    for key in keys {
        let mut h = DefaultHasher::default();
        key.hash(&mut h);
        hash.extend_from_slice(&u64::to_le_bytes(h.finish())[..]);
    }
    hash
}

#[inline]
fn normalize_key(key: &str) -> &str {
    key.chars()
        .next()
        .map(|c| &key[c.len_utf8()..])
        .unwrap_or("")
}

#[inline]
fn split_key(key: &str) -> impl Iterator<Item = &str> {
    key.split('/')
}

// fn split key on '/'

// fn hash(splitkey) {
// allocate Vec::with_capacity(8* splitkey.len())
// siphash24 each key in splitkey
// sip hasher use
// let hasher = DefaultHasher::new();
//}
