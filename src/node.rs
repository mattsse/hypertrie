use crate::hypertrie_proto as proto;
use crate::trie::{Trie};
use prost::Message;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::{Div, Sub};
use std::path::Path;

pub(crate) const HIDDEN_FLAG: u64 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// hypertrie key
    pub(crate) key: String,
    /// index of the node in the Feed
    pub(crate) seq: u64,
    /// decoded node value from the stored valuebuffer in the node
    pub(crate) value: Option<Vec<u8>>,
    /// decoded trie from the stored triebuffer in the node
    pub(crate) trie: Trie,
    /// hashed key
    pub(crate) hash: Vec<u8>,
    /// flags of the stored node
    pub(crate) flags: Option<u64>,
}

impl Node {
    #[inline]
    pub fn terminator(idx: u64) -> bool {
        idx > 0 && idx & 31 == 0
    }

    pub(crate) fn hash_key(key: &str) -> Vec<u8> {
        hash_sip24(split_key(key))
    }

    pub fn new(key: impl Into<String>, seq: u64) -> Self {
        let key = key.into();
        let key = if key.starts_with('/') {
            key.replacen('/', "", 1)
        } else {
            key
        };
        let hash = Self::hash_key(&key);
        Self {
            key,
            seq,
            value: None,
            trie: Default::default(),
            hash,
            flags: None,
        }
    }

    pub fn with_value(key: impl Into<String>, seq: u64, value: Vec<u8>) -> Self {
        let mut n = Self::new(key, seq);
        n.value = Some(value);
        n
    }

    #[inline]
    pub fn set_value(&mut self, value: Vec<u8>) -> Option<Vec<u8>> {
        std::mem::replace(&mut self.value, Some(value))
    }

    #[inline]
    pub fn set_flags(&mut self, flags: u64) -> Option<u64> {
        self.flags.replace(flags)
    }

    #[inline]
    pub fn flags(&self) -> Option<u64> {
        self.flags.clone()
    }

    #[inline]
    pub fn set_seq(&mut self, seq: u64) -> u64 {
        std::mem::replace(&mut self.seq, seq)
    }

    #[inline]
    pub fn seq(&self) -> u64 {
        self.seq
    }

    #[inline]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[inline]
    pub fn value(&self) -> Option<&Vec<u8>> {
        self.value.as_ref()
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
    pub fn compare(&self, other: &Node) -> i64 {
        let min = std::cmp::min(self.len(), other.len());
        for i in 0..min {
            let diff = self.path(i) as i64 - other.path(i) as i64;
            if diff != 0 {
                return diff;
            }
        }
        0
    }

    #[inline]
    pub fn collides(&self, other: &Node, i: u64) -> bool {
        if i == 0 {
            return false;
        }
        if i == self.len() - 1 {
            return self.key() != other.key();
        }

        let j = ((i - 1) / 32) as usize;
        if let Some(a) = split_key(self.key()).nth(j) {
            if let Some(b) = split_key(other.key()).nth(j) {
                return a != b;
            }
        }
        false
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

    pub(crate) fn encode(&self) -> anyhow::Result<Vec<u8>> {
        let node = self.as_proto()?;
        let mut buf = Vec::with_capacity(node.encoded_len());
        node.encode(&mut buf)?;
        Ok(buf)
    }

    pub fn from_proto(node: proto::Node) -> anyhow::Result<Self> {
        // assumed key is correctly normalized before
        let hash = Self::hash_key(&node.key);

        let trie = if let Some(trie) = node.trie_buffer {
            Trie::decode(&trie)
        } else {
            Default::default()
        };

        Ok(Self {
            seq: node.seq.unwrap_or_default(),
            key: node.key,
            value: node.value_buffer,
            trie,
            hash,
            flags: node.flags,
        })
    }

    // TODO add encoding option
    pub(crate) fn as_proto(&self) -> anyhow::Result<proto::Node> {
        // TODO shift flags?
        let mut node = proto::Node {
            key: self.key.clone(),
            value_buffer: self.value.clone(),
            trie_buffer: None,
            seq: Some(self.seq),
            flags: self.flags,
        };
        if !self.trie.is_empty() {
            node.trie_buffer = Some(self.trie.encode());
        }
        Ok(node)
    }

    pub(crate) fn into_proto(self) -> anyhow::Result<proto::Node> {
        // TODO shift flags?
        let mut node = proto::Node {
            key: self.key,
            value_buffer: self.value,
            trie_buffer: None,
            seq: Some(self.seq),
            flags: self.flags,
        };
        if self.trie.is_empty() {
            node.trie_buffer = Some(self.trie.encode());
        }
        Ok(node)
    }

    pub(crate) fn decode(buf: &[u8], seq: u64) -> anyhow::Result<Self> {
        Ok(Self::from_proto(proto::Node::decode(buf)?)?)
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
fn hash_sip24<'a>(keys: impl Iterator<Item = &'a str>) -> Vec<u8> {
    let mut hash = Vec::new();
    for key in keys {
        let mut h = siphasher::sip::SipHasher::new_with_keys(0, 0);
        h.write(key.as_bytes());
        hash.extend_from_slice(&u64::to_le_bytes(h.finish())[..]);
    }
    hash
}

#[inline]
fn split_key(key: &str) -> impl Iterator<Item = &str> {
    key.split('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collision() {
        let a = Node::new("hello", 0);
        assert!(!a.collides(&a.clone(), 0));

        let a = Node::new("hello/world", 0);
        assert!(!a.collides(&a.clone(), 1));
        assert!(!a.collides(&a.clone(), 2));

        let b = Node::new("root/world", 0);
        assert!(a.collides(&b, 1));
        assert!(!a.collides(&b, 64));
        assert!(!a.collides(&b, 33));
        assert!(a.collides(&b, 65));

        let b = Node::new("root/etc/world", 0);
        assert!(a.collides(&b, 33));
        assert!(a.collides(&b, 65));
    }

    #[test]
    fn sip24() {
        let hash = hash_sip24(split_key("hello"));
        assert_eq!(hash, vec![185, 82, 247, 178, 93, 93, 193, 140]);

        let hash = hash_sip24(split_key("hello/world"));
        assert_eq!(
            hash,
            vec![185, 82, 247, 178, 93, 93, 193, 140, 110, 176, 189, 160, 206, 65, 131, 168]
        );
    }

    #[test]
    fn path() {
        let a = Node::new("hello", 0);
        let b = Node::new("world", 0);
        assert_eq!(a.path(0), 0);
        assert_eq!(a.path(0), b.path(0));
        assert_eq!(a.path(1), 1);
        assert_eq!(b.path(1), 2);
        assert_eq!(b.path(4), 1);
        assert_eq!(b.path(100), 4);
    }
}
