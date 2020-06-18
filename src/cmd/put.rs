use std::fmt;

use async_trait::async_trait;
use prost::Message;
use random_access_storage::RandomAccess;

use crate::node::{Node, HIDDEN_FLAG};
use crate::trie::Trie;
use crate::{HyperTrie, TrieCommand};

#[derive(Clone, Debug)]
pub struct Put {
    closest: bool,
    hidden: bool,
    flags: u64,
    prefix: Option<String>,
    node: Node,
    head: u64,
    delete: Option<u64>,
}

impl Put {
    pub fn len(&self) -> u64 {
        if self.prefix.is_some() {
            self.node.len() - 1
        } else {
            self.node.len()
        }
    }

    pub fn new(opts: impl Into<PutOptions>, value: Vec<u8>) -> Self {
        let opts = opts.into();
        opts.into_put_with_value(value)
    }

    pub(crate) fn new_delete(
        opts: impl Into<PutOptions>,
        delete: u64,
        value: Option<Vec<u8>>,
    ) -> Self {
        let opts = opts.into();
        let mut put: Put = opts.into();
        if let Some(value) = value {
            put.node.set_value(value);
        }
        put.delete = Some(delete);
        put
    }

    async fn update<T>(
        mut self,
        mut seq: u64,
        mut head: Node,
        db: &mut HyperTrie<T>,
    ) -> anyhow::Result<Node>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        let mut i = seq;

        while i < self.len() {
            let check_collision = Node::terminator(i);
            let val = self.node.path(i);
            let head_val = head.path(i);
            let bucket = head.bucket(i as usize);

            if let Some(bucket) = bucket {
                for j in 0..bucket.len() as u64 {
                    if !check_collision && j == val {
                        continue;
                    }

                    if let Some(s) = bucket.get(j as usize).cloned().flatten() {
                        if check_collision {
                            self.push_collidable(i, j, s, db).await?;
                        } else {
                            // TODO only if (seq !== this._del)
                            self.push(i, j, s);
                        }
                    }
                }
            }

            // we copied the head bucket, if this is still the closest node, continue
            // if no collision is possible
            if head_val == val && (!check_collision || !self.node.collides(&head, i)) {
                i += 1;
                continue;
            }

            // TODO only  if (seq !== this._del)
            self.push(i, head_val, head.seq());

            if check_collision {
                if let Some(bucket) = bucket {
                    // update head collides
                    let mut missing = 1u64;
                    let mut node = None;

                    for j in (val as usize..bucket.len()).step_by(5) {
                        if let Some(s) = bucket.get(j).cloned().flatten() {
                            missing += 1;
                            if let Some(n) = db.get_by_seq(s).await? {
                                if !n.collides(&self.node, i) {
                                    node = Some(n);
                                }

                                missing -= 1;
                                if missing > 0 {
                                    continue;
                                }

                                if node.is_none() {
                                    self.finalize(db).await?;
                                    continue;
                                }
                                seq += 1;
                                i = seq;
                                continue;
                            }
                        } else {
                            break;
                        }
                    }
                } else {
                    seq += 1;
                    i = seq;
                    continue;
                }
            }

            if let Some(bucket) = bucket {
                // update head
                if let Some(s) = bucket.get(val as usize).cloned().flatten() {
                    // update head
                    if let Some(h) = db.get_by_seq(s).await? {
                        head = h
                    }
                    seq = i + 1;
                    i = seq;
                    continue;
                }
            }
            break;
        }

        // TODO this._head = head?
        self.finalize(db).await?;
        Ok(self.node)
    }

    fn push(&mut self, i: u64, mut val: u64, seq: u64) {
        if Some(seq) == self.delete {
            return;
        }

        while val >= 5 {
            val -= 5;
        }

        let bucket = self.node.trie_mut().bucket_or_insert(i as usize);

        while bucket.len() as u64 > val && bucket.get(val as usize).cloned().flatten().is_some() {
            val += 5
        }
        if !bucket.contains(&Some(seq)) {
            Trie::insert_value(val as usize, seq, bucket);
        }
    }

    async fn push_collidable<T>(
        &mut self,
        i: u64,
        val: u64,
        seq: u64,
        db: &mut HyperTrie<T>,
    ) -> anyhow::Result<()>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        if Some(seq) == self.delete {
            return Ok(());
        }

        if let Some(other) = db.get_by_seq(seq).await? {
            if other.collides(&self.node, i) {
                self.push(i, val, seq)
            }
            // TODO finalize()?
        }

        Ok(())
    }

    async fn finalize<T>(&mut self, db: &mut HyperTrie<T>) -> anyhow::Result<u64>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        let seq = db.feed.len();
        self.node.set_seq(seq);
        db.feed.append(&self.node.encode()?).await?;
        Ok(seq)
    }
}

#[async_trait]
impl TrieCommand for Put {
    type Item = anyhow::Result<Node>;

    async fn execute<T>(mut self, db: &mut HyperTrie<T>) -> Self::Item
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        self.head = db.head_seq();

        // TODO handle _sendExt

        if let Some(head) = db.get_by_seq(self.head).await? {
            Ok(self.update(0, head, db).await?)
        } else {
            self.finalize(db).await?;
            self.node.shift_flags();
            Ok(self.node)
        }
    }
}

#[derive(Clone, Debug)]
pub struct PutOptions {
    key: String,
    closest: bool,
    hidden: bool,
    prefix: Option<String>,
    flags: u64,
}

impl PutOptions {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            closest: false,
            prefix: None,
            hidden: false,
            flags: 0,
        }
    }

    pub fn set_closest(mut self, closest: bool) -> Self {
        self.closest = closest;
        self
    }

    pub fn closest(mut self) -> Self {
        self.closest = true;
        self
    }

    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn set_hidden(mut self, hidden: bool) -> Self {
        self.hidden = hidden;
        self
    }

    pub fn hidden(mut self) -> Self {
        self.hidden = true;
        self
    }

    pub fn flags(mut self, flags: u64) -> Self {
        self.flags = flags;
        self
    }

    pub fn into_put_with_value(self, value: Vec<u8>) -> Put {
        let mut put: Put = self.into();
        put.node.set_value(value);
        put
    }
}

// used so we can pass a single str as well as configured options to the put function
impl<T: Into<String>> From<T> for PutOptions {
    fn from(s: T) -> Self {
        Self::new(s)
    }
}

impl Into<Put> for PutOptions {
    fn into(self) -> Put {
        let mut node = Node::new(self.key, 0);
        let flags = self.flags << 8;
        let flags = if self.hidden {
            flags | HIDDEN_FLAG
        } else {
            flags
        };
        node.set_flags(flags);

        Put {
            node,
            closest: self.closest,
            prefix: self.prefix,
            hidden: self.hidden,
            head: 0,
            flags: self.flags,
            delete: None,
        }
    }
}

impl<K, V> From<(K, V)> for Put
where
    K: Into<String>,
    V: AsRef<[u8]>,
{
    fn from((key, val): (K, V)) -> Self {
        let opts = PutOptions::from(key);
        opts.into_put_with_value(val.as_ref().to_vec())
    }
}
