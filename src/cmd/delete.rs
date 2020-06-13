use std::fmt;

use futures::StreamExt;
use random_access_storage::RandomAccess;

use crate::cmd::put::{Put, PutOptions};
use crate::node::{Node, HIDDEN_FLAG};
use crate::HyperTrie;

#[derive(Clone, Debug)]
pub struct Delete {
    return_closest: bool,
    node: Node,
    closest: u64,
}

impl Delete {
    pub fn new(opts: impl Into<DeleteOptions>) -> Self {
        let opts = opts.into();
        let mut node = Node::new(opts.key, 0);

        let flags = if opts.hidden { HIDDEN_FLAG } else { 0 };
        node.set_flags(flags);

        Self {
            node,
            return_closest: opts.closest,
            closest: 0,
        }
    }

    pub fn len(&self) -> u64 {
        self.node.len()
    }

    // TODO put this in an async trait?
    pub(crate) async fn execute<T>(mut self, db: &mut HyperTrie<T>) -> anyhow::Result<Option<Node>>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        if let Some(head) = db.head().await? {
            Ok(self.update(0, head, db).await?)
        } else {
            Ok(None)
        }
    }

    async fn update<T>(
        mut self,
        mut seq: u64,
        mut head: Node,
        db: &mut HyperTrie<T>,
    ) -> anyhow::Result<Option<Node>>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        let mut i = seq;

        while i < self.len() {
            let val = self.node.path(i);
            // println!("\ni {}, val {}, head_val {}, seq {}", i, val, head_val, seq);
            let bucket = head.bucket(i as usize);

            if let Some(bucket) = bucket.clone() {
                if head.path(i) == val {
                    if let Some(closest) = Self::first_seq(bucket, val) {
                        self.closest = closest;
                        continue;
                    }
                }
            }

            self.closest = head.seq();

            if let Some(bucket) = bucket {
                // update head
                if let Some(s) = bucket.get(val as usize).cloned().flatten() {
                    if let Some(h) = db.get_by_seq(s).await? {
                        head = h
                    }
                    seq = i + 1;
                    i = seq;
                    continue;
                }
            }
            return Ok(None);
        }

        // TODO collisions
        if self.node.key() != head.key() {
            return Ok(None);
        }

        Ok(self.splice_closest(head, db).await.map(Option::Some)?)
    }

    async fn splice_closest<T>(
        &mut self,
        mut head: Node,
        db: &mut HyperTrie<T>,
    ) -> anyhow::Result<Node>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        if self.closest == 0 {
            Ok(self.splice(None, head, db).await?)
        } else {
            let closest = db.get_by_seq(self.closest).await?;
            Ok(self.splice(closest, head, db).await?)
        }
    }

    async fn splice<T>(
        &mut self,
        closest: Option<Node>,
        node: Node,
        db: &mut HyperTrie<T>,
    ) -> anyhow::Result<Node>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        let put = if let Some(mut closest) = closest {
            let hidden = closest.is_hidden();
            let flags = closest.flags.map(|f| f >> 8).unwrap_or_default();
            let opts = PutOptions::new(closest.key).set_hidden(hidden).flags(flags);
            Put::new_delete(opts, node.seq, closest.value)
        } else {
            Put::new_delete(
                PutOptions::new("").set_hidden(node.is_hidden()),
                node.seq,
                None,
            )
        };

        Ok(put.execute(db).await?)
    }

    fn first_seq(bucket: &Vec<Option<u64>>, val: u64) -> Option<u64> {
        for i in 0..bucket.len() as u64 {
            if i == val {
                continue;
            }
            if let Some(seq) = bucket.get(i as usize).cloned().flatten() {
                return Some(seq);
            }
        }
        None
    }
}

#[derive(Clone, Debug)]
pub struct DeleteOptions {
    key: String,
    closest: bool,
    hidden: bool,
}

impl DeleteOptions {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            closest: false,
            hidden: false,
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

    pub fn set_hidden(mut self, hidden: bool) -> Self {
        self.hidden = hidden;
        self
    }

    pub fn hidden(mut self) -> Self {
        self.hidden = true;
        self
    }
}

// used so we can pass a single str as well as configured options to the put function
impl<T: Into<String>> From<T> for DeleteOptions {
    fn from(s: T) -> Self {
        Self::new(s)
    }
}
