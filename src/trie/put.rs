use crate::trie::node::{Node, HIDDEN_FLAG};
use crate::trie::{HyperTrie, Trie};
use prost::Message;
use random_access_storage::RandomAccess;
use std::fmt;

#[derive(Clone, Debug)]
pub struct Put {
    closest: bool,
    hidden: bool,
    flags: u64,
    prefix: Option<String>,
    node: Node,
    head: u64,
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
        let mut node = Node::with_value(opts.key, 0, value);
        let mut flags = opts.flags << 8;
        if opts.hidden {
            flags | HIDDEN_FLAG;
        } else {
            flags | 0;
        }
        node.set_flags(flags);

        Self {
            node,
            closest: opts.closest,
            prefix: opts.prefix,
            hidden: opts.hidden,
            head: 0,
            flags: opts.flags,
        }
    }

    // TODO put this in an async trait?
    pub(crate) async fn execute<T>(mut self, db: &mut HyperTrie<T>) -> anyhow::Result<Node>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        self.head = db.head_seq();

        // TODO handle _sendExt

        if let Some(head) = db.get_by_seq(self.head).await? {
            Ok(self.update(0, head, db).await?)
        } else {
            self.finalize(db).await?;
            Ok(self.node)
        }
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

            if let Some(bucket) = bucket.clone() {
                for j in 0..bucket.len() as u64 {
                    if !check_collision && j == val {
                        continue;
                    }

                    if let Some(s) = bucket.get(j as usize).cloned().flatten() {
                        if check_collision {
                            self.push_collidable(i, j, s, db).await?;
                        } else {
                            // TODO only if (seq !== this._del)
                            Self::push(self.node.trie_mut(), i, j, s);
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
            Self::push(self.node.trie_mut(), i, head_val, head.seq());

            if check_collision {
                if let Some(bucket) = bucket.clone() {
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
                    seq += 1;
                    i = seq;
                    continue;
                }
            }
            break;

            i += 1;
        }

        // TODO this._head = head?
        self.finalize(db).await?;
        Ok(self.node)
    }

    fn push(trie: &mut Trie, i: u64, mut val: u64, seq: u64) {
        while val >= 5 {
            val -= 5;
        }
        let bucket = trie.bucket_or_insert(i as usize);
        while bucket.len() as u64 > val && bucket.get(val as usize).is_some() {
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
        // TODO if (seq === this._del) return
        if let Some(other) = db.get_by_seq(seq).await? {
            if other.collides(&self.node, i) {
                Self::push(self.node.trie_mut(), i, val, seq)
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

#[derive(Clone, Debug)]
pub struct PutOptions {
    key: String,
    closest: bool,
    hidden: bool,
    prefix: Option<String>,
    flags: u64,
}

// used so we can pass a single str as well as configured options to the put function
impl<T: Into<String>> From<T> for PutOptions {
    fn from(s: T) -> Self {
        Self {
            key: s.into(),
            closest: false,
            prefix: None,
            hidden: false,
            flags: 0,
        }
    }
}
