use std::fmt;
use std::ops::Range;

use random_access_storage::RandomAccess;

use crate::cmd::get::{Get, GetOptions};
use crate::cmd::TrieCommand;
use crate::node::Node;
use crate::HyperTrie;

pub struct DiffCheckoutStream<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    stack: Vec<Entry>,
    checkout_set: bool,
    opts: DiffOptions,
    target_checkout: u64,
    db_checkout: Option<u64>,
    db: &'a mut HyperTrie<T>,
    needs_check: Vec<usize>,
    pending: u64,
    prefix: String,
    hidden: bool,
    skip_right_null: bool,
    skip_left_null: bool,
}

impl<'a, T> DiffCheckoutStream<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    fn toggle_checkout(&mut self) {
        if self.checkout_set {
            self.checkout()
        } else {
            self.un_checkout()
        }
    }

    fn checkout(&mut self) {
        self.db.set_checkout(self.target_checkout);
        self.checkout_set = true;
    }

    fn un_checkout(&mut self) {
        if let Some(c) = self.db_checkout {
            self.db.set_checkout(c);
        } else {
            self.db.remove_checkout();
        }
        self.checkout_set = false
    }

    async fn checkout_get(&mut self, opts: impl Into<GetOptions>) -> anyhow::Result<Option<Node>> {
        self.checkout();
        let node = self.db.get(opts).await?;
        self.un_checkout();
        Ok(node)
    }

    async fn open(&mut self) -> anyhow::Result<()> {
        let opts = GetOptions::new(self.prefix.clone())
            .prefix()
            .set_hidden(self.hidden);

        let get = Get::new(opts.clone());
        let i = get.len();

        let left = get.execute(self.db).await?;
        let right = self.checkout_get(opts).await?;

        self.stack.push(Entry {
            i,
            left,
            right,
            skip: false,
        });
        Ok(())
    }

    pub async fn next(&mut self) -> Option<()> {
        unimplemented!()
    }

    async fn get_node(&mut self, _seq: u64, _top: u64, _left: u64) {
        unimplemented!()
        // loop {
        //     break;
        // }
    }

    async fn finalize(&mut self) {
        while !self.needs_check.is_empty() {
            let end = self.needs_check.pop().unwrap();
            let start = self.needs_check.pop().unwrap();
            self.maybe_collides(start..end)
        }

        // self.next()
        unimplemented!()
    }

    // all nodes, start -> end, share the same hash
    // we need to check that there are no collisions
    fn maybe_collides(&mut self, range: Range<usize>) {
        // much simpler and *much* more likely - only one node
        if range.end - range.start == 1 {
            if let Some(entry) = self.stack.get_mut(range.start).and_then(|top| {
                if top.collides() {
                    Some(Entry {
                        i: top.i,
                        left: None,
                        right: top.right.take(),
                        skip: top.skip,
                    })
                } else {
                    None
                }
            }) {
                self.stack.push(entry);
            }
            return;
        }

        // very unlikely, but multiple collisions or a trie reordering
        // due to a collision being deleted

        let mut i = range.start;
        while i < range.end && i < self.stack.len() {
            let mut top = self.stack.remove(i);
            let mut swapped = false;

            if top.right.is_some() {
                let from = i;
                for j in from..std::cmp::min(range.end, self.stack.len()) {
                    let mut other = &mut self.stack[j];
                    let mut collides = false;

                    if let Some(ref left) = other.left {
                        if let Some(ref right) = top.right {
                            collides = left.collides(right, top.i);
                        }
                    }

                    if collides {
                        std::mem::swap(&mut other.right, &mut top.right);
                        swapped = true;
                        i -= 1;
                        break;
                    }
                }
            }

            if !swapped && top.left.is_some() {
                self.stack.push(Entry {
                    i: top.i,
                    left: None,
                    right: top.right.clone(),
                    skip: false,
                });
            }

            self.stack.insert(i, top);
            i += i;
        }
    }

    fn push_stack(&mut self, len: usize, i: u64) -> Option<&Entry> {
        if self.stack.len() == len {
            self.stack.push(Entry::new(i));
        }
        self.stack.get(len)
    }
}

pub struct DiffStream<'a, Left, Right>
where
    Left: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    Right: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    stack: Vec<Entry>,
    left: &'a mut HyperTrie<Left>,
    right: &'a mut HyperTrie<Right>,
}

impl<'a, Left, Right> DiffStream<'a, Left, Right>
where
    Left: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    Right: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    pub async fn next(&mut self) -> Option<()> {
        unimplemented!()
    }
}

fn not_in_bucket(bucket: &[Option<u64>], val: u64, seq: u64) -> bool {
    let mut val = val as usize;
    while val < bucket.len() {
        if bucket.get(val).cloned().flatten() == Some(seq) {
            return false;
        }
        val += 5
    }
    true
}

#[derive(Debug, Clone)]
struct Entry {
    i: u64,
    left: Option<Node>,
    right: Option<Node>,
    skip: bool,
}

impl Entry {
    fn new(i: u64) -> Self {
        Self {
            i,
            left: None,
            right: None,
            skip: false,
        }
    }

    fn collides(&self) -> bool {
        if self.left.is_none() || self.right.is_none() || !Node::terminator(self.i) {
            false
        } else {
            self.left
                .as_ref()
                .unwrap()
                .collides(self.right.as_ref().unwrap(), self.i)
        }
    }
}

pub struct TrieDiff {
    key: String,
    left: Option<Node>,
    right: Option<Node>,
}

impl TrieDiff {}

impl TrieDiff {
    fn is_same(&self) -> bool {
        self.left == self.right
    }

    fn is_different(&self) -> bool {
        !self.is_same()
    }
}

#[derive(Clone, Debug)]
pub struct DiffOptions {
    checkout: u64,
    prefix: Option<String>,
    closest: bool,
    hidden: bool,
    skip_right_null: bool,
    skip_left_null: bool,
    reconnect: bool,
    // TODO is this a buffer?
    checkpoint: Option<u64>,
}

impl DiffOptions {
    pub fn new(checkout: u64) -> Self {
        Self {
            checkout,
            prefix: None,
            closest: false,
            hidden: false,
            skip_right_null: false,
            skip_left_null: false,
            reconnect: false,
            checkpoint: None,
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

impl From<u64> for DiffOptions {
    fn from(checkout: u64) -> Self {
        Self::new(checkout)
    }
}
