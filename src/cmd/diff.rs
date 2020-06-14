use std::fmt;

use random_access_storage::RandomAccess;

use crate::cmd::get::{Get, GetOptions};
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
    pending: u64,
    prefix: String,
    hidden: bool,
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
        loop {
            break;
        }
    }

    async fn finalize(&mut self) {
        unimplemented!()
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
