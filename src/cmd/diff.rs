use crate::node::Node;
use crate::HyperTrie;
use random_access_storage::RandomAccess;
use std::fmt;

pub struct DiffCheckoutStream<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    stack: Vec<Entry>,
    checkout_set: bool,
    target_checkout: u64,
    initial_checkout: Option<u64>,
    db: &'a mut HyperTrie<T>,
}

impl<'a, T> DiffCheckoutStream<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    fn toggle_checkout(&mut self) {}
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
}

#[derive(Debug, Clone)]
struct Entry {
    i: u64,
    left: u64,
    right: u64,
    skip: bool,
}

pub struct TrieDiff {
    key: String,
    left: Option<Node>,
    right: Option<Node>,
}

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
