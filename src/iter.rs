use crate::{HyperTrie, Node};
use rand::Rng;
use random_access_storage::RandomAccess;
use std::fmt;
use std::ops::Range;

const SORT_ORDER: [u64; 5] = [3, 2, 1, 0, 4];
const REVERSE_SORT_ORDER: [u64; 5] = [4, 0, 1, 2, 3];

pub struct HyperTrieIterator<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    db: &'a mut HyperTrie<T>,
    prefix: String,
    flags: Option<u64>,
    order: SortOrder,
    recursive: u64,
    stack: Vec<Entry>,
    gt: Option<u64>,
    range: Range<u64>,
    pending: u64,
    had_missing_block: bool,
    needs_sort: Vec<u64>,
    random: bool,
}

impl<'a, T> HyperTrieIterator<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    pub fn new(opts: impl Into<IteratorOpts>, db: &'a mut HyperTrie<T>) -> Self {
        let mut opts = opts.into();

        if let Some(flags) = opts.flags {
            opts = opts
                .recursive(flags & 1)
                .reverse(flags & 2)
                .gt(flags & 4)
                .hidden(flags & 8);
        }

        unimplemented!()
    }

    async fn next(&mut self) -> Option<anyhow::Result<Node>> {
        unimplemented!()
    }

    async fn push(&mut self, i: u64, seq: u64) -> anyhow::Result<()> {
        if let Some(node) = self.db.get_by_seq(seq).await? {
            let top = Entry { i, seq, node };
            self.pending += 1;
            self.stack.push(top);

            if !self.had_missing_block && !self.db.feed_mut().has(seq) {
                self.had_missing_block = true;
            }
        }
        Ok(())
    }

    async fn pop_stack(&mut self) -> anyhow::Result<Entry> {
        while let Some(mut top) = self.stack.pop() {
            let len = std::cmp::min(top.node.len(), self.range.end);
            let i = top.i;
            top.i += 1;

            if i >= len {
                return Ok(top);
            }

            let bucket = top.node.trie.bucket(i as usize);

            let order = if self.random {
                Self::random_order()
            } else {
                self.order.order()
            };

            for val in order.iter().cloned() {
                if val != 4 || self.gt.is_none() || i != self.range.start {}

                // TODO bucket

                if self
                    .stack
                    .len()
                    .checked_sub(len as usize)
                    .unwrap_or_default()
                    > 1
                {
                    self.needs_sort
                        .extend_from_slice(&[len, self.stack.len() as u64]);
                }
            }
        }

        unimplemented!()
    }

    /// only ran when there are potential collisions to make sure
    /// the iterator sorts consistently
    fn sort(&mut self) {
        while let Some(end) = self.needs_sort.pop() {
            let start = self.needs_sort.pop().unwrap() as usize;
            Self::sort_stack(&mut self.stack, start..end as usize);
        }
    }

    fn sort_stack(list: &mut [Entry], range: Range<usize>) {
        for i in range.start + 1..range.end {
            for j in (range.start + 1..=i).rev() {
                if let Some(a) = list.get(j) {
                    if let Some(b) = list.get(j - 1) {
                        if a.node.key() <= b.node.key() {
                            break;
                        }
                    }
                }

                list.swap(j as usize, j - 1);
            }
        }
    }
    #[inline]
    fn random_order() -> [u64; 5] {
        use rand::{seq::SliceRandom, RngCore};
        let mut order = [0, 1, 2, 3, 4];
        order.shuffle(&mut rand::thread_rng());
        order
    }
}

struct Entry {
    i: u64,
    seq: u64,
    node: Node,
}

struct ExtensionState {
    active: bool,
    missing: u64,
    head: u64,
}

#[derive(Debug, Clone)]
enum SortOrder {
    Normal,
    Reverse,
}

impl SortOrder {
    fn order(&self) -> [u64; 5] {
        match self {
            SortOrder::Normal => SORT_ORDER,
            SortOrder::Reverse => REVERSE_SORT_ORDER,
        }
    }
}

impl Default for SortOrder {
    fn default() -> Self {
        SortOrder::Normal
    }
}

#[derive(Debug, Clone)]
pub struct IteratorOpts {
    prefix: String,
    flags: Option<u64>,
    order: Option<SortOrder>,
    reverse: Option<u64>,
    recursive: Option<u64>,
    gt: Option<u64>,
    hidden: Option<u64>,
    random: bool,
}

impl IteratorOpts {
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    pub fn hidden(mut self, hidden: u64) -> Self {
        self.hidden = Some(hidden);
        self
    }

    pub fn flags(mut self, flags: u64) -> Self {
        self.flags = Some(flags);
        self
    }

    pub fn gt(mut self, gt: u64) -> Self {
        self.gt = Some(gt);
        self
    }

    pub fn recursive(mut self, recursive: u64) -> Self {
        self.recursive = Some(recursive);
        self
    }

    pub fn reverse(mut self, reverse: u64) -> Self {
        self.reverse = Some(reverse);
        self
    }
}

impl<T: Into<String>> From<T> for IteratorOpts {
    fn from(prefix: T) -> Self {
        Self {
            prefix: prefix.into(),
            flags: None,
            order: None,
            recursive: None,
            reverse: None,
            gt: None,
            hidden: None,
            random: false,
        }
    }
}
