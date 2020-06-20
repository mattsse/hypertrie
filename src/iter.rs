use crate::{Get, GetOptions, HyperTrie, Node, TrieCommand};
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
    flags: u64,
    order: SortOrder,
    recursive: bool,
    stack: Vec<Entry>,
    gt: bool,
    start: u64,
    end: u64,
    had_missing_block: bool,
    needs_sort: Vec<u64>,
    random: bool,
    hidden: bool,
    opened: bool,
}

impl<'a, T> HyperTrieIterator<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    pub fn new(opts: impl Into<IteratorOpts>, db: &'a mut HyperTrie<T>) -> Self {
        let mut opts = opts.into();

        let mut flags = if opts.recursive { 1 } else { 0 };

        let order = if opts.reverse {
            flags |= 2;
            SortOrder::Reverse
        } else {
            SortOrder::Normal
        };

        if opts.gt {
            flags |= 4;
        }
        if opts.hidden {
            flags |= 8;
        }

        Self {
            db,
            prefix: opts.prefix,
            flags,
            order,
            recursive: opts.recursive,
            stack: vec![],
            gt: opts.gt,
            start: 0,
            end: 0,
            had_missing_block: false,
            needs_sort: vec![],
            random: opts.random,
            hidden: opts.hidden,
            opened: false,
        }
    }

    async fn open(&mut self) -> anyhow::Result<()> {
        let get = Get::new(
            GetOptions::new(self.prefix.clone())
                .prefix()
                .set_hidden(self.hidden),
        );

        let prefix = get.len();

        if let Some(node) = get.execute(self.db).await? {
            self.stack.push(Entry {
                i: prefix,
                seq: node.seq(),
                node,
            });
            self.start = prefix;
            if self.recursive {
                self.end = u64::MAX;
            } else {
                self.end = prefix + 32;
            }
        }
        Ok(())
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<Node>> {
        if !self.opened {
            self.open().await?;
            self.opened = true;
        }
        Ok(self.pop_stack().await?)
    }

    pub async fn collect(&mut self) -> anyhow::Result<Vec<Node>> {
        let mut nodes = Vec::with_capacity(self.db.head_seq() as usize);
        while let Some(node) = self.next().await? {
            nodes.push(node);
        }
        Ok(nodes)
    }

    async fn push(&mut self, i: u64, seq: u64) -> anyhow::Result<()> {
        if let Some(node) = self.db.get_by_seq(seq).await? {
            let top = Entry { i, seq, node };
            self.stack.push(top);

            if !self.had_missing_block && !self.db.feed_mut().has(seq) {
                self.had_missing_block = true;
            }
        }
        Ok(())
    }

    async fn pop_stack(&mut self) -> anyhow::Result<Option<Node>> {
        while let Some(mut top) = self.stack.pop() {
            let len = std::cmp::min(top.node.len(), self.end);
            let i = top.i;
            top.i += 1;

            if i >= len {
                return Ok(Some(top.node.finalize()));
            }

            let bucket = top.node.trie.bucket(i as usize);

            let order = if self.random {
                Self::random_order()
            } else {
                self.order.order()
            };

            for val in order.iter().cloned() {
                if val != 4 || !self.gt || i != self.start {
                    if top.node.path(i) == val {
                        self.stack.push(top.clone());
                    }
                    if let Some(bucket) = bucket {
                        for j in (val as usize..bucket.len()).step_by(5) {
                            if let Some(seq) = bucket.get(j).cloned().flatten() {
                                self.push(i + 1, seq).await?;
                            }
                        }
                    }

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
        }
        Ok(None)
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

#[derive(Debug, Clone)]
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
    reverse: bool,
    recursive: bool,
    gt: bool,
    hidden: bool,
    random: bool,
}

impl IteratorOpts {
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    pub fn hidden(mut self) -> Self {
        self.hidden = true;
        self
    }

    pub fn flags(mut self, flags: u64) -> Self {
        self.flags = Some(flags);
        self
    }

    pub fn gt(mut self, gt: bool) -> Self {
        self.gt = gt;
        self
    }

    pub fn set_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    pub fn reverse(mut self) -> Self {
        self.reverse = true;
        self
    }

    pub fn random(mut self) -> Self {
        self.random = true;
        self
    }
}

impl Default for IteratorOpts {
    fn default() -> Self {
        Self::from("")
    }
}

impl<T: Into<String>> From<T> for IteratorOpts {
    fn from(prefix: T) -> Self {
        Self {
            prefix: prefix.into(),
            flags: None,
            reverse: false,
            recursive: true,
            gt: false,
            hidden: false,
            random: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HyperTrieBuilder, PutOptions};
    use std::collections::HashMap;

    fn to_map(vals: impl IntoIterator<Item = Node>) -> HashMap<String, Option<Vec<u8>>> {
        vals.into_iter().map(|n| (n.key, n.value)).collect()
    }

    #[async_std::test]
    async fn basic_iter() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;
        let mut nodes = trie
            .batch_put(vec![("a", b"a"), ("b", b"b"), ("c", b"c")])
            .await?;

        let mut iter = trie.iter();
        assert_eq!(to_map(nodes), to_map(iter.collect().await?));

        Ok(())
    }

    #[async_std::test]
    async fn iter_big_db() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        let num = 1000;
        let mut nodes = Vec::with_capacity(num);
        for val in 0..num {
            let key = format!("#{}", val);
            let node = trie.put(key.clone(), key.as_bytes()).await?;
            nodes.push(node);
        }

        let mut iter = trie.iter();
        assert_eq!(to_map(nodes), to_map(iter.collect().await?));

        Ok(())
    }

    #[async_std::test]
    async fn prefix_basic_iter() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        let nodes = trie
            .batch_put(vec![
                ("foo/a", b"foo/a"),
                ("foo/b", b"foo/b"),
                ("foo/c", b"foo/c"),
            ])
            .await?;

        trie.batch_put(vec![("a", b"a"), ("b", b"b"), ("c", b"c")])
            .await?;

        let mut iter = trie.iter_with_options("foo");
        assert_eq!(to_map(nodes), to_map(iter.collect().await?));

        Ok(())
    }

    #[async_std::test]
    async fn empty_prefix_iter() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        trie.batch_put(vec![
            ("foo/a", b"foo/a"),
            ("foo/b", b"foo/b"),
            ("foo/c", b"foo/c"),
        ])
        .await?;

        let mut iter = trie.iter_with_options("bar");
        assert_eq!(to_map(vec![]), to_map(iter.collect().await?));

        Ok(())
    }

    #[async_std::test]
    async fn non_recursive_iter() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        for &val in &["a", "a/b/c/d", "a/c", "b", "b/b/c", "c/a", "c"] {
            trie.put(val, val.as_bytes()).await?;
        }

        let mut iter = trie.iter_with_options(IteratorOpts::default().set_recursive(false));

        let mut keys: Vec<_> = iter
            .collect()
            .await?
            .into_iter()
            .map(|n| n.key.split('/').next().unwrap().to_string())
            .collect();
        keys.sort();

        assert_eq!(
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            keys
        );

        Ok(())
    }

    #[async_std::test]
    async fn mixed_iter() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        let mut nodes = vec![];
        for &val in &["a", "a/a", "a/b", "a/c", "a/a/a", "a/a/b", "a/a/c"] {
            nodes.push(trie.put(val, val.as_bytes()).await?);
        }

        let mut iter = trie.iter();
        assert_eq!(to_map(nodes), to_map(iter.collect().await?));

        Ok(())
    }
}
