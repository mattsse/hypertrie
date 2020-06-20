use std::fmt;
use std::pin::Pin;

use async_std::task::{Context, Poll};
use futures::stream::Stream;
use futures::{Future, FutureExt};
use random_access_storage::RandomAccess;

use crate::node::Node;
use crate::HyperTrie;

pub struct History<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    pub(crate) get_by_seq: Option<
        Pin<Box<dyn Future<Output = (anyhow::Result<Option<Node>>, &'a mut HyperTrie<T>)> + 'a>>,
    >,
    pub(crate) lte: u64,
    pub(crate) gte: u64,
    pub(crate) reverse: bool,
}

impl<'a, T> History<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    pub fn new(opts: impl Into<HistoryOpts>, db: &'a mut HyperTrie<T>) -> Self {
        let opts = opts.into();
        let lte = opts.lte.unwrap_or(db.head_seq());

        let mut hist = Self {
            lte,
            get_by_seq: None,
            gte: opts.gte,
            reverse: opts.reverse,
        };
        if let Some(seq) = hist.next_seq() {
            hist.get_by_seq = Some(Box::pin(db.get_by_seq_compat(seq)));
        }
        hist
    }

    fn next_seq(&mut self) -> Option<u64> {
        if self.gte > self.lte {
            return None;
        }

        let seq = if self.reverse {
            let lte = self.lte;
            self.lte -= 1;
            lte
        } else {
            let gte = self.gte;
            self.gte += 1;
            gte
        };
        Some(seq)
    }
}

impl<'a, T> Stream for History<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    type Item = anyhow::Result<Node>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(mut fut) = self.get_by_seq.take() {
            match fut.poll_unpin(cx) {
                Poll::Ready((Ok(Some(node)), db)) => {
                    if let Some(seq) = self.next_seq() {
                        let fut = db.get_by_seq_compat(seq);
                        self.get_by_seq = Some(Box::pin(fut));
                    }
                    Poll::Ready(Some(Ok(node)))
                }
                Poll::Ready((Ok(None), db)) => Poll::Ready(None),
                Poll::Ready((Err(err), db)) => {
                    if let Some(seq) = self.next_seq() {
                        let fut = db.get_by_seq_compat(seq);
                        self.get_by_seq = Some(Box::pin(fut));
                    }
                    Poll::Ready(Some(Err(err)))
                }
                Poll::Pending => {
                    self.get_by_seq = Some(fut);
                    Poll::Pending
                }
            }
        } else {
            Poll::Ready(None)
        }
    }
}

#[derive(Debug, Clone)]
pub struct HistoryOpts {
    pub(crate) lte: Option<u64>,
    pub(crate) gte: u64,
    pub(crate) reverse: bool,
}

impl HistoryOpts {
    pub fn lte(mut self, lte: u64) -> Self {
        self.lte = Some(lte);
        self
    }

    pub fn gte(mut self, gte: u64) -> Self {
        self.gte = gte;
        self
    }

    pub fn reverse(mut self) -> Self {
        self.reverse = true;
        self
    }
}

impl Default for HistoryOpts {
    fn default() -> Self {
        Self {
            lte: None,
            gte: 1,
            reverse: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::{History, HistoryOpts, HyperTrie};

    #[async_std::test]
    async fn history() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        let init = trie.put("hello", b"world").await?;
        let mut history = trie.history();

        let node = history.next().await.unwrap();
        assert_eq!(node.unwrap(), init);

        let node = history.next().await;
        assert!(node.is_none());

        Ok(())
    }

    #[async_std::test]
    async fn history_on_empty() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        let mut history = trie.history();
        let node = history.next().await;
        assert!(node.is_none());

        Ok(())
    }

    #[async_std::test]
    async fn history_reverse() -> Result<(), Box<dyn std::error::Error>> {
        let mut trie = HyperTrie::ram().await?;

        let hello = trie.put("hello", b"world").await?;
        let world = trie.put("world", b"hello").await?;
        let mut history = History::new(HistoryOpts::default().reverse(), &mut trie);

        let node = history.next().await.unwrap();
        assert_eq!(node.unwrap(), world);

        let node = history.next().await.unwrap();
        assert_eq!(node.unwrap(), hello);

        let node = history.next().await;
        assert!(node.is_none());

        Ok(())
    }
}
