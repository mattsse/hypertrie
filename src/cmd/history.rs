use std::fmt;

use futures::task::{Context, Poll};
use random_access_disk::RandomAccessDisk;
use random_access_storage::RandomAccess;

use crate::node::Node;
use crate::HyperTrie;

pub struct History<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
{
    pub(crate) db: &'a mut HyperTrie<T>,
    pub(crate) lte: u64,
    pub(crate) gte: u64,
    pub(crate) reverse: bool,
}

impl<'a, T> History<'a, T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send + Unpin,
{
    pub async fn next(&mut self) -> Option<anyhow::Result<Node>> {
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

        match self.db.get_by_seq(seq).await {
            Ok(Some(node)) => Some(Ok(node)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
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

    pub fn set_reverse(mut self, reverse: bool) -> Self {
        self.reverse = reverse;
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

impl From<bool> for HistoryOpts {
    fn from(reverse: bool) -> Self {
        HistoryOpts::default().reverse()
    }
}


