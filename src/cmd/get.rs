use std::fmt;

use random_access_storage::RandomAccess;

use crate::node::Node;
use crate::HyperTrie;

#[derive(Clone, Debug)]
pub struct Get {
    node: Node,
    closest: bool,
    prefix: bool,
    hidden: bool,
    // TODO needed?
    head: u64,
}

impl Get {
    pub fn new(opts: impl Into<GetOptions>) -> Self {
        let opts = opts.into();
        let node = Node::new(opts.key, 0);
        Self {
            node,
            closest: opts.closest,
            prefix: opts.prefix,
            hidden: opts.hidden,
            head: 0,
        }
    }

    pub fn len(&self) -> u64 {
        if self.prefix {
            self.node.len() - 1
        } else {
            self.node.len()
        }
    }

    // TODO put this in an async trait?
    pub(crate) async fn execute<T>(mut self, db: &mut HyperTrie<T>) -> anyhow::Result<Option<Node>>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        self.head = db.head_seq();
        if self.head == 0 {
            return Ok(None);
        }
        // TODO handle _sendExt

        if let Some(head) = db.get_by_seq(self.head).await? {
            Ok(self.update(0, head, db).await?)
        } else {
            Ok(None)
        }
    }

    async fn update<T>(
        self,
        mut seq: u64,
        mut head: Node,
        db: &mut HyperTrie<T>,
    ) -> anyhow::Result<Option<Node>>
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send,
    {
        let mut i = seq;

        while i < self.len() {
            let check_collision = Node::terminator(i);
            let val = self.node.path(i);

            if head.path(i) == val && (!check_collision || !self.node.collides(&head, i)) {
                i += 1;
                continue;
            }

            let bucket = head.bucket(i as usize);

            if let Some(bucket) = bucket {
                if check_collision {
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
                                    return if self.closest {
                                        Ok(Some(self.node))
                                    } else {
                                        Ok(None)
                                    };
                                }

                                seq = i + 1;
                                i = seq;
                                continue;
                            } else {
                                return Err(anyhow::anyhow!("no node for seq {}", s));
                            }
                        } else {
                            break;
                        }
                    }
                }
            }

            if let Some(bucket) = bucket {
                if let Some(s) = bucket.get(val as usize).cloned().flatten() {
                    // return update head
                    if let Some(node) = db.get_by_seq(s).await? {
                        // restart for new node
                        head = node;
                        seq += 1;
                        i = seq;
                        continue;
                    }
                }
            }
            return if self.closest {
                Ok(Some(head))
            } else {
                Ok(None)
            };
        }
        Ok(Some(head))
    }
}

#[derive(Clone, Debug)]
pub struct GetOptions {
    key: String,
    closest: bool,
    prefix: bool,
    hidden: bool,
}

impl GetOptions {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            closest: false,
            prefix: false,
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

    pub fn prefix(mut self) -> Self {
        self.prefix = true;
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
impl<T: Into<String>> From<T> for GetOptions {
    fn from(s: T) -> Self {
        Self::new(s)
    }
}
