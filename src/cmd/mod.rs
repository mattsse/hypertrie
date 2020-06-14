use crate::cmd::delete::Delete;
use crate::cmd::get::Get;
use crate::cmd::put::Put;
use crate::HyperTrie;
use async_trait::async_trait;
use random_access_storage::RandomAccess;
use std::fmt;

pub mod batch;
pub mod delete;
pub mod diff;
pub mod extension;
pub mod get;
pub mod history;
pub mod put;

#[async_trait]
pub trait TrieCommand {
    type Item;

    async fn execute<T>(self, db: &mut HyperTrie<T>) -> Self::Item
    where
        T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + fmt::Debug + Send;
}
