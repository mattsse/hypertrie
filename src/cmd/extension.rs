use crate::hypertrie_proto as proto;

pub const MAX_ACTIVE: u64 = 32;
pub const FLUSH_BATCH: u64 = 128;
pub const MAX_PASSIVE_BATCH: u64 = 2048;
pub const MAX_ACTIVE_BATCH: u64 = MAX_PASSIVE_BATCH + FLUSH_BATCH;

#[derive(Debug, Clone, PartialEq)]
pub struct HypertrieExtension {
    extension: proto::Extension,
}
