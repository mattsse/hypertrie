use crate::hypertrie_proto as proto;
use prost::Message;

pub const MAX_ACTIVE: u64 = 32;
pub const FLUSH_BATCH: u64 = 128;
pub const MAX_PASSIVE_BATCH: u64 = 2048;
pub const MAX_ACTIVE_BATCH: u64 = MAX_PASSIVE_BATCH + FLUSH_BATCH;

pub struct HypertrieExtension {
    extension: proto::Extension,
}
