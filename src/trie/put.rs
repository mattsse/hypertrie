use crate::trie::node::Node;
use prost::Message;

pub struct PutOptions {
    key: String,
    closest: Option<bool>,
    hidden: Option<bool>,
    flags: u64,
}

// used so we can pass a single str as well as configured options to the put function
impl<T: Into<String>> From<T> for PutOptions {
    fn from(s: T) -> Self {
        Self {
            key: s.into(),
            closest: None,
            hidden: None,
            flags: 0,
        }
    }
}
