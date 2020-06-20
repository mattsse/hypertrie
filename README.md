hypertrie
=====================
![Build](https://github.com/mattsse/hypertrie/workflows/Continuous%20integration/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/hypertrie.svg)](https://crates.io/crates/hypertrie)
[![Documentation](https://docs.rs/hypertrie/badge.svg)](https://docs.rs/hypertrie)

Distributed single writer key/value store.

A wip rust implementation of [hypertrie](https://github.com/hypercore-protocol/hypertrie) that uses a rolling hash array mapped trie to index key/value data.

Full docs available on [docs.rs](https://docs.rs/hypertrie)

## Usage

```rust

let mut trie = hypertrie::HyperTrie::ram().await?;

let hello = trie.put("hello", b"world").await?;
let world = trie.put("hello/world", b"b").await?;

let get = trie.get("hello").await?.unwrap();
assert_eq!(hello, get);

trie.delete("hello").await?;
let get = trie.get("hello").await?;
assert_eq!(None, get);

let get = trie.get("hello/world").await?.unwrap();
assert_eq!(world, get);
```


## References

- [github.com/hypercore-protocol/hypertrie](https://github.com/hypercore-protocol/hypertrie)
- [github.com/hypercore-protocol/hypercore](https://github.com/hypercore-protocol/hypercore)

## License

Licensed under either of these:

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   https://opensource.org/licenses/MIT)
   
