use bytes::{BufMut, Buf, BytesMut};

// TODO use btreehashmap instead?
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Trie(pub Vec<Option<Vec<Option<u64>>>>);

impl Trie {
    #[inline]
    pub(crate) fn insert_value(index: usize, value: u64, bucket: &mut Vec<Option<u64>>) {
        while index >= bucket.len() {
            bucket.push(None);
        }
        bucket[index] = Some(value)
    }

    #[inline]
    pub fn bucket(&self, idx: usize) -> Option<&Vec<Option<u64>>> {
        if let Some(b) = self.0.get(idx) {
            if let Some(b) = b {
                return Some(b);
            }
        }
        None
    }

    #[inline]
    pub fn bucket_mut(&mut self, idx: usize) -> Option<&mut Vec<Option<u64>>> {
        if let Some(b) = self.0.get_mut(idx) {
            if let Some(b) = b {
                return Some(b);
            }
        }
        None
    }

    fn fill_up_to(&mut self, mut index: usize) {
        while index >= self.len() {
            self.0.push(None);
        }
    }

    pub fn bucket_or_insert(&mut self, index: usize) -> &mut Vec<Option<u64>> {
        self.fill_up_to(index);
        if self.0[index].is_none() {
            self.0[index] = Some(Vec::new());
        }
        self.0[index].as_mut().unwrap()
    }

    /// # Panics
    ///
    /// Panics if `index > len`.
    pub fn insert_bucket(
        &mut self,
        index: usize,
        bucket: Vec<Option<u64>>,
    ) -> &mut Vec<Option<u64>> {
        self.fill_up_to(index);
        self.0[index] = Some(bucket);
        self.0[index].as_mut().unwrap()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(self.len());
        varintbuf::encode(self.len() as u64, &mut buf);

        for i in 0..self.len() {
            if let Some(bucket) = self.bucket(i) {
                varintbuf::encode(i as u64, &mut buf);

                let mut bit = 1;
                let mut bitfield = 0;

                for j in 0..bucket.len() {
                    if bucket.get(j).cloned().flatten().is_some() {
                        bitfield |= bit;
                    }
                    bit *= 2;
                }

                varintbuf::encode(bitfield, &mut buf);

                for j in 0..bucket.len() {
                    if let Some(seq) = bucket.get(j).cloned().flatten() {
                        varintbuf::encode(seq as u64, &mut buf);
                    }
                }
            }
        }
        buf.to_vec()
    }

    pub fn decode(mut buf: &[u8]) -> Self {
        if !buf.has_remaining() {
            return Trie(vec![]);
        }
        let remaining = buf.remaining();
        let mut len = varintbuf::decode(&mut buf);

        let mut trie = Trie(Vec::with_capacity(len as usize));

        if buf.has_remaining() {
            // the JS implementations starts at trie[offset] with the first bucket
            let offset = remaining - buf.remaining();
            trie.0.extend(std::iter::repeat(None).take(offset));
        }

        while buf.has_remaining() {
            let idx = varintbuf::decode(&mut buf);

            let mut bitfield = varintbuf::decode(&mut buf);
            let mut pos = 0;

            let mut bucket = Vec::with_capacity(64 - (bitfield.leading_zeros() as usize));

            while bitfield > 0 {
                let bit = bitfield & 1;

                if bit != 0 {
                    let val = varintbuf::decode(&mut buf);
                    Trie::insert_value(pos, val, &mut bucket);
                }

                bitfield = (bitfield - bit) / 2;
                pos += 1;
            }
            trie.insert_bucket(idx as usize, bucket);
        }
        trie
    }
}

impl Default for Trie {
    fn default() -> Self {
        Trie(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_trie() {
        let trie = Trie(vec![]);
        let buf = trie.encode();
        assert_eq!(buf, vec![0]);
        let decoded = Trie::decode(&*buf);
        assert_eq!(trie, decoded);

        let trie = Trie(vec![None, Some(vec![None, Some(1)])]);
        let buf = trie.encode();
        assert_eq!(buf.len(), 4);
        assert_eq!(buf, vec![2, 1, 2, 1]);

        let decoded = Trie::decode(&*buf);
        assert_eq!(trie, decoded);
    }
}
