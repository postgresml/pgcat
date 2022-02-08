use sha1::{Digest, Sha1};

// https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/catalog/partition.h#L20
const PARTITION_HASH_SEED: u64 = 0x7A5B22367996DCFD;

pub struct Sharder {
    shards: usize,
}

impl Sharder {
    pub fn new(shards: usize) -> Sharder {
        Sharder { shards: shards }
    }

    /// Use SHA1 to pick a shard for the key. The key can be anything,
    /// including an int or a string.
    pub fn sha1(&self, key: &[u8]) -> usize {
        let mut hasher = Sha1::new();
        hasher.update(key);
        let result = hasher.finalize_reset();

        let i = u32::from_le_bytes(result[result.len() - 4..result.len()].try_into().unwrap());
        i as usize % self.shards
    }

    /// Hash function used by Postgres to determine which partition
    /// to put the row in when using HASH(column) partitioning.
    /// Source: https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/common/hashfn.c#L631
    pub fn pg_bigint_hash(&self, key: i64) -> usize {
        let mut lohalf = key as u32;
        let hihalf = (key >> 32) as u32;
        lohalf ^= if key >= 0 { hihalf } else { !hihalf };
        Self::pg_u32_hash(lohalf) as usize % self.shards
    }

    fn rot(x: u32, k: u32) -> u32 {
        ((x) << (k)) | ((x) >> (32 - (k)))
    }

    #[inline]
    fn mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
        a = a.wrapping_sub(c);
        a ^= Self::rot(c, 4);
        c = c.wrapping_add(b);

        b = b.wrapping_sub(a);
        b ^= Self::rot(a, 6);
        a = a.wrapping_add(c);

        c = c.wrapping_sub(b);
        c ^= Self::rot(b, 8);
        b = b.wrapping_add(a);

        a = a.wrapping_sub(c);
        a ^= Self::rot(c, 16);
        c = c.wrapping_add(b);

        b = b.wrapping_sub(a);
        b ^= Self::rot(a, 19);
        a = a.wrapping_add(c);

        c = c.wrapping_sub(b);
        c ^= Self::rot(b, 4);
        b = b.wrapping_add(a);

        (a, b, c)
    }

    #[inline]
    fn _final(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
        c ^= b;
        c = c.wrapping_sub(Self::rot(b, 14));
        a ^= c;
        a = a.wrapping_sub(Self::rot(c, 11));
        b ^= a;
        b = b.wrapping_sub(Self::rot(a, 25));
        c ^= b;
        c = c.wrapping_sub(Self::rot(b, 16));
        a ^= c;
        a = a.wrapping_sub(Self::rot(c, 4));
        b ^= a;
        b = b.wrapping_sub(Self::rot(a, 14));
        c ^= b;
        c = c.wrapping_sub(Self::rot(b, 24));
        (a, b, c)
    }

    fn pg_u32_hash(k: u32) -> u64 {
        let mut a: u32 = 0x9e3779b9 as u32 + std::mem::size_of::<u32>() as u32 + 3923095 as u32;
        let mut b = a;
        let c = a;
        let seed = PARTITION_HASH_SEED;

        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        let (mut a, b, c) = Self::mix(a, b, c);

        a = a.wrapping_add(k);

        let (a, b, c) = Self::_final(a, b, c);

        ((b as u64) << 32) | (c as u64)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sha1() {
        let sharder = Sharder::new(12);
        let key = b"1234";
        let shard = sharder.sha1(key);
        assert_eq!(shard, 1);
    }

    #[test]
    fn test_pg_bigint_hash() {
        let sharder = Sharder::new(2);
        let key = 1 as i64;
        let shard = sharder.pg_bigint_hash(key);
        assert_eq!(shard, 0);

        let key = 2 as i64;
        let shard = sharder.pg_bigint_hash(key);
        assert_eq!(shard, 0);

        let key = 3 as i64;
        let shard = sharder.pg_bigint_hash(key);
        assert_eq!(shard, 1);
    }
}
