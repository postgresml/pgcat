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
    /// Supports only 1 bigint at the moment, but we can add more later.
    pub fn pg_bigint_hash(&self, key: i64) -> usize {
        let mut lohalf = key as u32;
        let hihalf = (key >> 32) as u32;
        lohalf ^= if key >= 0 { hihalf } else { !hihalf };
        Self::combine(0, Self::pg_u32_hash(lohalf)) as usize % self.shards as usize
    }

    #[inline]
    fn rot(x: u32, k: u32) -> u32 {
        (x << k) | (x >> (32 - k))
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

    #[inline]
    fn combine(mut a: u64, b: u64) -> u64 {
        a ^= b
            .wrapping_add(0x49a0f4dd15e5a8e3 as u64)
            .wrapping_add(a << 54)
            .wrapping_add(a >> 7);
        a
    }

    fn pg_u32_hash(k: u32) -> u64 {
        let mut a: u32 = 0x9e3779b9 as u32 + std::mem::size_of::<u32>() as u32 + 3923095 as u32;
        let mut b = a;
        let c = a;

        a = a.wrapping_add((PARTITION_HASH_SEED >> 32) as u32);
        b = b.wrapping_add(PARTITION_HASH_SEED as u32);
        let (mut a, b, c) = Self::mix(a, b, c);

        a = a.wrapping_add(k);

        let (_a, b, c) = Self::_final(a, b, c);

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

    // See tests/sharding/setup.sql
    // The output of those SELECT statements will match this test,
    // confirming that we implemented Postgres BIGINT hashing correctly.
    #[test]
    fn test_pg_bigint_hash() {
        let sharder = Sharder::new(5);

        let shard_0 = vec![1, 4, 5, 14, 19, 39, 40, 46, 47, 53];

        for v in shard_0 {
            assert_eq!(sharder.pg_bigint_hash(v), 0);
        }

        let shard_1 = vec![2, 3, 11, 17, 21, 23, 30, 49, 51, 54];

        for v in shard_1 {
            assert_eq!(sharder.pg_bigint_hash(v), 1);
        }

        let shard_2 = vec![6, 7, 15, 16, 18, 20, 25, 28, 34, 35];

        for v in shard_2 {
            assert_eq!(sharder.pg_bigint_hash(v), 2);
        }

        let shard_3 = vec![8, 12, 13, 22, 29, 31, 33, 36, 41, 43];

        for v in shard_3 {
            assert_eq!(sharder.pg_bigint_hash(v), 3);
        }

        let shard_4 = vec![9, 10, 24, 26, 27, 32, 37, 38, 42, 45];

        for v in shard_4 {
            assert_eq!(sharder.pg_bigint_hash(v), 4);
        }
    }
}
