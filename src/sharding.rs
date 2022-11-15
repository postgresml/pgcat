use serde_derive::{Deserialize, Serialize};
/// Implements various sharding functions.
use sha1::{Digest, Sha1};

/// See: <https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/catalog/partition.h#L20>.
const PARTITION_HASH_SEED: u64 = 0x7A5B22367996DCFD;

/// The sharding functions we support.
#[derive(Debug, PartialEq, Copy, Clone, Serialize, Deserialize, Hash, std::cmp::Eq)]
pub enum ShardingFunction {
    #[serde(alias = "pg_bigint_hash", alias = "PgBigintHash")]
    PgBigintHash,
    #[serde(alias = "sha1", alias = "Sha1")]
    Sha1,
}

impl ToString for ShardingFunction {
    fn to_string(&self) -> String {
        match *self {
            ShardingFunction::PgBigintHash => "pg_bigint_hash".to_string(),
            ShardingFunction::Sha1 => "sha1".to_string(),
        }
    }
}

/// The sharder.
pub struct Sharder {
    /// Number of shards in the cluster.
    shards: usize,

    /// The sharding function in use.
    sharding_function: ShardingFunction,
}

impl Sharder {
    /// Create new instance of the sharder.
    pub fn new(shards: usize, sharding_function: ShardingFunction) -> Sharder {
        Sharder {
            shards,
            sharding_function,
        }
    }

    /// Compute the shard given sharding key.
    pub fn shard(&self, key: i64) -> usize {
        match self.sharding_function {
            ShardingFunction::PgBigintHash => self.pg_bigint_hash(key),
            ShardingFunction::Sha1 => self.sha1(key),
        }
    }

    /// Hash function used by Postgres to determine which partition
    /// to put the row in when using HASH(column) partitioning.
    /// Source: <https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/common/hashfn.c#L631>.
    /// Supports only 1 bigint at the moment, but we can add more later.
    fn pg_bigint_hash(&self, key: i64) -> usize {
        let mut lohalf = key as u32;
        let hihalf = (key >> 32) as u32;
        lohalf ^= if key >= 0 { hihalf } else { !hihalf };
        Self::combine(0, Self::pg_u32_hash(lohalf)) as usize % self.shards
    }

    /// Example of a hashing function based on SHA1.
    fn sha1(&self, key: i64) -> usize {
        let mut hasher = Sha1::new();

        hasher.update(&key.to_string().as_bytes());

        let result = hasher.finalize();

        // Convert the SHA1 hash into hex so we can parse it as a large integer.
        let hex = format!("{:x}", result);

        // Parse the last 8 bytes as an integer (8 bytes = bigint).
        let key = i64::from_str_radix(&hex[hex.len() - 8..], 16).unwrap() as usize;

        key % self.shards
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
            .wrapping_add(0x49a0f4dd15e5a8e3_u64)
            .wrapping_add(a << 54)
            .wrapping_add(a >> 7);
        a
    }

    #[inline]
    fn pg_u32_hash(k: u32) -> u64 {
        let mut a: u32 = 0x9e3779b9_u32 + std::mem::size_of::<u32>() as u32 + 3923095_u32;
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

    // See tests/sharding/partition_hash_test_setup.sql
    // The output of those SELECT statements will match this test,
    // confirming that we implemented Postgres BIGINT hashing correctly.
    #[test]
    fn test_pg_bigint_hash() {
        let sharder = Sharder::new(5, ShardingFunction::PgBigintHash);

        let shard_0 = vec![1, 4, 5, 14, 19, 39, 40, 46, 47, 53];

        for v in shard_0 {
            assert_eq!(sharder.shard(v), 0);
        }

        let shard_1 = vec![2, 3, 11, 17, 21, 23, 30, 49, 51, 54];

        for v in shard_1 {
            assert_eq!(sharder.shard(v), 1);
        }

        let shard_2 = vec![6, 7, 15, 16, 18, 20, 25, 28, 34, 35];

        for v in shard_2 {
            assert_eq!(sharder.shard(v), 2);
        }

        let shard_3 = vec![8, 12, 13, 22, 29, 31, 33, 36, 41, 43];

        for v in shard_3 {
            assert_eq!(sharder.shard(v), 3);
        }

        let shard_4 = vec![9, 10, 24, 26, 27, 32, 37, 38, 42, 45];

        for v in shard_4 {
            assert_eq!(sharder.shard(v), 4);
        }
    }

    #[test]
    fn test_sha1_hash() {
        let sharder = Sharder::new(12, ShardingFunction::Sha1);
        let ids = vec![
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ];
        let shards = vec![
            4, 7, 8, 3, 6, 0, 0, 10, 3, 11, 1, 7, 4, 4, 11, 2, 5, 0, 8, 3,
        ];

        for (i, id) in ids.iter().enumerate() {
            assert_eq!(sharder.shard(*id), shards[i]);
        }
    }
}
