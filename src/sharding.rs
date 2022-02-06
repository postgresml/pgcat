use sha1::{Digest, Sha1};

pub struct Sharder {
    shards: usize,
}

impl Sharder {
    pub fn new(shards: usize) -> Sharder {
        Sharder { shards: shards }
    }

    pub fn sha1(&self, key: &[u8]) -> usize {
        let mut hasher = Sha1::new();
        hasher.update(key);
        let result = hasher.finalize_reset();

        let i = u32::from_le_bytes(result[result.len() - 4..result.len()].try_into().unwrap());
        i as usize % self.shards
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
}
