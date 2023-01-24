-- For testing external shard control at the individual statement level

/* shard_id: __shard__ */ begin;

/* shard_id: __shard__ */ INSERT INTO pgbench_external_shard_data (shard_id, data) VALUES (__shard__, 'external shard __shard__');

-- Fail if anything from another shard is found
/* shard_id: __shard__ */ SELECT cast(data as int) FROM pgbench_external_shard_data WHERE shard_id <> __shard__;

-- Fail if nothing from this shard is found
/* shard_id: __shard__ */ SELECT 1/count(*) FROM pgbench_external_shard_data WHERE shard_id = __shard__;

/* shard_id: __shard__ */ commit;
