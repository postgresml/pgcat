\set ON_ERROR_STOP on


SET SHARD TO 0;

DROP TABLE IF EXISTS pgbench_external_shard_data CASCADE;

CREATE TABLE pgbench_external_shard_data (
    shard_id BIGINT,
    data VARCHAR
);

SET SHARD TO 1;

DROP TABLE IF EXISTS pgbench_external_shard_data CASCADE;

CREATE TABLE pgbench_external_shard_data (
    shard_id BIGINT,
    data VARCHAR
);

SET SHARD TO 2;

DROP TABLE IF EXISTS pgbench_external_shard_data CASCADE;

CREATE TABLE pgbench_external_shard_data (
    shard_id BIGINT,
    data VARCHAR
);