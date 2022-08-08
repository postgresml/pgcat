DROP DATABASE IF EXISTS shard0;
DROP DATABASE IF EXISTS shard1;
DROP DATABASE IF EXISTS shard2;
DROP DATABASE IF EXISTS some_db

CREATE DATABASE shard0;
CREATE DATABASE shard1;
CREATE DATABASE shard2;
CREATE DATABASE some_db

\c shard0

DROP TABLE IF EXISTS data CASCADE;

CREATE TABLE data (
    id BIGINT,
    value VARCHAR
) PARTITION BY HASH (id);

CREATE TABLE data_shard_0 PARTITION OF data FOR VALUES WITH (MODULUS 3, REMAINDER 0);

\c shard1

DROP TABLE IF EXISTS data CASCADE;

CREATE TABLE data (
    id BIGINT,
    value VARCHAR
) PARTITION BY HASH (id);

CREATE TABLE data_shard_1 PARTITION OF data FOR VALUES WITH (MODULUS 3, REMAINDER 1);


\c shard2

DROP TABLE IF EXISTS data CASCADE;

CREATE TABLE data (
    id BIGINT,
    value VARCHAR
) PARTITION BY HASH (id);

CREATE TABLE data_shard_2 PARTITION OF data FOR VALUES WITH (MODULUS 3, REMAINDER 2);


\c some_db

DROP TABLE IF EXISTS data CASCADE;

CREATE TABLE data (
    id BIGINT,
    value VARCHAR
);

DROP ROLE IF EXISTS sharding_user;
DROP ROLE IF EXISTS simple_user;
CREATE ROLE sharding_user ENCRYPTED PASSWORD 'sharding_user' LOGIN;
CREATE ROLE simple_user ENCRYPTED PASSWORD 'simple_user' LOGIN;

GRANT CONNECT ON DATABASE shard0  TO sharding_user;
GRANT CONNECT ON DATABASE shard1  TO sharding_user;
GRANT CONNECT ON DATABASE shard2  TO sharding_user;
GRANT CONNECT ON DATABASE some_db TO simple_user;

\c shard0
GRANT ALL ON SCHEMA public TO sharding_user;
GRANT ALL ON TABLE data TO sharding_user;

\c shard1
GRANT ALL ON SCHEMA public TO sharding_user;
GRANT ALL ON TABLE data TO sharding_user;

\c shard2
GRANT ALL ON SCHEMA public TO sharding_user;
GRANT ALL ON TABLE data TO sharding_user;

\c some_db
GRANT ALL ON SCHEMA public TO simple_user;
GRANT ALL ON TABLE data TO simple_user;
