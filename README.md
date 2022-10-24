![PgCat](./pgcat3.png)

##### PgCat: PostgreSQL at petabyte scale

[![CircleCI](https://circleci.com/gh/levkk/pgcat/tree/main.svg?style=svg)](https://circleci.com/gh/levkk/pgcat/tree/main)
<a href="https://discord.gg/DmyJP3qJ7U" target="_blank">
    <img src="https://img.shields.io/discord/1013868243036930099" alt="Join our Discord!" />
</a>

PostgreSQL pooler (like PgBouncer) with sharding, load balancing and failover support.

**Beta**: looking for beta testers, see [#35](https://github.com/levkk/pgcat/issues/35).

## Features
| **Feature**                    | **Status**                  | **Comments**                                                                                                                                          |
|--------------------------------|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| Transaction pooling            | :white_check_mark:          | Identical to PgBouncer.                                                                                                                               |
| Session pooling                | :white_check_mark:          | Identical to PgBouncer.                                                                                                                               |
| `COPY` support                 | :white_check_mark:          | Both `COPY TO` and `COPY FROM` are supported.                                                                                                         |
| Query cancellation             | :white_check_mark:          | Supported both in transaction and session pooling modes.                                                                                              |
| Load balancing of read queries | :white_check_mark:          | Using random between replicas. Primary is included when `primary_reads_enabled` is enabled (default).                                            |
| Sharding                       | :white_check_mark:          | Transactions are sharded using `SET SHARD TO` and `SET SHARDING KEY TO` syntax extensions; see examples below.                                        |
| Failover                       | :white_check_mark:          | Replicas are tested with a health check. If a health check fails, remaining replicas are attempted; see below for algorithm description and examples. |
| Statistics                     | :white_check_mark:          | Statistics available in the admin database (`pgcat` and `pgbouncer`) with `SHOW STATS`, `SHOW POOLS` and others.                                      |
| Live configuration reloading   | :white_check_mark:          | Reload supported settings with a `SIGHUP` to the process, e.g. `kill -s SIGHUP $(pgrep pgcat)` or `RELOAD` query issued to the admin database.        |
| Client authentication          | :white_check_mark: :wrench: | MD5 password authentication is supported, SCRAM is on the roadmap; one user is used to connect to Postgres with both SCRAM and MD5 supported.         |
| Admin database                 | :white_check_mark:          | The admin database, similar to PgBouncer's, allows to query for statistics and reload the configuration.                                              |

## Deployment

See `Dockerfile` for example deployment using Docker. The pooler is configured to spawn 4 workers so 4 CPUs are recommended for optimal performance. That setting can be adjusted to spawn as many (or as little) workers as needed.

For quick local example, use the Docker Compose environment provided:

```bash
docker-compose up

# In a new terminal:
PGPASSWORD=postgres psql -h 127.0.0.1 -p 6432 -U postgres -c 'SELECT 1'
```

### Config

| **Name**                     | **Description**                                                                                                                            | **Examples**                     |
|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------|
| **`general`**                |                                                                                                                                            |                                  |
| `host`                       | The pooler will run on this host, 0.0.0.0 means accessible from everywhere.                                                                | `0.0.0.0`                        |
| `port`                       | The pooler will run on this port.                                                                                                          | `6432`                           |
| `enable_prometheus_exporter` | Enable prometheus exporter which will export metrics in prometheus exposition format.                                                      | `true`                           |
| `prometheus_exporter_port`   | Port at which prometheus exporter listens on.                                                                                              | `9930`                           |
| `pool_size`                  | Maximum allowed server connections per pool. Pools are separated for each user/shard/server role. The connections are allocated as needed. | `15`                             |
| `pool_mode`                  | The pool mode to use, i.e. `session` or `transaction`.                                                                                     | `transaction`                    |
| `connect_timeout`            | Maximum time to establish a connection to a server (milliseconds). If reached, the server is banned and the next target is attempted.      | `5000`                           |
| `healthcheck_timeout`        | Maximum time to pass a health check (`SELECT 1`, milliseconds). If reached, the server is banned and the next target is attempted.         | `1000`                           |
| `shutdown_timeout`           | Maximum time to give clients during shutdown before forcibly killing client connections (ms).                                              | `60000`                          |
| `healthcheck_delay`          | How long to keep connection available for immediate re-use, without running a healthcheck query on it                                      | `30000`                          |
| `ban_time`                   | Ban time for a server (seconds). It won't be allowed to serve transactions until the ban expires; failover targets will be used instead.   | `60`                             |
| `autoreload`                 | Enable auto-reload of config after fixed time-interval.                                                                                    | `false`                          |
|                              |                                                                                                                                            |                                  |
| **`user`**                   |                                                                                                                                            |                                  |
| `name`                       | The user name.                                                                                                                             | `sharding_user`                  |
| `password`                   | The user password in plaintext.                                                                                                            | `hunter2`                        |
| `statement_timeout` | Timeout in milliseconds for how long a query takes to execute | `0 (disabled)`
|                              |                                                                                                                                            |                                  |
| **`shards`**                 | Shards are numerically numbered starting from 0; the order in the config is preserved by the pooler to route queries accordingly.          | `[shards.0]`                     |
| `servers`                    | List of servers to connect to and their roles. A server is: `[host, port, role]`, where `role` is either `primary` or `replica`.           | `["127.0.0.1", 5432, "primary"]` |
| `database`                   | The name of the database to connect to. This is the same on all servers that are part of one shard.                                        |                                  |
|                              |                                                                                                                                            |                                  |
| **`query_router`**           |                                                                                                                                            |                                  |
| `default_role`               | Traffic is routed to this role by default (random), unless the client specifies otherwise. Default is `any`, for any role available.  | `any`, `primary`, `replica`      |
| `query_parser_enabled`       | Enable the query parser which will inspect incoming queries and route them to a primary or replicas.                                       | `false`                          |
| `primary_reads_enabled`      | Enable this to allow read queries on the primary; otherwise read queries are routed to the replicas.                                       | `true`                           |

## Local development

1. Install Rust (latest stable will work great).
2. `cargo build --release` (to get better benchmarks).
3. Change the config in `pgcat.toml` to fit your setup (optional given next step).
4. Install Postgres and run `psql -f tests/sharding/query_routing_setup.sql` (user/password may be required depending on your setup)
5. `RUST_LOG=info cargo run --release` You're ready to go!

### Tests

Quickest way to test your changes is to use pgbench:

```
pgbench -i -h 127.0.0.1 -p 6432 && \
pgbench -t 1000 -p 6432 -h 127.0.0.1 --protocol simple && \
pgbench -t 1000 -p 6432 -h 127.0.0.1 --protocol extended
```

See [sharding README](./tests/sharding/README.md) for sharding logic testing.

Run `cargo test` to run Rust tests.

Run the following commands to run Integration tests locally.
```
cd tests/docker/
docker compose up --exit-code-from main # This will also produce coverage report under ./cov/
```

| **Feature**           | **Tested in CI**   | **Tested manually** | **Comments**                                                                                                             |
|-----------------------|--------------------|---------------------|--------------------------------------------------------------------------------------------------------------------------|
| Transaction pooling   | :white_check_mark: | :white_check_mark:  | Used by default for all tests.                                                                                           |
| Session pooling       | :white_check_mark: | :white_check_mark:  | Tested by running pgbench with `--protocol prepared` which only works in session mode.                                   |
| `COPY`                | :white_check_mark: | :white_check_mark:  | `pgbench -i` uses `COPY`. `COPY FROM` is tested as well.                                                                 |
| Query cancellation    | :white_check_mark: | :white_check_mark:  | `psql -c 'SELECT pg_sleep(1000);'` and press `Ctrl-C`.                                                                   |
| Load balancing        | :white_check_mark: | :white_check_mark:  | We could test this by emitting statistics for each replica and compare them.                                             |
| Failover              | :white_check_mark: | :white_check_mark:  | Misconfigure a replica in `pgcat.toml` and watch it forward queries to spares. CI testing is using Toxiproxy.            |
| Sharding              | :white_check_mark: | :white_check_mark:  | See `tests/sharding` and `tests/ruby` for an Rails/ActiveRecord example.                                                 |
| Statistics            | :white_check_mark: | :white_check_mark:  | Query the admin database with `psql -h 127.0.0.1 -p 6432 -d pgbouncer -c 'SHOW STATS'`.                                  |
| Live config reloading | :white_check_mark: | :white_check_mark:  | Run `kill -s SIGHUP $(pgrep pgcat)` and watch the config reload.                                                         |

## Usage

### Session mode
In session mode, a client talks to one server for the duration of the connection. Prepared statements, `SET`, and advisory locks are supported. In terms of supported features, there is very little if any difference between session mode and talking directly to the server.

To use session mode, change `pool_mode = "session"`.

### Transaction mode
In transaction mode, a client talks to one server for the duration of a single transaction; once it's over, the server is returned to the pool. Prepared statements, `SET`, and advisory locks are not supported; alternatives are to use `SET LOCAL` and `pg_advisory_xact_lock` which are scoped to the transaction.

This mode is enabled by default.

### Load balancing of read queries
All queries are load balanced against the configured servers using the random algorithm. The most straight forward configuration example would be to put this pooler in front of several replicas and let it load balance all queries.

If the configuration includes a primary and replicas, the queries can be separated with the built-in query parser. The query parser will interpret the query and route all `SELECT` queries to a replica, while all other queries including explicit transactions will be routed to the primary.

The query parser is disabled by default.

#### Query parser
The query parser will do its best to determine where the query should go, but sometimes that's not possible. In that case, the client can select which server it wants using this custom SQL syntax:

```sql
-- To talk to the primary for the duration of the next transaction:
SET SERVER ROLE TO 'primary';

-- To talk to the replica for the duration of the next transaction:
SET SERVER ROLE TO 'replica';

-- Let the query parser decide
SET SERVER ROLE TO 'auto';

-- Pick any server at random
SET SERVER ROLE TO 'any';

-- Reset to default configured settings
SET SERVER ROLE TO 'default';
```

The setting will persist until it's changed again or the client disconnects.

By default, all queries are routed to the first available server; `default_role` setting controls this behavior.

### Failover
All servers are checked with a `SELECT 1` query before being given to a client. If the server is not reachable, it will be banned and cannot serve any more transactions for the duration of the ban. The queries are routed to the remaining servers. If all servers become banned, the ban list is cleared: this is a safety precaution against false positives. The primary can never be banned.

The ban time can be changed with `ban_time`. The default is 60 seconds.

Failover behavior can get pretty interesting (read complex) when multiple configurations and factors are involved. The table below will try to explain what PgCat does in each scenario:

| **Query**                 | **`SET SERVER ROLE TO`** | **`query_parser_enabled`** | **`primary_reads_enabled`** | **Target state** | **Outcome**                                                                                                                                                          |
|---------------------------|--------------------------|----------------------------|-----------------------------|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Read query, i.e. `SELECT` | unset (any)              | false                      | false                       | up               | Query is routed to the first instance in the random loop.                                                                                                       |
| Read query                | unset (any)              | true                       | false                       | up               | Query is routed to the first replica instance in the random loop.                                                                                               |
| Read query                | unset (any)              | true                       | true                        | up               | Query is routed to the first instance in the random loop.                                                                                                       |
| Read query                | replica                  | false                      | false                       | up               | Query is routed to the first replica instance in the random loop.                                                                                               |
| Read query                | primary                  | false                      | false                       | up               | Query is routed to the primary.                                                                                                                                      |
| Read query                | unset (any)              | false                      | false                       | down             | First instance is banned for reads. Next target in the random loop is attempted.                                                                                |
| Read query                | unset (any)              | true                       | false                       | down             | First replica instance is banned. Next replica instance is attempted in the random loop.                                                                        |
| Read query                | unset (any)              | true                       | true                        | down             | First instance (even if primary) is banned for reads. Next instance is attempted in the random loop.                                                            |
| Read query                | replica                  | false                      | false                       | down             | First replica instance is banned. Next replica instance is attempted in the random loop.                                                                        |
| Read query                | primary                  | false                      | false                       | down             | The query is attempted against the primary and fails. The client receives an error.                                                                                  |
|                           |                          |                            |                             |                  |                                                                                                                                                                      |
| Write query e.g. `INSERT` | unset (any)              | false                      | false                       | up               | The query is attempted against the first available instance in the random loop. If the instance is a replica, the query fails and the client receives an error. |
| Write query               | unset (any)              | true                       | false                       | up               | The query is routed to the primary.                                                                                                                                  |
| Write query               | unset (any)              | true                       | true                        | up               | The query is routed to the primary.                                                                                                                                  |
| Write query               | primary                  | false                      | false                       | up               | The query is routed to the primary.                                                                                                                                  |
| Write query               | replica                  | false                      | false                       | up               | The query is routed to the replica and fails. The client receives an error.                                                                                          |
| Write query               | unset (any)              | true                       | false                       | down             | The query is routed to the primary and fails. The client receives an error.                                                                                          |
| Write query               | unset (any)              | true                       | true                        | down             | The query is routed to the primary and fails. The client receives an error.                                                                                          |
| Write query               | primary                  | false                      | false                       | down             | The query is routed to the primary and fails. The client receives an error.                                                                                          |
|                           |                          |                            |                             |                  |                                                                                                                                                                      |

### Sharding
We use the `PARTITION BY HASH` hashing function, the same as used by Postgres for declarative partitioning. This allows to shard the database using Postgres partitions and place the partitions on different servers (shards). Both read and write queries can be routed to the shards using this pooler.

To route queries to a particular shard, we use this custom SQL syntax:

```sql
-- To talk to a shard explicitely
SET SHARD TO '1';

-- To let the pooler choose based on a value
SET SHARDING KEY TO '1234';
```

The active shard will last until it's changed again or the client disconnects. By default, the queries are routed to shard 0.

For hash function implementation, see `src/sharding.rs` and `tests/sharding/partition_hash_test_setup.sql`.

#### ActiveRecord/Rails

```ruby
class User < ActiveRecord::Base
end

# Metadata will be fetched from shard 0
ActiveRecord::Base.establish_connection

# Grab a bunch of users from shard 1
User.connection.execute "SET SHARD TO '1'"
User.take(10)

# Using id as the sharding key
User.connection.execute "SET SHARDING KEY TO '1234'"
User.find_by_id(1234)

# Using geographical sharding
User.connection.execute "SET SERVER ROLE TO 'primary'"
User.connection.execute "SET SHARDING KEY TO '85'"
User.create(name: "test user", email: "test@example.com", zone_id: 85)

# Let the query parser figure out where the query should go.
# We are still on shard = hash(85) % shards.
User.connection.execute "SET SERVER ROLE TO 'auto'"
User.find_by_email("test@example.com")
```

#### Raw SQL

```sql
-- Grab a bunch of users from shard 1
SET SHARD TO '1';
SELECT * FROM users LIMT 10;

-- Find by id
SET SHARDING KEY TO '1234';
SELECT * FROM USERS WHERE id = 1234;

-- Writing in a primary/replicas configuration.
SET SHARDING ROLE TO 'primary';
SET SHARDING KEY TO '85';
INSERT INTO users (name, email, zome_id) VALUES ('test user', 'test@example.com', 85);

SET SERVER ROLE TO 'auto'; -- let the query router figure out where the query should go
SELECT * FROM users WHERE email = 'test@example.com'; -- shard setting lasts until set again; we are reading from the primary
```

### Statistics reporting

The stats are very similar to what Pgbouncer reports and the names are kept to be comparable. They are accessible by querying the admin database `pgcat`, and `pgbouncer` for compatibility.

```
psql -h 127.0.0.1 -p 6432 -d pgbouncer -c 'SHOW DATABASES'
```

### Live configuration reloading

The config can be reloaded by sending a `kill -s SIGHUP` to the process or by querying `RELOAD` to the admin database. Not all settings are currently supported by live reload:

| **Config**              | **Requires restart** |
|-------------------------|----------------------|
| `host`                  | yes                  |
| `port`                  | yes                  |
| `pool_mode`             | no                   |
| `connect_timeout`       | yes                  |
| `healthcheck_timeout`   | no                   |
| `shutdown_timeout`      | no                   |
| `healthcheck_delay`     | no                   |
| `ban_time`              | no                   |
| `user`                  | yes                  |
| `shards`                | yes                  |
| `default_role`          | no                   |
| `primary_reads_enabled` | no                   |
| `query_parser_enabled`  | no                   |


## Benchmarks

You can setup PgBench locally through PgCat:

```
pgbench -h 127.0.0.1 -p 6432 -i
```

Coincidenly, this uses `COPY` so you can test if that works. Additionally, we'll be running the following PgBench configurations:

1. 16 clients, 2 threads
2. 32 clients, 2 threads
3. 64 clients, 2 threads
4. 128 clients, 2 threads

All queries will be `SELECT` only (`-S`) just so disks don't get in the way, since the dataset will be effectively all in RAM.

My setup:

- 8 cores, 16 hyperthreaded (AMD Ryzen 5800X)
- 32GB RAM (doesn't matter for this benchmark, except to prove that Postgres will fit the whole dataset into RAM)

### PgBouncer

#### Config

```ini
[databases]
shard0 = host=localhost port=5432 user=sharding_user password=sharding_user

[pgbouncer]
pool_mode = transaction
max_client_conn = 1000
```

Everything else stays default.

#### Runs


```
$ pgbench -t 1000 -c 16 -j 2 -p 6432 -h 127.0.0.1 -S --protocol extended shard0

starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 16
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 16000/16000
latency average = 0.155 ms
tps = 103417.377469 (including connections establishing)
tps = 103510.639935 (excluding connections establishing)


$ pgbench -t 1000 -c 32 -j 2 -p 6432 -h 127.0.0.1 -S --protocol extended shard0

starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 32
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 32000/32000
latency average = 0.290 ms
tps = 110325.939785 (including connections establishing)
tps = 110386.513435 (excluding connections establishing)


$ pgbench -t 1000 -c 64 -j 2 -p 6432 -h 127.0.0.1 -S --protocol extended shard0

starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 64
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 64000/64000
latency average = 0.692 ms
tps = 92470.427412 (including connections establishing)
tps = 92618.389350 (excluding connections establishing)

$ pgbench -t 1000 -c 128 -j 2 -p 6432 -h 127.0.0.1 -S --protocol extended shard0

starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 128
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 128000/128000
latency average = 1.406 ms
tps = 91013.429985 (including connections establishing)
tps = 91067.583928 (excluding connections establishing)
```

### PgCat

#### Config

The only thing that matters here is the number of workers in the Tokio pool. Make sure to set it to < than the number of your CPU cores.
Also account for hyper-threading, so if you have that, take the number you got above and divide it by two, that way only "real" cores serving
requests.

My setup is 16 threads, 8 cores (`htop` shows as 16 CPUs), so I set the `max_workers` in Tokio to 4. Too many, and it starts conflicting with PgBench
which is also running on the same system.

#### Runs


```
$ pgbench -t 1000 -c 16 -j 2 -p 6432 -h 127.0.0.1 -S --protocol extended
starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 16
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 16000/16000
latency average = 0.164 ms
tps = 97705.088232 (including connections establishing)
tps = 97872.216045 (excluding connections establishing)


$ pgbench -t 1000 -c 32 -j 2 -p 6432 -h 127.0.0.1 -S --protocol extended

starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 32
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 32000/32000
latency average = 0.288 ms
tps = 111300.488119 (including connections establishing)
tps = 111413.107800 (excluding connections establishing)


$ pgbench -t 1000 -c 64 -j 2 -p 6432 -h 127.0.0.1 -S --protocol extended

starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 64
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 64000/64000
latency average = 0.556 ms
tps = 115190.496139 (including connections establishing)
tps = 115247.521295 (excluding connections establishing)

$ pgbench -t 1000 -c 128 -j 2 -p 6432 -h 127.0.0.1 -S --protocol extended

starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 128
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 128000/128000
latency average = 1.135 ms
tps = 112770.562239 (including connections establishing)
tps = 112796.502381 (excluding connections establishing)
```

### Direct Postgres

Always good to have a base line.

#### Runs

```
$ pgbench -t 1000 -c 16 -j 2 -p 5432 -h 127.0.0.1 -S --protocol extended shard0
Password:
starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 16
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 16000/16000
latency average = 0.115 ms
tps = 139443.955722 (including connections establishing)
tps = 142314.859075 (excluding connections establishing)

$ pgbench -t 1000 -c 32 -j 2 -p 5432 -h 127.0.0.1 -S --protocol extended shard0
Password:
starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 32
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 32000/32000
latency average = 0.212 ms
tps = 150644.840891 (including connections establishing)
tps = 152218.499430 (excluding connections establishing)

$ pgbench -t 1000 -c 64 -j 2 -p 5432 -h 127.0.0.1 -S --protocol extended shard0
Password:
starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 64
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 64000/64000
latency average = 0.420 ms
tps = 152517.663404 (including connections establishing)
tps = 153319.188482 (excluding connections establishing)

$ pgbench -t 1000 -c 128 -j 2 -p 5432 -h 127.0.0.1 -S --protocol extended shard0
Password:
starting vacuum...end.
transaction type: <builtin: select only>
scaling factor: 1
query mode: extended
number of clients: 128
number of threads: 2
number of transactions per client: 1000
number of transactions actually processed: 128000/128000
latency average = 0.854 ms
tps = 149818.594087 (including connections establishing)
tps = 150200.603049 (excluding connections establishing)
```
