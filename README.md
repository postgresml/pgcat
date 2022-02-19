# PgCat

[![CircleCI](https://circleci.com/gh/levkk/pgcat/tree/main.svg?style=svg)](https://circleci.com/gh/levkk/pgcat/tree/main)

![PgCat](./pgcat3.png)

Meow. PgBouncer rewritten in Rust, with sharding, load balancing and failover support.

**Alpha**: don't use in production just yet.

## Features

| **Feature**                    | **Status**         | **Comments**                                                                                                                                          |
|--------------------------------|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| Transaction pooling            | :heavy_check_mark: | Identical to PgBouncer.                                                                                                                               |
| Session pooling                | :heavy_check_mark: | Identical to PgBouncer.                                                                                                                               |
| `COPY` support                 | :heavy_check_mark: | Both `COPY TO` and `COPY FROM` are supported.                                                                                                         |
| Query cancellation             | :heavy_check_mark: | Supported both in transaction and session pooling modes.                                                                                              |
| Load balancing of read queries | :heavy_check_mark: | Using round-robin between replicas. Primary is included when `primary_reads_enabled` is enabled (default).                                            |
| Sharding                       | :heavy_check_mark: | Transactions are sharded using `SET SHARD TO` and `SET SHARDING KEY TO` syntax extensions; see examples below.                                        |
| Failover                       | :heavy_check_mark: | Replicas are tested with a health check. If a health check fails, remaining replicas are attempted; see below for algorithm description and examples. |
| Statistics reporting           | :heavy_check_mark: | Statistics similar to PgBouncers are reported via StatsD.                                                                                             |
| Live configuration reloading   | :x: :wrench:       | On the roadmap; currently config changes require restart.                                                                                             |

## Deployment

See [`Dockerfile`]('./Dockerfile') for example deployment using Docker. The pooler is configured to spawn 4 workers, so 4 CPUs are recommended for optimal performance.
That setting can be adjusted to spawn as many (or as little) workers as needed.

## Local development

1. Install Rust (latest stable will work great).
2. `cargo build --release` (to get better benchmarks).
3. Change the config in `pgcat.toml` to fit your setup (optional given next step).
4. Install Postgres and run `psql -f tests/sharding/query_routing_setup.sql` (user/password may be required depending on your setup)
5. `cargo run --release` You're ready to go!

### Tests

Quickest way to test your changes is to use pgbench:

```
pgbench -i -h 127.0.0.1 -p 6432 && \
pgbench -t 1000 -p 6432 -h 127.0.0.1 --protocol simple && \
pgbench -t 1000 -p 6432 -h 127.0.0.1 --protocol extended
```

See [sharding README](./tests/sharding/README.md) for sharding logic testing.

| **Feature**          | **Tested in CI**   | **Tested manually** | **Comments**                                                                                                             |
|----------------------|--------------------|---------------------|--------------------------------------------------------------------------------------------------------------------------|
| Transaction pooling  | :heavy_check_mark: | :heavy_check_mark:  | Used by default for all tests.                                                                                           |
| Session pooling      | :x:                | :heavy_check_mark:  | Easiest way to test is to enable it and run pgbench - results will be better than transaction pooling as expected.       |
| `COPY`               | :heavy_check_mark: | :heavy_check_mark:  | `pgbench -i` uses `COPY`. `COPY FROM` is tested as well.                                                                 |
| Query cancellation   | :heavy_check_mark: | :heavy_check_mark:  | `psql -c 'SELECT pg_sleep(1000);'` and press `Ctrl-C`.                                                                   |
| Load balancing       | :x:                | :heavy_check_mark:  | We could test this by emitting statistics for each replica and compare them.                                             |
| Failover             | :x:                | :heavy_check_mark:  | Misconfigure a replica in `pgcat.toml` and watch it forward queries to spares. CI testing could include using Toxiproxy. |
| Sharding             | :heavy_check_mark: | :heavy_check_mark:  | See `tests/sharding` and `tests/ruby` for an Rails/ActiveRecord example.                                                 |
| Statistics reporting | :x:                | :heavy_check_mark:  | Run `nc -l -u 8125` and watch the stats come in every 15 seconds.                                                        |


## Feature descriptions

### Session mode
In session mode, a client talks to one server for the duration of the connection. That server is not shared between other clients, so session mode is best used for low traffic deployments. However session mode does provide higher throughput (queries per second) than tranasactionn mode. Prepared statements are supported and commands like `SET` and advisory locks are supported as well.

### Transaction mode
In transaction mode, a client talks to one server only for the duration of a single transaction. The server is then returned to the pool to be used by other clients. This is recommended for high traffic deployments. Transaction mode would have slightly lower per-client throughput (queries per second) because of pool contention, but this is expected. Overall system throughput is much higher in transaction mode than in session mode. Prepared statements are not supported because they are set on the Postgres server connection, but that connection is re-used between multiple clients! The same applies to advisory locks and `SET` (not supported). Alternatives are to use `SET LOCAL` and `pg_advisory_xact_lock` which are scoped to the transaction.

This mode is enabled by default.

### Load balancing of read queries
All queries are load balanced against the configured servers using the round-robin algorithm. The most straight forward configuration example would be to put this pooler in front of several replicas and let it load balance all queries. If however the configuration includes a primary and replicas, the queries can be separated with the built-in query parser. The query parser will parse the SQL and figure out if it's a `SELECT` query (which is presumed to be a read) or something else (which is presumed to be a write) and will forward the query to the replicas or the primary respectively. This is disabled by default since inferring client intent based on the query alone is tricky. Some `SELECT` queries can write, for example; however for 99% of the workloads, this should work just fine. You can enable the query parser by changing the `query_parser_enabled` setting.

#### Query parser
The query parser will do its best to determine where the query should go, but sometimes that's not possible. In that case, the client can select which server it wants using this custom SQL syntax:

```sql
-- To talk to the primary for the duration of the next transaction:
SET SERVER ROLE TO 'primary';

-- To talk to the replica for the duration of the next transaction:
SET SERVER ROLE TO 'replica';
```

This will override whatever decision the query parser was going to make, for the duration of the next transaction.

If the query parser is disabled, the client should indicate which server it wants to talk to. By default, it will be any server that's available, but that can be changed with the `default_role` setting. If it's set to `primary`, all queries will go to the primary by default; if it's set to `replica`, they will go to the replica.

### Failover
All servers are checked with a health check before being given to a client. If a server is not reachable, it will be placed in a ban list. No more transactions will be attempted against that server for the duration of the ban, which is 60 seconds by default. Next server in the configuration will be attempted until either the number of attempts is exceeded or a healthy server is found. The ban time is configurable with the `ban_time` setting. The ban time allows the server to recover from whatever error condition it's experiencing - most often it's an overload or a dead host. This feature will decrease error rates substantially, but it will swing traffic that's normally served by the broken server to the other replicas. They should be overprovisioned sufficiently to absorb the new query load.


### Sharding

The sharding algorithm of choice is the `PARTITION BY HASH` function used by Postgres. This allows us to place partitions of a table in different databases (and physical servers) and route read and write queries using this pooler. Presently, the client needs to indicate which shard it wants to talk to using this custom SQL syntax:

```sql
-- To talk to a shard explicitely
SET SHARD TO '1';

-- To let the pooler choose based on a value
SET SHARDING KEY TO '1234';
```

The first query will tell the pooler to route the next transaction to shard 1, as set in the configuration. The second query will let the pooler hash the value and determine which shard the query should go to based on the hashing function modulo the number of shards. This is the same algorithm as Postgres uses to figure out which partition to use. This is workable because the latency to the pooler in normal deployments should be very low, so the cost of issuing an extra query is also very low.


We're implemeting Postgres' `PARTITION BY HASH` sharding function for `BIGINT` fields. This works well for tables that use `BIGSERIAL` primary key which I think is common enough these days. We can also add many more functions here, but this is a good start. See `src/sharding.rs` and `tests/sharding/partition_hash_test_setup.sql` for more details on the implementation.

The biggest advantage of using this sharding function is that anyone can shard the dataset using Postgres partitions
while also access it for both reads and writes using this pooler. No custom obscure sharding function is needed and database sharding can be done entirely in Postgres.

To select the shard we want to talk to, we introduced special syntax:

```sql
SET SHARDING KEY TO '1234';
```

This sharding key will be hashed and the pooler will select a shard to use until the key or shard is set again. If the pooler is in session mode, this sharding key has to be set as the first query on startup & cannot be changed until the client re-connects.

### Explicit read/write query routing

If you want to have the primary and replicas in the same pooler, you'd probably want to
route queries explicitely to the primary or replicas, depending if they are reads or writes (e.g `SELECT`s or `INSERT`/`UPDATE`, etc). To help with this, we introduce some more custom syntax:

```sql
SET SERVER ROLE TO 'primary';
SET SERVER ROLE TO 'replica';
```

After executing this, the next transaction will be routed to the primary or replica respectively. By default, all queries will be load-balanced between all servers, so if the client wants to write or talk to the primary, they have to explicitely select it using the syntax above.



## Missing

1. Authentication, ehem, this proxy is letting anyone in at the moment.

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
