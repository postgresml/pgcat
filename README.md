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

## Local development

1. Install Rust (latest stable will work great).
2. `cargo build --release` (to get better benchmarks).
3. Change the config in `pgcat.toml` to fit your setup (optional given next step).
4. Install Postgres and run `psql -f tests/sharding/query_routing_setup.sql` (user/password may be required depending on your setup)
5. `cargo run --release` You're ready to go!

### Tests

You can just PgBench to test your changes:

```
pgbench -i -h 127.0.0.1 -p 6432 && \
pgbench -t 1000 -p 6432 -h 127.0.0.1 --protocol simple && \
pgbench -t 1000 -p 6432 -h 127.0.0.1 --protocol extended
```

See [sharding README](./tests/sharding/README.md) for sharding logic testing.

## Features

1. Session mode.
2. Transaction mode.
3. `COPY` protocol support.
4. Query cancellation.
5. Round-robin load balancing of replicas.
6. Banlist & failover.
7. Sharding!
8. Explicit query routing to primary or replicas.

### Session mode
Each client owns its own server for the duration of the session. Commands like `SET` are allowed.
This is identical to PgBouncer session mode.

### Transaction mode
The connection is attached to the server for the duration of the transaction. `SET` will pollute the connection,
but `SET LOCAL` works great. Identical to PgBouncer transaction mode.

### COPY protocol
That one isn't particularly special, but good to mention that you can `COPY` data in and from the server
using this pooler.

### Query cancellation
Okay, this is just basic stuff, but we support cancelling queries. If you know the Postgres protocol,
this might be relevant given than this is a transactional pooler but if you're new to Pg, don't worry about it, it works.

### Round-robin load balancing
This is the novel part. PgBouncer doesn't support it and suggests we use DNS or a TCP proxy instead.
We prefer to have everything as part of one package; arguably, it's easier to understand and optimize.
This pooler will round-robin between multiple replicas keeping load reasonably even. If the primary is in
the pool as well, it'll be treated as a replica for read-only queries.

### Banlist & failover
This is where it gets even more interesting. If we fail to connect to one of the replicas or it fails a health check,
we add it to a ban list. No more new transactions will be served by that replica for, in our case, 60 seconds. This
gives it the opportunity to recover while clients are happily served by the remaining replicas.

This decreases error rates substantially! Worth noting here that on busy systems, if the replicas are running too hot,
failing over could bring even more load and tip over the remaining healthy-ish replicas. In this case, a decision should be made:
either lose 1/x of your traffic or risk losing it all eventually. Ideally you overprovision your system, so you don't necessarily need
to make this choice :-).

### Sharding
We're implemeting Postgres' `PARTITION BY HASH` sharding function for `BIGINT` fields. This works well for tables that use `BIGSERIAL` primary key which I think is common enough these days. We can also add many more functions here, but this is a good start. See `src/sharding.rs` and `tests/sharding/partition_hash_test_setup.sql` for more details on the implementation.

The biggest advantage of using this sharding function is that anyone can shard the dataset using Postgres partitions
while also access it for both reads and writes using this pooler. No custom obscure sharding function is needed and database sharding can be done entirely in Postgres.

To select the shard we want to talk to, we introduced special syntax:

```sql
SET SHARDING KEY TO '1234';
```

This sharding key will be hashed and the pooler will select a shard to use for the next transaction. If the pooler is in session mode, this sharding key has to be set as the first query on startup & cannot be changed until the client re-connects.

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
