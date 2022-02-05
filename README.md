# PgCat

Meow. PgBouncer rewritten in Rust, with sharding, load balancing and failover support.

**Alpha**: don't use in production just yet.

## Local development

1. Install Rust (latest stable is fine).
2. `cargo run --release` (to get better benchmarks).

### Tests

You can just PgBench to test your changes:

```
pgbench -i -h 127.0.0.1 -p 5433 && \
pgbench -t 1000 -p 5433 -h 127.0.0.1 --protocol simple && \
pgbench -t 1000 -p 5433 -h 127.0.0.1 --protocol extended
```

## Features

1. Session mode.
2. Transaction mode (basic).
3. `COPY` protocol support.
4. Query cancellation.
5. Round-robin load balancing of replicas.
6. Banlist & failover

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
This is the novel part. PgBouncer doesn't support it and suggests we use DNS of a TCP proxy instead.
We prefer to have everything as part of one package; arguably, it's easier to understand and optimize.
This pooler will round-robin between multiple replicas keeping load reasonably even.

### Banlist & failover
This is where it gets even more interesting. If we fail to connect to one of the replicas or it fails a health check,
we add it to a ban list. No more new transactions will be served by that replica for, in our case, 60 seconds. This
gives it the opportunity to recover while clients are happily served by the remaining replicas.

This decreases error rates substantially! Worth noting here that on busy systems, if the replicas are running too hot,
failing over could bring even more load and tip over the remaining healthy-ish replicas. In this case, a decision should be made:
either lose 1/x of your traffic or risk losing it all eventually. Ideally you overprovision your system, so you don't necessarily need
to make this choice :-).


## Missing

1. Sharding. Soon :-).
2. Authentication, ehem, this proxy is letting anyone in at the moment.

## Benchmarks

You can setup PgBench locally through PgCat:

```
pgbench -h 127.0.0.1 -p 5433 -i
```

Coincidenly, this uses `COPY` so you can test if that works.

### PgBouncer

```
pgbench -h 127.0.0.1 -p 6432 --protocol extended -t 1000
starting vacuum...end.
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 1
query mode: extended
number of clients: 1
number of threads: 1
number of transactions per client: 1000
number of transactions actually processed: 1000/1000
latency average = 1.116 ms
tps = 895.900600 (including connections establishing)
tps = 896.115205 (excluding connections establishing)
```

### PgCat

```
pgbench -h 127.0.0.1 -p 5433 --protocol extended -t 1000
starting vacuum...end.
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 1
query mode: extended
number of clients: 1
number of threads: 1
number of transactions per client: 1000
number of transactions actually processed: 1000/1000
latency average = 1.152 ms
tps = 867.761579 (including connections establishing)
tps = 867.881391 (excluding connections establishing)
```

### Direct Postgres

```
pgbench -h 127.0.0.1 -p 5432 --protocol extended -t 1000
Password:
starting vacuum...end.
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 1
query mode: extended
number of clients: 1
number of threads: 1
number of transactions per client: 1000
number of transactions actually processed: 1000/1000
latency average = 0.944 ms
tps = 1059.007346 (including connections establishing)
tps = 1061.700877 (excluding connections establishing)
```