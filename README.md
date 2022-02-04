# PgCat

Meow. PgBouncer rewritten in Rust, with sharding, load balancing and failover support.

**Alpha**: don't use in production just yet.

## Local development

1. Install Rust (latest stable is fine).
2. `cargo run --release` (to get better benchmarks).

## Features

1. Session mode.
2. Transaction mode (basic).
3. `COPY` protocol support.

## Missing

1. Query cancellation support.
2. All the features I promised above. Will make them soon, promise :-).
3. Authentication, ehem, this proxy is letting anyone in at the moment.

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