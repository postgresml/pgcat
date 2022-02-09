# Sharding tests

This helps us test the sharding algorithm we implemented.


## Setup

We setup 3 Postgres DBs, `shard0`, `shard1`, and `shard2`. In each database, we create a partitioned table called `data`. The table is partitioned by hash, and each database will only have _one_ partition, `shard0` will satisfy `modulus 3, remainder 0`, `shard1` will satisfy `modulus 3, remainder 1`, etc.

To set this up, you can just run:

```bash
psql -f query_routing_setup.sql
```

## Run the tests

Start up PgCat by running `cargo run --release` in the root of the repo. In a different tab, run this:

```bash
psql -h 127.0.0.1 -p 6432 -f query_routing_test_insert.sql
psql -h 127.0.0.1 -p 6432 -f query_routing_test_select.sql
```

Note that no errors should take place. If our sharding logic was incorrect, we would get some errors
about unsatisfiable partition bounds. We don't because the pooler picked the correct databases
given the sharding keys.

Finally, you can validate the result again by running

```bash
psql -f query_routing_test_validate.sql
```

## That's it!