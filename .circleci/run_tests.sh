#!/bin/bash

set -e
set -o xtrace

./target/debug/pgcat &

sleep 1

psql -e -h 127.0.0.1 -p 5432 -U postgres -f tests/sharding/query_routing_setup.sql

# Setup PgBench
pgbench -i -h 127.0.0.1 -p 6432

# Run it
pgbench -h 127.0.0.1 -p 6432 -t 500 -c 2 --protocol simple

# Extended protocol
pgbench -h 127.0.0.1 -p 6432 -t 500 -c 2 --protocol extended

# COPY TO STDOUT test
psql -h 127.0.0.1 -p 6432 -c 'COPY (SELECT * FROM pgbench_accounts LIMIT 15) TO STDOUT;' > /dev/null

# Sharding insert
psql -e -h 127.0.0.1 -p 6432 -f tests/sharding/query_routing_test_insert.sql

# Sharding select
psql -e -h 127.0.0.1 -p 6432 -f tests/sharding/query_routing_test_select.sql > /dev/null

# Replica/primary selection & more sharding tests
psql -e -h 127.0.0.1 -p 6432 -f tests/sharding/query_routing_test_primary_replica.sql > /dev/null

# Attempt clean shut down
killall pgcat -s SIGINT
