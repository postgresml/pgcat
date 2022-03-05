#!/bin/bash

set -e
set -o xtrace

# Start PgCat with a particular log level
# for inspection.
function start_pgcat() {
    kill -s SIGINT $(pgrep pgcat) || true
    RUST_LOG=${1} ./target/debug/pgcat .circleci/pgcat.toml &
    sleep 1
}

# Setup the database with shards and user
psql -e -h 127.0.0.1 -p 5432 -U postgres -f tests/sharding/query_routing_setup.sql
PGPASSWORD=sharding_user pgbench -h 127.0.0.1 -U sharding_user shard0 -i
PGPASSWORD=sharding_user pgbench -h 127.0.0.1 -U sharding_user shard1 -i
PGPASSWORD=sharding_user pgbench -h 127.0.0.1 -U sharding_user shard2 -i

# Install Toxiproxy to simulate a downed/slow database
wget -O toxiproxy-2.1.4.deb https://github.com/Shopify/toxiproxy/releases/download/v2.1.4/toxiproxy_2.1.4_amd64.deb
sudo dpkg -i toxiproxy-2.1.4.deb

# Start Toxiproxy
toxiproxy-server &
sleep 1

# Create a database at port 5433, forward it to Postgres
toxiproxy-cli create -l 127.0.0.1:5433 -u 127.0.0.1:5432 postgres_replica

start_pgcat "info"

# pgbench test
pgbench -i -h 127.0.0.1 -p 6432 && \
    pgbench -h 127.0.0.1 -p 6432 -t 500 -c 2 --protocol simple -f tests/pgbench/simple.sql && \
    pgbench -h 127.0.0.1 -p 6432 -t 500 -c 2 --protocol extended

# COPY TO STDOUT test
psql -h 127.0.0.1 -p 6432 -c 'COPY (SELECT * FROM pgbench_accounts LIMIT 15) TO STDOUT;' > /dev/null

# Query cancellation test
(psql -h 127.0.0.1 -p 6432 -c 'SELECT pg_sleep(5)' || true) &
killall psql -s SIGINT

# Sharding insert
psql -e -h 127.0.0.1 -p 6432 -f tests/sharding/query_routing_test_insert.sql

# Sharding select
psql -e -h 127.0.0.1 -p 6432 -f tests/sharding/query_routing_test_select.sql > /dev/null

# Replica/primary selection & more sharding tests
psql -e -h 127.0.0.1 -p 6432 -f tests/sharding/query_routing_test_primary_replica.sql > /dev/null

#
# ActiveRecord tests
#
cd tests/ruby && \
    sudo gem install bundler && \
    bundle install && \
    ruby tests.rb && \
cd ../..

# Admin tests
psql -e -h 127.0.0.1 -p 6432 -d pgbouncer -c 'SHOW STATS' > /dev/null
psql -h 127.0.0.1 -p 6432 -d pgbouncer -c 'RELOAD' > /dev/null
psql -h 127.0.0.1 -p 6432 -d pgbouncer -c 'SHOW CONFIG' > /dev/null
psql -h 127.0.0.1 -p 6432 -d pgbouncer -c 'SHOW DATABASES' > /dev/null
psql -h 127.0.0.1 -p 6432 -d pgbouncer -c 'SHOW LISTS' > /dev/null
psql -h 127.0.0.1 -p 6432 -d pgbouncer -c 'SHOW POOLS' > /dev/null
psql -h 127.0.0.1 -p 6432 -d pgbouncer -c 'SHOW VERSION' > /dev/null
psql -h 127.0.0.1 -p 6432 -d pgbouncer -c "SET client_encoding TO 'utf8'" > /dev/null # will ignore
(! psql -e -h 127.0.0.1 -p 6432 -d random_db -c 'SHOW STATS' > /dev/null)

# Start PgCat in debug to demonstrate failover better
start_pgcat "debug"

# Add latency to the replica at port 5433 slightly above the healthcheck timeout
toxiproxy-cli toxic add -t latency -a latency=300 postgres_replica
sleep 1

# Note the failover in the logs
timeout 5 psql -e -h 127.0.0.1 -p 6432 <<-EOF
SELECT 1;
SELECT 1;
SELECT 1;
EOF

# Remove latency
toxiproxy-cli toxic remove --toxicName latency_downstream postgres_replica

start_pgcat "info"

# Test session mode (and config reload)
sed -i 's/pool_mode = "transaction"/pool_mode = "session"/' pgcat.toml

# Reload config test
kill -SIGHUP $(pgrep pgcat)

# Prepared statements that will only work in session mode
pgbench -h 127.0.0.1 -p 6432 -t 500 -c 2 --protocol prepared

# Attempt clean shut down
killall pgcat -s SIGINT

# Allow for graceful shutdown
sleep 1
