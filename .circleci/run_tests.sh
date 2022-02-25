#!/bin/bash

set -e
set -o xtrace

psql -e -h 127.0.0.1 -p 5432 -U postgres -f tests/sharding/query_routing_setup.sql

# Toxiproxy
wget -O toxiproxy-2.1.4.deb https://github.com/Shopify/toxiproxy/releases/download/v2.1.4/toxiproxy_2.1.4_amd64.deb
sudo dpkg -i toxiproxy-2.1.4.deb
toxiproxy-server &
sleep 2
toxiproxy-cli create -l 127.0.0.1:5433 -u 127.0.0.1:5432 postgres_replica

RUST_LOG=info ./target/debug/pgcat .circleci/pgcat.toml &
sleep 1

# Setup PgBench
pgbench -i -h 127.0.0.1 -p 6432

# Run it
pgbench -h 127.0.0.1 -p 6432 -t 500 -c 2 --protocol simple

# Extended protocol
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
cd tests/ruby
sudo gem install bundler
bundle install
ruby tests.rb

cd ../..
kill -s SIGINT $(pgrep pgcat)
RUST_LOG=debug ./target/debug/pgcat .circleci/pgcat.toml &
sleep 1

# Failover tests
toxiproxy-cli toxic add -t latency -a latency=300 postgres_replica
sleep 1

# Note the failover in the logs
timeout 5 psql -e -h 127.0.0.1 -p 6432 'SELECT 1' > /dev/null
timeout 5 psql -e -h 127.0.0.1 -p 6432 'SELECT 1' > /dev/null
timeout 5 psql -e -h 127.0.0.1 -p 6432 'SELECT 1' > /dev/null

# Test session mode (and config reload)
sed -i 's/pool_mode = "transaction"/pool_mode = "session"/' pgcat.toml

# Reload config
kill -SIGHUP $(pgrep pgcat)

# Prepared statements that will only work in session mode
pgbench -h 127.0.0.1 -p 6432 -t 500 -c 2 --protocol prepared

# Attempt clean shut down
killall pgcat -s SIGINT

# Allow for graceful shutdown
sleep 1
