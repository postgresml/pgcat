#!/bin/bash

set -e
set -o xtrace

# non-zero exit code if we provide bad configs
(! ./target/debug/pgcat "fake_configs"  2>/dev/null)

# Start PgCat with a particular log level
# for inspection.
function start_pgcat() {
    kill -s SIGINT $(pgrep pgcat) || true
    RUST_LOG=${1} ./target/debug/pgcat .circleci/pgcat.toml &
    sleep 1
}

# Setup the database with shards and user
PGPASSWORD=postgres psql -e -h 127.0.0.1 -p 5432 -U postgres -f tests/sharding/query_routing_setup.sql
PGPASSWORD=postgres psql -e -h 127.0.0.1 -p 7432 -U postgres -f tests/sharding/query_routing_setup.sql
PGPASSWORD=postgres psql -e -h 127.0.0.1 -p 8432 -U postgres -f tests/sharding/query_routing_setup.sql
PGPASSWORD=postgres psql -e -h 127.0.0.1 -p 9432 -U postgres -f tests/sharding/query_routing_setup.sql
PGPASSWORD=postgres psql -e -h 127.0.0.1 -p 10432 -U postgres -f tests/sharding/query_routing_setup.sql

PGPASSWORD=sharding_user pgbench -h 127.0.0.1 -U sharding_user shard0 -i
PGPASSWORD=sharding_user pgbench -h 127.0.0.1 -U sharding_user shard1 -i
PGPASSWORD=sharding_user pgbench -h 127.0.0.1 -U sharding_user shard2 -i

# Start Toxiproxy
LOG_LEVEL=error toxiproxy-server &
sleep 1

# Create a database at port 5433, forward it to Postgres
toxiproxy-cli create -l 127.0.0.1:5433 -u 127.0.0.1:5432 postgres_replica

start_pgcat "info"

# Check that prometheus is running
curl --fail localhost:9930/metrics

export PGPASSWORD=sharding_user
export PGDATABASE=sharded_db

# pgbench test
pgbench -U sharding_user -i -h 127.0.0.1 -p 6432
pgbench -U sharding_user -h 127.0.0.1 -p 6432 -t 500 -c 2 --protocol simple -f tests/pgbench/simple.sql
pgbench -U sharding_user -h 127.0.0.1 -p 6432 -t 500 -c 2 --protocol extended

# COPY TO STDOUT test
psql -U sharding_user -h 127.0.0.1 -p 6432 -c 'COPY (SELECT * FROM pgbench_accounts LIMIT 15) TO STDOUT;' > /dev/null

# Query cancellation test
(psql -U sharding_user -h 127.0.0.1 -p 6432 -c 'SELECT pg_sleep(50)' || true) &
sleep 1
killall psql -s SIGINT

# Pause/resume test.
# Running benches before, during, and after pause/resume.
pgbench -U sharding_user -t 500 -c 2 -h 127.0.0.1 -p 6432 --protocol extended &
BENCH_ONE=$!
PGPASSWORD=admin_pass psql -U admin_user -h 127.0.0.1 -p 6432 -d pgbouncer -c 'PAUSE sharded_db,sharding_user'
pgbench -U sharding_user -h 127.0.0.1 -p 6432 -t 500 -c 2 --protocol extended &
BENCH_TWO=$!
PGPASSWORD=admin_pass psql -U admin_user -h 127.0.0.1 -p 6432 -d pgbouncer -c 'RESUME sharded_db,sharding_user'
wait ${BENCH_ONE}
wait ${BENCH_TWO}

# Reload pool (closing unused server connections)
PGPASSWORD=admin_pass psql -U admin_user -h 127.0.0.1 -p 6432 -d pgbouncer -c 'RELOAD'

(psql -U sharding_user -h 127.0.0.1 -p 6432 -c 'SELECT pg_sleep(50)' || true) &
sleep 1
killall psql -s SIGINT

# Sharding insert
psql -U sharding_user -e -h 127.0.0.1 -p 6432 -f tests/sharding/query_routing_test_insert.sql

# Sharding select
psql -U sharding_user -e -h 127.0.0.1 -p 6432 -f tests/sharding/query_routing_test_select.sql > /dev/null

# Replica/primary selection & more sharding tests
psql -U sharding_user -e -h 127.0.0.1 -p 6432 -f tests/sharding/query_routing_test_primary_replica.sql > /dev/null

# Statement timeout tests
sed -i 's/statement_timeout = 0/statement_timeout = 100/' .circleci/pgcat.toml
kill -SIGHUP $(pgrep pgcat) # Reload config
sleep 0.2

# This should timeout
(! psql -U sharding_user -e -h 127.0.0.1 -p 6432 -c 'select pg_sleep(0.5)')

# Disable statement timeout
sed -i 's/statement_timeout = 100/statement_timeout = 0/' .circleci/pgcat.toml
kill -SIGHUP $(pgrep pgcat) # Reload config again

#
# Integration tests and ActiveRecord tests
#
cd tests/ruby
sudo bundle install
bundle exec ruby tests.rb --format documentation || exit 1
bundle exec rspec *_spec.rb --format documentation || exit 1
cd ../..

#
# Python tests
# These tests will start and stop the pgcat server so it will need to be restarted after the tests
#
pip3 install -r tests/python/requirements.txt
python3 tests/python/tests.py || exit 1


#
# Go tests
# Starts its own pgcat server
#
pushd tests/go
/usr/local/go/bin/go test || exit 1
popd

start_pgcat "info"

#
# Rust tests
#
cd tests/rust
cargo run
cd ../../

# Admin tests
export PGPASSWORD=admin_pass
psql -U admin_user -e -h 127.0.0.1 -p 6432 -d pgbouncer -c 'SHOW STATS' > /dev/null
psql -U admin_user -h 127.0.0.1 -p 6432 -d pgbouncer -c 'RELOAD' > /dev/null
psql -U admin_user -h 127.0.0.1 -p 6432 -d pgbouncer -c 'SHOW CONFIG' > /dev/null
psql -U admin_user -h 127.0.0.1 -p 6432 -d pgbouncer -c 'SHOW DATABASES' > /dev/null
psql -U admin_user -h 127.0.0.1 -p 6432 -d pgcat -c 'SHOW LISTS' > /dev/null
psql -U admin_user -h 127.0.0.1 -p 6432 -d pgcat -c 'SHOW POOLS' > /dev/null
psql -U admin_user -h 127.0.0.1 -p 6432 -d pgcat -c 'SHOW VERSION' > /dev/null
psql -U admin_user -h 127.0.0.1 -p 6432 -d pgbouncer -c "SET client_encoding TO 'utf8'" > /dev/null # will ignore
(! psql -U admin_user -e -h 127.0.0.1 -p 6432 -d random_db -c 'SHOW STATS' > /dev/null)
export PGPASSWORD=sharding_user

# Start PgCat in debug to demonstrate failover better
start_pgcat "trace"

# Add latency to the replica at port 5433 slightly above the healthcheck timeout
toxiproxy-cli toxic add -t latency -a latency=300 postgres_replica
sleep 1

# Note the failover in the logs
timeout 5 psql -U sharding_user -e -h 127.0.0.1 -p 6432 <<-EOF
SELECT 1;
SELECT 1;
SELECT 1;
EOF

# Remove latency
toxiproxy-cli toxic remove --toxicName latency_downstream postgres_replica

start_pgcat "info"

# Test session mode (and config reload)
sed -i '0,/simple_db/s/pool_mode = "transaction"/pool_mode = "session"/' .circleci/pgcat.toml

# Reload config test
kill -SIGHUP $(pgrep pgcat)

# Revert settings after reload. Makes test runs idempotent
sed -i '0,/simple_db/s/pool_mode = "session"/pool_mode = "transaction"/' .circleci/pgcat.toml

sleep 1

# Prepared statements that will only work in session mode
pgbench -U sharding_user -h 127.0.0.1 -p 6432 -t 500 -c 2 --protocol prepared

# Attempt clean shut down
killall pgcat -s SIGINT

# Allow for graceful shutdown
sleep 1
