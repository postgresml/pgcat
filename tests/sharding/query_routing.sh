#/bin/bash
set -e

# Setup all the shards.
# sudo service postgresql restart

echo "Giving Postgres 5 seconds to start up..."

# sleep 5

# psql -f query_routing_setup.sql

psql -h 127.0.0.1 -p 6432 -f query_routing_test_insert.sql

psql -h 127.0.0.1 -p 6432 -f query_routing_test_select.sql

psql -e -h 127.0.0.1 -p 6432 -f query_routing_test_primary_replica.sql

psql -f query_routing_test_validate.sql
