#/bin/bash

# Setup all the shards.
sudo service postgresql restart

psql -f query_routing_setup.sql

psql -h 127.0.0.1 -p 6432 -f query_routing_test_insert.sql

psql -h 127.0.0.1 -p 6432 -f query_routing_test_select.sql

psql -f query_routing_test_validate.sql