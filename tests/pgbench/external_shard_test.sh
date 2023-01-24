#! /usr/bin/env bash

EXIT_CODE=0
psql postgres://sharding_user:sharding_user@localhost:6432/sharded_db < tests/pgbench/external_shard_setup.sql

mkdir -p target/tests/external_shard_test
cd target/tests/external_shard_test

pids=()
for SHARD in 0 1 2 ; do
  cat ../../../tests/pgbench/external_shard_test.sql | sed "s/__shard__/$SHARD/g" > .test_$SHARD.sql
  PGPASSWORD=sharding_user pgbench -l -n -f .test_$SHARD.sql -h localhost -p 6432 -U sharding_user -M extended -t 100 sharded_db &
  pids+=("$!")
  PGPASSWORD=sharding_user pgbench -l -n -f .test_$SHARD.sql -h localhost -p 6432 -U sharding_user -M simple -t 100 sharded_db &
  pids+=("$!")
done

for job in "${pids[@]}"; do
  CODE=0;
  wait ${job} || CODE=$?
  if [[ "${CODE}" != "0" ]]; then
    echo "At least one test failed with exit code => ${CODE}" ;
    EXIT_CODE=1;
  fi
done
exit "$EXIT_CODE"