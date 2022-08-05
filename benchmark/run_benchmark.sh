#!/usr/bin/env bash

RECOMPILE_PGCAT=0
PGCAT_ONLY=0
PGBOUNCER_ONLY=0

for ((i=1;i<=$#;i++));
do
    if [ ${!i} = "--pgcat-only" ]
    then
        PGCAT_ONLY=1
    elif [ ${!i} = "--pgbouncer-only" ]
    then
        PGBOUNCER_ONLY=1
    elif [ ${!i} = "--recompile" ]
    then
        RECOMPILE_PGCAT=1
    fi
done;

drop_and_recreate_database() {
    docker compose stop proxy &> /dev/null # Avoid errors if proxy is connected to the database
    docker compose exec --env PGPASSWORD=main_user postgres psql -p 5432 -h 127.0.0.1 -U main_user -d postgres -c "DROP DATABASE shard0"  &> /dev/null
    docker compose exec --env PGPASSWORD=main_user postgres psql -p 5432 -h 127.0.0.1 -U main_user -d postgres -c "CREATE DATABASE shard0"  &> /dev/null
    docker compose start proxy &> /dev/null
    wait_for_proxy
}


run_benchmark() {
    docker compose exec --env PGPASSWORD=main_user proxy pgbench -p $1 -h 127.0.0.1 -U main_user -i shard0
    docker compose exec --env PGPASSWORD=main_user proxy pgbench -t 25000 -c 128 -j 2 -p $1 -h 127.0.0.1 -U main_user -S --protocol extended shard0
}

wait_for_proxy() {
    until docker compose exec --env PGPASSWORD=main_user proxy psql -p 6432 -h 127.0.0.1 -U main_user -d shard0 -c "select 1" &> /dev/null
    do
        echo "Waiting for postgres server"
        sleep 1
    done
    sleep 2
}

benchmark_pgbouncer() {
    echo "======================"
    echo "Running Pgbouncer Test"
    echo "======================"
    cd pgbouncer
    docker compose down -v  # Remove any stored data from previous runs
    docker compose up -d &> /dev/null
    wait_for_proxy

    echo ""
    echo "================================================"
    echo "[Pgbouncer] Running test directly against the DB"
    echo "================================================"
    drop_and_recreate_database
    run_benchmark 5432

    echo ""
    echo "=========================================="
    echo "[Pgbouncer] Running test against the proxy"
    echo "=========================================="
    drop_and_recreate_database
    run_benchmark 6432

    echo ""
    echo ""
    cd ..
}

benchmark_pgcat() {
    echo "=========="
    echo "Pgcat Test"
    echo "=========="
    cd pgcat
    if [ $RECOMPILE_PGCAT = 1 ];
    then
        echo "Recompiling Pgcat"
        docker compose build proxy --no-cache
    fi
    docker compose down -v # Remove any stored data from previous runs
    docker compose up -d &> /dev/null
    wait_for_proxy

    echo ""
    echo "============================================"
    echo "[Pgcat] Running test directly against the DB"
    echo "============================================"
    drop_and_recreate_database
    run_benchmark 5432

    echo ""
    echo "======================================"
    echo "[Pgcat] Running test against the proxy"
    echo "======================================"
    drop_and_recreate_database
    run_benchmark 6432

    echo ""
    echo ""

    cd ..
}


if [ $PGCAT_ONLY = 1 ]
then
    benchmark_pgcat
    exit 0
fi

if [ $PGBOUNCER_ONLY = 1 ]
then
    benchmark_pgbouncer
    exit 0
fi

benchmark_pgbouncer
benchmark_pgcat

