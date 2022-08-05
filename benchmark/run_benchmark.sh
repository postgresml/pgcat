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

wait_for_containers() {
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
    docker compose up -d
    wait_for_containers

    docker compose exec --env PGPASSWORD=main_user proxy pgbench -p 6432 -h 127.0.0.1 -U main_user -i shard0

    echo ""
    echo "================================================"
    echo "[Pgbouncer] Running test directly against the DB"
    echo "================================================"
    docker compose exec --env PGPASSWORD=main_user proxy pgbench -t 1000 -c 128 -j 2 -p 5432 -h 127.0.0.1 -U main_user -S --protocol extended shard0


    echo ""
    echo "=========================================="
    echo "[Pgbouncer] Running test against the proxy"
    echo "=========================================="
    docker compose exec --env PGPASSWORD=main_user proxy pgbench -t 1000 -c 128 -j 2 -p 6432 -h 127.0.0.1 -U main_user -S --protocol extended shard0

    docker compose down -v

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
    docker compose up -d
    wait_for_containers

    docker compose exec --env PGPASSWORD=main_user proxy pgbench -p 6432 -h 127.0.0.1 -U main_user -i shard0

    echo ""
    echo "============================================"
    echo "[Pgcat] Running test directly against the DB"
    echo "============================================"
    docker compose exec --env PGPASSWORD=main_user proxy pgbench -t 1000 -c 128 -j 2 -p 5432 -h 127.0.0.1 -U main_user -S --protocol extended shard0

    echo ""
    echo "======================================"
    echo "[Pgcat] Running test against the proxy"
    echo "======================================"
    docker compose exec --env PGPASSWORD=main_user proxy pgbench -t 1000 -c 128 -j 2 -p 6432 -h 127.0.0.1 -U main_user -S --protocol extended shard0

    docker compose down -v

    echo ""
    echo ""

    cd ..
}


if [ PGCAT_ONLY = 1 ]
then
    benchmark_pgcat
    exit 0
fi

if [ PGBOUNCER_ONLY = 1 ]
then
    benchmark_pgbouncer
    exit 0
fi

benchmark_pgbouncer
benchmark_pgcat

