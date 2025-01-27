#!/bin/bash

set -e

CLEAN_BUILD=true

if [ $1 = "no-clean" ]; then
    echo "INFO: clean build is NOT going to be performed."
    CLEAN_BUILD=false
    find /app/target/debug/deps -name *.gcda  -exec rm {} \;
fi

if $CLEAN_BUILD ; then
    rm -rf /app/target/ || true
fi
rm /app/*.profraw || true
rm /app/pgcat.profdata || true
rm -rf /app/cov || true

# Prepares the interactive test environment
# 
if [ -n "$INTERACTIVE_TEST_ENVIRONMENT" ]; then
    ports=(5432 7432 8432 9432 10432)
    for port in "${ports[@]}"; do
        is_it_up=0
        attempts=0
        while [ $is_it_up -eq 0 ]; do
            PGPASSWORD=postgres psql -h 127.0.0.1 -p $port -U postgres -c '\q' > /dev/null 2>&1
            if [ $? -eq 0 ]; then
                echo "PostgreSQL on port $port is up."
                is_it_up=1
            else
                attempts=$((attempts+1))
                if [ $attempts -gt 10 ]; then
                    echo "PostgreSQL on port $port is down, giving up."
                    exit 1
                fi
                echo "PostgreSQL on port $port is down, waiting for it to start."
                sleep 1
            fi
        done
    done
    PGPASSWORD=postgres psql -e -h 127.0.0.1 -p 5432  -U postgres -f /app/tests/sharding/query_routing_setup.sql
    PGPASSWORD=postgres psql -e -h 127.0.0.1 -p 7432  -U postgres -f /app/tests/sharding/query_routing_setup.sql
    PGPASSWORD=postgres psql -e -h 127.0.0.1 -p 8432  -U postgres -f /app/tests/sharding/query_routing_setup.sql
    PGPASSWORD=postgres psql -e -h 127.0.0.1 -p 9432  -U postgres -f /app/tests/sharding/query_routing_setup.sql
    PGPASSWORD=postgres psql -e -h 127.0.0.1 -p 10432 -U postgres -f /app/tests/sharding/query_routing_setup.sql
    sleep 100000000000000000
    exit 0
fi

export LLVM_PROFILE_FILE="/app/pgcat-%m-%p.profraw"
export RUSTC_BOOTSTRAP=1
export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code -Coverflow-checks=off -Zpanic_abort_tests -Cpanic=abort -Cinstrument-coverage"
export RUSTDOCFLAGS="-Cpanic=abort"

cd /app/
if $CLEAN_BUILD ; then
    cargo clean
fi
cargo build
cargo test --tests

bash .circleci/run_tests.sh

TEST_OBJECTS=$( \
    for file in $(cargo test --no-run 2>&1 | grep "target/debug/deps/pgcat-[[:alnum:]]\+" -o); \
    do \
        printf "%s %s " --object $file; \
    done \
)

echo "Generating coverage report"

rust-profdata merge -sparse /app/pgcat-*.profraw -o /app/pgcat.profdata

bash -c "rust-cov export -ignore-filename-regex='rustc|registry' -Xdemangler=rustfilt -instr-profile=/app/pgcat.profdata $TEST_OBJECTS --object ./target/debug/pgcat --format lcov > ./lcov.info"

genhtml lcov.info --title "PgCat Code Coverage" --css-file ./cov-style.css --highlight --no-function-coverage --ignore-errors source --legend  --output-directory cov --prefix $(pwd)

rm /app/*.profraw
rm /app/pgcat.profdata
