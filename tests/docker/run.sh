#!/bin/bash

rm -rf /app/target/ || true
rm /app/*.profraw || true
rm /app/pgcat.profdata || true
rm -rf /app/cov || true

export LLVM_PROFILE_FILE="/app/pgcat-%m-%p.profraw"
export RUSTC_BOOTSTRAP=1
export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code -Coverflow-checks=off -Zpanic_abort_tests -Cpanic=abort -Cinstrument-coverage"
export RUSTDOCFLAGS="-Cpanic=abort"

cd /app/
cargo clean
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
