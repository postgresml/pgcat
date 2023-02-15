#!/bin/bash

rm /app/*.profraw || true
rm /app/pgcat.profdata || true
rm -rf /app/cov || true

export RUSTFLAGS="-C instrument-coverage"
export LLVM_PROFILE_FILE="pgcat-%m.profraw"

cd /app/

cargo build
cargo test --tests

bash .circleci/run_tests.sh

TEST_OBJECTS=$( \
    for file in $(cargo test --no-run 2>&1 | grep "target/debug/deps/pgcat-[[:alnum:]]\+" -o); \
    do \
        printf "%s %s " --object $file; \
    done \
)

rust-profdata merge -sparse pgcat-*.profraw -o pgcat.profdata

bash -c "rust-cov export -ignore-filename-regex='rustc|registry' -Xdemangler=rustfilt -instr-profile=pgcat.profdata $TEST_OBJECTS --object ./target/debug/pgcat --format lcov > ./lcov.info"

genhtml lcov.info --output-directory cov --prefix $(pwd)

rm /app/*.profraw
rm /app/pgcat.profdata
