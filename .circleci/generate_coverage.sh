#!/bin/bash


# inspired by https://doc.rust-lang.org/rustc/instrument-coverage.html#tips-for-listing-the-binaries-automatically
TEST_OBJECTS=$( \
    for file in $(cargo test --no-run 2>&1 | grep "target/debug/deps/pgcat-[[:alnum:]]\+" -o); \
    do \
        printf "%s %s " --object $file; \
    done \
)

rust-profdata merge -sparse pgcat-*.profraw -o pgcat.profdata

bash -c "rust-cov export -ignore-filename-regex='rustc|registry' -Xdemangler=rustfilt -instr-profile=pgcat.profdata $TEST_OBJECTS --object ./target/debug/pgcat --format lcov > ./lcov.info"

genhtml lcov.info -show-details --highlight --ignore-errors source --legend  --output-directory /tmp/cov --prefix $(pwd)
