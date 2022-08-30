#!/bin/bash

rm /app/*.profraw || true
rm /app/pgcat.profdata || true
rm -rf /app/cov || true

cd /app/

cargo build
cargo test --tests

bash .circleci/run_tests.sh

rust-profdata merge -sparse pgcat-*.profraw -o pgcat.profdata

rust-cov export -ignore-filename-regex="rustc|registry" -Xdemangler=rustfilt -instr-profile=pgcat.profdata --object ./target/debug/pgcat --format lcov > ./lcov.info

genhtml lcov.info --output-directory cov --prefix $(pwd)

rm /app/*.profraw
rm /app/pgcat.profdata
