#!/bin/bash

rust-profdata merge -sparse pgcat-*.profraw -o pgcat.profdata

rust-cov export -ignore-filename-regex=rustc|registry -Xdemangler=rustfilt -instr-profile=pgcat.profdata --object ./target/debug/pgcat --format lcov > ./lcov.info

genhtml lcov.info --output-directory /tmp/cov --prefix $(pwd)
