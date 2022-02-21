## Introduction

Thank you for contributing! Just a few tips here:

1. `cargo fmt` your code before opening up a PR
2. Run the test suite (e.g. `pgbench`) to make sure everything still works. The tests are in `.circleci/run_tests.sh`.
3. Performance is important, make sure there are no regressions in your branch vs. `main`.

Happy hacking!

## TODOs

A non-exhaustive list of things that would be useful to implement:

#### Client authentication
MD5 is probably sufficient, but maybe others too.

#### Admin
Admin database for stats collection and pooler administration. PgBouncer gives us a nice example on how to do that, specifically how to implement `RowDescription` and `DataRow` messages, [example here](https://github.com/pgbouncer/pgbouncer/blob/4f9ced8e63d317a6ff45c8b0efa876b32161f6db/src/admin.c#L813).
