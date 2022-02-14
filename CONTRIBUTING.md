## Introduction

Thank you for contributing! Just a few tips here:

1. `cargo fmt` your code before opening up a PR
2. Run the "test suite" (i.e. PgBench) to make sure everything still works.

Happy hacking!

## TODOs

A non-exhaustive list of things that would be useful to implement.

#### Client authentication
MD5 is probably sufficient, but maybe others too.

#### Statistics
Same as PgBouncer, e.g. client wait, transactions, timings, etc. I'm thinking we can use `mpsc` here ([docs](https://tokio.rs/tokio/tutorial/channels)), with clients sending stats and a task collecting and aggregating them. This should avoid atomics/mutexes. Caveat is the task should make sure not to crash, so the channels don't get backed up.

#### Admin
Admin database for stats collection and pooler administration. PgBouncer gives us a nice example on how to do that, specifically how to implement `RowDescription` and `DataRow` messages, [example here](https://github.com/pgbouncer/pgbouncer/blob/4f9ced8e63d317a6ff45c8b0efa876b32161f6db/src/admin.c#L813).
