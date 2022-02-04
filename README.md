# PgCat

Meow. PgBouncer rewritten in Rust, with sharding, load balancing and failover support.

**Alpha**: don't use in production just yet.

## Local development

1. Install Rust (latest stable is fine).
2. `cargo run --release` (to get better benchmarks).

## Features

1. Session mode.
2. Transaction mode (basic).

## Missing

1. `COPY` support.
2. Query cancellation support.
2. All the features I promised above. Will make them soon, promise :-).
