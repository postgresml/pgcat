FROM rust:1 AS builder
WORKDIR /app
COPY Cargo.toml /app/Cargo.toml
COPY src /app/src
RUN cargo build --release

FROM debian:bullseye-slim
COPY --from=builder /app/target/release/pgcat /usr/bin/pgcat
RUN apt-get update && apt-get install postgresql-contrib -y
COPY ./benchmark/pgcat/pgcat.toml /etc/pgcat/pgcat.toml
#COPY ./benchmark/pgcat/pgcat_old.toml /etc/pgcat/pgcat.toml
WORKDIR /etc/pgcat
ENV RUST_LOG=error
CMD ["pgcat"]
