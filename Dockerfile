FROM rust:bullseye AS builder
COPY . /app
WORKDIR /app
RUN cargo build --release

WORKDIR /etc/pgcat
RUN cp /app/target/release/pgcat /usr/bin/pgcat
RUN cp /app/pgcat.toml /etc/pgcat/pgcat.toml

ENV RUST_LOG=info
CMD ["pgcat"]
