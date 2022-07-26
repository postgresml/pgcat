FROM rust:1 AS builder
COPY . /app
WORKDIR /app
RUN cargo build --release

FROM debian:bullseye-slim
COPY --from=builder /app/target/release/pgcat /usr/bin/pgcat
COPY --from=builder /app/pgcat.toml /etc/pgcat/pgcat.toml
WORKDIR /etc/pgcat
ENV RUST_LOG=info
CMD ["pgcat"]
