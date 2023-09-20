FROM rust:1-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y build-essential

COPY . /app
WORKDIR /app
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/pgcat /usr/bin/pgcat
COPY --from=builder /app/pgcat.toml /etc/pgcat/pgcat.toml
WORKDIR /etc/pgcat
ENV RUST_LOG=info
CMD ["pgcat"]
