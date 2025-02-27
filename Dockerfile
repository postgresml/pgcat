FROM rust:1.81.0-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y build-essential

COPY . /app
WORKDIR /app
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install  -o Dpkg::Options::=--force-confdef -yq --no-install-recommends \
    postgresql-client \
    # Clean up layer
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && truncate -s 0 /var/log/*log
COPY --from=builder /app/target/release/pgcat /usr/bin/pgcat
COPY --from=builder /app/pgcat.toml /etc/pgcat/pgcat.toml
WORKDIR /etc/pgcat
ENV RUST_LOG=info
CMD ["pgcat"]
STOPSIGNAL SIGINT
