FROM ubuntu:23.04 AS builder

RUN apt-get update && \
    apt-get install -y build-essential curl

# Get Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

COPY . /app
WORKDIR /app
RUN /root/.cargo/bin/cargo build --release

FROM ubuntu:23.04
COPY --from=builder /app/target/release /usr/bin/
COPY --from=builder /app/pgcat.toml /etc/pgcat/pgcat.toml
WORKDIR /etc/pgcat
ENV RUST_LOG=info
CMD ["pgcat"]
