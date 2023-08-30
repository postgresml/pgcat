FROM rust:1.72.0-bullseye
RUN ls -la /usr/bin
COPY . /app
WORKDIR /app
RUN cargo build --release

COPY /app/target/release/pgcat /usr/bin/pgcat
COPY /app/pgcat.toml /etc/pgcat/pgcat.toml
WORKDIR /etc/pgcat
ENV RUST_LOG=info
CMD ["pgcat"]
