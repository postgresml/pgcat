FROM rust:bullseye

RUN apt-get update && apt-get install llvm-11 psmisc postgresql-contrib postgresql-client ruby ruby-dev libpq-dev python3 python3-pip lcov sudo curl -y
RUN cargo install cargo-binutils rustfilt
RUN rustup component add llvm-tools-preview
