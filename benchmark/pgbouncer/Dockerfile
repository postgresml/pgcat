FROM debian:bullseye-slim as builder

ARG VERSION=1.17.0
RUN \
  cd tmp && \
  apt-get update && apt-get upgrade && apt-get install libevent-2.1-7 autoconf autoconf-doc automake bash libc-ares2 curl gcc jq libc-dev libevent-dev libtool make man libssl1.1 libssl-dev pkg-config libpq5 libpq-dev util-linux -y && \
  curl -o  /tmp/pgbouncer.tar.gz -L https://pgbouncer.github.io/downloads/files/$VERSION/pgbouncer-$VERSION.tar.gz && \
  tar xvfz /tmp/pgbouncer.tar.gz && \
  mv pgbouncer-$VERSION pgbouncer && \
  cd pgbouncer && \
  ./configure --prefix=/usr && \
  make

FROM debian:bullseye-slim

RUN apt-get update && apt-get upgrade && apt-get install libevent-2.1-7 postgresql-contrib -y
COPY --from=builder /tmp/pgbouncer/pgbouncer /usr/bin/pgbouncer
COPY userlist.txt /etc/userlist.txt
RUN  mkdir -p /etc/pgbouncer /var/log/pgbouncer /var/run/pgbouncer && \
     adduser --disabled-password --system pgbouncer && \
     chown pgbouncer /var/run/pgbouncer && \
     chown pgbouncer /var/log/pgbouncer

USER pgbouncer
ENTRYPOINT ["/usr/bin/pgbouncer", "/etc/pgbouncer.ini"]

