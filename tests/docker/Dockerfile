FROM rust:bullseye

COPY --from=sclevine/yj /bin/yj /bin/yj
RUN /bin/yj -h
RUN apt-get update && apt-get install llvm-11 psmisc postgresql-contrib postgresql-client ruby ruby-dev libpq-dev python3 python3-pip lcov curl sudo iproute2 gnupg lsb-release -y
RUN env DEBIAN_FRONTEND=noninteractive apt-get -y install krb5-kdc krb5-admin-server krb5-user

RUN cargo install cargo-binutils rustfilt
RUN rustup component add llvm-tools-preview
RUN sudo gem install bundler
RUN wget -O toxiproxy-2.4.0.deb https://github.com/Shopify/toxiproxy/releases/download/v2.4.0/toxiproxy_2.4.0_linux_$(dpkg --print-architecture).deb && \
	sudo dpkg -i toxiproxy-2.4.0.deb
RUN wget -O go1.21.3.linux-$(dpkg --print-architecture).tar.gz https://go.dev/dl/go1.21.3.linux-$(dpkg --print-architecture).tar.gz && \
    sudo tar -C /usr/local -xzf go1.21.3.linux-$(dpkg --print-architecture).tar.gz && \
    rm go1.21.3.linux-$(dpkg --print-architecture).tar.gz
