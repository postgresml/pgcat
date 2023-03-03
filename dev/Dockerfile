FROM rust:bullseye

# Dependencies
RUN apt-get update -y \
    && apt-get install -y \
    llvm-11 psmisc postgresql-contrib postgresql-client \
    ruby ruby-dev libpq-dev python3 python3-pip lcov curl sudo iproute2 \
    strace ngrep iproute2 dnsutils lsof net-tools telnet

# Rust
RUN cargo install cargo-binutils rustfilt
RUN rustup component add llvm-tools-preview

# Ruby
RUN sudo gem install bundler

# Toxyproxy
RUN wget -O toxiproxy-2.4.0.deb https://github.com/Shopify/toxiproxy/releases/download/v2.4.0/toxiproxy_2.4.0_linux_$(dpkg --print-architecture).deb && \
    sudo dpkg -i toxiproxy-2.4.0.deb

# Config
ENV APP_ROOT=/app
ARG APP_USER=pgcat
COPY dev_bashrc /etc/bash.bashrc

RUN useradd -m -o -u 999 ${APP_USER} || exit 0 && mkdir ${APP_ROOT} && chown ${APP_USER} ${APP_ROOT}
RUN adduser ${APP_USER} sudo \
    && echo "${APP_USER} ALL=NOPASSWD: ALL" > /etc/sudoers.d/${APP_USER} \
    && chmod ugo+s /usr/sbin/usermod /usr/sbin/groupmod
ENV HOME=${APP_ROOT}
WORKDIR ${APP_ROOT}

ENTRYPOINT ["/bin/bash"]
