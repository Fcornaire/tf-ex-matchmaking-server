FROM rust:1.95-slim-bookworm AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/tf-ex-matchmaking-server

COPY ./src /usr/src/tf-ex-matchmaking-server/src
COPY ./Cargo.toml /usr/src/tf-ex-matchmaking-server/Cargo.toml
COPY ./Cargo.lock /usr/src/tf-ex-matchmaking-server/Cargo.lock
COPY ./README.md /usr/src/tf-ex-matchmaking-server/README.md

RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/tf-ex-matchmaking-server/target/release/tf-ex-matchmaking-server /usr/local/bin/tf-ex-matchmaking-server

CMD ["tf-ex-matchmaking-server"]
