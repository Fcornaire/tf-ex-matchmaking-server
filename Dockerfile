FROM rust:1.86-slim-bookworm AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/tf-matchmaking-server

COPY ./src /usr/src/tf-matchmaking-server/src
COPY ./Cargo.toml /usr/src/tf-matchmaking-server/Cargo.toml
COPY ./Cargo.lock /usr/src/tf-matchmaking-server/Cargo.lock
COPY ./README.md /usr/src/tf-matchmaking-server/README.md

RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/tf-matchmaking-server/target/release/tf-matchmaking-server /usr/local/bin/tf-matchmaking-server

CMD ["tf-matchmaking-server"]
