FROM rust:1.86.0

COPY . /app
WORKDIR /app

RUN cargo update
RUN cargo build --release

CMD ["./target/release/tf-matchmaking-server"]
