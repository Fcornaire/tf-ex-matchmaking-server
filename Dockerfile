FROM rust:latest

COPY . /app
WORKDIR /app

RUN cargo update
RUN cargo build --release

CMD ["./target/release/tf-matchmaking-server"]
