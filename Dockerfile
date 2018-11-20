FROM rust:1.30.0

WORKDIR /app
COPY . .

RUN cargo build

CMD ["cargo", "run"]
