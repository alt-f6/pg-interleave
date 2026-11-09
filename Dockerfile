FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev

WORKDIR /app
COPY Cargo.toml ./
COPY crates/ crates/

RUN cargo build --release -p pg-interleave-server

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/target/release/pg-interleave-server /usr/local/bin/pg-interleave

EXPOSE 5433 9090

ENTRYPOINT ["pg-interleave"]
CMD ["--listen-addr", "0.0.0.0:5433", "--target-addr", "postgres:5432", "--control-addr", "0.0.0.0:9090"]