# Стейдж сборки
FROM rust:1.80-alpine AS builder
RUN apk add --no-cache musl-dev

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

RUN cargo build --release -p pg-interleave-server

# Финальный минимальный образ
FROM alpine:3.20
RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /app/target/release/pg-interleave-server /usr/local/bin/pg-interleave

EXPOSE 5433 9090

ENTRYPOINT ["pg-interleave"]
CMD ["--listen-addr", "0.0.0.0:5433", "--target-addr", "postgres:5432", "--control-addr", "0.0.0.0:9090"]