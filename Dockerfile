# Build stage
FROM rust:1.81-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src/ ./src/
RUN cargo build --release --target x86_64-unknown-linux-musl

# Runtime stage — scratch (fully static binary)
FROM scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/ingest-parser /ingest-parser
ENTRYPOINT ["/ingest-parser"]
