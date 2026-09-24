FROM rust:1.85-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY . .
RUN cargo build --locked --release -p kvstore2

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 kuberic
COPY --from=builder /workspace/target/release/kvstore2 /usr/local/bin/kvstore2
USER 10001
EXPOSE 8080 50051 50052
ENTRYPOINT ["/usr/local/bin/kvstore2"]
