# syntax=docker/dockerfile:1.7

ARG RUST_IMAGE=rust:1.85-bookworm
ARG DEBIAN_IMAGE=debian:bookworm-slim

FROM ${RUST_IMAGE} AS builder
WORKDIR /workspace
ENV CARGO_HOME=/usr/local/cargo \
    CARGO_TERM_COLOR=never \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
RUN apt-get update \
    && apt-get install --no-install-recommends -y pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release --locked --offline=false \
    && rm -rf src target/release/deps/edge_accelerator*
COPY src ./src
RUN cargo build --release --locked \
    && strip target/release/edge-accelerator

FROM ${DEBIAN_IMAGE} AS runtime
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app
COPY --from=builder /workspace/target/release/edge-accelerator /usr/local/bin/edge-accelerator
COPY docker/healthcheck.sh /usr/local/bin/healthcheck.sh
RUN chmod 0555 /usr/local/bin/edge-accelerator /usr/local/bin/healthcheck.sh
USER app:app
EXPOSE 20516
ENTRYPOINT ["/usr/local/bin/edge-accelerator"]