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
    && cargo build --release --locked \
    && rm -rf src target/release/deps/web_proxy*
COPY src ./src
COPY assets ./assets
RUN cargo build --release --locked \
    && strip target/release/web-proxy

FROM ${DEBIAN_IMAGE} AS runtime
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app
COPY --from=builder /workspace/target/release/web-proxy /usr/local/bin/web-proxy
COPY docker/healthcheck.sh /usr/local/bin/healthcheck.sh
RUN chmod 0555 /usr/local/bin/web-proxy /usr/local/bin/healthcheck.sh \
    && mkdir -p /data \
    && chown app:app /data
USER app:app
VOLUME ["/data"]
EXPOSE 20516
ENTRYPOINT ["/usr/local/bin/web-proxy"]