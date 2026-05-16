# syntax=docker/dockerfile:1

FROM public.ecr.aws/docker/library/rust:1.94-bookworm AS builder

WORKDIR /opt/nangman-crypto

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . /opt/nangman-crypto

RUN cargo build --release \
    --manifest-path /opt/nangman-crypto/Cargo.toml

FROM public.ecr.aws/docker/library/debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /usr/sbin/nologin market-ingest \
    && mkdir -p /opt/nangman-crypto/data/spool/market-ingest/l0 \
    && mkdir -p /opt/nangman-crypto/data/spool/market-ingest/l1 \
    && mkdir -p /opt/nangman-crypto/strategies/crypto/rust-engine/config \
    && chown -R market-ingest:market-ingest /opt/nangman-crypto

COPY --from=builder \
    /opt/nangman-crypto/target/release/market-ingest-app \
    /usr/local/bin/market-ingest-app
COPY --from=builder \
    /opt/nangman-crypto/target/release/market-normalize \
    /usr/local/bin/market-normalize
COPY --from=builder \
    /opt/nangman-crypto/target/release/market-backfill \
    /usr/local/bin/market-backfill
COPY --from=builder \
    /opt/nangman-crypto/target/release/crypto-market-ingest-supervisor \
    /usr/local/bin/crypto-market-ingest-supervisor
COPY --from=builder \
    /opt/nangman-crypto/config \
    /opt/nangman-crypto/strategies/crypto/rust-engine/config

USER market-ingest

ENV AWS_SDK_LOAD_CONFIG=1

ENTRYPOINT ["/usr/local/bin/crypto-market-ingest-supervisor"]
