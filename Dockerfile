FROM public.ecr.aws/docker/library/rust:1.94-bookworm AS builder

WORKDIR /opt/nangman-crypto

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*

ARG NANGMAN_GIT_SHA=unknown
ARG NANGMAN_GIT_DIRTY=true
ENV NANGMAN_GIT_SHA=${NANGMAN_GIT_SHA} \
    NANGMAN_GIT_DIRTY=${NANGMAN_GIT_DIRTY}

COPY . /opt/nangman-crypto

RUN cargo build --release \
    --manifest-path /opt/nangman-crypto/Cargo.toml

FROM public.ecr.aws/docker/library/debian:bookworm-slim AS runtime-layout

RUN mkdir -p /opt/nangman-crypto/data/spool/market-ingest/l0 \
    && mkdir -p /opt/nangman-crypto/data/spool/market-ingest/l1 \
    && mkdir -p /opt/nangman-crypto/data/spool/market-normalize/catchup \
    && chown -R 65532:65532 /opt/nangman-crypto

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

COPY --from=builder --chown=nonroot:nonroot \
    /opt/nangman-crypto/target/release/market-ingest-app \
    /usr/local/bin/market-ingest-app
COPY --from=builder --chown=nonroot:nonroot \
    /opt/nangman-crypto/target/release/market-normalize \
    /usr/local/bin/market-normalize
COPY --from=builder --chown=nonroot:nonroot \
    /opt/nangman-crypto/target/release/market-backfill \
    /usr/local/bin/market-backfill
COPY --from=builder --chown=nonroot:nonroot \
    /opt/nangman-crypto/target/release/crypto-market-ingest-supervisor \
    /usr/local/bin/crypto-market-ingest-supervisor
COPY --from=builder --chown=nonroot:nonroot \
    /opt/nangman-crypto/config \
    /opt/nangman-crypto/strategies/crypto/rust-engine/config
COPY --from=runtime-layout --chown=nonroot:nonroot \
    /opt/nangman-crypto/data \
    /opt/nangman-crypto/data

USER nonroot:nonroot

ENV AWS_SDK_LOAD_CONFIG=1 \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

ENTRYPOINT ["/usr/local/bin/crypto-market-ingest-supervisor"]
