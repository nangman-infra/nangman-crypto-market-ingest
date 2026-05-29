pub(super) fn print_help() {
    println!(
        "market-ingest-app\n\
         Usage:\n\
           cargo run --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml -- \\\n\
             --venue binance \\\n\
             --config /opt/nangman-crypto/strategies/crypto/rust-engine/config \\\n\
             --duration-seconds 15 \\\n\
             --log-interval-seconds 5 \\\n\
             --depth-snapshot-limit 100 \\\n\
             --binance-futures-rest-base-url https://fapi.binance.com \\\n\
             --binance-derivatives-snapshot-interval-seconds 300 \\\n\
             --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-<account-suffix>\n\
          cargo run --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml -- \\\n\
             --venue upbit \\\n\
             --config /opt/nangman-crypto/strategies/crypto/rust-engine/config \\\n\
             --duration-seconds 15 \\\n\
             --log-interval-seconds 5 \\\n\
             --expect-symbol-count 50 \\\n\
             --upbit-orderbook-unit 5 \\\n\
             --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-<account-suffix>\n\
         \n\
         This reads Binance or Upbit public WebSocket streams only. It does not use private APIs,\n\
         credentials, AI hot-path decisions, order placement, or live trading.\n\
         S3 retention cleanup is app-owned when --l0-s3-bucket is set. L0 defaults to 45 days;\n\
         bucket lifecycle remains a fallback safety net at 60 days."
    );
}
