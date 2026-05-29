pub fn print_help() {
    println!(
        "market-backfill\n\
         Usage:\n\
           cargo run --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml --bin market-backfill -- \\\n\
             --venue binance \\\n\
             --config /opt/nangman-crypto/strategies/crypto/rust-engine/config \\\n\
             --input-start-ms 1778042400000 \\\n\
             --input-end-ms 1778043300000 \\\n\
             --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-<account-suffix>\n\
           cargo run --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml --bin market-backfill -- \\\n\
             --venue upbit \\\n\
             --input-start-ms 1778572800000 \\\n\
             --input-end-ms 1778573400000 \\\n\
             --symbols KRW-BTC,KRW-ETH \\\n\
             --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-<account-suffix>\n\
         \n\
         This worker writes historical raw trade events into MARKET_L0_BUCKET only.\n\
         It also runs one app-owned S3 retention cleanup pass after the backfill manifest upload.\n\
         L0 defaults to 45 days; bucket lifecycle remains a fallback safety net at 60 days.\n\
         Binance uses public aggTrades for long-range trade backfill.\n\
         Upbit uses public recent trade history and rejects ranges older than the recent window."
    );
}
