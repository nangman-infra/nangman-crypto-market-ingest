use super::{DEFAULT_L0_S3_BUCKET, DEFAULT_L1_S3_BUCKET};

pub fn print_help() {
    println!(
        r#"crypto-market-ingest-supervisor
Usage:
  crypto-market-ingest-supervisor \
    --l0-s3-bucket {} \
    --l1-s3-bucket {}

Runs the all-in-one market data service:
  1. realtime L0 ingest
  2. historical bootstrap backfill
  3. long-lived L1 normalization

The ECS service should run this supervisor as the only container entrypoint."#,
        DEFAULT_L0_S3_BUCKET, DEFAULT_L1_S3_BUCKET
    );
}
