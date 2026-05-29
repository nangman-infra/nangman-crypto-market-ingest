use super::*;

fn base_args() -> Vec<String> {
    vec![
        "--venue".to_owned(),
        "binance".to_owned(),
        "--input-start-ms".to_owned(),
        "1".to_owned(),
        "--input-end-ms".to_owned(),
        "2".to_owned(),
        "--l0-s3-bucket".to_owned(),
        "bucket".to_owned(),
    ]
}

#[test]
fn parses_symbols_as_uppercase() {
    let mut raw = base_args();
    raw.push("--symbols".to_owned());
    raw.push("btcusdt, ethusdt".to_owned());
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(
        parsed.symbols,
        Some(vec!["BTCUSDT".to_owned(), "ETHUSDT".to_owned()])
    );
    assert_eq!(parsed.s3_retention_days, 45);
    assert_eq!(parsed.s3_retention_max_deletes_per_run, 1_000);
    assert!(parsed.s3_retention_enabled);
}

#[test]
fn parses_s3_retention_knobs() {
    let mut raw = base_args();
    raw.push("--s3-retention-days".to_owned());
    raw.push("365".to_owned());
    raw.push("--s3-retention-max-deletes-per-run".to_owned());
    raw.push("50".to_owned());
    raw.push("--disable-s3-retention".to_owned());
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(parsed.s3_retention_days, 365);
    assert_eq!(parsed.s3_retention_max_deletes_per_run, 50);
    assert!(!parsed.s3_retention_enabled);
}

#[test]
fn rejects_missing_bucket() {
    let err = parse_args(
        vec![
            "--venue".to_owned(),
            "binance".to_owned(),
            "--input-start-ms".to_owned(),
            "1".to_owned(),
            "--input-end-ms".to_owned(),
            "2".to_owned(),
        ]
        .into_iter(),
    )
    .err()
    .unwrap();
    assert!(err.to_string().contains("--l0-s3-bucket"));
}

#[test]
fn rejects_non_increasing_range() {
    let mut raw = base_args();
    raw[3] = "5".to_owned();
    raw[5] = "5".to_owned();
    let err = parse_args(raw.into_iter()).err().unwrap();
    assert!(err.to_string().contains("greater"));
}

#[test]
fn rejects_public_doc_bucket_placeholder() {
    let mut raw = base_args();
    raw[7] = "nangman-crypto-dev-market-ingest-l0-<account-suffix>".to_owned();
    let err = parse_args(raw.into_iter()).err().unwrap();
    assert!(err.to_string().contains("public-doc placeholder"));
}

#[test]
fn rejects_relative_config_dir() {
    let mut raw = base_args();
    raw.push("--config".to_owned());
    raw.push("config".to_owned());
    let err = parse_args(raw.into_iter()).err().unwrap();
    assert!(
        err.to_string()
            .contains("--config requires an absolute config directory path")
    );
}

#[test]
fn rejects_relative_l0_spool_root() {
    let mut raw = base_args();
    raw.push("--l0-spool-root".to_owned());
    raw.push("spool".to_owned());
    let err = parse_args(raw.into_iter()).err().unwrap();
    assert!(
        err.to_string()
            .contains("--l0-spool-root requires an absolute directory path")
    );
}

#[test]
fn rejects_rest_base_url_credentials() {
    let mut raw = base_args();
    raw.push("--rest-base-url".to_owned());
    raw.push("https://user:secret@api.binance.com".to_owned());
    let err = parse_args(raw.into_iter()).err().unwrap();

    assert!(err.to_string().contains("must not include credentials"));
}

#[test]
fn rejects_rest_base_url_query_or_fragment() {
    for rest_base_url in [
        "https://api.binance.com?existing=query",
        "https://api.binance.com#fragment",
    ] {
        let mut raw = base_args();
        raw.push("--rest-base-url".to_owned());
        raw.push(rest_base_url.to_owned());
        let err = parse_args(raw.into_iter()).err().unwrap();

        assert!(err.to_string().contains("query or fragment"));
    }
}
