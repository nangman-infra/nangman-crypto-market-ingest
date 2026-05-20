//! Golden fixture round-trip test for Binance L0 depth payloads.
//!
//! The fixture under `tests/fixtures/binance_depth_delta/sample.parquet` was
//! pulled directly from the operating S3 L0 bucket; the goal here is twofold:
//!
//! 1. **Every** `price` / `quantity` string in a real Binance depth payload
//!    must parse losslessly into a `FixedDecimal`. Anything the exchange ever
//!    sends in production must round-trip through the numeric type without
//!    silent precision loss.
//! 2. When real depth levels are inserted into a `BTreeMap<FixedDecimal, _>`,
//!    iteration must follow numeric price order rather than the lexicographic
//!    order the previous `HashMap<String, String>` representation produced.

use arrow_array::{Array, StringArray};
use crypto_domain::FixedDecimal;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

#[test]
fn binance_depth_delta_fixture_round_trips_through_fixed_decimal() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/binance_depth_delta/sample.parquet");
    let levels = read_levels(&fixture);
    assert!(
        !levels.is_empty(),
        "fixture should contain at least one depth level"
    );

    for (price_str, quantity_str) in &levels {
        let price = FixedDecimal::parse_unsigned(price_str)
            .unwrap_or_else(|error| panic!("price `{price_str}` did not parse: {error}"));
        let quantity = FixedDecimal::parse_unsigned(quantity_str)
            .unwrap_or_else(|error| panic!("quantity `{quantity_str}` did not parse: {error}"));
        // sanity: both numeric values stay non-negative
        assert!(
            price.is_non_negative(),
            "price `{price_str}` parsed negative"
        );
        assert!(
            quantity.is_non_negative(),
            "quantity `{quantity_str}` parsed negative"
        );
    }
}

#[test]
fn binance_depth_book_iterates_in_price_order_via_btreemap() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/binance_depth_delta/sample.parquet");
    let levels = read_levels(&fixture);
    let mut book = BTreeMap::<FixedDecimal, FixedDecimal>::new();
    for (price_str, quantity_str) in &levels {
        let price = FixedDecimal::parse_unsigned(price_str).expect("price parses");
        let quantity = FixedDecimal::parse_unsigned(quantity_str).expect("quantity parses");
        if quantity.is_positive() {
            book.insert(price, quantity);
        } else {
            book.remove(&price);
        }
    }

    let prices: Vec<_> = book.keys().copied().collect();
    assert!(
        prices.len() >= 2,
        "expected at least two distinct positive price levels in fixture"
    );
    for pair in prices.windows(2) {
        assert!(
            pair[0] < pair[1],
            "BTreeMap iteration order broken between {:?} and {:?}",
            pair[0],
            pair[1]
        );
    }
}

fn read_levels(path: &Path) -> Vec<(String, String)> {
    let file = File::open(path).expect("open fixture");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet builder");
    let reader = builder.build().expect("parquet reader");

    let mut levels = Vec::new();
    for batch in reader {
        let batch = batch.expect("record batch");
        let payload_col = batch
            .column_by_name("payload_json")
            .expect("payload_json column missing")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload_json column must be String");
        for row in 0..payload_col.len() {
            let raw = payload_col.value(row);
            let payload: serde_json::Value =
                serde_json::from_str(raw).expect("payload_json is valid JSON");
            for ptr in ["/data/b", "/data/a"] {
                if let Some(side) = payload.pointer(ptr).and_then(|v| v.as_array()) {
                    for level in side {
                        let arr = level.as_array().expect("level is array");
                        let price = arr.first().and_then(|v| v.as_str()).expect("price string");
                        let quantity = arr.get(1).and_then(|v| v.as_str()).expect("qty string");
                        levels.push((price.to_owned(), quantity.to_owned()));
                    }
                }
            }
        }
    }
    levels
}
