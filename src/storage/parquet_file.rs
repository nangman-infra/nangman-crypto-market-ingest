use super::StorageError;
use super::record::RawMarketEventRecord;
use columns::raw_market_event_batch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use schema::raw_market_event_schema;
use std::fs::File;
use std::path::Path;

mod columns;
mod schema;
#[cfg(test)]
mod tests;

pub fn write_raw_market_event_parquet(
    path: &Path,
    records: &[RawMarketEventRecord],
) -> Result<(), StorageError> {
    let schema = raw_market_event_schema();
    let batch = raw_market_event_batch(schema.clone(), records)?;
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
