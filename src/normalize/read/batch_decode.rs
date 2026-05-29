use super::super::args::NormalizeArgs;
use super::super::build::BuildAccumulator;
use super::super::model::NormalizeInputs;
use crate::storage::StorageError;
use append::{append_batch_to_accumulator, append_batch_to_inputs};
use family::InputObjectFamily;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::Path;

mod append;
mod columns;
mod family;
mod row;

pub(super) fn append_batches(
    key: &str,
    path: &Path,
    inputs: &mut NormalizeInputs,
) -> Result<(), StorageError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let family = InputObjectFamily::from_key(key);
    for batch in reader {
        let batch = batch?;
        if let Some(family) = family {
            append_batch_to_inputs(family, &batch, inputs)?;
        }
    }
    Ok(())
}

pub(super) fn append_batches_to_accumulator(
    key: &str,
    path: &Path,
    args: &NormalizeArgs,
    accumulator: &mut BuildAccumulator,
) -> Result<(), StorageError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let Some(family) = InputObjectFamily::from_key(key) else {
        return Ok(());
    };
    for batch in reader {
        append_batch_to_accumulator(family, &batch?, args, accumulator)?;
    }
    Ok(())
}
