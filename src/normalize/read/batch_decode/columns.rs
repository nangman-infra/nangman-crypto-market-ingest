use crate::storage::StorageError;
use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};

pub(super) fn string_value(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<String, StorageError> {
    let array = downcast_column::<StringArray>(batch, name)?;
    Ok(if array.is_null(row) {
        String::new()
    } else {
        array.value(row).to_owned()
    })
}

pub(super) fn int64_value(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<i64, StorageError> {
    let array = downcast_column::<Int64Array>(batch, name)?;
    Ok(if array.is_null(row) {
        0
    } else {
        array.value(row)
    })
}

pub(super) fn int64_optional(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<i64>, StorageError> {
    let array = downcast_column::<Int64Array>(batch, name)?;
    Ok(if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    })
}

pub(super) fn bool_value(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<bool, StorageError> {
    let array = downcast_column::<BooleanArray>(batch, name)?;
    Ok(!array.is_null(row) && array.value(row))
}

fn downcast_column<'a, T: 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a T, StorageError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|error| StorageError::InvalidConfig(error.to_string()))?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| StorageError::InvalidConfig(format!("column {name} has unexpected type")))
}
