use crate::normalize::model::SliceRow;
use crate::storage::StorageError;
use arrow_array::builder::{ArrayBuilder, Int64Builder, ListBuilder, StringBuilder, StructBuilder};
use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use std::sync::Arc;

pub(super) fn string_const_col(len: usize, value: &str) -> ArrayRef {
    Arc::new(StringArray::from_iter_values((0..len).map(|_| value)))
}

pub(super) fn string_col(rows: &[&SliceRow], value: impl Fn(&SliceRow) -> &String) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        rows.iter().map(|row| value(row)),
    ))
}

pub(super) fn int64_col(rows: &[&SliceRow], value: impl Fn(&SliceRow) -> i64) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|row| value(row)),
    ))
}

pub(super) fn float64_col(
    rows: &[&SliceRow],
    value: impl Fn(&SliceRow) -> Option<f64>,
) -> ArrayRef {
    Arc::new(Float64Array::from_iter(rows.iter().map(|row| value(row))))
}

pub(super) fn bool_col(rows: &[&SliceRow], value: impl Fn(&SliceRow) -> bool) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(
        rows.iter().map(|row| Some(value(row))),
    ))
}

pub(super) fn string_list_col(
    rows: &[&SliceRow],
    value: impl Fn(&SliceRow) -> &Vec<String>,
) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        for item in value(row) {
            builder.values().append_value(item);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

pub(super) fn struct_field_mut<'a, T: ArrayBuilder + 'static>(
    builder: &'a mut StructBuilder,
    field_index: usize,
    field_name: &str,
) -> Result<&'a mut T, StorageError> {
    builder.field_builder::<T>(field_index).ok_or_else(|| {
        StorageError::InvalidConfig(format!(
            "normalize writer schema mismatch: field `{field_name}` at index {field_index} has unexpected builder type"
        ))
    })
}

pub(super) fn append_i64_optional(
    builder: &mut StructBuilder,
    field_index: usize,
    field_name: &str,
    value: Option<i64>,
) -> Result<(), StorageError> {
    let field = struct_field_mut::<Int64Builder>(builder, field_index, field_name)?;
    if let Some(value) = value {
        field.append_value(value);
    } else {
        field.append_null();
    }
    Ok(())
}

pub(super) fn append_struct_string_list(
    builder: &mut StructBuilder,
    field_index: usize,
    field_name: &str,
    values: &[String],
) -> Result<(), StorageError> {
    let list = struct_field_mut::<ListBuilder<StringBuilder>>(builder, field_index, field_name)?;
    for value in values {
        list.values().append_value(value);
    }
    list.append(true);
    Ok(())
}
