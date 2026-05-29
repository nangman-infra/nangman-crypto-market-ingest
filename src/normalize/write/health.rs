use super::primitive::{append_struct_string_list, struct_field_mut};
use crate::normalize::model::SliceRow;
use crate::normalize::write_schema::{source_health_fields, symbol_health_fields};
use crate::storage::StorageError;
use arrow_array::ArrayRef;
use arrow_array::builder::{
    BooleanBuilder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
};
use std::sync::Arc;

pub(super) fn symbol_health_col(rows: &[&SliceRow]) -> Result<ArrayRef, StorageError> {
    let mut builder = symbol_health_builder();
    for row in rows {
        if let Some(snapshot) = &row.symbol_health_snapshot {
            struct_field_mut::<Int64Builder>(&mut builder, 0, "observed_at_ms")?
                .append_value(snapshot.observed_at_ms);
            struct_field_mut::<Int64Builder>(&mut builder, 1, "last_event_time_ms")?
                .append_value(snapshot.last_event_time_ms);
            struct_field_mut::<Int64Builder>(&mut builder, 2, "last_received_time_ms")?
                .append_value(snapshot.last_received_time_ms);
            struct_field_mut::<Int64Builder>(&mut builder, 3, "latency_ms")?
                .append_value(snapshot.latency_ms);
            struct_field_mut::<BooleanBuilder>(&mut builder, 4, "is_tradeable")?
                .append_value(snapshot.is_tradeable);
            append_struct_string_list(&mut builder, 5, "reason_codes", &snapshot.reason_codes)?;
            builder.append(true);
        } else {
            append_null_symbol_health(&mut builder)?;
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub(super) fn source_health_col(rows: &[&SliceRow]) -> Result<ArrayRef, StorageError> {
    let mut builder = source_health_builder();
    for row in rows {
        if let Some(snapshot) = &row.source_health_snapshot {
            struct_field_mut::<Int64Builder>(&mut builder, 0, "observed_at_ms")?
                .append_value(snapshot.observed_at_ms);
            struct_field_mut::<StringBuilder>(&mut builder, 1, "connection_status")?
                .append_value(&snapshot.connection_status);
            struct_field_mut::<StringBuilder>(&mut builder, 2, "health_level")?
                .append_value(&snapshot.health_level);
            struct_field_mut::<Int64Builder>(&mut builder, 3, "heartbeat_delay_ms")?
                .append_value(snapshot.heartbeat_delay_ms);
            struct_field_mut::<Int64Builder>(&mut builder, 4, "stream_lag_ms")?
                .append_value(snapshot.stream_lag_ms);
            struct_field_mut::<Int64Builder>(&mut builder, 5, "recent_gap_count")?
                .append_value(snapshot.recent_gap_count);
            struct_field_mut::<Int64Builder>(&mut builder, 6, "book_rebuild_count")?
                .append_value(snapshot.book_rebuild_count);
            builder.append(true);
        } else {
            append_null_source_health(&mut builder)?;
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn symbol_health_builder() -> StructBuilder {
    StructBuilder::new(
        symbol_health_fields(),
        vec![
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
            Box::new(BooleanBuilder::new()),
            Box::new(ListBuilder::new(StringBuilder::new())),
        ],
    )
}

fn source_health_builder() -> StructBuilder {
    StructBuilder::new(
        source_health_fields(),
        vec![
            Box::new(Int64Builder::new()),
            Box::new(StringBuilder::new()),
            Box::new(StringBuilder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
        ],
    )
}

fn append_null_symbol_health(builder: &mut StructBuilder) -> Result<(), StorageError> {
    struct_field_mut::<Int64Builder>(builder, 0, "observed_at_ms")?.append_null();
    struct_field_mut::<Int64Builder>(builder, 1, "last_event_time_ms")?.append_null();
    struct_field_mut::<Int64Builder>(builder, 2, "last_received_time_ms")?.append_null();
    struct_field_mut::<Int64Builder>(builder, 3, "latency_ms")?.append_null();
    struct_field_mut::<BooleanBuilder>(builder, 4, "is_tradeable")?.append_null();
    struct_field_mut::<ListBuilder<StringBuilder>>(builder, 5, "reason_codes")?.append(false);
    builder.append(false);
    Ok(())
}

fn append_null_source_health(builder: &mut StructBuilder) -> Result<(), StorageError> {
    struct_field_mut::<Int64Builder>(builder, 0, "observed_at_ms")?.append_null();
    struct_field_mut::<StringBuilder>(builder, 1, "connection_status")?.append_null();
    struct_field_mut::<StringBuilder>(builder, 2, "health_level")?.append_null();
    struct_field_mut::<Int64Builder>(builder, 3, "heartbeat_delay_ms")?.append_null();
    struct_field_mut::<Int64Builder>(builder, 4, "stream_lag_ms")?.append_null();
    struct_field_mut::<Int64Builder>(builder, 5, "recent_gap_count")?.append_null();
    struct_field_mut::<Int64Builder>(builder, 6, "book_rebuild_count")?.append_null();
    builder.append(false);
    Ok(())
}
