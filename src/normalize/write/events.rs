use super::primitive::{append_i64_optional, struct_field_mut};
use crate::normalize::model::{CompactEventRef, SliceRow};
use crate::normalize::write_schema::{book_fields, compact_fields, trade_fields};
use crate::storage::StorageError;
use arrow_array::ArrayRef;
use arrow_array::builder::{
    Float64Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
};
use std::sync::Arc;

pub(super) fn trade_events_col(rows: &[&SliceRow]) -> Result<ArrayRef, StorageError> {
    let mut builder = ListBuilder::new(StructBuilder::from_fields(trade_fields(), 0));
    for row in rows {
        for trade in &row.trade_events {
            let values = builder.values();
            struct_field_mut::<Int64Builder>(values, 0, "exchange_timestamp_ms")?
                .append_value(trade.exchange_timestamp_ms);
            struct_field_mut::<Int64Builder>(values, 1, "ingest_timestamp_ms")?
                .append_value(trade.ingest_timestamp_ms);
            struct_field_mut::<Float64Builder>(values, 2, "price")?.append_value(trade.price);
            struct_field_mut::<Float64Builder>(values, 3, "quantity")?.append_value(trade.quantity);
            struct_field_mut::<StringBuilder>(values, 4, "side")?.append_value(&trade.side);
            append_i64_optional(values, 5, "exchange_sequence", trade.exchange_sequence)?;
            struct_field_mut::<StringBuilder>(values, 6, "parent_event_id")?
                .append_value(&trade.parent_event_id);
            values.append(true);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

pub(super) fn book_ticker_events_col(rows: &[&SliceRow]) -> Result<ArrayRef, StorageError> {
    let mut builder = ListBuilder::new(StructBuilder::from_fields(book_fields(), 0));
    for row in rows {
        for book in &row.book_ticker_events {
            let values = builder.values();
            struct_field_mut::<Int64Builder>(values, 0, "exchange_timestamp_ms")?
                .append_value(book.exchange_timestamp_ms);
            struct_field_mut::<Int64Builder>(values, 1, "ingest_timestamp_ms")?
                .append_value(book.ingest_timestamp_ms);
            struct_field_mut::<Float64Builder>(values, 2, "best_bid")?.append_value(book.best_bid);
            struct_field_mut::<Float64Builder>(values, 3, "best_bid_qty")?
                .append_value(book.best_bid_qty);
            struct_field_mut::<Float64Builder>(values, 4, "best_ask")?.append_value(book.best_ask);
            struct_field_mut::<Float64Builder>(values, 5, "best_ask_qty")?
                .append_value(book.best_ask_qty);
            append_i64_optional(values, 6, "exchange_sequence", book.exchange_sequence)?;
            struct_field_mut::<StringBuilder>(values, 7, "parent_event_id")?
                .append_value(&book.parent_event_id);
            values.append(true);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

pub(super) fn compact_events_col(
    rows: &[&SliceRow],
    value: impl Fn(&SliceRow) -> &Vec<CompactEventRef>,
) -> Result<ArrayRef, StorageError> {
    let mut builder = ListBuilder::new(StructBuilder::from_fields(compact_fields(), 0));
    for row in rows {
        for event in value(row) {
            let values = builder.values();
            struct_field_mut::<Int64Builder>(values, 0, "exchange_timestamp_ms")?
                .append_value(event.exchange_timestamp_ms);
            struct_field_mut::<Int64Builder>(values, 1, "ingest_timestamp_ms")?
                .append_value(event.ingest_timestamp_ms);
            struct_field_mut::<StringBuilder>(values, 2, "event_type")?
                .append_value(&event.event_type);
            struct_field_mut::<StringBuilder>(values, 3, "parent_event_id")?
                .append_value(&event.parent_event_id);
            values.append(true);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}
