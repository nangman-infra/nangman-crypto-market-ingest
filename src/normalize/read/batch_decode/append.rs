use super::family::InputObjectFamily;
use super::row::{
    gap_alert_from_batch, raw_event_from_batch, source_health_from_batch, symbol_health_from_batch,
};
use crate::normalize::args::NormalizeArgs;
use crate::normalize::build::BuildAccumulator;
use crate::normalize::model::{
    GapAlertInput, NormalizeInputs, RawInputEvent, SourceHealthInput, SymbolHealthInput,
};
use crate::storage::StorageError;
use arrow_array::RecordBatch;

pub(super) fn append_batch_to_inputs(
    family: InputObjectFamily,
    batch: &RecordBatch,
    inputs: &mut NormalizeInputs,
) -> Result<(), StorageError> {
    match family {
        InputObjectFamily::RawMarketEvent => append_raw_events(&mut inputs.raw_events, batch),
        InputObjectFamily::SymbolHealth => append_symbol_health(&mut inputs.symbol_health, batch),
        InputObjectFamily::SourceHealth => append_source_health(&mut inputs.source_health, batch),
        InputObjectFamily::GapAlert => append_gap_alerts(&mut inputs.gap_alerts, batch),
    }
}

pub(super) fn append_batch_to_accumulator(
    family: InputObjectFamily,
    batch: &RecordBatch,
    args: &NormalizeArgs,
    accumulator: &mut BuildAccumulator,
) -> Result<(), StorageError> {
    for row in 0..batch.num_rows() {
        append_accumulator_row(family, batch, row, args, accumulator)?;
    }
    Ok(())
}

fn append_accumulator_row(
    family: InputObjectFamily,
    batch: &RecordBatch,
    row: usize,
    args: &NormalizeArgs,
    accumulator: &mut BuildAccumulator,
) -> Result<(), StorageError> {
    match family {
        InputObjectFamily::RawMarketEvent => {
            accumulator.ingest_raw_event(args, raw_event_from_batch(batch, row)?);
        }
        InputObjectFamily::SymbolHealth => {
            accumulator.ingest_symbol_health(symbol_health_from_batch(batch, row)?);
        }
        InputObjectFamily::SourceHealth => {
            accumulator.ingest_source_health(source_health_from_batch(batch, row)?);
        }
        InputObjectFamily::GapAlert => {
            accumulator.ingest_gap_alert(gap_alert_from_batch(batch, row)?);
        }
    }
    Ok(())
}

fn append_raw_events(
    output: &mut Vec<RawInputEvent>,
    batch: &RecordBatch,
) -> Result<(), StorageError> {
    for row in 0..batch.num_rows() {
        output.push(raw_event_from_batch(batch, row)?);
    }
    Ok(())
}

fn append_symbol_health(
    output: &mut Vec<SymbolHealthInput>,
    batch: &RecordBatch,
) -> Result<(), StorageError> {
    for row in 0..batch.num_rows() {
        output.push(symbol_health_from_batch(batch, row)?);
    }
    Ok(())
}

fn append_source_health(
    output: &mut Vec<SourceHealthInput>,
    batch: &RecordBatch,
) -> Result<(), StorageError> {
    for row in 0..batch.num_rows() {
        output.push(source_health_from_batch(batch, row)?);
    }
    Ok(())
}

fn append_gap_alerts(
    output: &mut Vec<GapAlertInput>,
    batch: &RecordBatch,
) -> Result<(), StorageError> {
    for row in 0..batch.num_rows() {
        output.push(gap_alert_from_batch(batch, row)?);
    }
    Ok(())
}
