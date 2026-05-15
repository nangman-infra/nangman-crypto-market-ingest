use serde::{Deserialize, Serialize};

use crate::{
    AssetCode, Bps, Direction, Notional, OrderStyle, Price, Quantity, RecordId, Symbol, TimestampMs,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PositionEventType {
    Opened,
    Increased,
    Reduced,
    Closed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PositionEffect {
    OpenLong,
    IncreaseLong,
    ReduceLong,
    CloseLong,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AccountingMethod {
    WeightedAverageCost,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaperPositionFill {
    pub requested_quantity: Quantity,
    pub filled_quantity: Quantity,
    pub simulated_price: Price,
    pub reference_price: Price,
    pub executed_notional_quote: Notional,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaperPositionFee {
    pub fee_bps: Bps,
    pub fee_quote: Notional,
    pub fee_asset: AssetCode,
    pub fee_model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaperPositionState {
    pub symbol: Symbol,
    pub open_quantity: Quantity,
    pub average_entry_price: Price,
    pub cost_basis_quote: Notional,
    pub entry_fee_quote: Notional,
    pub realized_gross_pnl_quote: Notional,
    pub realized_net_pnl_quote: Notional,
    pub gross_unrealized_pnl_quote: Notional,
    pub net_unrealized_pnl_quote: Notional,
    pub last_mark_price: Price,
    pub updated_at_ms: TimestampMs,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaperRealizedPnl {
    pub matched_quantity: Quantity,
    pub matched_cost_basis_quote: Notional,
    pub matched_entry_fee_quote: Notional,
    pub gross_realized_pnl_quote: Notional,
    pub net_realized_pnl_quote: Notional,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaperUnrealizedPnl {
    pub mark_price: Price,
    pub gross_unrealized_pnl_quote: Notional,
    pub net_unrealized_pnl_quote: Notional,
    pub conservative_exit_fee_quote: Notional,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaperPositionAttribution {
    pub direction: Direction,
    pub order_style: OrderStyle,
    pub slippage_bps: Bps,
    pub latency_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaperPositionUpdatePayload {
    pub schema_version: u32,
    pub position_event_type: PositionEventType,
    pub source_order_record_id: RecordId,
    pub source_fill_record_id: RecordId,
    pub accounting_method: AccountingMethod,
    pub quote_asset: AssetCode,
    pub position_effect: PositionEffect,
    pub fill: PaperPositionFill,
    pub fee: PaperPositionFee,
    pub pre_position: PaperPositionState,
    pub post_position: PaperPositionState,
    pub realized_pnl: PaperRealizedPnl,
    pub unrealized_pnl: PaperUnrealizedPnl,
    pub attribution: PaperPositionAttribution,
}
