use serde_json::Value;

pub(super) fn binance_data(value: &Value) -> Option<&Value> {
    value.get("data")
}

pub(super) fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}
