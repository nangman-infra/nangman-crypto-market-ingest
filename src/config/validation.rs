use std::error::Error;

use super::types::ExchangeSettings;

pub(super) fn validate_max_latency(max_latency_ms: i64) -> Result<(), Box<dyn Error>> {
    if max_latency_ms <= 0 {
        return Err("cost.paper.toml max_latency_ms must be positive".into());
    }
    Ok(())
}

pub(super) fn validate_exchanges(exchanges: &[ExchangeSettings]) -> Result<(), Box<dyn Error>> {
    for exchange in exchanges {
        if exchange.enabled {
            validate_endpoint_url(
                &exchange.id,
                "rest_base_url",
                &exchange.rest_base_url,
                "https",
            )?;
            validate_endpoint_url(
                &exchange.id,
                "websocket_url",
                &exchange.websocket_url,
                "wss",
            )?;
        }
    }
    Ok(())
}

fn validate_endpoint_url(
    exchange_id: &str,
    field_name: &str,
    value: &str,
    expected_scheme: &str,
) -> Result<(), Box<dyn Error>> {
    let url = reqwest::Url::parse(value.trim()).map_err(|error| {
        format!("exchange {exchange_id} {field_name} must be a valid URL: {error}")
    })?;
    if url.scheme() != expected_scheme {
        return Err(
            format!("exchange {exchange_id} {field_name} must use {expected_scheme}").into(),
        );
    }
    if url.host_str().is_none() {
        return Err(format!("exchange {exchange_id} {field_name} must include a host").into());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(
            format!("exchange {exchange_id} {field_name} must not include credentials").into(),
        );
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(format!(
            "exchange {exchange_id} {field_name} must not include query or fragment components"
        )
        .into());
    }
    Ok(())
}
