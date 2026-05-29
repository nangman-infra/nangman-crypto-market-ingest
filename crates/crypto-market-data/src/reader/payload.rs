use crate::error::MarketDataError;
use std::str;
use tokio_tungstenite::tungstenite;

pub(super) fn websocket_text_payload(
    message: tungstenite::Message,
) -> Result<Option<String>, MarketDataError> {
    websocket_text_payload_for(message, "ticker payload")
}

pub(super) fn websocket_text_payload_for(
    message: tungstenite::Message,
    close_context: &str,
) -> Result<Option<String>, MarketDataError> {
    match message {
        tungstenite::Message::Text(text) => Ok(Some(text.to_string())),
        tungstenite::Message::Binary(bytes) => {
            let text = str::from_utf8(bytes.as_ref()).map_err(|error| {
                MarketDataError::InvalidMessage(format!(
                    "binary websocket payload is not utf-8: {error}"
                ))
            })?;
            Ok(Some(text.to_owned()))
        }
        tungstenite::Message::Ping(_)
        | tungstenite::Message::Pong(_)
        | tungstenite::Message::Frame(_) => Ok(None),
        tungstenite::Message::Close(frame) => Err(MarketDataError::InvalidMessage(format!(
            "websocket closed before {close_context}: {frame:?}"
        ))),
    }
}
