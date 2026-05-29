use super::{LiveMarketNatsConfig, MarketLiveTick};
use bytes::Bytes;
use std::error::Error;

pub struct LiveMarketPublisher {
    client: async_nats::Client,
    jetstream: async_nats::jetstream::Context,
    stream: String,
    subject_prefix: String,
}

impl LiveMarketPublisher {
    pub async fn connect(config: &LiveMarketNatsConfig) -> Result<Self, Box<dyn Error>> {
        let client = async_nats::connect(&config.url).await?;
        let jetstream = async_nats::jetstream::new(client.clone());
        Ok(Self {
            client,
            jetstream,
            stream: config.stream.clone(),
            subject_prefix: config.subject_prefix.clone(),
        })
    }

    pub async fn publish_tick(&self, tick: &MarketLiveTick) -> Result<(), Box<dyn Error>> {
        if !tick.has_mark_price() {
            return Ok(());
        }
        let bytes = Bytes::from(serde_json::to_vec(tick)?);
        let message = async_nats::jetstream::message::PublishMessage::build()
            .expected_stream(&self.stream)
            .message_id(&tick.event_id)
            .payload(bytes);
        let subject = format!(
            "{}.{}.{}",
            self.subject_prefix,
            subject_token(&tick.venue),
            subject_token(&tick.symbol_canonical)
        );
        let ack = self.jetstream.send_publish(subject, message).await?.await?;
        if ack.stream != self.stream {
            return Err(format!(
                "NATS JetStream ack stream mismatch: expected {}, got {}",
                self.stream, ack.stream
            )
            .into());
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), Box<dyn Error>> {
        self.client.flush().await?;
        Ok(())
    }
}

fn subject_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}
