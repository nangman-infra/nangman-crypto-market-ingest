use super::FixedDecimal;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};

impl Serialize for FixedDecimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("FixedDecimal", 2)?;
        state.serialize_field("value", &self.value.to_string())?;
        state.serialize_field("scale", &self.scale)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for FixedDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct FixedDecimalWire {
            value: String,
            scale: u32,
        }

        let wire = FixedDecimalWire::deserialize(deserializer)?;
        let value = wire
            .value
            .parse::<i128>()
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            value,
            scale: wire.scale,
        })
    }
}
