use super::dtos::ws::incoming::TickMessage;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum OutgoingWsMessageMethod {
    #[serde(rename = "SUBSCRIBE")]
    Subscribe,
    #[serde(rename = "UNSUBSCRIBE")]
    Unsubscribe,
    #[serde(rename = "LIST_SUBSCRIPTIONS")]
    ListSubscriptions,
    #[serde(rename = "SET_PROPERTY")]
    SetProperty,
    #[serde(rename = "GET_PROPERTY")]
    GetProperty,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum IncomingWsMessage {
    None,
    Tick(TickMessage),
}

impl Default for IncomingWsMessage {
    fn default() -> Self {
        Self::None
    }
}
