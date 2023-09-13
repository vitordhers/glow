use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};

use super::models::BinanceWsKlineResponse;

#[derive(EnumString, Serialize, Deserialize, Debug, Display)]
pub enum BinanceRequestMethod {
    #[strum(serialize = "SUBSCRIBE")]
    SUBSCRIBE,
    #[strum(serialize = "UNSUBSCRIBE")]
    UNSUBSCRIBE,
    #[strum(serialize = "LIST_SUBSCRIPTIONS")]
    LISTSUBSCRIPTIONS,
    #[strum(serialize = "SET_PROPERTY")]
    SETPROPERTY,
    #[strum(serialize = "GET_PROPERTY")]
    GETPROPERTY,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum BinanceWsResponse {
    Nil,
    Kline(BinanceWsKlineResponse),
}

impl Default for BinanceWsResponse {
    fn default() -> Self {
        Self::Nil
    }
}
