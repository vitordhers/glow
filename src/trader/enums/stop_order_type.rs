use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Copy)]
pub enum StopOrderType {
    StopLoss,
    TakeGain,
}
