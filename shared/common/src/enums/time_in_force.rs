use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Copy, Default)]
pub enum TimeInForce {
    #[default]
    GTC, //GoodTillCancel
    IOC, //ImmediateOrCancel
    FOK, //FillOrKill
    PostOnly,
}
