use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Debug, PartialEq, PartialOrd, Clone, Copy)]
#[repr(u8)]
pub enum PositionLock {
    #[default]
    #[serde(rename="none")]
    None = 0,
    #[serde(rename="fee")]
    Fee = 1,  // absolute value of revenue > transaction fee
    #[serde(rename="loss")]
    Loss = 2, // least percentage for the trade to close
}
