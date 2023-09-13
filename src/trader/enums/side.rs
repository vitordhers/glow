use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy)]
pub enum Side {
    Sell,
    None,
    Buy,
}

impl Into<i32> for Side {
    fn into(self) -> i32 {
        match self {
            Side::Sell => -1,
            Side::None => 0,
            Side::Buy => 1,
        }
    }
}

impl From<i32> for Side {
    fn from(position: i32) -> Self {
        if position == 0 {
            Side::None
        } else if position == 1 {
            Side::Buy
        } else if position == -1 {
            Side::Sell
        } else {
            panic!("Invalid position for Side");
        }
    }
}
