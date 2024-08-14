use glow_error::GlowError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy, Default)]
pub enum Side {
    #[default]
    None,
    Sell,
    Buy,
}

impl Side {
    pub fn get_opposite_side(&self) -> Result<Self, GlowError> {
        match self {
            Self::Buy => Ok(Self::Sell),
            Self::Sell => Ok(Self::Buy),
            Self::None => {
                let error = format!("get_opposite_side -> original side was None");
                return Err(GlowError::new(String::from("Invalid opposite Side"), error));
            }
        }
    }
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
            unreachable!("Invalid position for Side");
        }
    }
}
