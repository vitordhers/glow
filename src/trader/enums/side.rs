use serde::{Deserialize, Serialize};

use crate::trader::errors::{CustomError, Error};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy, Default)]
pub enum Side {
    #[default]
    Nil,
    Sell,
    Buy,
}

impl Side {
    pub fn get_opposite_side(&self) -> Result<Self, Error> {
        match self {
            Self::Buy => Ok(Self::Sell),
            Self::Sell => Ok(Self::Buy),
            Self::Nil => {
                let error = format!("get_opposite_side -> original side was Nil");
                return Err(Error::CustomError(CustomError::new(error)));
            }
        }
    }
}

impl Into<i32> for Side {
    fn into(self) -> i32 {
        match self {
            Side::Sell => -1,
            Side::Nil => 0,
            Side::Buy => 1,
        }
    }
}

impl From<i32> for Side {
    fn from(position: i32) -> Self {
        if position == 0 {
            Side::Nil
        } else if position == 1 {
            Side::Buy
        } else if position == -1 {
            Side::Sell
        } else {
            panic!("Invalid position for Side");
        }
    }
}
