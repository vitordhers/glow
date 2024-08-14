use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Copy)]
pub enum OrderStage {
    Open,
    Close,
}

impl ToString for OrderStage {
    fn to_string(&self) -> String {
        match self {
            OrderStage::Open => "open".to_string(),
            OrderStage::Close => "close".to_string(),
        }
    }
}
