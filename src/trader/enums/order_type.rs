use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Copy, Default)]
pub enum OrderType {
    Market,
    #[default]
    Limit,
}
